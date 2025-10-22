//! Server1
//!
//! S1 (Server 1) acts as the entry point for client writes in Myco, coordinating a tree-based
//! oblivious data structure system. When clients write messages, S1 receives encrypted message
//! content along with a pseudorandom location value and encryption key. S1 effectively functions
//! as an "ORAM-like client" that mediates between the writing clients and S2's storage tree.

use crate::{
    constants::*, crypto::{prf_fixed_length, sign}, dtypes::{Metadata, MetadataBucket, Path, TreeMycoKey}, logging::LatencyMetric, network::server2::Server2Access, tree::{BinaryTree, SparseBinaryTree}, utils::{derive_rng, get_path_indices, l_to_u64}
};
use crate::{constants::{D, DELTA}, crypto::{decrypt, encrypt}, dtypes::{Block, Bucket, Key}, error::MycoError};
use dashmap::DashMap;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator
};

/// The main server1 struct.
pub struct Server1 {
    /// The current epoch of the server.
    pub epoch: u64,
    /// The key used for server1 transactions.
    pub k_s1_t: Key<TreeMycoKey>,
    /// The number of clients connected to the server.
    pub num_clients: usize,
    /// Access to Server2.
    pub s2: Box<dyn Server2Access>,
    /// Sparse binary tree for storing buckets.
    pub p: SparseBinaryTree<Bucket>,
    /// Sparse binary tree for temporary storage of buckets.
    pub pt: SparseBinaryTree<Bucket>,
    /// Sparse binary tree for temporary storage of metadata.
    pub metadata_pt: SparseBinaryTree<MetadataBucket>,
    /// Binary tree for storing metadata.
    pub metadata: BinaryTree<MetadataBucket>,
    /// Indices of the pathset.
    pub pathset_indices: Vec<usize>,
    /// Queue for storing messages.
    pub message_queue: DashMap<usize, Vec<(Vec<u8>, Key<TreeMycoKey>, u64, Path)>>,
    /// Mirror of the tree in S2
    pub tree: BinaryTree<Bucket>,
    /// Table for storing notifications.
    pub t_table_ntf: Vec<Bucket>,
    rng: ChaCha20Rng,
}

impl Server1 {
    /// Create a new Server1 instance.
    pub fn new(s2: Box<dyn Server2Access>, num_clients: usize) -> Self {
        let mut tree = BinaryTree::new_with_depth(D);
        let mut metadata = BinaryTree::new_with_depth(D);
        let rng = ChaCha20Rng::from_entropy();

        tree.fill(Bucket::default());
        metadata.fill(MetadataBucket::default());
        let t_table_ntf = vec![Bucket::default(); Q * num_clients];

        Self {
            epoch: 0,
            k_s1_t: Key::new(vec![]),
            num_clients,
            s2,
            p: SparseBinaryTree::new(),
            pt: SparseBinaryTree::new(),
            metadata_pt: SparseBinaryTree::new(),
            metadata,
            pathset_indices: vec![],
            message_queue: DashMap::new(),
            tree,
            t_table_ntf,
            rng,
        }
    }

    /// Initialize the server for a new batch.
    ///
    /// When server_type is Async, this performs asynchronous initialization with latency tracking.
    /// When server_type is Sync, this performs synchronous initialization by blocking on async calls.

    pub fn batch_init(&mut self) -> Result<(), MycoError> {
        let local_latency = LatencyMetric::new("server1_batch_init");
        
        self.t_table_ntf.par_iter_mut().for_each(|bucket| {
            bucket.clear();
        });
        
        let paths = (0..(NU * self.num_clients))
            .map(|_| Path::random(&mut self.rng))
            .collect::<Vec<Path>>();
        
        self.pathset_indices = get_path_indices(paths);
        
        let buckets: Vec<Bucket> = self
            .pathset_indices
            .iter()
            .map(|i| self.tree.value[*i].clone().unwrap())
            .collect();
        
        let bucket_size = buckets.len();
        
        self.p = SparseBinaryTree::new_with_data(buckets, self.pathset_indices.clone());
        
        self.pt = SparseBinaryTree::new_with_data(vec![Bucket::default(); bucket_size], self.pathset_indices.clone());
        
        self.metadata_pt = SparseBinaryTree::new_with_data(vec![MetadataBucket::default(); bucket_size], self.pathset_indices.clone());

        self.k_s1_t = Key::random(&mut self.rng);
        
        local_latency.finish();
        Ok(())
    }

    /// Queues an individual write.
    pub fn queue_write(
        &mut self,
        ct: Vec<u8>,
        ct_ntf: Vec<u8>,
        f: Vec<u8>,
        f_ntf: Vec<u8>,
        k_renc_t: Key<TreeMycoKey>,
        c_s: Vec<u8>,
    ) -> Result<(), MycoError> {
        let local_latency = LatencyMetric::new("server1_queue_write");
        
        let t_exp = self.epoch + DELTA as u64;
        let l: Vec<u8> = prf_fixed_length(&self.k_s1_t.bytes, &[&f[..], &c_s[..]].concat(), D_BYTES)
            .map_err(|e| MycoError::ServerError(format!(
                "PRF computation failed in queue_write: {:?}",
                e
            )))?;

        let intended_message_path = Path::from(l);
        let (lca_idx, _) = self
            .pt
            .lca_idx(&intended_message_path)
            .ok_or(MycoError::CryptoError("LCA not found".to_string()))?;
        
        self.message_queue.entry(lca_idx).or_default().push((
            ct,
            k_renc_t,
            t_exp,
            intended_message_path,
        ));
        
        let l_ntf = prf_fixed_length(&self.k_s1_t.bytes, &[&f_ntf[..], &c_s[..]].concat(), NOTIF_LOC_BYTES)
            .map_err(|e| MycoError::ServerError(format!(
                "PRF computation for notification failed: {:?}",
                e
            )))?;

        let l_ntf = l_to_u64(&l_ntf, self.num_clients) as usize;
        self.t_table_ntf[l_ntf].push(Block(ct_ntf));
        
        local_latency.finish();
        Ok(())
    }

    /// Internal batch write function.
    pub fn _batch_write(&mut self) -> Result<(), MycoError> {
        let local_latency = LatencyMetric::new("server1_batch_write_local");

        let global_seed: [u8; 32] = self.rng.gen();

        // Process old buckets in parallel
        self.p.zip_with_binary_tree(&self.metadata)
            .par_iter()
            .for_each(|(bucket, metadata_bucket, _)| {
                if let (Some(bucket), Some(metadata_bucket)) = (bucket, metadata_bucket) {
                    let mut real_decrypt_count = 0;
                    (0..bucket.len()).for_each(|b| {
                        if let Some(metadata_block) = metadata_bucket.get(b) {
                            let l = &metadata_block.path;
                            let k_renc_t = &metadata_block.key;
                            let t_exp = metadata_block.timestamp;
                            
                            if self.epoch < t_exp {
                                let c_msg = bucket.get(b).unwrap();
                                let ct = decrypt(&k_renc_t.bytes, &c_msg.0).unwrap();
                                let (lca_idx, _) = self.pt.lca_idx(l).unwrap();
                                self.message_queue
                                    .entry(lca_idx)
                                    .or_default()
                                    .push((ct, k_renc_t.clone(), t_exp, l.clone()));
                                real_decrypt_count += 1;
                            }
                        }
                    });
                    let fake_decrypt_count = Z - real_decrypt_count;
                    (0..fake_decrypt_count).into_par_iter().for_each(|_| {
                        let _ = decrypt(&[0u8; LAMBDA_BYTES], &[0u8; BLOCK_SIZE]).unwrap_or_default();
                    });
                }
            });

        // Collect data into indices_buckets
        let indices_buckets: Vec<_> = self.pt.zip_mut(&mut self.metadata_pt)
            .enumerate()
            .collect();

        // Process new messages in parallel
        indices_buckets.into_par_iter().for_each(|(idx, (mut bucket, mut metadata_bucket, _))| {
            let original_idx = self.pathset_indices[idx];
            if let Some(blocks) = self.message_queue.get(&original_idx) {
                for (ct, k_renc_t, t_exp, intended_message_path) in blocks.iter() {
                    let c_msg = encrypt(&k_renc_t.bytes, &ct, INNER_BLOCK_SIZE)
                        .map_err(|_| MycoError::CryptoError("Encryption failed".to_string()))
                        .unwrap();
                    if let Some(bucket) = bucket.as_mut() {
                        bucket.push(Block::new(c_msg));
                    }
                    if let Some(metadata_bucket) = metadata_bucket.as_mut() {
                        metadata_bucket.push(Metadata::new(
                            intended_message_path.clone(),
                            k_renc_t.clone(),
                            *t_exp,
                        ));
                    }
                }
            }
            if let Some(bucket) = bucket.as_mut() {
                (bucket.len()..Z).for_each(|_| {
                    bucket.push(Block::default());
                });
                assert!(
                    bucket.len() <= Z,
                    "Bucket length exceeds Z in epoch {}: bucket length={}, expected<={}",
                    self.epoch,
                    bucket.len(),
                    Z
                );
            }
            if let Some(metadata_bucket) = metadata_bucket.as_mut() {
                (metadata_bucket.len()..Z).for_each(|_| {
                    metadata_bucket.push(Metadata::default());
                });
                assert!(
                    metadata_bucket.len() <= Z,
                    "Metadata bucket length exceeds Z: bucket length={}, expected<={}",
                    metadata_bucket.len(),
                    Z
                );
            }

            let random_string = global_seed;
            let mut bucket_shuffle_rng = ChaCha20Rng::from_seed(random_string);
            let mut metadata_shuffle_rng = ChaCha20Rng::from_seed(random_string);
            if let Some(bucket) = bucket.as_mut() {
                bucket.shuffle(&mut bucket_shuffle_rng);
            }
            if let Some(metadata_bucket) = metadata_bucket.as_mut() {
                metadata_bucket.shuffle(&mut metadata_shuffle_rng);
            }
        });

        self.message_queue.clear();
        self.metadata.overwrite_from_sparse(&self.metadata_pt);
        self.tree.overwrite_from_sparse(&self.pt);
        
        // Now replace default blocks with proper dummy blocks for transmission
        self.pt.packed_buckets.par_iter_mut().enumerate().for_each(|(idx, bucket)| {
            let mut bucket_rng = derive_rng(global_seed, idx);
            for i in 0..bucket.len() {
                if bucket[i].is_default() {
                    bucket[i] = Block::new_dummy(BLOCK_SIZE, &mut bucket_rng);
                }
            }
        });

        self.t_table_ntf.par_iter_mut().enumerate().for_each(|(i, bucket)| {
            let mut rng = derive_rng(global_seed, i);
            for _ in bucket.len()..Z_M {
                bucket.push(Block::new_random(LAMBDA_BYTES, &mut rng));
            }
            bucket.shuffle(&mut rng);
        });

        self.pt.packed_buckets.par_iter_mut().enumerate().for_each(|(i, bucket)| {
            let tree_index = self.pathset_indices[i];
            // Pre-calculate capacity needed to avoid reallocation
            let block_data_size = bucket.iter().map(|block| block.0.len()).sum::<usize>();
            let mut data = Vec::with_capacity(std::mem::size_of::<usize>() + block_data_size);
            
            data.extend_from_slice(&tree_index.to_be_bytes());
            for block in bucket.iter() {
                data.extend_from_slice(&block.0);
            }
            bucket.1 = Some(
                sign(&data).expect("Bucket signing failed")
            );
        });

        local_latency.finish();
        Ok(())
    }

    /// Asynchronous batch write using Server2.
    pub async fn async_batch_write(&mut self) -> Result<(), MycoError> {
        let end_to_end_latency = LatencyMetric::new("server1_batch_write_end_to_end");
        
        self._batch_write()?;

        // println!("Server1: started writing the result");
        let write_result: Result<(), MycoError> = self
            .s2
            .write(&self.pt, &self.t_table_ntf, &self.k_s1_t)
            .await;
        match write_result {
            Ok(_) => {
                self.epoch += 1;
                Ok(())
            }
            Err(e) => {
                Err(MycoError::ServerError(format!(
                    "async_batch_write: error writing to Server2: {:?}",
                    e
                )))
            }
        }?;
        
        end_to_end_latency.finish();
        Ok(())
    }

    /// Synchronous batch write.
    pub fn batch_write(&mut self) -> Result<(), MycoError> {
        let end_to_end_latency = LatencyMetric::new("server1_batch_write_end_to_end");
        
        self._batch_write()?;
        let write_result = futures::executor::block_on(
            self.s2.write(&self.pt, &self.t_table_ntf, &self.k_s1_t)
        );
        match write_result {
            Ok(_) => {
                self.epoch += 1;
                Ok(())
            }
            Err(e) => {
                Err(MycoError::ServerError(format!(
                    "batch_write: error writing to Server2: {:?}",
                    e
                )))
            }
        }?;
        
        end_to_end_latency.finish();
        Ok(())
    }

    /// Queue multiple writes in parallel from megaclient
    pub fn megaclient_queue_write(
        &mut self,
        cts: Vec<Vec<u8>>,
        ct_ntfs: Vec<Vec<u8>>,
        fs: Vec<Vec<u8>>,
        f_ntfs: Vec<Vec<u8>>,
        k_renc_ts: Vec<Key<TreeMycoKey>>,
        c_ss: Vec<Vec<u8>>,
    ) -> Result<(), MycoError> {
        let local_latency = LatencyMetric::new("server1_megaclient_queue_write");

        // println!("megaclient_queue_write: cts.len(): {}", cts.len());
        
        // Prepare a thread-safe structure to collect results
        let message_queue_updates: DashMap<usize, Vec<(Vec<u8>, Key<TreeMycoKey>, u64, Path)>> = DashMap::new();
        let t_table_ntf_updates: DashMap<usize, Vec<Block>> = DashMap::new();
        
        // Process all writes in parallel
        (0..cts.len()).into_par_iter().try_for_each(|i| -> Result<(), MycoError> {
            let t_exp = self.epoch + DELTA as u64;
            
            // Message path computation
            let l: Vec<u8> = prf_fixed_length(&self.k_s1_t.bytes, &[&fs[i][..], &c_ss[i][..]].concat(), D_BYTES)
                .map_err(|e| MycoError::ServerError(format!(
                    "PRF computation failed in megaclient_queue_write: {:?}",
                    e
                )))?;
            
            let intended_message_path = Path::from(l);
            let (lca_idx, _) = self
                .pt
                .lca_idx(&intended_message_path)
                .ok_or(MycoError::CryptoError("LCA not found".to_string()))?;
            
            // Add to message queue updates
            message_queue_updates.entry(lca_idx).or_default().push((
                cts[i].clone(),
                k_renc_ts[i].clone(),
                t_exp,
                intended_message_path,
            ));
            
            // Notification path computation
            let l_ntf = prf_fixed_length(&self.k_s1_t.bytes, &[&f_ntfs[i][..], &c_ss[i][..]].concat(), NOTIF_LOC_BYTES)
                .map_err(|e| MycoError::ServerError(format!(
                    "PRF computation for notification failed: {:?}",
                    e
                )))?;
            let l_ntf = l_to_u64(&l_ntf, self.num_clients) as usize;
            
            // Add to t_table_ntf updates
            t_table_ntf_updates.entry(l_ntf).or_default().push(Block(ct_ntfs[i].clone()));
            
            Ok(())
        })?;
        
        // Apply all updates to the message queue
        for entry in message_queue_updates.into_iter() {
            let lca_idx = entry.0;
            let messages = entry.1;
            self.message_queue.entry(lca_idx).or_default().extend(messages);
        }
        
        // Apply all updates to the t_table_ntf
        for entry in t_table_ntf_updates.into_iter() {
            let idx = entry.0;
            let blocks = entry.1;
            self.t_table_ntf[idx].extend(blocks);
        }
        
        local_latency.finish();
        Ok(())
    }
}