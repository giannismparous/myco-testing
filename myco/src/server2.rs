//! Server2
//! 
//! S2 (Server 2) functions as a storage server, maintaining a tree-based data structure for message 
//! storage and handling client read operations. It receives batched updates from S1 with re-encrypted 
//! and reorganized messages, but cannot discern their intended locations. Clients read messages by 
//! downloading paths from S2's tree, but S2 cannot link these reads to previous writes due to the 
//! random path selection. S2 also stores and provides PRF keys for clients to compute message paths, 
//! ensuring privacy by preventing correlation between writes and reads.

use std::collections::HashMap;

use crate::{constants::{D, DELTA, Q}, dtypes::{Bucket, Key}, error::MycoError, logging::LatencyMetric};
use crate::{dtypes::{Path, TreeMycoKey}, tree::{BinaryTree, SparseBinaryTree}, utils::{l_to_u64, Index}};
use rayon::prelude::*;
use dashmap;
use sys_info::mem_info;
use tracing::info;

/// The main server2 struct.
pub struct Server2 {
    /// The tree storing the buckets.
    pub tree: BinaryTree<Bucket>,
    /// The PRF keys.
    pub prf_keys: Vec<Key<TreeMycoKey>>,
    /// The current epoch.
    pub epoch: u64,
    /// The t-tables.
    pub notification_tables: Vec<Vec<Bucket>>,
    /// The number of clients.
    pub num_clients: usize,
    /// The index of the latest notification table.
    pub latest_notif_table_idx: usize,
}

impl Server2 {
    /// Create a new Server2 instance.
    pub fn new(num_clients: usize) -> Self {
        // Initialize a binary tree with fixed depth
        let mut tree = BinaryTree::new_with_depth(D);

        tree.fill(Bucket::default());
        
        // Generate PRF keys
        let prf_keys: Vec<Key<TreeMycoKey>> = (0..DELTA).map(|_| Key::<TreeMycoKey>::random(&mut rand::thread_rng())).collect();
            
        println!("Server2::new: Created server with D={}, DB_SIZE={}, num_clients={}, DELTA={}",
                crate::constants::D, 1 << crate::constants::D, num_clients, crate::constants::DELTA);
        
        // Pre-allocate all notification tables (DELTA of them)
        let notification_tables = vec![Vec::new(); DELTA as usize];
                 
        // Server2 instance.
        Server2 {
            tree,
            notification_tables,
            prf_keys,
            epoch: 0,
            num_clients,
            latest_notif_table_idx: 0,
        }
    }

    /// Read a path from the tree.
    pub fn read(&self, l: Vec<u8>) -> Result<Vec<Bucket>, MycoError> {
        let local_latency = LatencyMetric::new("server2_read");
        let path = Path::from(l);
        let buckets = self.tree.get_all_nodes_along_path(&path);
        local_latency.finish();
        Ok(buckets)
    }
    /// Write a batch of buckets to the tree.
    pub fn write(&mut self, pt: SparseBinaryTree<Bucket>) {
        let local_latency = LatencyMetric::new("server2_write_local");
        // Iterate over self.pathset_indices and packed_buckets, and overwrite corresponding values in self.tree
        for (index, bucket) in pt.packed_indices.iter().zip(pt.packed_buckets.iter()) {
            self.tree.value[*index] = Some(bucket.clone());
        }
        // Increment the epoch
        self.epoch += 1;
        local_latency.finish();
    }

    /// Write a single chunk of buckets to the server.
    pub fn chunk_write(&mut self, buckets: Vec<Bucket>, pathset_indices: Vec<usize>) {
        let local_latency = LatencyMetric::new("server2_chunk_write");
        
        // Write buckets to the tree at the indices specified by pathset_indices
        pathset_indices
            .iter()
            .zip(buckets)
            .for_each(|(idx, bucket)| {
                self.tree.value[*idx] = Some(bucket);
            });
            
        local_latency.finish();
    }

    /// Increments the epoch and adds the new PRF key.
    pub fn finalize_epoch(&mut self, key: &Key<TreeMycoKey>) {
        // Increment the epoch.
        self.epoch += 1;

        self.add_prf_key(key);
    }

    /// Logs the memory usage of the server.
    pub fn log_memory_usage(&self) {
        let _num_buckets = self.tree.value.len();

        let mut total_tree_size = 0;
        for bucket in self.tree.value.iter() {
            let mut total_size_of_bucket = 0;
            if let Some(bucket) = bucket {
                for block in bucket.iter() {
                    total_size_of_bucket += block.0.len();
                }
                total_size_of_bucket += bucket.1.as_ref().map_or(0, |x| x.len());
            }
            total_tree_size += total_size_of_bucket;
        }

        let mut total_ntf_size = 0;
        for table in self.notification_tables.iter() {
            let table_size = table.iter().map(|bucket| {
                let mut bucket_size = 0;
                for block in bucket.iter() {
                    bucket_size += block.0.len();
                }
                bucket_size += bucket.1.as_ref().map_or(0, |x| x.len());
                bucket_size
            }).sum::<usize>();
            total_ntf_size += table_size;
        }

        let server_2_data_structure_mem_usage = total_tree_size + total_ntf_size;

        let (used_gb, total_gb) = get_system_memory_stats();
        info!("{} - Tree size: {:.2} GB", "Server2::finalize_epoch", total_tree_size as f64 / (1024.0 * 1024.0 * 1024.0));
        info!("{} - Notif table size: {:.2} GB", "Server2::finalize_epoch", total_ntf_size as f64 / (1024.0 * 1024.0 * 1024.0));
        info!("{} - Total \"data\" memory: {:.2} GB", "Server2::finalize_epoch", server_2_data_structure_mem_usage as f64 / (1024.0 * 1024.0 * 1024.0));
        info!("{} - Process memory usage: {:.2}/{:.2} GB ({:.1}%)", "Server2::finalize_epoch", used_gb, total_gb, (used_gb / total_gb) * 100.0);
    }

    /// Get the PRF keys.
    pub fn get_prf_keys(&self) -> Result<Vec<Key<TreeMycoKey>>, MycoError> {
        Ok(self.prf_keys.clone())
    }

    /// Add a PRF key to the server.
    pub fn add_prf_key(&mut self, key: &Key<TreeMycoKey>) {
        let local_latency = LatencyMetric::new("server2_add_prf_key");
        self.prf_keys.push(key.clone());
        self.prf_keys.remove(0);
        self.latest_notif_table_idx = (self.latest_notif_table_idx + 1) % DELTA as usize;
        local_latency.finish();
    }

    /// Get the notification tables.
    pub fn read_notifs(&self, ls: HashMap<u64, Vec<Index>>) -> Result<HashMap<u64, Vec<Bucket>>, MycoError> {
        let local_latency = LatencyMetric::new("server2_read_notifs");
        let mut epoch_buckets: HashMap<u64, Vec<Bucket>> = HashMap::with_capacity(ls.len());
        // Process each epoch
        for (&delta, indices) in &ls {
            // Get the notification table for this epoch using modular arithmetic
            let index = (self.latest_notif_table_idx + DELTA as usize - delta as usize) % DELTA as usize;
            let t_table_ntf = &self.notification_tables[index];

            // Pre-allocate the vector to avoid reallocations
            let mut buckets = Vec::with_capacity(indices.len());
            
            // For smaller sets of indices, sequential is faster than parallel
            if indices.len() < 100 {
                // Sequential processing - avoids parallelization overhead for small sets
                for l in indices {
                    let idx = l_to_u64(l, self.num_clients) as usize;
                    // Use unchecked access if we're confident indices are valid, otherwise keep the get()
                    if let Some(bucket) = t_table_ntf.get(idx) {
                        // Use a reference or shallow clone if possible
                        buckets.push(bucket.clone());
                    }
                }
            } else {
                // Use parallelization only for larger workloads where it makes sense
                buckets = indices
                    .par_iter()
                    .map(|l| {
                        let idx = l_to_u64(l, self.num_clients) as usize;
                        t_table_ntf.get(idx).cloned().unwrap()
                    })
                    .collect();
            }
            epoch_buckets.insert(delta, buckets);
        }
        local_latency.finish();
        Ok(epoch_buckets)
    }

    /// Get the notification tables without cloning, returning references.
    pub fn read_notifs_refs<'a>(&'a self, ls: HashMap<u64, Vec<Index>>) -> Result<HashMap<u64, Vec<&'a Bucket>>, MycoError> {
        let local_latency = LatencyMetric::new("server2_read_notifs_refs");
        
        if self.notification_tables.is_empty() {
            return Ok(HashMap::new());
        }
                 
        // Use DashMap for concurrent access
        let results = dashmap::DashMap::with_capacity(ls.len());
        
        // Process each delta and indices
        let process_result = ls.into_par_iter().try_for_each(|(delta, indices)| {
            // Get the notification table for this epoch using modular arithmetic
            let table_idx = (self.latest_notif_table_idx + DELTA as usize - delta as usize) % DELTA as usize;
            let t_table_ntf = &self.notification_tables[table_idx];
            
            // Collect buckets based on indices
            let buckets: Vec<&Bucket> = if indices.len() > 10000 {
                indices.par_chunks(1000)
                    .flat_map_iter(|chunk| {
                        chunk.iter().map(|l| {
                            let idx = l_to_u64(l, self.num_clients) as usize;
                            if idx >= t_table_ntf.len() {
                                return None;
                            }
                            Some(&t_table_ntf[idx])
                        }).filter_map(|x| x)
                    })
                    .collect()
            } else {
                indices.iter()
                    .filter_map(|l| {
                        let idx = l_to_u64(l, self.num_clients) as usize;
                        if idx >= t_table_ntf.len() {
                            return None;
                        }
                        Some(&t_table_ntf[idx])
                    })
                    .collect()
            };
            
            results.insert(delta, buckets);
            Ok::<_, MycoError>(())
        });
        
        if let Err(e) = process_result {
            return Err(e);
        }
        
        // Convert DashMap back to HashMap
        let results = results.into_iter().collect();
        
        local_latency.finish();
        Ok(results)
    }

    pub fn write_notifs(&mut self, notif_buckets: Vec<Bucket>) {
        let local_latency = LatencyMetric::new("server2_write_notifs");
        
        // Write to the current notification table using latest_notif_table_idx
        let current_idx = self.latest_notif_table_idx;
        self.notification_tables[current_idx] = notif_buckets;
        
        local_latency.finish();
    }

    /// Write notification buckets at specific indices
    pub fn write_notifs_with_indices(&mut self, notif_buckets: Vec<Bucket>, indices: Vec<usize>) {
        let local_latency = LatencyMetric::new("server2_write_notifs_with_indices");
        
        let max_index = indices.last().unwrap();

        // Get the current table using modular arithmetic
        let current_idx = self.latest_notif_table_idx;
        let current_table_ntf = &mut self.notification_tables[current_idx];
        
        // Make sure the table has enough capacity
        let required_capacity = std::cmp::max(max_index + 1, self.num_clients * Q);
        if current_table_ntf.len() < required_capacity {
            current_table_ntf.resize(required_capacity, Bucket::default());
        }
        
        // Check if indices are contiguous - this is an optimization for the common case
        let is_contiguous = indices.len() > 1 && 
            indices.windows(2).all(|w| w[1] == w[0] + 1) &&
            indices.first().map_or(false, |&first| first + indices.len() - 1 == *max_index);
        
        if is_contiguous && notif_buckets.len() == indices.len() {
            // Contiguous indices case - use efficient bulk copy
            let start_idx = indices[0];
            for (i, bucket) in notif_buckets.into_iter().enumerate() {
                let idx = start_idx + i;
                if idx >= current_table_ntf.len() {
                    continue;
                }
                current_table_ntf[idx] = bucket;
            }
        } else if notif_buckets.len() > 1000 {
            // For large, non-contiguous datasets, prepare in parallel but write sequentially
            // Zip bucket and index into pairs
            let mut pairs: Vec<_> = notif_buckets.into_iter()
                .zip(indices.into_iter())
                .collect();
            
            // Sort by index for better cache locality
            pairs.sort_unstable_by_key(|(_, idx)| *idx);
            
            // Write in sorted order
            for (bucket, idx) in pairs {
                if idx >= current_table_ntf.len() {
                    continue;
                }
                current_table_ntf[idx] = bucket;
            }
        } else {
            // For smaller sets, direct approach is more efficient
            for (bucket, idx) in notif_buckets.into_iter().zip(indices.into_iter()) {
                if idx >= current_table_ntf.len() {
                    continue;
                }
                current_table_ntf[idx] = bucket;
            }
        }
        
        local_latency.finish();
    }

    /// Read multiple paths from the tree in parallel.
    pub fn parallel_read(&self, indices: Vec<Vec<u8>>) -> Result<Vec<Vec<Bucket>>, MycoError> {        
        // Process all indices in parallel using Rayon
        let results: Vec<Vec<Bucket>> = indices.par_iter()
            .map(|l| {
                let path = Path::from(l.clone());
                self.tree.get_all_nodes_along_path(&path)
            })
            .collect();
            
        Ok(results)
    }
}

/// Get the memory usage in GB for used and total memory.
fn get_system_memory_stats() -> (f64, f64) {
    match mem_info() {
        Ok(info) => {
            let used_gb = (info.total - info.free) as f64 / (1024.0 * 1024.0);
            let total_gb = info.total as f64 / (1024.0 * 1024.0);
            (used_gb, total_gb)
        },
        Err(e) => {
            println!("Failed to get memory info: {}", e);
            (0.0, 0.0)
        },
    }
}