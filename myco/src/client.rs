//! Client
//! 
//! In Myco, clients in Myco participate in two main activities: sending (writing) and receiving 
//! (reading) messages. When sending, a client encrypts their message using a shared key, derives a 
//! pseudorandom location using another shared key, and sends this encrypted message to S1 along with 
//! a specially derived encryption key for that epoch. When receiving, a client computes where their 
//! message should be located using shared keys and the epoch information, downloads the corresponding 
//! path from S2's tree structure, and then decrypts their message using their shared keys. 
//! Importantly, clients must participate in every epoch by either sending real messages or fake ones 
//! ("cover traffic"), and must perform a fixed number of reads per epoch (using fake reads to fill 
//! any gaps) to maintain privacy.

use crate::{
    constants::{D_BYTES, INNER_BLOCK_SIZE, LAMBDA_BYTES, MESSAGE_SIZE, NOTIF_LOC_BYTES}, crypto::{prf_fixed_length, verify}, dtypes::TreeMycoKey, logging::LatencyMetric, network::{server1::Server1Access, server2::Server2Access}, tree::get_tree_index_for_bucket, utils::Index
};
use crate::{constants::{D, Q}, crypto::{decrypt, encrypt, kdf}, dtypes::{Block, Bucket, Key}, error::MycoError, utils::trim_zeros};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;

/// Define the Keys struct
#[derive(Clone)]
pub struct Keys {
    pub k_enc: Key<TreeMycoKey>,
    pub k_renc: Key<TreeMycoKey>,
    pub k_rk: Key<TreeMycoKey>,
    pub k_ntf: Key<TreeMycoKey>,
    pub k_rk_ntf: Key<TreeMycoKey>,
}

/// A Myco client (user).
// #[derive(Clone)]
pub struct Client {
    /// The client's ID.
    pub id: String,
    /// The current epoch of the client.
    pub epoch: usize,
    /// The client's keys.
    pub sending_keys: HashMap<String, Keys>,
    /// The client's keys.
    pub receiving_keys: HashMap<String, Keys>,
    /// Access to Server1.
    pub s1: Box<dyn Server1Access>,
    /// Access to Server2.
    pub s2: Box<dyn Server2Access>,
    /// Number of total clients in the system.
    pub num_clients: usize,
    /// Random number generator for the client.
    rng: ChaCha20Rng,
}

impl Client {
    /// Create a new Client instance.
    pub fn new(id: String, s1: Box<dyn Server1Access>, s2: Box<dyn Server2Access>, num_clients: usize) -> Self {
        Client {
            id,
            epoch: 0,
            sending_keys: HashMap::new(),
            receiving_keys: HashMap::new(),
            s1,
            s2,
            num_clients,
            rng: ChaCha20Rng::from_entropy(),
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Setup Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////

    /// Setup the client with a key.
    pub fn setup(&mut self, k_q: Vec<Key<TreeMycoKey>>, contact_list: Vec<String>) -> Result<(), MycoError> {
        let local_latency = LatencyMetric::new("client_setup");

        // Parallelize key setup using rayon
        let results: Vec<(String, Keys, Keys)> = k_q.into_par_iter()
            .zip(contact_list.into_par_iter())
            .map(|(k, c)| {
                let (suffix_sending, suffix_receiving) = if self.id < c {
                    ("1", "2")
                } else if self.id > c {
                    ("2", "1")
                } else {
                    ("", "")
                };

                // Derive sending keys
                let sending_keys = Keys {
                    k_enc: Key::new(kdf(&k.bytes, &format!("MSG{}", suffix_sending)).unwrap()),
                    k_renc: Key::new(kdf(&k.bytes, &format!("ORAM{}", suffix_sending)).unwrap()),
                    k_rk: Key::new(kdf(&k.bytes, &format!("PRF{}", suffix_sending)).unwrap()),
                    k_ntf: Key::new(kdf(&k.bytes, &format!("MSG_NOTIF{}", suffix_sending)).unwrap()),
                    k_rk_ntf: Key::new(kdf(&k.bytes, &format!("PRF_ntf{}", suffix_sending)).unwrap()),
                };

                // Derive receiving keys
                let receiving_keys = Keys {
                    k_enc: Key::new(kdf(&k.bytes, &format!("MSG{}", suffix_receiving)).unwrap()),
                    k_renc: Key::new(kdf(&k.bytes, &format!("ORAM{}", suffix_receiving)).unwrap()),
                    k_rk: Key::new(kdf(&k.bytes, &format!("PRF{}", suffix_receiving)).unwrap()),
                    k_ntf: Key::new(kdf(&k.bytes, &format!("MSG_NOTIF{}", suffix_receiving)).unwrap()),
                    k_rk_ntf: Key::new(kdf(&k.bytes, &format!("PRF_ntf{}", suffix_receiving)).unwrap()),
                };
                
                (c, sending_keys, receiving_keys)
            })
            .collect();
        
        // Insert the keys into the maps
        for (c, sending_keys, receiving_keys) in results {
            self.sending_keys.insert(c.clone(), sending_keys);
            self.receiving_keys.insert(c, receiving_keys);
        }
        
        local_latency.finish();
        Ok(())
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Write Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////

    /// Internal function to prepare the data for writing to Server1.
    fn _prepare_write_data(&mut self, msg: &[u8], c_r: &str) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Key<TreeMycoKey>, Vec<u8>), MycoError> {
        let local_latency = LatencyMetric::new("client_write_local");
        let epoch = self.epoch;
        let c_s = self.id.as_bytes().to_vec();
        
        let keys = self.sending_keys.get(c_r)
            .ok_or_else(|| MycoError::KeyError(format!("Sending key for recipient '{}' not found", c_r)))?;
        
        let ct = encrypt(&keys.k_enc.bytes, msg, MESSAGE_SIZE)?;
        let ct_ntf = prf_fixed_length(&keys.k_ntf.bytes, &epoch.to_be_bytes(), LAMBDA_BYTES)?;
        let f = prf_fixed_length(&keys.k_rk.bytes, &epoch.to_be_bytes(), D_BYTES)?;
        let f_ntf = prf_fixed_length(&keys.k_rk_ntf.bytes, &epoch.to_be_bytes(), NOTIF_LOC_BYTES)?;
        
        let k_renc_t = kdf(&keys.k_renc.bytes, &epoch.to_string())?;
        
        self.epoch += 1;
        local_latency.finish();
        
        Ok((ct, ct_ntf, f, f_ntf, Key::new(k_renc_t), c_s))
    }
    
    /// Asynchronously write a message to Server1.
    pub async fn async_write(&mut self, msg: &[u8], c_r: &str) -> Result<(), MycoError> {
        let end_to_end_latency = LatencyMetric::new("client_write_end_to_end");
        let (ct, ct_ntf, f, f_ntf, k_renc_t, c_s) = self._prepare_write_data(msg, c_r)?;
        
        let result = self.s1
            .queue_write(ct, ct_ntf, f, f_ntf, k_renc_t, c_s)
            .await;
            
        result.map_err(|e| MycoError::ServerError(format!("queue_write failed: {:?}", e)))?;
        end_to_end_latency.finish();
        Ok(())
    }
    
    /// Synchronously write a message to Server1.
    pub fn write(&mut self, msg: &[u8], c_r: &str) -> Result<(), MycoError> {
        let end_to_end_latency = LatencyMetric::new("client_write_end_to_end");
        let (ct, ct_ntf, f, f_ntf, k_renc_t, c_s) = self._prepare_write_data(msg, c_r)?;
        
        let result = futures::executor::block_on(
            self.s1.queue_write(ct, ct_ntf, f, f_ntf, k_renc_t, c_s)
        );
        
        result.map_err(|e| MycoError::ServerError(format!("queue_write failed: {:?}", e)))?;
        end_to_end_latency.finish();
        Ok(())
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Read Operations
    //////////////////////////////////////////////////////////////////////////////////////////////////

    /// Internal function to process notifications locally.
    fn _prepare_notifications(&self, delta: Option<usize>, k_srk_ts: &[Key<TreeMycoKey>]) -> HashMap<u64, Vec<Index>> {
        let local_latency = LatencyMetric::new("client_prepare_notifications_local");
        let delta = delta.unwrap_or(1);
        
        let ls_mutex = std::sync::Mutex::new(HashMap::new());
        
        (1..=delta).into_par_iter().for_each(|epochs_past| {
            let current_epoch = self.epoch - epochs_past;
            let k_s1_t = &k_srk_ts[k_srk_ts.len() - epochs_past];
            
            let indices: Vec<Index> = self.receiving_keys.par_iter()
                .map(|(c_s, receiving_keys)| {
                    let f_ntf = prf_fixed_length(
                        &receiving_keys.k_rk_ntf.bytes,
                        &current_epoch.to_be_bytes(),
                        NOTIF_LOC_BYTES
                    )
                    .unwrap();
                    
                    let l_ntf = prf_fixed_length(
                        &k_s1_t.bytes,
                        &[&f_ntf[..], c_s.as_bytes()].concat(),
                        NOTIF_LOC_BYTES
                    )
                    .unwrap();
                    
                    l_ntf
                })
                .collect();
            
            let mut ls = ls_mutex.lock().unwrap();
            ls.insert(epochs_past as u64, indices);
        });
        
        local_latency.finish();
        std::sync::Mutex::into_inner(ls_mutex).unwrap()
    }

    /// Internal function to process epoch buckets locally using parallel iteration over buckets.
    fn _read_notifications(&self, epoch_buckets: HashMap<u64, Vec<Bucket>>) -> HashMap<usize, Vec<String>> {
        let local_latency = LatencyMetric::new("client_read_notifications_local");
        let mut notifications: HashMap<usize, Vec<String>> = HashMap::new();

        for (epochs_past, buckets) in epoch_buckets {
            let current_epoch = self.epoch - epochs_past as usize;
            
            let epoch_notifs: HashMap<String, Vec<u8>> = self.receiving_keys
                .par_iter()
                .map(|(c_s, keys)| {
                    let computed_ct_ntf = prf_fixed_length(
                        &keys.k_ntf.bytes, 
                        &current_epoch.to_be_bytes(), 
                        LAMBDA_BYTES
                    ).unwrap();
                    
                    (c_s.clone(), computed_ct_ntf)
                })
                .collect();
            
            let epoch_senders: Vec<String> = buckets.par_iter()
                .flat_map(|bucket| {
                    let mut senders = Vec::new();
                    for block in bucket.iter() {
                        for (c_s, expected_notif) in &epoch_notifs {
                            if *expected_notif == block.0 {
                                senders.push(c_s.clone());
                            }
                        }
                    }
                    senders
                })
                .collect();
            
            if !epoch_senders.is_empty() {
                notifications.insert(current_epoch, epoch_senders);
            }
        }
        
        local_latency.finish();
        notifications
    }

    /// Internal function to process read operations locally.
    fn _prepare_read(&self, notifications: HashMap<usize, Vec<String>>, k_srk_ts: Vec<Key<TreeMycoKey>>) -> Result<(String, Key<TreeMycoKey>, Vec<u8>), MycoError> {
        let local_latency = LatencyMetric::new("client_prepare_read_local");
        let (c_s, t) = self.push_policy(notifications.clone());
        
        let keys = self.receiving_keys.get(&c_s)
            .ok_or_else(|| MycoError::KeyError(format!("Receiving key for '{}' not found", c_s)))?;
        
        let k_renc_t = kdf(&keys.k_renc.bytes, &t.to_string())
            .map_err(|_e| MycoError::CryptoError(format!("Encryption failed: {:?}", _e)))?;
        
        let f = prf_fixed_length(&keys.k_rk.bytes, &t.to_be_bytes(), D_BYTES)?;

        let idx = t - (self.epoch - k_srk_ts.len());
        let k_s1_t = &k_srk_ts[idx];
        // Calculate the path location using the server's key and the derived PRF value.
        let l = prf_fixed_length(&k_s1_t.bytes, &[&f[..], c_s.as_bytes()].concat(), D_BYTES)?;
        
        local_latency.finish();
        Ok((c_s, Key::new(k_renc_t), l))
    }

    /// Internal function to process the decrypted message from the bucket tree.
    fn _process_read(&self, buckets: Vec<Bucket>, k_renc_t: &Key<TreeMycoKey>, keys: &Keys, path_l: &Vec<u8>) -> Result<Vec<u8>, MycoError> {
        let local_latency = LatencyMetric::new("client_process_read_local");

        let potential_message = buckets.par_iter().enumerate().find_map_any(|(i, bucket)| {
            bucket.iter().find_map(|block| {
                if let Ok(ct) = decrypt(&k_renc_t.bytes, &block.0) {
                    if let Ok(msg) = decrypt(&keys.k_enc.bytes, &ct) {
                        return Some((trim_zeros(&msg), bucket, i));
                    }
                }
                None
            })
        });
        
        let result = if let Some((message, bucket, bucket_index)) = potential_message {            
            let mut data = Vec::new();
            let tree_index = get_tree_index_for_bucket(path_l, bucket_index);
            data.extend_from_slice(&tree_index.to_be_bytes());
            for block in bucket.iter() {
                data.extend_from_slice(&block.0);
            }
            
            match &bucket.1 {
                Some(sig) => {
                    verify(&data, sig)
                        .map_err(|_| MycoError::CryptoError("Bucket signature invalid".to_string()))
                        .map(|_| message)
                },
                None => {
                    Err(MycoError::CryptoError("Bucket missing signature".to_string()))
                }
            }
        } else {
            Err(MycoError::CryptoError("No valid message found in any bucket".to_string()))
        };

        local_latency.finish();
        result
    }

    /// Synchronously read messages from Server2.
    pub fn read(&mut self, delta: Option<usize>) -> Result<Vec<u8>, MycoError> {
        let end_to_end_latency = LatencyMetric::new("client_read_end_to_end");
        
        let k_srk_ts = futures::executor::block_on(self.s2.get_prf_keys())
            .map_err(|e| MycoError::ServerError(format!("Failed to get PRF keys: {:?}", e)))?;

        let k_srk_ts_slice = &k_srk_ts[k_srk_ts.len() - delta.unwrap_or(1)..];

        let ls = self._prepare_notifications(delta, k_srk_ts_slice);

        let epoch_buckets = futures::executor::block_on(self.s2.read_notifs(ls))
            .map_err(|e| MycoError::ServerError(format!("Failed to read notifications: {:?}", e)))?;
        
        let notifications = self._read_notifications(epoch_buckets);

        // If no notifications exist, then perform a fake read (cover traffic) and return an empty message.
        if notifications.is_empty() {
            let _ = self.fake_read();
            return Ok(vec![]);
        }

        let (c_s, k_renc_t, l) = self._prepare_read(notifications, k_srk_ts)?;

        let buckets = futures::executor::block_on(self.s2.read(l.clone()))
            .map_err(|e| MycoError::ServerError(format!("Failed to read path indices: {:?}", e)))?;

        // Process the buckets to try to decrypt the message.
        let result = self._process_read(
            buckets,
            &k_renc_t,
            self.receiving_keys.get(&c_s)
                .ok_or_else(|| MycoError::KeyError(format!("3. Receiving key for '{}' not found", c_s)))?,
            &l, // Pass the path to _process_read
        );
        
        end_to_end_latency.finish();
        result
    }

    /// Generate fake write data.
    pub fn fake_write(&mut self) -> Result<(), MycoError> {
        let f: Vec<u8> = (0..D).map(|_| self.rng.gen()).collect();
        let f_ntf: Vec<u8> = (0..Q * self.num_clients).map(|_| self.rng.gen()).collect();
        // Generate random data for a fake write operation
        let k_renc_t: Key<TreeMycoKey> = Key::random(&mut self.rng);
        let ct: Vec<u8> = Block::new_dummy(INNER_BLOCK_SIZE, &mut self.rng).0;
        let ct_ntf: Vec<u8> = Block::new_random(LAMBDA_BYTES, &mut self.rng).0;
        let c_s: Vec<u8> = self.id.clone().into_bytes();
        futures::executor::block_on(self.s1.queue_write(ct, ct_ntf, f, f_ntf, k_renc_t, c_s))
    }

    /// Generate fake read data.
    pub fn fake_read(&mut self) -> Vec<Bucket> {
        let l: Vec<u8> = (0..D).map(|_| self.rng.gen()).collect();
        futures::executor::block_on(self.s2.read(l)).unwrap()
    }

    fn push_policy(&self, notifications: HashMap<usize, Vec<String>>) -> (String, usize) {
        if let Some((&epoch, senders)) = notifications.iter().next() {
            if let Some(sender) = senders.first() {
                return (sender.clone(), epoch);
            }
        }
        ("".to_string(), 0)
    }

    /// Increments the client's current epoch.
    pub fn increment_epoch(&mut self) {
        self.epoch += 1;
    }

    /// Asynchronously read messages from Server2.
    pub async fn async_read(&mut self, delta: Option<usize>) -> Result<Vec<u8>, MycoError> {
        let end_to_end_latency = LatencyMetric::new("client_read_end_to_end");
        
        // Step 1: Retrieve PRF keys from Server2.
        let k_srk_ts = self
            .s2
            .get_prf_keys()
            .await
            .map_err(|e| MycoError::ServerError(format!("Failed to retrieve PRF keys from server2: {:?}", e)))?;
    
        // Step 2: Extract the relevant PRF key values.
        let k_s1_t_values = k_srk_ts[k_srk_ts.len() - delta.unwrap_or(1)..].to_vec();
    
        // Step 3: Process notifications locally.
        let ls = self._prepare_notifications(delta, &k_s1_t_values);
    
        // Step 4: Retrieve notifications from Server2.
        let epoch_buckets = self.s2.read_notifs(ls)
            .await
            .map_err(|e| MycoError::ServerError(format!("Failed to read notifications from server2: {:?}", e)))?;
    
        // Step 5: Process the epoch buckets locally.
        let notifications = self._read_notifications(epoch_buckets);
    
        // If no notifications exist, then perform an asynchronous fake read
        if notifications.is_empty() {
            let _ = self.async_fake_read().await;
            return Ok(vec![]);
        }
    
        // Step 6: Process the read operation and compute required parameters.
        let (c_s, k_renc_t, l) = self._prepare_read(notifications, k_s1_t_values)?;
    
        // Step 7: Retrieve the corresponding path buckets from Server2.
        let buckets = self.s2.read(l.clone())
            .await
            .map_err(|e| MycoError::ServerError(format!("Failed to read path indices: {:?}", e)))?;
        
        // Step 8: Process the buckets to attempt decryption of the client message.
        let result = self._process_read(
            buckets,
            &k_renc_t,
            self.receiving_keys.get(&c_s)
                .ok_or_else(|| MycoError::KeyError(format!("4. Receiving key for '{}' not found", c_s)))?,
            &l, // Pass the path to _process_read
        );
        
        end_to_end_latency.finish();
        result
    }

    /// Asynchronously perform a fake read (cover traffic) without blocking.
    pub async fn async_fake_read(&mut self) -> Vec<Bucket> {
        let l: Vec<u8> = (0..D).map(|_| self.rng.gen()).collect();
        self.s2.read(l).await.unwrap()
    }
}


