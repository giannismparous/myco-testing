use myco::constants::{LATENCY_BENCH_COUNT, MESSAGE_SIZE, Q, WARMUP_COUNT};
use myco::dtypes::Key;
use myco::logging::calculate_and_append_averages;
use myco::{
    client::Client,
    network::{server1::RemoteServer1Access, server2::RemoteServer2Access},
    enron_data,
};
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use std::error::Error;
use tokio;
use myco::utils::build_tls_channel;
use tikv_jemallocator::Jemalloc;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let s1_addr = args.get(1)
        .map(|addr| if !addr.starts_with("https://") { format!("https://{}", addr) } else { addr.clone() })
        .unwrap_or("https://localhost:3002".to_string());
    let s2_addr = args.get(2)
        .map(|addr| if !addr.starts_with("https://") { format!("https://{}", addr) } else { addr.clone() })
        .unwrap_or("https://localhost:3004".to_string());
    let num_clients = args.get(3).map(|s| s.parse::<usize>().unwrap_or(3)).unwrap_or(3);

    println!("Multi-Client Enron Test");
    println!("======================");
    println!("Connecting to Server1 at: {}", s1_addr);
    println!("Connecting to Server2 at: {}", s2_addr);
    println!("Running {} clients", num_clients);

    // Load Enron data
    println!("\nLoading Enron data...");
    let users_csv_path = "sparta-model-evaluation-main/data/enron/users.csv";
    let clean_csv_path = "sparta-model-evaluation-main/data/enron/clean.csv";
    
    // Verify files exist
    if !std::path::Path::new(users_csv_path).exists() {
        return Err(format!("Enron users file not found: {}", users_csv_path).into());
    }
    if !std::path::Path::new(clean_csv_path).exists() {
        return Err(format!("Enron clean file not found: {}", clean_csv_path).into());
    }
    
    let max_users = num_clients.max(10);  // At least 10 users, or more if needed
    let max_contacts_per_user = Q;
    let max_epochs = (WARMUP_COUNT + LATENCY_BENCH_COUNT + 100).max(1000);  // Ensure we have enough epochs
    
    let (enron_users, contacts_map, epoch_map) = enron_data::load_enron_subset(
        users_csv_path,
        clean_csv_path,
        max_users,
        max_contacts_per_user,
        max_epochs
    )?;
    
    println!("Loaded {} Enron users", enron_users.len());
    println!("Loaded {} epoch mappings", epoch_map.len());
    println!("Max epochs configured: {}", max_epochs);
    
    // Analyze epoch distribution
    let mut epoch_counts = std::collections::HashMap::new();
    for (_, &epoch_num) in &epoch_map {
        *epoch_counts.entry(epoch_num).or_insert(0) += 1;
    }
    
    let total_test_epochs = WARMUP_COUNT + LATENCY_BENCH_COUNT;
    let epochs_with_messages = epoch_counts.len();
    let max_epoch_in_data = epoch_counts.keys().max().unwrap_or(&0);
    
    println!("Epoch analysis:");
    println!("  Total test epochs: {}", total_test_epochs);
    println!("  Epochs with Enron messages: {}", epochs_with_messages);
    println!("  Max epoch in data: {}", max_epoch_in_data);
    println!("  Messages per epoch: avg {:.1}", epoch_map.len() as f64 / epochs_with_messages as f64);
    
    // Validate we have enough data
    if *max_epoch_in_data < total_test_epochs as u64 {
        println!("  WARNING: Not enough Enron epochs for full test (need {}, have {})", 
                 total_test_epochs, max_epoch_in_data);
    }
    
    // Helper function to get messages for a specific epoch
    let get_messages_for_epoch = |epoch: u64| -> Vec<(String, String)> {
        epoch_map.iter()
            .filter(|(_, &epoch_num)| epoch_num == epoch)
            .map(|((sender, receiver), _)| (sender.clone(), receiver.clone()))
            .collect()
    };
    
    // Create clients with proper connections
    let mut clients = Vec::new();
    let mut rng = ChaCha20Rng::from_entropy();
    
    // Create a list of running client names for proper communication
    let running_clients: Vec<String> = enron_users[0..num_clients].to_vec();
    
    for i in 0..num_clients {
        let client_name = enron_users[i].clone();
        println!("Setting up client {}: {}", i, client_name);
        
        // Each client gets its own connection
        let s1_channel = build_tls_channel(&s1_addr).await?;
        let s1_access = Box::new(RemoteServer1Access::from_channel(s1_channel));
        let s2_access = Box::new(RemoteServer2Access::new(&s2_addr).await?);
        
        let mut client = Client::new(client_name.clone(), s1_access, s2_access, enron_users.len());
        
        // Create contact list from running clients (not from Enron data)
        let mut contact_list = running_clients.clone();
        contact_list.retain(|name| name != &client_name); // Remove self
        
        // Generate keys for all running clients
        let key_list = (0..contact_list.len()).map(|_| Key::random(&mut rng)).collect::<Vec<_>>();
        
        println!("  Client {} will communicate with {} other clients", i, contact_list.len());
        client.setup(key_list, contact_list.clone())?;
        
        clients.push((client, client_name, contact_list));
    }
    
    println!("\nStarting warm-up phase ({} iterations)...", WARMUP_COUNT);
    
    // Warm-up phase
    for iteration in 0..WARMUP_COUNT {
        let current_epoch = iteration as u64;
        println!("\nWarm-up iteration {}/{} (Epoch {})", iteration + 1, WARMUP_COUNT, current_epoch);
        
        // BEGINNING OF EPOCH: Initialize the batch
        if let Some((client, _, _)) = clients.first() {
            client.s1.batch_init().await?;
        }
        
        // DURING EPOCH: Send messages based on Enron epoch mapping
        let epoch_messages = get_messages_for_epoch(current_epoch);
        println!("  Epoch {} has {} Enron messages", current_epoch, epoch_messages.len());
        
        if !epoch_messages.is_empty() {
            // Send messages based on Enron data
            for (sender, receiver) in epoch_messages {
                // Find the client that matches this sender
                if let Some((client_idx, (client, client_name, _))) = clients.iter_mut().enumerate().find(|(_, (_, name, _))| name == &sender) {
                    // Check if receiver is one of our running clients
                    if running_clients.contains(&receiver) {
                        let mut message = format!("Enron message from {} to {} in epoch {}", 
                                                sender, receiver, current_epoch).into_bytes();
                        message.resize(MESSAGE_SIZE, 0);
                        
                        client.async_write(&message, &receiver).await?;
                        println!("    Client {} ({}) sent message to {}", client_idx, client_name, receiver);
                    }
                }
            }
        } else {
            // No Enron messages for this epoch - send synthetic messages for testing
            for (i, (client, client_name, contact_list)) in clients.iter_mut().enumerate() {
                let mut message = format!("Synthetic warm-up message from {} (client {}) at epoch {}", 
                                        client_name, i, current_epoch).into_bytes();
                message.resize(MESSAGE_SIZE, 0);
                
                // Send to first contact (which is another running client)
                let recipient = if !contact_list.is_empty() {
                    &contact_list[0]
                } else {
                    &clients[(i + 1) % clients.len()].1  // Round-robin to next client
                };
                
                client.async_write(&message, recipient).await?;
            }
        }
        
        // END OF EPOCH: Process all queued messages
        if let Some((client, _, _)) = clients.first() {
            client.s1.batch_write().await?;
        }
        
        // AFTER EPOCH: All clients read messages
        for (i, (client, client_name, _)) in clients.iter_mut().enumerate() {
            let messages = client.async_read(Some(1)).await?;
            if !messages.is_empty() {
                println!("  Client {} ({}) received {} messages", i, client_name, messages.len());
            }
        }
        
        // Small delay between epochs
        sleep(Duration::from_millis(100)).await;
    }
    
    println!("\nStarting measurement phase ({} iterations)...", LATENCY_BENCH_COUNT);
    let mut all_latencies = Vec::new();
    
    // Measurement phase
    for iteration in 0..LATENCY_BENCH_COUNT {
        let current_epoch = (WARMUP_COUNT + iteration) as u64;
        println!("\nMeasurement iteration {}/{} (Epoch {})", iteration + 1, LATENCY_BENCH_COUNT, current_epoch);
        
        let start = std::time::Instant::now();
        
        // BEGINNING OF EPOCH: Initialize the batch
        if let Some((client, _, _)) = clients.first() {
            client.s1.batch_init().await?;
        }
        
        // DURING EPOCH: Send messages based on Enron epoch mapping
        let epoch_messages = get_messages_for_epoch(current_epoch);
        println!("  Epoch {} has {} Enron messages", current_epoch, epoch_messages.len());
        
        if !epoch_messages.is_empty() {
            // Send messages based on Enron data
            for (sender, receiver) in epoch_messages {
                // Find the client that matches this sender
                if let Some((client_idx, (client, client_name, _))) = clients.iter_mut().enumerate().find(|(_, (_, name, _))| name == &sender) {
                    // Check if receiver is one of our running clients
                    if running_clients.contains(&receiver) {
                        let mut message = format!("Enron measurement message from {} to {} in epoch {}", 
                                                sender, receiver, current_epoch).into_bytes();
                        message.resize(MESSAGE_SIZE, 0);
                        
                        client.async_write(&message, &receiver).await?;
                        println!("    Client {} ({}) sent message to {}", client_idx, client_name, receiver);
                    }
                }
            }
        } else {
            // No Enron messages for this epoch - send synthetic messages for testing
            for (i, (client, client_name, contact_list)) in clients.iter_mut().enumerate() {
                let mut message = format!("Synthetic measurement message from {} (client {}) at epoch {}", 
                                        client_name, i, current_epoch).into_bytes();
                message.resize(MESSAGE_SIZE, 0);
                
                // Send to first contact (which is another running client)
                let recipient = if !contact_list.is_empty() {
                    &contact_list[0]
                } else {
                    &clients[(i + 1) % clients.len()].1  // Round-robin to next client
                };
                
                client.async_write(&message, recipient).await?;
            }
        }
        
        // END OF EPOCH: Process all queued messages
        if let Some((client, _, _)) = clients.first() {
            client.s1.batch_write().await?;
        }
        
        // AFTER EPOCH: All clients read messages
        for (i, (client, client_name, _)) in clients.iter_mut().enumerate() {
            let messages = client.async_read(Some(1)).await?;
            if !messages.is_empty() {
                println!("  Client {} ({}) received {} messages", i, client_name, messages.len());
            }
        }
        
        let duration = start.elapsed().as_secs_f64() * 1000.0;
        all_latencies.push(duration);
        
        println!("Total iteration latency: {:.2}ms", duration);
        
        // Small delay between epochs
        sleep(Duration::from_millis(100)).await;
    }
    
    // Calculate statistics
    if !all_latencies.is_empty() {
        let avg_latency = all_latencies.iter().sum::<f64>() / all_latencies.len() as f64;
        let min_latency = all_latencies.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_latency = all_latencies.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        println!("\nMulti-Client Latency Statistics (ms):");
        println!("  Average: {:.2}", avg_latency);
        println!("  Min: {:.2}", min_latency);
        println!("  Max: {:.2}", max_latency);
        println!("  Total iterations: {}", all_latencies.len());
        println!("  Clients: {}", num_clients);
        
        calculate_and_append_averages("multi_client_latency.csv", "multi_client_bytes.csv");
    }
    
    println!("\nMulti-client Enron test completed successfully!");
    Ok(())
}
