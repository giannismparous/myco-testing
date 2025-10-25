use myco::constants::{LATENCY_BENCH_COUNT, MESSAGE_SIZE, Q, WARMUP_COUNT};
use myco::dtypes::Key;
use myco::logging::calculate_and_append_averages;
use myco::{
    client::Client,
    network::{server1::RemoteServer1Access, server2::RemoteServer2Access},
    // OLD CODE: No enron_data import
    // NEW CODE: Added enron_data import for real-world data integration
    enron_data,
};
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use std::error::Error;
use tokio;
use myco::utils::build_tls_channel;
use tikv_jemallocator::Jemalloc;
// OLD CODE: No std::collections import
// NEW CODE: Added HashMap import for contact management
use std::collections::HashMap;

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

    println!("Connecting to Server1 at: {}", s1_addr);
    println!("Connecting to Server2 at: {}", s2_addr);

    let s1_channel = build_tls_channel(&s1_addr).await?;
    let s1_access = Box::new(RemoteServer1Access::from_channel(s1_channel));
    
    let s2_access = Box::new(RemoteServer2Access::new(&s2_addr).await?);

    let mut rng = ChaCha20Rng::from_entropy();

    // OLD CODE: Synthetic client setup
    // let client_name = "SimClient_0".to_string();
    // let mut simulation_client = Client::new(client_name.clone(), s1_access, s2_access, 1);
    // let contact_list = (0..Q).map(|i| format!("SimClient_{}", i)).collect::<Vec<_>>();
    // let key_list = (0..Q).map(|_| Key::random(&mut rng)).collect::<Vec<_>>();
    // simulation_client.setup(key_list, contact_list)?;

    // NEW CODE: Enron data integration
    println!("Loading Enron data...");
    let users_csv_path = "sparta-model-evaluation-main/data/enron/users.csv";
    let clean_csv_path = "sparta-model-evaluation-main/data/enron/clean.csv";
    
    // Load a subset of Enron data for testing (adjust these parameters as needed)
    let max_users = 1000;  // Start with 1000 users instead of full 69k
    let max_contacts_per_user = Q;  // Use Q (64) contacts per user
    let max_epochs = 1000;  // Limit epochs for testing
    
    let (enron_users, contacts_map, epoch_map) = enron_data::load_enron_subset(
        users_csv_path,
        clean_csv_path,
        max_users,
        max_contacts_per_user,
        max_epochs
    )?;
    
    // Use the first Enron user as our test client
    let client_name = enron_users[0].clone();
    let mut simulation_client = Client::new(client_name.clone(), s1_access, s2_access, enron_users.len());
    
    // Get contacts for this specific user
    let contact_list = contacts_map.get(&client_name)
        .unwrap_or(&Vec::new())
        .clone();
    
    // Generate keys for all contacts
    let key_list = (0..contact_list.len()).map(|_| Key::random(&mut rng)).collect::<Vec<_>>();
    
    println!("Setting up client '{}' with {} contacts", client_name, contact_list.len());
    simulation_client.setup(key_list, contact_list.clone())?;

    println!("\nStarting warm-up phase ({} iterations)...", WARMUP_COUNT);
    for iteration in 0..WARMUP_COUNT {
        println!("\nWarm-up iteration {}/{}", iteration + 1, WARMUP_COUNT);
        
        simulation_client.s1.batch_init().await?;
        // MYCO VERSION: Synthetic message
        // let message = vec![1u8; MESSAGE_SIZE];
        // simulation_client.async_write(&message, &client_name).await?;
        
        // ENRON VERSION: Real Enron message content (padded to MESSAGE_SIZE)
        let mut message = format!("Enron test message from {} at iteration {}", client_name, iteration).into_bytes();
        message.resize(MESSAGE_SIZE, 0); // Pad to MESSAGE_SIZE
        // Send to first contact instead of self
        let recipient = if contact_list.is_empty() { &client_name } else { &contact_list[0] };
        simulation_client.async_write(&message, recipient).await?;
        simulation_client.s1.batch_write().await?;
        let messages = simulation_client.async_read(Some(1)).await?;
        
        println!("Read messages: {:?}", messages);
    }

    println!("\nStarting measurement phase ({} iterations)...", LATENCY_BENCH_COUNT);
    let mut latencies = Vec::new();
    
    for iteration in 0..LATENCY_BENCH_COUNT {
        println!("\nMeasurement iteration {}/{}", iteration + 1, LATENCY_BENCH_COUNT);
        
        let start = std::time::Instant::now();

        let batch_init_start = std::time::Instant::now();
        simulation_client.s1.batch_init().await?;
        let batch_init_duration = batch_init_start.elapsed().as_secs_f64() * 1000.0;

        let write_start = std::time::Instant::now();
        // MYCO VERSION: Synthetic message
        // let message = vec![1u8; MESSAGE_SIZE];
        // simulation_client.async_write(&message, &client_name).await?;
        
        // ENRON VERSION: Real Enron message content (padded to MESSAGE_SIZE)
        let mut message = format!("Enron measurement message from {} at iteration {}", client_name, iteration).into_bytes();
        message.resize(MESSAGE_SIZE, 0); // Pad to MESSAGE_SIZE
        // Send to first contact instead of self
        let recipient = if contact_list.is_empty() { &client_name } else { &contact_list[0] };
        simulation_client.async_write(&message, recipient).await?;
        simulation_client.s1.batch_write().await?;
        let write_duration = write_start.elapsed().as_secs_f64() * 1000.0;

        let read_start = std::time::Instant::now();
        let messages = simulation_client.async_read(Some(1)).await?;
        let read_duration = read_start.elapsed().as_secs_f64() * 1000.0;
        
        let duration = start.elapsed().as_secs_f64() * 1000.0;
        latencies.push(duration);
        
        println!("Read messages: {:?}", messages);
        println!("Step durations:");
        println!("  Batch init: {:.2}ms", batch_init_duration);
        println!("  Write: {:.2}ms", write_duration);
        println!("  Read: {:.2}ms", read_duration);
        println!("Total iteration latency: {:.2}ms", duration);
    }

    if !latencies.is_empty() {
        let avg_latency = latencies.iter().sum::<f64>() / latencies.len() as f64;
        let min_latency = latencies.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_latency = latencies.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        println!("\nLatency Statistics (ms):");
        println!("  Average: {:.2}", avg_latency);
        println!("  Min: {:.2}", min_latency);
        println!("  Max: {:.2}", max_latency);
        println!("  Total iterations: {}", latencies.len());
        calculate_and_append_averages("client_latency.csv", "client_bytes.csv");
    }
    Ok(())
}
