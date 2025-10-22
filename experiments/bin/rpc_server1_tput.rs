use myco::constants::NUM_CLIENTS;
use myco::constants::S1_S2_CONNECTION_COUNT;
use myco::constants::WARMUP_COUNT;
use myco::logging;
use myco::megaclient::MegaClient;
use myco::network::server2::Server2Access;
use myco::utils::generate_test_certificates;
use myco::{
    constants::THROUGHPUT_ITERATIONS,
    network::server2::MultiConnectionServer2Access,
    server1::Server1,
};
use tracing::info;
use std::env;
use std::{
    sync::Arc,
    time::Instant,
};
use tokio::sync::{Mutex, RwLock};

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;

#[derive(Clone)]
struct Server1TputState {
    server1: Arc<RwLock<Server1>>,
    mega_client: Arc<Mutex<MegaClient>>,
    message_count: Arc<Mutex<usize>>,
}

async fn run_iteration(state: &Server1TputState, iteration_num: usize, total_iterations: usize) -> Result<f64, Box<dyn std::error::Error>> {
    let iteration_start = Instant::now();
    println!(
        "\nIteration {}/{}",
        iteration_num + 1,
        total_iterations
    );

    println!("Current iteration: {}, Total iterations: {}, Warmup count: {}", 
             iteration_num, total_iterations, WARMUP_COUNT);

    println!("===== Pre-generation Phase =====");
    
    println!("Requesting Server2 to pre-generate test data...");
    let server2_pregen_start = std::time::Instant::now();
    {
        let server1 = state.server1.read().await;
        let result = server1.s2.pre_generate_test_data().await;
        match result {
            Ok(success) => {
                println!("Server2 pre-generation completed successfully: {}", success);
            },
            Err(e) => {
                println!("Failed to pre-generate Server2 data: {}", e);
                return Err(e.into());
            }
        }
    }
    println!("Server2 data pre-generation took: {:?}", server2_pregen_start.elapsed());
    
    println!("Pre-generating Server1 test data...");
    let pre_gen_start = std::time::Instant::now();
    
    let notification_indices_flat: Vec<Vec<u8>> = state.mega_client.lock().await.generate_read_notifs()
        .into_iter()
        .flatten()
        .collect();
    
    let read_indices_flat: Vec<Vec<u8>> = state.mega_client.lock().await.generate_reads()
        .into_iter()
        .collect();
    
    let pre_gen_elapsed = pre_gen_start.elapsed();
    println!("Server1 test data generation completed in {:?}", pre_gen_elapsed);
    
    println!("===== End of Pre-generation Phase =====\n");

    println!("Starting timed throughput measurement...");
    let throughput_start = std::time::Instant::now();

    println!("Batch init about to start");
    let start_time = std::time::Instant::now();
    state.server1.write().await.batch_init()?;
    println!("Batch init took: {:?}", start_time.elapsed());

    println!("Requesting write data from Server2...");
    let _ = std::time::Instant::now();
    let (cts, ct_ntfs, fs, f_ntfs, k_renc_ts, c_ss) = {
        let server1 = state.server1.read().await;
        match server1.s2.get_mega_client_writes().await {
            Ok((cts, ct_ntfs, fs, f_ntfs, k_renc_ts, c_ss)) => {
                (cts, ct_ntfs, fs, f_ntfs, k_renc_ts, c_ss)
            },
            Err(e) => {
                println!("Failed to get write data from Server2: {}", e);
                return Err(e.into());
            }
        }
    };

    let local_queue_write_start = std::time::Instant::now();
    println!("cts.len(): {}", cts.len());
    state.server1.write().await.megaclient_queue_write(cts, ct_ntfs, fs, f_ntfs, k_renc_ts, c_ss)?;
    let local_queue_write_elapsed = local_queue_write_start.elapsed();
    println!("Local megaclient queue write took: {:?}", local_queue_write_elapsed);


    println!("Local batch write computation starting...");
    let batch_write_start = std::time::Instant::now();
    state.server1.write().await.async_batch_write().await?;
    let batch_write_elapsed = batch_write_start.elapsed();
    println!("Local batch write took: {:?}", batch_write_elapsed);

    let prf_keys_start_time = std::time::Instant::now();
    let server1 = state.server1.read().await;
    let result = server1.s2.get_prf_keys().await;
    match result {
        Ok(_keys) => {
            let elapsed = prf_keys_start_time.elapsed();
            println!("PRF keys collection completed in {:?}", elapsed);
        }
        Err(e) => {
            println!("Failed to get PRF keys: {}", e);
            return Err(e.into());
        }
    }
    let result = state.server1.read().await.s2.process_notification_indices(notification_indices_flat).await;
    if let Err(e) = result {
        return Err(e.into());
    }
    
    let result = {
        let server1 = state.server1.read().await;
        match server1.s2.process_read_indices(read_indices_flat).await {
            Ok(_) => {
                if iteration_num >= WARMUP_COUNT {
                    let mut message_count = state.message_count.lock().await;
                    *message_count += NUM_CLIENTS;
                }
                
                Ok(())
            },
            Err(e) => {
                println!("Failed to process read indices: {}", e);
                Err(e)
            }
        }
    };

    if let Err(e) = result {
        return Err(e.into());
    }
    
    let throughput_elapsed = throughput_start.elapsed();
    
    let throughput_seconds = throughput_elapsed.as_secs_f64();
    let iteration_throughput = NUM_CLIENTS as f64 / throughput_seconds;
    
    println!("\nIteration {} Throughput:", iteration_num + 1);
    println!("Messages: {}", NUM_CLIENTS);
    println!("Time: {:.2} seconds", throughput_seconds);
    println!("Throughput: {:.2} messages/second", iteration_throughput);
    
    let iteration_elapsed = iteration_start.elapsed();
    println!("Total iteration time (including data generation): {:.2} seconds", iteration_elapsed.as_secs_f64());

    if iteration_num >= WARMUP_COUNT {
        println!("Past warmup phase, iteration {} of {}", iteration_num + 1, total_iterations);
        if iteration_num == total_iterations - 1 {
            println!("Server1 Tput: Logging final averages to CSV files...");
            logging::calculate_and_append_averages("server1_tput_latency.csv", "server1_tput_bytes.csv");
            println!("Server1 Tput: Logging complete");
        }
    }

    Ok(iteration_throughput)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    use tracing::info;
    use tracing_subscriber;

    env::set_var("RUST_LOG", "info");
    
    tracing_subscriber::fmt::init();
    info!("Tracer initialized");

    let args: Vec<String> = std::env::args().collect();
    
    let s2_addr = args.get(1)
        .map(|addr| {
            if !addr.starts_with("http://") && !addr.starts_with("https://") { 
                format!("https://{}", addr) 
            } else { 
                addr.clone() 
            }
        })
        .unwrap_or("https://127.0.0.1:3004".to_string());

    println!("Connecting to Server2 at: {}", s2_addr);

    if tokio::fs::metadata("certs/server-cert.pem").await.is_err() 
        || tokio::fs::metadata("certs/server-key.pem").await.is_err() 
    {
        println!("Certificate or key not found in 'certs/' folder. Generating test certificates...");
        generate_test_certificates().map_err(|e| format!("Failed generating test certificates: {}", e))?;
    }

    println!("Attempting to connect to Server2 at: {}", s2_addr);
    
    println!("Establishing {} secure connections to {}...", S1_S2_CONNECTION_COUNT, s2_addr);
    let s2_access = MultiConnectionServer2Access::new(&s2_addr).await
        .map(|access| Box::new(access) as Box<dyn Server2Access>)
        .map_err(|e| {
            eprintln!("Failed to connect to Server2: {:?}", e);
            e
        })?;

    let server1 = Server1::new(s2_access, NUM_CLIENTS);
    let server1 = Arc::new(RwLock::new(server1));

    let mega_client = MegaClient::new();

    let state = Server1TputState {
        server1,
        mega_client: Arc::new(Mutex::new(mega_client)),
        message_count: Arc::new(Mutex::new(0)),
    };
    
    let total_iterations = WARMUP_COUNT + THROUGHPUT_ITERATIONS;
    println!("Starting test with {} warmup iterations and {} measurement iterations (total: {})", 
             WARMUP_COUNT, THROUGHPUT_ITERATIONS, total_iterations);

    for iteration in 0..WARMUP_COUNT {
        let _ = run_iteration(&state, iteration, total_iterations).await?;
    }

    let mut iteration_throughputs = Vec::with_capacity(THROUGHPUT_ITERATIONS);
    for iteration in WARMUP_COUNT..total_iterations {
        let throughput = run_iteration(&state, iteration, total_iterations).await?;
        iteration_throughputs.push(throughput);
    }

    let avg_throughput = if !iteration_throughputs.is_empty() {
        iteration_throughputs.iter().sum::<f64>() / iteration_throughputs.len() as f64
    } else {
        0.0
    };

    println!("\nOverall Throughput Results:");
    println!("Number of measurement iterations: {}", THROUGHPUT_ITERATIONS);
    println!("Average throughput: {:.2} messages/second", avg_throughput);
    
    if iteration_throughputs.len() <= 10 {
        println!("Individual iteration throughputs: {:?}", iteration_throughputs);
    } else {
        println!("First 5 iteration throughputs: {:?}", &iteration_throughputs[..5]);
        println!("Last 5 iteration throughputs: {:?}", &iteration_throughputs[iteration_throughputs.len()-5..]);
    }

    Ok(())
}