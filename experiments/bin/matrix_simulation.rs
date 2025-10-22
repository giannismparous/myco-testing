use rand::Rng;
use std::fs::File;
use std::io::Write;
use std::time::Instant;
use myco::constants::{Q, NUM_CLIENTS};

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;

fn main() -> std::io::Result<()> {
    // Parameters
    const TOTAL_ITEMS: usize = Q * NUM_CLIENTS;
    const TOTAL_BUCKETS: usize = Q * NUM_CLIENTS;
    
    let output_file = "matrix_simulation_results.csv";
    let mut file = File::create(output_file)?;
    
    println!("Starting simulation with Q={}, NUM_CLIENTS={}", Q, NUM_CLIENTS);
    println!("Total items: {}, Total buckets: {}", TOTAL_ITEMS, TOTAL_BUCKETS);
    
    writeln!(file, "Epoch,CurrentMax,HistoryMax")?;
    
    let mut epoch = 0;
    let mut max_saturation_so_far = 0;
    let mut rng = rand::thread_rng();
    
    loop {
        let start_time = Instant::now();
        let mut buckets = vec![0; TOTAL_BUCKETS];
        
        for _ in 0..TOTAL_ITEMS {
            let bucket_idx = rng.gen_range(0..TOTAL_BUCKETS);
            buckets[bucket_idx] += 1;
        }
        
        let max_saturation = *buckets.iter().max().unwrap_or(&0);
        
        if max_saturation > max_saturation_so_far {
            max_saturation_so_far = max_saturation;
        }
        
        writeln!(
            file,
            "{},{},{}",
            epoch,
            max_saturation,
            max_saturation_so_far
        )?;
        
        let elapsed = start_time.elapsed();
        println!(
            "Epoch {}: Max bucket saturation = {} items, Max so far = {} items, Time: {:.2?}",
            epoch, max_saturation, max_saturation_so_far, elapsed
        );
        
        epoch += 1;
    }
}
