use myco::constants::{D, NUM_CLIENTS, Z, DELTA};
use myco::dtypes::Path;
use myco::tree::{BinaryTree, SparseBinaryTree, TreeValue};
use myco::utils::get_path_indices;
use serde::{Serialize, Deserialize};
use rayon::prelude::*;
use rand_chacha::ChaCha20Rng;
use std::fs::File;
use std::io::Write;

use tikv_jemallocator::Jemalloc;
    
#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;

/// A lightweight simulation bucket. Instead of storing blocks (as in the real Bucket),
/// this bucket simply collects the intended Path values.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
struct SimBucket(Vec<(Path, usize)>);

impl SimBucket {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl TreeValue for SimBucket {
    fn new_random(_rng: &mut ChaCha20Rng) -> Self {
        SimBucket(Vec::new())
    }
}

fn main() {
    // Create output file
    let output_file = "simulation_results.csv";
    let mut file = File::create(output_file).expect("Failed to create output file");
    
    // Write CSV header
    writeln!(
        file,
        "Epoch,PathSetMax,PathSetMaxDepth,TreeMax,TreeMaxDepth,HistoryMax,HistoryMaxDepth"
    ).expect("Failed to write header");

    // Create a full binary tree of depth D.
    let mut tree: BinaryTree<SimBucket> = BinaryTree::new_with_depth(D);
    tree.fill(SimBucket::default());

    // Variables to track maximum bucket sizes across epochs
    let mut tree_max_bucket_size = 0;
    let mut tree_max_bucket_depth = 0;
    let mut tree_max_bucket_idx = 0;
    
    let mut history_max_bucket_size = 0;
    let mut history_max_bucket_depth = 0;

    let mut epoch = 0;
    loop {
        // --------------------------
        // Step 1: Generate a pathset (random paths) and get node indices.
        let random_paths: Vec<Path> = (0..NUM_CLIENTS)
            .into_par_iter()
            .map(|_| {
                let mut local_rng = rand::thread_rng();
                Path::random(&mut local_rng)
            })
            .collect();
        let path_indices = get_path_indices(random_paths);

        // --------------------------
        // Step 2: Create ephemeral sparse tree p (using buckets from the persistent tree).
        let buckets: Vec<SimBucket> = path_indices
            .par_iter()
            .map(|&i| tree.value[i].clone().unwrap())
            .collect();
        let p = SparseBinaryTree::new_with_data(buckets, path_indices.clone());

        // --------------------------
        // Step 3: Create an empty ephemeral sparse tree pt with the same structure.
        let mut pt = SparseBinaryTree::new_with_data(
            vec![SimBucket::default(); path_indices.len()],
            path_indices.clone(),
        );

        // --------------------------
        // Step 4: Compute transferred (old) items.
        let transferred_items: Vec<(usize, (Path, usize))> = p.packed_indices
            .par_iter()
            .enumerate()
            .flat_map_iter(|(i, &_idx)| {
                p.packed_buckets[i]
                    .0
                    .iter()
                    .filter_map(|item| {
                        let (ref msg_path, t_exp) = item;
                        // Compute new LCA index for the message.
                        let (new_lca_idx, _) = p.lca_idx(msg_path).unwrap();
                        // Only transfer the item if not expired.
                        if epoch < *t_exp {
                            Some((new_lca_idx, (msg_path.clone(), *t_exp)))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .collect();

        // --------------------------
        // Step 5: Process new writes.
        let new_writes: Vec<(usize, (Path, usize))> = (0..NUM_CLIENTS)
            .into_par_iter()
            .map(|_| {
                let mut local_rng = rand::thread_rng();
                let intended = Path::random(&mut local_rng);
                let t_exp = epoch + DELTA;
                let (lca_idx, _) = p.lca_idx(&intended).unwrap();
                (lca_idx, (intended, t_exp))
            })
            .collect();

        // --------------------------
        // Combine transferred items and new writes, and perform a single insertion loop.
        let combined_writes: Vec<(usize, (Path, usize))> = transferred_items
            .into_iter()
            .chain(new_writes.into_iter())
            .collect();
        for (lca_idx, msg) in combined_writes {
            if let Some(bucket) = pt.get_by_index_mut(lca_idx) {
                bucket.0.push(msg);
            }
        }

        // --------------------------
        // Step 6: Update the persistent binary tree using the ephemeral tree pt.
        tree.overwrite_from_sparse(&pt);
        
        // --------------------------
        // Step 7: Compute statistics (max bucket size).
        // 1. Current max bucket size in path-set (epoch_max, epoch_max_depth)
        let mut epoch_max = 0;
        let mut epoch_max_depth = 0;
        let mut epoch_max_idx = 0;
        
        for (i, bucket) in pt.packed_buckets.iter().enumerate() {
            let size = bucket.len();
            if size > epoch_max {
                epoch_max = size;
                // Calculate depth from the node index in the tree
                epoch_max_idx = pt.packed_indices[i];
                epoch_max_depth = tree.depth_of_node(epoch_max_idx);
            }
        }
        
        // 2. Current max bucket size in overall tree - update only when necessary
        // Check if the current max bucket is in the path-set (and might have changed)
        let max_bucket_in_pathset = pt.packed_indices.contains(&tree_max_bucket_idx);
        
        if max_bucket_in_pathset {
            // The max bucket was updated in this epoch, get its new size
            let new_size = tree.value[tree_max_bucket_idx].as_ref().map_or(0, |b| b.len());
            tree_max_bucket_size = new_size;
        }
        
        // If the epoch max exceeds the tree max, update the tree max
        if epoch_max > tree_max_bucket_size {
            tree_max_bucket_size = epoch_max;
            tree_max_bucket_depth = epoch_max_depth;
            tree_max_bucket_idx = epoch_max_idx;
        }
        
        // 3. Overall max bucket size throughout history
        if tree_max_bucket_size > history_max_bucket_size {
            history_max_bucket_size = tree_max_bucket_size;
            history_max_bucket_depth = tree_max_bucket_depth;
        }

        if epoch_max > Z {
            eprintln!(
                "Overflow detected at epoch {}: bucket size {} at depth {} exceeds threshold {}. Aborting simulation.",
                epoch, epoch_max, epoch_max_depth, Z
            );
            std::process::exit(1);
        }
        
        println!(
            "Epoch {}: Path-set max: {} at depth {}, Tree max: {} at depth {}, History max: {} at depth {}",
            epoch, 
            epoch_max, epoch_max_depth, 
            tree_max_bucket_size, tree_max_bucket_depth, 
            history_max_bucket_size, history_max_bucket_depth
        );

        // Add CSV logging before the overflow check
        writeln!(
            file,
            "{},{},{},{},{},{},{}",
            epoch,
            epoch_max, epoch_max_depth,
            tree_max_bucket_size, tree_max_bucket_depth,
            history_max_bucket_size, history_max_bucket_depth
        ).expect("Failed to write to CSV");
        

        epoch += 1;
    }
}
