//! This module provides binary tree implementations for both dense and sparse trees.
//! 
//! The main types are:
//! - `BinaryTree<T>`: A dense binary tree implementation that stores values of type T
//! - `SparseBinaryTree<T>`: A sparse binary tree that only stores non-empty nodes
//! - `TreeValue`: A trait for values that can be stored in binary trees

use std::{
    cmp::max,
    fmt::{self, Debug},
    collections::HashMap,
};

use crate::dtypes::{Bucket, Direction, Path};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};


/// A binary tree implementation that stores values of type T.
/// 
/// The tree is stored as a vector where:
/// - Index 0 is unused
/// - Index 1 is the root node
/// - For any node at index i:
///   - Left child is at index 2i
///   - Right child is at index 2i + 1
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinaryTree<T> {
    /// Vector storing the tree nodes, with None representing empty nodes
    pub value: Vec<Option<T>>,
}

/// A trait for values that can be stored in binary trees.
pub trait TreeValue: Clone + Debug + PartialEq + Default {
    /// Creates a new random value of this type
    fn new_random(rng: &mut ChaCha20Rng) -> Self;
}

impl<T: TreeValue> BinaryTree<T> {
    /// Creates a new binary tree with a single value at the root
    pub fn new(value: T) -> Self {
        BinaryTree {
            value: vec![None, Some(value)],
        }
    }

    /// Creates a new empty binary tree with space for root and one level
    pub fn new_empty() -> Self {
        BinaryTree {
            value: vec![None; 2],
        }
    }

    /// Creates a new empty binary tree with space for the specified depth
    pub fn new_with_depth(depth: usize) -> Self {
        BinaryTree {
            // Prev 1 << depth + 1
            // For a binary tree of depth d, the number of nodes is 2^(d+1) - 1. However, we ignore the zero index, and instead represent the root node as index 1.
            // This is for simpler calculation.
            value: vec![None; 1 << (depth + 1)],
        }
    }

    /// Fills all nodes in the tree with the given value
    pub fn fill(&mut self, value: T) {
        self.value[1..].fill(Some(value));
    }

    /// Inserts values along a path in the tree
    pub fn insert_path(&mut self, path: Path, values: Vec<T>) {
        let mut idx: usize = 1;
        self.value[1] = Some(values[0].clone());

        for (direction, value) in path.zip(&values[1..]) {
            idx = 2 * idx + u8::from(direction) as usize;
            if idx + 1 >= self.value.len() {
                self.value.resize((idx + 1).next_power_of_two(), None);
            }
            self.value[idx] = Some(value.clone());
        }
    }

    /// Creates a tree from a vector of (values, path) pairs
    pub fn from_vec_with_paths(items: Vec<(Vec<T>, Path)>) -> Self
    where
        T: TreeValue,
    {
        let mut tree = BinaryTree::new_empty();
        for (values, path) in items {
            tree.insert_path(path, values);
        }
        tree
    }

    /// Returns the height of the tree
    pub fn height(&self) -> usize {
        ((self.value.len() as f64).log2().ceil() as usize) - 1
    }

    /// Creates a tree from arrays of values and their indices
    pub fn from_array(values: Vec<T>, indices: Vec<usize>) -> Self {
        let mut tree = BinaryTree::new_empty();
        for (value, index) in values.iter().zip(indices) {
            if index >= tree.value.len() {
                tree.value.resize((index + 1).next_power_of_two(), None);
            }
            tree.value[index] = Some(value.clone());
        }
        tree
    }

    /// Gets the value at a given path
    pub fn get(&self, path: &Path) -> Option<T> {
        let mut current = self.value[1].clone();
        let mut idx = 1;
        for &direction in path {
            idx = 2 * idx + u8::from(direction) as usize;
            if idx >= self.value.len() {
                return None;
            }
            current = self.value[idx].clone();
        }
        current
    }

    /// Gets the index for a given path
    pub fn get_index(&self, path: &Path) -> usize {
        let mut idx = 1;
        for &direction in path {
            idx = 2 * idx + u8::from(direction) as usize;
            if idx >= self.value.len() {
                return 1;
            }
        }
        idx
    }

    /// Gets all nodes along a given path
    pub fn get_all_nodes_along_path(&self, path: &Path) -> Vec<T> {
        let mut nodes = vec![];
        let mut idx = 1;

        // Include the root node if it has a value
        if let Some(value) = &self.value[1] {
            nodes.push(value.clone());
        }

        for &direction in path {
            idx = 2 * idx + u8::from(direction) as usize;

            if idx >= self.value.len() || self.value[idx].is_none() {
                return nodes;
            }
            nodes.push(self.value[idx].clone().unwrap());
        }

        nodes
    }

    /// Finds the lowest common ancestor for a given path
    pub fn lca(&mut self, path: &Path) -> Option<(&mut T, Path)> {
        let mut current_path = Path::new(Vec::new());
        let mut idx = 1;

        for &direction in path {
            let next_idx = 2 * idx + u8::from(direction) as usize;
            if next_idx >= self.value.len() || self.value[next_idx].is_none() {
                return self.value[idx].as_mut().map(|value| (value, current_path));
            }
            idx = next_idx;
            current_path.push(direction);
        }

        self.value[idx].as_mut().map(|value| (value, current_path))
    }

    /// Writes a value at a given path
    pub fn write(&mut self, value: T, path: Path) {
        let mut idx = 1;
        for direction in path {
            idx = 2 * idx + u8::from(direction) as usize;
            if idx >= self.value.len() {
                self.value.resize((idx + 1).next_power_of_two(), None);
            }
        }

        self.value[idx] = Some(value);
    }

    /// Overwrites this tree with values from another tree
    pub fn overwrite(&mut self, other: &BinaryTree<T>) {
        if self.value.len() < other.value.len() {
            self.value.resize(other.value.len(), None);
        }
        self.value
            .iter_mut()
            .zip(other.value.iter())
            .for_each(|(a, b)| {
                if b.is_some() {
                    *a = b.clone();
                }
            });
    }

    /// Overwrites this tree with values from a sparse tree
    pub fn overwrite_from_sparse(&mut self, sparse_tree: &SparseBinaryTree<T>) 
    where
        T: Send + Sync,
    {
        // Ensure capacity
        if let Some(&max_index) = sparse_tree.packed_indices.iter().max() {
            if max_index >= self.value.len() {
                self.value.resize(max_index + 1, None);
            }
        }

        // Iterate over the sparse tree and copy its values into the binary tree at the corresponding indices
        for (bucket, &index) in sparse_tree
            .packed_buckets
            .iter()
            .zip(&sparse_tree.packed_indices)
        {
            self.value[index] = Some(bucket.clone());
        }
    }

    /// Zips this tree with another tree, returning tuples of values and paths
    pub fn zip<S: Clone>(&self, rhs: &BinaryTree<S>) -> Vec<(Option<T>, Option<S>, Path)> {
        let len = max(self.value.len(), rhs.value.len());
        let mut lhs = self.value.clone();
        let mut rhs = rhs.value.clone();

        lhs.resize(len, None);
        rhs.resize(len, None);

        lhs.iter()
            .zip(rhs.iter())
            .enumerate()
            .filter_map(|(i, (a, b))| {
                if a.is_some() {
                    Some((a.clone(), b.clone(), Path::from(i)))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Calculate the depth of a node in the tree based on its index
    pub fn depth_of_node(&self, index: usize) -> usize {
        if index == 0 {
            return 0; // Root node
        }
        // In a binary tree, the depth is the number of bits needed to represent the index
        // minus 1 (because the root is at index 0, not 1)
        (usize::BITS - (index + 1).leading_zeros() - 1) as usize
    }
}
impl fmt::Display for BinaryTree<Bucket> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Binary Tree:")?;
        write!(f, "[")?;
        for (i, bucket) in self.value.iter().enumerate() {
            let bucket_len = bucket.as_ref().map_or(0, |b| b.len());
            write!(f, "{}", bucket_len)?;
            if i < self.value.len() - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}

// Sparse binary tree

/// A sparse binary tree implementation that only stores non-empty nodes.
/// 
/// Instead of storing all nodes in a vector like BinaryTree, this implementation
/// only stores the non-empty nodes in a compressed format using:
/// - packed_buckets: Vector of actual values
/// - packed_indices: Vector of indices where those values belong
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SparseBinaryTree<T> {
    /// List of non-None buckets
    pub packed_buckets: Vec<T>,
    /// Indices corresponding to these buckets
    pub packed_indices: Vec<usize>,
    /// HashMap for fast index lookups (index -> position in packed_buckets)
    #[serde(skip)]
    index_map: HashMap<usize, usize>,
}

impl<T> SparseBinaryTree<T>
where
    T: Clone + Debug + PartialEq + Default + Send + Sync,
{
    /// Creates a new empty sparse binary tree
    pub fn new() -> Self {
        SparseBinaryTree {
            packed_buckets: Vec::new(),
            packed_indices: Vec::new(),
            index_map: HashMap::new(),
        }
    }

    /// Creates a new sparse binary tree with provided packed_buckets and packed_indices
    pub fn new_with_data(packed_buckets: Vec<T>, packed_indices: Vec<usize>) -> Self {
        let mut index_map = HashMap::with_capacity(packed_indices.len());
        for (pos, &idx) in packed_indices.iter().enumerate() {
            index_map.insert(idx, pos);
        }
        
        SparseBinaryTree {
            packed_buckets,
            packed_indices,
            index_map,
        }
    }

    // Helper function to add a bucket and its index
    fn add_bucket(&mut self, index: usize, value: T) {
        // Check if the index already exists
        if let Some(&pos) = self.index_map.get(&index) {
            // If it exists, replace the corresponding value in packed_buckets
            self.packed_buckets[pos] = value;
        } else {
            // Otherwise, add the new value and index
            let pos = self.packed_buckets.len();
            self.packed_buckets.push(value);
            self.packed_indices.push(index);
            self.index_map.insert(index, pos);
        }
    }

    /// Retrieve the value at the given path in a sparse binary tree
    pub fn get(&self, path: &Path) -> Option<&T> {
        let mut idx = 1; // Start at the root
        for &direction in path {
            idx = 2 * idx + u8::from(direction) as usize;
        }
        self.get_by_index(idx)
    }

    /// Get the index for a given path
    pub fn get_index(&self, path: &Path) -> usize {
        let mut idx = 1; // Start at the root
        for &direction in path {
            idx = 2 * idx + u8::from(direction) as usize;
        }
        idx
    }

    /// Retrieves value by index - now O(1) instead of O(n)
    pub fn get_by_index(&self, index: usize) -> Option<&T> {
        self.index_map.get(&index).map(|&pos| &self.packed_buckets[pos])
    }

    /// Retrieves a mutable reference to the bucket by index - also O(1) now
    pub fn get_by_index_mut(&mut self, index: usize) -> Option<&mut T> {
        self.index_map.get(&index).copied().map(move |pos| &mut self.packed_buckets[pos])
    }

    /// Write a value into the sparse tree at the specified path
    pub fn write(&mut self, value: T, path: Path) {
        let mut idx = 1; // Start at the root

        for direction in path {
            idx = 2 * idx + u8::from(direction) as usize;
        }

        self.add_bucket(idx, value);
    }

    /// Find the lowest common ancestor (LCA) index of a given path
    pub fn lca_idx(&self, path: &Path) -> Option<(usize, Path)> {
        let mut current_path = Path::new(Vec::new());
        let mut idx = 1; // Start at the root

        for &direction in path {
            let next_idx = 2 * idx + u8::from(direction) as usize;
            if self.get_by_index(next_idx).is_none() {
                return Some((idx, current_path.clone()));
            }
            idx = next_idx;
            current_path.push(direction);
        }
        Some((idx, current_path.clone()))
    }

    /// Find the lowest common ancestor (LCA) of a given path and return a mutable reference to its value
    /// along with the path to reach it.
    ///
    /// # Arguments
    /// * `path` - The path to find the LCA for
    ///
    /// # Returns
    /// * `Option<(&mut T, Path)>` - A tuple containing:
    ///   - A mutable reference to the LCA node's value
    ///   - The path from root to the LCA node
    pub fn lca(&mut self, path: &Path) -> Option<(&mut T, Path)> {
        // Initialize empty path starting from root
        let mut current_path = Path::new(Vec::new());
        let mut idx = 1; // Start at the root

        // Traverse down the tree following the given path
        for &direction in path {
            let next_idx = 2 * idx + u8::from(direction) as usize;
            // If next node doesn't exist, current node is the LCA
            if self.get_by_index(next_idx).is_none() {
                return self
                    .get_by_index_mut(idx)
                    .map(|value| (value, current_path.clone()));
            }
            // Continue down the path
            idx = next_idx;
            current_path.push(direction);
        }

        // If we reach the end of the path, that node is the LCA
        self.get_by_index_mut(idx)
            .map(|value| (value, current_path.clone()))
    }

    /// Zips two sparse binary trees together
    pub fn zip<S: Clone>(&self, rhs: &SparseBinaryTree<S>) -> Vec<(Option<T>, Option<S>, Path)> {
        let mut results = Vec::new();

        let max_len = self.packed_indices.len().max(rhs.packed_indices.len());

        for i in 0..max_len {
            let lhs_bucket = self.packed_buckets.get(i).cloned();
            let rhs_bucket = rhs.packed_buckets.get(i).cloned();

            if let Some(lhs_index) = self.packed_indices.get(i) {
                let path = Path::from(*lhs_index);
                results.push((lhs_bucket, rhs_bucket, path));
            }
        }

        results
    }

    /// Returns an iterator that zips two sparse binary trees together with mutable references
    pub fn zip_mut<'a, S>(
        &'a mut self,
        rhs: &'a mut SparseBinaryTree<S>,
    ) -> ZipMutIterator<'a, T, S> {
        ZipMutIterator::new(self, rhs)
    }

    /// Zips this sparse tree with a regular binary tree
    pub fn zip_with_binary_tree<S: Clone>(
        &self,
        rhs: &BinaryTree<S>,
    ) -> Vec<(Option<T>, Option<S>, Path)> {
        let mut results = Vec::new();

        // Iterate only over the indices in the sparse binary tree
        for (i, &index) in self.packed_indices.iter().enumerate() {
            let lhs_bucket = self.packed_buckets.get(i).cloned(); // Get the value from the sparse tree

            // Get the corresponding value from the normal binary tree
            let rhs_bucket = if index < rhs.value.len() {
                rhs.value[index].clone()
            } else {
                None
            };

            // Create a Path from the current index
            let path = Path::from(index);

            // Add the zipped result to the results vector
            results.push((lhs_bucket, rhs_bucket, path));
        }

        results
    }

    /// Gets all nodes along a given path
    pub fn get_all_nodes_along_path(&self, path: &Path) -> Vec<&T> {
        let mut nodes = Vec::new();
        let mut idx = 1;  // Start at root

        // Check root
        if let Some(value) = self.get_by_index(idx) {
            nodes.push(value);
        }

        // Check each node along the path
        for &direction in path {
            idx = 2 * idx + u8::from(direction) as usize;
            if let Some(value) = self.get_by_index(idx) {
                nodes.push(value);
            }
        }

        nodes
    }
}

/// Helper struct to track which parts of the slices we're working with
struct SlicePair<'a, T, S> {
    left_indices: &'a [usize],
    right_indices: &'a [usize],
    left_buckets: &'a mut [T],
    right_buckets: &'a mut [S],
}

/// Iterator for zipping two sparse binary trees together with mutable references
pub struct ZipMutIterator<'a, T, S> {
    slices: Option<SlicePair<'a, T, S>>,
}

impl<'a, T, S> ZipMutIterator<'a, T, S> {
    /// Creates a new ZipMutIterator from two sparse binary trees
    /// 
    /// # Arguments
    /// * `left_tree` - First sparse binary tree to zip
    /// * `right_tree` - Second sparse binary tree to zip
    ///
    /// # Panics
    /// Panics if the trees have different numbers of elements
    fn new(left_tree: &'a mut SparseBinaryTree<T>, right_tree: &'a mut SparseBinaryTree<S>) -> Self {
        if left_tree.packed_indices.len() != right_tree.packed_indices.len() {
            panic!("Trees must have the same number of elements to zip.");
        }

        // Create slice pairs to track current position in both trees
        let slices = Some(SlicePair {
            left_indices: &left_tree.packed_indices[..],
            right_indices: &right_tree.packed_indices[..],
            left_buckets: &mut left_tree.packed_buckets[..],
            right_buckets: &mut right_tree.packed_buckets[..],
        });

        Self { slices }
    }
}

impl<'a, T, S> Iterator for ZipMutIterator<'a, T, S> {
    /// Each iteration returns a tuple containing:
    /// - Optional mutable reference to left tree bucket
    /// - Optional mutable reference to right tree bucket  
    /// - Path representing the current position
    type Item = (Box<Option<&'a mut T>>, Box<Option<&'a mut S>>, Path);

    /// Advances the iterator and returns the next pair of corresponding buckets
    fn next(&mut self) -> Option<Self::Item> {
        // Take ownership of the current slices
        let slices = self.slices.take()?;
        
        // If we have no more elements, return None
        if slices.left_indices.is_empty() {
            return None;
        }

        // Split off the first elements and the remaining slices for both trees
        let (left_idx, rest_left_indices) = slices.left_indices.split_first()?;
        let (right_idx, rest_right_indices) = slices.right_indices.split_first()?;
        let (left_bucket, rest_left_buckets) = slices.left_buckets.split_first_mut()?;
        let (right_bucket, rest_right_buckets) = slices.right_buckets.split_first_mut()?;

        // Store the remaining slices for the next iteration
        self.slices = Some(SlicePair {
            left_indices: rest_left_indices,
            right_indices: rest_right_indices,
            left_buckets: rest_left_buckets,
            right_buckets: rest_right_buckets,
        });

        // Indices should match since we verified equal lengths in new()
        if left_idx == right_idx {
            Some((
                Box::new(Some(left_bucket)),
                Box::new(Some(right_bucket)), 
                Path::from(*left_idx),
            ))
        } else {
            panic!("Indices don't match in the same-length trees.");
        }
    }
}

/// Computes the tree index for a specific bucket along a path
/// 
/// # Arguments
/// * `path_bytes` - The path from root as a Vec<u8>
/// * `bucket_index` - The 0-based index of the bucket along the path (0=root, 1=first level, etc.)
/// 
/// # Returns
/// The tree index for the specified bucket
pub fn get_tree_index_for_bucket(path_bytes: &Vec<u8>, bucket_index: usize) -> usize {
    if bucket_index == 0 {
        // Root bucket is always at index 1
        return 1;
    }
    
    // Convert the bytes to a proper Path
    let path = Path::from(path_bytes.clone());
    
    // Follow the path up to the correct depth
    let mut idx = 1; // Start at root
    for i in 0..bucket_index {
        if i < path.len() {
            // Left child = 2*parent, right child = 2*parent + 1
            let direction = path.0.get(i).unwrap_or(&Direction::Left);
            idx = 2 * idx + u8::from(*direction) as usize;
        }
    }
    idx
}
