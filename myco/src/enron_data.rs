//! # Enron Data Integration Module
//! 
//! This module provides functions to integrate real Enron email data into Myco.
//! It handles loading users, generating contact patterns, and converting timestamps.

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use csv::Reader;

/// Load Enron users from the users.csv file
/// Returns a vector of user IDs in the format "user_{numeric_id}"
pub fn load_enron_users(users_csv_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut users = Vec::new();
    let file = File::open(users_csv_path)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    
    // Skip header row
    for (index, result) in rdr.records().enumerate() {
        let record = result?;
        if let Some(email) = record.get(0) {
            // Convert email to user ID format
            users.push(format!("user_{}", index));
        }
    }
    
    println!("Loaded {} Enron users", users.len());
    Ok(users)
}

/// Generate contact patterns for a specific user based on Enron communication data
/// Returns a vector of contact user IDs that this user has communicated with
pub fn generate_enron_contacts(
    user_id: &str, 
    clean_csv_path: &str,
    max_contacts: usize
) -> Result<Vec<String>, Box<dyn Error>> {
    let mut contacts = HashMap::new();
    let file = File::open(clean_csv_path)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    
    // Skip header row
    for result in rdr.records() {
        let record = result?;
        if let (Some(sender_str), Some(receiver_str)) = (record.get(0), record.get(1)) {
            let sender_id = format!("user_{}", sender_str);
            let receiver_id = format!("user_{}", receiver_str);
            
            if sender_id == user_id {
                let count = contacts.entry(receiver_id.clone()).or_insert(0);
                *count += 1;
            }
        }
    }
    
    // Sort contacts by communication frequency and take top max_contacts
    let mut contact_list: Vec<(String, usize)> = contacts.into_iter().collect();
    contact_list.sort_by(|a, b| b.1.cmp(&a.1));
    
    let result: Vec<String> = contact_list
        .into_iter()
        .take(max_contacts)
        .map(|(contact, _)| contact)
        .collect();
    
    println!("Generated {} contacts for user {}", result.len(), user_id);
    Ok(result)
}

/// Convert Enron timestamps to Myco epochs
/// Enron timestamps are Unix timestamps, Myco uses sequential epochs
pub fn convert_timestamps_to_epochs(
    clean_csv_path: &str,
    max_epochs: usize
) -> Result<HashMap<(String, String), u64>, Box<dyn Error>> {
    let mut timestamp_map = HashMap::new();
    let mut unique_timestamps = Vec::new();
    let file = File::open(clean_csv_path)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    
    // Skip header row
    for result in rdr.records() {
        let record = result?;
        if let (Some(sender_str), Some(receiver_str), Some(timestamp_str)) = 
            (record.get(0), record.get(1), record.get(2)) {
            
            let sender_id = format!("user_{}", sender_str);
            let receiver_id = format!("user_{}", receiver_str);
            let timestamp: u64 = timestamp_str.parse()?;
            
            unique_timestamps.push(timestamp);
            timestamp_map.insert((sender_id, receiver_id), timestamp);
        }
    }
    
    // Sort timestamps and create epoch mapping
    unique_timestamps.sort();
    unique_timestamps.dedup();
    
    let mut epoch_map = HashMap::new();
    for (i, timestamp) in unique_timestamps.iter().enumerate() {
        if i >= max_epochs {
            break;
        }
        
        // Find all message pairs with this timestamp
        for ((sender, receiver), ts) in &timestamp_map {
            if *ts == *timestamp {
                epoch_map.insert((sender.clone(), receiver.clone()), i as u64);
            }
        }
    }
    
    println!("Converted {} unique timestamps to {} epochs", 
             unique_timestamps.len(), epoch_map.len());
    Ok(epoch_map)
}

/// Load a subset of Enron data for testing
/// Returns (users, contacts_map, epoch_map) for a limited number of users
pub fn load_enron_subset(
    users_csv_path: &str,
    clean_csv_path: &str,
    max_users: usize,
    max_contacts_per_user: usize,
    max_epochs: usize
) -> Result<(Vec<String>, HashMap<String, Vec<String>>, HashMap<(String, String), u64>), Box<dyn Error>> {
    
    println!("Loading Enron subset: {} users, {} contacts per user, {} epochs", 
             max_users, max_contacts_per_user, max_epochs);
    
    // Load users
    let all_users = load_enron_users(users_csv_path)?;
    let users: Vec<String> = all_users.into_iter().take(max_users).collect();
    
    // Generate contacts for each user
    let mut contacts_map = HashMap::new();
    for user in &users {
        let contacts = generate_enron_contacts(user, clean_csv_path, max_contacts_per_user)?;
        contacts_map.insert(user.clone(), contacts);
    }
    
    // Convert timestamps to epochs
    let epoch_map = convert_timestamps_to_epochs(clean_csv_path, max_epochs)?;
    
    println!("Successfully loaded Enron subset: {} users, {} total contacts, {} epochs", 
             users.len(), contacts_map.values().map(|v| v.len()).sum::<usize>(), epoch_map.len());
    
    Ok((users, contacts_map, epoch_map))
}
