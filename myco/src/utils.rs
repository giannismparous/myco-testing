//! Utility functions for the Myco protocol.
//! Utility functions for the Myco protocol.

use std::{
    collections::HashSet,
    error::Error,
    fs,
    path::Path as StdPath,
    process::Command,
};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use crate::{
    constants::{NONCE_SIZE, Q, TAG_SIZE},
    crypto::encrypt,
    dtypes::*,
    error::MycoError,
};


/// Helper function to get the indices of the paths.
pub fn get_path_indices(paths: Vec<Path>) -> Vec<usize> {
    // Initialize empty set to store unique node indices, starting with root (index 1)
    let mut pathset: HashSet<usize> = HashSet::new();
    pathset.insert(1);

    // Use thread-safe collection for parallel processing
    let thread_safe_indices: HashSet<usize> = paths.par_iter()
        .flat_map(|p| {
            let mut indices = Vec::new();
            p.clone().into_iter().fold(1, |acc, d| {
                // Calculate child index: left child is 2*parent, right child is 2*parent + 1
                let idx = 2 * acc + u8::from(d) as usize;
                indices.push(idx);
                idx
            });
            indices
        })
        .collect();
    
    // Merge results into the pathset
    pathset.extend(thread_safe_indices);

    // Convert set to vector and return
    pathset.into_iter().collect()
}

/// Generates self-signed TLS certificates for testing purposes.
/// Creates a certificate and private key in the 'certs' directory.
pub fn generate_test_certificates() -> Result<(), MycoError> {
    // Ensure the "certs" directory exists.
    if !StdPath::new("certs").exists() {
        fs::create_dir("certs")?;
    }
    if StdPath::new("certs/server-cert.pem").exists() && StdPath::new("certs/server-key.pem").exists() {
        // Clean up old certificates to ensure we have fresh ones.
        fs::remove_file("certs/server-cert.pem")?;
        fs::remove_file("certs/server-key.pem")?;
    }

    // Create a temporary OpenSSL config file.
    fs::write(
        "openssl.cnf",
        r#"
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
"#,
    )?;

    // Generate private key and self-signed certificate using OpenSSL.
    let output = Command::new("openssl")
        .args(&[
            "req",
            "-x509",
            "-newkey", "rsa:4096",
            "-keyout", "certs/server-key.pem",
            "-out", "certs/server-cert.pem",
            "-days", "365",
            "-nodes",
            "-config", "openssl.cnf",
            "-extensions", "v3_req",
        ])
        .output()?;
    if !output.status.success() {
        return Err(MycoError::CryptoError(format!(
            "Failed to generate self-signed certificate: {:?}",
            output
        )));
    }

    // Convert the key to PKCS8 format which rustls expects.
    let output = Command::new("openssl")
        .args(&[
            "pkcs8",
            "-topk8",
            "-nocrypt",
            "-in", "certs/server-key.pem",
            "-out", "certs/server-key.pem.tmp",
        ])
        .output()?;
    if !output.status.success() {
        return Err(MycoError::CryptoError(format!(
            "Failed to convert key to PKCS8 format: {:?}",
            output
        )));
    }

    // Replace the original key with the PKCS8 version.
    fs::rename("certs/server-key.pem.tmp", "certs/server-key.pem")?;
    // Clean up the config file.
    fs::remove_file("openssl.cnf")?;
    Ok(())
}

pub type Index = Vec<u8>;

pub(crate) fn l_to_u64(l: &Index, num_clients: usize) -> u64 {
    let mut bytes = [0u8; 8];
    let len = l.len().min(8);
    bytes[..len].copy_from_slice(&l[..len]);
    let result = u64::from_le_bytes(bytes);
    result % (Q * num_clients) as u64
}


/// Pads a message to a target length by appending zeros.
///
/// # Arguments
/// * `message` - The message bytes to pad
/// * `target_length` - The desired length after padding
///
/// # Returns
/// A new vector containing the padded message
pub(crate) fn pad_message(message: &[u8], target_length: usize) -> Vec<u8> {
    let mut padded = message.to_vec();
    if padded.len() < target_length {
        padded.resize(target_length, 0);
    }
    padded
}

/// Trims trailing zeros from a byte slice.
///
/// # Arguments
/// * `buffer` - The byte slice to trim
///
/// # Returns
/// A new vector with trailing zeros removed
pub fn trim_zeros(buffer: &[u8]) -> Vec<u8> {
    let buf: Vec<u8> = buffer
        .iter()
        .rev()
        .skip_while(|&&x| x == 0)
        .cloned()
        .collect();
    buf.into_iter().rev().collect()
}

/// Generates a dummy encrypted message of specified size with a random key
///
/// # Arguments
/// * `size` - The size of the dummy message to encrypt
///
/// # Returns
/// * `Ok(Vec<u8>)` - The encrypted dummy message
/// * `Err(MycoError)` - If encryption fails
pub fn generate_dummy_message(block_size: usize, rng: &mut ChaCha20Rng) -> Result<Vec<u8>, MycoError> {    
    // Generate random 16-byte key
    let mut key = vec![0u8; 16];
    rng.fill_bytes(&mut key);

    // Create zero vector of specified size
    let message = vec![0u8; block_size - NONCE_SIZE - TAG_SIZE];

    // Encrypt the zero vector
    let ciphertext = encrypt(&key, &message, block_size - NONCE_SIZE - TAG_SIZE)?;
    Ok(ciphertext)
}

/// Derive a per-bucket RNG by combining a global seed with the bucket index.
pub fn derive_rng(base_seed: [u8; 32], idx: usize) -> ChaCha20Rng {
    let mut new_seed = base_seed;
    new_seed[0] ^= idx as u8;
    ChaCha20Rng::from_seed(new_seed)
}

/// Constructs a TLS-enabled channel using the certificate in the `certs/` folder.
/// We load the server certificate (which, for self-signed setups, can act as the CA cert)
/// and configure TLS with the proper domain name.
pub async fn build_tls_channel(url: &str) -> Result<Channel, Box<dyn Error>> {
    // Check if the certificate file exists.
    if !std::path::Path::new("certs/server-cert.pem").exists() {
        println!("Certificate not found at 'certs/server-cert.pem'. Generating test certificates...");
        generate_test_certificates().map_err(|e| format!("Failed generating test certificates: {}", e))?;
    }

    // Load the server certificate (for self-signed setups, this certificate acts as the CA cert).
    let cert = fs::read("certs/server-cert.pem")?;
    let ca_cert = Certificate::from_pem(cert);

    let tls_config = ClientTlsConfig::new()
        .ca_certificate(ca_cert)
        // The domain name here should match the certificate's CN or SAN.
        .domain_name("localhost");

    let channel = Channel::from_shared(url.to_string())?
        .tls_config(tls_config)?
        .connect()
        .await?;
    Ok(channel)
}