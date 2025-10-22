//! # Myco Error Types
//!
//! This module contains the error types used throughout the Myco library.
use thiserror::Error;

#[derive(Debug, Error)]
/// An enum representing the different types of errors that can occur in Myco
pub enum MycoError {
    /// Cryptographic operation errors
    #[error("Cryptographic error: {0}")]
    CryptoError(String),
    
    /// Key management errors
    #[error("Key error: {0}")]
    KeyError(String),
    
    /// Server communication errors
    #[error("Server error: {0}")]
    ServerError(String),
    
    /// Storage-related errors
    #[error("Storage error: {0}")]
    StorageError(String),
    
    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    /// IO errors
    #[error("{0}")]
    IoError(std::io::Error),
    
    /// TLS errors
    #[error("{0}")]
    TlsError(rustls::Error),
    
    /// Concurrency errors (mutex, thread, channel)
    #[error("Concurrency error: {0}")]
    ConcurrencyError(String),
    
    /// Parsing errors
    #[error("Parsing error: {0}")]
    ParseError(String),
    
    /// Configuration errors
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    /// Network errors
    #[error("Network error: {0}")]
    NetworkError(String),
    
    /// Protocol errors
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    /// Certificate errors
    #[error("Certificate error: {0}")]
    CertificateError(String),
    
    /// Validation errors
    #[error("Validation error: {0}")]
    ValidationError(String),
}

impl From<std::io::Error> for MycoError {
    fn from(err: std::io::Error) -> Self {
        MycoError::IoError(err)
    }
}

impl From<rustls::Error> for MycoError {
    fn from(err: rustls::Error) -> Self {
        MycoError::TlsError(err)
    }
}

impl<T> From<std::sync::PoisonError<T>> for MycoError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        MycoError::ConcurrencyError(format!("Mutex lock failed: {}", err))
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for MycoError {
    fn from(err: std::sync::mpsc::SendError<T>) -> Self {
        MycoError::ConcurrencyError(format!("Channel send error: {}", err))
    }
}

impl From<std::sync::mpsc::RecvError> for MycoError {
    fn from(err: std::sync::mpsc::RecvError) -> Self {
        MycoError::ConcurrencyError(format!("Channel receive error: {}", err))
    }
}

impl From<std::num::ParseIntError> for MycoError {
    fn from(err: std::num::ParseIntError) -> Self {
        MycoError::ParseError(format!("Failed to parse integer: {}", err))
    }
}

impl From<std::num::ParseFloatError> for MycoError {
    fn from(err: std::num::ParseFloatError) -> Self {
        MycoError::ParseError(format!("Failed to parse float: {}", err))
    }
}

// Add conversion from tonic::Status to MycoError
impl From<tonic::Status> for MycoError {
    fn from(status: tonic::Status) -> Self {
        MycoError::NetworkError(status.to_string())
    }
}
