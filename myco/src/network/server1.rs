//! # Myco Server1 Network Module
//!
//! This module contains the network communication code for interacting with Server1.

use crate::dtypes::TreeMycoKey;
use crate::proto::myco::{
    server1_service_client::Server1ServiceClient,
    QueueWriteRequest, BatchInitRequest, BatchWriteRequest,
};
use crate::server1::Server1;
use crate::error::MycoError;
use crate::network::common::Result;
use tokio::sync::RwLock;
use std::sync::Arc;
use tonic::{transport::Channel, Request};
use async_trait::async_trait;
#[cfg(feature = "bandwidth")]
use prost::Message;
/// A trait for interacting with Server1
#[async_trait]
pub trait Server1Access: Send + Sync {
    /// Queue a write to Server1
    async fn queue_write(
        &self,
        ct: Vec<u8>,
        ct_ntf: Vec<u8>,
        f: Vec<u8>,
        f_ntf: Vec<u8>,
        k_renc_t: crate::dtypes::Key<TreeMycoKey>,
        c_s: Vec<u8>,
    ) -> Result<()>;

    /// Initialize batch processing
    async fn batch_init(&self) -> Result<()>;

    /// Execute batch write
    async fn batch_write(&self) -> Result<()>;
}

/// Local access - direct memory access
#[derive(Clone)]
pub struct LocalServer1Access {
    /// The server instance
    pub server: Arc<RwLock<Server1>>,
}

impl LocalServer1Access {
    /// Create a new LocalServer1Access instance
    pub fn new(server: Arc<RwLock<Server1>>) -> Self {
        Self { server }
    }
}

#[async_trait]
impl Server1Access for LocalServer1Access {
    async fn queue_write(
        &self,
        ct: Vec<u8>,
        ct_ntf: Vec<u8>,
        f: Vec<u8>,
        f_ntf: Vec<u8>,
        k_renc_t: crate::dtypes::Key<TreeMycoKey>,
        c_s: Vec<u8>,
    ) -> Result<()> {
        let result = self.server
            .write()
            .await
            .queue_write(ct, ct_ntf, f, f_ntf, k_renc_t, c_s);
        result
    }

    async fn batch_init(&self) -> Result<()> {
        let mut server = self.server.write().await;
        server.batch_init()
    }

    async fn batch_write(&self) -> Result<()> {
        self.server
            .write()
            .await
            .batch_write()
    }
}

/// Remote access - serialized network access
pub struct RemoteServer1Access {
    client: Server1ServiceClient<Channel>,
}

impl RemoteServer1Access {
    pub async fn new(server1_addr: &str) -> Result<Self> {
        let channel = Channel::from_shared(server1_addr.to_string())
            .map_err(|e| MycoError::NetworkError(e.to_string()))?
            .connect()
            .await
            .map_err(|e| MycoError::NetworkError(e.to_string()))?;
        Ok(Self {
            client: Server1ServiceClient::new(channel),
        })
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            client: Server1ServiceClient::new(channel),
        }
    }
}

#[async_trait]
impl Server1Access for RemoteServer1Access {
    async fn queue_write(
        &self,
        ct: Vec<u8>,
        ct_ntf: Vec<u8>,
        f: Vec<u8>,
        f_ntf: Vec<u8>,
        k_renc_t: crate::dtypes::Key<TreeMycoKey>,
        c_s: Vec<u8>,
    ) -> Result<()> {
        let mut client = self.client.clone();
        
        let request = QueueWriteRequest {
            ct,
            ct_ntf,
            f,
            f_ntf,
            k_renc_t: Some(k_renc_t.into()),
            c_s,
        };
        
        #[cfg(feature = "bandwidth")]
        {
            let request_size = request.encoded_len();
            crate::logging::BytesMetric::new("client_write_bandwidth", request_size).log();
        }

        let response = client
            .queue_write(Request::new(request))
            .await
            .map_err(|e| MycoError::NetworkError(e.to_string()))?
            .into_inner();

        if response.success {
            Ok(())
        } else {
            Err(MycoError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Unexpected response from Server1",
            )))
        }
    }

    async fn batch_init(&self) -> Result<()> {
        let mut client = self.client.clone();
        let request = Request::new(BatchInitRequest {
            num_writes: 0
        });
        let _ = client
            .batch_init(request)
            .await
            .map_err(|e| MycoError::NetworkError(e.to_string()))?
            .into_inner();
        Ok(())
    }

    async fn batch_write(&self) -> Result<()> {
        let mut client = self.client.clone();
        let request = Request::new(BatchWriteRequest {});
        let _ = client
            .batch_write(request)
            .await
            .map_err(|e| MycoError::NetworkError(e.to_string()))?
            .into_inner();
        Ok(())
    }
}
