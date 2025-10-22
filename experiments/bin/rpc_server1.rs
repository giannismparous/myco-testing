use myco::{
    constants::{LATENCY_BENCH_COUNT, NUM_CLIENTS, S1_S2_CONNECTION_COUNT, WARMUP_COUNT}, network::server2::MultiConnectionServer2Access, proto::myco::{server1_service_server::{Server1Service, Server1ServiceServer}, BatchInitRequest, BatchInitResponse, BatchWriteRequest, BatchWriteResponse, QueueWriteRequest, QueueWriteResponse}, server1::Server1
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{transport::{Identity, Server}, Request, Response, Status};
use tonic::transport::ServerTlsConfig;
use myco::utils::generate_test_certificates;
use myco::logging;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicBool};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static ALLOCATOR: Jemalloc = Jemalloc;

pub struct MyServer1Service {
    server1: Arc<RwLock<Server1>>,
    batch_write_count: Arc<AtomicUsize>,
    warm_up_complete: Arc<AtomicBool>,
}

impl MyServer1Service {
    pub async fn new() -> Self {
        println!("Establishing {} secure connections to Server2...", S1_S2_CONNECTION_COUNT);
        let s2_access = Box::new(
            MultiConnectionServer2Access::new("https://[::1]:3004").await.unwrap()
        );
        Self {
            server1: Arc::new(RwLock::new(Server1::new(s2_access, NUM_CLIENTS))),
            batch_write_count: Arc::new(AtomicUsize::new(0)),
            warm_up_complete: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[tonic::async_trait]
impl Server1Service for MyServer1Service {
    async fn queue_write(
        &self,
        request: Request<QueueWriteRequest>,
    ) -> Result<Response<QueueWriteResponse>, Status> {
        let req = request.into_inner();
        let k_renc_t = req.k_renc_t.ok_or_else(|| Status::invalid_argument("Missing k_renc_t"))?;
        self.server1
            .write()
            .await
            .queue_write(req.ct, req.ct_ntf, req.f, req.f_ntf, k_renc_t.into(), req.c_s)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(QueueWriteResponse { success: true }))
    }

    async fn batch_write(
        &self,
        _request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        self.server1
            .write()
            .await
            .async_batch_write()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let count = self.batch_write_count.fetch_add(1, Ordering::SeqCst) + 1;
        
        println!("Batch write count: {}, Warmup complete: {}, WARMUP_COUNT: {}, LATENCY_BENCH_COUNT: {}", 
                 count, 
                 self.warm_up_complete.load(Ordering::SeqCst),
                 WARMUP_COUNT,
                 LATENCY_BENCH_COUNT);
        
        if !self.warm_up_complete.load(Ordering::SeqCst) && count >= WARMUP_COUNT {
            println!("Warm-up phase complete, starting measurements");
            self.warm_up_complete.store(true, Ordering::SeqCst);
            self.batch_write_count.store(0, Ordering::SeqCst);
        } else if self.warm_up_complete.load(Ordering::SeqCst) && count >= LATENCY_BENCH_COUNT {
            println!("Measurement phase complete - logging results");
            self.batch_write_count.store(0, Ordering::SeqCst);
            logging::calculate_and_append_averages("server1_latency.csv", "server1_bytes.csv");
        }

        Ok(Response::new(BatchWriteResponse { success: true }))
    }

    async fn batch_init(
        &self,
        _request: Request<BatchInitRequest>,
    ) -> Result<Response<BatchInitResponse>, Status> {
        self.server1
            .write()
            .await
            .batch_init()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(BatchInitResponse { success: true }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let self_addr = args.get(1).unwrap_or(&"0.0.0.0:3002".to_string()).to_string();
    let s2_addr = args.get(2).unwrap_or(&"https://localhost:3004".to_string()).to_string();

    println!("Establishing {} secure connections to Server2 at {}...", S1_S2_CONNECTION_COUNT, s2_addr);
    let s2_access = Box::new(MultiConnectionServer2Access::new(&s2_addr).await?);
    
    let server1_service = MyServer1Service {
        server1: Arc::new(RwLock::new(Server1::new(s2_access, NUM_CLIENTS))),
        batch_write_count: Arc::new(AtomicUsize::new(0)),
        warm_up_complete: Arc::new(AtomicBool::new(false)),
    };
    println!("Server1 listening on {}", self_addr);

    if tokio::fs::metadata("certs/server-cert.pem").await.is_err() 
        || tokio::fs::metadata("certs/server-key.pem").await.is_err() 
    {
        println!("Certificate or key not found in 'certs/' folder. Generating test certificates...");
        tokio::task::spawn_blocking(|| {
            generate_test_certificates().map_err(|e| format!("Failed generating test certificates: {}", e))
        }).await??;
    }

    let cert = tokio::fs::read("certs/server-cert.pem").await?;
    let key = tokio::fs::read("certs/server-key.pem").await?;

    let tls_config = ServerTlsConfig::new()
        .identity(Identity::from_pem(cert, key));

    Server::builder()
        .tls_config(tls_config)?
        .add_service(Server1ServiceServer::new(server1_service))
        .serve(self_addr.parse()?)
        .await?;

    Ok(())
}