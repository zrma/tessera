use anyhow::Result;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let addr: SocketAddr = std::env::var("TESSERA_GW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:4000".to_string())
        .parse()
        .expect("invalid TESSERA_GW_ADDR");

    let listener = TcpListener::bind(addr).await?;
    info!(target: "gateway", %addr, "listening");

    loop {
        let (socket, peer) = listener.accept().await?;
        info!(target: "gateway", %peer, "accepted");
        tokio::spawn(async move {
            if let Err(e) = handle_conn(socket, peer).await {
                error!(target: "gateway", %peer, error = ?e, "connection error");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, peer: SocketAddr) -> Result<()> {
    // Connect to worker upstream per client
    let upstream_addr: SocketAddr = std::env::var("TESSERA_WORKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5001".to_string())
        .parse()
        .expect("invalid TESSERA_WORKER_ADDR");
    let mut upstream = TcpStream::connect(upstream_addr).await?;
    info!(target: "gateway", %peer, upstream=%upstream_addr, "proxied to upstream");

    loop {
        // Read frame from client
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof
                || e.kind() == std::io::ErrorKind::ConnectionReset
            {
                info!(target: "gateway", %peer, "closed");
                return Ok(());
            }
            return Err(e.into());
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 1_000_000 {
            warn!(target: "gateway", %peer, len, "frame too large");
            return Ok(());
        }
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        // Forward client frame to worker
        upstream.write_all(&len_buf).await?;
        upstream.write_all(&payload).await?;

        // Read reply from worker and send back to client
        let mut rlen_buf = [0u8; 4];
        if let Err(e) = upstream.read_exact(&mut rlen_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof
                || e.kind() == std::io::ErrorKind::ConnectionReset
            {
                warn!(target: "gateway", %peer, "upstream closed");
                return Ok(());
            }
            return Err(e.into());
        }
        let rlen = u32::from_be_bytes(rlen_buf) as usize;
        if rlen > 1_000_000 {
            warn!(target: "gateway", %peer, len=rlen, "reply frame too large");
            return Ok(());
        }
        let mut rpayload = vec![0u8; rlen];
        upstream.read_exact(&mut rpayload).await?;
        stream.write_all(&rlen_buf).await?;
        stream.write_all(&rpayload).await?;
    }
}

// No per-message handling in V0 proxy; messages are forwarded to Worker.

fn init_tracing() {
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
}
