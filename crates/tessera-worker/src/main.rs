use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use tessera_core::{
    ActorState, CellId, ClientMsg, EntityId, Envelope, Position, ServerMsg, Tick, encode_frame,
    try_decode_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!(target: "worker", "tessera-worker starting");

    // Start upstream TCP server for Gateway
    let addr: SocketAddr = std::env::var("TESSERA_WORKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5001".to_string())
        .parse()
        .expect("invalid TESSERA_WORKER_ADDR");
    let server = tokio::spawn(async move {
        if let Err(e) = run_server(addr).await {
            error!(target: "worker", error=?e, "server error");
        }
    });

    // Basic 30Hz tick loop
    let mut tick = Tick(0);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(33));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                tick.0 += 1;
                on_tick(tick).await;
            }
            _ = tokio::signal::ctrl_c() => {
                info!(target: "worker", "shutdown signal received");
                break;
            }
        }
    }

    info!(target: "worker", "tessera-worker stopped");
    // Let server task stop naturally when process exits
    server.abort();
    Ok(())
}

async fn on_tick(tick: Tick) {
    // TODO: cell queues, AOI broadcasting, metrics
    if tick.0 % 30 == 0 {
        info!(target: "worker", tick = tick.0, "tick heartbeat");
    }
}

fn init_tracing() {
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
}

async fn run_server(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(target: "worker", %addr, "listening upstream");
    loop {
        let (sock, peer) = listener.accept().await?;
        info!(target: "worker", %peer, "upstream accepted");
        tokio::spawn(async move {
            if let Err(e) = handle_upstream(sock, peer).await {
                error!(target: "worker", %peer, error=?e, "upstream connection error");
            }
        });
    }
}

async fn handle_upstream(mut stream: TcpStream, peer: SocketAddr) -> Result<()> {
    // Per-connection minimal state
    let mut actors: HashMap<EntityId, Position> = HashMap::new();
    let cell = CellId::grid(0, 0, 0);
    let epoch: u32 = 0;
    let mut seq_out: u64 = 0;

    loop {
        // Read one frame from Gateway
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof
                || e.kind() == std::io::ErrorKind::ConnectionReset
            {
                info!(target: "worker", %peer, "upstream closed");
                return Ok(());
            }
            return Err(e.into());
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 1_000_000 {
            warn!(target: "worker", %peer, len, "frame too large");
            return Ok(());
        }
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        // Decode as Envelope<ClientMsg>
        let mut buf = bytes::BytesMut::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(&payload);
        let Some(env_in) = try_decode_frame::<Envelope<ClientMsg>>(&mut buf) else {
            warn!(target: "worker", %peer, "failed to decode frame");
            continue;
        };

        // Handle message and reply
        let reply = match env_in.payload {
            ClientMsg::Ping { ts } => ServerMsg::Pong { ts },
            ClientMsg::Join { actor, pos } => {
                actors.insert(actor, pos);
                ServerMsg::Snapshot {
                    cell,
                    actors: vec![ActorState { id: actor, pos }],
                }
            }
            ClientMsg::Move { actor, dx, dy } => {
                let p = actors.entry(actor).or_insert(Position { x: 0.0, y: 0.0 });
                p.x += dx;
                p.y += dy;
                ServerMsg::Delta {
                    cell,
                    moved: vec![ActorState { id: actor, pos: *p }],
                }
            }
        };

        let env_out = Envelope {
            cell,
            seq: seq_out,
            epoch,
            payload: reply,
        };
        seq_out = seq_out.wrapping_add(1);
        let frame = encode_frame(&env_out);
        stream.write_all(&frame).await?;
    }
}
