use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tessera_core::{
    ActorState, CellId, ClientMsg, EntityId, Envelope, Position, ServerMsg, Tick, encode_frame,
    try_decode_frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    Mutex,
    mpsc::{UnboundedSender, unbounded_channel},
};
use tracing::{error, info, warn};

#[derive(Default)]
struct SharedState {
    actors: Mutex<HashMap<EntityId, Position>>,
    clients: Mutex<Vec<UnboundedSender<ServerMsg>>>,
}

impl SharedState {
    async fn broadcast(&self, msg: ServerMsg) {
        let mut clients = self.clients.lock().await;
        clients.retain(|tx| tx.send(msg.clone()).is_ok());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!(target: "worker", "tessera-worker starting");

    // Start upstream TCP server for Gateway
    let addr: SocketAddr = std::env::var("TESSERA_WORKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5001".to_string())
        .parse()
        .expect("invalid TESSERA_WORKER_ADDR");
    let state = Arc::new(SharedState::default());
    let server_state = state.clone();
    let server = tokio::spawn(async move {
        if let Err(e) = run_server(addr, server_state).await {
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

async fn run_server(addr: SocketAddr, state: Arc<SharedState>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(target: "worker", %addr, "listening upstream");
    loop {
        let (sock, peer) = listener.accept().await?;
        info!(target: "worker", %peer, "upstream accepted");
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_upstream(sock, peer, st).await {
                error!(target: "worker", %peer, error=?e, "upstream connection error");
            }
        });
    }
}

async fn handle_upstream(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<SharedState>,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (tx, mut rx) = unbounded_channel::<ServerMsg>();
    {
        let mut clients = state.clients.lock().await;
        clients.push(tx.clone());
    }

    let cell = CellId::grid(0, 0, 0);
    let epoch: u32 = 0;
    let mut seq_out: u64 = 0;

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let env_out = Envelope {
                cell,
                seq: seq_out,
                epoch,
                payload: msg,
            };
            seq_out = seq_out.wrapping_add(1);
            let frame = encode_frame(&env_out);
            if let Err(e) = writer.write_all(&frame).await {
                error!(target: "worker", %peer, error=?e, "write error");
                break;
            }
        }
        info!(target: "worker", %peer, "writer task ended");
    });

    loop {
        // Read one frame from Gateway
        let mut len_buf = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut len_buf).await {
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
        reader.read_exact(&mut payload).await?;

        // Decode as Envelope<ClientMsg>
        let mut buf = bytes::BytesMut::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(&payload);
        let Some(env_in) = try_decode_frame::<Envelope<ClientMsg>>(&mut buf) else {
            warn!(target: "worker", %peer, "failed to decode frame");
            continue;
        };

        match env_in.payload {
            ClientMsg::Ping { ts } => {
                let _ = tx.send(ServerMsg::Pong { ts });
            }
            ClientMsg::Join { actor, pos } => {
                {
                    let mut actors = state.actors.lock().await;
                    actors.insert(actor, pos);
                    let snapshot = actors
                        .iter()
                        .map(|(id, pos)| ActorState { id: *id, pos: *pos })
                        .collect::<Vec<_>>();
                    let _ = tx.send(ServerMsg::Snapshot {
                        cell,
                        actors: snapshot,
                    });
                }
                state
                    .broadcast(ServerMsg::Delta {
                        cell,
                        moved: vec![ActorState { id: actor, pos }],
                    })
                    .await;
            }
            ClientMsg::Move { actor, dx, dy } => {
                let pos = {
                    let mut actors = state.actors.lock().await;
                    let p = actors.entry(actor).or_insert(Position { x: 0.0, y: 0.0 });
                    p.x += dx;
                    p.y += dy;
                    *p
                };
                state
                    .broadcast(ServerMsg::Delta {
                        cell,
                        moved: vec![ActorState { id: actor, pos }],
                    })
                    .await;
            }
        }
    }
}
