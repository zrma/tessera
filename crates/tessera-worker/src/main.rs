use anyhow::{Context, Result, anyhow};
use bytes::BytesMut;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tessera_core::{
    ActorState, CellId, ClientMsg, EntityId, Envelope, Position, ServerMsg, Tick, encode_frame,
    try_decode_frame,
};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{Assignment, WorkerRegistration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    Mutex,
    mpsc::{UnboundedSender, unbounded_channel},
};
use tracing::{error, info, warn};

#[derive(Default)]
struct SharedState {
    actors: Mutex<HashMap<CellId, HashMap<EntityId, Position>>>,
    clients: Mutex<Vec<UnboundedSender<(CellId, ServerMsg)>>>,
}

impl SharedState {
    async fn broadcast(&self, cell: CellId, msg: ServerMsg) {
        let mut clients = self.clients.lock().await;
        clients.retain(|tx| tx.send((cell, msg.clone())).is_ok());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!(target: "worker", "tessera-worker starting");

    let addr: SocketAddr = std::env::var("TESSERA_WORKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5001".to_string())
        .parse()
        .expect("invalid TESSERA_WORKER_ADDR");
    let worker_id =
        std::env::var("TESSERA_WORKER_ID").unwrap_or_else(|_| "worker-local".to_string());

    let assignments = match fetch_assignments(&worker_id, addr).await {
        Ok(cells) if !cells.is_empty() => cells,
        Ok(_) => {
            warn!(
                target: "worker",
                worker_id = worker_id.as_str(),
                "orchestrator returned no assignments; falling back to default cell"
            );
            vec![CellId::grid(0, 0, 0)]
        }
        Err(e) => {
            warn!(
                target: "worker",
                worker_id = worker_id.as_str(),
                error = ?e,
                "failed to fetch assignments; falling back to default cell"
            );
            vec![CellId::grid(0, 0, 0)]
        }
    };

    info!(
        target: "worker",
        worker_id = worker_id.as_str(),
        cells = ?assignments,
        "owning cells"
    );
    let owned_cells: Arc<HashSet<CellId>> =
        Arc::new(assignments.iter().copied().collect::<HashSet<_>>());

    let state = Arc::new(SharedState::default());
    let server_state = state.clone();
    let owned_for_server = owned_cells.clone();
    let server = tokio::spawn(async move {
        if let Err(e) = run_server(addr, server_state, owned_for_server).await {
            error!(target: "worker", error = ?e, "server error");
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

async fn fetch_assignments(worker_id: &str, worker_addr: SocketAddr) -> Result<Vec<CellId>> {
    let orchestrator_addr =
        std::env::var("TESSERA_ORCH_ADDR").unwrap_or_else(|_| "127.0.0.1:6000".to_string());
    let endpoint =
        if orchestrator_addr.starts_with("http://") || orchestrator_addr.starts_with("https://") {
            orchestrator_addr
        } else {
            format!("http://{}", orchestrator_addr)
        };

    let mut client = OrchestratorClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connect orchestrator at {}", endpoint))?;
    let response = client
        .register_worker(WorkerRegistration {
            worker_id: worker_id.to_string(),
            addr: worker_addr.to_string(),
        })
        .await
        .context("register worker with orchestrator")?
        .into_inner();

    let mut cells = Vec::with_capacity(response.cells.len());
    for assignment in response.cells {
        cells.push(assignment_to_cell(assignment)?);
    }
    Ok(cells)
}

fn assignment_to_cell(assignment: Assignment) -> Result<CellId> {
    let depth = u8::try_from(assignment.depth)
        .map_err(|_| anyhow!("assignment depth {} out of range", assignment.depth))?;
    let sub = u8::try_from(assignment.sub)
        .map_err(|_| anyhow!("assignment sub {} out of range", assignment.sub))?;
    Ok(CellId {
        world: assignment.world,
        cx: assignment.cx,
        cy: assignment.cy,
        depth,
        sub,
    })
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

async fn run_server(
    addr: SocketAddr,
    state: Arc<SharedState>,
    owned_cells: Arc<HashSet<CellId>>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(target: "worker", %addr, "listening upstream");
    loop {
        let (sock, peer) = listener.accept().await?;
        info!(target: "worker", %peer, "upstream accepted");
        let st = state.clone();
        let owned = owned_cells.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_upstream(sock, peer, st, owned).await {
                error!(target: "worker", %peer, error = ?e, "upstream connection error");
            }
        });
    }
}

async fn handle_upstream(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<SharedState>,
    owned_cells: Arc<HashSet<CellId>>,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (tx, mut rx) = unbounded_channel::<(CellId, ServerMsg)>();
    {
        let mut clients = state.clients.lock().await;
        clients.push(tx.clone());
    }

    tokio::spawn(async move {
        let epoch: u32 = 0;
        let mut seq_out: u64 = 0;
        while let Some((cell, msg)) = rx.recv().await {
            let env_out = Envelope {
                cell,
                seq: seq_out,
                epoch,
                payload: msg,
            };
            seq_out = seq_out.wrapping_add(1);
            let frame = encode_frame(&env_out);
            if let Err(e) = writer.write_all(&frame).await {
                error!(target: "worker", %peer, error = ?e, "write error");
                break;
            }
        }
        info!(target: "worker", %peer, "writer task ended");
    });

    loop {
        // Read one frame from Gateway
        let mut len_buf = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut len_buf).await {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
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
        let mut buf = BytesMut::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(&payload);
        let Some(env_in) = try_decode_frame::<Envelope<ClientMsg>>(&mut buf) else {
            warn!(target: "worker", %peer, "failed to decode frame");
            continue;
        };
        let cell = env_in.cell;
        if !owned_cells.contains(&cell) {
            warn!(
                target: "worker",
                %peer,
                cell = ?cell,
                "received message for cell not owned by this worker"
            );
            continue;
        }

        match env_in.payload {
            ClientMsg::Ping { ts } => {
                let _ = tx.send((cell, ServerMsg::Pong { ts }));
            }
            ClientMsg::Join { actor, pos } => {
                let snapshot = {
                    let mut actors = state.actors.lock().await;
                    let cell_actors = actors.entry(cell).or_default();
                    cell_actors.insert(actor, pos);
                    cell_actors
                        .iter()
                        .map(|(id, pos)| ActorState { id: *id, pos: *pos })
                        .collect::<Vec<_>>()
                };
                let _ = tx.send((
                    cell,
                    ServerMsg::Snapshot {
                        cell,
                        actors: snapshot,
                    },
                ));

                let moved = ActorState { id: actor, pos };
                state
                    .broadcast(
                        cell,
                        ServerMsg::Delta {
                            cell,
                            moved: vec![moved],
                        },
                    )
                    .await;
            }
            ClientMsg::Move { actor, dx, dy } => {
                let moved = {
                    let mut actors = state.actors.lock().await;
                    let cell_actors = actors.entry(cell).or_default();
                    let entry = cell_actors
                        .entry(actor)
                        .or_insert(Position { x: 0.0, y: 0.0 });
                    entry.x += dx;
                    entry.y += dy;
                    ActorState {
                        id: actor,
                        pos: *entry,
                    }
                };
                state
                    .broadcast(
                        cell,
                        ServerMsg::Delta {
                            cell,
                            moved: vec![moved],
                        },
                    )
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_to_cell_round_trip() {
        let assignment = Assignment {
            world: 7,
            cx: -2,
            cy: 5,
            depth: 2,
            sub: 1,
        };
        let cell = assignment_to_cell(assignment).expect("convert");
        assert_eq!(cell.world, 7);
        assert_eq!(cell.cx, -2);
        assert_eq!(cell.cy, 5);
        assert_eq!(cell.depth, 2);
        assert_eq!(cell.sub, 1);
    }

    #[test]
    fn assignment_to_cell_depth_overflow() {
        let assignment = Assignment {
            world: 0,
            cx: 0,
            cy: 0,
            depth: 512,
            sub: 0,
        };
        let err = assignment_to_cell(assignment).expect_err("should fail");
        assert!(err.to_string().contains("depth"));
    }
}
