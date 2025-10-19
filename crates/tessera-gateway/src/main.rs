use anyhow::{Context, Result};
use bytes::BytesMut;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tessera_core::{CellId, ClientMsg, Envelope, try_decode_frame};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{Assignment, AssignmentBundle, ListAssignmentsRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CellKey(CellId);

#[derive(Clone, Debug)]
struct WorkerRoute {
    worker_id: String,
    addr: SocketAddr,
}

#[derive(Clone)]
struct RoutingTable {
    routes: Arc<HashMap<CellKey, WorkerRoute>>,
}

impl RoutingTable {
    fn lookup(&self, cell: &CellId) -> Option<&WorkerRoute> {
        self.routes.get(&CellKey(*cell))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let addr: SocketAddr = std::env::var("TESSERA_GW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:4000".to_string())
        .parse()
        .expect("invalid TESSERA_GW_ADDR");

    let table = match fetch_routing_table().await {
        Ok(table) if !table.routes.is_empty() => table,
        Ok(_) => {
            warn!(target: "gateway", "orchestrator returned empty assignment listing; falling back to static mapping");
            RoutingTable {
                routes: Arc::new(default_routes()?),
            }
        }
        Err(e) => {
            warn!(
                target: "gateway",
                error = ?e,
                "failed to fetch assignments; falling back to static mapping"
            );
            RoutingTable {
                routes: Arc::new(default_routes()?),
            }
        }
    };

    info!(target: "gateway", cells = table.routes.len(), "routing table loaded");

    let listener = TcpListener::bind(addr).await?;
    info!(target: "gateway", %addr, "listening");

    loop {
        let (socket, peer) = listener.accept().await?;
        let routing = table.clone();
        info!(target: "gateway", %peer, "accepted");
        tokio::spawn(async move {
            if let Err(e) = handle_conn(socket, peer, routing).await {
                error!(target: "gateway", %peer, error = ?e, "connection error");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, peer: SocketAddr, routing: RoutingTable) -> Result<()> {
    let mut upstream: Option<UpstreamConn> = None;

    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
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

        // Decode to inspect cell for routing.
        let mut buf = BytesMut::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(&payload);
        let Some(env_in) = try_decode_frame::<Envelope<ClientMsg>>(&mut buf) else {
            warn!(target: "gateway", %peer, "failed to decode frame for routing");
            continue;
        };
        let cell = env_in.cell;
        let Some(route) = routing.lookup(&cell) else {
            warn!(
                target: "gateway",
                %peer,
                cell = ?cell,
                "no route available for cell"
            );
            continue;
        };

        // Ensure we are connected to the correct worker.
        let target_worker = route.clone();
        match upstream.as_mut() {
            Some(conn) if conn.route.addr == target_worker.addr => {}
            Some(conn) => {
                info!(
                    target: "gateway",
                    %peer,
                    old_worker = %conn.route.worker_id,
                    new_worker = %target_worker.worker_id,
                    "cell routed to different worker; reconnecting"
                );
                conn.close().await;
                let stream = TcpStream::connect(target_worker.addr).await?;
                info!(
                    target: "gateway",
                    %peer,
                    worker = %target_worker.worker_id,
                    addr = %target_worker.addr,
                    "new upstream connection established"
                );
                *conn = UpstreamConn {
                    stream,
                    route: target_worker.clone(),
                };
            }
            None => {
                let stream = TcpStream::connect(target_worker.addr).await?;
                info!(
                    target: "gateway",
                    %peer,
                    worker = %target_worker.worker_id,
                    addr = %target_worker.addr,
                    "connected to upstream worker"
                );
                upstream = Some(UpstreamConn {
                    stream,
                    route: target_worker.clone(),
                });
            }
        }

        let conn = upstream.as_mut().expect("upstream to be established");
        conn.stream.write_all(&len_buf).await?;
        conn.stream.write_all(&payload).await?;

        // Read reply
        let mut rlen_buf = [0u8; 4];
        if let Err(e) = conn.stream.read_exact(&mut rlen_buf).await {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %conn.route.worker_id,
                    "upstream closed connection"
                );
                return Ok(());
            }
            return Err(e.into());
        }
        let rlen = u32::from_be_bytes(rlen_buf) as usize;
        if rlen > 1_000_000 {
            warn!(
                target: "gateway",
                %peer,
                worker = %conn.route.worker_id,
                len = rlen,
                "reply frame too large"
            );
            return Ok(());
        }
        let mut rpayload = vec![0u8; rlen];
        conn.stream.read_exact(&mut rpayload).await?;
        stream.write_all(&rlen_buf).await?;
        stream.write_all(&rpayload).await?;
    }
}

struct UpstreamConn {
    stream: TcpStream,
    route: WorkerRoute,
}

impl UpstreamConn {
    async fn close(&mut self) {
        if let Err(e) = self.stream.shutdown().await {
            warn!(
                target: "gateway",
                worker = %self.route.worker_id,
                error = ?e,
                "failed to shutdown upstream connection"
            );
        }
    }
}

async fn fetch_routing_table() -> Result<RoutingTable> {
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
        .list_assignments(ListAssignmentsRequest {})
        .await
        .context("list assignments")?
        .into_inner();

    let mut routes = HashMap::new();
    for bundle in response.workers {
        let parsed_addr: SocketAddr = bundle.addr.parse().with_context(|| {
            format!("parse worker addr {} for {}", bundle.addr, bundle.worker_id)
        })?;
        populate_routes(&mut routes, parsed_addr, &bundle)?;
    }

    Ok(RoutingTable {
        routes: Arc::new(routes),
    })
}

fn populate_routes(
    routes: &mut HashMap<CellKey, WorkerRoute>,
    addr: SocketAddr,
    bundle: &AssignmentBundle,
) -> Result<()> {
    for assignment in &bundle.cells {
        let cell = assignment_to_cell(assignment)?;
        routes.insert(
            CellKey(cell),
            WorkerRoute {
                worker_id: bundle.worker_id.clone(),
                addr,
            },
        );
    }
    Ok(())
}

fn default_routes() -> Result<HashMap<CellKey, WorkerRoute>> {
    let addr: SocketAddr = std::env::var("TESSERA_WORKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5001".to_string())
        .parse()
        .context("parse fallback TESSERA_WORKER_ADDR")?;
    let mut map = HashMap::new();
    map.insert(
        CellKey(CellId::grid(0, 0, 0)),
        WorkerRoute {
            worker_id: "worker-default".to_string(),
            addr,
        },
    );
    Ok(map)
}

fn assignment_to_cell(assignment: &Assignment) -> Result<CellId> {
    let depth = u8::try_from(assignment.depth)
        .map_err(|_| anyhow::anyhow!("assignment depth {} out of range", assignment.depth))?;
    let sub = u8::try_from(assignment.sub)
        .map_err(|_| anyhow::anyhow!("assignment sub {} out of range", assignment.sub))?;
    Ok(CellId {
        world: assignment.world,
        cx: assignment.cx,
        cy: assignment.cy,
        depth,
        sub,
    })
}

fn init_tracing() {
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_to_cell_round_trip() {
        let assignment = Assignment {
            world: 5,
            cx: -1,
            cy: 4,
            depth: 1,
            sub: 2,
        };
        let cell = assignment_to_cell(&assignment).expect("convert");
        assert_eq!(cell.world, 5);
        assert_eq!(cell.cx, -1);
        assert_eq!(cell.cy, 4);
        assert_eq!(cell.depth, 1);
        assert_eq!(cell.sub, 2);
    }
}
