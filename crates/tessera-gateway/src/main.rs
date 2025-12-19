use anyhow::{Context, Result};
use bytes::BytesMut;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tessera_core::{CellId, ClientMsg, Envelope, try_decode_frame};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{
    Assignment, AssignmentBundle, AssignmentListing, ListAssignmentsRequest,
    WatchAssignmentsRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

const UPSTREAM_RETRY_MAX: usize = 3;
const UPSTREAM_RETRY_BACKOFF_MS: u64 = 50;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CellKey(CellId);

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkerRoute {
    worker_id: String,
    addr: SocketAddr,
}

#[derive(Clone)]
struct RoutingTable {
    routes: Arc<RwLock<HashMap<CellKey, WorkerRoute>>>,
}

impl RoutingTable {
    fn new(initial: HashMap<CellKey, WorkerRoute>) -> Self {
        Self {
            routes: Arc::new(RwLock::new(initial)),
        }
    }

    async fn lookup(&self, cell: &CellId) -> Option<WorkerRoute> {
        let guard = self.routes.read().await;
        guard.get(&CellKey(*cell)).cloned()
    }

    fn handle(&self) -> Arc<RwLock<HashMap<CellKey, WorkerRoute>>> {
        Arc::clone(&self.routes)
    }

    async fn len(&self) -> usize {
        self.routes.read().await.len()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let addr: SocketAddr = std::env::var("TESSERA_GW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:4000".to_string())
        .parse()
        .expect("invalid TESSERA_GW_ADDR");

    let initial_routes = load_initial_routes().await?;
    let table = RoutingTable::new(initial_routes);
    {
        let routes = table.handle();
        tokio::spawn(async move {
            routing_refresh_loop(routes).await;
        });
    }
    {
        let routes = table.handle();
        tokio::spawn(async move {
            routing_watch_loop(routes).await;
        });
    }

    let cell_count = table.len().await;
    info!(target: "gateway", cells = cell_count, "routing table loaded");

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

async fn handle_conn(stream: TcpStream, peer: SocketAddr, routing: RoutingTable) -> Result<()> {
    let (mut client_reader, client_writer) = stream.into_split();
    let client_writer = Arc::new(Mutex::new(client_writer));
    let mut upstream: Option<UpstreamConn> = None;

    let result = 'conn_loop: loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = client_reader.read_exact(&mut len_buf).await {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
                info!(target: "gateway", %peer, "closed");
                break Ok(());
            }
            break Err(e.into());
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 1_000_000 {
            warn!(target: "gateway", %peer, len, "frame too large");
            break Ok(());
        }
        let mut payload = vec![0u8; len];
        client_reader.read_exact(&mut payload).await?;

        // Decode to inspect cell for routing.
        let mut buf = BytesMut::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(&payload);
        let Some(env_in) = try_decode_frame::<Envelope<ClientMsg>>(&mut buf) else {
            warn!(target: "gateway", %peer, "failed to decode frame for routing");
            continue;
        };
        let cell = env_in.cell;
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            let Some(route) = routing.lookup(&cell).await else {
                warn!(
                    target: "gateway",
                    %peer,
                    cell = ?cell,
                    attempt,
                    max_attempts = UPSTREAM_RETRY_MAX,
                    "no route available for cell"
                );
                if attempt >= UPSTREAM_RETRY_MAX {
                    warn!(target: "gateway", %peer, cell = ?cell, "closing client connection");
                    break 'conn_loop Ok(());
                }
                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                continue;
            };

            if let Err(e) = ensure_upstream(
                &mut upstream,
                route.clone(),
                Arc::clone(&client_writer),
                peer,
            )
            .await
            {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %route.worker_id,
                    addr = %route.addr,
                    attempt,
                    max_attempts = UPSTREAM_RETRY_MAX,
                    error = ?e,
                    "failed to connect to worker; retrying"
                );
                upstream = None;
                if attempt >= UPSTREAM_RETRY_MAX {
                    warn!(target: "gateway", %peer, "closing client connection");
                    break 'conn_loop Ok(());
                }
                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                continue;
            }

            let conn = upstream.as_mut().expect("upstream to be established");
            if let Err(e) = conn.writer.write_all(&len_buf).await {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %route.worker_id,
                    attempt,
                    max_attempts = UPSTREAM_RETRY_MAX,
                    error = ?e,
                    "failed forwarding frame to worker; reconnecting"
                );
                conn.close().await;
                upstream = None;
                if attempt >= UPSTREAM_RETRY_MAX {
                    warn!(target: "gateway", %peer, "closing client connection");
                    break 'conn_loop Ok(());
                }
                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                continue;
            }
            if let Err(e) = conn.writer.write_all(&payload).await {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %route.worker_id,
                    attempt,
                    max_attempts = UPSTREAM_RETRY_MAX,
                    error = ?e,
                    "failed forwarding frame payload to worker; reconnecting"
                );
                conn.close().await;
                upstream = None;
                if attempt >= UPSTREAM_RETRY_MAX {
                    warn!(target: "gateway", %peer, "closing client connection");
                    break 'conn_loop Ok(());
                }
                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                continue;
            }
            break;
        }
    };

    if let Some(mut conn) = upstream {
        conn.close().await;
    }

    result
}

struct UpstreamConn {
    writer: OwnedWriteHalf,
    route: WorkerRoute,
    reader_handle: tokio::task::JoinHandle<()>,
}

impl UpstreamConn {
    async fn close(&mut self) {
        self.reader_handle.abort();
        let _ = self.writer.shutdown().await;
    }
}

async fn ensure_upstream(
    upstream: &mut Option<UpstreamConn>,
    target_worker: WorkerRoute,
    client_writer: Arc<Mutex<OwnedWriteHalf>>,
    peer: SocketAddr,
) -> Result<()> {
    // Reconnect if the existing upstream reader task has completed.
    if upstream
        .as_ref()
        .is_some_and(|conn| conn.reader_handle.is_finished())
    {
        if let Some(conn) = upstream.as_mut() {
            info!(
                target: "gateway",
                %peer,
                worker = %conn.route.worker_id,
                "upstream connection ended; reconnecting"
            );
            conn.close().await;
        }
        *upstream = None;
    }

    let needs_new = match upstream {
        Some(conn) if conn.route.addr == target_worker.addr => false,
        Some(conn) => {
            info!(
                target: "gateway",
                %peer,
                old_worker = %conn.route.worker_id,
                new_worker = %target_worker.worker_id,
                "cell routed to different worker; reconnecting"
            );
            conn.close().await;
            true
        }
        None => true,
    };

    if needs_new {
        let new_conn = connect_upstream(target_worker.clone(), client_writer, peer).await?;
        if upstream.replace(new_conn).is_some() {
            // old connection already closed above
        }
    }

    Ok(())
}

async fn connect_upstream(
    target_worker: WorkerRoute,
    client_writer: Arc<Mutex<OwnedWriteHalf>>,
    peer: SocketAddr,
) -> Result<UpstreamConn> {
    let stream = TcpStream::connect(target_worker.addr).await?;
    info!(
        target: "gateway",
        %peer,
        worker = %target_worker.worker_id,
        addr = %target_worker.addr,
        "connected to upstream worker"
    );
    let (reader, writer) = stream.into_split();
    let reader_handle = spawn_upstream_reader(
        reader,
        Arc::clone(&client_writer),
        peer,
        target_worker.clone(),
    );
    Ok(UpstreamConn {
        writer,
        route: target_worker,
        reader_handle,
    })
}

fn spawn_upstream_reader(
    mut reader: OwnedReadHalf,
    client_writer: Arc<Mutex<OwnedWriteHalf>>,
    peer: SocketAddr,
    route: WorkerRoute,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut rlen_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut rlen_buf).await {
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) {
                    info!(
                        target: "gateway",
                        %peer,
                        worker = %route.worker_id,
                        "upstream closed connection"
                    );
                } else {
                    warn!(
                        target: "gateway",
                        %peer,
                        worker = %route.worker_id,
                        error = ?e,
                        "failed reading reply from upstream"
                    );
                }
                break;
            }
            let rlen = u32::from_be_bytes(rlen_buf) as usize;
            if rlen > 1_000_000 {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %route.worker_id,
                    len = rlen,
                    "reply frame too large"
                );
                break;
            }
            let mut rpayload = vec![0u8; rlen];
            if let Err(e) = reader.read_exact(&mut rpayload).await {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %route.worker_id,
                    error = ?e,
                    "failed to read reply payload from upstream"
                );
                break;
            }

            let mut writer = client_writer.lock().await;
            if let Err(e) = writer.write_all(&rlen_buf).await {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %route.worker_id,
                    error = ?e,
                    "failed to write reply length to client"
                );
                break;
            }
            if let Err(e) = writer.write_all(&rpayload).await {
                warn!(
                    target: "gateway",
                    %peer,
                    worker = %route.worker_id,
                    error = ?e,
                    "failed to write reply payload to client"
                );
                break;
            }
        }
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

async fn load_initial_routes() -> Result<HashMap<CellKey, WorkerRoute>> {
    match fetch_remote_routes().await {
        Ok(routes) => {
            if routes.is_empty() {
                warn!(
                    target: "gateway",
                    "orchestrator returned empty assignment listing; gateway will have no routes"
                );
            }
            Ok(routes)
        }
        Err(e) => {
            warn!(
                target: "gateway",
                error = ?e,
                "failed to fetch assignments; falling back to static mapping"
            );
            default_routes()
        }
    }
}

async fn fetch_remote_routes() -> Result<HashMap<CellKey, WorkerRoute>> {
    let endpoint = orchestrator_endpoint();
    let mut client = OrchestratorClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connect orchestrator at {}", endpoint))?;
    let response = client
        .list_assignments(ListAssignmentsRequest {})
        .await
        .context("list assignments")?
        .into_inner();

    routes_from_listing(&response)
}

async fn routing_refresh_loop(routes: Arc<RwLock<HashMap<CellKey, WorkerRoute>>>) {
    let mut ticker = time::interval(routing_refresh_interval());
    loop {
        ticker.tick().await;
        match fetch_remote_routes().await {
            Ok(new_routes) => {
                let mut guard = routes.write().await;
                if *guard != new_routes {
                    let new_len = new_routes.len();
                    *guard = new_routes;
                    if new_len == 0 {
                        warn!(
                            target: "gateway",
                            "routing table refreshed from orchestrator; no routes available"
                        );
                    } else {
                        info!(
                            target: "gateway",
                            cells = new_len,
                            "routing table refreshed from orchestrator"
                        );
                    }
                } else {
                    debug!(target: "gateway", "routing table unchanged after refresh");
                }
            }
            Err(e) => {
                warn!(
                    target: "gateway",
                    error = ?e,
                    "failed to refresh assignments; keeping existing routes"
                );
            }
        }
    }
}

async fn routing_watch_loop(routes: Arc<RwLock<HashMap<CellKey, WorkerRoute>>>) {
    loop {
        match watch_assignments_once(routes.clone()).await {
            Ok(_) => {
                warn!(
                    target: "gateway",
                    "assignments watch stream ended; reconnecting after backoff"
                );
            }
            Err(e) => {
                warn!(
                    target: "gateway",
                    error = ?e,
                    "assignments watch failed; reconnecting after backoff"
                );
            }
        }
        time::sleep(Duration::from_secs(1)).await;
    }
}

async fn watch_assignments_once(routes: Arc<RwLock<HashMap<CellKey, WorkerRoute>>>) -> Result<()> {
    let endpoint = orchestrator_endpoint();
    let mut client = OrchestratorClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connect orchestrator at {}", endpoint))?;
    let response = client
        .watch_assignments(WatchAssignmentsRequest {})
        .await
        .context("subscribe to watch_assignments")?;
    let stream = response.into_inner();
    apply_listing_stream(routes, stream)
        .await
        .context("apply routing updates from watch")
}

fn orchestrator_endpoint() -> String {
    let orchestrator_addr =
        std::env::var("TESSERA_ORCH_ADDR").unwrap_or_else(|_| "127.0.0.1:6000".to_string());
    if orchestrator_addr.starts_with("http://") || orchestrator_addr.starts_with("https://") {
        orchestrator_addr
    } else {
        format!("http://{}", orchestrator_addr)
    }
}

fn routing_refresh_interval() -> Duration {
    const DEFAULT_SECS: u64 = 5;
    let secs = std::env::var("TESSERA_GW_REFRESH_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|val| *val > 0)
        .unwrap_or(DEFAULT_SECS);
    Duration::from_secs(secs)
}

async fn apply_listing_update(
    routes: &Arc<RwLock<HashMap<CellKey, WorkerRoute>>>,
    listing: AssignmentListing,
) -> Result<bool> {
    let new_routes = routes_from_listing(&listing)?;
    let mut guard = routes.write().await;
    if *guard == new_routes {
        debug!(target: "gateway", "assignment listing unchanged; skipping route swap");
        return Ok(false);
    }
    let new_len = new_routes.len();
    *guard = new_routes;
    if new_len == 0 {
        warn!(
            target: "gateway",
            "routing table updated from orchestrator watch; no routes available"
        );
    } else {
        info!(
            target: "gateway",
            cells = new_len,
            "routing table updated from orchestrator watch"
        );
    }
    Ok(true)
}

async fn apply_listing_stream<S>(
    routes: Arc<RwLock<HashMap<CellKey, WorkerRoute>>>,
    mut stream: S,
) -> Result<()>
where
    S: tokio_stream::Stream<Item = Result<AssignmentListing, tonic::Status>> + Unpin,
{
    while let Some(listing) = stream.next().await {
        let listing = listing.context("receive assignment listing from watch stream")?;
        let _ = apply_listing_update(&routes, listing).await?;
    }
    Ok(())
}

fn routes_from_listing(listing: &AssignmentListing) -> Result<HashMap<CellKey, WorkerRoute>> {
    let mut routes = HashMap::new();
    for bundle in &listing.workers {
        let parsed_addr: SocketAddr = bundle.addr.parse().with_context(|| {
            format!("parse worker addr {} for {}", bundle.addr, bundle.worker_id)
        })?;
        populate_routes(&mut routes, parsed_addr, bundle)?;
    }
    Ok(routes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;
    use tessera_core::{ActorState, EntityId, Position, ServerMsg, encode_frame};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;
    use tokio_stream::iter;

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

    #[tokio::test]
    async fn listing_stream_updates_routes() {
        let routes = Arc::new(RwLock::new(HashMap::new()));
        let listing_initial = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 0,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };
        let listing_updated = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-b".to_string(),
                addr: "127.0.0.1:5002".to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 0,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };

        let stream = iter(vec![
            Ok(listing_initial.clone()),
            Ok(listing_updated.clone()),
        ]);
        apply_listing_stream(routes.clone(), stream)
            .await
            .expect("process listing stream");

        let guard = routes.read().await;
        let route = guard
            .get(&CellKey(CellId::grid(0, 0, 0)))
            .expect("route present");
        assert_eq!(route.worker_id, "worker-b");
        assert_eq!(route.addr, "127.0.0.1:5002".parse().unwrap());
    }

    #[tokio::test]
    async fn upstream_connect_failure_closes_client_after_retries() {
        let unused_addr = {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind unused port");
            let addr = listener.local_addr().expect("listener addr");
            drop(listener);
            addr
        };

        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "missing".to_string(),
                addr: unused_addr,
            },
        );
        let routing = RoutingTable::new(map);

        let gateway_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway listener");
        let gw_addr = gateway_listener.local_addr().expect("gateway addr");
        let routing_clone = routing.clone();
        tokio::spawn(async move {
            if let Ok((sock, peer)) = gateway_listener.accept().await {
                let _ = handle_conn(sock, peer, routing_clone).await;
            }
        });

        let mut client = TcpStream::connect(gw_addr)
            .await
            .expect("connect to gateway");
        let env = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 1 },
        };
        client
            .write_all(&encode_frame(&env))
            .await
            .expect("send first frame");

        let mut buf = [0u8; 1];
        let read = timeout(Duration::from_millis(500), client.read_exact(&mut buf)).await;
        match read {
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) => {}
            Ok(Ok(_)) => panic!("unexpected data from gateway after upstream failures"),
            Ok(Err(e)) => panic!("unexpected error while waiting for close: {e:?}"),
            Err(_) => panic!("gateway did not close client connection in time"),
        }
    }

    #[tokio::test]
    async fn gateway_basic_flow_forwards_join_and_move() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");

        let mut worker_task = tokio::spawn(async move {
            run_stub_worker(worker_listener).await;
        });

        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-test".to_string(),
                addr: worker_addr,
            },
        );
        let routing = RoutingTable::new(map);

        let gateway_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway listener");
        let gw_addr = gateway_listener.local_addr().expect("gateway addr");
        let routing_clone = routing.clone();
        let mut gateway_task = tokio::spawn(async move {
            if let Ok((sock, peer)) = gateway_listener.accept().await {
                handle_conn(sock, peer, routing_clone)
                    .await
                    .expect("handle conn");
            }
        });

        let mut client = TcpStream::connect(gw_addr)
            .await
            .expect("connect to gateway");
        let cell = CellId::grid(0, 0, 0);

        let join_env = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        client
            .write_all(&encode_frame(&join_env))
            .await
            .expect("send join");
        let snapshot = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        match snapshot.payload {
            ServerMsg::Snapshot {
                cell: snap_cell,
                actors,
            } => {
                assert_eq!(snap_cell, cell);
                assert!(actors.iter().any(|actor| actor.id == EntityId(1)));
            }
            other => panic!("expected snapshot, got {other:?}"),
        }

        let move_env = Envelope {
            cell,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: 1.0,
                dy: 0.5,
            },
        };
        client
            .write_all(&encode_frame(&move_env))
            .await
            .expect("send move");
        let delta = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        match delta.payload {
            ServerMsg::Delta {
                cell: delta_cell,
                moved,
            } => {
                assert_eq!(delta_cell, cell);
                let moved_actor = moved
                    .iter()
                    .find(|actor| actor.id == EntityId(1))
                    .expect("delta for actor");
                assert_eq!(moved_actor.pos, Position { x: 1.0, y: 0.5 });
            }
            other => panic!("expected delta, got {other:?}"),
        }

        drop(client);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    #[tokio::test]
    async fn empty_listing_clears_routes() {
        let routes = Arc::new(RwLock::new(HashMap::new()));
        let listing_initial = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 1,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };
        let stream = iter(vec![
            Ok(listing_initial.clone()),
            Ok(AssignmentListing { workers: vec![] }),
        ]);
        apply_listing_stream(routes.clone(), stream)
            .await
            .expect("process listing stream");

        let guard = routes.read().await;
        assert!(guard.is_empty());
    }

    async fn run_stub_worker(listener: TcpListener) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let mut seq_out = 0u64;
        let mut actors: HashMap<EntityId, Position> = HashMap::new();

        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = socket.read_exact(&mut len_buf).await {
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) {
                    break;
                }
                panic!("worker read len error: {e:?}");
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            if let Err(e) = socket.read_exact(&mut payload).await {
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) {
                    break;
                }
                panic!("worker read payload error: {e:?}");
            }

            let env_in: Envelope<ClientMsg> =
                serde_json::from_slice(&payload).expect("decode client frame");
            let cell = env_in.cell;

            let reply = match env_in.payload {
                ClientMsg::Ping { ts } => ServerMsg::Pong { ts },
                ClientMsg::Join { actor, pos } => {
                    actors.insert(actor, pos);
                    let snapshot = actors
                        .iter()
                        .map(|(id, pos)| ActorState { id: *id, pos: *pos })
                        .collect::<Vec<_>>();
                    ServerMsg::Snapshot {
                        cell,
                        actors: snapshot,
                    }
                }
                ClientMsg::Move { actor, dx, dy } => {
                    let entry = actors.entry(actor).or_insert(Position { x: 0.0, y: 0.0 });
                    entry.x += dx;
                    entry.y += dy;
                    ServerMsg::Delta {
                        cell,
                        moved: vec![ActorState {
                            id: actor,
                            pos: *entry,
                        }],
                    }
                }
            };

            let env_out = Envelope {
                cell,
                seq: seq_out,
                epoch: env_in.epoch,
                payload: reply,
            };
            seq_out = seq_out.wrapping_add(1);
            let frame = encode_frame(&env_out);
            if let Err(e) = socket.write_all(&frame).await {
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) {
                    break;
                }
                panic!("worker write error: {e:?}");
            }
        }
    }

    async fn read_env_with_timeout(
        stream: &mut TcpStream,
        timeout_after: Duration,
    ) -> Envelope<ServerMsg> {
        timeout(timeout_after, async {
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.expect("read len");
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).await.expect("read payload");
            serde_json::from_slice::<Envelope<ServerMsg>>(&payload).expect("decode reply")
        })
        .await
        .expect("read timeout")
    }

    async fn wait_task(label: &str, handle: &mut tokio::task::JoinHandle<()>) {
        match timeout(Duration::from_millis(500), &mut *handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("{label} task failed: {e:?}"),
            Err(_) => {
                handle.abort();
                panic!("{label} task timeout");
            }
        }
    }
}
