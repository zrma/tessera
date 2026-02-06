use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tessera_core::{CellId, ClientMsg, Envelope, ServerMsg};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{
    Assignment, AssignmentBundle, AssignmentListing, ListAssignmentsRequest,
    WatchAssignmentsRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

const UPSTREAM_RETRY_MAX: usize = 3;
const UPSTREAM_RETRY_BACKOFF_MS: u64 = 50;
const MAX_FRAME_LEN: usize = 1_000_000;
const MAX_PENDING_PINGS: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CellKey(CellId);

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkerRoute {
    worker_id: String,
    addr: String,
}

#[derive(Clone)]
struct RoutingTable {
    routes: Arc<RwLock<HashMap<CellKey, WorkerRoute>>>,
    version: Arc<AtomicU64>,
}

impl RoutingTable {
    fn new(initial: HashMap<CellKey, WorkerRoute>) -> Self {
        Self {
            routes: Arc::new(RwLock::new(initial)),
            version: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn lookup(&self, cell: &CellId) -> Option<WorkerRoute> {
        let guard = self.routes.read().await;
        guard.get(&CellKey(*cell)).cloned()
    }

    async fn len(&self) -> usize {
        self.routes.read().await.len()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let addr_raw =
        std::env::var("TESSERA_GW_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let addr = resolve_socket_addr(&addr_raw)
        .await
        .with_context(|| format!("resolve TESSERA_GW_ADDR={addr_raw}"))?;

    let initial_routes = load_initial_routes().await?;
    let table = RoutingTable::new(initial_routes);
    {
        let routing = table.clone();
        tokio::spawn(async move {
            routing_refresh_loop(routing).await;
        });
    }
    {
        let routing = table.clone();
        tokio::spawn(async move {
            routing_watch_loop(routing).await;
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
    let (mut client_reader, mut client_writer) = stream.into_split();
    let mut upstream: Option<UpstreamConn> = None;
    let mut last_cell: Option<CellId> = None;
    // Ping은 응답(Pong)이 있어 재전송이 비교적 안전하다. 업스트림 커넥션이 끊기면
    // 아직 Pong을 못 받은 Ping을 재전송해, close 경쟁으로 인한 유실을 완화한다.
    let mut pending_pings: VecDeque<(u64, Bytes)> = VecDeque::new();

    let mut client_buf = BytesMut::with_capacity(8 * 1024);

    'conn_loop: loop {
        tokio::select! {
            // 업스트림 EOF/프레임을 우선 처리해 stale 커넥션에 쓰는 윈도우를 줄인다.
            biased;

            read = async {
                let conn = upstream.as_mut().expect("upstream present");
                conn.reader.read_buf(&mut conn.read_buf).await
            }, if upstream.is_some() => {
                match read {
                    Ok(0) => {
                        let Some(mut conn) = upstream.take() else { continue };
                        warn!(
                            target: "gateway",
                            %peer,
                            worker = %conn.route.worker_id,
                            addr = %conn.route.addr,
                            "upstream closed connection; attempting reconnect"
                        );
                        conn.close().await;
                        let Some(cell) = last_cell else {
                            warn!(
                                target: "gateway",
                                %peer,
                                "upstream closed before first client frame; closing connection"
                            );
                            break Ok(());
                        };
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
                                    "no route available while reconnecting"
                                );
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    warn!(target: "gateway", %peer, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            };
                            if let Err(e) = ensure_upstream(&mut upstream, route.clone(), peer).await {
                                warn!(
                                    target: "gateway",
                                    %peer,
                                    worker = %route.worker_id,
                                    addr = %route.addr,
                                    attempt,
                                    max_attempts = UPSTREAM_RETRY_MAX,
                                    error = ?e,
                                    "failed to reconnect to worker; retrying"
                                );
                                upstream = None;
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    warn!(target: "gateway", %peer, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            }
                            if !pending_pings.is_empty() {
                                let conn = upstream.as_mut().expect("upstream to be established");
                                let mut replay_failed = None;
                                for (ts, frame) in pending_pings.iter() {
                                    if let Err(e) = conn.writer.write_all(frame).await {
                                        replay_failed = Some((ts, e));
                                        break;
                                    }
                                }
                                if let Some((ts, e)) = replay_failed {
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        worker = %route.worker_id,
                                        addr = %route.addr,
                                        ts,
                                        error = ?e,
                                        "failed replaying pending ping after reconnect; retrying"
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
                            }
                            break;
                        }
                    }
                    Ok(_) => {
                        let conn = upstream.as_mut().expect("upstream present");
                        loop {
                            match try_take_frame(&mut conn.read_buf) {
                                Ok(Some(frame)) => {
                                    if !pending_pings.is_empty() {
                                        let payload = &frame[4..];
                                        if let Ok(env_out) =
                                            serde_json::from_slice::<Envelope<ServerMsg>>(payload)
                                            && let ServerMsg::Pong { ts } = env_out.payload
                                        {
                                            pending_pings
                                                .retain(|(pending_ts, _)| *pending_ts != ts);
                                        }
                                    }
                                    if let Err(e) = client_writer.write_all(&frame).await {
                                        warn!(
                                            target: "gateway",
                                            %peer,
                                            worker = %conn.route.worker_id,
                                            error = ?e,
                                            "failed to write reply frame to client; closing connection"
                                        );
                                        break 'conn_loop Ok(());
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        worker = %conn.route.worker_id,
                                        error = ?e,
                                        "invalid upstream frame; closing connection"
                                    );
                                    break 'conn_loop Ok(());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let Some(mut conn) = upstream.take() else { continue };
                        warn!(
                            target: "gateway",
                            %peer,
                            worker = %conn.route.worker_id,
                            addr = %conn.route.addr,
                            error = ?e,
                            "failed reading from upstream; attempting reconnect"
                        );
                        conn.close().await;
                        let Some(cell) = last_cell else {
                            warn!(
                                target: "gateway",
                                %peer,
                                "upstream closed before first client frame; closing connection"
                            );
                            break Ok(());
                        };
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
                                    "no route available while reconnecting"
                                );
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    warn!(target: "gateway", %peer, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            };
                            if let Err(e) = ensure_upstream(&mut upstream, route.clone(), peer).await {
                                warn!(
                                    target: "gateway",
                                    %peer,
                                    worker = %route.worker_id,
                                    addr = %route.addr,
                                    attempt,
                                    max_attempts = UPSTREAM_RETRY_MAX,
                                    error = ?e,
                                    "failed to reconnect to worker; retrying"
                                );
                                upstream = None;
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    warn!(target: "gateway", %peer, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            }
                            if !pending_pings.is_empty() {
                                let conn = upstream.as_mut().expect("upstream to be established");
                                let mut replay_failed = None;
                                for (ts, frame) in pending_pings.iter() {
                                    if let Err(e) = conn.writer.write_all(frame).await {
                                        replay_failed = Some((ts, e));
                                        break;
                                    }
                                }
                                if let Some((ts, e)) = replay_failed {
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        worker = %route.worker_id,
                                        addr = %route.addr,
                                        ts,
                                        error = ?e,
                                        "failed replaying pending ping after reconnect; retrying"
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
                            }
                            break;
                        }
                    }
                }
            }

            read = client_reader.read_buf(&mut client_buf) => {
                match read {
                    Ok(0) => {
                        info!(target: "gateway", %peer, "closed");
                        break Ok(());
                    }
                    Ok(_) => {
                        loop {
                            let frame = match try_take_frame(&mut client_buf) {
                                Ok(Some(frame)) => frame.freeze(),
                                Ok(None) => break,
                                Err(e) => {
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        error = ?e,
                                        "invalid frame; closing client connection"
                                    );
                                    break 'conn_loop Ok(());
                                }
                            };

                            let payload = &frame[4..];
                            let env_in: Envelope<ClientMsg> = match serde_json::from_slice(payload) {
                                Ok(env_in) => env_in,
                                Err(e) => {
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        error = ?e,
                                        "failed to decode frame for routing; closing client connection"
                                    );
                                    break 'conn_loop Ok(());
                                }
                            };
                            let cell = env_in.cell;
                            let ping_ts = match env_in.payload {
                                ClientMsg::Ping { ts } => Some(ts),
                                _ => None,
                            };
                            last_cell = Some(cell);

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

                                if let Err(e) = ensure_upstream(&mut upstream, route.clone(), peer).await {
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
                                if let Err(e) = conn.writer.write_all(&frame).await {
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
                                if let Some(ts) = ping_ts {
                                    // 동일 ts가 중복으로 들어오면 최신 frame으로 치환한다.
                                    pending_pings.retain(|(pending_ts, _)| *pending_ts != ts);
                                    while pending_pings.len() >= MAX_PENDING_PINGS {
                                        pending_pings.pop_front();
                                    }
                                    pending_pings.push_back((ts, frame.clone()));
                                }
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                        ) {
                            info!(target: "gateway", %peer, "closed");
                            break Ok(());
                        }
                        warn!(
                            target: "gateway",
                            %peer,
                            error = ?e,
                            "failed reading from client; closing connection"
                        );
                        break Ok(());
                    }
                }
            }
        }
    }
}

struct UpstreamConn {
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    route: WorkerRoute,
    read_buf: BytesMut,
}

impl UpstreamConn {
    async fn close(&mut self) {
        let _ = self.writer.shutdown().await;
    }
}

async fn ensure_upstream(
    upstream: &mut Option<UpstreamConn>,
    target_worker: WorkerRoute,
    peer: SocketAddr,
) -> Result<()> {
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
        let new_conn = connect_upstream(target_worker.clone(), peer).await?;
        if upstream.replace(new_conn).is_some() {
            // old connection already closed above
        }
    }

    Ok(())
}

async fn connect_upstream(target_worker: WorkerRoute, peer: SocketAddr) -> Result<UpstreamConn> {
    let stream = TcpStream::connect(target_worker.addr.as_str()).await?;
    info!(
        target: "gateway",
        %peer,
        worker = %target_worker.worker_id,
        addr = %target_worker.addr,
        "connected to upstream worker"
    );
    let (reader, writer) = stream.into_split();
    Ok(UpstreamConn {
        reader,
        writer,
        route: target_worker,
        read_buf: BytesMut::with_capacity(8 * 1024),
    })
}

fn try_take_frame(buf: &mut BytesMut) -> Result<Option<BytesMut>> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(anyhow::anyhow!(
            "frame length {} exceeds max {}",
            len,
            MAX_FRAME_LEN
        ));
    }
    let Some(total_len) = len.checked_add(4) else {
        return Err(anyhow::anyhow!("frame length overflow"));
    };
    if buf.len() < total_len {
        return Ok(None);
    }
    Ok(Some(buf.split_to(total_len)))
}

fn populate_routes(
    routes: &mut HashMap<CellKey, WorkerRoute>,
    addr: &str,
    bundle: &AssignmentBundle,
) -> Result<()> {
    for assignment in &bundle.cells {
        let cell = assignment_to_cell(assignment)?;
        let key = CellKey(cell);
        if let Some(existing) = routes.get(&key) {
            return Err(anyhow::anyhow!(
                "cell {:?} already assigned to worker {} (new worker {})",
                cell,
                existing.worker_id,
                bundle.worker_id
            ));
        }
        routes.insert(
            key,
            WorkerRoute {
                worker_id: bundle.worker_id.clone(),
                addr: addr.to_string(),
            },
        );
    }
    Ok(())
}

fn default_routes() -> Result<HashMap<CellKey, WorkerRoute>> {
    let addr = fallback_worker_addr()?;
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

fn fallback_worker_addr() -> Result<String> {
    let advertise = std::env::var("TESSERA_WORKER_ADVERTISE_ADDR")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|val| !val.is_empty());
    let raw = advertise.unwrap_or_else(|| {
        std::env::var("TESSERA_WORKER_ADDR").unwrap_or_else(|_| "127.0.0.1:5001".to_string())
    });
    let addr = raw.trim().to_string();
    if addr.is_empty() {
        return Err(anyhow::anyhow!("fallback worker addr is empty"));
    }
    if let Ok(parsed) = addr.parse::<SocketAddr>()
        && parsed.ip().is_unspecified()
    {
        return Err(anyhow::anyhow!(
            "fallback worker addr {} is unspecified; set TESSERA_WORKER_ADVERTISE_ADDR",
            addr
        ));
    }
    validate_addr_with_port(&addr, "fallback worker addr")?;
    Ok(addr)
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

async fn resolve_socket_addr(raw: &str) -> Result<SocketAddr> {
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }
    let mut addrs = tokio::net::lookup_host(raw)
        .await
        .with_context(|| format!("lookup host for {raw}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no socket address resolved for {raw}"))
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

async fn routing_refresh_loop(routing: RoutingTable) {
    let mut ticker = time::interval(routing_refresh_interval());
    loop {
        ticker.tick().await;
        let start_version = routing.version.load(Ordering::Relaxed);
        match fetch_remote_routes().await {
            Ok(new_routes) => {
                // Avoid overwriting newer watch updates with a stale refresh snapshot.
                match try_apply_refresh(&routing, start_version, new_routes).await {
                    RefreshOutcome::Applied { new_len } => {
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
                    }
                    RefreshOutcome::Unchanged => {
                        debug!(target: "gateway", "routing table unchanged after refresh");
                    }
                    RefreshOutcome::SkippedStale { current_version } => {
                        debug!(
                            target: "gateway",
                            start_version,
                            current_version,
                            "routing changed during refresh; skipping stale update"
                        );
                    }
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

async fn routing_watch_loop(routing: RoutingTable) {
    loop {
        match watch_assignments_once(routing.clone()).await {
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

async fn watch_assignments_once(routing: RoutingTable) -> Result<()> {
    let endpoint = orchestrator_endpoint();
    let mut client = OrchestratorClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connect orchestrator at {}", endpoint))?;
    let response = client
        .watch_assignments(WatchAssignmentsRequest {})
        .await
        .context("subscribe to watch_assignments")?;
    let stream = response.into_inner();
    apply_listing_stream(routing, stream)
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

async fn apply_listing_update(routing: &RoutingTable, listing: AssignmentListing) -> Result<bool> {
    let new_routes = routes_from_listing(&listing)?;
    let mut guard = routing.routes.write().await;
    if *guard == new_routes {
        debug!(target: "gateway", "assignment listing unchanged; skipping route swap");
        return Ok(false);
    }
    let new_len = new_routes.len();
    *guard = new_routes;
    routing.version.fetch_add(1, Ordering::Relaxed);
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

async fn apply_listing_stream<S>(routing: RoutingTable, mut stream: S) -> Result<()>
where
    S: tokio_stream::Stream<Item = Result<AssignmentListing, tonic::Status>> + Unpin,
{
    while let Some(listing) = stream.next().await {
        let listing = listing.context("receive assignment listing from watch stream")?;
        let _ = apply_listing_update(&routing, listing).await?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum RefreshOutcome {
    Applied { new_len: usize },
    Unchanged,
    SkippedStale { current_version: u64 },
}

async fn try_apply_refresh(
    routing: &RoutingTable,
    expected_version: u64,
    new_routes: HashMap<CellKey, WorkerRoute>,
) -> RefreshOutcome {
    let mut guard = routing.routes.write().await;
    let current_version = routing.version.load(Ordering::Relaxed);
    if current_version != expected_version {
        return RefreshOutcome::SkippedStale { current_version };
    }
    if *guard == new_routes {
        return RefreshOutcome::Unchanged;
    }
    let new_len = new_routes.len();
    *guard = new_routes;
    routing.version.fetch_add(1, Ordering::Relaxed);
    RefreshOutcome::Applied { new_len }
}

fn routes_from_listing(listing: &AssignmentListing) -> Result<HashMap<CellKey, WorkerRoute>> {
    let mut routes = HashMap::new();
    for bundle in &listing.workers {
        let addr = bundle.addr.trim();
        if addr.is_empty() {
            return Err(anyhow::anyhow!(
                "worker addr empty for {}",
                bundle.worker_id
            ));
        }
        if let Ok(parsed) = addr.parse::<SocketAddr>()
            && parsed.ip().is_unspecified()
        {
            return Err(anyhow::anyhow!(
                "worker addr {} is unspecified for {}",
                addr,
                bundle.worker_id
            ));
        }
        let label = format!("worker addr for {}", bundle.worker_id);
        validate_addr_with_port(addr, &label)?;
        populate_routes(&mut routes, addr, bundle)?;
    }
    Ok(routes)
}

fn validate_addr_with_port(addr: &str, label: &str) -> Result<()> {
    if let Ok(parsed) = addr.parse::<SocketAddr>() {
        if parsed.port() == 0 {
            return Err(anyhow!("{label} port must not be 0"));
        }
        return Ok(());
    }

    if let Some(rest) = addr.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(anyhow!("{label} must use [addr]:port for IPv6"));
        };
        let host = &rest[..end];
        if host.is_empty() {
            return Err(anyhow!("{label} host must not be empty"));
        }
        let port_str = rest[end + 1..]
            .strip_prefix(':')
            .ok_or_else(|| anyhow!("{label} must include a port"))?;
        let port: u16 = port_str
            .parse()
            .map_err(|_| anyhow!("{label} port must be numeric"))?;
        if port == 0 {
            return Err(anyhow!("{label} port must not be 0"));
        }
        return Ok(());
    }

    let Some((host, port_str)) = addr.rsplit_once(':') else {
        return Err(anyhow!("{label} must include a port"));
    };
    if host.is_empty() {
        return Err(anyhow!("{label} host must not be empty"));
    }
    if host.contains(':') {
        return Err(anyhow!("{label} must use [addr]:port for IPv6"));
    }
    let port: u16 = port_str
        .parse()
        .map_err(|_| anyhow!("{label} port must be numeric"))?;
    if port == 0 {
        return Err(anyhow!("{label} port must not be 0"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;
    use tessera_core::{ActorState, EntityId, Position, ServerMsg, encode_frame};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tokio_stream::iter;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvGuard {
        advertise: Option<String>,
        worker: Option<String>,
    }

    impl EnvGuard {
        fn capture() -> Self {
            Self {
                advertise: std::env::var("TESSERA_WORKER_ADVERTISE_ADDR").ok(),
                worker: std::env::var("TESSERA_WORKER_ADDR").ok(),
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.advertise {
                    Some(value) => std::env::set_var("TESSERA_WORKER_ADVERTISE_ADDR", value),
                    None => std::env::remove_var("TESSERA_WORKER_ADVERTISE_ADDR"),
                }
                match &self.worker {
                    Some(value) => std::env::set_var("TESSERA_WORKER_ADDR", value),
                    None => std::env::remove_var("TESSERA_WORKER_ADDR"),
                }
            }
        }
    }

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
        let routing = RoutingTable::new(HashMap::new());
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
        apply_listing_stream(routing.clone(), stream)
            .await
            .expect("process listing stream");

        let guard = routing.routes.read().await;
        let route = guard
            .get(&CellKey(CellId::grid(0, 0, 0)))
            .expect("route present");
        assert_eq!(route.worker_id, "worker-b");
        assert_eq!(route.addr, "127.0.0.1:5002");
    }

    #[tokio::test]
    async fn refresh_skips_stale_snapshot_after_watch_update() {
        let mut initial = HashMap::new();
        initial.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
            },
        );
        let routing = RoutingTable::new(initial);

        let start_version = routing.version.load(Ordering::Relaxed);
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
        apply_listing_update(&routing, listing_updated)
            .await
            .expect("apply watch update");

        let mut stale_routes = HashMap::new();
        stale_routes.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-c".to_string(),
                addr: "127.0.0.1:5003".to_string(),
            },
        );
        let outcome = try_apply_refresh(&routing, start_version, stale_routes).await;
        assert!(matches!(outcome, RefreshOutcome::SkippedStale { .. }));

        let guard = routing.routes.read().await;
        let route = guard
            .get(&CellKey(CellId::grid(0, 0, 0)))
            .expect("route present");
        assert_eq!(route.worker_id, "worker-b");
    }

    #[test]
    fn listing_accepts_hostname_addrs() {
        let listing = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-a".to_string(),
                addr: "worker-a:5001".to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 0,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };

        let routes = routes_from_listing(&listing).expect("parse listing");
        let route = routes
            .get(&CellKey(CellId::grid(0, 0, 0)))
            .expect("route present");
        assert_eq!(route.addr, "worker-a:5001");
    }

    #[test]
    fn routes_from_listing_rejects_empty_addr() {
        let listing = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-a".to_string(),
                addr: "   ".to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 0,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };

        let err = routes_from_listing(&listing).expect_err("should reject empty addr");
        assert!(err.to_string().contains("worker addr empty"));
    }

    #[test]
    fn routes_from_listing_rejects_missing_port() {
        let listing = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-a".to_string(),
                addr: "worker-a".to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 0,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };

        let err = routes_from_listing(&listing).expect_err("should reject missing port");
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn routes_from_listing_rejects_zero_port() {
        let listing = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:0".to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 0,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };

        let err = routes_from_listing(&listing).expect_err("should reject zero port");
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn listing_rejects_duplicate_cells() {
        let listing = AssignmentListing {
            workers: vec![
                AssignmentBundle {
                    worker_id: "worker-a".to_string(),
                    addr: "127.0.0.1:5001".to_string(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 0,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
                AssignmentBundle {
                    worker_id: "worker-b".to_string(),
                    addr: "127.0.0.1:5002".to_string(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 0,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
            ],
        };

        let err = routes_from_listing(&listing).expect_err("should reject duplicates");
        assert!(err.to_string().contains("already assigned"));
    }

    #[test]
    fn default_routes_prefers_advertise_addr() {
        let _lock = ENV_LOCK.lock().expect("lock env");
        let _env = EnvGuard::capture();

        unsafe {
            std::env::set_var("TESSERA_WORKER_ADVERTISE_ADDR", "127.0.0.1:5999");
            std::env::set_var("TESSERA_WORKER_ADDR", "0.0.0.0:5001");
        }

        let routes = default_routes().expect("default routes");
        let route = routes
            .get(&CellKey(CellId::grid(0, 0, 0)))
            .expect("route present");
        assert_eq!(route.addr, "127.0.0.1:5999");
    }

    #[test]
    fn default_routes_rejects_unspecified_addr() {
        let _lock = ENV_LOCK.lock().expect("lock env");
        let _env = EnvGuard::capture();

        unsafe {
            std::env::remove_var("TESSERA_WORKER_ADVERTISE_ADDR");
            std::env::set_var("TESSERA_WORKER_ADDR", "0.0.0.0:5001");
        }

        let err = default_routes().expect_err("should reject unspecified addr");
        assert!(err.to_string().contains("fallback worker addr"));
    }

    #[test]
    fn fallback_worker_addr_uses_worker_when_advertise_empty() {
        let _lock = ENV_LOCK.lock().expect("lock env");
        let _env = EnvGuard::capture();

        unsafe {
            std::env::set_var("TESSERA_WORKER_ADVERTISE_ADDR", "   ");
            std::env::set_var("TESSERA_WORKER_ADDR", "127.0.0.1:5111");
        }

        let addr = fallback_worker_addr().expect("fallback addr");
        assert_eq!(addr, "127.0.0.1:5111");
    }

    #[test]
    fn fallback_worker_addr_rejects_missing_port() {
        let _lock = ENV_LOCK.lock().expect("lock env");
        let _env = EnvGuard::capture();

        unsafe {
            std::env::remove_var("TESSERA_WORKER_ADVERTISE_ADDR");
            std::env::set_var("TESSERA_WORKER_ADDR", "worker-a");
        }

        let err = fallback_worker_addr().expect_err("should reject missing port");
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn fallback_worker_addr_rejects_zero_port() {
        let _lock = ENV_LOCK.lock().expect("lock env");
        let _env = EnvGuard::capture();

        unsafe {
            std::env::remove_var("TESSERA_WORKER_ADVERTISE_ADDR");
            std::env::set_var("TESSERA_WORKER_ADDR", "127.0.0.1:0");
        }

        let err = fallback_worker_addr().expect_err("should reject zero port");
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn assignment_to_cell_rejects_depth_overflow() {
        let assignment = Assignment {
            world: 0,
            cx: 0,
            cy: 0,
            depth: 300,
            sub: 0,
        };
        let err = assignment_to_cell(&assignment).expect_err("should reject depth");
        assert!(err.to_string().contains("depth"));
    }

    #[test]
    fn assignment_to_cell_rejects_sub_overflow() {
        let assignment = Assignment {
            world: 0,
            cx: 0,
            cy: 0,
            depth: 0,
            sub: 300,
        };
        let err = assignment_to_cell(&assignment).expect_err("should reject sub");
        assert!(err.to_string().contains("sub"));
    }

    #[tokio::test]
    async fn apply_listing_update_noop_when_same() {
        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
            },
        );
        let routing = RoutingTable::new(map);

        let listing = AssignmentListing {
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

        let start_version = routing.version.load(Ordering::Relaxed);
        let changed = apply_listing_update(&routing, listing)
            .await
            .expect("apply listing");
        let end_version = routing.version.load(Ordering::Relaxed);

        assert!(!changed);
        assert_eq!(start_version, end_version);
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
                addr: unused_addr.to_string(),
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
    async fn route_change_reconnects_to_new_worker() {
        let worker_a_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker a");
        let worker_a_addr = worker_a_listener.local_addr().expect("worker a addr");
        let worker_b_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker b");
        let worker_b_addr = worker_b_listener.local_addr().expect("worker b addr");

        let (a_tx, a_rx) = oneshot::channel();
        let (b_tx, b_rx) = oneshot::channel();
        let mut worker_a_task =
            tokio::spawn(run_ping_worker_expect_close(worker_a_listener, 1, a_tx));
        let mut worker_b_task = tokio::spawn(run_ping_worker_once(worker_b_listener, 2, b_tx));

        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
                addr: worker_a_addr.to_string(),
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

        let ping1 = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 1 },
        };
        client
            .write_all(&encode_frame(&ping1))
            .await
            .expect("send first ping");
        let reply1 = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply1.payload, ServerMsg::Pong { ts } if ts == 1));

        let listing_updated = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-b".to_string(),
                addr: worker_b_addr.to_string(),
                cells: vec![Assignment {
                    world: 0,
                    cx: 0,
                    cy: 0,
                    depth: 0,
                    sub: 0,
                }],
            }],
        };
        apply_listing_update(&routing, listing_updated)
            .await
            .expect("apply listing update");

        let ping2 = Envelope {
            cell,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 2 },
        };
        client
            .write_all(&encode_frame(&ping2))
            .await
            .expect("send second ping");
        let reply2 = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply2.payload, ServerMsg::Pong { ts } if ts == 2));

        drop(client);

        timeout(Duration::from_millis(500), a_rx)
            .await
            .expect("worker a timeout")
            .expect("worker a ack");
        timeout(Duration::from_millis(500), b_rx)
            .await
            .expect("worker b timeout")
            .expect("worker b ack");

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker-a", &mut worker_a_task).await;
        wait_task("worker-b", &mut worker_b_task).await;
    }

    #[tokio::test]
    async fn upstream_disconnect_allows_reconnect() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");

        let mut worker_task = tokio::spawn(async move {
            run_ping_worker_once_per_conn(worker_listener, 2).await;
        });

        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-test".to_string(),
                addr: worker_addr.to_string(),
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

        let ping1 = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 1 },
        };
        client
            .write_all(&encode_frame(&ping1))
            .await
            .expect("send first ping");
        let reply1 = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply1.payload, ServerMsg::Pong { ts } if ts == 1));

        let ping2 = Envelope {
            cell,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 2 },
        };
        client
            .write_all(&encode_frame(&ping2))
            .await
            .expect("send second ping");
        let reply2 = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply2.payload, ServerMsg::Pong { ts } if ts == 2));

        drop(client);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    #[tokio::test]
    async fn upstream_close_during_partial_frame_keeps_client_bytes() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");
        let (close_tx, close_rx) = oneshot::channel();

        let mut worker_task =
            tokio::spawn(run_ping_worker_with_close_signal(worker_listener, close_rx));

        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-test".to_string(),
                addr: worker_addr.to_string(),
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

        let ping1 = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 1 },
        };
        let ping2 = Envelope {
            cell,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 2 },
        };
        let frame1 = encode_frame(&ping1);
        let frame2 = encode_frame(&ping2);

        client.write_all(&frame1).await.expect("send first ping");
        client
            .write_all(&frame2[..1])
            .await
            .expect("send partial second ping");
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = close_tx.send(());
        client
            .write_all(&frame2[1..])
            .await
            .expect("send remaining second ping");

        let reply1 = read_env_with_timeout(&mut client, Duration::from_secs(1)).await;
        assert!(matches!(reply1.payload, ServerMsg::Pong { ts } if ts == 1));
        let reply2 = read_env_with_timeout(&mut client, Duration::from_secs(1)).await;
        assert!(matches!(reply2.payload, ServerMsg::Pong { ts } if ts == 2));

        drop(client);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    #[tokio::test]
    async fn upstream_disconnect_reconnects_while_idle() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");

        let (done_tx, done_rx) = oneshot::channel();
        let mut worker_task =
            tokio::spawn(run_ping_then_push_worker(worker_listener, 1, 99, done_tx));

        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-test".to_string(),
                addr: worker_addr.to_string(),
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

        let ping = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 1 },
        };
        client
            .write_all(&encode_frame(&ping))
            .await
            .expect("send ping");
        let reply = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply.payload, ServerMsg::Pong { ts } if ts == 1));

        let push = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(push.payload, ServerMsg::Pong { ts } if ts == 99));

        timeout(Duration::from_millis(500), done_rx)
            .await
            .expect("worker timeout")
            .expect("worker ack");

        drop(client);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
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
                addr: worker_addr.to_string(),
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
        let routing = RoutingTable::new(HashMap::new());
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
        apply_listing_stream(routing.clone(), stream)
            .await
            .expect("process listing stream");

        let guard = routing.routes.read().await;
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

    async fn run_ping_worker_once_per_conn(listener: TcpListener, expected: usize) {
        for _ in 0..expected {
            let (mut socket, _peer) = listener.accept().await.expect("accept worker");
            let mut len_buf = [0u8; 4];
            socket.read_exact(&mut len_buf).await.expect("read len");
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            socket.read_exact(&mut payload).await.expect("read payload");
            let env_in: Envelope<ClientMsg> =
                serde_json::from_slice(&payload).expect("decode client frame");
            let ts = match env_in.payload {
                ClientMsg::Ping { ts } => ts,
                other => panic!("expected ping, got {other:?}"),
            };
            let env_out = Envelope {
                cell: env_in.cell,
                seq: 0,
                epoch: env_in.epoch,
                payload: ServerMsg::Pong { ts },
            };
            let frame = encode_frame(&env_out);
            socket.write_all(&frame).await.expect("write reply");
        }
    }

    async fn run_ping_worker_with_close_signal(
        listener: TcpListener,
        close_rx: oneshot::Receiver<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_in = read_client_env(&mut socket).await;
        let ts = match env_in.payload {
            ClientMsg::Ping { ts } => ts,
            other => panic!("expected ping, got {other:?}"),
        };
        let env_out = Envelope {
            cell: env_in.cell,
            seq: 0,
            epoch: env_in.epoch,
            payload: ServerMsg::Pong { ts },
        };
        let frame = encode_frame(&env_out);
        socket.write_all(&frame).await.expect("write reply");
        let _ = close_rx.await;
        let _ = socket.shutdown().await;
        drop(socket);

        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_in = read_client_env(&mut socket).await;
        let ts = match env_in.payload {
            ClientMsg::Ping { ts } => ts,
            other => panic!("expected ping, got {other:?}"),
        };
        let env_out = Envelope {
            cell: env_in.cell,
            seq: 0,
            epoch: env_in.epoch,
            payload: ServerMsg::Pong { ts },
        };
        let frame = encode_frame(&env_out);
        socket.write_all(&frame).await.expect("write reply");
    }

    async fn run_ping_worker_once(
        listener: TcpListener,
        expected_ts: u64,
        done: oneshot::Sender<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await.expect("read len");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        socket.read_exact(&mut payload).await.expect("read payload");
        let env_in: Envelope<ClientMsg> =
            serde_json::from_slice(&payload).expect("decode client frame");
        let cell = env_in.cell;
        let epoch = env_in.epoch;
        let ts = match env_in.payload {
            ClientMsg::Ping { ts } => ts,
            other => panic!("expected ping, got {other:?}"),
        };
        assert_eq!(ts, expected_ts);
        let env_out = Envelope {
            cell,
            seq: 0,
            epoch,
            payload: ServerMsg::Pong { ts },
        };
        let frame = encode_frame(&env_out);
        socket.write_all(&frame).await.expect("write reply");
        let _ = done.send(());
    }

    async fn run_ping_then_push_worker(
        listener: TcpListener,
        expected_ts: u64,
        push_ts: u64,
        done: oneshot::Sender<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await.expect("read len");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        socket.read_exact(&mut payload).await.expect("read payload");
        let env_in: Envelope<ClientMsg> =
            serde_json::from_slice(&payload).expect("decode client frame");
        let cell = env_in.cell;
        let epoch = env_in.epoch;
        let ts = match env_in.payload {
            ClientMsg::Ping { ts } => ts,
            other => panic!("expected ping, got {other:?}"),
        };
        assert_eq!(ts, expected_ts);
        let env_out = Envelope {
            cell,
            seq: 0,
            epoch,
            payload: ServerMsg::Pong { ts },
        };
        let frame = encode_frame(&env_out);
        socket.write_all(&frame).await.expect("write reply");
        let _ = socket.shutdown().await;
        drop(socket);

        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_out = Envelope {
            cell,
            seq: 0,
            epoch,
            payload: ServerMsg::Pong { ts: push_ts },
        };
        let frame = encode_frame(&env_out);
        socket.write_all(&frame).await.expect("write push");
        let _ = done.send(());
    }

    async fn run_ping_worker_expect_close(
        listener: TcpListener,
        expected_ts: u64,
        done: oneshot::Sender<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await.expect("read len");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        socket.read_exact(&mut payload).await.expect("read payload");
        let env_in: Envelope<ClientMsg> =
            serde_json::from_slice(&payload).expect("decode client frame");
        let ts = match env_in.payload {
            ClientMsg::Ping { ts } => ts,
            other => panic!("expected ping, got {other:?}"),
        };
        assert_eq!(ts, expected_ts);
        let env_out = Envelope {
            cell: env_in.cell,
            seq: 0,
            epoch: env_in.epoch,
            payload: ServerMsg::Pong { ts },
        };
        let frame = encode_frame(&env_out);
        socket.write_all(&frame).await.expect("write reply");
        let _ = done.send(());

        let mut next_len = [0u8; 4];
        let read = timeout(Duration::from_millis(500), socket.read_exact(&mut next_len)).await;
        match read {
            Ok(Ok(_)) => panic!("unexpected second frame on worker a"),
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) => {}
            Ok(Err(e)) => panic!("unexpected worker read error: {e:?}"),
            Err(_) => panic!("worker did not observe gateway close"),
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

    async fn read_client_env(stream: &mut TcpStream) -> Envelope<ClientMsg> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.expect("read len");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.expect("read payload");
        serde_json::from_slice::<Envelope<ClientMsg>>(&payload).expect("decode client frame")
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
