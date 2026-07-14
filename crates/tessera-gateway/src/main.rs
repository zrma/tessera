use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tessera_core::{
    CellId, ClientEnvelope, ClientMsg, MAX_FRAME_LEN, ServerEnvelope, ServerMsg, encode_frame,
};
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
const MAX_PENDING_PINGS: usize = 32;
const MAX_PENDING_REQUESTS: usize = 128;
const GATEWAY_PING_RTT_BUCKETS_MICROS: [u64; 11] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000,
];
const GATEWAY_PING_RTT_BUCKET_LABELS: [&str; 11] = [
    "0.001", "0.005", "0.010", "0.025", "0.050", "0.100", "0.250", "0.500", "1.000", "2.500",
    "5.000",
];
static NEXT_GATEWAY_SESSION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_GATEWAY_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Default)]
struct GatewayMetricsCounters {
    accepted_connections: AtomicU64,
    upstream_connect_attempts: AtomicU64,
    upstream_connects: AtomicU64,
    upstream_connect_failures: AtomicU64,
    upstream_route_changes: AtomicU64,
    upstream_route_change_reconnects: AtomicU64,
    upstream_reconnect_attempts: AtomicU64,
    upstream_closed: AtomicU64,
    upstream_read_errors: AtomicU64,
    upstream_write_errors: AtomicU64,
    invalid_client_frames: AtomicU64,
    no_route_lookup_failures: AtomicU64,
    pending_ping_replays: AtomicU64,
    pending_ping_tracking_evictions: AtomicU64,
    pending_request_tracking_evictions: AtomicU64,
    client_closes_no_route: AtomicU64,
    client_closes_upstream_retry_exhausted: AtomicU64,
    client_closes_pending_ping_route_change: AtomicU64,
    client_closes_ambiguous_upstream: AtomicU64,
    ping_roundtrip_latency: GatewayLatencyHistogram,
    join_roundtrip_latency: GatewayLatencyHistogram,
    move_roundtrip_latency: GatewayLatencyHistogram,
}

#[derive(Debug)]
struct GatewayLatencyHistogram {
    buckets: [AtomicU64; GATEWAY_PING_RTT_BUCKETS_MICROS.len() + 1],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Default for GatewayLatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::default()),
            sum_micros: AtomicU64::default(),
            count: AtomicU64::default(),
        }
    }
}

impl GatewayLatencyHistogram {
    fn observe(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = GATEWAY_PING_RTT_BUCKETS_MICROS
            .iter()
            .position(|boundary| micros <= *boundary)
            .unwrap_or(GATEWAY_PING_RTT_BUCKETS_MICROS.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> GatewayLatencySnapshot {
        GatewayLatencySnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            sum_micros: self.sum_micros.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct GatewayLatencySnapshot {
    buckets: Vec<u64>,
    sum_micros: u64,
    count: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct GatewayMetricsSnapshot {
    ready: u64,
    routes: u64,
    routing_version: u64,
    accepted_connections: u64,
    upstream_connect_attempts: u64,
    upstream_connects: u64,
    upstream_connect_failures: u64,
    upstream_route_changes: u64,
    upstream_route_change_reconnects: u64,
    upstream_reconnect_attempts: u64,
    upstream_closed: u64,
    upstream_read_errors: u64,
    upstream_write_errors: u64,
    invalid_client_frames: u64,
    no_route_lookup_failures: u64,
    pending_ping_replays: u64,
    pending_ping_tracking_evictions: u64,
    pending_request_tracking_evictions: u64,
    client_closes_no_route: u64,
    client_closes_upstream_retry_exhausted: u64,
    client_closes_pending_ping_route_change: u64,
    client_closes_ambiguous_upstream: u64,
    ping_roundtrip_latency: GatewayLatencySnapshot,
    join_roundtrip_latency: GatewayLatencySnapshot,
    move_roundtrip_latency: GatewayLatencySnapshot,
}

#[derive(Debug, PartialEq, Eq)]
struct GatewayReadinessSnapshot {
    ready: bool,
    routes: u64,
    routing_version: u64,
}

#[derive(Debug, Clone, Copy)]
enum GatewayClientCloseReason {
    NoRoute,
    UpstreamRetryExhausted,
    PendingPingRouteChange,
    AmbiguousUpstream,
}

impl GatewayClientCloseReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::NoRoute => "no_route",
            Self::UpstreamRetryExhausted => "upstream_retry_exhausted",
            Self::PendingPingRouteChange => "pending_ping_route_change",
            Self::AmbiguousUpstream => "ambiguous_upstream",
        }
    }
}

#[derive(Clone, Debug)]
struct PendingPing {
    cell: CellId,
    ts: u64,
    frame: Bytes,
    sent_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingRequestKind {
    Join,
    Move,
}

impl PendingRequestKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Move => "move",
        }
    }
}

#[derive(Clone, Debug)]
struct PendingRequest {
    cell: CellId,
    request_id: u64,
    kind: PendingRequestKind,
    sent_at: Instant,
}

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
    metrics: Arc<GatewayMetricsCounters>,
}

impl RoutingTable {
    fn new(initial: HashMap<CellKey, WorkerRoute>) -> Self {
        Self {
            routes: Arc::new(RwLock::new(initial)),
            version: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(GatewayMetricsCounters::default()),
        }
    }

    async fn lookup(&self, cell: &CellId) -> Option<WorkerRoute> {
        let guard = self.routes.read().await;
        guard.get(&CellKey(*cell)).cloned()
    }

    async fn len(&self) -> usize {
        self.routes.read().await.len()
    }

    async fn metrics_snapshot(&self) -> GatewayMetricsSnapshot {
        let routes = self.len().await as u64;
        GatewayMetricsSnapshot {
            ready: u64::from(routes > 0),
            routes,
            routing_version: self.version.load(Ordering::Relaxed),
            accepted_connections: self.metrics.accepted_connections.load(Ordering::Relaxed),
            upstream_connect_attempts: self
                .metrics
                .upstream_connect_attempts
                .load(Ordering::Relaxed),
            upstream_connects: self.metrics.upstream_connects.load(Ordering::Relaxed),
            upstream_connect_failures: self
                .metrics
                .upstream_connect_failures
                .load(Ordering::Relaxed),
            upstream_route_changes: self.metrics.upstream_route_changes.load(Ordering::Relaxed),
            upstream_route_change_reconnects: self
                .metrics
                .upstream_route_change_reconnects
                .load(Ordering::Relaxed),
            upstream_reconnect_attempts: self
                .metrics
                .upstream_reconnect_attempts
                .load(Ordering::Relaxed),
            upstream_closed: self.metrics.upstream_closed.load(Ordering::Relaxed),
            upstream_read_errors: self.metrics.upstream_read_errors.load(Ordering::Relaxed),
            upstream_write_errors: self.metrics.upstream_write_errors.load(Ordering::Relaxed),
            invalid_client_frames: self.metrics.invalid_client_frames.load(Ordering::Relaxed),
            no_route_lookup_failures: self
                .metrics
                .no_route_lookup_failures
                .load(Ordering::Relaxed),
            pending_ping_replays: self.metrics.pending_ping_replays.load(Ordering::Relaxed),
            pending_ping_tracking_evictions: self
                .metrics
                .pending_ping_tracking_evictions
                .load(Ordering::Relaxed),
            pending_request_tracking_evictions: self
                .metrics
                .pending_request_tracking_evictions
                .load(Ordering::Relaxed),
            client_closes_no_route: self.metrics.client_closes_no_route.load(Ordering::Relaxed),
            client_closes_upstream_retry_exhausted: self
                .metrics
                .client_closes_upstream_retry_exhausted
                .load(Ordering::Relaxed),
            client_closes_pending_ping_route_change: self
                .metrics
                .client_closes_pending_ping_route_change
                .load(Ordering::Relaxed),
            client_closes_ambiguous_upstream: self
                .metrics
                .client_closes_ambiguous_upstream
                .load(Ordering::Relaxed),
            ping_roundtrip_latency: self.metrics.ping_roundtrip_latency.snapshot(),
            join_roundtrip_latency: self.metrics.join_roundtrip_latency.snapshot(),
            move_roundtrip_latency: self.metrics.move_roundtrip_latency.snapshot(),
        }
    }

    async fn readiness_snapshot(&self) -> GatewayReadinessSnapshot {
        let routes = self.len().await as u64;
        GatewayReadinessSnapshot {
            ready: routes > 0,
            routes,
            routing_version: self.version.load(Ordering::Relaxed),
        }
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
    if let Some(listener) = load_metrics_listener().await? {
        let metrics_table = table.clone();
        let metrics_addr = listener
            .local_addr()
            .context("read gateway prometheus metrics listener addr")?;
        info!(target: "gateway", %metrics_addr, "tessera-gateway prometheus metrics listening");
        tokio::spawn(async move {
            if let Err(e) = serve_prometheus_metrics(listener, metrics_table).await {
                error!(target: "gateway", error = ?e, "prometheus metrics exporter exited with error");
            }
        });
    }
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
        routing
            .metrics
            .accepted_connections
            .fetch_add(1, Ordering::Relaxed);
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
    let session_id = NEXT_GATEWAY_SESSION_ID.fetch_add(1, Ordering::Relaxed);
    // Ping은 응답(Pong)이 있어 재전송이 비교적 안전하다. 업스트림 커넥션이 끊기면
    // 아직 Pong을 못 받은 Ping을 재전송해, close 경쟁으로 인한 유실을 완화한다.
    let mut pending_pings: VecDeque<PendingPing> = VecDeque::new();
    let mut pending_requests: VecDeque<PendingRequest> = VecDeque::new();

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
                        routing
                            .metrics
                            .upstream_closed
                            .fetch_add(1, Ordering::Relaxed);
                        if conn.has_non_ping_traffic
                            && pending_requests.is_empty()
                            && pending_pings.is_empty()
                        {
                            warn!(
                                target: "gateway",
                                %peer,
                                worker = %conn.route.worker_id,
                                addr = %conn.route.addr,
                                "upstream closed after completed non-ping traffic; keeping client connection open"
                            );
                            conn.close().await;
                            continue;
                        }
                        if conn.has_non_ping_traffic {
                            let close_reason = record_client_close(
                                routing.metrics.as_ref(),
                                GatewayClientCloseReason::AmbiguousUpstream,
                            );
                            warn!(
                                target: "gateway",
                                %peer,
                                worker = %conn.route.worker_id,
                                addr = %conn.route.addr,
                                close_reason,
                                "upstream closed after non-ping traffic; closing client to avoid dropping replies"
                            );
                            conn.close().await;
                            break Ok(());
                        }
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
                                routing
                                    .metrics
                                    .no_route_lookup_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    target: "gateway",
                                    %peer,
                                    cell = ?cell,
                                    attempt,
                                    max_attempts = UPSTREAM_RETRY_MAX,
                                    "no route available while reconnecting"
                                );
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    let close_reason = record_client_close(
                                        routing.metrics.as_ref(),
                                        GatewayClientCloseReason::NoRoute,
                                    );
                                    warn!(target: "gateway", %peer, close_reason, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            };
                            routing
                                .metrics
                                .upstream_reconnect_attempts
                                .fetch_add(1, Ordering::Relaxed);
                            if let Err(e) = ensure_upstream(
                                &mut upstream,
                                route.clone(),
                                peer,
                                routing.metrics.as_ref(),
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
                                    "failed to reconnect to worker; retrying"
                                );
                                upstream = None;
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    let close_reason = record_client_close(
                                        routing.metrics.as_ref(),
                                        GatewayClientCloseReason::UpstreamRetryExhausted,
                                    );
                                    warn!(target: "gateway", %peer, close_reason, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            }
                            if !pending_pings.is_empty() {
                                let conn = upstream.as_mut().expect("upstream to be established");
                                if let Err(e) =
                                    replay_pending_pings(conn, &routing, &pending_pings).await
                                {
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        worker = %route.worker_id,
                                        addr = %route.addr,
                                        error = ?e,
                                        "failed replaying pending ping after reconnect; retrying"
                                    );
                                    conn.close().await;
                                    upstream = None;
                                    if attempt >= UPSTREAM_RETRY_MAX {
                                        let close_reason = record_client_close(
                                            routing.metrics.as_ref(),
                                            GatewayClientCloseReason::UpstreamRetryExhausted,
                                        );
                                        warn!(target: "gateway", %peer, close_reason, "closing client connection");
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
                                    if !pending_pings.is_empty()
                                        || !pending_requests.is_empty()
                                    {
                                        let payload = &frame[4..];
                                        if let Ok(env_out) =
                                            serde_json::from_slice::<ServerEnvelope>(payload)
                                        {
                                            let ServerEnvelope {
                                                cell,
                                                request_id,
                                                payload,
                                                ..
                                            } = env_out;
                                            if let ServerMsg::Pong { ts } = payload
                                                && let Some(sent_at) = acknowledge_pending_ping(
                                                    &mut pending_pings,
                                                    cell,
                                                    ts,
                                                )
                                            {
                                                routing
                                                    .metrics
                                                    .ping_roundtrip_latency
                                                    .observe(sent_at.elapsed());
                                            }
                                            if let Some(request_id) = request_id
                                                && let Some((kind, sent_at)) =
                                                    acknowledge_pending_request(
                                                        &mut pending_requests,
                                                        cell,
                                                        request_id,
                                                    )
                                            {
                                                observe_request_latency(
                                                    routing.metrics.as_ref(),
                                                    kind,
                                                    sent_at.elapsed(),
                                                );
                                            }
                                        }
                                    }
                                    if let Err(e) = client_writer.write_all(&frame).await {
                                        routing
                                            .metrics
                                            .upstream_write_errors
                                            .fetch_add(1, Ordering::Relaxed);
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
                                    routing
                                        .metrics
                                        .invalid_client_frames
                                        .fetch_add(1, Ordering::Relaxed);
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
                        routing
                            .metrics
                            .upstream_read_errors
                            .fetch_add(1, Ordering::Relaxed);
                        if conn.has_non_ping_traffic
                            && pending_requests.is_empty()
                            && pending_pings.is_empty()
                        {
                            warn!(
                                target: "gateway",
                                %peer,
                                worker = %conn.route.worker_id,
                                addr = %conn.route.addr,
                                error = ?e,
                                "upstream read failed after completed non-ping traffic; keeping client connection open"
                            );
                            conn.close().await;
                            continue;
                        }
                        if conn.has_non_ping_traffic {
                            let close_reason = record_client_close(
                                routing.metrics.as_ref(),
                                GatewayClientCloseReason::AmbiguousUpstream,
                            );
                            warn!(
                                target: "gateway",
                                %peer,
                                worker = %conn.route.worker_id,
                                addr = %conn.route.addr,
                                error = ?e,
                                close_reason,
                                "failed reading from upstream after non-ping traffic; closing client to avoid dropping replies"
                            );
                            conn.close().await;
                            break Ok(());
                        }
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
                                routing
                                    .metrics
                                    .no_route_lookup_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    target: "gateway",
                                    %peer,
                                    cell = ?cell,
                                    attempt,
                                    max_attempts = UPSTREAM_RETRY_MAX,
                                    "no route available while reconnecting"
                                );
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    let close_reason = record_client_close(
                                        routing.metrics.as_ref(),
                                        GatewayClientCloseReason::NoRoute,
                                    );
                                    warn!(target: "gateway", %peer, close_reason, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            };
                            routing
                                .metrics
                                .upstream_reconnect_attempts
                                .fetch_add(1, Ordering::Relaxed);
                            if let Err(e) = ensure_upstream(
                                &mut upstream,
                                route.clone(),
                                peer,
                                routing.metrics.as_ref(),
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
                                    "failed to reconnect to worker; retrying"
                                );
                                upstream = None;
                                if attempt >= UPSTREAM_RETRY_MAX {
                                    let close_reason = record_client_close(
                                        routing.metrics.as_ref(),
                                        GatewayClientCloseReason::UpstreamRetryExhausted,
                                    );
                                    warn!(target: "gateway", %peer, close_reason, "closing client connection");
                                    break 'conn_loop Ok(());
                                }
                                time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                continue;
                            }
                            if !pending_pings.is_empty() {
                                let conn = upstream.as_mut().expect("upstream to be established");
                                if let Err(e) =
                                    replay_pending_pings(conn, &routing, &pending_pings).await
                                {
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        worker = %route.worker_id,
                                        addr = %route.addr,
                                        error = ?e,
                                        "failed replaying pending ping after reconnect; retrying"
                                    );
                                    conn.close().await;
                                    upstream = None;
                                    if attempt >= UPSTREAM_RETRY_MAX {
                                        let close_reason = record_client_close(
                                            routing.metrics.as_ref(),
                                            GatewayClientCloseReason::UpstreamRetryExhausted,
                                        );
                                        warn!(target: "gateway", %peer, close_reason, "closing client connection");
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
                            let raw_frame = match try_take_frame(&mut client_buf) {
                                Ok(Some(frame)) => frame.freeze(),
                                Ok(None) => break,
                                Err(e) => {
                                    routing
                                        .metrics
                                        .invalid_client_frames
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        error = ?e,
                                        "invalid frame; closing client connection"
                                    );
                                    break 'conn_loop Ok(());
                                }
                            };

                            let payload = &raw_frame[4..];
                            let mut env_in: ClientEnvelope = match serde_json::from_slice(payload) {
                                Ok(env_in) => env_in,
                                Err(e) => {
                                    routing
                                        .metrics
                                        .invalid_client_frames
                                        .fetch_add(1, Ordering::Relaxed);
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
                            let ping_ts = match &env_in.payload {
                                ClientMsg::Ping { ts } => Some(*ts),
                                _ => None,
                            };
                            let ping_sent_at = ping_ts.map(|_| Instant::now());
                            let request_kind = match &env_in.payload {
                                ClientMsg::Join { .. } => Some(PendingRequestKind::Join),
                                ClientMsg::Move { .. } => Some(PendingRequestKind::Move),
                                ClientMsg::Ping { .. } => None,
                            };
                            let request_id = request_kind.map(|_| {
                                NEXT_GATEWAY_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
                            });
                            let request_sent_at = request_id.map(|_| Instant::now());
                            env_in.session = Some(session_id);
                            env_in.request_id = request_id;
                            let frame = encode_frame(&env_in);
                            last_cell = Some(cell);

                            let mut attempt = 0usize;
                            loop {
                                attempt += 1;
                                let Some(route) = routing.lookup(&cell).await else {
                                    routing
                                        .metrics
                                        .no_route_lookup_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        cell = ?cell,
                                        attempt,
                                        max_attempts = UPSTREAM_RETRY_MAX,
                                        "no route available for cell"
                                    );
                                    if attempt >= UPSTREAM_RETRY_MAX {
                                        let close_reason = record_client_close(
                                            routing.metrics.as_ref(),
                                            GatewayClientCloseReason::NoRoute,
                                        );
                                        warn!(target: "gateway", %peer, cell = ?cell, close_reason, "closing client connection");
                                        break 'conn_loop Ok(());
                                    }
                                    time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                    continue;
                                };

                                if let Some(current) = upstream
                                    .as_ref()
                                    .map(|conn| (conn.route.clone(), conn.has_non_ping_traffic))
                                    && current.0.addr != route.addr
                                {
                                    if current.1 {
                                        warn!(
                                            target: "gateway",
                                            %peer,
                                            cell = ?cell,
                                            old_worker = %current.0.worker_id,
                                            new_worker = %route.worker_id,
                                            session_id,
                                            "route changed after non-ping traffic; reconnecting upstream with stable session"
                                        );
                                        routing
                                            .metrics
                                            .upstream_route_change_reconnects
                                            .fetch_add(1, Ordering::Relaxed);
                                        upstream
                                            .as_mut()
                                            .expect("upstream to be established")
                                            .close()
                                            .await;
                                        upstream = None;
                                    }

                                    else if pending_pings_conflict_with_route(
                                        &routing,
                                        &pending_pings,
                                        &route.addr,
                                    )
                                    .await
                                    {
                                        let close_reason = record_client_close(
                                            routing.metrics.as_ref(),
                                            GatewayClientCloseReason::PendingPingRouteChange,
                                        );
                                        warn!(
                                            target: "gateway",
                                            %peer,
                                            cell = ?cell,
                                            old_worker = %current.0.worker_id,
                                            new_worker = %route.worker_id,
                                            pending = pending_pings.len(),
                                            close_reason,
                                            "route changed while earlier ping replies are still pending; closing client to avoid silently dropping pongs"
                                        );
                                        upstream
                                            .as_mut()
                                            .expect("upstream to be established")
                                            .close()
                                            .await;
                                        break 'conn_loop Ok(());
                                    }
                                }

                                let connected_new =
                                    match ensure_upstream(
                                        &mut upstream,
                                        route.clone(),
                                        peer,
                                        routing.metrics.as_ref(),
                                    )
                                    .await
                                    {
                                        Ok(changed) => changed,
                                        Err(e) => {
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
                                                let close_reason = record_client_close(
                                                    routing.metrics.as_ref(),
                                                    GatewayClientCloseReason::UpstreamRetryExhausted,
                                                );
                                                warn!(target: "gateway", %peer, close_reason, "closing client connection");
                                                break 'conn_loop Ok(());
                                            }
                                            time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                            continue;
                                        }
                                    };

                                if connected_new && !pending_pings.is_empty() {
                                    let conn =
                                        upstream.as_mut().expect("upstream to be established");
                                    if let Err(e) =
                                        replay_pending_pings(conn, &routing, &pending_pings).await
                                    {
                                        warn!(
                                            target: "gateway",
                                            %peer,
                                            worker = %route.worker_id,
                                            addr = %route.addr,
                                            attempt,
                                            max_attempts = UPSTREAM_RETRY_MAX,
                                            error = ?e,
                                            "failed replaying pending ping after reconnect; reconnecting"
                                        );
                                        conn.close().await;
                                        upstream = None;
                                        if attempt >= UPSTREAM_RETRY_MAX {
                                            let close_reason = record_client_close(
                                                routing.metrics.as_ref(),
                                                GatewayClientCloseReason::UpstreamRetryExhausted,
                                            );
                                            warn!(target: "gateway", %peer, close_reason, "closing client connection");
                                            break 'conn_loop Ok(());
                                        }
                                        time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                        continue;
                                    }
                                }

                                let conn = upstream.as_mut().expect("upstream to be established");
                                if let Err(e) = conn.writer.write_all(&frame).await {
                                    routing
                                        .metrics
                                        .upstream_write_errors
                                        .fetch_add(1, Ordering::Relaxed);
                                    let has_non_ping_traffic =
                                        ping_ts.is_none() || conn.has_non_ping_traffic;
                                    warn!(
                                        target: "gateway",
                                        %peer,
                                        worker = %route.worker_id,
                                        attempt,
                                        max_attempts = UPSTREAM_RETRY_MAX,
                                        error = ?e,
                                        "failed forwarding frame to worker"
                                    );
                                    conn.close().await;
                                    upstream = None;
                                    if has_non_ping_traffic {
                                        let close_reason = record_client_close(
                                            routing.metrics.as_ref(),
                                            GatewayClientCloseReason::AmbiguousUpstream,
                                        );
                                        warn!(
                                            target: "gateway",
                                            %peer,
                                            close_reason,
                                            "closing client connection after ambiguous non-ping write failure"
                                        );
                                        break 'conn_loop Ok(());
                                    }
                                    if attempt >= UPSTREAM_RETRY_MAX {
                                        let close_reason = record_client_close(
                                            routing.metrics.as_ref(),
                                            GatewayClientCloseReason::UpstreamRetryExhausted,
                                        );
                                        warn!(target: "gateway", %peer, close_reason, "closing client connection");
                                        break 'conn_loop Ok(());
                                    }
                                    time::sleep(Duration::from_millis(UPSTREAM_RETRY_BACKOFF_MS)).await;
                                    continue;
                                }
                                if let Some(ts) = ping_ts {
                                    let evicted = push_pending_ping(
                                        &mut pending_pings,
                                        PendingPing {
                                            cell,
                                            ts,
                                            frame: frame.clone(),
                                            sent_at: ping_sent_at.expect("ping timestamp"),
                                        },
                                    );
                                    routing
                                        .metrics
                                        .pending_ping_tracking_evictions
                                        .fetch_add(evicted as u64, Ordering::Relaxed);
                                } else {
                                    if let (Some(kind), Some(request_id), Some(sent_at)) =
                                        (request_kind, request_id, request_sent_at)
                                    {
                                        let evicted = push_pending_request(
                                            &mut pending_requests,
                                            PendingRequest {
                                                cell,
                                                request_id,
                                                kind,
                                                sent_at,
                                            },
                                        );
                                        routing
                                            .metrics
                                            .pending_request_tracking_evictions
                                            .fetch_add(evicted as u64, Ordering::Relaxed);
                                    }
                                    conn.has_non_ping_traffic = true;
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
    has_non_ping_traffic: bool,
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
    metrics: &GatewayMetricsCounters,
) -> Result<bool> {
    let needs_new = match upstream {
        Some(conn) if conn.route.addr == target_worker.addr => false,
        Some(conn) => {
            metrics
                .upstream_route_changes
                .fetch_add(1, Ordering::Relaxed);
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
        let new_conn = connect_upstream(target_worker.clone(), peer, metrics).await?;
        if upstream.replace(new_conn).is_some() {
            // old connection already closed above
        }
        return Ok(true);
    }

    Ok(false)
}

async fn connect_upstream(
    target_worker: WorkerRoute,
    peer: SocketAddr,
    metrics: &GatewayMetricsCounters,
) -> Result<UpstreamConn> {
    metrics
        .upstream_connect_attempts
        .fetch_add(1, Ordering::Relaxed);
    let stream = match TcpStream::connect(target_worker.addr.as_str()).await {
        Ok(stream) => {
            metrics.upstream_connects.fetch_add(1, Ordering::Relaxed);
            stream
        }
        Err(e) => {
            metrics
                .upstream_connect_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(e.into());
        }
    };
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
        has_non_ping_traffic: false,
    })
}

fn record_client_close(
    metrics: &GatewayMetricsCounters,
    reason: GatewayClientCloseReason,
) -> &'static str {
    match reason {
        GatewayClientCloseReason::NoRoute => metrics
            .client_closes_no_route
            .fetch_add(1, Ordering::Relaxed),
        GatewayClientCloseReason::UpstreamRetryExhausted => metrics
            .client_closes_upstream_retry_exhausted
            .fetch_add(1, Ordering::Relaxed),
        GatewayClientCloseReason::PendingPingRouteChange => metrics
            .client_closes_pending_ping_route_change
            .fetch_add(1, Ordering::Relaxed),
        GatewayClientCloseReason::AmbiguousUpstream => metrics
            .client_closes_ambiguous_upstream
            .fetch_add(1, Ordering::Relaxed),
    };
    reason.as_str()
}

async fn replay_pending_pings(
    conn: &mut UpstreamConn,
    routing: &RoutingTable,
    pending_pings: &VecDeque<PendingPing>,
) -> Result<()> {
    for ping in pending_pings {
        let Some(route) = routing.lookup(&ping.cell).await else {
            continue;
        };
        if route.addr != conn.route.addr {
            continue;
        }
        conn.writer
            .write_all(ping.frame.as_ref())
            .await
            .with_context(|| format!("replay ping cell={:?} ts={}", ping.cell, ping.ts))?;
        routing
            .metrics
            .pending_ping_replays
            .fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

async fn pending_pings_conflict_with_route(
    routing: &RoutingTable,
    pending_pings: &VecDeque<PendingPing>,
    target_addr: &str,
) -> bool {
    for ping in pending_pings {
        let Some(route) = routing.lookup(&ping.cell).await else {
            return true;
        };
        if route.addr != target_addr {
            return true;
        }
    }
    false
}

fn acknowledge_pending_ping(
    pending_pings: &mut VecDeque<PendingPing>,
    cell: CellId,
    ts: u64,
) -> Option<Instant> {
    if let Some(index) = pending_pings
        .iter()
        .position(|pending| pending.cell == cell && pending.ts == ts)
    {
        return pending_pings.remove(index).map(|pending| pending.sent_at);
    }
    None
}

fn acknowledge_pending_request(
    pending_requests: &mut VecDeque<PendingRequest>,
    cell: CellId,
    request_id: u64,
) -> Option<(PendingRequestKind, Instant)> {
    if let Some(index) = pending_requests
        .iter()
        .position(|pending| pending.cell == cell && pending.request_id == request_id)
    {
        return pending_requests
            .remove(index)
            .map(|pending| (pending.kind, pending.sent_at));
    }
    None
}

fn push_pending_ping(pending_pings: &mut VecDeque<PendingPing>, ping: PendingPing) -> usize {
    let mut evicted = 0;
    while pending_pings.len() >= MAX_PENDING_PINGS {
        pending_pings.pop_front();
        evicted += 1;
    }
    pending_pings.push_back(ping);
    evicted
}

fn push_pending_request(
    pending_requests: &mut VecDeque<PendingRequest>,
    request: PendingRequest,
) -> usize {
    let mut evicted = 0;
    while pending_requests.len() >= MAX_PENDING_REQUESTS {
        pending_requests.pop_front();
        evicted += 1;
    }
    pending_requests.push_back(request);
    evicted
}

fn observe_request_latency(
    metrics: &GatewayMetricsCounters,
    kind: PendingRequestKind,
    elapsed: Duration,
) {
    match kind {
        PendingRequestKind::Join => metrics.join_roundtrip_latency.observe(elapsed),
        PendingRequestKind::Move => metrics.move_roundtrip_latency.observe(elapsed),
    }
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

async fn load_metrics_listener() -> Result<Option<TcpListener>> {
    let Ok(raw) = std::env::var("TESSERA_GW_METRICS_ADDR") else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("disabled")
    {
        return Ok(None);
    }

    let addr = resolve_socket_addr(trimmed)
        .await
        .with_context(|| format!("resolve TESSERA_GW_METRICS_ADDR={trimmed}"))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind gateway prometheus metrics listener at {addr}"))?;
    Ok(Some(listener))
}

async fn serve_prometheus_metrics(listener: TcpListener, routing: RoutingTable) -> Result<()> {
    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("accept gateway prometheus metrics connection")?;
        let routing = routing.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_prometheus_metrics_request(stream, routing).await {
                tracing::debug!(
                    target: "gateway",
                    peer = %peer,
                    error = ?e,
                    "prometheus metrics request failed"
                );
            }
        });
    }
}

async fn handle_prometheus_metrics_request(
    mut stream: TcpStream,
    routing: RoutingTable,
) -> Result<()> {
    let mut buf = [0_u8; 1024];
    let n = stream
        .read(&mut buf)
        .await
        .context("read gateway prometheus metrics request")?;
    if n == 0 {
        return Ok(());
    }

    let request = std::str::from_utf8(&buf[..n]).unwrap_or_default();
    let mut parts = request
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();

    if method == "GET" && path == "/metrics" {
        let snapshot = routing.metrics_snapshot().await;
        let body = format_gateway_prometheus_metrics(&snapshot);
        write_http_response(
            &mut stream,
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            &body,
        )
        .await?;
    } else if method == "GET" && path == "/ready" {
        let snapshot = routing.readiness_snapshot().await;
        let status = if snapshot.ready {
            "200 OK"
        } else {
            "503 Service Unavailable"
        };
        let body = format_gateway_readiness(&snapshot);
        write_http_response(&mut stream, status, "text/plain; charset=utf-8", &body).await?;
    } else {
        write_http_response(
            &mut stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n",
        )
        .await?;
    }

    Ok(())
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("write gateway prometheus metrics response")?;
    stream
        .shutdown()
        .await
        .context("shutdown gateway prometheus metrics response")?;
    Ok(())
}

fn format_gateway_prometheus_metrics(metrics: &GatewayMetricsSnapshot) -> String {
    let mut out = String::new();
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_gateway_ready",
        "Gateway readiness state derived from currently loaded routes.",
        metrics.ready,
    );
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_gateway_routes",
        "Cell routes currently loaded by the gateway.",
        metrics.routes,
    );
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_gateway_routing_version",
        "Gateway routing table version.",
        metrics.routing_version,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_accepted_connections_total",
        "Client connections accepted by the gateway.",
        metrics.accepted_connections,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_connect_attempts_total",
        "Total upstream worker connection attempts.",
        metrics.upstream_connect_attempts,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_connects_total",
        "Successful upstream worker connections opened by the gateway.",
        metrics.upstream_connects,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_connect_failures_total",
        "Failed upstream worker connection attempts.",
        metrics.upstream_connect_failures,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_route_changes_total",
        "Upstream worker connection swaps caused by routing changes.",
        metrics.upstream_route_changes,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_route_change_reconnects_total",
        "Route changes that required reconnecting an established non-ping upstream session.",
        metrics.upstream_route_change_reconnects,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_reconnect_attempts_total",
        "Reconnect attempts after an upstream connection closes or fails.",
        metrics.upstream_reconnect_attempts,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_closed_total",
        "Upstream worker connections that closed cleanly.",
        metrics.upstream_closed,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_read_errors_total",
        "Upstream worker read errors observed by the gateway.",
        metrics.upstream_read_errors,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_upstream_write_errors_total",
        "Gateway write errors while forwarding frames.",
        metrics.upstream_write_errors,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_invalid_client_frames_total",
        "Invalid client frames that caused connection closure.",
        metrics.invalid_client_frames,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_no_route_lookup_failures_total",
        "Route lookups that found no worker for a requested cell.",
        metrics.no_route_lookup_failures,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_pending_ping_replays_total",
        "Pending ping frames replayed after upstream reconnect.",
        metrics.pending_ping_replays,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_pending_ping_tracking_evictions_total",
        "Oldest pending ping correlation entries evicted at the fixed tracking limit.",
        metrics.pending_ping_tracking_evictions,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_pending_request_tracking_evictions_total",
        "Oldest pending Join or Move correlation entries evicted at the fixed tracking limit.",
        metrics.pending_request_tracking_evictions,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_client_closes_no_route_total",
        "Client connections closed after route lookup failed for all retry attempts.",
        metrics.client_closes_no_route,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_client_closes_upstream_retry_exhausted_total",
        "Client connections closed after upstream reconnect or write retry budget was exhausted.",
        metrics.client_closes_upstream_retry_exhausted,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_client_closes_pending_ping_route_change_total",
        "Client connections closed because a route changed while ping replies were pending.",
        metrics.client_closes_pending_ping_route_change,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_gateway_client_closes_ambiguous_upstream_total",
        "Client connections closed when upstream state could not be retried without risking dropped replies.",
        metrics.client_closes_ambiguous_upstream,
    );
    push_prometheus_histogram(
        &mut out,
        "tessera_gateway_ping_roundtrip_seconds",
        "Gateway-observed client Ping to upstream Pong round-trip latency.",
        &metrics.ping_roundtrip_latency,
    );
    push_prometheus_labeled_histogram(
        &mut out,
        "tessera_gateway_request_roundtrip_seconds",
        "Gateway-observed client request to correlated upstream reply latency.",
        &[
            (
                "kind",
                PendingRequestKind::Join.as_str(),
                &metrics.join_roundtrip_latency,
            ),
            (
                "kind",
                PendingRequestKind::Move.as_str(),
                &metrics.move_roundtrip_latency,
            ),
        ],
    );
    out
}

fn format_gateway_readiness(snapshot: &GatewayReadinessSnapshot) -> String {
    let status = if snapshot.ready { "ready" } else { "not_ready" };
    format!(
        "status {status}\nroutes {}\nrouting_version {}\n",
        snapshot.routes, snapshot.routing_version
    )
}

fn push_prometheus_metric(out: &mut String, metric_type: &str, name: &str, help: &str, value: u64) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn push_prometheus_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    histogram: &GatewayLatencySnapshot,
) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" histogram\n");
    push_prometheus_histogram_samples(out, name, None, histogram);
}

fn push_prometheus_labeled_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    labeled_histograms: &[(&str, &str, &GatewayLatencySnapshot)],
) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" histogram\n");
    for (label_name, label_value, histogram) in labeled_histograms {
        push_prometheus_histogram_samples(out, name, Some((*label_name, *label_value)), histogram);
    }
}

fn push_prometheus_histogram_samples(
    out: &mut String,
    name: &str,
    label: Option<(&str, &str)>,
    histogram: &GatewayLatencySnapshot,
) {
    let mut cumulative = 0_u64;
    for (idx, label_value) in GATEWAY_PING_RTT_BUCKET_LABELS.iter().enumerate() {
        cumulative += histogram.buckets.get(idx).copied().unwrap_or(0);
        push_prometheus_histogram_sample(
            out,
            &format!("{name}_bucket"),
            label,
            Some(label_value),
            &cumulative.to_string(),
        );
    }
    cumulative += histogram
        .buckets
        .get(GATEWAY_PING_RTT_BUCKETS_MICROS.len())
        .copied()
        .unwrap_or(0);
    push_prometheus_histogram_sample(
        out,
        &format!("{name}_bucket"),
        label,
        Some("+Inf"),
        &cumulative.to_string(),
    );

    push_prometheus_histogram_sample(
        out,
        &format!("{name}_sum"),
        label,
        None,
        &format!("{:.6}", histogram.sum_micros as f64 / 1_000_000.0),
    );

    push_prometheus_histogram_sample(
        out,
        &format!("{name}_count"),
        label,
        None,
        &histogram.count.to_string(),
    );
}

fn push_prometheus_histogram_sample(
    out: &mut String,
    name: &str,
    label: Option<(&str, &str)>,
    le: Option<&str>,
    value: &str,
) {
    out.push_str(name);
    if label.is_some() || le.is_some() {
        out.push('{');
        if let Some((label_name, label_value)) = label {
            out.push_str(label_name);
            out.push_str("=\"");
            out.push_str(label_value);
            out.push('"');
            if le.is_some() {
                out.push(',');
            }
        }
        if let Some(le) = le {
            out.push_str("le=\"");
            out.push_str(le);
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(value);
    out.push('\n');
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
    use std::time::{Duration, Instant};
    use tessera_core::{ActorState, EntityId, Envelope, Position, ServerMsg, encode_frame};
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

    fn cell_to_assignment(cell: &CellId) -> Assignment {
        Assignment {
            world: cell.world,
            cx: cell.cx,
            cy: cell.cy,
            depth: cell.depth.into(),
            sub: cell.sub.into(),
        }
    }

    #[test]
    fn gateway_prometheus_metrics_text_uses_snapshot_values() {
        let metrics = GatewayMetricsSnapshot {
            ready: 1,
            routes: 2,
            routing_version: 3,
            accepted_connections: 5,
            upstream_connect_attempts: 6,
            upstream_connects: 7,
            upstream_connect_failures: 11,
            upstream_route_changes: 13,
            upstream_route_change_reconnects: 15,
            upstream_reconnect_attempts: 17,
            upstream_closed: 19,
            upstream_read_errors: 23,
            upstream_write_errors: 29,
            invalid_client_frames: 31,
            no_route_lookup_failures: 37,
            pending_ping_replays: 41,
            pending_ping_tracking_evictions: 43,
            pending_request_tracking_evictions: 47,
            client_closes_no_route: 53,
            client_closes_upstream_retry_exhausted: 59,
            client_closes_pending_ping_route_change: 61,
            client_closes_ambiguous_upstream: 67,
            ping_roundtrip_latency: GatewayLatencySnapshot {
                buckets: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                sum_micros: 1_250_000,
                count: 78,
            },
            join_roundtrip_latency: GatewayLatencySnapshot {
                buckets: vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                sum_micros: 500,
                count: 1,
            },
            move_roundtrip_latency: GatewayLatencySnapshot {
                buckets: vec![0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                sum_micros: 4_000,
                count: 2,
            },
        };

        let text = format_gateway_prometheus_metrics(&metrics);
        assert!(text.contains("# TYPE tessera_gateway_ready gauge\n"));
        assert!(text.contains("tessera_gateway_ready 1\n"));
        assert!(text.contains("# TYPE tessera_gateway_routes gauge\n"));
        assert!(text.contains("tessera_gateway_routes 2\n"));
        assert!(text.contains("tessera_gateway_upstream_connect_attempts_total 6\n"));
        assert!(text.contains("# TYPE tessera_gateway_upstream_connects_total counter\n"));
        assert!(text.contains("tessera_gateway_upstream_connect_failures_total 11\n"));
        assert!(text.contains("tessera_gateway_upstream_route_change_reconnects_total 15\n"));
        assert!(text.contains("tessera_gateway_pending_ping_replays_total 41\n"));
        assert!(text.contains("tessera_gateway_pending_ping_tracking_evictions_total 43\n"));
        assert!(text.contains("tessera_gateway_pending_request_tracking_evictions_total 47\n"));
        assert!(text.contains("tessera_gateway_client_closes_no_route_total 53\n"));
        assert!(text.contains("tessera_gateway_client_closes_upstream_retry_exhausted_total 59\n"));
        assert!(
            text.contains("tessera_gateway_client_closes_pending_ping_route_change_total 61\n")
        );
        assert!(text.contains("tessera_gateway_client_closes_ambiguous_upstream_total 67\n"));
        assert!(text.contains("# TYPE tessera_gateway_ping_roundtrip_seconds histogram\n"));
        assert!(text.contains("tessera_gateway_ping_roundtrip_seconds_bucket{le=\"0.001\"} 1\n"));
        assert!(text.contains("tessera_gateway_ping_roundtrip_seconds_bucket{le=\"0.005\"} 3\n"));
        assert!(text.contains("tessera_gateway_ping_roundtrip_seconds_bucket{le=\"+Inf\"} 78\n"));
        assert!(text.contains("tessera_gateway_ping_roundtrip_seconds_sum 1.250000\n"));
        assert!(text.contains("tessera_gateway_ping_roundtrip_seconds_count 78\n"));
        assert!(text.contains("# TYPE tessera_gateway_request_roundtrip_seconds histogram\n"));
        assert!(text.contains(
            "tessera_gateway_request_roundtrip_seconds_bucket{kind=\"join\",le=\"0.001\"} 1\n"
        ));
        assert!(
            text.contains(
                "tessera_gateway_request_roundtrip_seconds_sum{kind=\"join\"} 0.000500\n"
            )
        );
        assert!(
            text.contains("tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"} 2\n")
        );
    }

    #[test]
    fn gateway_ping_roundtrip_histogram_records_exact_buckets() {
        let histogram = GatewayLatencyHistogram::default();
        histogram.observe(Duration::from_micros(2_000));
        histogram.observe(Duration::from_micros(6_000_000));

        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.buckets[1], 1);
        assert_eq!(snapshot.buckets[GATEWAY_PING_RTT_BUCKETS_MICROS.len()], 1);
        assert_eq!(snapshot.sum_micros, 6_002_000);
        assert_eq!(snapshot.count, 2);
    }

    #[test]
    fn gateway_readiness_text_uses_route_state() {
        let ready = format_gateway_readiness(&GatewayReadinessSnapshot {
            ready: true,
            routes: 1,
            routing_version: 2,
        });
        assert_eq!(ready, "status ready\nroutes 1\nrouting_version 2\n");

        let not_ready = format_gateway_readiness(&GatewayReadinessSnapshot {
            ready: false,
            routes: 0,
            routing_version: 3,
        });
        assert_eq!(not_ready, "status not_ready\nroutes 0\nrouting_version 3\n");
    }

    #[tokio::test]
    async fn gateway_readiness_snapshot_tracks_route_availability() {
        let routing = RoutingTable::new(HashMap::new());
        assert_eq!(
            routing.readiness_snapshot().await,
            GatewayReadinessSnapshot {
                ready: false,
                routes: 0,
                routing_version: 0,
            }
        );

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
            handovers: vec![],
        };
        apply_listing_update(&routing, listing)
            .await
            .expect("apply listing update");

        assert_eq!(
            routing.readiness_snapshot().await,
            GatewayReadinessSnapshot {
                ready: true,
                routes: 1,
                routing_version: 1,
            }
        );
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
            handovers: vec![],
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
            handovers: vec![],
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
            handovers: vec![],
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
            handovers: vec![],
        };

        let routes = routes_from_listing(&listing).expect("parse listing");
        let route = routes
            .get(&CellKey(CellId::grid(0, 0, 0)))
            .expect("route present");
        assert_eq!(route.addr, "worker-a:5001");
    }

    #[tokio::test]
    async fn split_publish_listing_replaces_parent_route_with_child_routes() {
        let parent = CellId::grid(0, 0, 0);
        let child_assignments = (0..4)
            .map(|sub| Assignment {
                world: 0,
                cx: 0,
                cy: 0,
                depth: 1,
                sub,
            })
            .collect::<Vec<_>>();
        let mut initial_routes = HashMap::new();
        initial_routes.insert(
            CellKey(parent),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
            },
        );
        let routing = RoutingTable::new(initial_routes);
        let listing = AssignmentListing {
            workers: vec![
                AssignmentBundle {
                    worker_id: "worker-a".to_string(),
                    addr: "127.0.0.1:5001".to_string(),
                    cells: Vec::new(),
                },
                AssignmentBundle {
                    worker_id: "worker-b".to_string(),
                    addr: "127.0.0.1:5002".to_string(),
                    cells: child_assignments,
                },
            ],
            handovers: vec![],
        };

        assert!(
            apply_listing_update(&routing, listing)
                .await
                .expect("apply split publish listing")
        );
        assert!(routing.lookup(&parent).await.is_none());
        for sub in 0..4 {
            let child = CellId {
                world: 0,
                cx: 0,
                cy: 0,
                depth: 1,
                sub,
            };
            let route = routing.lookup(&child).await.expect("child route");
            assert_eq!(route.worker_id, "worker-b");
            assert_eq!(route.addr, "127.0.0.1:5002");
        }
    }

    #[tokio::test]
    async fn canonical_split_listing_replaces_parent_route_with_exact_child_routes() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let child_assignments = children.iter().map(cell_to_assignment).collect::<Vec<_>>();
        let mut initial_routes = HashMap::new();
        initial_routes.insert(
            CellKey(parent),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
            },
        );
        let routing = RoutingTable::new(initial_routes);
        let listing = AssignmentListing {
            workers: vec![
                AssignmentBundle {
                    worker_id: "worker-a".to_string(),
                    addr: "127.0.0.1:5001".to_string(),
                    cells: Vec::new(),
                },
                AssignmentBundle {
                    worker_id: "worker-b".to_string(),
                    addr: "127.0.0.1:5002".to_string(),
                    cells: child_assignments,
                },
            ],
            handovers: vec![],
        };

        assert!(
            apply_listing_update(&routing, listing)
                .await
                .expect("apply canonical split listing")
        );
        assert!(routing.lookup(&parent).await.is_none());
        for child in children {
            let route = routing.lookup(&child).await.expect("canonical child route");
            assert_eq!(route.worker_id, "worker-b");
            assert_eq!(route.addr, "127.0.0.1:5002");
        }
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
            handovers: vec![],
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
            handovers: vec![],
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
            handovers: vec![],
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
            handovers: vec![],
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
            handovers: vec![],
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

        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.upstream_connect_attempts, UPSTREAM_RETRY_MAX as u64);
        assert_eq!(metrics.upstream_connect_failures, UPSTREAM_RETRY_MAX as u64);
        assert_eq!(metrics.client_closes_upstream_retry_exhausted, 1);
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
            handovers: vec![],
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
    async fn scale_out_watch_routes_new_cell_on_active_session() {
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

        let cell_a = CellId::grid(0, 0, 0);
        let cell_b = CellId::grid(0, 1, 0);
        let mut routes = HashMap::new();
        routes.insert(
            CellKey(cell_a),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
                addr: worker_a_addr.to_string(),
            },
        );
        let routing = RoutingTable::new(routes);
        let (gateway_addr, mut gateway_task) = spawn_gateway_once(routing.clone()).await;
        let mut client = TcpStream::connect(gateway_addr)
            .await
            .expect("connect gateway");

        send_ping_and_expect_pong(&mut client, cell_a, 0, 1).await;

        let scale_out_listing = AssignmentListing {
            workers: vec![
                AssignmentBundle {
                    worker_id: "worker-a".to_string(),
                    addr: worker_a_addr.to_string(),
                    cells: vec![cell_to_assignment(&cell_a)],
                },
                AssignmentBundle {
                    worker_id: "worker-b".to_string(),
                    addr: worker_b_addr.to_string(),
                    cells: vec![cell_to_assignment(&cell_b)],
                },
            ],
            handovers: vec![],
        };
        apply_listing_stream(
            routing.clone(),
            iter(vec![Ok::<_, tonic::Status>(scale_out_listing)]),
        )
        .await
        .expect("apply scale-out watch listing");

        assert_eq!(routing.len().await, 2);
        send_ping_and_expect_pong(&mut client, cell_b, 1, 2).await;
        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.routing_version, 1);
        assert_eq!(metrics.upstream_route_changes, 1);

        timeout(Duration::from_millis(500), a_rx)
            .await
            .expect("worker a timeout")
            .expect("worker a ack");
        timeout(Duration::from_millis(500), b_rx)
            .await
            .expect("worker b timeout")
            .expect("worker b ack");
        drop(client);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker-a", &mut worker_a_task).await;
        wait_task("worker-b", &mut worker_b_task).await;
    }

    #[tokio::test]
    async fn refresh_replaces_worker_address_on_active_session() {
        let original_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind original worker");
        let original_addr = original_listener
            .local_addr()
            .expect("original worker addr");
        let replacement_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind replacement worker");
        let replacement_addr = replacement_listener
            .local_addr()
            .expect("replacement worker addr");

        let (original_tx, original_rx) = oneshot::channel();
        let (replacement_tx, replacement_rx) = oneshot::channel();
        let mut original_task = tokio::spawn(run_ping_worker_expect_close(
            original_listener,
            1,
            original_tx,
        ));
        let mut replacement_task = tokio::spawn(run_ping_worker_once(
            replacement_listener,
            2,
            replacement_tx,
        ));

        let cell = CellId::grid(0, 0, 0);
        let mut routes = HashMap::new();
        routes.insert(
            CellKey(cell),
            WorkerRoute {
                worker_id: "worker-stable".to_string(),
                addr: original_addr.to_string(),
            },
        );
        let routing = RoutingTable::new(routes);
        let (gateway_addr, mut gateway_task) = spawn_gateway_once(routing.clone()).await;
        let mut client = TcpStream::connect(gateway_addr)
            .await
            .expect("connect gateway");

        send_ping_and_expect_pong(&mut client, cell, 0, 1).await;

        let start_version = routing.version.load(Ordering::Relaxed);
        let mut refreshed_routes = HashMap::new();
        refreshed_routes.insert(
            CellKey(cell),
            WorkerRoute {
                worker_id: "worker-stable".to_string(),
                addr: replacement_addr.to_string(),
            },
        );
        assert_eq!(
            try_apply_refresh(&routing, start_version, refreshed_routes).await,
            RefreshOutcome::Applied { new_len: 1 }
        );

        send_ping_and_expect_pong(&mut client, cell, 1, 2).await;
        let route = routing.lookup(&cell).await.expect("replacement route");
        assert_eq!(route.worker_id, "worker-stable");
        assert_eq!(route.addr, replacement_addr.to_string());
        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.routing_version, 1);
        assert_eq!(metrics.upstream_route_changes, 1);

        timeout(Duration::from_millis(500), original_rx)
            .await
            .expect("original worker timeout")
            .expect("original worker ack");
        timeout(Duration::from_millis(500), replacement_rx)
            .await
            .expect("replacement worker timeout")
            .expect("replacement worker ack");
        drop(client);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("original worker", &mut original_task).await;
        wait_task("replacement worker", &mut replacement_task).await;
    }

    #[tokio::test]
    async fn route_change_after_non_ping_reconnects_with_stable_session() {
        let worker_a_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker a");
        let worker_a_addr = worker_a_listener.local_addr().expect("worker a addr");
        let worker_b_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker b");
        let worker_b_addr = worker_b_listener.local_addr().expect("worker b addr");

        let (a_received_tx, a_received_rx) = oneshot::channel();
        let mut worker_a_task = tokio::spawn(run_join_worker_expect_close_without_reply(
            worker_a_listener,
            a_received_tx,
        ));
        let (b_done_tx, b_done_rx) = oneshot::channel();
        let mut worker_b_task = tokio::spawn(run_ping_worker_once(worker_b_listener, 1, b_done_tx));

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

        let join = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        client
            .write_all(&encode_frame(&join))
            .await
            .expect("send join");

        timeout(Duration::from_millis(500), a_received_rx)
            .await
            .expect("worker a did not receive join")
            .expect("worker a receive ack");

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
            handovers: vec![],
        };
        apply_listing_update(&routing, listing_updated)
            .await
            .expect("apply listing update");

        let ping = Envelope {
            cell,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 1 },
        };
        client
            .write_all(&encode_frame(&ping))
            .await
            .expect("send ping after route change");
        let pong = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert_eq!(pong.payload, ServerMsg::Pong { ts: 1 });
        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.upstream_route_change_reconnects, 1);

        timeout(Duration::from_millis(500), b_done_rx)
            .await
            .expect("worker b did not receive routed ping")
            .expect("worker b done");
        drop(client);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker-a", &mut worker_a_task).await;
        wait_task("worker-b", &mut worker_b_task).await;
    }

    #[tokio::test]
    async fn join_request_id_is_correlated_into_latency_metric() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");
        let (worker_done_tx, worker_done_rx) = oneshot::channel();
        let mut worker_task = tokio::spawn(run_join_worker_echo_request_id(
            worker_listener,
            worker_done_tx,
        ));

        let mut map = HashMap::new();
        map.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
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
        let join = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        client
            .write_all(&encode_frame(&join))
            .await
            .expect("send join");
        let reply = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply.payload, ServerMsg::Snapshot { .. }));

        let echoed_request_id = timeout(Duration::from_millis(500), worker_done_rx)
            .await
            .expect("worker did not echo request id")
            .expect("worker done");
        assert!(echoed_request_id > 0);

        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.join_roundtrip_latency.count, 1);
        assert_eq!(metrics.move_roundtrip_latency.count, 0);

        drop(client);
        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    #[tokio::test]
    async fn switching_workers_with_pending_ping_closes_client() {
        let worker_a_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker a");
        let worker_a_addr = worker_a_listener.local_addr().expect("worker a addr");
        let worker_b_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker b");
        let worker_b_addr = worker_b_listener.local_addr().expect("worker b addr");

        let (a_received_tx, a_received_rx) = oneshot::channel();
        let (a_done_tx, a_done_rx) = oneshot::channel();
        let mut worker_a_task = tokio::spawn(run_silent_ping_worker_expect_close(
            worker_a_listener,
            1,
            a_received_tx,
            a_done_tx,
        ));
        let mut worker_b_task = tokio::spawn(run_worker_expect_no_connection(
            worker_b_listener,
            "worker-b",
        ));

        let cell_a = CellId::grid(0, 0, 0);
        let cell_b = CellId::grid(0, 1, 0);
        let mut map = HashMap::new();
        map.insert(
            CellKey(cell_a),
            WorkerRoute {
                worker_id: "worker-a".to_string(),
                addr: worker_a_addr.to_string(),
            },
        );
        map.insert(
            CellKey(cell_b),
            WorkerRoute {
                worker_id: "worker-b".to_string(),
                addr: worker_b_addr.to_string(),
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

        let ping_a = Envelope {
            cell: cell_a,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 1 },
        };
        client
            .write_all(&encode_frame(&ping_a))
            .await
            .expect("send first ping");

        timeout(Duration::from_millis(500), a_received_rx)
            .await
            .expect("worker a did not receive first ping")
            .expect("worker a receive ack");

        let ping_b = Envelope {
            cell: cell_b,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 2 },
        };
        client
            .write_all(&encode_frame(&ping_b))
            .await
            .expect("send second ping");

        expect_client_close(&mut client).await;
        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.client_closes_pending_ping_route_change, 1);

        timeout(Duration::from_millis(500), a_done_rx)
            .await
            .expect("worker a timeout")
            .expect("worker a ack");

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker-a", &mut worker_a_task).await;
        wait_task("worker-b", &mut worker_b_task).await;
    }

    #[tokio::test]
    async fn route_change_replays_pending_ping_after_reconnect() {
        let worker_a_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker a");
        let worker_a_addr = worker_a_listener.local_addr().expect("worker a addr");
        let worker_b_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker b");
        let worker_b_addr = worker_b_listener.local_addr().expect("worker b addr");

        let (a_received_tx, a_received_rx) = oneshot::channel();
        let (a_done_tx, a_done_rx) = oneshot::channel();
        let (b_done_tx, b_done_rx) = oneshot::channel();
        let mut worker_a_task = tokio::spawn(run_silent_ping_worker_expect_close(
            worker_a_listener,
            1,
            a_received_tx,
            a_done_tx,
        ));
        let mut worker_b_task =
            tokio::spawn(run_ping_worker_twice(worker_b_listener, 1, 2, b_done_tx));

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

        timeout(Duration::from_millis(500), a_received_rx)
            .await
            .expect("worker a did not receive first ping")
            .expect("worker a receive ack");

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
            handovers: vec![],
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

        let reply_a = read_env_with_timeout(&mut client, Duration::from_secs(1)).await;
        let reply_b = read_env_with_timeout(&mut client, Duration::from_secs(1)).await;
        let mut ts_values = vec![];
        for reply in [reply_a, reply_b] {
            match reply.payload {
                ServerMsg::Pong { ts } => ts_values.push(ts),
                other => panic!("expected pong, got {other:?}"),
            }
        }
        ts_values.sort();
        assert_eq!(ts_values, vec![1, 2]);

        drop(client);

        timeout(Duration::from_millis(500), a_done_rx)
            .await
            .expect("worker a timeout")
            .expect("worker a ack");
        timeout(Duration::from_millis(500), b_done_rx)
            .await
            .expect("worker b timeout")
            .expect("worker b ack");

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker-a", &mut worker_a_task).await;
        wait_task("worker-b", &mut worker_b_task).await;
    }

    #[tokio::test]
    async fn upstream_disconnect_after_non_ping_closes_client() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");

        let (received_tx, received_rx) = oneshot::channel();
        let mut worker_task = tokio::spawn(run_join_then_close_and_expect_no_reconnect(
            worker_listener,
            received_tx,
        ));

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
        let join = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        client
            .write_all(&encode_frame(&join))
            .await
            .expect("send join");

        timeout(Duration::from_millis(500), received_rx)
            .await
            .expect("worker did not receive join")
            .expect("worker receive ack");
        expect_client_close(&mut client).await;

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    #[tokio::test]
    async fn completed_non_ping_disconnect_keeps_client_for_next_route() {
        let worker_a_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker a");
        let worker_a_addr = worker_a_listener.local_addr().expect("worker a addr");
        let worker_b_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind worker b");
        let worker_b_addr = worker_b_listener.local_addr().expect("worker b addr");

        let (a_done_tx, a_done_rx) = oneshot::channel();
        let mut worker_a_task = tokio::spawn(run_join_reply_then_close_worker(
            worker_a_listener,
            a_done_tx,
        ));
        let (b_done_tx, b_done_rx) = oneshot::channel();
        let mut worker_b_task = tokio::spawn(run_ping_worker_once(worker_b_listener, 9, b_done_tx));

        let cell = CellId::grid(0, 0, 0);
        let mut map = HashMap::new();
        map.insert(
            CellKey(cell),
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
        let join = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        client
            .write_all(&encode_frame(&join))
            .await
            .expect("send join");
        let reply = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply.payload, ServerMsg::Snapshot { .. }));
        timeout(Duration::from_millis(500), a_done_rx)
            .await
            .expect("worker a did not close")
            .expect("worker a done");

        let listing_updated = AssignmentListing {
            workers: vec![AssignmentBundle {
                worker_id: "worker-b".to_string(),
                addr: worker_b_addr.to_string(),
                cells: vec![Assignment {
                    world: cell.world,
                    cx: cell.cx,
                    cy: cell.cy,
                    depth: u32::from(cell.depth),
                    sub: u32::from(cell.sub),
                }],
            }],
            handovers: vec![],
        };
        apply_listing_update(&routing, listing_updated)
            .await
            .expect("apply listing update");
        wait_for_upstream_closed(&routing).await;

        let ping = Envelope {
            cell,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 9 },
        };
        client
            .write_all(&encode_frame(&ping))
            .await
            .expect("send ping after completed upstream close");
        let pong = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert_eq!(pong.payload, ServerMsg::Pong { ts: 9 });

        let metrics = routing.metrics_snapshot().await;
        assert!(metrics.upstream_closed >= 1);
        assert_eq!(metrics.client_closes_ambiguous_upstream, 0);

        timeout(Duration::from_millis(500), b_done_rx)
            .await
            .expect("worker b timeout")
            .expect("worker b done");
        drop(client);

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
            handovers: vec![],
        };
        let stream = iter(vec![
            Ok(listing_initial.clone()),
            Ok(AssignmentListing {
                workers: vec![],
                handovers: vec![],
            }),
        ]);
        apply_listing_stream(routing.clone(), stream)
            .await
            .expect("process listing stream");

        let guard = routing.routes.read().await;
        assert!(guard.is_empty());
    }

    #[test]
    fn acknowledge_pending_ping_only_removes_one_duplicate() {
        let cell = CellId::grid(0, 0, 0);
        let frame = Bytes::from_static(b"ping");
        let sent_at = Instant::now();
        let mut pending = VecDeque::from([
            PendingPing {
                cell,
                ts: 7,
                frame: frame.clone(),
                sent_at,
            },
            PendingPing {
                cell,
                ts: 7,
                frame,
                sent_at,
            },
        ]);

        assert!(acknowledge_pending_ping(&mut pending, cell, 7).is_some());

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].cell, cell);
        assert_eq!(pending[0].ts, 7);
    }

    #[test]
    fn pending_correlation_queues_evict_oldest_at_fixed_capacity() {
        let cell = CellId::grid(0, 0, 0);
        let sent_at = Instant::now();
        let mut pings = VecDeque::new();
        let mut requests = VecDeque::new();

        for index in 0..(MAX_PENDING_PINGS + 3) {
            let evicted = push_pending_ping(
                &mut pings,
                PendingPing {
                    cell,
                    ts: index as u64,
                    frame: Bytes::from_static(b"ping"),
                    sent_at,
                },
            );
            assert_eq!(evicted, usize::from(index >= MAX_PENDING_PINGS));
        }
        for index in 0..(MAX_PENDING_REQUESTS + 3) {
            let evicted = push_pending_request(
                &mut requests,
                PendingRequest {
                    cell,
                    request_id: index as u64,
                    kind: if index % 2 == 0 {
                        PendingRequestKind::Join
                    } else {
                        PendingRequestKind::Move
                    },
                    sent_at,
                },
            );
            assert_eq!(evicted, usize::from(index >= MAX_PENDING_REQUESTS));
        }

        assert_eq!(pings.len(), MAX_PENDING_PINGS);
        assert_eq!(pings.front().expect("oldest retained ping").ts, 3);
        assert_eq!(
            pings.back().expect("newest retained ping").ts,
            (MAX_PENDING_PINGS + 2) as u64
        );
        assert_eq!(requests.len(), MAX_PENDING_REQUESTS);
        assert_eq!(
            requests
                .front()
                .expect("oldest retained request")
                .request_id,
            3
        );
        assert_eq!(
            requests.back().expect("newest retained request").request_id,
            (MAX_PENDING_REQUESTS + 2) as u64
        );
    }

    #[tokio::test]
    async fn fragmented_client_frame_reaches_upstream_once() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");
        let mut worker_task = tokio::spawn(async move {
            run_stub_worker(worker_listener).await;
        });
        let routing = routing_for_worker(worker_addr);
        let (gateway_addr, mut gateway_task) = spawn_gateway_once(routing.clone()).await;
        let mut client = TcpStream::connect(gateway_addr)
            .await
            .expect("connect gateway");
        let frame = encode_frame(&Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Ping { ts: 42 },
        });

        for byte in frame.iter() {
            client
                .write_all(std::slice::from_ref(byte))
                .await
                .expect("write fragmented byte");
            tokio::task::yield_now().await;
        }

        let reply = read_env_with_timeout(&mut client, Duration::from_millis(500)).await;
        assert!(matches!(reply.payload, ServerMsg::Pong { ts: 42 }));
        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.upstream_connects, 1);
        assert_eq!(metrics.invalid_client_frames, 0);

        drop(client);
        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    #[tokio::test]
    async fn pipelined_ping_burst_preserves_order_and_observes_tracking_pressure() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");
        let mut worker_task = tokio::spawn(async move {
            run_stub_worker(worker_listener).await;
        });
        let routing = routing_for_worker(worker_addr);
        let (gateway_addr, mut gateway_task) = spawn_gateway_once(routing.clone()).await;
        let mut client = TcpStream::connect(gateway_addr)
            .await
            .expect("connect gateway");
        let burst_len = MAX_PENDING_PINGS * 3;
        let mut burst = Vec::new();
        for index in 0..burst_len {
            burst.extend_from_slice(&encode_frame(&Envelope {
                cell: CellId::grid(0, 0, 0),
                seq: index as u64,
                epoch: 0,
                payload: ClientMsg::Ping { ts: index as u64 },
            }));
        }
        client
            .write_all(&burst)
            .await
            .expect("write pipelined burst");

        for expected in 0..burst_len {
            let reply = read_env_with_timeout(&mut client, Duration::from_secs(1)).await;
            assert!(matches!(reply.payload, ServerMsg::Pong { ts } if ts == expected as u64));
        }
        let metrics = routing.metrics_snapshot().await;
        assert!(
            metrics.pending_ping_tracking_evictions > 0,
            "burst should exceed the bounded correlation window"
        );

        drop(client);
        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    #[tokio::test]
    async fn oversized_client_prefix_closes_before_upstream_connect() {
        let worker_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind worker");
        let worker_addr = worker_listener.local_addr().expect("worker addr");
        let mut worker_task = tokio::spawn(async move {
            run_worker_expect_no_connection(worker_listener, "oversized client frame").await;
        });
        let routing = routing_for_worker(worker_addr);
        let (gateway_addr, mut gateway_task) = spawn_gateway_once(routing.clone()).await;
        let mut client = TcpStream::connect(gateway_addr)
            .await
            .expect("connect gateway");

        let oversized_len = u32::try_from(MAX_FRAME_LEN + 1).expect("frame limit fits u32");
        client
            .write_all(&oversized_len.to_be_bytes())
            .await
            .expect("write oversized prefix");
        expect_client_close(&mut client).await;

        let metrics = routing.metrics_snapshot().await;
        assert_eq!(metrics.invalid_client_frames, 1);
        assert_eq!(metrics.upstream_connect_attempts, 0);
        assert_eq!(metrics.upstream_connects, 0);

        wait_task("gateway", &mut gateway_task).await;
        wait_task("worker", &mut worker_task).await;
    }

    fn routing_for_worker(worker_addr: SocketAddr) -> RoutingTable {
        let mut routes = HashMap::new();
        routes.insert(
            CellKey(CellId::grid(0, 0, 0)),
            WorkerRoute {
                worker_id: "worker-test".to_string(),
                addr: worker_addr.to_string(),
            },
        );
        RoutingTable::new(routes)
    }

    async fn spawn_gateway_once(
        routing: RoutingTable,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind gateway");
        let address = listener.local_addr().expect("gateway addr");
        let task = tokio::spawn(async move {
            let (socket, peer) = listener.accept().await.expect("accept gateway client");
            handle_conn(socket, peer, routing)
                .await
                .expect("handle gateway client");
        });
        (address, task)
    }

    async fn send_ping_and_expect_pong(client: &mut TcpStream, cell: CellId, seq: u64, ts: u64) {
        client
            .write_all(&encode_frame(&Envelope {
                cell,
                seq,
                epoch: 0,
                payload: ClientMsg::Ping { ts },
            }))
            .await
            .expect("send ping");
        let reply = read_env_with_timeout(client, Duration::from_millis(500)).await;
        assert_eq!(reply.cell, cell);
        assert_eq!(reply.payload, ServerMsg::Pong { ts });
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

    async fn run_join_worker_expect_close_without_reply(
        listener: TcpListener,
        received: oneshot::Sender<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_in = read_client_env(&mut socket).await;
        match env_in.payload {
            ClientMsg::Join { actor, .. } => assert_eq!(actor, EntityId(1)),
            other => panic!("expected join, got {other:?}"),
        }
        let _ = received.send(());

        let mut next_len = [0u8; 4];
        let read = timeout(Duration::from_millis(500), socket.read_exact(&mut next_len)).await;
        match read {
            Ok(Ok(_)) => panic!("unexpected second frame on worker"),
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) => {}
            Ok(Err(e)) => panic!("unexpected worker read error: {e:?}"),
            Err(_) => panic!("worker did not observe gateway close"),
        }
    }

    async fn run_join_worker_echo_request_id(listener: TcpListener, done: oneshot::Sender<u64>) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_in = read_client_envelope(&mut socket).await;
        let request_id = env_in.request_id.expect("gateway request id");
        assert!(env_in.session.is_some());
        let cell = env_in.cell;
        let actor = match env_in.payload {
            ClientMsg::Join { actor, .. } => actor,
            other => panic!("expected join, got {other:?}"),
        };
        let env_out = ServerEnvelope {
            cell,
            seq: 0,
            epoch: env_in.epoch,
            request_id: Some(request_id),
            payload: ServerMsg::Snapshot {
                cell,
                actors: vec![ActorState {
                    id: actor,
                    pos: Position { x: 0.0, y: 0.0 },
                }],
            },
        };
        socket
            .write_all(&encode_frame(&env_out))
            .await
            .expect("write join reply");
        let _ = done.send(request_id);
    }

    async fn run_join_reply_then_close_worker(listener: TcpListener, done: oneshot::Sender<()>) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_in = read_client_envelope(&mut socket).await;
        let request_id = env_in.request_id.expect("gateway request id");
        assert!(env_in.session.is_some());
        let cell = env_in.cell;
        let actor = match env_in.payload {
            ClientMsg::Join { actor, .. } => actor,
            other => panic!("expected join, got {other:?}"),
        };
        let env_out = ServerEnvelope {
            cell,
            seq: 0,
            epoch: env_in.epoch,
            request_id: Some(request_id),
            payload: ServerMsg::Snapshot {
                cell,
                actors: vec![ActorState {
                    id: actor,
                    pos: Position { x: 0.0, y: 0.0 },
                }],
            },
        };
        socket
            .write_all(&encode_frame(&env_out))
            .await
            .expect("write join reply");
        let _ = socket.shutdown().await;
        drop(socket);
        let _ = done.send(());
    }

    async fn run_join_then_close_and_expect_no_reconnect(
        listener: TcpListener,
        received: oneshot::Sender<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_in = read_client_env(&mut socket).await;
        match env_in.payload {
            ClientMsg::Join { actor, .. } => assert_eq!(actor, EntityId(1)),
            other => panic!("expected join, got {other:?}"),
        }
        let _ = received.send(());
        let _ = socket.shutdown().await;
        drop(socket);

        let accept = timeout(Duration::from_millis(300), listener.accept()).await;
        assert!(accept.is_err(), "gateway unexpectedly reconnected upstream");
    }

    async fn run_worker_expect_no_connection(listener: TcpListener, label: &str) {
        let accept = timeout(Duration::from_millis(300), listener.accept()).await;
        assert!(
            accept.is_err(),
            "{label} unexpectedly accepted a connection"
        );
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

    async fn run_ping_worker_twice(
        listener: TcpListener,
        expected_a: u64,
        expected_b: u64,
        done: oneshot::Sender<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let mut seen = Vec::new();
        for _ in 0..2 {
            let env_in = read_client_env(&mut socket).await;
            let ts = match env_in.payload {
                ClientMsg::Ping { ts } => ts,
                other => panic!("expected ping, got {other:?}"),
            };
            seen.push(ts);
            let env_out = Envelope {
                cell: env_in.cell,
                seq: 0,
                epoch: env_in.epoch,
                payload: ServerMsg::Pong { ts },
            };
            let frame = encode_frame(&env_out);
            socket.write_all(&frame).await.expect("write reply");
        }
        seen.sort();
        assert_eq!(seen, vec![expected_a, expected_b]);
        let _ = done.send(());
    }

    async fn run_silent_ping_worker_expect_close(
        listener: TcpListener,
        expected_ts: u64,
        received: oneshot::Sender<()>,
        done: oneshot::Sender<()>,
    ) {
        let (mut socket, _peer) = listener.accept().await.expect("accept worker");
        let env_in = read_client_env(&mut socket).await;
        let ts = match env_in.payload {
            ClientMsg::Ping { ts } => ts,
            other => panic!("expected ping, got {other:?}"),
        };
        assert_eq!(ts, expected_ts);
        let _ = received.send(());

        let mut next_len = [0u8; 4];
        let read = timeout(Duration::from_millis(500), socket.read_exact(&mut next_len)).await;
        match read {
            Ok(Ok(_)) => panic!("unexpected second frame on worker"),
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) => {}
            Ok(Err(e)) => panic!("unexpected worker read error: {e:?}"),
            Err(_) => panic!("worker did not observe gateway close"),
        }

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

    async fn read_client_envelope(stream: &mut TcpStream) -> ClientEnvelope {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.expect("read len");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.expect("read payload");
        serde_json::from_slice::<ClientEnvelope>(&payload).expect("decode client envelope")
    }

    async fn expect_client_close(stream: &mut TcpStream) {
        let mut buf = [0u8; 1];
        let read = timeout(Duration::from_millis(500), stream.read_exact(&mut buf)).await;
        match read {
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) => {}
            Ok(Ok(_)) => panic!("unexpected data from gateway"),
            Ok(Err(e)) => panic!("unexpected client read error: {e:?}"),
            Err(_) => panic!("gateway did not close client connection in time"),
        }
    }

    async fn wait_for_upstream_closed(routing: &RoutingTable) {
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if routing.metrics_snapshot().await.upstream_closed > 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "gateway did not observe upstream close in time"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
