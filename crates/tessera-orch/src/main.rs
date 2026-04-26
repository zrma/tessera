use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tessera_core::CellId;
use tessera_proto::orch::v1::orchestrator_server::{Orchestrator, OrchestratorServer};
use tessera_proto::orch::v1::{
    Assignment, AssignmentBundle, AssignmentListing, AssignmentQuery, AssignmentSnapshot,
    GetMetricsRequest, HandoverClientMovePolicy, HandoverCommand, HandoverCommandRequest,
    HandoverCommandResponse, HandoverFailurePolicy, HandoverState, HandoverStatus,
    HealthCheckRequest, ListAssignmentsRequest, OrchestratorHealth, OrchestratorMetrics,
    WatchAssignmentsRequest, WorkerHealth, WorkerRegistration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, watch};
use tokio_stream::Stream;
use tokio_stream::{StreamExt, wrappers::WatchStream};
use tonic::{Request, Response, Status, async_trait, transport::Server};
use tracing::{error, info, warn};

const HANDOVER_COMMIT_RETRY_LIMIT: u32 = 3;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = load_config().context("load orchestrator config")?;
    let addr_raw =
        std::env::var("TESSERA_ORCH_ADDR").unwrap_or_else(|_| "127.0.0.1:6000".to_string());
    let listen_addr = resolve_socket_addr(&addr_raw)
        .await
        .with_context(|| format!("resolve TESSERA_ORCH_ADDR={addr_raw}"))?;

    let service = OrchestratorService::new(config);
    let metrics_listener = load_metrics_listener().await?;
    info!(target: "orch", %listen_addr, "tessera-orch listening");

    let grpc_server = Server::builder()
        .add_service(OrchestratorServer::new(service.clone()))
        .serve(listen_addr);

    if let Some(listener) = metrics_listener {
        let metrics_addr = listener
            .local_addr()
            .context("read prometheus metrics listener addr")?;
        info!(target: "orch", %metrics_addr, "tessera-orch prometheus metrics listening");

        tokio::select! {
            grpc_result = grpc_server => {
                if let Err(e) = grpc_result {
                    error!(target: "orch", error = ?e, "server exited with error");
                    return Err(e.into());
                }
            }
            metrics_result = serve_prometheus_metrics(listener, service) => {
                if let Err(e) = metrics_result {
                    error!(target: "orch", error = ?e, "prometheus metrics exporter exited with error");
                    return Err(e);
                }
            }
        }
    } else if let Err(e) = grpc_server.await {
        error!(target: "orch", error = ?e, "server exited with error");
        return Err(e.into());
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    workers: HashMap<String, WorkerStatic>,
}

type AssignmentMap = HashMap<String, Vec<CellId>>;

#[derive(Debug, Clone)]
struct WorkerStatic {
    addr: String,
    cells: Vec<CellId>,
}

impl Config {
    fn default_single_cell() -> Self {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-local".to_string(),
            WorkerStatic {
                addr: "127.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        Self { workers }
    }

    fn worker(&self, worker_id: &str) -> Option<&WorkerStatic> {
        self.workers.get(worker_id)
    }

    fn initial_assignments(&self) -> AssignmentMap {
        self.workers
            .iter()
            .map(|(worker_id, worker)| (worker_id.clone(), worker.cells.clone()))
            .collect()
    }
}

fn owner_for_cell<'a>(assignments: &'a AssignmentMap, cell: &CellId) -> Option<&'a str> {
    assignments.iter().find_map(|(worker_id, cells)| {
        cells
            .iter()
            .any(|assigned| assigned == cell)
            .then_some(worker_id.as_str())
    })
}

fn sort_cells(cells: &mut [CellId]) {
    cells.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
}

fn transfer_cell_assignment(
    assignments: &mut AssignmentMap,
    cell: CellId,
    source_worker_id: &str,
    target_worker_id: &str,
) -> Result<bool, String> {
    match owner_for_cell(assignments, &cell) {
        Some(owner) if owner == source_worker_id => {}
        Some(owner) => {
            return Err(format!(
                "source_worker_id={} no longer owns cell; current_owner={}",
                source_worker_id, owner
            ));
        }
        None => return Err("cell is not assigned to any worker".to_string()),
    }

    let source_cells = assignments
        .get_mut(source_worker_id)
        .ok_or_else(|| format!("unknown source_worker_id={source_worker_id}"))?;
    let source_len = source_cells.len();
    source_cells.retain(|assigned| assigned != &cell);
    if source_cells.len() == source_len {
        return Err(format!(
            "source_worker_id={} no longer owns cell",
            source_worker_id
        ));
    }

    let target_cells = assignments
        .get_mut(target_worker_id)
        .ok_or_else(|| format!("unknown target_worker_id={target_worker_id}"))?;
    if target_cells.iter().any(|assigned| assigned == &cell) {
        return Ok(false);
    }
    target_cells.push(cell);
    sort_cells(target_cells);
    Ok(true)
}

fn listing_with_runtime(
    config: &Config,
    assignments: &AssignmentMap,
    runtime: &HashMap<String, WorkerRuntime>,
    handovers: &HashMap<CellId, HandoverRuntime>,
) -> AssignmentListing {
    let mut bundles = Vec::new();
    let mut worker_ids: Vec<&String> = config.workers.keys().collect();
    worker_ids.sort();
    for worker_id in worker_ids {
        let worker = config.workers.get(worker_id).expect("worker id from keys");
        let addr = runtime
            .get(worker_id.as_str())
            .map(|rt| rt.addr.as_str())
            .unwrap_or(worker.addr.as_str());
        let cells = assignments
            .get(worker_id.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .map(cell_to_assignment)
            .collect::<Vec<_>>();
        bundles.push(AssignmentBundle {
            worker_id: worker_id.clone(),
            addr: addr.to_string(),
            cells,
        });
    }
    AssignmentListing {
        workers: bundles,
        handovers: handover_statuses(handovers),
    }
}

#[derive(Debug, serde::Deserialize)]
struct OrchConfig {
    workers: Vec<WorkerConfig>,
}

#[derive(Debug, serde::Deserialize)]
struct WorkerConfig {
    id: String,
    addr: String,
    cells: Vec<CellConfig>,
}

#[derive(Debug, serde::Deserialize)]
struct CellConfig {
    world: u32,
    cx: i32,
    cy: i32,
    #[serde(default)]
    depth: u8,
    #[serde(default)]
    sub: u8,
}

impl From<CellConfig> for CellId {
    fn from(value: CellConfig) -> Self {
        Self {
            world: value.world,
            cx: value.cx,
            cy: value.cy,
            depth: value.depth,
            sub: value.sub,
        }
    }
}

fn load_config() -> Result<Config> {
    if let Ok(path) = std::env::var("TESSERA_ORCH_CONFIG") {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("read orchestrator config from {path}"))?;
        return parse_config(&raw);
    }
    if let Ok(raw) = std::env::var("TESSERA_ORCH_CONFIG_JSON") {
        return parse_config(&raw);
    }
    warn!(target: "orch", "no config provided; using default single-cell mapping");
    Ok(Config::default_single_cell())
}

fn parse_config(raw: &str) -> Result<Config> {
    let parsed: OrchConfig =
        serde_json::from_str(raw).context("parse orchestrator config as JSON")?;
    let mut workers = HashMap::new();
    let mut assigned_cells: HashMap<CellId, String> = HashMap::new();
    for w in parsed.workers {
        let id = w.id.trim();
        if id.is_empty() {
            return Err(anyhow!("worker id must not be empty"));
        }
        if workers.contains_key(id) {
            return Err(anyhow!("duplicate worker id: {}", id));
        }
        let addr = w.addr.trim();
        if addr.is_empty() {
            return Err(anyhow!("worker addr must not be empty for {}", id));
        }
        let label = format!("worker addr for {}", id);
        validate_worker_addr(addr, &label)?;
        let mut cells = Vec::with_capacity(w.cells.len());
        for cell_cfg in w.cells {
            let cell = CellId::from(cell_cfg);
            if let Some(prev) = assigned_cells.get(&cell) {
                return Err(anyhow!(
                    "cell world={},cx={},cy={},depth={},sub={} assigned to both {} and {}",
                    cell.world,
                    cell.cx,
                    cell.cy,
                    cell.depth,
                    cell.sub,
                    prev,
                    id
                ));
            }
            assigned_cells.insert(cell, id.to_string());
            cells.push(cell);
        }
        workers.insert(
            id.to_string(),
            WorkerStatic {
                addr: addr.to_string(),
                cells,
            },
        );
    }
    Ok(Config { workers })
}

fn validate_worker_addr(addr: &str, label: &str) -> Result<()> {
    validate_addr_with_port(addr, label)?;
    if let Ok(parsed) = addr.parse::<SocketAddr>()
        && parsed.ip().is_unspecified()
    {
        return Err(anyhow!("{label} must not be an unspecified address"));
    }
    Ok(())
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

#[derive(Debug, Clone)]
struct WorkerRuntime {
    addr: String,
    last_seen: SystemTime,
}

#[derive(Debug, Clone)]
struct HandoverRuntime {
    operation_id: String,
    source_worker_id: String,
    target_worker_id: String,
    state: HandoverState,
    commit_attempts: u32,
}

fn handover_statuses(handovers: &HashMap<CellId, HandoverRuntime>) -> Vec<HandoverStatus> {
    let mut entries = handovers.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(cell, handover)| {
        (
            cell.world,
            cell.cy,
            cell.cx,
            cell.depth,
            cell.sub,
            handover.operation_id.as_str(),
        )
    });

    entries
        .into_iter()
        .map(|(cell, handover)| HandoverStatus {
            operation_id: handover.operation_id.clone(),
            cell: Some(cell_to_assignment(cell)),
            source_worker_id: handover.source_worker_id.clone(),
            target_worker_id: handover.target_worker_id.clone(),
            state: handover.state as i32,
            client_move_policy: client_move_policy_for(handover.state) as i32,
        })
        .collect()
}

#[allow(dead_code)]
mod split_merge_planner {
    use super::CellId;
    use std::collections::{HashMap, HashSet};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum SplitMergePlanKind {
        Split,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) struct SplitMergePlannerConfig {
        pub high_actor_count: u64,
        pub high_move_queue_pressure: u64,
        pub high_tick_stage_micros: u64,
        pub high_relay_fanout: u64,
        pub high_handover_failures: u64,
        pub min_split_pressure_signals: u8,
        pub required_split_windows: u32,
        pub min_cell_age_secs: u64,
        pub max_active_plans_per_world: usize,
        pub max_handover_ops_per_interval: usize,
        pub max_cells_moved_per_interval: usize,
    }

    impl Default for SplitMergePlannerConfig {
        fn default() -> Self {
            Self {
                high_actor_count: 100,
                high_move_queue_pressure: 64,
                high_tick_stage_micros: 20_000,
                high_relay_fanout: 64,
                high_handover_failures: 1,
                min_split_pressure_signals: 2,
                required_split_windows: 3,
                min_cell_age_secs: 60,
                max_active_plans_per_world: 1,
                max_handover_ops_per_interval: 1,
                max_cells_moved_per_interval: 1,
            }
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub(super) struct SplitMergeMetricsSnapshot {
        pub cells: Vec<SplitMergeCellMetrics>,
        pub active_plans: Vec<SplitMergeActivePlan>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) struct SplitMergeCellMetrics {
        pub cell: CellId,
        pub actor_count: u64,
        pub move_queue_pressure: u64,
        pub tick_stage_micros: u64,
        pub relay_fanout: u64,
        pub handover_failures: u64,
        pub high_pressure_windows: u32,
        pub cell_age_secs: u64,
        pub cooldown_remaining_secs: u64,
        pub active_handover: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) struct SplitMergeActivePlan {
        pub kind: SplitMergePlanKind,
        pub cell: CellId,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) struct SplitMergePlan {
        pub kind: SplitMergePlanKind,
        pub cell: CellId,
        pub pressure_signals: u8,
        pub score: u64,
        pub required_handover_ops: usize,
        pub cells_moved: usize,
    }

    pub(super) fn plan_split_merge(
        snapshot: &SplitMergeMetricsSnapshot,
        config: &SplitMergePlannerConfig,
    ) -> Vec<SplitMergePlan> {
        if config.max_active_plans_per_world == 0
            || config.max_handover_ops_per_interval == 0
            || config.max_cells_moved_per_interval == 0
        {
            return Vec::new();
        }

        let mut active_plans_by_world: HashMap<u32, usize> = HashMap::new();
        let mut blocked_families = HashSet::new();
        for active in &snapshot.active_plans {
            *active_plans_by_world.entry(active.cell.world).or_default() += 1;
            blocked_families.insert(cell_family_key(&active.cell));
        }

        let mut candidates = snapshot
            .cells
            .iter()
            .filter(|cell| !blocked_families.contains(&cell_family_key(&cell.cell)))
            .filter(|cell| cell.cooldown_remaining_secs == 0)
            .filter(|cell| !cell.active_handover)
            .filter(|cell| cell.cell_age_secs >= config.min_cell_age_secs)
            .filter(|cell| cell.high_pressure_windows >= config.required_split_windows)
            .filter_map(|cell| {
                let pressure_signals = split_pressure_signals(cell, config);
                (pressure_signals >= config.min_split_pressure_signals).then(|| SplitMergePlan {
                    kind: SplitMergePlanKind::Split,
                    cell: cell.cell,
                    pressure_signals,
                    score: split_pressure_score(cell, pressure_signals),
                    required_handover_ops: 1,
                    cells_moved: 1,
                })
            })
            .collect::<Vec<_>>();

        candidates.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| cell_family_key(&left.cell).cmp(&cell_family_key(&right.cell)))
        });

        let mut planned_by_world: HashMap<u32, usize> = HashMap::new();
        let mut remaining_handover_ops = config.max_handover_ops_per_interval;
        let mut remaining_cells_moved = config.max_cells_moved_per_interval;
        let mut plans = Vec::new();

        for candidate in candidates {
            let active_in_world = active_plans_by_world
                .get(&candidate.cell.world)
                .copied()
                .unwrap_or_default();
            let planned_in_world = planned_by_world
                .get(&candidate.cell.world)
                .copied()
                .unwrap_or_default();
            if active_in_world + planned_in_world >= config.max_active_plans_per_world {
                continue;
            }
            if candidate.required_handover_ops > remaining_handover_ops
                || candidate.cells_moved > remaining_cells_moved
            {
                continue;
            }

            remaining_handover_ops -= candidate.required_handover_ops;
            remaining_cells_moved -= candidate.cells_moved;
            *planned_by_world.entry(candidate.cell.world).or_default() += 1;
            plans.push(candidate);
        }

        plans
    }

    fn split_pressure_signals(
        cell: &SplitMergeCellMetrics,
        config: &SplitMergePlannerConfig,
    ) -> u8 {
        [
            cell.actor_count >= config.high_actor_count,
            cell.move_queue_pressure >= config.high_move_queue_pressure,
            cell.tick_stage_micros >= config.high_tick_stage_micros,
            cell.relay_fanout >= config.high_relay_fanout,
            cell.handover_failures >= config.high_handover_failures,
        ]
        .into_iter()
        .filter(|signal| *signal)
        .count() as u8
    }

    fn split_pressure_score(cell: &SplitMergeCellMetrics, pressure_signals: u8) -> u64 {
        u64::from(pressure_signals) * 1_000_000
            + cell.actor_count * 10_000
            + cell.move_queue_pressure * 1_000
            + cell.relay_fanout * 100
            + cell.tick_stage_micros / 1_000
            + cell.handover_failures * 10
    }

    fn cell_family_key(cell: &CellId) -> (u32, i32, i32, u8, u8) {
        (cell.world, cell.cx, cell.cy, cell.depth, cell.sub)
    }
}

#[derive(Debug, Default)]
struct OrchestratorMetricsCounters {
    registration_attempts: AtomicU64,
    unknown_worker_registrations: AtomicU64,
    assignment_snapshot_requests: AtomicU64,
    listing_requests: AtomicU64,
    watch_streams_started: AtomicU64,
    listing_updates: AtomicU64,
}

#[derive(Clone)]
struct OrchestratorService {
    config: Arc<Config>,
    assignments: Arc<RwLock<AssignmentMap>>,
    runtime: Arc<RwLock<HashMap<String, WorkerRuntime>>>,
    handovers: Arc<RwLock<HashMap<CellId, HandoverRuntime>>>,
    listing_tx: watch::Sender<AssignmentListing>,
    metrics: Arc<OrchestratorMetricsCounters>,
}

impl OrchestratorService {
    fn new(config: Config) -> Self {
        let config = Arc::new(config);
        let initial_assignments = config.initial_assignments();
        let initial_listing = listing_with_runtime(
            &config,
            &initial_assignments,
            &HashMap::new(),
            &HashMap::new(),
        );
        let (listing_tx, _) = watch::channel(initial_listing);
        Self {
            config,
            assignments: Arc::new(RwLock::new(initial_assignments)),
            runtime: Arc::new(RwLock::new(HashMap::new())),
            handovers: Arc::new(RwLock::new(HashMap::new())),
            listing_tx,
            metrics: Arc::new(OrchestratorMetricsCounters::default()),
        }
    }

    async fn snapshot_for(&self, worker_id: &str) -> AssignmentSnapshot {
        let assignments = self.assignments.read().await;
        let mut entries = Vec::new();
        if let Some(cells) = assignments.get(worker_id) {
            entries.reserve(cells.len());
            entries.extend(cells.iter().map(cell_to_assignment));
        }
        AssignmentSnapshot { cells: entries }
    }

    async fn listing(&self) -> AssignmentListing {
        let runtime = self.runtime.read().await.clone();
        // Handover commit mutates assignments while holding the handover lock, so read
        // handovers first to avoid pairing old assignments with a post-commit empty status.
        let handovers = self.handovers.read().await.clone();
        let assignments = self.assignments.read().await.clone();
        listing_with_runtime(&self.config, &assignments, &runtime, &handovers)
    }

    async fn publish_listing_if_changed(&self) {
        let listing = self.listing().await;
        if *self.listing_tx.borrow() == listing {
            return;
        }
        // `send_replace` updates the stored value even when there are no receivers yet.
        let _ = self.listing_tx.send_replace(listing);
        self.metrics.listing_updates.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn push_listing_for_test(&self, listing: AssignmentListing) {
        let _ = self.listing_tx.send(listing);
    }

    async fn worker_health(&self) -> Vec<WorkerHealth> {
        let runtime = self.runtime.read().await;
        let assignments = self.assignments.read().await;
        let mut worker_ids: Vec<&String> = self.config.workers.keys().collect();
        worker_ids.sort();

        worker_ids
            .into_iter()
            .map(|worker_id| {
                let configured = self
                    .config
                    .workers
                    .get(worker_id)
                    .expect("worker id from config keys");
                let runtime = runtime.get(worker_id.as_str());
                let runtime_addr = runtime.map(|rt| rt.addr.clone()).unwrap_or_default();
                WorkerHealth {
                    worker_id: worker_id.clone(),
                    configured_addr: configured.addr.clone(),
                    runtime_addr: runtime_addr.clone(),
                    registered: runtime.is_some(),
                    assigned_cells: assignments
                        .get(worker_id.as_str())
                        .map(Vec::len)
                        .unwrap_or_default() as u64,
                    last_seen_unix_secs: runtime
                        .map(|rt| unix_timestamp_secs(rt.last_seen))
                        .unwrap_or_default(),
                    addr_matches_config: runtime_addr.is_empty() || runtime_addr == configured.addr,
                }
            })
            .collect()
    }

    async fn health_snapshot(&self) -> OrchestratorHealth {
        let workers = self.worker_health().await;
        let configured_workers = workers.len() as u64;
        let registered_workers = workers.iter().filter(|w| w.registered).count() as u64;
        let assigned_cells = workers.iter().map(|w| w.assigned_cells).sum();
        let addr_mismatch_workers = workers
            .iter()
            .filter(|w| w.registered && !w.addr_matches_config)
            .count();
        let status = if registered_workers == configured_workers && addr_mismatch_workers == 0 {
            "SERVING"
        } else {
            "DEGRADED"
        };

        OrchestratorHealth {
            status: status.to_string(),
            configured_workers,
            registered_workers,
            assigned_cells,
            workers,
        }
    }

    async fn metrics_snapshot(&self) -> OrchestratorMetrics {
        let workers = self.worker_health().await;
        OrchestratorMetrics {
            configured_workers: workers.len() as u64,
            registered_workers: workers.iter().filter(|w| w.registered).count() as u64,
            assigned_cells: workers.iter().map(|w| w.assigned_cells).sum(),
            empty_workers: workers.iter().filter(|w| w.assigned_cells == 0).count() as u64,
            addr_mismatch_workers: workers
                .iter()
                .filter(|w| w.registered && !w.addr_matches_config)
                .count() as u64,
            registration_attempts: self.metrics.registration_attempts.load(Ordering::Relaxed),
            unknown_worker_registrations: self
                .metrics
                .unknown_worker_registrations
                .load(Ordering::Relaxed),
            assignment_snapshot_requests: self
                .metrics
                .assignment_snapshot_requests
                .load(Ordering::Relaxed),
            listing_requests: self.metrics.listing_requests.load(Ordering::Relaxed),
            watch_streams_started: self.metrics.watch_streams_started.load(Ordering::Relaxed),
            listing_updates: self.metrics.listing_updates.load(Ordering::Relaxed),
        }
    }

    async fn apply_handover_command(
        &self,
        request: HandoverCommandRequest,
    ) -> Result<HandoverCommandResponse, Status> {
        let parsed =
            ParsedHandoverCommand::try_from_request(request).map_err(Status::invalid_argument)?;
        if self.config.worker(&parsed.source_worker_id).is_none() {
            return Ok(handover_response(
                false,
                HandoverState::Idle,
                format!("unknown source_worker_id={}", parsed.source_worker_id),
                false,
            ));
        }
        if self.config.worker(&parsed.target_worker_id).is_none() {
            return Ok(handover_response(
                false,
                HandoverState::Idle,
                format!("unknown target_worker_id={}", parsed.target_worker_id),
                false,
            ));
        }
        let current_owner = {
            let assignments = self.assignments.read().await;
            owner_for_cell(&assignments, &parsed.cell).map(str::to_owned)
        };
        match current_owner.as_deref() {
            Some(owner) if owner == parsed.source_worker_id => {}
            Some(owner) => {
                return Ok(handover_response(
                    false,
                    HandoverState::Idle,
                    format!(
                        "source_worker_id={} does not own cell; current_owner={}",
                        parsed.source_worker_id, owner
                    ),
                    false,
                ));
            }
            None => {
                return Ok(handover_response(
                    false,
                    HandoverState::Idle,
                    "cell is not assigned to any configured worker",
                    false,
                ));
            }
        }
        let mut handovers = self.handovers.write().await;
        let current = handovers.get(&parsed.cell).cloned();

        if parsed.command == HandoverCommand::PreCopy {
            if let Some(active) = current {
                return Ok(handover_response(
                    false,
                    active.state,
                    format!(
                        "handover already active for cell with operation_id={}",
                        active.operation_id
                    ),
                    false,
                ));
            }
            handovers.insert(
                parsed.cell,
                HandoverRuntime {
                    operation_id: parsed.operation_id,
                    source_worker_id: parsed.source_worker_id,
                    target_worker_id: parsed.target_worker_id,
                    state: HandoverState::PreCopying,
                    commit_attempts: 0,
                },
            );
            drop(handovers);
            self.publish_listing_if_changed().await;
            return Ok(handover_response(
                true,
                HandoverState::PreCopying,
                "pre-copy accepted; assignments unchanged",
                false,
            ));
        }

        let Some(active) = current else {
            return Ok(handover_response(
                false,
                HandoverState::Idle,
                "handover must start with PreCopy",
                false,
            ));
        };

        if active.operation_id != parsed.operation_id
            || active.source_worker_id != parsed.source_worker_id
            || active.target_worker_id != parsed.target_worker_id
        {
            return Ok(handover_response(
                false,
                active.state,
                "handover command does not match the active operation",
                false,
            ));
        }

        let next_state = match (active.state, parsed.command) {
            (HandoverState::PreCopying, HandoverCommand::Freeze) => HandoverState::Frozen,
            (HandoverState::Frozen, HandoverCommand::Diff) => HandoverState::Diffing,
            (HandoverState::Diffing, HandoverCommand::Commit) => HandoverState::Committed,
            (_, HandoverCommand::Abort) => HandoverState::Aborted,
            _ => {
                return Ok(handover_response(
                    false,
                    active.state,
                    format!(
                        "invalid handover transition from {:?} with {:?}",
                        active.state, parsed.command
                    ),
                    false,
                ));
            }
        };

        let mut assignments_changed = false;
        if next_state == HandoverState::Committed {
            let target_registered = {
                let runtime = self.runtime.read().await;
                runtime.contains_key(&parsed.target_worker_id)
            };
            if !target_registered {
                let attempts = if let Some(active) = handovers.get_mut(&parsed.cell) {
                    active.commit_attempts = active.commit_attempts.saturating_add(1);
                    active.commit_attempts
                } else {
                    1
                };

                if attempts >= HANDOVER_COMMIT_RETRY_LIMIT {
                    handovers.remove(&parsed.cell);
                    drop(handovers);
                    self.publish_listing_if_changed().await;
                    return Ok(handover_response(
                        false,
                        HandoverState::Aborted,
                        format!(
                            "target_worker_id={} is not registered after {attempts} commit attempts; handover aborted before assignment transfer",
                            parsed.target_worker_id
                        ),
                        false,
                    ));
                }

                return Ok(handover_response(
                    false,
                    active.state,
                    format!(
                        "target_worker_id={} is not registered; commit should retry ({attempts}/{HANDOVER_COMMIT_RETRY_LIMIT})",
                        parsed.target_worker_id
                    ),
                    false,
                ));
            }

            let mut assignments = self.assignments.write().await;
            match transfer_cell_assignment(
                &mut assignments,
                parsed.cell,
                &parsed.source_worker_id,
                &parsed.target_worker_id,
            ) {
                Ok(changed) => assignments_changed = changed,
                Err(reason) => {
                    return Ok(handover_response(false, active.state, reason, false));
                }
            }
            handovers.remove(&parsed.cell);
        } else if next_state == HandoverState::Aborted {
            handovers.remove(&parsed.cell);
        } else if let Some(active) = handovers.get_mut(&parsed.cell) {
            active.state = next_state;
        }

        drop(handovers);
        self.publish_listing_if_changed().await;
        let reason = match next_state {
            HandoverState::Committed => "handover committed; assignments moved to target",
            HandoverState::Aborted => "handover aborted; assignments unchanged",
            _ => "handover command accepted; assignments unchanged",
        };
        Ok(handover_response(
            true,
            next_state,
            reason,
            assignments_changed,
        ))
    }
}

struct ParsedHandoverCommand {
    operation_id: String,
    cell: CellId,
    source_worker_id: String,
    target_worker_id: String,
    command: HandoverCommand,
}

impl ParsedHandoverCommand {
    fn try_from_request(request: HandoverCommandRequest) -> Result<Self, String> {
        let operation_id = request.operation_id.trim();
        if operation_id.is_empty() {
            return Err("operation_id must not be empty".to_string());
        }
        let Some(cell_assignment) = request.cell.as_ref() else {
            return Err("cell must be set".to_string());
        };
        let cell = assignment_to_cell_ref(cell_assignment).map_err(|e| e.to_string())?;

        let source_worker_id = request.source_worker_id.trim();
        if source_worker_id.is_empty() {
            return Err("source_worker_id must not be empty".to_string());
        }
        let target_worker_id = request.target_worker_id.trim();
        if target_worker_id.is_empty() {
            return Err("target_worker_id must not be empty".to_string());
        }
        if source_worker_id == target_worker_id {
            return Err("source_worker_id and target_worker_id must differ".to_string());
        }

        let command =
            HandoverCommand::try_from(request.command).unwrap_or(HandoverCommand::Unspecified);
        if command == HandoverCommand::Unspecified {
            return Err("handover command must be set".to_string());
        }
        Ok(Self {
            operation_id: operation_id.to_string(),
            cell,
            source_worker_id: source_worker_id.to_string(),
            target_worker_id: target_worker_id.to_string(),
            command,
        })
    }
}

fn handover_response(
    accepted: bool,
    state: HandoverState,
    reason: impl Into<String>,
    assignments_changed: bool,
) -> HandoverCommandResponse {
    HandoverCommandResponse {
        accepted,
        state: state as i32,
        reason: reason.into(),
        client_move_policy: client_move_policy_for(state) as i32,
        failure_policy: HandoverFailurePolicy::KeepSourceAndAbort as i32,
        assignments_changed,
    }
}

fn client_move_policy_for(state: HandoverState) -> HandoverClientMovePolicy {
    match state {
        HandoverState::Frozen | HandoverState::Diffing => {
            HandoverClientMovePolicy::RejectDuringFreeze
        }
        _ => HandoverClientMovePolicy::Allow,
    }
}

#[async_trait]
impl Orchestrator for OrchestratorService {
    type WatchAssignmentsStream =
        Pin<Box<dyn Stream<Item = Result<AssignmentListing, Status>> + Send + 'static>>;

    async fn register_worker(
        &self,
        request: Request<WorkerRegistration>,
    ) -> Result<Response<AssignmentSnapshot>, Status> {
        self.metrics
            .registration_attempts
            .fetch_add(1, Ordering::Relaxed);
        let req = request.into_inner();
        let worker_id = req.worker_id.trim();
        if worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id must not be empty"));
        }
        let addr = req.addr.trim();
        if addr.is_empty() {
            return Err(Status::invalid_argument("addr must not be empty"));
        }
        if let Err(e) = validate_worker_addr(addr, "addr") {
            return Err(Status::invalid_argument(e.to_string()));
        }

        let planned = self.config.worker(worker_id);
        let snapshot = self.snapshot_for(worker_id).await;
        let cell_count = snapshot.cells.len();

        // Config is the source of truth for assignments. Unknown workers should not be stored in
        // runtime state (they would never appear in listings anyway) to avoid unbounded growth.
        let Some(expected) = planned else {
            warn!(
                target: "orch",
                worker_id,
                addr,
                "worker registered but not defined in config"
            );
            self.metrics
                .unknown_worker_registrations
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(snapshot));
        };

        {
            let mut guard = self.runtime.write().await;
            guard.insert(
                worker_id.to_string(),
                WorkerRuntime {
                    addr: addr.to_string(),
                    last_seen: SystemTime::now(),
                },
            );
        }

        if expected.addr != addr {
            warn!(
                target: "orch",
                worker_id,
                expected_addr = %expected.addr,
                reported_addr = addr,
                "worker address mismatch"
            );
        }
        if cell_count == 0 {
            warn!(
                target: "orch",
                worker_id,
                addr,
                "worker registered but no cells assigned"
            );
        } else {
            info!(
                target: "orch",
                worker_id,
                addr,
                cells = cell_count,
                "worker registered"
            );
        }

        self.publish_listing_if_changed().await;
        Ok(Response::new(snapshot))
    }

    async fn get_assignments(
        &self,
        request: Request<AssignmentQuery>,
    ) -> Result<Response<AssignmentSnapshot>, Status> {
        let req = request.into_inner();
        let worker_id = req.worker_id.trim();
        if worker_id.is_empty() {
            return Err(Status::invalid_argument("worker_id must not be empty"));
        }
        self.metrics
            .assignment_snapshot_requests
            .fetch_add(1, Ordering::Relaxed);

        {
            let mut guard = self.runtime.write().await;
            if let Some(entry) = guard.get_mut(worker_id) {
                entry.last_seen = SystemTime::now();
                tracing::debug!(
                    target: "orch",
                    worker_id,
                    addr = %entry.addr,
                    "served assignment snapshot"
                );
            } else {
                warn!(
                    target: "orch",
                    worker_id,
                    "assignment requested before registration"
                );
            }
        }

        let snapshot = self.snapshot_for(worker_id).await;
        Ok(Response::new(snapshot))
    }

    async fn list_assignments(
        &self,
        _request: Request<ListAssignmentsRequest>,
    ) -> Result<Response<AssignmentListing>, Status> {
        self.metrics
            .listing_requests
            .fetch_add(1, Ordering::Relaxed);
        let listing = self.listing().await;
        Ok(Response::new(listing))
    }

    async fn watch_assignments(
        &self,
        _request: Request<WatchAssignmentsRequest>,
    ) -> Result<Response<Self::WatchAssignmentsStream>, Status> {
        self.metrics
            .watch_streams_started
            .fetch_add(1, Ordering::Relaxed);
        let rx = self.listing_tx.subscribe();
        let stream = WatchStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_health(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<OrchestratorHealth>, Status> {
        Ok(Response::new(self.health_snapshot().await))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<OrchestratorMetrics>, Status> {
        Ok(Response::new(self.metrics_snapshot().await))
    }

    async fn submit_handover_command(
        &self,
        request: Request<HandoverCommandRequest>,
    ) -> Result<Response<HandoverCommandResponse>, Status> {
        let response = self.apply_handover_command(request.into_inner()).await?;
        Ok(Response::new(response))
    }
}

fn cell_to_assignment(cell: &CellId) -> Assignment {
    Assignment {
        world: cell.world,
        cx: cell.cx,
        cy: cell.cy,
        depth: cell.depth as u32,
        sub: cell.sub as u32,
    }
}

fn assignment_to_cell_ref(assignment: &Assignment) -> Result<CellId> {
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
        .ok_or_else(|| anyhow!("no socket address resolved for {raw}"))
}

async fn load_metrics_listener() -> Result<Option<TcpListener>> {
    let Ok(raw) = std::env::var("TESSERA_ORCH_METRICS_ADDR") else {
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
        .with_context(|| format!("resolve TESSERA_ORCH_METRICS_ADDR={trimmed}"))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind prometheus metrics listener at {addr}"))?;
    Ok(Some(listener))
}

async fn serve_prometheus_metrics(
    listener: TcpListener,
    service: OrchestratorService,
) -> Result<()> {
    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .context("accept prometheus metrics connection")?;
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_prometheus_metrics_request(stream, service).await {
                tracing::debug!(
                    target: "orch",
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
    service: OrchestratorService,
) -> Result<()> {
    let mut buf = [0_u8; 1024];
    let n = stream
        .read(&mut buf)
        .await
        .context("read prometheus metrics request")?;
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
        let snapshot = service.metrics_snapshot().await;
        let body = format_prometheus_metrics(&snapshot);
        write_http_response(
            &mut stream,
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            &body,
        )
        .await?;
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
        .context("write prometheus metrics response")?;
    stream
        .shutdown()
        .await
        .context("shutdown prometheus metrics response")?;
    Ok(())
}

fn format_prometheus_metrics(metrics: &OrchestratorMetrics) -> String {
    let mut out = String::new();
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_orch_configured_workers",
        "Configured workers in the orchestrator assignment map.",
        metrics.configured_workers,
    );
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_orch_registered_workers",
        "Workers that have registered with the orchestrator.",
        metrics.registered_workers,
    );
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_orch_assigned_cells",
        "Cells currently assigned by orchestrator config.",
        metrics.assigned_cells,
    );
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_orch_empty_workers",
        "Configured workers with no assigned cells.",
        metrics.empty_workers,
    );
    push_prometheus_metric(
        &mut out,
        "gauge",
        "tessera_orch_addr_mismatch_workers",
        "Registered workers whose runtime address differs from config.",
        metrics.addr_mismatch_workers,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_orch_registration_attempts_total",
        "Worker registration attempts observed by the orchestrator.",
        metrics.registration_attempts,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_orch_unknown_worker_registrations_total",
        "Registration attempts from workers absent from orchestrator config.",
        metrics.unknown_worker_registrations,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_orch_assignment_snapshot_requests_total",
        "GetAssignments requests served by the orchestrator.",
        metrics.assignment_snapshot_requests,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_orch_listing_requests_total",
        "ListAssignments requests served by the orchestrator.",
        metrics.listing_requests,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_orch_watch_streams_started_total",
        "WatchAssignments streams started by the orchestrator.",
        metrics.watch_streams_started,
    );
    push_prometheus_metric(
        &mut out,
        "counter",
        "tessera_orch_listing_updates_total",
        "Assignment listing updates published to watchers.",
        metrics.listing_updates,
    );
    out
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

fn unix_timestamp_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::split_merge_planner::{
        SplitMergeActivePlan, SplitMergeCellMetrics, SplitMergeMetricsSnapshot, SplitMergePlanKind,
        SplitMergePlannerConfig, plan_split_merge,
    };
    use super::*;
    use tokio_stream::StreamExt;
    use tonic::Request;

    fn two_worker_handover_config() -> Config {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        workers.insert(
            "worker-b".to_string(),
            WorkerStatic {
                addr: "10.0.0.2:5001".to_string(),
                cells: Vec::new(),
            },
        );
        Config { workers }
    }

    fn handover_request(command: HandoverCommand) -> HandoverCommandRequest {
        HandoverCommandRequest {
            operation_id: "handover-1".to_string(),
            cell: Some(cell_to_assignment(&CellId::grid(0, 0, 0))),
            source_worker_id: "worker-a".to_string(),
            target_worker_id: "worker-b".to_string(),
            command: command as i32,
        }
    }

    fn split_metrics(cell: CellId) -> SplitMergeCellMetrics {
        SplitMergeCellMetrics {
            cell,
            actor_count: 0,
            move_queue_pressure: 0,
            tick_stage_micros: 0,
            relay_fanout: 0,
            handover_failures: 0,
            high_pressure_windows: 3,
            cell_age_secs: 60,
            cooldown_remaining_secs: 0,
            active_handover: false,
        }
    }

    #[test]
    fn split_merge_planner_ranks_candidates_with_hysteresis() {
        let config = SplitMergePlannerConfig {
            max_active_plans_per_world: 2,
            max_handover_ops_per_interval: 2,
            max_cells_moved_per_interval: 2,
            ..SplitMergePlannerConfig::default()
        };
        let mut lower_score = split_metrics(CellId::grid(0, 0, 0));
        lower_score.actor_count = 120;
        lower_score.move_queue_pressure = 64;
        let mut higher_score = split_metrics(CellId::grid(0, 1, 0));
        higher_score.actor_count = 200;
        higher_score.move_queue_pressure = 64;
        higher_score.relay_fanout = 64;
        let mut too_few_windows = split_metrics(CellId::grid(0, 2, 0));
        too_few_windows.actor_count = 200;
        too_few_windows.move_queue_pressure = 64;
        too_few_windows.high_pressure_windows = 2;
        let mut cooling_down = split_metrics(CellId::grid(0, 3, 0));
        cooling_down.actor_count = 200;
        cooling_down.move_queue_pressure = 64;
        cooling_down.cooldown_remaining_secs = 30;
        let mut handover_active = split_metrics(CellId::grid(0, 4, 0));
        handover_active.actor_count = 200;
        handover_active.move_queue_pressure = 64;
        handover_active.active_handover = true;

        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: vec![
                    lower_score,
                    too_few_windows,
                    higher_score,
                    cooling_down,
                    handover_active,
                ],
                active_plans: Vec::new(),
            },
            &config,
        );

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].cell, CellId::grid(0, 1, 0));
        assert_eq!(plans[0].pressure_signals, 3);
        assert_eq!(plans[1].cell, CellId::grid(0, 0, 0));
        assert_eq!(plans[1].pressure_signals, 2);
    }

    #[test]
    fn split_merge_planner_enforces_churn_budget() {
        let config = SplitMergePlannerConfig {
            max_active_plans_per_world: 3,
            max_handover_ops_per_interval: 1,
            max_cells_moved_per_interval: 1,
            ..SplitMergePlannerConfig::default()
        };
        let mut first = split_metrics(CellId::grid(0, 0, 0));
        first.actor_count = 150;
        first.move_queue_pressure = 64;
        let mut second = split_metrics(CellId::grid(0, 1, 0));
        second.actor_count = 220;
        second.move_queue_pressure = 80;

        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: vec![first, second],
                active_plans: Vec::new(),
            },
            &config,
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].cell, CellId::grid(0, 1, 0));
    }

    #[test]
    fn split_merge_planner_blocks_overlapping_active_plans() {
        let config = SplitMergePlannerConfig {
            max_active_plans_per_world: 2,
            max_handover_ops_per_interval: 2,
            max_cells_moved_per_interval: 2,
            ..SplitMergePlannerConfig::default()
        };
        let mut blocked = split_metrics(CellId::grid(0, 0, 0));
        blocked.actor_count = 250;
        blocked.move_queue_pressure = 100;
        let mut allowed = split_metrics(CellId::grid(0, 1, 0));
        allowed.actor_count = 120;
        allowed.move_queue_pressure = 64;

        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: vec![blocked, allowed],
                active_plans: vec![SplitMergeActivePlan {
                    kind: SplitMergePlanKind::Split,
                    cell: CellId::grid(0, 0, 0),
                }],
            },
            &config,
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].cell, CellId::grid(0, 1, 0));

        let world_budget = SplitMergePlannerConfig {
            max_active_plans_per_world: 1,
            max_handover_ops_per_interval: 2,
            max_cells_moved_per_interval: 2,
            ..SplitMergePlannerConfig::default()
        };
        let mut candidate = split_metrics(CellId::grid(0, 2, 0));
        candidate.actor_count = 120;
        candidate.move_queue_pressure = 64;
        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: vec![candidate],
                active_plans: vec![SplitMergeActivePlan {
                    kind: SplitMergePlanKind::Split,
                    cell: CellId::grid(0, 0, 0),
                }],
            },
            &world_budget,
        );
        assert!(plans.is_empty());
    }

    #[tokio::test]
    async fn split_merge_planner_is_not_connected_to_assignment_listing() {
        let service = OrchestratorService::new(two_worker_handover_config());
        let before = service.listing().await;
        let mut candidate = split_metrics(CellId::grid(0, 0, 0));
        candidate.actor_count = 120;
        candidate.move_queue_pressure = 64;

        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: vec![candidate],
                active_plans: Vec::new(),
            },
            &SplitMergePlannerConfig::default(),
        );
        let after = service.listing().await;

        assert_eq!(plans.len(), 1);
        assert_eq!(before, after);
    }

    #[test]
    fn parse_config_single_worker() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "worker-a",
                    "addr": "10.0.0.1:5001",
                    "cells": [
                        {"world": 1, "cx": 2, "cy": -3},
                        {"world": 1, "cx": 3, "cy": -3, "depth": 1, "sub": 2}
                    ]
                }
            ]
        }"#;

        let cfg = parse_config(json).expect("parse config");
        assert!(cfg.worker("worker-a").is_some());
        let assignments = cfg.initial_assignments();
        let cells = assignments.get("worker-a").expect("cells");
        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0].world, 1);
        assert_eq!(cells[0].cx, 2);
        assert_eq!(cells[0].cy, -3);
        assert_eq!(cells[1].depth, 1);
        assert_eq!(cells[1].sub, 2);
        assert!(!assignments.contains_key("missing"));
    }

    #[test]
    fn parse_config_invalid_json() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "worker-a"
                    "addr": "10.0.0.1:5001"
                }
            ]
        }"#;

        let err = parse_config(json).expect_err("should fail");
        assert!(
            err.to_string()
                .contains("parse orchestrator config as JSON")
        );
    }

    #[test]
    fn parse_config_rejects_empty_worker_id() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "",
                    "addr": "127.0.0.1:5001",
                    "cells": [{"world": 0, "cx": 0, "cy": 0}]
                }
            ]
        }"#;

        let err = parse_config(json).expect_err("should fail");
        assert!(err.to_string().contains("worker id"));
    }

    #[test]
    fn parse_config_rejects_empty_worker_addr() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "worker-a",
                    "addr": "   ",
                    "cells": [{"world": 0, "cx": 0, "cy": 0}]
                }
            ]
        }"#;

        let err = parse_config(json).expect_err("should fail");
        assert!(err.to_string().contains("worker addr"));
    }

    #[test]
    fn parse_config_rejects_missing_port_worker_addr() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "worker-a",
                    "addr": "worker-a",
                    "cells": [{"world": 0, "cx": 0, "cy": 0}]
                }
            ]
        }"#;

        let err = parse_config(json).expect_err("should fail");
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn parse_config_rejects_unspecified_worker_addr() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "worker-a",
                    "addr": "0.0.0.0:5001",
                    "cells": [{"world": 0, "cx": 0, "cy": 0}]
                }
            ]
        }"#;

        let err = parse_config(json).expect_err("should fail");
        assert!(err.to_string().contains("unspecified"));
    }

    #[test]
    fn parse_config_rejects_duplicate_cells() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "worker-a",
                    "addr": "127.0.0.1:5001",
                    "cells": [{"world": 0, "cx": 0, "cy": 0}]
                },
                {
                    "id": "worker-b",
                    "addr": "127.0.0.1:5002",
                    "cells": [{"world": 0, "cx": 0, "cy": 0}]
                }
            ]
        }"#;

        let err = parse_config(json).expect_err("should fail");
        assert!(err.to_string().contains("assigned to both"));
    }

    #[test]
    fn parse_config_rejects_duplicate_worker_id() {
        let json = r#"
        {
            "workers": [
                {
                    "id": "worker-a",
                    "addr": "127.0.0.1:5001",
                    "cells": [{"world": 0, "cx": 0, "cy": 0}]
                },
                {
                    "id": "worker-a",
                    "addr": "127.0.0.1:5002",
                    "cells": [{"world": 0, "cx": 1, "cy": 0}]
                }
            ]
        }"#;

        let err = parse_config(json).expect_err("should fail");
        assert!(err.to_string().contains("duplicate worker id"));
    }

    #[tokio::test]
    async fn register_and_get_assignments() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![
                    CellId::grid(0, 0, 0),
                    CellId {
                        world: 0,
                        cx: 1,
                        cy: 0,
                        depth: 1,
                        sub: 0,
                    },
                ],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let response = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.1:5001".into(),
            }))
            .await
            .expect("register worker")
            .into_inner();
        assert_eq!(response.cells.len(), 2);

        let snapshot = service
            .get_assignments(Request::new(AssignmentQuery {
                worker_id: "worker-a".into(),
            }))
            .await
            .expect("get assignments")
            .into_inner();
        assert_eq!(snapshot.cells.len(), 2);
        assert_eq!(snapshot.cells[0].cx, 0);
        assert_eq!(snapshot.cells[1].depth, 1);

        let empty_snapshot = service
            .get_assignments(Request::new(AssignmentQuery {
                worker_id: "worker-unknown".into(),
            }))
            .await
            .expect("get assignments for unknown worker")
            .into_inner();
        assert!(empty_snapshot.cells.is_empty());
    }

    #[tokio::test]
    async fn register_worker_updates_listing_addr() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let listing = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list assignments")
            .into_inner();
        assert_eq!(listing.workers.len(), 1);
        assert_eq!(listing.workers[0].addr, "10.0.0.1:5001");

        service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.9:5001".into(),
            }))
            .await
            .expect("register worker");

        let listing = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list assignments")
            .into_inner();
        assert_eq!(listing.workers.len(), 1);
        assert_eq!(listing.workers[0].addr, "10.0.0.9:5001");
    }

    #[tokio::test]
    async fn register_unknown_worker_does_not_store_runtime() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let snapshot = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-unknown".into(),
                addr: "10.0.0.9:5001".into(),
            }))
            .await
            .expect("register unknown worker")
            .into_inner();
        assert!(snapshot.cells.is_empty());

        let runtime = service.runtime.read().await;
        assert!(runtime.is_empty());
    }

    #[tokio::test]
    async fn register_worker_rejects_empty_addr() {
        let service = OrchestratorService::new(Config::default_single_cell());
        let err = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-local".into(),
                addr: "   ".into(),
            }))
            .await
            .expect_err("should reject empty addr");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn register_worker_rejects_missing_port_addr() {
        let service = OrchestratorService::new(Config::default_single_cell());
        let err = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-local".into(),
                addr: "worker-a".into(),
            }))
            .await
            .expect_err("should reject missing port");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn register_worker_rejects_unspecified_addr() {
        let service = OrchestratorService::new(Config::default_single_cell());
        let err = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-local".into(),
                addr: "0.0.0.0:5001".into(),
            }))
            .await
            .expect_err("should reject unspecified addr");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn register_worker_rejects_empty_id() {
        let service = OrchestratorService::new(Config::default_single_cell());
        let err = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "   ".into(),
                addr: "127.0.0.1:5001".into(),
            }))
            .await
            .expect_err("should reject empty id");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_assignments_rejects_empty_id() {
        let service = OrchestratorService::new(Config::default_single_cell());
        let err = service
            .get_assignments(Request::new(AssignmentQuery {
                worker_id: "   ".into(),
            }))
            .await
            .expect_err("should reject empty id");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn list_assignments_snapshot() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        workers.insert(
            "worker-b".to_string(),
            WorkerStatic {
                addr: "10.0.0.2:5001".to_string(),
                cells: vec![CellId::grid(0, 1, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let listing = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list assignments")
            .into_inner();
        assert_eq!(listing.workers.len(), 2);
        let mut worker_ids = listing
            .workers
            .iter()
            .map(|b| b.worker_id.as_str())
            .collect::<Vec<_>>();
        worker_ids.sort();
        assert_eq!(worker_ids, vec!["worker-a", "worker-b"]);
    }

    #[tokio::test]
    async fn watch_assignments_initial_value_reflects_prior_register() {
        use tokio::time::{Duration, timeout};

        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.9:5001".into(),
            }))
            .await
            .expect("register worker");

        let response = service
            .watch_assignments(Request::new(
                tessera_proto::orch::v1::WatchAssignmentsRequest {},
            ))
            .await
            .expect("watch assignments");
        let mut stream = response.into_inner();

        let first = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for first listing")
            .expect("stream closed")
            .expect("first listing");
        assert_eq!(first.workers.len(), 1);
        assert_eq!(first.workers[0].addr, "10.0.0.9:5001");
    }

    #[tokio::test]
    async fn watch_assignments_emits_runtime_addr_change() {
        use tokio::time::{Duration, timeout};

        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let response = service
            .watch_assignments(Request::new(
                tessera_proto::orch::v1::WatchAssignmentsRequest {},
            ))
            .await
            .expect("watch assignments");
        let mut stream = response.into_inner();

        let first = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for first listing")
            .expect("stream closed")
            .expect("first listing");
        assert_eq!(first.workers.len(), 1);
        assert_eq!(first.workers[0].addr, "10.0.0.1:5001");

        service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.9:5001".into(),
            }))
            .await
            .expect("register worker");

        let second = timeout(Duration::from_millis(200), stream.next())
            .await
            .expect("timeout waiting for second listing")
            .expect("stream closed")
            .expect("second listing");
        assert_eq!(second.workers.len(), 1);
        assert_eq!(second.workers[0].addr, "10.0.0.9:5001");
    }

    #[tokio::test]
    async fn watch_assignments_streams_listing_updates() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let response = service
            .watch_assignments(Request::new(
                tessera_proto::orch::v1::WatchAssignmentsRequest {},
            ))
            .await
            .expect("watch assignments");
        let mut stream = response.into_inner();

        let first = stream
            .next()
            .await
            .expect("stream closed unexpectedly")
            .expect("first listing");
        assert_eq!(first.workers.len(), 1);

        let mut updated = first.clone();
        updated.workers.push(AssignmentBundle {
            worker_id: "worker-b".to_string(),
            addr: "10.0.0.2:5001".to_string(),
            cells: vec![Assignment {
                world: 0,
                cx: 1,
                cy: 0,
                depth: 0,
                sub: 0,
            }],
        });

        service.push_listing_for_test(updated.clone());

        let second = stream
            .next()
            .await
            .expect("stream closed unexpectedly")
            .expect("second listing");
        assert_eq!(second, updated);
    }

    #[tokio::test]
    async fn health_reports_registration_state() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let health = service
            .get_health(Request::new(HealthCheckRequest {}))
            .await
            .expect("get health")
            .into_inner();
        assert_eq!(health.status, "DEGRADED");
        assert_eq!(health.configured_workers, 1);
        assert_eq!(health.registered_workers, 0);
        assert_eq!(health.assigned_cells, 1);
        assert_eq!(health.workers.len(), 1);
        assert!(!health.workers[0].registered);

        service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.1:5001".into(),
            }))
            .await
            .expect("register worker");

        let health = service
            .get_health(Request::new(HealthCheckRequest {}))
            .await
            .expect("get health")
            .into_inner();
        assert_eq!(health.status, "SERVING");
        assert_eq!(health.registered_workers, 1);
        assert_eq!(health.workers[0].runtime_addr, "10.0.0.1:5001");
        assert!(health.workers[0].addr_matches_config);
        assert!(health.workers[0].last_seen_unix_secs > 0);
    }

    #[tokio::test]
    async fn metrics_count_requests_and_runtime_state() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0), CellId::grid(0, 1, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });

        let _ = service
            .get_assignments(Request::new(AssignmentQuery {
                worker_id: "worker-a".into(),
            }))
            .await
            .expect("get assignments");
        let _ = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list assignments");
        let _ = service
            .watch_assignments(Request::new(WatchAssignmentsRequest {}))
            .await
            .expect("watch assignments");
        let _ = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.9:5001".into(),
            }))
            .await
            .expect("register worker");
        let _ = service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-unknown".into(),
                addr: "10.0.0.8:5001".into(),
            }))
            .await
            .expect("register unknown worker");

        let metrics = service
            .get_metrics(Request::new(GetMetricsRequest {}))
            .await
            .expect("get metrics")
            .into_inner();
        assert_eq!(metrics.configured_workers, 1);
        assert_eq!(metrics.registered_workers, 1);
        assert_eq!(metrics.assigned_cells, 2);
        assert_eq!(metrics.empty_workers, 0);
        assert_eq!(metrics.addr_mismatch_workers, 1);
        assert_eq!(metrics.registration_attempts, 2);
        assert_eq!(metrics.unknown_worker_registrations, 1);
        assert_eq!(metrics.assignment_snapshot_requests, 1);
        assert_eq!(metrics.listing_requests, 1);
        assert_eq!(metrics.watch_streams_started, 1);
        assert_eq!(metrics.listing_updates, 1);
    }

    #[tokio::test]
    async fn handover_commands_follow_safe_skeleton_sequence() {
        let service = OrchestratorService::new(two_worker_handover_config());

        let pre_copy = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::PreCopy)))
            .await
            .expect("submit pre-copy")
            .into_inner();
        assert!(pre_copy.accepted);
        assert_eq!(pre_copy.state, HandoverState::PreCopying as i32);
        assert_eq!(
            pre_copy.client_move_policy,
            HandoverClientMovePolicy::Allow as i32
        );
        assert_eq!(
            pre_copy.failure_policy,
            HandoverFailurePolicy::KeepSourceAndAbort as i32
        );
        assert!(!pre_copy.assignments_changed);

        let freeze = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::Freeze)))
            .await
            .expect("submit freeze")
            .into_inner();
        assert!(freeze.accepted);
        assert_eq!(freeze.state, HandoverState::Frozen as i32);
        assert_eq!(
            freeze.client_move_policy,
            HandoverClientMovePolicy::RejectDuringFreeze as i32
        );
        assert!(!freeze.assignments_changed);

        let frozen_listing = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list frozen assignments")
            .into_inner();
        assert_eq!(frozen_listing.handovers.len(), 1);
        let frozen_handover = &frozen_listing.handovers[0];
        assert_eq!(frozen_handover.operation_id, "handover-1");
        assert_eq!(frozen_handover.source_worker_id, "worker-a");
        assert_eq!(frozen_handover.target_worker_id, "worker-b");
        let frozen_cell = frozen_handover.cell.as_ref().expect("handover cell");
        assert_eq!(frozen_cell.world, 0);
        assert_eq!(frozen_cell.cx, 0);
        assert_eq!(frozen_cell.cy, 0);
        assert_eq!(frozen_handover.state, HandoverState::Frozen as i32);
        assert_eq!(
            frozen_handover.client_move_policy,
            HandoverClientMovePolicy::RejectDuringFreeze as i32
        );

        let diff = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::Diff)))
            .await
            .expect("submit diff")
            .into_inner();
        assert!(diff.accepted);
        assert_eq!(diff.state, HandoverState::Diffing as i32);
        assert_eq!(
            diff.client_move_policy,
            HandoverClientMovePolicy::RejectDuringFreeze as i32
        );
        assert!(!diff.assignments_changed);

        service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-b".into(),
                addr: "10.0.0.2:5001".into(),
            }))
            .await
            .expect("register target worker");

        let commit = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::Commit)))
            .await
            .expect("submit commit")
            .into_inner();
        assert!(commit.accepted);
        assert_eq!(commit.state, HandoverState::Committed as i32);
        assert_eq!(
            commit.client_move_policy,
            HandoverClientMovePolicy::Allow as i32
        );
        assert!(commit.assignments_changed);

        let listing = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list assignments")
            .into_inner();
        assert!(listing.handovers.is_empty());
        let source = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("source worker listing");
        assert!(source.cells.is_empty());
        let target = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-b")
            .expect("target worker listing");
        assert_eq!(target.cells.len(), 1);
        assert_eq!(target.cells[0].cx, 0);

        let target_snapshot = service
            .get_assignments(Request::new(AssignmentQuery {
                worker_id: "worker-b".into(),
            }))
            .await
            .expect("get target assignments")
            .into_inner();
        assert_eq!(target_snapshot.cells.len(), 1);
    }

    #[tokio::test]
    async fn handover_commit_waits_for_registered_target() {
        let service = OrchestratorService::new(two_worker_handover_config());
        for command in [
            HandoverCommand::PreCopy,
            HandoverCommand::Freeze,
            HandoverCommand::Diff,
        ] {
            service
                .submit_handover_command(Request::new(handover_request(command)))
                .await
                .expect("advance handover");
        }

        let rejected = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::Commit)))
            .await
            .expect("submit commit")
            .into_inner();

        assert!(!rejected.accepted);
        assert_eq!(rejected.state, HandoverState::Diffing as i32);
        assert!(rejected.reason.contains("commit should retry"));
        assert!(!rejected.assignments_changed);

        let listing = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list assignments")
            .into_inner();
        assert_eq!(listing.handovers.len(), 1);
        let source = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("source worker listing");
        assert_eq!(source.cells.len(), 1);
    }

    #[tokio::test]
    async fn handover_commit_retry_budget_aborts_before_assignment_transfer() {
        let service = OrchestratorService::new(two_worker_handover_config());
        for command in [
            HandoverCommand::PreCopy,
            HandoverCommand::Freeze,
            HandoverCommand::Diff,
        ] {
            service
                .submit_handover_command(Request::new(handover_request(command)))
                .await
                .expect("advance handover");
        }

        for attempt in 1..HANDOVER_COMMIT_RETRY_LIMIT {
            let rejected = service
                .submit_handover_command(Request::new(handover_request(HandoverCommand::Commit)))
                .await
                .expect("submit retryable commit")
                .into_inner();
            assert!(!rejected.accepted);
            assert_eq!(rejected.state, HandoverState::Diffing as i32);
            assert!(
                rejected
                    .reason
                    .contains(&format!("commit should retry ({attempt}/"))
            );
            assert!(!rejected.assignments_changed);
        }

        let aborted = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::Commit)))
            .await
            .expect("submit exhausted commit")
            .into_inner();
        assert!(!aborted.accepted);
        assert_eq!(aborted.state, HandoverState::Aborted as i32);
        assert!(
            aborted
                .reason
                .contains("aborted before assignment transfer")
        );
        assert!(!aborted.assignments_changed);

        let listing = service
            .list_assignments(Request::new(ListAssignmentsRequest {}))
            .await
            .expect("list assignments")
            .into_inner();
        assert!(listing.handovers.is_empty());
        let source = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("source worker listing");
        assert_eq!(source.cells.len(), 1);
        let target = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-b")
            .expect("target worker listing");
        assert!(target.cells.is_empty());
    }

    #[tokio::test]
    async fn handover_rejects_out_of_order_command() {
        let service = OrchestratorService::new(two_worker_handover_config());

        let rejected = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::Diff)))
            .await
            .expect("submit diff")
            .into_inner();

        assert!(!rejected.accepted);
        assert_eq!(rejected.state, HandoverState::Idle as i32);
        assert!(rejected.reason.contains("PreCopy"));
        assert!(!rejected.assignments_changed);
    }

    #[tokio::test]
    async fn handover_rejects_source_that_does_not_own_cell() {
        let service = OrchestratorService::new(two_worker_handover_config());
        let mut req = handover_request(HandoverCommand::PreCopy);
        req.source_worker_id = "worker-b".to_string();
        req.target_worker_id = "worker-a".to_string();

        let rejected = service
            .submit_handover_command(Request::new(req))
            .await
            .expect("submit pre-copy")
            .into_inner();

        assert!(!rejected.accepted);
        assert_eq!(rejected.state, HandoverState::Idle as i32);
        assert!(rejected.reason.contains("current_owner=worker-a"));
        assert!(!rejected.assignments_changed);
    }

    #[tokio::test]
    async fn handover_rejects_mismatched_active_operation() {
        let service = OrchestratorService::new(two_worker_handover_config());
        service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::PreCopy)))
            .await
            .expect("submit pre-copy");

        let mut req = handover_request(HandoverCommand::Freeze);
        req.operation_id = "handover-2".to_string();
        let rejected = service
            .submit_handover_command(Request::new(req))
            .await
            .expect("submit mismatched freeze")
            .into_inner();

        assert!(!rejected.accepted);
        assert_eq!(rejected.state, HandoverState::PreCopying as i32);
        assert!(rejected.reason.contains("does not match"));
    }

    #[tokio::test]
    async fn handover_abort_clears_active_operation() {
        let service = OrchestratorService::new(two_worker_handover_config());
        service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::PreCopy)))
            .await
            .expect("submit pre-copy");

        let aborted = service
            .submit_handover_command(Request::new(handover_request(HandoverCommand::Abort)))
            .await
            .expect("submit abort")
            .into_inner();
        assert!(aborted.accepted);
        assert_eq!(aborted.state, HandoverState::Aborted as i32);
        assert!(!aborted.assignments_changed);

        let mut next = handover_request(HandoverCommand::PreCopy);
        next.operation_id = "handover-2".to_string();
        let accepted = service
            .submit_handover_command(Request::new(next))
            .await
            .expect("submit next pre-copy")
            .into_inner();
        assert!(accepted.accepted);
        assert_eq!(accepted.state, HandoverState::PreCopying as i32);
    }

    #[test]
    fn prometheus_metrics_text_uses_snapshot_values() {
        let metrics = OrchestratorMetrics {
            configured_workers: 2,
            registered_workers: 1,
            assigned_cells: 8,
            empty_workers: 0,
            addr_mismatch_workers: 1,
            registration_attempts: 3,
            unknown_worker_registrations: 1,
            assignment_snapshot_requests: 5,
            listing_requests: 7,
            watch_streams_started: 11,
            listing_updates: 13,
        };

        let text = format_prometheus_metrics(&metrics);
        assert!(text.contains("# TYPE tessera_orch_configured_workers gauge\n"));
        assert!(text.contains("tessera_orch_configured_workers 2\n"));
        assert!(text.contains("# TYPE tessera_orch_registration_attempts_total counter\n"));
        assert!(text.contains("tessera_orch_registration_attempts_total 3\n"));
        assert!(text.contains("tessera_orch_listing_updates_total 13\n"));
    }

    #[tokio::test]
    async fn prometheus_metrics_http_serves_current_snapshot() {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: vec![CellId::grid(0, 0, 0)],
            },
        );
        let service = OrchestratorService::new(Config { workers });
        service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.1:5001".into(),
            }))
            .await
            .expect("register worker");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind metrics listener");
        let addr = listener.local_addr().expect("metrics listener addr");
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept metrics request");
            handle_prometheus_metrics_request(stream, server_service)
                .await
                .expect("handle metrics request");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect metrics");
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write metrics request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read metrics response");
        server.await.expect("metrics server task");

        let response = String::from_utf8(response).expect("utf8 metrics response");
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"));
        assert!(response.contains("tessera_orch_registered_workers 1\n"));
        assert!(response.contains("tessera_orch_registration_attempts_total 1\n"));
    }
}
