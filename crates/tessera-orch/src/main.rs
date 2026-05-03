use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tessera_core::{
    CellChildFamilyKind, CellId, Envelope, MAX_FRAME_LEN, MergeReplayTarget, SplitReplayTarget,
    WorkerRelayMsg, encode_frame,
};
use tessera_proto::orch::v1::orchestrator_server::{Orchestrator, OrchestratorServer};
use tessera_proto::orch::v1::{
    Assignment, AssignmentBundle, AssignmentListing, AssignmentQuery, AssignmentSnapshot,
    GetMetricsRequest, HandoverClientMovePolicy, HandoverCommand, HandoverCommandRequest,
    HandoverCommandResponse, HandoverFailurePolicy, HandoverState, HandoverStatus,
    HealthCheckRequest, ListAssignmentsRequest, MergeActivationRequest, MergeActivationResponse,
    MergeActivationState, OrchestratorHealth, OrchestratorMetrics, SplitActivationRequest,
    SplitActivationResponse, SplitActivationState, StagedSplitChildAssignment,
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
const ASSIGNMENT_STATE_SCHEMA: &str = "tessera.orch.assignment_state.v1";
const OPERATION_LEDGER_SCHEMA: &str = "tessera.orch.operation_ledger.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SplitActivationMode {
    Disabled,
    Manual,
}

fn load_split_activation_mode() -> Result<SplitActivationMode> {
    let Ok(raw) = std::env::var("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION") else {
        return Ok(SplitActivationMode::Disabled);
    };
    let mode = raw.trim();
    if mode.is_empty() || mode.eq_ignore_ascii_case("off") || mode.eq_ignore_ascii_case("disabled")
    {
        return Ok(SplitActivationMode::Disabled);
    }
    if mode.eq_ignore_ascii_case("manual") {
        return Ok(SplitActivationMode::Manual);
    }
    Err(anyhow!(
        "TESSERA_ORCH_SPLIT_MERGE_ACTIVATION must be unset, off, disabled, or manual"
    ))
}

fn load_assignment_state_path() -> Result<Option<PathBuf>> {
    let Ok(raw) = std::env::var("TESSERA_ORCH_ASSIGNMENT_STATE_PATH") else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("disabled")
    {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(trimmed)))
}

fn load_operation_ledger_path() -> Result<Option<PathBuf>> {
    let Ok(raw) = std::env::var("TESSERA_ORCH_OPERATION_LEDGER_PATH") else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("disabled")
    {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(trimmed)))
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config = load_config().context("load orchestrator config")?;
    let split_activation_mode =
        load_split_activation_mode().context("load split/merge activation mode")?;
    let assignment_state_path =
        load_assignment_state_path().context("load orchestrator assignment state path")?;
    let operation_ledger_path =
        load_operation_ledger_path().context("load orchestrator operation ledger path")?;
    let addr_raw =
        std::env::var("TESSERA_ORCH_ADDR").unwrap_or_else(|_| "127.0.0.1:6000".to_string());
    let listen_addr = resolve_socket_addr(&addr_raw)
        .await
        .with_context(|| format!("resolve TESSERA_ORCH_ADDR={addr_raw}"))?;

    let service = OrchestratorService::try_new_with_persistence(
        config,
        split_activation_mode,
        assignment_state_path,
        operation_ledger_path,
    )?;
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

#[derive(Debug, Serialize, Deserialize)]
struct AssignmentStateFile {
    schema: String,
    workers: Vec<AssignmentStateWorker>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AssignmentStateWorker {
    worker_id: String,
    cells: Vec<CellId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OperationLedgerFile {
    schema: String,
    records: Vec<OperationRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OperationRecord {
    operation_id: String,
    kind: DynamicOperationKind,
    status: OperationStatus,
    created_unix_secs: u64,
    updated_unix_secs: u64,
    proposal: OperationProposal,
    approval: Option<OperationApproval>,
    phases: Vec<OperationPhase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DynamicOperationKind {
    Split,
    Merge,
    MultiDepthSplit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperationStatus {
    Proposed,
    BlockedByPolicy,
    Approved,
    Executing,
    Observing,
    RecoveryRequired,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OperationProposal {
    source: String,
    proposal_hash: String,
    parent: Option<CellId>,
    targets: Vec<OperationTarget>,
    preconditions: Vec<String>,
    submission_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OperationTarget {
    cell: CellId,
    worker_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OperationApproval {
    policy_id: String,
    approver: String,
    allowed_kind: DynamicOperationKind,
    approved_unix_secs: u64,
    expires_unix_secs: u64,
    expected_proposal_hash: String,
    cooldown_key: String,
    budget_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OperationPhase {
    name: String,
    state: OperationPhaseState,
    unix_secs: u64,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperationPhaseState {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Serialize)]
struct OperationLedgerSnapshot {
    schema: String,
    persistence_enabled: bool,
    records: Vec<OperationRecord>,
}

#[derive(Debug, Serialize)]
struct OperationProposalWriteResponse {
    schema: String,
    mode: &'static str,
    source: String,
    assignments_changed: bool,
    planned_count: usize,
    recorded_count: usize,
    already_recorded_count: usize,
    skipped_count: usize,
    operation_ids: Vec<String>,
    skipped: Vec<OperationProposalSkip>,
}

#[derive(Debug, Serialize)]
struct OperationProposalSkip {
    kind: String,
    cell: CellId,
    reason: String,
}

#[derive(Debug, Serialize)]
struct OperationApprovalWriteResponse {
    schema: String,
    operation_id: String,
    status: &'static str,
    assignments_changed: bool,
    expected_proposal_hash: String,
    expires_unix_secs: u64,
}

#[derive(Debug, Clone)]
struct OperationApprovalRequest {
    operation_id: String,
    policy_id: String,
    approver: String,
    expected_proposal_hash: String,
    ttl_secs: u64,
    cooldown_key: String,
    budget_key: String,
}

#[derive(Debug, Clone)]
struct OperationProposalWorker {
    worker_id: String,
    cell_count: usize,
    registered: bool,
    active_handover: bool,
}

impl OperationLedgerFile {
    fn empty() -> Self {
        Self {
            schema: OPERATION_LEDGER_SCHEMA.to_string(),
            records: Vec::new(),
        }
    }
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

fn owners_for_cell(assignments: &AssignmentMap, cell: &CellId) -> Vec<String> {
    let mut owners = assignments
        .iter()
        .filter_map(|(worker_id, cells)| {
            cells
                .iter()
                .any(|assigned| assigned == cell)
                .then_some(worker_id.clone())
        })
        .collect::<Vec<_>>();
    owners.sort();
    owners
}

fn sort_cells(cells: &mut [CellId]) {
    cells.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
}

#[cfg(test)]
fn split_child_cell(parent: CellId, sub: u8) -> CellId {
    parent
        .legacy_shallow_child(sub)
        .expect("legacy split child requires a depth=0/sub=0 parent and sub=0..=3")
}

fn legacy_split_child_cells(parent: CellId) -> Option<[CellId; 4]> {
    let children = parent.legacy_shallow_children()?;
    (parent.child_family_kind(&children) == Some(CellChildFamilyKind::LegacyShallow))
        .then_some(children)
}

fn merge_child_family_candidates(parent: CellId) -> Vec<[CellId; 4]> {
    let mut families = Vec::new();
    if let Some(children) = parent.legacy_shallow_children() {
        families.push(children);
    }
    if parent.depth > 0
        && let Some(children) = parent.canonical_children()
        && !families.iter().any(|existing| existing == &children)
    {
        families.push(children);
    }
    families
}

fn merge_child_cells_for_assignments(
    assignments: &AssignmentMap,
    parent: CellId,
) -> Result<Vec<CellId>, String> {
    for children in merge_child_family_candidates(parent) {
        if children
            .iter()
            .any(|child| !owners_for_cell(assignments, child).is_empty())
        {
            return Ok(children.to_vec());
        }
    }
    Err(format!(
        "merge activation supports complete legacy shallow or canonical child families; got parent depth={},sub={}",
        parent.depth, parent.sub
    ))
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

fn publish_split_assignments(
    assignments: &mut AssignmentMap,
    parent: CellId,
    source_worker_id: &str,
    children: &[SplitReplayTarget],
) -> Result<bool, String> {
    match owner_for_cell(assignments, &parent) {
        Some(owner) if owner == source_worker_id => {}
        Some(owner) => {
            return Err(format!(
                "source_worker_id={} no longer owns split parent; current_owner={}",
                source_worker_id, owner
            ));
        }
        None => return Err("split parent is not assigned to any worker".to_string()),
    }
    for child in children {
        let owners = owners_for_cell(assignments, &child.cell);
        if !owners.is_empty() {
            return Err(format!(
                "split child depth={},sub={} was published before split commit for owners={}",
                child.cell.depth,
                child.cell.sub,
                owners.join(",")
            ));
        }
    }

    let source_cells = assignments
        .get_mut(source_worker_id)
        .ok_or_else(|| format!("unknown source_worker_id={source_worker_id}"))?;
    let source_len = source_cells.len();
    source_cells.retain(|assigned| assigned != &parent);
    if source_cells.len() == source_len {
        return Err(format!(
            "source_worker_id={} no longer owns split parent",
            source_worker_id
        ));
    }

    for child in children {
        let target_cells = assignments
            .get_mut(&child.target_worker_id)
            .ok_or_else(|| format!("unknown target_worker_id={}", child.target_worker_id))?;
        target_cells.push(child.cell);
        sort_cells(target_cells);
    }

    Ok(true)
}

fn publish_merge_assignments(
    assignments: &mut AssignmentMap,
    parent: CellId,
    owner_worker_id: &str,
    children: &[(CellId, String)],
) -> Result<bool, String> {
    let parent_owners = owners_for_cell(assignments, &parent);
    if !parent_owners.is_empty() {
        return Err(format!(
            "merge parent is already assigned to owners={}",
            parent_owners.join(",")
        ));
    }

    for (child, expected_owner) in children {
        match owners_for_cell(assignments, child).as_slice() {
            [owner] if owner == expected_owner => {}
            [owner] => {
                return Err(format!(
                    "merge child depth={},sub={} is owned by {}; expected source owner={}",
                    child.depth, child.sub, owner, expected_owner
                ));
            }
            [] => {
                return Err(format!(
                    "merge child depth={},sub={} is not assigned",
                    child.depth, child.sub
                ));
            }
            owners => {
                return Err(format!(
                    "merge child depth={},sub={} has multiple owners={}",
                    child.depth,
                    child.sub,
                    owners.join(",")
                ));
            }
        }
    }

    let owner_cells = assignments
        .get(owner_worker_id)
        .ok_or_else(|| format!("unknown owner_worker_id={owner_worker_id}"))?;
    if owner_cells.iter().any(|assigned| assigned == &parent) {
        return Ok(false);
    }

    for (child, source_worker_id) in children {
        let source_cells = assignments
            .get_mut(source_worker_id)
            .ok_or_else(|| format!("unknown source_worker_id={source_worker_id}"))?;
        let before_len = source_cells.len();
        source_cells.retain(|assigned| assigned != child);
        if source_cells.len() + 1 != before_len {
            return Err(format!(
                "source_worker_id={} no longer owns merge child depth={},sub={}",
                source_worker_id, child.depth, child.sub
            ));
        }
    }

    let owner_cells = assignments
        .get_mut(owner_worker_id)
        .ok_or_else(|| format!("unknown owner_worker_id={owner_worker_id}"))?;
    owner_cells.push(parent);
    sort_cells(owner_cells);
    Ok(true)
}

fn assignment_state_from_map(assignments: &AssignmentMap) -> AssignmentStateFile {
    let mut worker_ids = assignments.keys().collect::<Vec<_>>();
    worker_ids.sort();
    let workers = worker_ids
        .into_iter()
        .map(|worker_id| {
            let mut cells = assignments
                .get(worker_id.as_str())
                .cloned()
                .unwrap_or_default();
            sort_cells(&mut cells);
            AssignmentStateWorker {
                worker_id: (*worker_id).clone(),
                cells,
            }
        })
        .collect();
    AssignmentStateFile {
        schema: ASSIGNMENT_STATE_SCHEMA.to_string(),
        workers,
    }
}

fn assignments_from_state_file(
    config: &Config,
    state: AssignmentStateFile,
) -> Result<AssignmentMap> {
    if state.schema != ASSIGNMENT_STATE_SCHEMA {
        return Err(anyhow!(
            "assignment state schema must be {}; got {}",
            ASSIGNMENT_STATE_SCHEMA,
            state.schema
        ));
    }

    let mut assignments = config
        .workers
        .keys()
        .map(|worker_id| (worker_id.clone(), Vec::new()))
        .collect::<AssignmentMap>();
    let mut assigned_cells: HashMap<CellId, String> = HashMap::new();
    let mut seen_workers = HashMap::new();
    for worker_state in state.workers {
        let worker_id = worker_state.worker_id.trim();
        if worker_id.is_empty() {
            return Err(anyhow!("assignment state worker_id must not be empty"));
        }
        if seen_workers.insert(worker_id.to_string(), ()).is_some() {
            return Err(anyhow!(
                "assignment state contains duplicate worker_id={worker_id}"
            ));
        }
        if config.worker(worker_id).is_none() {
            return Err(anyhow!(
                "assignment state references unknown worker_id={worker_id}"
            ));
        }
        let cells = assignments
            .get_mut(worker_id)
            .expect("worker id was checked against config");
        for cell in worker_state.cells {
            if let Some(prev) = assigned_cells.insert(cell, worker_id.to_string()) {
                return Err(anyhow!(
                    "assignment state cell world={},cx={},cy={},depth={},sub={} assigned to both {} and {}",
                    cell.world,
                    cell.cx,
                    cell.cy,
                    cell.depth,
                    cell.sub,
                    prev,
                    worker_id
                ));
            }
            cells.push(cell);
        }
        sort_cells(cells);
    }
    Ok(assignments)
}

fn load_assignment_state_file(config: &Config, path: &Path) -> Result<AssignmentMap> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read orchestrator assignment state from {}", path.display()))?;
    let state: AssignmentStateFile =
        serde_json::from_str(&raw).context("parse orchestrator assignment state as JSON")?;
    assignments_from_state_file(config, state)
}

fn load_initial_assignments(config: &Config, state_path: Option<&Path>) -> Result<AssignmentMap> {
    let Some(path) = state_path else {
        return Ok(config.initial_assignments());
    };
    if !path
        .try_exists()
        .with_context(|| format!("check assignment state path {}", path.display()))?
    {
        return Ok(config.initial_assignments());
    }
    load_assignment_state_file(config, path)
}

fn write_assignment_state_file(path: &Path, assignments: &AssignmentMap) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create assignment state dir {}", parent.display()))?;
    }
    let body = format!(
        "{}\n",
        serde_json::to_string_pretty(&assignment_state_from_map(assignments))
            .context("serialize orchestrator assignment state")?
    );
    let tmp_path = assignment_state_tmp_path(path);
    fs::write(&tmp_path, body)
        .with_context(|| format!("write assignment state temp file {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "atomically replace assignment state {} from {}",
            path.display(),
            tmp_path.display()
        )
    })?;
    Ok(())
}

fn assignment_state_tmp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("assignment-state.json");
    path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
}

fn validate_operation_ledger(ledger: &OperationLedgerFile) -> Result<()> {
    if ledger.schema != OPERATION_LEDGER_SCHEMA {
        return Err(anyhow!(
            "operation ledger schema must be {}; got {}",
            OPERATION_LEDGER_SCHEMA,
            ledger.schema
        ));
    }
    let mut seen_operations = HashMap::new();
    for record in &ledger.records {
        let operation_id = record.operation_id.trim();
        if operation_id.is_empty() {
            return Err(anyhow!("operation ledger operation_id must not be empty"));
        }
        if seen_operations
            .insert(operation_id.to_string(), ())
            .is_some()
        {
            return Err(anyhow!(
                "operation ledger contains duplicate operation_id={operation_id}"
            ));
        }
        if record.updated_unix_secs < record.created_unix_secs {
            return Err(anyhow!(
                "operation ledger operation_id={operation_id} has updated_unix_secs before created_unix_secs"
            ));
        }
        validate_operation_proposal(operation_id, &record.proposal)?;
        if let Some(approval) = &record.approval {
            validate_operation_approval(operation_id, record.kind, &record.proposal, approval)?;
        }
        for phase in &record.phases {
            validate_operation_phase(operation_id, phase)?;
        }
    }
    Ok(())
}

fn validate_operation_proposal(operation_id: &str, proposal: &OperationProposal) -> Result<()> {
    if proposal.source.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} proposal.source must not be empty"
        ));
    }
    if proposal.proposal_hash.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} proposal.proposal_hash must not be empty"
        ));
    }
    if proposal.submission_command.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} proposal.submission_command must not be empty"
        ));
    }
    let mut seen_targets = HashMap::new();
    for target in &proposal.targets {
        if target.worker_id.trim().is_empty() {
            return Err(anyhow!(
                "operation ledger operation_id={operation_id} target.worker_id must not be empty"
            ));
        }
        if let Some(prev_worker) = seen_targets.insert(target.cell, target.worker_id.as_str()) {
            return Err(anyhow!(
                "operation ledger operation_id={operation_id} assigns target cell world={},cx={},cy={},depth={},sub={} to both {} and {}",
                target.cell.world,
                target.cell.cx,
                target.cell.cy,
                target.cell.depth,
                target.cell.sub,
                prev_worker,
                target.worker_id
            ));
        }
    }
    Ok(())
}

fn validate_operation_approval(
    operation_id: &str,
    kind: DynamicOperationKind,
    proposal: &OperationProposal,
    approval: &OperationApproval,
) -> Result<()> {
    if approval.policy_id.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} approval.policy_id must not be empty"
        ));
    }
    if approval.approver.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} approval.approver must not be empty"
        ));
    }
    if approval.expires_unix_secs <= approval.approved_unix_secs {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} approval must expire after approval time"
        ));
    }
    if approval.allowed_kind != kind {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} approval.allowed_kind must match operation kind"
        ));
    }
    if approval.expected_proposal_hash != proposal.proposal_hash {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} approval expected_proposal_hash does not match proposal_hash"
        ));
    }
    if approval.cooldown_key.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} approval.cooldown_key must not be empty"
        ));
    }
    if approval.budget_key.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} approval.budget_key must not be empty"
        ));
    }
    Ok(())
}

fn validate_operation_phase(operation_id: &str, phase: &OperationPhase) -> Result<()> {
    if phase.name.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} phase.name must not be empty"
        ));
    }
    if phase.reason.trim().is_empty() {
        return Err(anyhow!(
            "operation ledger operation_id={operation_id} phase.reason must not be empty"
        ));
    }
    Ok(())
}

fn load_operation_ledger_file(path: &Path) -> Result<OperationLedgerFile> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read orchestrator operation ledger from {}", path.display()))?;
    let ledger: OperationLedgerFile =
        serde_json::from_str(&raw).context("parse orchestrator operation ledger as JSON")?;
    validate_operation_ledger(&ledger)?;
    Ok(ledger)
}

fn load_initial_operation_ledger(path: Option<&Path>) -> Result<OperationLedgerFile> {
    let Some(path) = path else {
        return Ok(OperationLedgerFile::empty());
    };
    if path
        .try_exists()
        .with_context(|| format!("check operation ledger path {}", path.display()))?
    {
        return load_operation_ledger_file(path);
    }
    let ledger = OperationLedgerFile::empty();
    write_operation_ledger_file(path, &ledger)?;
    Ok(ledger)
}

fn write_operation_ledger_file(path: &Path, ledger: &OperationLedgerFile) -> Result<()> {
    validate_operation_ledger(ledger)?;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create operation ledger dir {}", parent.display()))?;
    }
    let body = format!(
        "{}\n",
        serde_json::to_string_pretty(ledger).context("serialize orchestrator operation ledger")?
    );
    let tmp_path = operation_ledger_tmp_path(path);
    fs::write(&tmp_path, body)
        .with_context(|| format!("write operation ledger temp file {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "atomically replace operation ledger {} from {}",
            path.display(),
            tmp_path.display()
        )
    })?;
    Ok(())
}

fn operation_ledger_tmp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("operation-ledger.json");
    path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
}

fn build_operation_record_for_plan(
    source: &str,
    snapshot: &split_merge_planner::SplitMergeMetricsSnapshot,
    plan: &split_merge_planner::SplitMergePlan,
    workers: &[OperationProposalWorker],
    now_unix_secs: u64,
) -> Result<OperationRecord, String> {
    match plan.kind {
        split_merge_planner::SplitMergePlanKind::Split => {
            build_split_operation_record(source, snapshot, plan, workers, now_unix_secs)
        }
        split_merge_planner::SplitMergePlanKind::Merge => {
            build_merge_operation_record(source, snapshot, plan, workers, now_unix_secs)
        }
    }
}

fn build_split_operation_record(
    source: &str,
    snapshot: &split_merge_planner::SplitMergeMetricsSnapshot,
    plan: &split_merge_planner::SplitMergePlan,
    workers: &[OperationProposalWorker],
    now_unix_secs: u64,
) -> Result<OperationRecord, String> {
    let parent = plan.cell;
    let source_worker_id = snapshot
        .cells
        .iter()
        .find(|cell| cell.cell == parent)
        .and_then(|cell| cell.owner_worker_id.as_deref())
        .ok_or_else(|| {
            format!(
                "split plan parent world={},cx={},cy={},depth={},sub={} has no owner_worker_id",
                parent.world, parent.cx, parent.cy, parent.depth, parent.sub
            )
        })?;
    let children = split_proposal_children(parent)?;
    let targets = infer_split_operation_targets(workers, source_worker_id, &children)?;
    let preconditions = vec![
        format!("planner selected split candidate from {source}"),
        format!("source worker {source_worker_id} owns the parent cell and is registered"),
        "target workers are registered and have no active handover".to_string(),
        "operator approval must match proposal_hash before mutation".to_string(),
        "TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual is enabled only for a controlled execution window".to_string(),
    ];
    let proposal_hash = operation_proposal_hash(source, plan, &targets, &preconditions);
    let kind = if parent.depth == 0 {
        DynamicOperationKind::Split
    } else {
        DynamicOperationKind::MultiDepthSplit
    };
    let operation_id = operation_id_for(kind, parent, &proposal_hash);
    let submission_command = split_operation_submission_command(&operation_id, parent, &targets);

    Ok(OperationRecord {
        operation_id,
        kind,
        status: OperationStatus::Proposed,
        created_unix_secs: now_unix_secs,
        updated_unix_secs: now_unix_secs,
        proposal: OperationProposal {
            source: source.to_string(),
            proposal_hash,
            parent: Some(parent),
            targets,
            preconditions,
            submission_command,
        },
        approval: None,
        phases: vec![OperationPhase {
            name: "proposal_recorded".to_string(),
            state: OperationPhaseState::Succeeded,
            unix_secs: now_unix_secs,
            reason: "planner proposal persisted without assignment mutation".to_string(),
        }],
    })
}

fn build_merge_operation_record(
    source: &str,
    snapshot: &split_merge_planner::SplitMergeMetricsSnapshot,
    plan: &split_merge_planner::SplitMergePlan,
    workers: &[OperationProposalWorker],
    now_unix_secs: u64,
) -> Result<OperationRecord, String> {
    let parent = plan.cell;
    let (owner_worker_id, children) = merge_proposal_owner_and_children(snapshot, parent)?;
    let owner = workers
        .iter()
        .find(|worker| worker.worker_id == owner_worker_id)
        .ok_or_else(|| format!("merge owner worker {owner_worker_id} is not configured"))?;
    if !owner.registered {
        return Err(format!(
            "merge owner worker {owner_worker_id} is not registered"
        ));
    }
    if owner.active_handover {
        return Err(format!(
            "merge owner worker {owner_worker_id} has an active handover"
        ));
    }

    let mut targets = children
        .into_iter()
        .map(|cell| OperationTarget {
            cell,
            worker_id: owner_worker_id.clone(),
        })
        .collect::<Vec<_>>();
    sort_operation_targets(&mut targets);
    let preconditions = vec![
        format!("planner selected merge candidate from {source}"),
        format!("complete child family shares registered owner worker {owner_worker_id}"),
        "parent route is not already published".to_string(),
        "operator approval must match proposal_hash before mutation".to_string(),
        "TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual is enabled only for a controlled execution window".to_string(),
    ];
    let proposal_hash = operation_proposal_hash(source, plan, &targets, &preconditions);
    let operation_id = operation_id_for(DynamicOperationKind::Merge, parent, &proposal_hash);
    let submission_command =
        merge_operation_submission_command(&operation_id, parent, owner_worker_id.as_str());

    Ok(OperationRecord {
        operation_id,
        kind: DynamicOperationKind::Merge,
        status: OperationStatus::Proposed,
        created_unix_secs: now_unix_secs,
        updated_unix_secs: now_unix_secs,
        proposal: OperationProposal {
            source: source.to_string(),
            proposal_hash,
            parent: Some(parent),
            targets,
            preconditions,
            submission_command,
        },
        approval: None,
        phases: vec![OperationPhase {
            name: "proposal_recorded".to_string(),
            state: OperationPhaseState::Succeeded,
            unix_secs: now_unix_secs,
            reason: "planner proposal persisted without assignment mutation".to_string(),
        }],
    })
}

fn split_proposal_children(parent: CellId) -> Result<[CellId; 4], String> {
    if parent.depth == 0 && parent.sub == 0 {
        return parent
            .legacy_shallow_children()
            .ok_or_else(|| "legacy split proposal requires a depth=0/sub=0 parent".to_string());
    }
    parent.canonical_children().ok_or_else(|| {
        format!(
            "multi-depth split proposal requires a canonical parent; got depth={},sub={}",
            parent.depth, parent.sub
        )
    })
}

fn infer_split_operation_targets(
    workers: &[OperationProposalWorker],
    source_worker_id: &str,
    children: &[CellId; 4],
) -> Result<Vec<OperationTarget>, String> {
    let source = workers
        .iter()
        .find(|worker| worker.worker_id == source_worker_id)
        .ok_or_else(|| format!("source worker {source_worker_id} is not configured"))?;
    if !source.registered {
        return Err(format!(
            "source worker {source_worker_id} is not registered"
        ));
    }
    if source.active_handover {
        return Err(format!(
            "source worker {source_worker_id} has an active handover"
        ));
    }

    let mut non_source = workers
        .iter()
        .filter(|worker| worker.worker_id != source_worker_id)
        .filter(|worker| worker.registered)
        .filter(|worker| !worker.active_handover)
        .collect::<Vec<_>>();
    non_source.sort_by(|left, right| {
        left.cell_count
            .cmp(&right.cell_count)
            .then_with(|| left.worker_id.cmp(&right.worker_id))
    });
    let Some(first_target) = non_source.first() else {
        return Err("no registered non-source worker is available for split proposal".to_string());
    };
    let second_target = non_source.get(1).unwrap_or(first_target);
    let worker_ids = [
        source_worker_id,
        first_target.worker_id.as_str(),
        source_worker_id,
        second_target.worker_id.as_str(),
    ];
    let mut targets = children
        .iter()
        .zip(worker_ids)
        .map(|(cell, worker_id)| OperationTarget {
            cell: *cell,
            worker_id: worker_id.to_string(),
        })
        .collect::<Vec<_>>();
    sort_operation_targets(&mut targets);
    Ok(targets)
}

fn merge_proposal_owner_and_children(
    snapshot: &split_merge_planner::SplitMergeMetricsSnapshot,
    parent: CellId,
) -> Result<(String, Vec<CellId>), String> {
    for children in merge_child_family_candidates(parent) {
        let mut owner_worker_id: Option<String> = None;
        let mut matched = Vec::new();
        for child in children {
            let Some(metrics) = snapshot.cells.iter().find(|cell| cell.cell == child) else {
                matched.clear();
                break;
            };
            let Some(owner) = metrics.owner_worker_id.as_deref() else {
                matched.clear();
                break;
            };
            if owner_worker_id
                .as_deref()
                .is_some_and(|existing| existing != owner)
            {
                matched.clear();
                break;
            }
            owner_worker_id.get_or_insert_with(|| owner.to_string());
            matched.push(child);
        }
        if matched.len() == 4
            && let Some(owner) = owner_worker_id
        {
            return Ok((owner, matched));
        }
    }
    Err(format!(
        "merge proposal requires a complete same-owner child family for parent world={},cx={},cy={},depth={},sub={}",
        parent.world, parent.cx, parent.cy, parent.depth, parent.sub
    ))
}

fn operation_proposal_hash(
    source: &str,
    plan: &split_merge_planner::SplitMergePlan,
    targets: &[OperationTarget],
    preconditions: &[String],
) -> String {
    let mut body = String::new();
    body.push_str("source=");
    body.push_str(source);
    body.push('\n');
    body.push_str("kind=");
    body.push_str(split_merge_plan_kind_slug(plan.kind));
    body.push('\n');
    push_cell_hash_part(&mut body, "cell", plan.cell);
    body.push_str(&format!(
        "pressure_signals={}\nscore={}\nrequired_handover_ops={}\ncells_moved={}\n",
        plan.pressure_signals, plan.score, plan.required_handover_ops, plan.cells_moved
    ));
    for target in targets {
        push_cell_hash_part(&mut body, "target", target.cell);
        body.push_str("worker=");
        body.push_str(&target.worker_id);
        body.push('\n');
    }
    for precondition in preconditions {
        body.push_str("precondition=");
        body.push_str(precondition);
        body.push('\n');
    }
    format!("fnv1a64:{:016x}", fnv1a64(body.as_bytes()))
}

fn push_cell_hash_part(out: &mut String, label: &str, cell: CellId) {
    out.push_str(label);
    out.push('=');
    out.push_str(&format!(
        "{},{},{},{},{}",
        cell.world, cell.cx, cell.cy, cell.depth, cell.sub
    ));
    out.push('\n');
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn operation_id_for(kind: DynamicOperationKind, cell: CellId, proposal_hash: &str) -> String {
    let hash = proposal_hash
        .strip_prefix("fnv1a64:")
        .unwrap_or(proposal_hash)
        .chars()
        .take(12)
        .collect::<String>();
    format!(
        "p7-{}-w{}-cx{}-cy{}-d{}-s{}-{}",
        dynamic_operation_kind_slug(kind),
        cell.world,
        cell.cx,
        cell.cy,
        cell.depth,
        cell.sub,
        hash
    )
}

fn split_operation_submission_command(
    operation_id: &str,
    parent: CellId,
    targets: &[OperationTarget],
) -> String {
    let mut parts = vec![
        "cargo xt split-activation".to_string(),
        "--orch-addr ${TESSERA_ORCH_ADDR:-127.0.0.1:6000}".to_string(),
        format!("--operation-id {operation_id}"),
        format!("--world {}", parent.world),
        format!("--cx {}", parent.cx),
        format!("--cy {}", parent.cy),
    ];
    if parent.depth > 0 || parent.sub > 0 {
        parts.push(format!("--depth {}", parent.depth));
        parts.push(format!("--sub {}", parent.sub));
    }

    let mut targets = targets.to_vec();
    sort_operation_targets(&mut targets);
    if parent.depth == 0 && parent.sub == 0 {
        parts.extend(
            targets
                .into_iter()
                .map(|target| format!("--target {}={}", target.cell.sub, target.worker_id)),
        );
    } else {
        parts.extend(targets.into_iter().map(|target| {
            format!(
                "--target-cell {},{},{},{},{}={}",
                target.cell.world,
                target.cell.cx,
                target.cell.cy,
                target.cell.depth,
                target.cell.sub,
                target.worker_id
            )
        }));
    }
    parts.join(" ")
}

fn merge_operation_submission_command(
    operation_id: &str,
    parent: CellId,
    owner_worker_id: &str,
) -> String {
    [
        "cargo xt merge-activation".to_string(),
        "--orch-addr ${TESSERA_ORCH_ADDR:-127.0.0.1:6000}".to_string(),
        format!("--operation-id {operation_id}"),
        format!("--world {}", parent.world),
        format!("--cx {}", parent.cx),
        format!("--cy {}", parent.cy),
        format!("--depth {}", parent.depth),
        format!("--owner-worker-id {owner_worker_id}"),
    ]
    .join(" ")
}

fn sort_operation_targets(targets: &mut [OperationTarget]) {
    targets.sort_by_key(|target| {
        (
            target.cell.world,
            target.cell.cy,
            target.cell.cx,
            target.cell.depth,
            target.cell.sub,
            target.worker_id.clone(),
        )
    });
}

fn split_merge_plan_kind_slug(kind: split_merge_planner::SplitMergePlanKind) -> &'static str {
    match kind {
        split_merge_planner::SplitMergePlanKind::Split => "split",
        split_merge_planner::SplitMergePlanKind::Merge => "merge",
    }
}

fn dynamic_operation_kind_slug(kind: DynamicOperationKind) -> &'static str {
    match kind {
        DynamicOperationKind::Split => "split",
        DynamicOperationKind::Merge => "merge",
        DynamicOperationKind::MultiDepthSplit => "multi-depth-split",
    }
}

fn approval_matches_request(
    approval: &OperationApproval,
    request: &OperationApprovalRequest,
) -> bool {
    approval.policy_id == request.policy_id
        && approval.approver == request.approver
        && approval.expected_proposal_hash == request.expected_proposal_hash
        && approval.cooldown_key == request.cooldown_key
        && approval.budget_key == request.budget_key
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

#[derive(Debug, Clone)]
struct StagedSplitActivation {
    operation_id: String,
    parent: CellId,
    source_worker_id: String,
    children: Vec<StagedSplitAssignment>,
}

#[derive(Debug, Clone)]
struct StagedSplitAssignment {
    cell: CellId,
    target_worker_id: String,
}

impl StagedSplitActivation {
    fn as_response_children(&self) -> Vec<StagedSplitChildAssignment> {
        self.children
            .iter()
            .map(|child| StagedSplitChildAssignment {
                cell: Some(cell_to_assignment(&child.cell)),
                target_worker_id: child.target_worker_id.clone(),
            })
            .collect()
    }
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
    use super::{CellChildFamilyKind, CellId};
    use serde::{Deserialize, Serialize};
    use std::collections::{HashMap, HashSet};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) enum SplitMergePlanKind {
        Split,
        Merge,
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
        pub low_actor_count: u64,
        pub low_move_queue_pressure: u64,
        pub low_tick_stage_micros: u64,
        pub low_relay_fanout: u64,
        pub low_handover_failures: u64,
        pub min_merge_low_pressure_signals: u8,
        pub required_merge_windows: u32,
        pub min_merge_cell_age_secs: u64,
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
                low_actor_count: 20,
                low_move_queue_pressure: 4,
                low_tick_stage_micros: 5_000,
                low_relay_fanout: 8,
                low_handover_failures: 0,
                min_merge_low_pressure_signals: 4,
                required_merge_windows: 5,
                min_merge_cell_age_secs: 120,
                max_active_plans_per_world: 1,
                max_handover_ops_per_interval: 1,
                max_cells_moved_per_interval: 4,
            }
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
    pub(super) struct SplitMergeMetricsSnapshot {
        pub cells: Vec<SplitMergeCellMetrics>,
        #[serde(default)]
        pub active_plans: Vec<SplitMergeActivePlan>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    pub(super) struct SplitMergeCellMetrics {
        pub cell: CellId,
        #[serde(default)]
        pub actor_count: u64,
        #[serde(default)]
        pub move_queue_pressure: u64,
        #[serde(default)]
        pub tick_stage_micros: u64,
        #[serde(default)]
        pub relay_fanout: u64,
        #[serde(default)]
        pub handover_failures: u64,
        #[serde(default)]
        pub high_pressure_windows: u32,
        #[serde(default)]
        pub low_pressure_windows: u32,
        #[serde(default)]
        pub cell_age_secs: u64,
        #[serde(default)]
        pub cooldown_remaining_secs: u64,
        #[serde(default)]
        pub active_handover: bool,
        #[serde(default)]
        pub owner_worker_id: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    pub(super) struct SplitMergeActivePlan {
        pub kind: SplitMergePlanKind,
        pub cell: CellId,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
        candidates.extend(merge_candidates(snapshot, config, &blocked_families));

        candidates.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| plan_kind_sort_key(left.kind).cmp(&plan_kind_sort_key(right.kind)))
                .then_with(|| cell_family_key(&left.cell).cmp(&cell_family_key(&right.cell)))
        });

        let mut planned_by_world: HashMap<u32, usize> = HashMap::new();
        let mut planned_families = HashSet::new();
        let mut remaining_handover_ops = config.max_handover_ops_per_interval;
        let mut remaining_cells_moved = config.max_cells_moved_per_interval;
        let mut plans = Vec::new();

        for candidate in candidates {
            let family = cell_family_key(&candidate.cell);
            if planned_families.contains(&family) {
                continue;
            }
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
            planned_families.insert(family);
            plans.push(candidate);
        }

        plans
    }

    fn merge_candidates(
        snapshot: &SplitMergeMetricsSnapshot,
        config: &SplitMergePlannerConfig,
        blocked_families: &HashSet<(u32, i32, i32, u8, u8)>,
    ) -> Vec<SplitMergePlan> {
        let mut groups: HashMap<CellId, HashMap<CellId, &SplitMergeCellMetrics>> = HashMap::new();
        let mut duplicate_groups = HashSet::new();
        for cell in &snapshot.cells {
            for parent in merge_sibling_parent_candidates(cell.cell) {
                let siblings = groups.entry(parent).or_default();
                if siblings.insert(cell.cell, cell).is_some() {
                    duplicate_groups.insert(parent);
                }
            }
        }

        let mut candidates = Vec::new();
        for (parent, siblings_by_cell) in groups {
            if duplicate_groups.contains(&parent) || siblings_by_cell.len() != 4 {
                continue;
            }

            let sibling_cells = siblings_by_cell.keys().copied().collect::<Vec<_>>();
            let Some(expected_children) = merge_expected_children(parent, &sibling_cells) else {
                continue;
            };
            if blocked_families.contains(&cell_family_key(&parent)) {
                continue;
            }

            let siblings = expected_children
                .into_iter()
                .map(|child| siblings_by_cell[&child])
                .collect::<Vec<_>>();
            if !merge_siblings_share_owner(&siblings) {
                continue;
            }
            if siblings.iter().any(|cell| {
                cell.cooldown_remaining_secs > 0
                    || cell.active_handover
                    || cell.cell_age_secs < config.min_merge_cell_age_secs
                    || cell.low_pressure_windows < config.required_merge_windows
            }) {
                continue;
            }

            let low_signals = siblings
                .iter()
                .map(|cell| merge_low_pressure_signals(cell, config))
                .min()
                .unwrap_or_default();
            if low_signals < config.min_merge_low_pressure_signals {
                continue;
            }

            candidates.push(SplitMergePlan {
                kind: SplitMergePlanKind::Merge,
                cell: parent,
                pressure_signals: low_signals,
                score: merge_cold_score(&siblings, low_signals, config),
                required_handover_ops: 1,
                cells_moved: siblings.len(),
            });
        }
        candidates
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

    fn merge_low_pressure_signals(
        cell: &SplitMergeCellMetrics,
        config: &SplitMergePlannerConfig,
    ) -> u8 {
        [
            cell.actor_count <= config.low_actor_count,
            cell.move_queue_pressure <= config.low_move_queue_pressure,
            cell.tick_stage_micros <= config.low_tick_stage_micros,
            cell.relay_fanout <= config.low_relay_fanout,
            cell.handover_failures <= config.low_handover_failures,
        ]
        .into_iter()
        .filter(|signal| *signal)
        .count() as u8
    }

    fn merge_cold_score(
        siblings: &[&SplitMergeCellMetrics],
        low_signals: u8,
        config: &SplitMergePlannerConfig,
    ) -> u64 {
        let cold_headroom = siblings.iter().fold(0_u64, |sum, cell| {
            sum + config.low_actor_count.saturating_sub(cell.actor_count) * 10_000
                + config
                    .low_move_queue_pressure
                    .saturating_sub(cell.move_queue_pressure)
                    * 1_000
                + config.low_relay_fanout.saturating_sub(cell.relay_fanout) * 100
                + config
                    .low_tick_stage_micros
                    .saturating_sub(cell.tick_stage_micros)
                    / 1_000
                + config
                    .low_handover_failures
                    .saturating_sub(cell.handover_failures)
                    * 10
        });
        let sustained_windows = siblings
            .iter()
            .map(|cell| cell.low_pressure_windows)
            .min()
            .unwrap_or_default();

        u64::from(low_signals) * 1_000_000 + u64::from(sustained_windows) * 10_000 + cold_headroom
    }

    fn merge_siblings_share_owner(siblings: &[&SplitMergeCellMetrics]) -> bool {
        let Some(owner) = siblings
            .first()
            .and_then(|cell| cell.owner_worker_id.as_deref())
        else {
            return false;
        };
        siblings
            .iter()
            .all(|cell| cell.owner_worker_id.as_deref() == Some(owner))
    }

    fn merge_sibling_parent_candidates(cell: CellId) -> Vec<CellId> {
        let mut parents = Vec::new();
        if cell.depth == 1 && cell.sub <= 3 {
            parents.push(CellId::grid(cell.world, cell.cx, cell.cy));
        }
        if cell.depth > 1
            && let Some(parent) = cell.canonical_parent()
            && !parents.contains(&parent)
        {
            parents.push(parent);
        }
        parents
    }

    fn merge_expected_children(parent: CellId, children: &[CellId]) -> Option<[CellId; 4]> {
        match parent.child_family_kind(children)? {
            CellChildFamilyKind::LegacyShallow => parent.legacy_shallow_children(),
            CellChildFamilyKind::CanonicalLeaf => parent.canonical_children(),
        }
    }

    fn cell_family_key(cell: &CellId) -> (u32, i32, i32, u8, u8) {
        (cell.world, cell.cx, cell.cy, cell.depth, cell.sub)
    }

    fn plan_kind_sort_key(kind: SplitMergePlanKind) -> u8 {
        match kind {
            SplitMergePlanKind::Split => 0,
            SplitMergePlanKind::Merge => 1,
        }
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
    staged_splits: Arc<RwLock<HashMap<CellId, StagedSplitActivation>>>,
    operation_ledger: Arc<RwLock<OperationLedgerFile>>,
    listing_tx: watch::Sender<AssignmentListing>,
    metrics: Arc<OrchestratorMetricsCounters>,
    split_activation_mode: SplitActivationMode,
    assignment_state_path: Option<Arc<PathBuf>>,
    operation_ledger_path: Option<Arc<PathBuf>>,
}

#[derive(Debug, Serialize)]
struct SplitMergePreviewResponse {
    mode: &'static str,
    source: String,
    assignments_changed: bool,
    plans: Vec<split_merge_planner::SplitMergePlan>,
}

impl OrchestratorService {
    #[cfg(test)]
    fn new(config: Config) -> Self {
        Self::new_with_split_activation(config, SplitActivationMode::Disabled)
    }

    #[cfg(test)]
    fn new_with_split_activation(
        config: Config,
        split_activation_mode: SplitActivationMode,
    ) -> Self {
        Self::try_new_with_split_activation(config, split_activation_mode, None)
            .expect("construct orchestrator service")
    }

    #[cfg(test)]
    fn try_new_with_split_activation(
        config: Config,
        split_activation_mode: SplitActivationMode,
        assignment_state_path: Option<PathBuf>,
    ) -> Result<Self> {
        Self::try_new_with_persistence(config, split_activation_mode, assignment_state_path, None)
    }

    fn try_new_with_persistence(
        config: Config,
        split_activation_mode: SplitActivationMode,
        assignment_state_path: Option<PathBuf>,
        operation_ledger_path: Option<PathBuf>,
    ) -> Result<Self> {
        let config = Arc::new(config);
        let initial_assignments =
            load_initial_assignments(&config, assignment_state_path.as_deref())?;
        let initial_operation_ledger =
            load_initial_operation_ledger(operation_ledger_path.as_deref())?;
        let operation_ledger_path = operation_ledger_path.map(Arc::new);
        let initial_listing = listing_with_runtime(
            &config,
            &initial_assignments,
            &HashMap::new(),
            &HashMap::new(),
        );
        let (listing_tx, _) = watch::channel(initial_listing);
        Ok(Self {
            config,
            assignments: Arc::new(RwLock::new(initial_assignments)),
            runtime: Arc::new(RwLock::new(HashMap::new())),
            handovers: Arc::new(RwLock::new(HashMap::new())),
            staged_splits: Arc::new(RwLock::new(HashMap::new())),
            operation_ledger: Arc::new(RwLock::new(initial_operation_ledger)),
            listing_tx,
            metrics: Arc::new(OrchestratorMetricsCounters::default()),
            split_activation_mode,
            assignment_state_path: assignment_state_path.map(Arc::new),
            operation_ledger_path,
        })
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

    async fn operation_ledger_snapshot(&self) -> OperationLedgerSnapshot {
        let ledger = self.operation_ledger.read().await;
        OperationLedgerSnapshot {
            schema: ledger.schema.clone(),
            persistence_enabled: self.operation_ledger_path.is_some(),
            records: ledger.records.clone(),
        }
    }

    async fn record_split_merge_proposals(&self) -> Result<OperationProposalWriteResponse, String> {
        let (source, snapshot) = self.split_merge_preview_snapshot().await?;
        self.record_split_merge_proposals_from_snapshot(source, snapshot, SystemTime::now())
            .await
    }

    async fn record_split_merge_proposals_from_snapshot(
        &self,
        source: String,
        snapshot: split_merge_planner::SplitMergeMetricsSnapshot,
        now: SystemTime,
    ) -> Result<OperationProposalWriteResponse, String> {
        if self.operation_ledger_path.is_none() {
            return Err(
                "operation ledger persistence is disabled; set TESSERA_ORCH_OPERATION_LEDGER_PATH"
                    .to_string(),
            );
        }

        let plans = split_merge_planner::plan_split_merge(
            &snapshot,
            &split_merge_planner::SplitMergePlannerConfig::default(),
        );
        let workers = self.operation_proposal_workers().await;
        let now_unix_secs = unix_timestamp_secs(now);
        let mut records = Vec::new();
        let mut skipped = Vec::new();
        for plan in &plans {
            match build_operation_record_for_plan(&source, &snapshot, plan, &workers, now_unix_secs)
            {
                Ok(record) => records.push(record),
                Err(reason) => skipped.push(OperationProposalSkip {
                    kind: split_merge_plan_kind_slug(plan.kind).to_string(),
                    cell: plan.cell,
                    reason,
                }),
            }
        }
        let planned_count = plans.len();
        let skipped_count = skipped.len();
        let (operation_ids, recorded_count, already_recorded_count) =
            self.append_operation_records(records).await?;

        Ok(OperationProposalWriteResponse {
            schema: OPERATION_LEDGER_SCHEMA.to_string(),
            mode: "planner_proposal_recording",
            source,
            assignments_changed: false,
            planned_count,
            recorded_count,
            already_recorded_count,
            skipped_count,
            operation_ids,
            skipped,
        })
    }

    async fn operation_proposal_workers(&self) -> Vec<OperationProposalWorker> {
        let runtime = self.runtime.read().await.clone();
        let assignments = self.assignments.read().await.clone();
        let handovers = self.handovers.read().await.clone();
        let mut workers = self
            .config
            .workers
            .keys()
            .map(|worker_id| OperationProposalWorker {
                worker_id: worker_id.clone(),
                cell_count: assignments
                    .get(worker_id.as_str())
                    .map(Vec::len)
                    .unwrap_or_default(),
                registered: runtime.contains_key(worker_id.as_str()),
                active_handover: handovers.values().any(|handover| {
                    handover.source_worker_id == *worker_id
                        || handover.target_worker_id == *worker_id
                }),
            })
            .collect::<Vec<_>>();
        workers.sort_by(|left, right| left.worker_id.cmp(&right.worker_id));
        workers
    }

    async fn append_operation_records(
        &self,
        records: Vec<OperationRecord>,
    ) -> Result<(Vec<String>, usize, usize), String> {
        let Some(path) = self.operation_ledger_path.as_ref() else {
            return Err(
                "operation ledger persistence is disabled; set TESSERA_ORCH_OPERATION_LEDGER_PATH"
                    .to_string(),
            );
        };
        let mut ledger = self.operation_ledger.write().await;
        let mut next_ledger = ledger.clone();
        let mut operation_ids = Vec::new();
        let mut recorded_count = 0;
        let mut already_recorded_count = 0;
        for record in records {
            operation_ids.push(record.operation_id.clone());
            if let Some(existing) = next_ledger
                .records
                .iter()
                .find(|existing| existing.operation_id == record.operation_id)
            {
                if existing.proposal.proposal_hash != record.proposal.proposal_hash {
                    return Err(format!(
                        "operation_id={} already exists with a different proposal_hash",
                        record.operation_id
                    ));
                }
                already_recorded_count += 1;
                continue;
            }
            next_ledger.records.push(record);
            recorded_count += 1;
        }
        if recorded_count > 0 {
            write_operation_ledger_file(path.as_path(), &next_ledger).map_err(|err| {
                format!("persist operation ledger to {}: {err:#}", path.display())
            })?;
            *ledger = next_ledger;
        }
        Ok((operation_ids, recorded_count, already_recorded_count))
    }

    async fn approve_operation(
        &self,
        request: OperationApprovalRequest,
        now: SystemTime,
    ) -> Result<OperationApprovalWriteResponse, String> {
        let Some(path) = self.operation_ledger_path.as_ref() else {
            return Err(
                "operation ledger persistence is disabled; set TESSERA_ORCH_OPERATION_LEDGER_PATH"
                    .to_string(),
            );
        };
        if request.ttl_secs == 0 {
            return Err("approval ttl_secs must be greater than zero".to_string());
        }
        let now_unix_secs = unix_timestamp_secs(now);
        let expires_unix_secs = now_unix_secs
            .checked_add(request.ttl_secs)
            .ok_or_else(|| "approval ttl_secs overflows expires_unix_secs".to_string())?;

        let mut ledger = self.operation_ledger.write().await;
        let mut next_ledger = ledger.clone();
        let Some(record) = next_ledger
            .records
            .iter_mut()
            .find(|record| record.operation_id == request.operation_id)
        else {
            return Err(format!("operation_id={} not found", request.operation_id));
        };
        if record.proposal.proposal_hash != request.expected_proposal_hash {
            return Err(format!(
                "operation_id={} expected_proposal_hash does not match proposal_hash",
                request.operation_id
            ));
        }
        if let Some(existing) = &record.approval {
            if approval_matches_request(existing, &request) {
                return Ok(OperationApprovalWriteResponse {
                    schema: OPERATION_LEDGER_SCHEMA.to_string(),
                    operation_id: request.operation_id,
                    status: "already_approved",
                    assignments_changed: false,
                    expected_proposal_hash: existing.expected_proposal_hash.clone(),
                    expires_unix_secs: existing.expires_unix_secs,
                });
            }
            return Err(format!(
                "operation_id={} already has a different approval record",
                request.operation_id
            ));
        }
        if record.status != OperationStatus::Proposed {
            return Err(format!(
                "operation_id={} is not in proposed status",
                request.operation_id
            ));
        }

        record.status = OperationStatus::Approved;
        record.updated_unix_secs = now_unix_secs;
        record.approval = Some(OperationApproval {
            policy_id: request.policy_id,
            approver: request.approver,
            allowed_kind: record.kind,
            approved_unix_secs: now_unix_secs,
            expires_unix_secs,
            expected_proposal_hash: request.expected_proposal_hash.clone(),
            cooldown_key: request.cooldown_key,
            budget_key: request.budget_key,
        });
        record.phases.push(OperationPhase {
            name: "approval_recorded".to_string(),
            state: OperationPhaseState::Succeeded,
            unix_secs: now_unix_secs,
            reason: "operator approval persisted without assignment mutation".to_string(),
        });
        write_operation_ledger_file(path.as_path(), &next_ledger)
            .map_err(|err| format!("persist operation ledger to {}: {err:#}", path.display()))?;
        *ledger = next_ledger;

        Ok(OperationApprovalWriteResponse {
            schema: OPERATION_LEDGER_SCHEMA.to_string(),
            operation_id: request.operation_id,
            status: "approved",
            assignments_changed: false,
            expected_proposal_hash: request.expected_proposal_hash,
            expires_unix_secs,
        })
    }

    fn persist_assignment_map(&self, assignments: &AssignmentMap) -> Result<(), String> {
        let Some(path) = self.assignment_state_path.as_deref() else {
            return Ok(());
        };
        write_assignment_state_file(path, assignments)
            .map_err(|err| format!("persist assignment state to {}: {err:#}", path.display()))
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

    async fn split_merge_preview(&self) -> Result<SplitMergePreviewResponse, String> {
        let (source, snapshot) = self.split_merge_preview_snapshot().await?;
        let plans = split_merge_planner::plan_split_merge(
            &snapshot,
            &split_merge_planner::SplitMergePlannerConfig::default(),
        );
        Ok(SplitMergePreviewResponse {
            mode: "dry_run",
            source,
            assignments_changed: false,
            plans,
        })
    }

    async fn split_merge_preview_snapshot(
        &self,
    ) -> Result<(String, split_merge_planner::SplitMergeMetricsSnapshot), String> {
        if let Ok(raw) = std::env::var("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON")
            && !raw.trim().is_empty()
        {
            return parse_split_merge_preview_snapshot(
                "env:TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON",
                &raw,
            );
        }
        if let Ok(path) = std::env::var("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_PATH") {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                let raw = fs::read_to_string(trimmed)
                    .map_err(|err| format!("read split/merge preview snapshot {trimmed}: {err}"))?;
                return parse_split_merge_preview_snapshot(
                    &format!("env:TESSERA_ORCH_SPLIT_MERGE_PREVIEW_PATH:{trimmed}"),
                    &raw,
                );
            }
        }

        let handovers = self.handovers.read().await.clone();
        let assignments = self.assignments.read().await.clone();
        let mut worker_ids = assignments.keys().collect::<Vec<_>>();
        worker_ids.sort();
        let mut cells = Vec::new();
        for worker_id in worker_ids {
            let Some(assigned_cells) = assignments.get(worker_id) else {
                continue;
            };
            for cell in assigned_cells {
                cells.push(split_merge_planner::SplitMergeCellMetrics {
                    cell: *cell,
                    actor_count: 0,
                    move_queue_pressure: 0,
                    tick_stage_micros: 0,
                    relay_fanout: 0,
                    handover_failures: 0,
                    high_pressure_windows: 0,
                    low_pressure_windows: 0,
                    cell_age_secs: 0,
                    cooldown_remaining_secs: 0,
                    active_handover: handovers.contains_key(cell),
                    owner_worker_id: Some(worker_id.to_string()),
                });
            }
        }
        cells.sort_by_key(|cell| {
            (
                cell.cell.world,
                cell.cell.cy,
                cell.cell.cx,
                cell.cell.depth,
                cell.cell.sub,
            )
        });
        Ok((
            "assignment_listing_zero_metrics".to_string(),
            split_merge_planner::SplitMergeMetricsSnapshot {
                cells,
                active_plans: Vec::new(),
            },
        ))
    }

    async fn apply_split_activation(
        &self,
        request: SplitActivationRequest,
    ) -> Result<SplitActivationResponse, Status> {
        let parsed =
            ParsedSplitActivation::try_from_request(request).map_err(Status::invalid_argument)?;
        if self.split_activation_mode != SplitActivationMode::Manual {
            return Ok(split_activation_response(
                false,
                SplitActivationState::Disabled,
                "split activation is disabled; set TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual to enable manual staging",
                String::new(),
                Vec::new(),
                false,
            ));
        }

        let target_map = match parsed.child_target_map() {
            Ok(target_map) => target_map,
            Err(reason) => {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    reason,
                    String::new(),
                    Vec::new(),
                    false,
                ));
            }
        };
        let ParsedSplitChildTargetMap {
            child_cells,
            target_worker_ids,
            ..
        } = target_map;

        let assignments = self.assignments.read().await.clone();
        let runtime = self.runtime.read().await.clone();
        let handovers = self.handovers.read().await.clone();

        let source_worker_id = match owners_for_cell(&assignments, &parsed.parent).as_slice() {
            [owner] => owner.clone(),
            [] => {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    "source parent cell is not assigned to any configured worker",
                    String::new(),
                    Vec::new(),
                    false,
                ));
            }
            owners => {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    format!(
                        "source parent cell must have exactly one owner; current_owners={}",
                        owners.join(",")
                    ),
                    String::new(),
                    Vec::new(),
                    false,
                ));
            }
        };
        if self.config.worker(&source_worker_id).is_none() {
            return Ok(split_activation_response(
                false,
                SplitActivationState::Rejected,
                format!("source_worker_id={source_worker_id} is not configured"),
                source_worker_id,
                Vec::new(),
                false,
            ));
        }
        if target_worker_ids
            .iter()
            .all(|target_worker_id| target_worker_id == &source_worker_id)
        {
            return Ok(split_activation_response(
                false,
                SplitActivationState::Rejected,
                "target map assigns all children to the source worker; keep this as a dry-run/no-op recommendation",
                source_worker_id,
                Vec::new(),
                false,
            ));
        }
        let source_runtime = match runtime.get(source_worker_id.as_str()) {
            Some(runtime) => runtime.clone(),
            None => {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    format!("source_worker_id={source_worker_id} is not registered"),
                    source_worker_id,
                    Vec::new(),
                    false,
                ));
            }
        };

        for target_worker_id in &target_worker_ids {
            if self.config.worker(target_worker_id).is_none() {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    format!("unknown target_worker_id={target_worker_id}"),
                    source_worker_id,
                    Vec::new(),
                    false,
                ));
            }
            if !runtime.contains_key(target_worker_id.as_str()) {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    format!("target_worker_id={target_worker_id} is not registered"),
                    source_worker_id,
                    Vec::new(),
                    false,
                ));
            }
        }

        for child in &child_cells {
            let owners = owners_for_cell(&assignments, child);
            if !owners.is_empty() {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    format!(
                        "child cell depth={},sub={} is already published for owners={}",
                        child.depth,
                        child.sub,
                        owners.join(",")
                    ),
                    source_worker_id,
                    Vec::new(),
                    false,
                ));
            }
        }

        if handovers
            .keys()
            .any(|cell| cell == &parsed.parent || child_cells.contains(cell))
        {
            return Ok(split_activation_response(
                false,
                SplitActivationState::Rejected,
                "active handover already touches the split parent or child family",
                source_worker_id,
                Vec::new(),
                false,
            ));
        }

        let mut staged_splits = self.staged_splits.write().await;
        if staged_splits
            .values()
            .any(|staged| staged.operation_id == parsed.operation_id)
        {
            return Ok(split_activation_response(
                false,
                SplitActivationState::Rejected,
                format!(
                    "split activation operation_id={} is already staged",
                    parsed.operation_id
                ),
                source_worker_id,
                Vec::new(),
                false,
            ));
        }
        if let Some(staged) = staged_splits
            .values()
            .find(|staged| staged.parent.world == parsed.parent.world)
        {
            return Ok(split_activation_response(
                false,
                SplitActivationState::Rejected,
                format!(
                    "another split activation is already staged for world={} by source_worker_id={}",
                    parsed.parent.world, staged.source_worker_id
                ),
                source_worker_id,
                Vec::new(),
                false,
            ));
        }
        if staged_splits.values().any(|staged| {
            staged.parent == parsed.parent
                || staged
                    .children
                    .iter()
                    .any(|child| child.cell == parsed.parent || child_cells.contains(&child.cell))
        }) {
            return Ok(split_activation_response(
                false,
                SplitActivationState::Rejected,
                "another split activation already touches this cell family",
                source_worker_id,
                Vec::new(),
                false,
            ));
        }

        let staged = StagedSplitActivation {
            operation_id: parsed.operation_id.clone(),
            parent: parsed.parent,
            source_worker_id: source_worker_id.clone(),
            children: child_cells
                .into_iter()
                .zip(target_worker_ids)
                .map(|(cell, target_worker_id)| StagedSplitAssignment {
                    cell,
                    target_worker_id,
                })
                .collect(),
        };
        let staged_children = staged.as_response_children();
        let split_replay_targets = staged
            .children
            .iter()
            .map(|child| {
                let runtime = runtime
                    .get(child.target_worker_id.as_str())
                    .ok_or_else(|| {
                        format!(
                            "target_worker_id={} is not registered",
                            child.target_worker_id
                        )
                    })?;
                Ok(SplitReplayTarget {
                    cell: child.cell,
                    target_worker_id: child.target_worker_id.clone(),
                    target_addr: runtime.addr.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>();
        let split_replay_targets = match split_replay_targets {
            Ok(targets) => targets,
            Err(reason) => {
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Rejected,
                    reason,
                    source_worker_id,
                    Vec::new(),
                    false,
                ));
            }
        };
        staged_splits.insert(staged.parent, staged);
        drop(staged_splits);

        let mut prepared_targets = Vec::new();
        if let Err(reason) = prepare_split_replay_targets(
            &parsed.operation_id,
            &split_replay_targets,
            &mut prepared_targets,
        )
        .await
        {
            self.staged_splits.write().await.remove(&parsed.parent);
            abort_prepared_split_targets(&parsed.operation_id, &prepared_targets).await;
            return Ok(split_activation_response(
                false,
                SplitActivationState::Failed,
                reason,
                source_worker_id,
                staged_children,
                false,
            ));
        }

        match request_split_replay_from_source(
            &source_runtime.addr,
            parsed.operation_id.as_str(),
            parsed.parent,
            split_replay_targets.clone(),
        )
        .await
        {
            Ok(()) => {}
            Err(reason) => {
                self.staged_splits.write().await.remove(&parsed.parent);
                abort_prepared_split_targets(&parsed.operation_id, &prepared_targets).await;
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Failed,
                    reason,
                    source_worker_id,
                    staged_children,
                    false,
                ));
            }
        }

        let publish_result = {
            let mut assignments = self.assignments.write().await;
            let before = assignments.clone();
            match publish_split_assignments(
                &mut assignments,
                parsed.parent,
                &source_worker_id,
                &split_replay_targets,
            ) {
                Ok(changed) => {
                    if changed && let Err(reason) = self.persist_assignment_map(&assignments) {
                        *assignments = before;
                        Err(reason)
                    } else {
                        Ok(changed)
                    }
                }
                Err(reason) => Err(reason),
            }
        };
        let assignments_changed = match publish_result {
            Ok(changed) => changed,
            Err(reason) => {
                self.staged_splits.write().await.remove(&parsed.parent);
                abort_prepared_split_targets(&parsed.operation_id, &prepared_targets).await;
                abort_split_source(&source_runtime.addr, &parsed.operation_id, parsed.parent).await;
                return Ok(split_activation_response(
                    false,
                    SplitActivationState::Failed,
                    reason,
                    source_worker_id,
                    staged_children,
                    false,
                ));
            }
        };

        self.staged_splits.write().await.remove(&parsed.parent);
        self.publish_listing_if_changed().await;

        Ok(split_activation_response(
            true,
            SplitActivationState::Published,
            "split activation replayed all children and published assignments atomically",
            source_worker_id,
            staged_children,
            assignments_changed,
        ))
    }

    async fn apply_merge_activation(
        &self,
        request: MergeActivationRequest,
    ) -> Result<MergeActivationResponse, Status> {
        let parsed =
            ParsedMergeActivation::try_from_request(request).map_err(Status::invalid_argument)?;
        if self.split_activation_mode != SplitActivationMode::Manual {
            return Ok(merge_activation_response(
                false,
                MergeActivationState::Disabled,
                "merge activation is disabled; set TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual to enable manual publishing",
                String::new(),
                Vec::new(),
                false,
            ));
        }
        if merge_child_family_candidates(parsed.parent).is_empty() {
            return Ok(merge_activation_response(
                false,
                MergeActivationState::Rejected,
                format!(
                    "merge activation supports complete legacy shallow or canonical child families; got parent depth={},sub={}",
                    parsed.parent.depth, parsed.parent.sub
                ),
                parsed.owner_worker_id,
                Vec::new(),
                false,
            ));
        }

        if self.config.worker(&parsed.owner_worker_id).is_none() {
            return Ok(merge_activation_response(
                false,
                MergeActivationState::Rejected,
                format!(
                    "owner_worker_id={} is not configured",
                    parsed.owner_worker_id
                ),
                parsed.owner_worker_id,
                Vec::new(),
                false,
            ));
        }
        if !self
            .runtime
            .read()
            .await
            .contains_key(parsed.owner_worker_id.as_str())
        {
            return Ok(merge_activation_response(
                false,
                MergeActivationState::Rejected,
                format!(
                    "owner_worker_id={} is not registered",
                    parsed.owner_worker_id
                ),
                parsed.owner_worker_id,
                Vec::new(),
                false,
            ));
        }

        let assignments = self.assignments.read().await.clone();
        let handovers = self.handovers.read().await.clone();
        let children = match merge_child_cells_for_assignments(&assignments, parsed.parent) {
            Ok(children) => children,
            Err(reason) => {
                return Ok(merge_activation_response(
                    false,
                    MergeActivationState::Rejected,
                    reason,
                    parsed.owner_worker_id,
                    Vec::new(),
                    false,
                ));
            }
        };

        let parent_owners = owners_for_cell(&assignments, &parsed.parent);
        if !parent_owners.is_empty() {
            return Ok(merge_activation_response(
                false,
                MergeActivationState::Rejected,
                format!(
                    "merge parent is already assigned to owners={}",
                    parent_owners.join(",")
                ),
                parsed.owner_worker_id,
                children,
                false,
            ));
        }

        let mut child_owners = Vec::with_capacity(children.len());
        for child in &children {
            match owners_for_cell(&assignments, child).as_slice() {
                [owner] => {
                    if self.config.worker(owner).is_none() {
                        return Ok(merge_activation_response(
                            false,
                            MergeActivationState::Rejected,
                            format!("source_worker_id={owner} is not configured"),
                            parsed.owner_worker_id,
                            children,
                            false,
                        ));
                    }
                    if !self.runtime.read().await.contains_key(owner.as_str()) {
                        return Ok(merge_activation_response(
                            false,
                            MergeActivationState::Rejected,
                            format!("source_worker_id={owner} is not registered"),
                            parsed.owner_worker_id,
                            children,
                            false,
                        ));
                    }
                    child_owners.push((*child, owner.clone()));
                }
                [] => {
                    return Ok(merge_activation_response(
                        false,
                        MergeActivationState::Rejected,
                        format!(
                            "merge child depth={},sub={} is not assigned",
                            child.depth, child.sub
                        ),
                        parsed.owner_worker_id,
                        children,
                        false,
                    ));
                }
                owners => {
                    return Ok(merge_activation_response(
                        false,
                        MergeActivationState::Rejected,
                        format!(
                            "merge child depth={},sub={} has multiple owners={}",
                            child.depth,
                            child.sub,
                            owners.join(",")
                        ),
                        parsed.owner_worker_id,
                        children,
                        false,
                    ));
                }
            }
        }

        if handovers
            .keys()
            .any(|cell| cell == &parsed.parent || children.contains(cell))
        {
            return Ok(merge_activation_response(
                false,
                MergeActivationState::Rejected,
                "active handover already touches the merge parent or sibling family",
                parsed.owner_worker_id,
                children,
                false,
            ));
        }

        let staged_splits = self.staged_splits.read().await;
        if staged_splits.values().any(|staged| {
            staged.parent == parsed.parent
                || children.contains(&staged.parent)
                || staged
                    .children
                    .iter()
                    .any(|child| child.cell == parsed.parent || children.contains(&child.cell))
        }) {
            return Ok(merge_activation_response(
                false,
                MergeActivationState::Rejected,
                "staged split activation already touches the merge cell family",
                parsed.owner_worker_id,
                children,
                false,
            ));
        }
        drop(staged_splits);

        let runtime = self.runtime.read().await.clone();
        let target_addr = runtime
            .get(parsed.owner_worker_id.as_str())
            .map(|runtime| runtime.addr.clone())
            .ok_or_else(|| {
                Status::internal(format!(
                    "owner_worker_id={} disappeared from runtime after preflight",
                    parsed.owner_worker_id
                ))
            })?;
        let remote_sources = child_owners
            .iter()
            .filter(|(_, source_worker_id)| source_worker_id != &parsed.owner_worker_id)
            .map(|(source_cell, source_worker_id)| {
                runtime
                    .get(source_worker_id.as_str())
                    .map(|runtime| MergeReplaySource {
                        source_cell: *source_cell,
                        source_worker_id: source_worker_id.clone(),
                        source_addr: runtime.addr.clone(),
                    })
                    .ok_or_else(|| {
                        format!(
                            "source_worker_id={} disappeared from runtime after preflight",
                            source_worker_id
                        )
                    })
            })
            .collect::<Result<Vec<_>, String>>();
        let remote_sources = match remote_sources {
            Ok(sources) => sources,
            Err(reason) => {
                return Ok(merge_activation_response(
                    false,
                    MergeActivationState::Rejected,
                    reason,
                    parsed.owner_worker_id,
                    children,
                    false,
                ));
            }
        };

        let mut prepared_target_operations: Vec<String> = Vec::new();
        let mut replayed_sources = Vec::new();
        if !remote_sources.is_empty() {
            for source in &remote_sources {
                let replay_operation_id =
                    merge_replay_operation_key(parsed.operation_id.as_str(), source.source_cell);
                if let Err(reason) = prepare_merge_replay_target(
                    replay_operation_id.as_str(),
                    parsed.owner_worker_id.as_str(),
                    target_addr.as_str(),
                    parsed.parent,
                )
                .await
                {
                    for prepared_operation_id in &prepared_target_operations {
                        abort_merge_replay_target(
                            prepared_operation_id,
                            parsed.owner_worker_id.as_str(),
                            target_addr.as_str(),
                            parsed.parent,
                        )
                        .await;
                    }
                    return Ok(merge_activation_response(
                        false,
                        MergeActivationState::Failed,
                        reason,
                        parsed.owner_worker_id,
                        children,
                        false,
                    ));
                }
                prepared_target_operations.push(replay_operation_id);
            }

            for source in &remote_sources {
                if let Err(reason) = request_merge_replay_from_source(
                    source,
                    parsed.operation_id.as_str(),
                    parsed.parent,
                    parsed.owner_worker_id.as_str(),
                    target_addr.as_str(),
                )
                .await
                {
                    for prepared_operation_id in &prepared_target_operations {
                        abort_merge_replay_target(
                            prepared_operation_id,
                            parsed.owner_worker_id.as_str(),
                            target_addr.as_str(),
                            parsed.parent,
                        )
                        .await;
                    }
                    for replayed_source in &replayed_sources {
                        abort_merge_replay_source(
                            parsed.operation_id.as_str(),
                            parsed.parent,
                            replayed_source,
                        )
                        .await;
                    }
                    return Ok(merge_activation_response(
                        false,
                        MergeActivationState::Failed,
                        reason,
                        parsed.owner_worker_id,
                        children,
                        false,
                    ));
                }
                replayed_sources.push(source.clone());
            }
        }

        let publish_result = {
            let mut assignments = self.assignments.write().await;
            let before = assignments.clone();
            match publish_merge_assignments(
                &mut assignments,
                parsed.parent,
                &parsed.owner_worker_id,
                &child_owners,
            ) {
                Ok(changed) => {
                    if changed && let Err(reason) = self.persist_assignment_map(&assignments) {
                        *assignments = before;
                        Err(reason)
                    } else {
                        Ok(changed)
                    }
                }
                Err(reason) => Err(reason),
            }
        };
        let assignments_changed = match publish_result {
            Ok(changed) => changed,
            Err(reason) => {
                if !remote_sources.is_empty() {
                    for prepared_operation_id in &prepared_target_operations {
                        abort_merge_replay_target(
                            prepared_operation_id,
                            parsed.owner_worker_id.as_str(),
                            target_addr.as_str(),
                            parsed.parent,
                        )
                        .await;
                    }
                    for replayed_source in &replayed_sources {
                        abort_merge_replay_source(
                            parsed.operation_id.as_str(),
                            parsed.parent,
                            replayed_source,
                        )
                        .await;
                    }
                }
                return Ok(merge_activation_response(
                    false,
                    MergeActivationState::Failed,
                    reason,
                    parsed.owner_worker_id,
                    children,
                    false,
                ));
            }
        };

        self.publish_listing_if_changed().await;
        info!(
            target: "orch",
            operation_id = %parsed.operation_id,
            owner_worker_id = %parsed.owner_worker_id,
            parent = ?parsed.parent,
            assignments_changed,
            cross_worker_replay_sources = remote_sources.len(),
            "merge activation published parent assignment"
        );
        Ok(merge_activation_response(
            true,
            MergeActivationState::Published,
            if remote_sources.is_empty() {
                "merge activation published parent assignment; same-worker Worker refresh coalesces child runtime state into parent"
            } else {
                "merge activation replayed remote child runtime state and published parent assignment"
            },
            parsed.owner_worker_id,
            children,
            assignments_changed,
        ))
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
            let before = assignments.clone();
            match transfer_cell_assignment(
                &mut assignments,
                parsed.cell,
                &parsed.source_worker_id,
                &parsed.target_worker_id,
            ) {
                Ok(changed) => {
                    if changed && let Err(reason) = self.persist_assignment_map(&assignments) {
                        *assignments = before;
                        return Ok(handover_response(false, active.state, reason, false));
                    }
                    assignments_changed = changed;
                }
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

fn parse_split_merge_preview_snapshot(
    source: &str,
    raw: &str,
) -> Result<(String, split_merge_planner::SplitMergeMetricsSnapshot), String> {
    let snapshot = serde_json::from_str(raw)
        .map_err(|err| format!("parse split/merge preview snapshot from {source}: {err}"))?;
    Ok((source.to_string(), snapshot))
}

fn parse_operation_approval_request(query: &str) -> Result<OperationApprovalRequest, String> {
    let params = parse_query_params(query)?;
    let ttl_secs = required_query_param(&params, "ttl_secs")?
        .parse::<u64>()
        .map_err(|err| format!("invalid ttl_secs: {err}"))?;
    Ok(OperationApprovalRequest {
        operation_id: required_query_param(&params, "operation_id")?.to_string(),
        policy_id: required_query_param(&params, "policy_id")?.to_string(),
        approver: required_query_param(&params, "approver")?.to_string(),
        expected_proposal_hash: required_query_param(&params, "expected_proposal_hash")?
            .to_string(),
        ttl_secs,
        cooldown_key: required_query_param(&params, "cooldown_key")?.to_string(),
        budget_key: required_query_param(&params, "budget_key")?.to_string(),
    })
}

fn parse_query_params(query: &str) -> Result<HashMap<String, String>, String> {
    let mut params = HashMap::new();
    if query.trim().is_empty() {
        return Ok(params);
    }
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let Some((key, value)) = part.split_once('=') else {
            return Err(format!(
                "invalid query parameter `{part}`; expected key=value"
            ));
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            return Err(format!(
                "invalid query parameter `{part}`; key and value are required"
            ));
        }
        if params.insert(key.to_string(), value.to_string()).is_some() {
            return Err(format!("duplicate query parameter `{key}`"));
        }
    }
    Ok(params)
}

fn required_query_param<'a>(
    params: &'a HashMap<String, String>,
    key: &str,
) -> Result<&'a str, String> {
    params
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| format!("missing query parameter `{key}`"))
}

struct ParsedSplitActivation {
    operation_id: String,
    parent: CellId,
    targets: Vec<ParsedSplitChildTarget>,
}

struct ParsedSplitChildTarget {
    sub: u32,
    cell: Option<CellId>,
    target_worker_id: String,
}

struct ParsedSplitChildTargetMap {
    child_cells: Vec<CellId>,
    target_worker_ids: Vec<String>,
}

impl ParsedSplitActivation {
    fn try_from_request(request: SplitActivationRequest) -> Result<Self, String> {
        let operation_id = request.operation_id.trim();
        if operation_id.is_empty() {
            return Err("operation_id must not be empty".to_string());
        }
        let Some(parent_assignment) = request.parent.as_ref() else {
            return Err("parent must be set".to_string());
        };
        let parent = assignment_to_cell_ref(parent_assignment).map_err(|e| e.to_string())?;
        let targets = request
            .targets
            .into_iter()
            .map(|target| {
                let cell = target
                    .cell
                    .as_ref()
                    .map(assignment_to_cell_ref)
                    .transpose()
                    .map_err(|e| e.to_string())?;
                Ok(ParsedSplitChildTarget {
                    sub: target.sub,
                    cell,
                    target_worker_id: target.target_worker_id.trim().to_string(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            operation_id: operation_id.to_string(),
            parent,
            targets,
        })
    }

    fn child_target_map(&self) -> Result<ParsedSplitChildTargetMap, String> {
        if self.targets.iter().any(|target| target.cell.is_some()) {
            return self.explicit_child_target_map();
        }
        self.legacy_child_target_map()
    }

    fn legacy_child_target_map(&self) -> Result<ParsedSplitChildTargetMap, String> {
        let child_cells = legacy_split_child_cells(self.parent)
            .ok_or_else(|| {
                format!(
                    "split activation legacy sub targets require a depth=0/sub=0 parent; got depth={},sub={}",
                    self.parent.depth, self.parent.sub
                )
            })?
            .to_vec();
        let mut targets = vec![None::<String>; 4];
        for target in &self.targets {
            if target.sub > 3 {
                return Err(format!(
                    "target map child sub={} is out of range; expected 0..=3",
                    target.sub
                ));
            }
            if target.target_worker_id.is_empty() {
                return Err(format!(
                    "target_worker_id must not be empty for child sub={}",
                    target.sub
                ));
            }
            let slot = &mut targets[target.sub as usize];
            if slot.is_some() {
                return Err(format!(
                    "target map contains duplicate child sub={}",
                    target.sub
                ));
            }
            *slot = Some(target.target_worker_id.clone());
        }

        targets
            .into_iter()
            .enumerate()
            .map(|(sub, target_worker_id)| {
                target_worker_id.ok_or_else(|| format!("target map must include child sub={sub}"))
            })
            .collect::<Result<Vec<_>, String>>()
            .map(|target_worker_ids| ParsedSplitChildTargetMap {
                child_cells,
                target_worker_ids,
            })
    }

    fn explicit_child_target_map(&self) -> Result<ParsedSplitChildTargetMap, String> {
        if self.targets.iter().any(|target| target.cell.is_none()) {
            return Err(
                "target map must not mix explicit child cells with legacy sub targets".to_string(),
            );
        }
        if self.targets.len() != 4 {
            return Err("target map must include exactly four explicit child cells".to_string());
        }

        let mut cells = Vec::with_capacity(self.targets.len());
        for target in &self.targets {
            if target.target_worker_id.is_empty() {
                let cell = target.cell.expect("checked explicit cell");
                return Err(format!(
                    "target_worker_id must not be empty for child world={},cx={},cy={},depth={},sub={}",
                    cell.world, cell.cx, cell.cy, cell.depth, cell.sub
                ));
            }
            let cell = target.cell.expect("checked explicit cell");
            if cells.contains(&cell) {
                return Err(format!(
                    "target map contains duplicate child cell world={},cx={},cy={},depth={},sub={}",
                    cell.world, cell.cx, cell.cy, cell.depth, cell.sub
                ));
            }
            cells.push(cell);
        }

        let family_kind = self.parent.child_family_kind(&cells).ok_or_else(|| {
            "target child cells must exactly match either the legacy shallow family or the canonical leaf-coordinate family for the split parent".to_string()
        })?;
        let expected_children = match family_kind {
            CellChildFamilyKind::LegacyShallow => self
                .parent
                .legacy_shallow_children()
                .expect("legacy family validation requires legacy children"),
            CellChildFamilyKind::CanonicalLeaf => self
                .parent
                .canonical_children()
                .expect("canonical family validation requires canonical children"),
        };

        let target_worker_ids = expected_children
            .iter()
            .map(|expected| {
                self.targets
                    .iter()
                    .find(|target| target.cell == Some(*expected))
                    .map(|target| target.target_worker_id.clone())
                    .ok_or_else(|| {
                        format!(
                            "target map must include child world={},cx={},cy={},depth={},sub={}",
                            expected.world, expected.cx, expected.cy, expected.depth, expected.sub
                        )
                    })
            })
            .collect::<Result<Vec<_>, String>>()?;

        Ok(ParsedSplitChildTargetMap {
            child_cells: expected_children.to_vec(),
            target_worker_ids,
        })
    }
}

fn split_activation_response(
    accepted: bool,
    state: SplitActivationState,
    reason: impl Into<String>,
    source_worker_id: String,
    staged_children: Vec<StagedSplitChildAssignment>,
    assignments_changed: bool,
) -> SplitActivationResponse {
    SplitActivationResponse {
        accepted,
        state: state as i32,
        reason: reason.into(),
        source_worker_id,
        staged_children,
        assignments_changed,
    }
}

struct ParsedMergeActivation {
    operation_id: String,
    parent: CellId,
    owner_worker_id: String,
}

impl ParsedMergeActivation {
    fn try_from_request(request: MergeActivationRequest) -> Result<Self, String> {
        let operation_id = request.operation_id.trim();
        if operation_id.is_empty() {
            return Err("operation_id must not be empty".to_string());
        }
        let Some(parent_assignment) = request.parent.as_ref() else {
            return Err("parent must be set".to_string());
        };
        let parent = assignment_to_cell_ref(parent_assignment).map_err(|e| e.to_string())?;
        let owner_worker_id = request.owner_worker_id.trim();
        if owner_worker_id.is_empty() {
            return Err("owner_worker_id must not be empty".to_string());
        }
        Ok(Self {
            operation_id: operation_id.to_string(),
            parent,
            owner_worker_id: owner_worker_id.to_string(),
        })
    }
}

fn merge_activation_response(
    accepted: bool,
    state: MergeActivationState,
    reason: impl Into<String>,
    owner_worker_id: String,
    merged_children: Vec<CellId>,
    assignments_changed: bool,
) -> MergeActivationResponse {
    MergeActivationResponse {
        accepted,
        state: state as i32,
        reason: reason.into(),
        owner_worker_id,
        merged_children: merged_children
            .iter()
            .map(cell_to_assignment)
            .collect::<Vec<_>>(),
        assignments_changed,
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

    async fn submit_split_activation(
        &self,
        request: Request<SplitActivationRequest>,
    ) -> Result<Response<SplitActivationResponse>, Status> {
        let response = self.apply_split_activation(request.into_inner()).await?;
        Ok(Response::new(response))
    }

    async fn submit_merge_activation(
        &self,
        request: Request<MergeActivationRequest>,
    ) -> Result<Response<MergeActivationResponse>, Status> {
        let response = self.apply_merge_activation(request.into_inner()).await?;
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
    let raw_path = parts.next().unwrap_or_default();
    let (path, query) = raw_path.split_once('?').unwrap_or((raw_path, ""));

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
    } else if method == "GET" && path == "/split-merge/preview" {
        match service.split_merge_preview().await {
            Ok(preview) => {
                let body = serde_json::to_string_pretty(&preview)
                    .context("serialize split/merge preview response")?;
                write_http_response(&mut stream, "200 OK", "application/json", &body).await?;
            }
            Err(reason) => {
                let body = format!("split/merge preview unavailable: {reason}\n");
                write_http_response(
                    &mut stream,
                    "500 Internal Server Error",
                    "text/plain; charset=utf-8",
                    &body,
                )
                .await?;
            }
        }
    } else if method == "GET" && path == "/operations" {
        let snapshot = service.operation_ledger_snapshot().await;
        let body = serde_json::to_string_pretty(&snapshot)
            .context("serialize operation ledger response")?;
        write_http_response(&mut stream, "200 OK", "application/json", &body).await?;
    } else if method == "POST" && path == "/operations/proposals" {
        match service.record_split_merge_proposals().await {
            Ok(response) => {
                let body = serde_json::to_string_pretty(&response)
                    .context("serialize operation proposal write response")?;
                write_http_response(&mut stream, "200 OK", "application/json", &body).await?;
            }
            Err(reason) => {
                let status = if reason.contains("TESSERA_ORCH_OPERATION_LEDGER_PATH") {
                    "409 Conflict"
                } else {
                    "500 Internal Server Error"
                };
                let body = format!("operation proposal recording unavailable: {reason}\n");
                write_http_response(&mut stream, status, "text/plain; charset=utf-8", &body)
                    .await?;
            }
        }
    } else if method == "POST" && path == "/operations/approvals" {
        match parse_operation_approval_request(query) {
            Ok(request) => match service.approve_operation(request, SystemTime::now()).await {
                Ok(response) => {
                    let body = serde_json::to_string_pretty(&response)
                        .context("serialize operation approval write response")?;
                    write_http_response(&mut stream, "200 OK", "application/json", &body).await?;
                }
                Err(reason) => {
                    let status = if reason.contains("TESSERA_ORCH_OPERATION_LEDGER_PATH") {
                        "409 Conflict"
                    } else {
                        "400 Bad Request"
                    };
                    let body = format!("operation approval unavailable: {reason}\n");
                    write_http_response(&mut stream, status, "text/plain; charset=utf-8", &body)
                        .await?;
                }
            },
            Err(reason) => {
                let body = format!("operation approval request invalid: {reason}\n");
                write_http_response(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain; charset=utf-8",
                    &body,
                )
                .await?;
            }
        }
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

async fn prepare_split_replay_targets(
    operation_id: &str,
    targets: &[SplitReplayTarget],
    prepared_targets: &mut Vec<SplitReplayTarget>,
) -> Result<(), String> {
    let mut grouped: HashMap<(String, String), Vec<CellId>> = HashMap::new();
    for target in targets {
        grouped
            .entry((target.target_worker_id.clone(), target.target_addr.clone()))
            .or_default()
            .push(target.cell);
    }
    let mut groups = grouped.into_iter().collect::<Vec<_>>();
    groups.sort_by(|((worker_a, addr_a), _), ((worker_b, addr_b), _)| {
        worker_a.cmp(worker_b).then_with(|| addr_a.cmp(addr_b))
    });

    for ((target_worker_id, target_addr), mut cells) in groups {
        cells.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
        let ack = send_worker_relay_command(
            &target_addr,
            cells[0],
            WorkerRelayMsg::SplitReplayPrepare {
                operation_id: operation_id.to_string(),
                cells: cells.clone(),
            },
        )
        .await?;
        match ack {
            WorkerRelayMsg::SplitReplayAck {
                operation_id: ack_operation_id,
                accepted,
                reason,
                ..
            } if ack_operation_id == operation_id => {
                if !accepted {
                    return Err(format!(
                        "target_worker_id={target_worker_id} rejected split replay prepare: {reason}"
                    ));
                }
            }
            other => {
                return Err(format!(
                    "target_worker_id={target_worker_id} returned unexpected split prepare ack: {other:?}"
                ));
            }
        }
        prepared_targets.extend(
            targets
                .iter()
                .filter(|target| {
                    target.target_worker_id == target_worker_id && cells.contains(&target.cell)
                })
                .cloned(),
        );
    }
    Ok(())
}

async fn request_split_replay_from_source(
    source_addr: &str,
    operation_id: &str,
    parent: CellId,
    children: Vec<SplitReplayTarget>,
) -> Result<(), String> {
    let ack = send_worker_relay_command(
        source_addr,
        parent,
        WorkerRelayMsg::SplitReplayRequest {
            operation_id: operation_id.to_string(),
            parent,
            children,
        },
    )
    .await?;
    match ack {
        WorkerRelayMsg::SplitReplayAck {
            operation_id: ack_operation_id,
            parent: ack_parent,
            accepted,
            reason,
            ..
        } if ack_operation_id == operation_id && ack_parent == parent => {
            if accepted {
                Ok(())
            } else {
                Err(format!("source worker rejected split replay: {reason}"))
            }
        }
        other => Err(format!(
            "source worker returned unexpected split replay ack: {other:?}"
        )),
    }
}

async fn abort_prepared_split_targets(operation_id: &str, targets: &[SplitReplayTarget]) {
    let mut grouped: HashMap<(String, String), Vec<CellId>> = HashMap::new();
    for target in targets {
        grouped
            .entry((target.target_worker_id.clone(), target.target_addr.clone()))
            .or_default()
            .push(target.cell);
    }
    for ((target_worker_id, target_addr), cells) in grouped {
        if let Err(reason) = send_worker_relay_command(
            &target_addr,
            cells[0],
            WorkerRelayMsg::SplitReplayAbort {
                operation_id: operation_id.to_string(),
                cells,
            },
        )
        .await
        {
            warn!(
                target: "orch",
                target_worker_id,
                reason,
                "failed to abort prepared split replay target"
            );
        }
    }
}

async fn abort_split_source(source_addr: &str, operation_id: &str, parent: CellId) {
    if let Err(reason) = send_worker_relay_command(
        source_addr,
        parent,
        WorkerRelayMsg::SplitReplayAbort {
            operation_id: operation_id.to_string(),
            cells: vec![parent],
        },
    )
    .await
    {
        warn!(
            target: "orch",
            source_addr,
            reason,
            "failed to abort source split replay state"
        );
    }
}

#[derive(Debug, Clone)]
struct MergeReplaySource {
    source_cell: CellId,
    source_worker_id: String,
    source_addr: String,
}

fn merge_replay_operation_key(operation_id: &str, source_cell: CellId) -> String {
    format!(
        "{}:merge-source:{}:{}:{}:{}:{}",
        operation_id,
        source_cell.world,
        source_cell.cx,
        source_cell.cy,
        source_cell.depth,
        source_cell.sub
    )
}

async fn prepare_merge_replay_target(
    operation_id: &str,
    target_worker_id: &str,
    target_addr: &str,
    parent: CellId,
) -> Result<(), String> {
    let ack = send_worker_relay_command(
        target_addr,
        parent,
        WorkerRelayMsg::MergeReplayPrepare {
            operation_id: operation_id.to_string(),
            parent,
        },
    )
    .await?;
    match ack {
        WorkerRelayMsg::MergeReplayAck {
            operation_id: ack_operation_id,
            parent: ack_parent,
            accepted,
            reason,
            ..
        } if ack_operation_id == operation_id && ack_parent == parent => {
            if accepted {
                Ok(())
            } else {
                Err(format!(
                    "target_worker_id={target_worker_id} rejected merge replay prepare: {reason}"
                ))
            }
        }
        other => Err(format!(
            "target_worker_id={target_worker_id} returned unexpected merge prepare ack: {other:?}"
        )),
    }
}

async fn request_merge_replay_from_source(
    source: &MergeReplaySource,
    operation_id: &str,
    parent: CellId,
    target_worker_id: &str,
    target_addr: &str,
) -> Result<(), String> {
    let ack = send_worker_relay_command(
        &source.source_addr,
        source.source_cell,
        WorkerRelayMsg::MergeReplayRequest {
            operation_id: operation_id.to_string(),
            target: MergeReplayTarget {
                parent,
                source_cell: source.source_cell,
                target_worker_id: target_worker_id.to_string(),
                target_addr: target_addr.to_string(),
            },
        },
    )
    .await?;
    match ack {
        WorkerRelayMsg::MergeReplayAck {
            operation_id: ack_operation_id,
            parent: ack_parent,
            source_cell,
            accepted,
            reason,
        } if ack_operation_id == operation_id
            && ack_parent == parent
            && source_cell == Some(source.source_cell) =>
        {
            if accepted {
                Ok(())
            } else {
                Err(format!(
                    "source_worker_id={} rejected merge replay for source_cell={:?}: {reason}",
                    source.source_worker_id, source.source_cell
                ))
            }
        }
        other => Err(format!(
            "source_worker_id={} returned unexpected merge replay ack: {other:?}",
            source.source_worker_id
        )),
    }
}

async fn abort_merge_replay_target(
    operation_id: &str,
    target_worker_id: &str,
    target_addr: &str,
    parent: CellId,
) {
    if let Err(reason) = send_worker_relay_command(
        target_addr,
        parent,
        WorkerRelayMsg::MergeReplayAbort {
            operation_id: operation_id.to_string(),
            parent,
            source_cell: None,
        },
    )
    .await
    {
        warn!(
            target: "orch",
            target_worker_id,
            reason,
            "failed to abort prepared merge replay target"
        );
    }
}

async fn abort_merge_replay_source(operation_id: &str, parent: CellId, source: &MergeReplaySource) {
    if let Err(reason) = send_worker_relay_command(
        &source.source_addr,
        source.source_cell,
        WorkerRelayMsg::MergeReplayAbort {
            operation_id: operation_id.to_string(),
            parent,
            source_cell: Some(source.source_cell),
        },
    )
    .await
    {
        warn!(
            target: "orch",
            source_worker_id = %source.source_worker_id,
            source_cell = ?source.source_cell,
            reason,
            "failed to abort merge replay source"
        );
    }
}

async fn send_worker_relay_command(
    addr: &str,
    cell: CellId,
    payload: WorkerRelayMsg,
) -> Result<WorkerRelayMsg, String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|err| format!("connect worker relay {addr}: {err}"))?;
    let env = Envelope {
        cell,
        seq: 0,
        epoch: 0,
        payload,
    };
    let frame = encode_frame(&env);
    stream
        .write_all(&frame)
        .await
        .map_err(|err| format!("write worker relay command to {addr}: {err}"))?;
    stream
        .shutdown()
        .await
        .map_err(|err| format!("shutdown worker relay command to {addr}: {err}"))?;
    read_worker_relay_response(&mut stream).await
}

async fn read_worker_relay_response(stream: &mut TcpStream) -> Result<WorkerRelayMsg, String> {
    let mut len_buf = [0_u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| format!("read worker relay response length: {err}"))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(format!(
            "worker relay response frame length {len} exceeds max {MAX_FRAME_LEN}"
        ));
    }
    let mut payload = vec![0_u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| format!("read worker relay response payload: {err}"))?;
    let env: Envelope<WorkerRelayMsg> = serde_json::from_slice(&payload)
        .map_err(|err| format!("decode worker relay response: {err}"))?;
    Ok(env.payload)
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
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
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

    fn two_worker_canonical_split_config(parent: CellId) -> Config {
        let mut config = two_worker_handover_config();
        config.workers.get_mut("worker-a").expect("worker-a").cells = vec![parent];
        config
    }

    fn two_worker_merge_config() -> Config {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: (0..4)
                    .map(|sub| split_child_cell(CellId::grid(0, 0, 0), sub))
                    .collect(),
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

    fn two_worker_canonical_merge_config(parent: CellId) -> Config {
        let mut workers = HashMap::new();
        workers.insert(
            "worker-a".to_string(),
            WorkerStatic {
                addr: "10.0.0.1:5001".to_string(),
                cells: parent
                    .canonical_children()
                    .expect("canonical merge parent")
                    .to_vec(),
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

    fn split_activation_request(targets: &[(u32, &str)]) -> SplitActivationRequest {
        SplitActivationRequest {
            operation_id: "split-1".to_string(),
            parent: Some(cell_to_assignment(&CellId::grid(0, 0, 0))),
            targets: targets
                .iter()
                .map(
                    |(sub, target_worker_id)| tessera_proto::orch::v1::SplitChildTarget {
                        sub: *sub,
                        target_worker_id: (*target_worker_id).to_string(),
                        cell: None,
                    },
                )
                .collect(),
        }
    }

    fn split_activation_cell_request(
        parent: CellId,
        targets: &[(CellId, &str)],
    ) -> SplitActivationRequest {
        SplitActivationRequest {
            operation_id: "split-cells-1".to_string(),
            parent: Some(cell_to_assignment(&parent)),
            targets: targets
                .iter()
                .map(
                    |(cell, target_worker_id)| tessera_proto::orch::v1::SplitChildTarget {
                        sub: 0,
                        target_worker_id: (*target_worker_id).to_string(),
                        cell: Some(cell_to_assignment(cell)),
                    },
                )
                .collect(),
        }
    }

    fn valid_split_activation_request() -> SplitActivationRequest {
        split_activation_request(&[
            (0, "worker-b"),
            (1, "worker-b"),
            (2, "worker-b"),
            (3, "worker-b"),
        ])
    }

    fn valid_merge_activation_request() -> MergeActivationRequest {
        MergeActivationRequest {
            operation_id: "merge-1".to_string(),
            parent: Some(cell_to_assignment(&CellId::grid(0, 0, 0))),
            owner_worker_id: "worker-a".to_string(),
        }
    }

    async fn register_worker_for_test(service: &OrchestratorService, worker_id: &str, addr: &str) {
        service
            .register_worker(Request::new(WorkerRegistration {
                worker_id: worker_id.to_string(),
                addr: addr.to_string(),
            }))
            .await
            .expect("register worker");
    }

    fn unique_assignment_state_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "tessera-orch-{name}-{}-{nanos}.json",
            std::process::id()
        ))
    }

    fn sample_operation_record(operation_id: &str) -> OperationRecord {
        OperationRecord {
            operation_id: operation_id.to_string(),
            kind: DynamicOperationKind::Split,
            status: OperationStatus::Proposed,
            created_unix_secs: 100,
            updated_unix_secs: 100,
            proposal: OperationProposal {
                source: "live_worker_metrics:worker-a=127.0.0.1:5100".to_string(),
                proposal_hash: "proposal-hash-1".to_string(),
                parent: Some(CellId::grid(0, 0, 0)),
                targets: vec![
                    OperationTarget {
                        cell: split_child_cell(CellId::grid(0, 0, 0), 0),
                        worker_id: "worker-a".to_string(),
                    },
                    OperationTarget {
                        cell: split_child_cell(CellId::grid(0, 0, 0), 1),
                        worker_id: "worker-b".to_string(),
                    },
                ],
                preconditions: vec![
                    "source worker registered".to_string(),
                    "target worker registered".to_string(),
                ],
                submission_command: "cargo xt split-activation --operation-id op-1".to_string(),
            },
            approval: None,
            phases: vec![OperationPhase {
                name: "proposal_recorded".to_string(),
                state: OperationPhaseState::Succeeded,
                unix_secs: 100,
                reason: "planner proposal persisted without assignment mutation".to_string(),
            }],
        }
    }

    fn spawn_split_replay_ack_server(
        listener: TcpListener,
        accept_count: usize,
    ) -> JoinHandle<Vec<WorkerRelayMsg>> {
        tokio::spawn(async move {
            let mut received = Vec::new();
            for _ in 0..accept_count {
                let (mut stream, _) = listener.accept().await.expect("accept split replay");
                let mut len_buf = [0_u8; 4];
                stream.read_exact(&mut len_buf).await.expect("read len");
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut payload = vec![0_u8; len];
                stream.read_exact(&mut payload).await.expect("read payload");
                let env: Envelope<WorkerRelayMsg> =
                    serde_json::from_slice(&payload).expect("decode worker relay command");
                let ack = match &env.payload {
                    WorkerRelayMsg::SplitReplayPrepare {
                        operation_id,
                        cells,
                    } => WorkerRelayMsg::SplitReplayAck {
                        operation_id: operation_id.clone(),
                        parent: cells.first().copied().unwrap_or(env.cell),
                        accepted: true,
                        reason: "prepared".to_string(),
                        children: cells.clone(),
                    },
                    WorkerRelayMsg::SplitReplayRequest {
                        operation_id,
                        parent,
                        children,
                    } => WorkerRelayMsg::SplitReplayAck {
                        operation_id: operation_id.clone(),
                        parent: *parent,
                        accepted: true,
                        reason: "replayed".to_string(),
                        children: children.iter().map(|child| child.cell).collect(),
                    },
                    WorkerRelayMsg::SplitReplayAbort {
                        operation_id,
                        cells,
                    } => WorkerRelayMsg::SplitReplayAck {
                        operation_id: operation_id.clone(),
                        parent: cells.first().copied().unwrap_or(env.cell),
                        accepted: true,
                        reason: "aborted".to_string(),
                        children: cells.clone(),
                    },
                    WorkerRelayMsg::MergeReplayPrepare {
                        operation_id,
                        parent,
                    } => WorkerRelayMsg::MergeReplayAck {
                        operation_id: operation_id.clone(),
                        parent: *parent,
                        source_cell: None,
                        accepted: true,
                        reason: "merge prepared".to_string(),
                    },
                    WorkerRelayMsg::MergeReplayRequest {
                        operation_id,
                        target,
                    } => WorkerRelayMsg::MergeReplayAck {
                        operation_id: operation_id.clone(),
                        parent: target.parent,
                        source_cell: Some(target.source_cell),
                        accepted: true,
                        reason: "merge replayed".to_string(),
                    },
                    WorkerRelayMsg::MergeReplayAbort {
                        operation_id,
                        parent,
                        source_cell,
                    } => WorkerRelayMsg::MergeReplayAck {
                        operation_id: operation_id.clone(),
                        parent: *parent,
                        source_cell: *source_cell,
                        accepted: true,
                        reason: "merge aborted".to_string(),
                    },
                    other => panic!("unexpected relay command: {other:?}"),
                };
                received.push(env.payload);
                let ack_env = Envelope {
                    cell: env.cell,
                    seq: 0,
                    epoch: env.epoch,
                    payload: ack,
                };
                let frame = encode_frame(&ack_env);
                stream.write_all(&frame).await.expect("write ack");
                stream.shutdown().await.expect("shutdown ack");
            }
            received
        })
    }

    fn spawn_split_replay_reject_source_server(
        listener: TcpListener,
    ) -> JoinHandle<WorkerRelayMsg> {
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept split replay");
            let mut len_buf = [0_u8; 4];
            stream.read_exact(&mut len_buf).await.expect("read len");
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut payload = vec![0_u8; len];
            stream.read_exact(&mut payload).await.expect("read payload");
            let env: Envelope<WorkerRelayMsg> =
                serde_json::from_slice(&payload).expect("decode worker relay command");
            let (operation_id, parent) = match &env.payload {
                WorkerRelayMsg::SplitReplayRequest {
                    operation_id,
                    parent,
                    ..
                } => (operation_id.clone(), *parent),
                other => panic!("unexpected relay command: {other:?}"),
            };
            let ack_env = Envelope {
                cell: parent,
                seq: 0,
                epoch: env.epoch,
                payload: WorkerRelayMsg::SplitReplayAck {
                    operation_id,
                    parent,
                    accepted: false,
                    reason: "source replay failed".to_string(),
                    children: Vec::new(),
                },
            };
            let frame = encode_frame(&ack_env);
            stream.write_all(&frame).await.expect("write ack");
            stream.shutdown().await.expect("shutdown ack");
            env.payload
        })
    }

    async fn assert_split_rejected_without_assignment_change(
        service: &OrchestratorService,
        request: SplitActivationRequest,
        reason: &str,
    ) -> SplitActivationResponse {
        let before = service.listing().await;
        let response = service
            .submit_split_activation(Request::new(request))
            .await
            .expect("submit split activation")
            .into_inner();
        let after = service.listing().await;

        assert!(!response.accepted);
        assert_eq!(response.state, SplitActivationState::Rejected as i32);
        assert!(response.reason.contains(reason), "{}", response.reason);
        assert!(!response.assignments_changed);
        assert_eq!(before, after);
        response
    }

    async fn assert_merge_rejected_without_assignment_change(
        service: &OrchestratorService,
        request: MergeActivationRequest,
        reason: &str,
    ) -> MergeActivationResponse {
        let before = service.listing().await;
        let response = service
            .submit_merge_activation(Request::new(request))
            .await
            .expect("submit merge activation")
            .into_inner();
        let after = service.listing().await;

        assert!(!response.accepted);
        assert_eq!(response.state, MergeActivationState::Rejected as i32);
        assert!(response.reason.contains(reason), "{}", response.reason);
        assert!(!response.assignments_changed);
        assert_eq!(before, after);
        response
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
            low_pressure_windows: 0,
            cell_age_secs: 60,
            cooldown_remaining_secs: 0,
            active_handover: false,
            owner_worker_id: Some("worker-a".to_string()),
        }
    }

    fn merge_child_cell(world: u32, cx: i32, cy: i32, sub: u8) -> CellId {
        CellId {
            world,
            cx,
            cy,
            depth: 1,
            sub,
        }
    }

    fn merge_metrics(world: u32, cx: i32, cy: i32, sub: u8, owner: &str) -> SplitMergeCellMetrics {
        SplitMergeCellMetrics {
            cell: merge_child_cell(world, cx, cy, sub),
            actor_count: 5,
            move_queue_pressure: 1,
            tick_stage_micros: 1_000,
            relay_fanout: 1,
            handover_failures: 0,
            high_pressure_windows: 0,
            low_pressure_windows: 5,
            cell_age_secs: 120,
            cooldown_remaining_secs: 0,
            active_handover: false,
            owner_worker_id: Some(owner.to_string()),
        }
    }

    fn canonical_merge_metrics(cell: CellId, owner: &str) -> SplitMergeCellMetrics {
        SplitMergeCellMetrics {
            cell,
            actor_count: 5,
            move_queue_pressure: 1,
            tick_stage_micros: 1_000,
            relay_fanout: 1,
            handover_failures: 0,
            high_pressure_windows: 0,
            low_pressure_windows: 5,
            cell_age_secs: 120,
            cooldown_remaining_secs: 0,
            active_handover: false,
            owner_worker_id: Some(owner.to_string()),
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

    #[test]
    fn split_merge_planner_requires_complete_cold_siblings_for_merge() {
        let config = SplitMergePlannerConfig {
            max_active_plans_per_world: 3,
            max_handover_ops_per_interval: 3,
            max_cells_moved_per_interval: 8,
            ..SplitMergePlannerConfig::default()
        };
        let mut hot_sibling = merge_metrics(0, 2, 0, 3, "worker-a");
        hot_sibling.actor_count = 80;
        hot_sibling.move_queue_pressure = 20;
        let mut too_young = merge_metrics(0, 3, 0, 0, "worker-a");
        too_young.cell_age_secs = 30;

        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: vec![
                    merge_metrics(0, 0, 0, 0, "worker-a"),
                    merge_metrics(0, 0, 0, 1, "worker-a"),
                    merge_metrics(0, 0, 0, 2, "worker-a"),
                    merge_metrics(0, 0, 0, 3, "worker-a"),
                    merge_metrics(0, 1, 0, 0, "worker-a"),
                    merge_metrics(0, 1, 0, 1, "worker-a"),
                    merge_metrics(0, 1, 0, 2, "worker-a"),
                    merge_metrics(0, 2, 0, 0, "worker-a"),
                    merge_metrics(0, 2, 0, 1, "worker-a"),
                    merge_metrics(0, 2, 0, 2, "worker-a"),
                    hot_sibling,
                    too_young,
                    merge_metrics(0, 3, 0, 1, "worker-a"),
                    merge_metrics(0, 3, 0, 2, "worker-a"),
                    merge_metrics(0, 3, 0, 3, "worker-a"),
                    merge_metrics(0, 4, 0, 0, "worker-a"),
                    merge_metrics(0, 4, 0, 1, "worker-a"),
                    merge_metrics(0, 4, 0, 2, "worker-b"),
                    merge_metrics(0, 4, 0, 3, "worker-a"),
                ],
                active_plans: Vec::new(),
            },
            &config,
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, SplitMergePlanKind::Merge);
        assert_eq!(plans[0].cell, CellId::grid(0, 0, 0));
        assert_eq!(plans[0].pressure_signals, 5);
        assert_eq!(plans[0].required_handover_ops, 1);
        assert_eq!(plans[0].cells_moved, 4);
    }

    #[test]
    fn split_merge_planner_detects_canonical_cold_siblings_for_merge() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let config = SplitMergePlannerConfig {
            max_active_plans_per_world: 3,
            max_handover_ops_per_interval: 3,
            max_cells_moved_per_interval: 8,
            ..SplitMergePlannerConfig::default()
        };
        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: children
                    .into_iter()
                    .map(|child| canonical_merge_metrics(child, "worker-a"))
                    .collect(),
                active_plans: Vec::new(),
            },
            &config,
        );

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, SplitMergePlanKind::Merge);
        assert_eq!(plans[0].cell, parent);
        assert_eq!(plans[0].pressure_signals, 5);
        assert_eq!(plans[0].required_handover_ops, 1);
        assert_eq!(plans[0].cells_moved, 4);
    }

    #[test]
    fn split_merge_planner_rejects_merge_budget_and_overlap() {
        let over_budget = SplitMergePlannerConfig {
            max_active_plans_per_world: 1,
            max_handover_ops_per_interval: 1,
            max_cells_moved_per_interval: 3,
            ..SplitMergePlannerConfig::default()
        };
        let cells = vec![
            merge_metrics(0, 0, 0, 0, "worker-a"),
            merge_metrics(0, 0, 0, 1, "worker-a"),
            merge_metrics(0, 0, 0, 2, "worker-a"),
            merge_metrics(0, 0, 0, 3, "worker-a"),
        ];
        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: cells.clone(),
                active_plans: Vec::new(),
            },
            &over_budget,
        );
        assert!(plans.is_empty());

        let blocked = SplitMergePlannerConfig {
            max_active_plans_per_world: 2,
            max_handover_ops_per_interval: 2,
            max_cells_moved_per_interval: 4,
            ..SplitMergePlannerConfig::default()
        };
        let plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells,
                active_plans: vec![SplitMergeActivePlan {
                    kind: SplitMergePlanKind::Split,
                    cell: CellId::grid(0, 0, 0),
                }],
            },
            &blocked,
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn split_merge_preview_snapshot_json_drives_dry_run_plans() {
        let raw = r#"
        {
            "cells": [
                {
                    "cell": {"world": 0, "cx": 0, "cy": 0},
                    "actor_count": 140,
                    "move_queue_pressure": 70,
                    "high_pressure_windows": 3,
                    "cell_age_secs": 120,
                    "owner_worker_id": "worker-a"
                }
            ]
        }
        "#;

        let (source, snapshot) =
            parse_split_merge_preview_snapshot("test-snapshot", raw).expect("parse snapshot");
        let plans = plan_split_merge(&snapshot, &SplitMergePlannerConfig::default());

        assert_eq!(source, "test-snapshot");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].kind, SplitMergePlanKind::Split);
        assert_eq!(plans[0].cell, CellId::grid(0, 0, 0));
    }

    #[tokio::test]
    async fn split_merge_preview_uses_assignment_listing_without_mutation() {
        let service = OrchestratorService::new(two_worker_handover_config());
        let before = service.listing().await;

        let preview = service.split_merge_preview().await.expect("preview");
        let after = service.listing().await;

        assert_eq!(preview.mode, "dry_run");
        assert_eq!(preview.source, "assignment_listing_zero_metrics");
        assert!(!preview.assignments_changed);
        assert!(preview.plans.is_empty());
        assert_eq!(before, after);
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
        let merge_plans = plan_split_merge(
            &SplitMergeMetricsSnapshot {
                cells: vec![
                    merge_metrics(0, 0, 0, 0, "worker-a"),
                    merge_metrics(0, 0, 0, 1, "worker-a"),
                    merge_metrics(0, 0, 0, 2, "worker-a"),
                    merge_metrics(0, 0, 0, 3, "worker-a"),
                ],
                active_plans: Vec::new(),
            },
            &SplitMergePlannerConfig {
                max_active_plans_per_world: 1,
                max_handover_ops_per_interval: 1,
                max_cells_moved_per_interval: 4,
                ..SplitMergePlannerConfig::default()
            },
        );
        let after = service.listing().await;

        assert_eq!(plans.len(), 1);
        assert_eq!(merge_plans.len(), 1);
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn split_activation_disabled_rejects_without_assignment_mutation() {
        let service = OrchestratorService::new(two_worker_handover_config());
        let before = service.listing().await;

        let response = service
            .submit_split_activation(Request::new(valid_split_activation_request()))
            .await
            .expect("submit split activation")
            .into_inner();
        let after = service.listing().await;

        assert!(!response.accepted);
        assert_eq!(response.state, SplitActivationState::Disabled as i32);
        assert!(response.reason.contains("disabled"));
        assert!(!response.assignments_changed);
        assert!(response.staged_children.is_empty());
        assert_eq!(before, after);
        assert!(service.staged_splits.read().await.is_empty());
    }

    #[tokio::test]
    async fn split_activation_publishes_child_assignments_after_replay() {
        let service = OrchestratorService::new_with_split_activation(
            two_worker_handover_config(),
            SplitActivationMode::Manual,
        );
        let source_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind source relay");
        let source_addr = source_listener.local_addr().expect("source addr");
        let target_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target relay");
        let target_addr = target_listener.local_addr().expect("target addr");
        let source_server = spawn_split_replay_ack_server(source_listener, 1);
        let target_server = spawn_split_replay_ack_server(target_listener, 1);
        register_worker_for_test(&service, "worker-a", &source_addr.to_string()).await;
        register_worker_for_test(&service, "worker-b", &target_addr.to_string()).await;

        let response = service
            .submit_split_activation(Request::new(valid_split_activation_request()))
            .await
            .expect("submit split activation")
            .into_inner();

        assert!(response.accepted);
        assert_eq!(response.state, SplitActivationState::Published as i32);
        assert_eq!(response.source_worker_id, "worker-a");
        assert!(response.assignments_changed);
        assert_eq!(response.staged_children.len(), 4);
        for (sub, child) in response.staged_children.iter().enumerate() {
            let cell = child.cell.as_ref().expect("staged child cell");
            assert_eq!(cell.world, 0);
            assert_eq!(cell.cx, 0);
            assert_eq!(cell.cy, 0);
            assert_eq!(cell.depth, 1);
            assert_eq!(cell.sub, sub as u32);
            assert_eq!(child.target_worker_id, "worker-b");
        }
        let source_received = source_server.await.expect("source server");
        let target_received = target_server.await.expect("target server");
        assert!(matches!(
            target_received.as_slice(),
            [WorkerRelayMsg::SplitReplayPrepare { .. }]
        ));
        assert!(matches!(
            source_received.as_slice(),
            [WorkerRelayMsg::SplitReplayRequest { .. }]
        ));

        let staged = service.staged_splits.read().await;
        assert!(staged.is_empty());

        let listing = service.listing().await;
        let source = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("source listing");
        assert!(source.cells.is_empty());
        let target = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-b")
            .expect("target listing");
        assert_eq!(target.cells.len(), 4);
        assert!(
            target
                .cells
                .iter()
                .enumerate()
                .all(|(sub, cell)| cell.depth == 1 && cell.sub == sub as u32)
        );

        let target_snapshot = service
            .get_assignments(Request::new(AssignmentQuery {
                worker_id: "worker-b".into(),
            }))
            .await
            .expect("get target assignments")
            .into_inner();
        assert_eq!(target_snapshot.cells.len(), 4);
    }

    #[tokio::test]
    async fn persistent_assignment_state_recovers_published_split_after_restart() {
        let state_path = unique_assignment_state_path("split-restart");
        let service = OrchestratorService::try_new_with_split_activation(
            two_worker_handover_config(),
            SplitActivationMode::Manual,
            Some(state_path.clone()),
        )
        .expect("construct service with assignment state");
        let source_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind source relay");
        let source_addr = source_listener.local_addr().expect("source addr");
        let target_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target relay");
        let target_addr = target_listener.local_addr().expect("target addr");
        let source_server = spawn_split_replay_ack_server(source_listener, 1);
        let target_server = spawn_split_replay_ack_server(target_listener, 1);
        register_worker_for_test(&service, "worker-a", &source_addr.to_string()).await;
        register_worker_for_test(&service, "worker-b", &target_addr.to_string()).await;

        let response = service
            .submit_split_activation(Request::new(valid_split_activation_request()))
            .await
            .expect("submit split activation")
            .into_inner();

        assert!(response.accepted);
        assert_eq!(response.state, SplitActivationState::Published as i32);
        assert!(response.assignments_changed);
        source_server.await.expect("source server");
        target_server.await.expect("target server");
        let raw_state = fs::read_to_string(&state_path).expect("read persisted assignment state");
        assert!(raw_state.contains(ASSIGNMENT_STATE_SCHEMA));

        let restarted = OrchestratorService::try_new_with_split_activation(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            Some(state_path.clone()),
        )
        .expect("restart service from assignment state");
        let listing = restarted.listing().await;
        let source = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("source listing");
        assert!(source.cells.is_empty());
        let target = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-b")
            .expect("target listing");
        assert_eq!(target.cells.len(), 4);
        assert!(
            target
                .cells
                .iter()
                .enumerate()
                .all(|(sub, cell)| cell.depth == 1 && cell.sub == sub as u32)
        );

        let target_snapshot = restarted
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-b".into(),
                addr: "10.0.0.2:5001".into(),
            }))
            .await
            .expect("register restarted worker")
            .into_inner();
        assert_eq!(target_snapshot.cells.len(), 4);
        let _ = fs::remove_file(state_path);
    }

    #[tokio::test]
    async fn split_activation_validates_target_map_and_depth_without_assignment_mutation() {
        let service = OrchestratorService::new_with_split_activation(
            two_worker_handover_config(),
            SplitActivationMode::Manual,
        );

        assert_split_rejected_without_assignment_change(
            &service,
            split_activation_request(&[(0, "worker-b"), (1, "worker-b"), (2, "worker-b")]),
            "child sub=3",
        )
        .await;
        assert_split_rejected_without_assignment_change(
            &service,
            split_activation_request(&[
                (0, "worker-b"),
                (1, "worker-b"),
                (1, "worker-b"),
                (3, "worker-b"),
            ]),
            "duplicate child sub=1",
        )
        .await;

        let mut nested_parent = valid_split_activation_request();
        nested_parent.parent = Some(cell_to_assignment(&CellId {
            world: 0,
            cx: 0,
            cy: 0,
            depth: 1,
            sub: 0,
        }));
        assert_split_rejected_without_assignment_change(&service, nested_parent, "depth=0/sub=0")
            .await;

        let mut nested_legacy_parent = valid_split_activation_request();
        nested_legacy_parent.parent = Some(cell_to_assignment(&CellId {
            world: 0,
            cx: 0,
            cy: 0,
            depth: 1,
            sub: 2,
        }));
        assert_split_rejected_without_assignment_change(
            &service,
            nested_legacy_parent,
            "depth=0/sub=0 parent",
        )
        .await;

        let parent = CellId::grid(0, 0, 0);
        let legacy_children = parent.legacy_shallow_children().expect("legacy children");
        assert_split_rejected_without_assignment_change(
            &service,
            split_activation_cell_request(
                parent,
                &[
                    (legacy_children[2], "worker-a"),
                    (legacy_children[0], "worker-a"),
                    (legacy_children[3], "worker-a"),
                    (legacy_children[1], "worker-a"),
                ],
            ),
            "dry-run/no-op",
        )
        .await;

        let canonical_children = parent.canonical_children().expect("canonical children");
        let mixed_family = split_activation_cell_request(
            parent,
            &[
                (legacy_children[0], "worker-b"),
                (legacy_children[1], "worker-b"),
                (canonical_children[2], "worker-b"),
                (canonical_children[3], "worker-b"),
            ],
        );
        assert_split_rejected_without_assignment_change(
            &service,
            mixed_family,
            "target child cells must exactly match",
        )
        .await;

        let mut mixed_target_mode = split_activation_cell_request(
            parent,
            &[
                (legacy_children[0], "worker-b"),
                (legacy_children[1], "worker-b"),
                (legacy_children[2], "worker-b"),
                (legacy_children[3], "worker-b"),
            ],
        );
        mixed_target_mode.targets[3].cell = None;
        mixed_target_mode.targets[3].sub = 3;
        assert_split_rejected_without_assignment_change(
            &service,
            mixed_target_mode,
            "must not mix explicit child cells",
        )
        .await;

        assert_split_rejected_without_assignment_change(
            &service,
            split_activation_request(&[
                (0, "worker-a"),
                (1, "worker-a"),
                (2, "worker-a"),
                (3, "worker-a"),
            ]),
            "dry-run/no-op",
        )
        .await;
    }

    #[tokio::test]
    async fn split_activation_publishes_canonical_child_assignments_after_replay() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let service = OrchestratorService::new_with_split_activation(
            two_worker_canonical_split_config(parent),
            SplitActivationMode::Manual,
        );
        let children = parent.canonical_children().expect("canonical children");
        let source_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind source relay");
        let source_addr = source_listener.local_addr().expect("source addr");
        let target_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target relay");
        let target_addr = target_listener.local_addr().expect("target addr");
        let source_server = spawn_split_replay_ack_server(source_listener, 1);
        let target_server = spawn_split_replay_ack_server(target_listener, 1);
        register_worker_for_test(&service, "worker-a", &source_addr.to_string()).await;
        register_worker_for_test(&service, "worker-b", &target_addr.to_string()).await;

        let response = service
            .submit_split_activation(Request::new(split_activation_cell_request(
                parent,
                &[
                    (children[3], "worker-b"),
                    (children[1], "worker-b"),
                    (children[0], "worker-b"),
                    (children[2], "worker-b"),
                ],
            )))
            .await
            .expect("submit canonical split activation")
            .into_inner();

        assert!(response.accepted, "{}", response.reason);
        assert_eq!(response.state, SplitActivationState::Published as i32);
        assert_eq!(response.source_worker_id, "worker-a");
        assert!(response.assignments_changed);
        assert_eq!(response.staged_children.len(), 4);
        for (expected, staged) in children.iter().zip(&response.staged_children) {
            assert_eq!(staged.cell, Some(cell_to_assignment(expected)));
            assert_eq!(staged.target_worker_id, "worker-b");
        }
        source_server.await.expect("source server");
        target_server.await.expect("target server");

        let listing = service.listing().await;
        let source = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("source listing");
        assert!(source.cells.is_empty());
        let target = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-b")
            .expect("target listing");
        assert_eq!(
            target.cells,
            children.iter().map(cell_to_assignment).collect::<Vec<_>>()
        )
    }

    #[tokio::test]
    async fn persistent_assignment_state_recovers_published_canonical_split_after_restart() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let state_path = unique_assignment_state_path("canonical-split-restart");
        let service = OrchestratorService::try_new_with_split_activation(
            two_worker_canonical_split_config(parent),
            SplitActivationMode::Manual,
            Some(state_path.clone()),
        )
        .expect("construct service with assignment state");
        let source_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind source relay");
        let source_addr = source_listener.local_addr().expect("source addr");
        let target_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target relay");
        let target_addr = target_listener.local_addr().expect("target addr");
        let source_server = spawn_split_replay_ack_server(source_listener, 1);
        let target_server = spawn_split_replay_ack_server(target_listener, 1);
        register_worker_for_test(&service, "worker-a", &source_addr.to_string()).await;
        register_worker_for_test(&service, "worker-b", &target_addr.to_string()).await;

        let response = service
            .submit_split_activation(Request::new(split_activation_cell_request(
                parent,
                &[
                    (children[0], "worker-b"),
                    (children[1], "worker-b"),
                    (children[2], "worker-b"),
                    (children[3], "worker-b"),
                ],
            )))
            .await
            .expect("submit canonical split activation")
            .into_inner();

        assert!(response.accepted, "{}", response.reason);
        assert_eq!(response.state, SplitActivationState::Published as i32);
        assert!(response.assignments_changed);
        source_server.await.expect("source server");
        target_server.await.expect("target server");

        let restarted = OrchestratorService::try_new_with_split_activation(
            two_worker_canonical_split_config(parent),
            SplitActivationMode::Disabled,
            Some(state_path.clone()),
        )
        .expect("restart service from assignment state");
        let listing = restarted.listing().await;
        let source = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("source listing");
        assert!(source.cells.is_empty());
        let target = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-b")
            .expect("target listing");
        assert_eq!(
            target.cells,
            children.iter().map(cell_to_assignment).collect::<Vec<_>>()
        );
        let _ = fs::remove_file(state_path);
    }

    #[tokio::test]
    async fn split_activation_requires_registered_targets_without_assignment_mutation() {
        let service = OrchestratorService::new_with_split_activation(
            two_worker_handover_config(),
            SplitActivationMode::Manual,
        );
        register_worker_for_test(&service, "worker-a", "10.0.0.1:5001").await;

        assert_split_rejected_without_assignment_change(
            &service,
            valid_split_activation_request(),
            "not registered",
        )
        .await;

        register_worker_for_test(&service, "worker-b", "10.0.0.2:5001").await;
        assert_split_rejected_without_assignment_change(
            &service,
            split_activation_request(&[
                (0, "worker-b"),
                (1, "worker-b"),
                (2, "worker-b"),
                (3, "worker-unknown"),
            ]),
            "unknown target_worker_id=worker-unknown",
        )
        .await;
    }

    #[tokio::test]
    async fn split_activation_rolls_back_when_source_replay_fails() {
        let service = OrchestratorService::new_with_split_activation(
            two_worker_handover_config(),
            SplitActivationMode::Manual,
        );
        let source_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind source relay");
        let source_addr = source_listener.local_addr().expect("source addr");
        let target_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target relay");
        let target_addr = target_listener.local_addr().expect("target addr");
        let source_server = spawn_split_replay_reject_source_server(source_listener);
        let target_server = spawn_split_replay_ack_server(target_listener, 2);
        register_worker_for_test(&service, "worker-a", &source_addr.to_string()).await;
        register_worker_for_test(&service, "worker-b", &target_addr.to_string()).await;
        let before = service.listing().await;

        let failed = service
            .submit_split_activation(Request::new(valid_split_activation_request()))
            .await
            .expect("submit split activation")
            .into_inner();
        assert!(!failed.accepted);
        assert_eq!(failed.state, SplitActivationState::Failed as i32);
        assert!(failed.reason.contains("source replay failed"));
        assert!(!failed.assignments_changed);

        let after = service.listing().await;
        assert_eq!(before, after);
        assert!(service.staged_splits.read().await.is_empty());
        assert!(matches!(
            source_server.await.expect("source server"),
            WorkerRelayMsg::SplitReplayRequest { .. }
        ));
        let target_received = target_server.await.expect("target server");
        assert!(matches!(
            target_received.as_slice(),
            [
                WorkerRelayMsg::SplitReplayPrepare { .. },
                WorkerRelayMsg::SplitReplayAbort { .. }
            ]
        ));
    }

    #[tokio::test]
    async fn merge_activation_publishes_parent_for_same_owner_siblings() {
        let service = OrchestratorService::new_with_split_activation(
            two_worker_merge_config(),
            SplitActivationMode::Manual,
        );
        register_worker_for_test(&service, "worker-a", "10.0.0.1:5001").await;

        let response = service
            .submit_merge_activation(Request::new(valid_merge_activation_request()))
            .await
            .expect("submit merge activation")
            .into_inner();

        assert!(response.accepted);
        assert_eq!(response.state, MergeActivationState::Published as i32);
        assert_eq!(response.owner_worker_id, "worker-a");
        assert!(response.assignments_changed);
        assert_eq!(response.merged_children.len(), 4);

        let listing = service.listing().await;
        let owner = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("owner listing");
        assert_eq!(
            owner.cells,
            vec![cell_to_assignment(&CellId::grid(0, 0, 0))]
        );
    }

    #[tokio::test]
    async fn merge_activation_publishes_canonical_parent_for_same_owner_siblings() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let service = OrchestratorService::new_with_split_activation(
            two_worker_canonical_merge_config(parent),
            SplitActivationMode::Manual,
        );
        register_worker_for_test(&service, "worker-a", "10.0.0.1:5001").await;

        let response = service
            .submit_merge_activation(Request::new(MergeActivationRequest {
                operation_id: "canonical-merge-1".to_string(),
                parent: Some(cell_to_assignment(&parent)),
                owner_worker_id: "worker-a".to_string(),
            }))
            .await
            .expect("submit canonical merge activation")
            .into_inner();

        assert!(response.accepted);
        assert_eq!(response.state, MergeActivationState::Published as i32);
        assert_eq!(response.owner_worker_id, "worker-a");
        assert!(response.assignments_changed);
        assert_eq!(response.merged_children.len(), 4);

        let listing = service.listing().await;
        let owner = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("owner listing");
        assert_eq!(owner.cells, vec![cell_to_assignment(&parent)]);
    }

    #[tokio::test]
    async fn merge_activation_replays_cross_worker_siblings_before_publish() {
        let mut config = two_worker_merge_config();
        let remote_child = split_child_cell(CellId::grid(0, 0, 0), 3);
        config
            .workers
            .get_mut("worker-a")
            .expect("worker-a")
            .cells
            .retain(|cell| cell != &remote_child);
        config
            .workers
            .get_mut("worker-b")
            .expect("worker-b")
            .cells
            .push(remote_child);
        let service =
            OrchestratorService::new_with_split_activation(config, SplitActivationMode::Manual);
        let target_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind merge target relay");
        let target_addr = target_listener.local_addr().expect("target addr");
        let source_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind merge source relay");
        let source_addr = source_listener.local_addr().expect("source addr");
        let target_server = spawn_split_replay_ack_server(target_listener, 1);
        let source_server = spawn_split_replay_ack_server(source_listener, 1);
        register_worker_for_test(&service, "worker-a", &target_addr.to_string()).await;
        register_worker_for_test(&service, "worker-b", &source_addr.to_string()).await;

        let response = service
            .submit_merge_activation(Request::new(valid_merge_activation_request()))
            .await
            .expect("submit cross-worker merge activation")
            .into_inner();

        assert!(response.accepted, "{}", response.reason);
        assert_eq!(response.state, MergeActivationState::Published as i32);
        assert_eq!(response.owner_worker_id, "worker-a");
        assert!(response.assignments_changed);
        assert_eq!(response.merged_children.len(), 4);

        let target_received = target_server.await.expect("target server");
        assert!(matches!(
            target_received.as_slice(),
            [WorkerRelayMsg::MergeReplayPrepare { .. }]
        ));
        let source_received = source_server.await.expect("source server");
        assert!(matches!(
            source_received.as_slice(),
            [WorkerRelayMsg::MergeReplayRequest { target, .. }]
                if target.parent == CellId::grid(0, 0, 0)
                    && target.source_cell == remote_child
                    && target.target_worker_id == "worker-a"
                    && target.target_addr == target_addr.to_string()
        ));

        let listing = service.listing().await;
        let owner = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("owner listing");
        assert_eq!(
            owner.cells,
            vec![cell_to_assignment(&CellId::grid(0, 0, 0))]
        );
        let remote = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-b")
            .expect("remote listing");
        assert!(remote.cells.is_empty());
    }

    #[tokio::test]
    async fn persistent_assignment_state_recovers_published_merge_after_restart() {
        let state_path = unique_assignment_state_path("merge-restart");
        let service = OrchestratorService::try_new_with_split_activation(
            two_worker_merge_config(),
            SplitActivationMode::Manual,
            Some(state_path.clone()),
        )
        .expect("construct merge service with assignment state");
        register_worker_for_test(&service, "worker-a", "10.0.0.1:5001").await;

        let response = service
            .submit_merge_activation(Request::new(valid_merge_activation_request()))
            .await
            .expect("submit merge activation")
            .into_inner();

        assert!(response.accepted);
        assert_eq!(response.state, MergeActivationState::Published as i32);
        assert!(response.assignments_changed);
        let raw_state = fs::read_to_string(&state_path).expect("read persisted assignment state");
        assert!(raw_state.contains(ASSIGNMENT_STATE_SCHEMA));

        let restarted = OrchestratorService::try_new_with_split_activation(
            two_worker_merge_config(),
            SplitActivationMode::Disabled,
            Some(state_path.clone()),
        )
        .expect("restart service from merge assignment state");
        let listing = restarted.listing().await;
        let owner = listing
            .workers
            .iter()
            .find(|worker| worker.worker_id == "worker-a")
            .expect("owner listing");
        assert_eq!(
            owner.cells,
            vec![cell_to_assignment(&CellId::grid(0, 0, 0))]
        );

        let snapshot = restarted
            .register_worker(Request::new(WorkerRegistration {
                worker_id: "worker-a".into(),
                addr: "10.0.0.1:5001".into(),
            }))
            .await
            .expect("register restarted merge owner")
            .into_inner();
        assert_eq!(
            snapshot.cells,
            vec![cell_to_assignment(&CellId::grid(0, 0, 0))]
        );
        let _ = fs::remove_file(state_path);
    }

    #[tokio::test]
    async fn merge_activation_rejects_nested_legacy_parent_without_assignment_mutation() {
        let service = OrchestratorService::new_with_split_activation(
            two_worker_merge_config(),
            SplitActivationMode::Manual,
        );
        let mut request = valid_merge_activation_request();
        request.parent = Some(cell_to_assignment(&CellId {
            world: 0,
            cx: 0,
            cy: 0,
            depth: 1,
            sub: 2,
        }));

        assert_merge_rejected_without_assignment_change(
            &service,
            request,
            "legacy shallow or canonical child families",
        )
        .await;
    }

    #[tokio::test]
    async fn merge_activation_validates_policy_and_complete_same_owner_family() {
        let disabled = OrchestratorService::new_with_split_activation(
            two_worker_merge_config(),
            SplitActivationMode::Disabled,
        );
        register_worker_for_test(&disabled, "worker-a", "10.0.0.1:5001").await;
        let response = disabled
            .submit_merge_activation(Request::new(valid_merge_activation_request()))
            .await
            .expect("submit disabled merge")
            .into_inner();
        assert!(!response.accepted);
        assert_eq!(response.state, MergeActivationState::Disabled as i32);
        assert!(!response.assignments_changed);

        let mut split_owner_config = two_worker_merge_config();
        split_owner_config
            .workers
            .get_mut("worker-a")
            .expect("worker-a")
            .cells
            .retain(|cell| cell.sub != 3);
        split_owner_config
            .workers
            .get_mut("worker-b")
            .expect("worker-b")
            .cells
            .push(split_child_cell(CellId::grid(0, 0, 0), 3));
        let service = OrchestratorService::new_with_split_activation(
            split_owner_config,
            SplitActivationMode::Manual,
        );
        register_worker_for_test(&service, "worker-a", "10.0.0.1:5001").await;
        let before = service.listing().await;

        let rejected = service
            .submit_merge_activation(Request::new(valid_merge_activation_request()))
            .await
            .expect("submit merge with missing source")
            .into_inner();
        assert!(!rejected.accepted);
        assert_eq!(rejected.state, MergeActivationState::Rejected as i32);
        assert!(
            rejected
                .reason
                .contains("source_worker_id=worker-b is not registered")
        );
        assert!(!rejected.assignments_changed);
        assert_eq!(service.listing().await, before);
    }

    #[test]
    fn load_assignment_state_rejects_unknown_worker() {
        let state_path = unique_assignment_state_path("unknown-worker");
        fs::write(
            &state_path,
            format!(
                r#"{{
                    "schema": "{ASSIGNMENT_STATE_SCHEMA}",
                    "workers": [
                        {{
                            "worker_id": "worker-unknown",
                            "cells": [{{"world": 0, "cx": 0, "cy": 0}}]
                        }}
                    ]
                }}"#
            ),
        )
        .expect("write test assignment state");

        let err = load_assignment_state_file(&two_worker_handover_config(), &state_path)
            .expect_err("unknown worker should fail");
        assert!(err.to_string().contains("unknown worker_id"));
        let _ = fs::remove_file(state_path);
    }

    #[test]
    fn operation_ledger_round_trips_proposal_records() {
        let ledger_path = unique_assignment_state_path("operation-ledger");
        let record = sample_operation_record("op-1");
        let ledger = OperationLedgerFile {
            schema: OPERATION_LEDGER_SCHEMA.to_string(),
            records: vec![record.clone()],
        };

        write_operation_ledger_file(&ledger_path, &ledger).expect("write operation ledger");
        let loaded = load_operation_ledger_file(&ledger_path).expect("load operation ledger");

        assert_eq!(loaded.schema, OPERATION_LEDGER_SCHEMA);
        assert_eq!(loaded.records, vec![record]);
        let raw = fs::read_to_string(&ledger_path).expect("read ledger json");
        assert!(raw.contains(OPERATION_LEDGER_SCHEMA));
        let _ = fs::remove_file(ledger_path);
    }

    #[test]
    fn operation_ledger_rejects_duplicate_operation_id() {
        let ledger = OperationLedgerFile {
            schema: OPERATION_LEDGER_SCHEMA.to_string(),
            records: vec![
                sample_operation_record("duplicate-op"),
                sample_operation_record("duplicate-op"),
            ],
        };

        let err = validate_operation_ledger(&ledger).expect_err("duplicate id should fail");
        assert!(err.to_string().contains("duplicate operation_id"));
    }

    #[test]
    fn operation_ledger_rejects_mismatched_approval_hash() {
        let mut record = sample_operation_record("approved-op");
        record.status = OperationStatus::Approved;
        record.approval = Some(OperationApproval {
            policy_id: "operator_approved_dynamic_operation_v1".to_string(),
            approver: "operator".to_string(),
            allowed_kind: DynamicOperationKind::Split,
            approved_unix_secs: 100,
            expires_unix_secs: 200,
            expected_proposal_hash: "other-hash".to_string(),
            cooldown_key: "world-0".to_string(),
            budget_key: "dynamic-ops-daily".to_string(),
        });
        let ledger = OperationLedgerFile {
            schema: OPERATION_LEDGER_SCHEMA.to_string(),
            records: vec![record],
        };

        let err = validate_operation_ledger(&ledger).expect_err("mismatched hash should fail");
        assert!(err.to_string().contains("expected_proposal_hash"));
    }

    #[test]
    fn operation_ledger_rejects_mismatched_approval_kind() {
        let mut record = sample_operation_record("wrong-kind-op");
        record.status = OperationStatus::Approved;
        record.approval = Some(OperationApproval {
            policy_id: "operator_approved_dynamic_operation_v1".to_string(),
            approver: "operator".to_string(),
            allowed_kind: DynamicOperationKind::Merge,
            approved_unix_secs: 100,
            expires_unix_secs: 200,
            expected_proposal_hash: "proposal-hash-1".to_string(),
            cooldown_key: "world-0".to_string(),
            budget_key: "dynamic-ops-daily".to_string(),
        });
        let ledger = OperationLedgerFile {
            schema: OPERATION_LEDGER_SCHEMA.to_string(),
            records: vec![record],
        };

        let err = validate_operation_ledger(&ledger).expect_err("mismatched kind should fail");
        assert!(err.to_string().contains("approval.allowed_kind"));
    }

    #[tokio::test]
    async fn operation_ledger_path_initializes_empty_read_only_snapshot() {
        let ledger_path = unique_assignment_state_path("operation-ledger-init");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");

        let snapshot = service.operation_ledger_snapshot().await;
        assert_eq!(snapshot.schema, OPERATION_LEDGER_SCHEMA);
        assert!(snapshot.persistence_enabled);
        assert!(snapshot.records.is_empty());
        let raw = fs::read_to_string(&ledger_path).expect("read initialized ledger");
        assert!(raw.contains(OPERATION_LEDGER_SCHEMA));
        let _ = fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn operation_ledger_records_split_planner_proposal_without_assignment_mutation() {
        let ledger_path = unique_assignment_state_path("operation-ledger-split-proposal");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");
        register_worker_for_test(&service, "worker-a", "127.0.0.1:5101").await;
        register_worker_for_test(&service, "worker-b", "127.0.0.1:5102").await;
        let before = service.listing().await;
        let mut candidate = split_metrics(CellId::grid(0, 0, 0));
        candidate.actor_count = 140;
        candidate.move_queue_pressure = 70;
        let snapshot = SplitMergeMetricsSnapshot {
            cells: vec![candidate],
            active_plans: Vec::new(),
        };

        let response = service
            .record_split_merge_proposals_from_snapshot(
                "live_worker_metrics:test".to_string(),
                snapshot.clone(),
                UNIX_EPOCH + std::time::Duration::from_secs(200),
            )
            .await
            .expect("record proposal");
        let after = service.listing().await;

        assert!(!response.assignments_changed);
        assert_eq!(response.planned_count, 1);
        assert_eq!(response.recorded_count, 1);
        assert_eq!(response.already_recorded_count, 0);
        assert_eq!(response.skipped_count, 0);
        assert_eq!(before, after);
        let loaded = load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        assert_eq!(loaded.records.len(), 1);
        let record = &loaded.records[0];
        assert_eq!(record.kind, DynamicOperationKind::Split);
        assert_eq!(record.status, OperationStatus::Proposed);
        assert_eq!(record.created_unix_secs, 200);
        assert_eq!(record.proposal.source, "live_worker_metrics:test");
        assert_eq!(record.proposal.parent, Some(CellId::grid(0, 0, 0)));
        assert_eq!(record.proposal.targets.len(), 4);
        assert!(
            record
                .proposal
                .targets
                .iter()
                .any(|target| target.worker_id == "worker-b")
        );
        assert!(
            record
                .proposal
                .submission_command
                .contains(&record.operation_id)
        );
        assert!(
            record
                .proposal
                .submission_command
                .contains("cargo xt split-activation")
        );

        let repeat = service
            .record_split_merge_proposals_from_snapshot(
                "live_worker_metrics:test".to_string(),
                snapshot,
                UNIX_EPOCH + std::time::Duration::from_secs(201),
            )
            .await
            .expect("repeat proposal");
        let loaded_after_repeat =
            load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        assert_eq!(repeat.recorded_count, 0);
        assert_eq!(repeat.already_recorded_count, 1);
        assert_eq!(loaded_after_repeat.records.len(), 1);
        let _ = fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn operation_ledger_records_merge_planner_proposal() {
        let ledger_path = unique_assignment_state_path("operation-ledger-merge-proposal");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_merge_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");
        register_worker_for_test(&service, "worker-a", "127.0.0.1:5101").await;
        let snapshot = SplitMergeMetricsSnapshot {
            cells: vec![
                merge_metrics(0, 0, 0, 0, "worker-a"),
                merge_metrics(0, 0, 0, 1, "worker-a"),
                merge_metrics(0, 0, 0, 2, "worker-a"),
                merge_metrics(0, 0, 0, 3, "worker-a"),
            ],
            active_plans: Vec::new(),
        };

        let response = service
            .record_split_merge_proposals_from_snapshot(
                "assignment_listing_zero_metrics".to_string(),
                snapshot,
                UNIX_EPOCH + std::time::Duration::from_secs(300),
            )
            .await
            .expect("record merge proposal");

        assert_eq!(response.planned_count, 1);
        assert_eq!(response.recorded_count, 1);
        assert_eq!(response.skipped_count, 0);
        let loaded = load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        assert_eq!(loaded.records.len(), 1);
        let record = &loaded.records[0];
        assert_eq!(record.kind, DynamicOperationKind::Merge);
        assert_eq!(record.proposal.parent, Some(CellId::grid(0, 0, 0)));
        assert_eq!(record.proposal.targets.len(), 4);
        assert!(
            record
                .proposal
                .targets
                .iter()
                .all(|target| target.worker_id == "worker-a")
        );
        assert!(
            record
                .proposal
                .submission_command
                .contains("cargo xt merge-activation")
        );
        let _ = fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn operation_ledger_approves_existing_proposal_without_assignment_mutation() {
        let ledger_path = unique_assignment_state_path("operation-ledger-approval");
        let record = sample_operation_record("approve-op");
        let proposal_hash = record.proposal.proposal_hash.clone();
        write_operation_ledger_file(
            &ledger_path,
            &OperationLedgerFile {
                schema: OPERATION_LEDGER_SCHEMA.to_string(),
                records: vec![record],
            },
        )
        .expect("write operation ledger");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");
        let before = service.listing().await;
        let request = OperationApprovalRequest {
            operation_id: "approve-op".to_string(),
            policy_id: "operator_approved_dynamic_operation_v1".to_string(),
            approver: "operator".to_string(),
            expected_proposal_hash: proposal_hash.clone(),
            ttl_secs: 600,
            cooldown_key: "world-0".to_string(),
            budget_key: "dynamic-ops-daily".to_string(),
        };

        let response = service
            .approve_operation(
                request.clone(),
                UNIX_EPOCH + std::time::Duration::from_secs(400),
            )
            .await
            .expect("approve operation");
        let after = service.listing().await;

        assert_eq!(response.status, "approved");
        assert!(!response.assignments_changed);
        assert_eq!(response.expires_unix_secs, 1000);
        assert_eq!(before, after);
        let loaded = load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        let approved = &loaded.records[0];
        assert_eq!(approved.status, OperationStatus::Approved);
        let approval = approved.approval.as_ref().expect("approval record");
        assert_eq!(approval.allowed_kind, DynamicOperationKind::Split);
        assert_eq!(approval.expected_proposal_hash, proposal_hash);
        assert!(
            approved
                .phases
                .iter()
                .any(|phase| phase.name == "approval_recorded")
        );

        let repeat = service
            .approve_operation(request, UNIX_EPOCH + std::time::Duration::from_secs(401))
            .await
            .expect("repeat approval");
        assert_eq!(repeat.status, "already_approved");
        let loaded_after_repeat =
            load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        assert_eq!(
            loaded_after_repeat.records[0].phases.len(),
            approved.phases.len()
        );
        let _ = fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn operation_ledger_approval_rejects_stale_proposal_hash() {
        let ledger_path = unique_assignment_state_path("operation-ledger-stale-approval");
        write_operation_ledger_file(
            &ledger_path,
            &OperationLedgerFile {
                schema: OPERATION_LEDGER_SCHEMA.to_string(),
                records: vec![sample_operation_record("stale-approval-op")],
            },
        )
        .expect("write operation ledger");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");

        let err = service
            .approve_operation(
                OperationApprovalRequest {
                    operation_id: "stale-approval-op".to_string(),
                    policy_id: "operator_approved_dynamic_operation_v1".to_string(),
                    approver: "operator".to_string(),
                    expected_proposal_hash: "stale-hash".to_string(),
                    ttl_secs: 600,
                    cooldown_key: "world-0".to_string(),
                    budget_key: "dynamic-ops-daily".to_string(),
                },
                UNIX_EPOCH + std::time::Duration::from_secs(400),
            )
            .await
            .expect_err("stale hash should fail");

        assert!(err.contains("expected_proposal_hash"));
        let loaded = load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        assert_eq!(loaded.records[0].status, OperationStatus::Proposed);
        assert!(loaded.records[0].approval.is_none());
        let _ = fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn operation_ledger_proposal_recording_requires_persistence_path() {
        let service = OrchestratorService::new(two_worker_handover_config());
        let err = service
            .record_split_merge_proposals_from_snapshot(
                "test".to_string(),
                SplitMergeMetricsSnapshot::default(),
                UNIX_EPOCH,
            )
            .await
            .expect_err("persistence disabled should reject writes");

        assert!(err.contains("TESSERA_ORCH_OPERATION_LEDGER_PATH"));
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

    #[tokio::test]
    async fn split_merge_preview_http_serves_dry_run_json() {
        let service = OrchestratorService::new(two_worker_handover_config());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind preview listener");
        let addr = listener.local_addr().expect("preview listener addr");
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept preview request");
            handle_prometheus_metrics_request(stream, server_service)
                .await
                .expect("handle preview request");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect preview");
        client
            .write_all(b"GET /split-merge/preview HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write preview request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read preview response");
        server.await.expect("preview server task");

        let response = String::from_utf8(response).expect("utf8 preview response");
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: application/json\r\n"));
        assert!(response.contains(r#""mode": "dry_run""#));
        assert!(response.contains(r#""assignments_changed": false"#));
        assert!(response.contains(r#""plans": []"#));
    }

    #[tokio::test]
    async fn operation_ledger_http_serves_read_only_snapshot() {
        let ledger_path = unique_assignment_state_path("operation-ledger-http");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind operations listener");
        let addr = listener.local_addr().expect("operations listener addr");
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept operations request");
            handle_prometheus_metrics_request(stream, server_service)
                .await
                .expect("handle operations request");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect operations");
        client
            .write_all(b"GET /operations HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write operations request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read operations response");
        server.await.expect("operations server task");

        let response = String::from_utf8(response).expect("utf8 operations response");
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: application/json\r\n"));
        assert!(response.contains(r#""schema": "tessera.orch.operation_ledger.v1""#));
        assert!(response.contains(r#""persistence_enabled": true"#));
        assert!(response.contains(r#""records": []"#));
        let _ = fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn operation_ledger_http_records_empty_proposal_batch() {
        let ledger_path = unique_assignment_state_path("operation-ledger-http-proposals");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind proposal listener");
        let addr = listener.local_addr().expect("proposal listener addr");
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept proposal request");
            handle_prometheus_metrics_request(stream, server_service)
                .await
                .expect("handle proposal request");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect proposals");
        client
            .write_all(b"POST /operations/proposals HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write proposal request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read proposal response");
        server.await.expect("proposal server task");

        let response = String::from_utf8(response).expect("utf8 proposal response");
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains(r#""mode": "planner_proposal_recording""#));
        assert!(response.contains(r#""assignments_changed": false"#));
        assert!(response.contains(r#""planned_count": 0"#));
        assert!(response.contains(r#""recorded_count": 0"#));
        let loaded = load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        assert!(loaded.records.is_empty());
        let _ = fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn operation_ledger_http_records_approval() {
        let ledger_path = unique_assignment_state_path("operation-ledger-http-approval");
        write_operation_ledger_file(
            &ledger_path,
            &OperationLedgerFile {
                schema: OPERATION_LEDGER_SCHEMA.to_string(),
                records: vec![sample_operation_record("http-approve-op")],
            },
        )
        .expect("write operation ledger");
        let service = OrchestratorService::try_new_with_persistence(
            two_worker_handover_config(),
            SplitActivationMode::Disabled,
            None,
            Some(ledger_path.clone()),
        )
        .expect("construct service with operation ledger");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind approval listener");
        let addr = listener.local_addr().expect("approval listener addr");
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept approval request");
            handle_prometheus_metrics_request(stream, server_service)
                .await
                .expect("handle approval request");
        });

        let request = b"POST /operations/approvals?operation_id=http-approve-op&policy_id=operator_approved_dynamic_operation_v1&approver=operator&expected_proposal_hash=proposal-hash-1&ttl_secs=600&cooldown_key=world-0&budget_key=dynamic-ops-daily HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let mut client = TcpStream::connect(addr).await.expect("connect approvals");
        client
            .write_all(request)
            .await
            .expect("write approval request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read approval response");
        server.await.expect("approval server task");

        let response = String::from_utf8(response).expect("utf8 approval response");
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains(r#""status": "approved""#));
        assert!(response.contains(r#""assignments_changed": false"#));
        let loaded = load_operation_ledger_file(&ledger_path).expect("load operation ledger");
        assert_eq!(loaded.records[0].status, OperationStatus::Approved);
        assert!(loaded.records[0].approval.is_some());
        let _ = fs::remove_file(ledger_path);
    }
}
