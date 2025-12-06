use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use tessera_core::CellId;
use tessera_proto::orch::v1::orchestrator_server::{Orchestrator, OrchestratorServer};
use tessera_proto::orch::v1::{
    Assignment, AssignmentBundle, AssignmentListing, AssignmentQuery, AssignmentSnapshot,
    ListAssignmentsRequest, WatchAssignmentsRequest, WorkerRegistration,
};
use tokio::sync::{RwLock, watch};
use tokio_stream::Stream;
use tokio_stream::{StreamExt, wrappers::WatchStream};
use tonic::{Request, Response, Status, async_trait, transport::Server};
use tracing::{error, info, warn};

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
    info!(target: "orch", %listen_addr, "tessera-orch listening");

    if let Err(e) = Server::builder()
        .add_service(OrchestratorServer::new(service))
        .serve(listen_addr)
        .await
    {
        error!(target: "orch", error = ?e, "server exited with error");
        return Err(e.into());
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    workers: HashMap<String, WorkerStatic>,
}

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

    fn assignments_for(&self, worker_id: &str) -> Option<&[CellId]> {
        self.worker(worker_id).map(|w| w.cells.as_slice())
    }
}

fn listing_with_runtime(
    config: &Config,
    runtime: &HashMap<String, WorkerRuntime>,
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
        let cells = worker
            .cells
            .iter()
            .map(cell_to_assignment)
            .collect::<Vec<_>>();
        bundles.push(AssignmentBundle {
            worker_id: worker_id.clone(),
            addr: addr.to_string(),
            cells,
        });
    }
    AssignmentListing { workers: bundles }
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

#[derive(Debug)]
struct WorkerRuntime {
    addr: String,
    last_seen: SystemTime,
}

#[derive(Clone)]
struct OrchestratorService {
    config: Arc<Config>,
    runtime: Arc<RwLock<HashMap<String, WorkerRuntime>>>,
    listing_tx: watch::Sender<AssignmentListing>,
}

impl OrchestratorService {
    fn new(config: Config) -> Self {
        let config = Arc::new(config);
        let initial_listing = listing_with_runtime(&config, &HashMap::new());
        let (listing_tx, _) = watch::channel(initial_listing);
        Self {
            config,
            runtime: Arc::new(RwLock::new(HashMap::new())),
            listing_tx,
        }
    }

    fn snapshot_for(&self, worker_id: &str) -> AssignmentSnapshot {
        let cells = self.config.assignments_for(worker_id);
        let mut entries = Vec::new();
        if let Some(cells) = cells {
            entries.reserve(cells.len());
            entries.extend(cells.iter().map(cell_to_assignment));
        }
        AssignmentSnapshot { cells: entries }
    }

    async fn listing(&self) -> AssignmentListing {
        let runtime = self.runtime.read().await;
        listing_with_runtime(&self.config, &runtime)
    }

    async fn publish_listing_if_changed(&self) {
        let listing = self.listing().await;
        if *self.listing_tx.borrow() == listing {
            return;
        }
        if let Err(e) = self.listing_tx.send(listing) {
            warn!(target: "orch", error = ?e, "failed to publish assignment listing");
        }
    }

    #[cfg(test)]
    fn push_listing_for_test(&self, listing: AssignmentListing) {
        let _ = self.listing_tx.send(listing);
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
        let snapshot = self.snapshot_for(worker_id);

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

        let cell_count = snapshot.cells.len();
        match planned {
            Some(expected) => {
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
            }
            None => {
                warn!(
                    target: "orch",
                    worker_id,
                    addr,
                    "worker registered but not defined in config"
                );
            }
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

        let snapshot = self.snapshot_for(worker_id);
        Ok(Response::new(snapshot))
    }

    async fn list_assignments(
        &self,
        _request: Request<ListAssignmentsRequest>,
    ) -> Result<Response<AssignmentListing>, Status> {
        let listing = self.listing().await;
        Ok(Response::new(listing))
    }

    async fn watch_assignments(
        &self,
        _request: Request<WatchAssignmentsRequest>,
    ) -> Result<Response<Self::WatchAssignmentsStream>, Status> {
        let rx = self.listing_tx.subscribe();
        let stream = WatchStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(stream)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;
    use tonic::Request;

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
        let cells = cfg.assignments_for("worker-a").expect("cells");
        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0].world, 1);
        assert_eq!(cells[0].cx, 2);
        assert_eq!(cells[0].cy, -3);
        assert_eq!(cells[1].depth, 1);
        assert_eq!(cells[1].sub, 2);
        assert!(cfg.assignments_for("missing").is_none());
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
}
