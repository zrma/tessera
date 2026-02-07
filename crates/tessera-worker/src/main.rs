use anyhow::{Context, Result, anyhow};
use bytes::BytesMut;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
};
use std::time::Duration;
use tessera_core::{
    ActorState, CellId, ClientMsg, EntityId, Envelope, MAX_FRAME_LEN, Position, ServerMsg, Tick,
    encode_frame, try_decode_frame,
};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{Assignment, WorkerRegistration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{
    Mutex, RwLock,
    mpsc::{Receiver, Sender, channel},
};
use tokio::time;
use tracing::{error, info, warn};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
const OUTBOUND_CHANNEL_CAPACITY: usize = 256;
type OutboundMsg = (CellId, Option<u32>, ServerMsg);
type CellSubscribers = HashMap<CellId, Vec<(u64, Sender<OutboundMsg>)>>;
type CellOwners = HashMap<CellId, HashMap<EntityId, u64>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimOutcome {
    GrantedNew,
    GrantedExisting,
    Denied(u64),
}

#[derive(Default)]
struct SharedState {
    actors: Mutex<HashMap<CellId, HashMap<EntityId, Position>>>,
    owners: Mutex<CellOwners>,
    subscribers: Mutex<CellSubscribers>,
}

impl SharedState {
    async fn subscribe(&self, cell: CellId, client_id: u64, tx: Sender<OutboundMsg>) {
        let mut subs = self.subscribers.lock().await;
        let entry = subs.entry(cell).or_default();
        if entry.iter().any(|(id, _)| *id == client_id) {
            return;
        }
        entry.push((client_id, tx));
    }

    async fn snapshot_and_subscribe(
        &self,
        cell: CellId,
        actor: EntityId,
        pos: Position,
        client_id: u64,
        tx: &Sender<OutboundMsg>,
        epoch: u32,
    ) -> Result<bool, ()> {
        let mut actors = self.actors.lock().await;
        let mut subs = self.subscribers.lock().await;
        // Snapshot send may fail if the outbound channel is closed/full. Avoid mutating
        // the actor map until the snapshot is successfully enqueued.
        let snapshot = if let Some(cell_actors) = actors.get(&cell) {
            let mut entries = Vec::with_capacity(cell_actors.len() + 1);
            let mut found = false;
            for (id, existing) in cell_actors {
                if *id == actor {
                    entries.push(ActorState { id: *id, pos });
                    found = true;
                } else {
                    entries.push(ActorState {
                        id: *id,
                        pos: *existing,
                    });
                }
            }
            if !found {
                entries.push(ActorState { id: actor, pos });
            }
            entries
        } else {
            vec![ActorState { id: actor, pos }]
        };

        if tx
            .try_send((
                cell,
                Some(epoch),
                ServerMsg::Snapshot {
                    cell,
                    actors: snapshot,
                },
            ))
            .is_err()
        {
            return Err(());
        }

        let cell_actors = actors.entry(cell).or_default();
        let inserted_new = cell_actors.insert(actor, pos).is_none();

        let entry = subs.entry(cell).or_default();
        if !entry.iter().any(|(id, _)| *id == client_id) {
            entry.push((client_id, tx.clone()));
        }

        Ok(inserted_new)
    }

    async fn release_owner(&self, cell: CellId, actor: EntityId, client_id: u64) {
        let mut owners = self.owners.lock().await;
        let mut remove_cell = false;
        if let Some(cell_owners) = owners.get_mut(&cell) {
            if cell_owners.get(&actor).copied() == Some(client_id) {
                cell_owners.remove(&actor);
            }
            remove_cell = cell_owners.is_empty();
        }
        if remove_cell {
            owners.remove(&cell);
        }
    }

    async fn remove_client_subscriptions(&self, client_id: u64) {
        let mut subs = self.subscribers.lock().await;
        subs.values_mut().for_each(|entries| {
            entries.retain(|(id, _)| *id != client_id);
        });
        subs.retain(|_, entries| !entries.is_empty());
    }

    async fn broadcast(
        &self,
        cell: CellId,
        msg: ServerMsg,
        exclude: Option<u64>,
        epoch_override: Option<u32>,
    ) {
        let mut subs = self.subscribers.lock().await;
        if let Some(entries) = subs.get_mut(&cell) {
            entries.retain(|(id, tx)| {
                if exclude.is_some_and(|excluded| excluded == *id) {
                    return !tx.is_closed();
                }
                match tx.try_send((cell, epoch_override, msg.clone())) {
                    Ok(()) => true,
                    Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Closed(_)) => false,
                }
            });
            if entries.is_empty() {
                subs.remove(&cell);
            }
        }
    }

    async fn remove_owned_actors(
        &self,
        client_id: u64,
        owned: &HashSet<(CellId, EntityId)>,
        epoch_override: Option<u32>,
    ) {
        let mut removed_by_cell: HashMap<CellId, Vec<EntityId>> = HashMap::new();
        let mut actors = self.actors.lock().await;
        let mut owners = self.owners.lock().await;
        for (cell, entity) in owned {
            let Some(owner) = owners
                .get(cell)
                .and_then(|cell_owners| cell_owners.get(entity))
                .copied()
            else {
                continue;
            };
            if owner != client_id {
                continue;
            }

            let mut remove_cell = false;
            if let Some(cell_map) = actors.get_mut(cell) {
                if cell_map.remove(entity).is_some() {
                    removed_by_cell.entry(*cell).or_default().push(*entity);
                }
                remove_cell = cell_map.is_empty();
            }
            if remove_cell {
                actors.remove(cell);
            }

            let mut remove_owner_cell = false;
            if let Some(cell_owners) = owners.get_mut(cell) {
                cell_owners.remove(entity);
                remove_owner_cell = cell_owners.is_empty();
            }
            if remove_owner_cell {
                owners.remove(cell);
            }
        }
        drop(actors);

        // 소유권 맵을 잠근 상태에서 despawn을 전파해 재조인 경쟁으로 인한 순서 역전을 막는다.
        for (cell, actors) in removed_by_cell {
            if !actors.is_empty() {
                self.broadcast(
                    cell,
                    ServerMsg::Despawn { cell, actors },
                    None,
                    epoch_override,
                )
                .await;
            }
        }
        drop(owners);
    }

    async fn drop_cells(&self, cells: &HashSet<CellId>) {
        let mut removed_by_cell: HashMap<CellId, Vec<EntityId>> = HashMap::new();
        {
            let mut actors = self.actors.lock().await;
            for cell in cells {
                if let Some(existing) = actors.remove(cell) {
                    let removed = existing.into_keys().collect::<Vec<_>>();
                    if !removed.is_empty() {
                        removed_by_cell.insert(*cell, removed);
                    }
                }
            }
        }
        {
            let mut owners = self.owners.lock().await;
            for cell in cells {
                owners.remove(cell);
            }
        }
        for (cell, actors) in removed_by_cell {
            self.broadcast(cell, ServerMsg::Despawn { cell, actors }, None, None)
                .await;
        }
        let mut subs = self.subscribers.lock().await;
        subs.retain(|cell, _| !cells.contains(cell));
    }

    async fn claim_owner(&self, cell: CellId, actor: EntityId, client_id: u64) -> ClaimOutcome {
        let mut owners = self.owners.lock().await;
        let entry = owners.entry(cell).or_default();
        match entry.get(&actor).copied() {
            None => {
                entry.insert(actor, client_id);
                ClaimOutcome::GrantedNew
            }
            Some(existing) if existing == client_id => ClaimOutcome::GrantedExisting,
            Some(existing) => ClaimOutcome::Denied(existing),
        }
    }

    async fn owner_for(&self, cell: &CellId, actor: &EntityId) -> Option<u64> {
        let owners = self.owners.lock().await;
        owners
            .get(cell)
            .and_then(|cell_map| cell_map.get(actor))
            .copied()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    info!(target: "worker", "tessera-worker starting");

    let addr_raw =
        std::env::var("TESSERA_WORKER_ADDR").unwrap_or_else(|_| "127.0.0.1:5001".to_string());
    let addr = resolve_socket_addr(&addr_raw)
        .await
        .with_context(|| format!("resolve TESSERA_WORKER_ADDR={addr_raw}"))?;
    let advertise_addr = resolve_advertise_addr(addr)?;
    let worker_id =
        std::env::var("TESSERA_WORKER_ID").unwrap_or_else(|_| "worker-local".to_string());

    let (assignments, used_fallback) = load_assignments(&worker_id, addr, &advertise_addr, || {
        fetch_assignments(&worker_id, &advertise_addr)
    })
    .await;

    info!(
        target: "worker",
        worker_id = worker_id.as_str(),
        cells = ?assignments,
        "owning cells"
    );
    let owned_cells: Arc<RwLock<HashSet<CellId>>> = Arc::new(RwLock::new(
        assignments.iter().copied().collect::<HashSet<_>>(),
    ));

    let state = Arc::new(SharedState::default());
    let server_state = state.clone();
    let owned_for_server = owned_cells.clone();
    let mut server = tokio::spawn(run_server(addr, server_state, owned_for_server));

    if used_fallback {
        let owned_for_retry = owned_cells.clone();
        let state_for_retry = state.clone();
        let worker_id_for_retry = worker_id.clone();
        let advertise_addr_for_retry = advertise_addr.clone();
        let fetcher = move || {
            let worker_id_for_retry = worker_id_for_retry.clone();
            let advertise_addr_for_retry = advertise_addr_for_retry.clone();
            async move { fetch_assignments(&worker_id_for_retry, &advertise_addr_for_retry).await }
        };
        tokio::spawn(retry_assignments_until_registered(
            worker_id.clone(),
            owned_for_retry,
            state_for_retry,
            fetcher,
            assignment_retry_interval(),
        ));
    }

    // Basic 30Hz tick loop
    let mut tick = Tick(0);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(33));

    let result = loop {
        tokio::select! {
            server_res = &mut server => {
                match server_res {
                    Ok(Ok(())) => {
                        info!(target: "worker", "server task completed");
                        break Ok(());
                    }
                    Ok(Err(e)) => {
                        error!(target: "worker", error = ?e, "server task failed");
                        break Err(e);
                    }
                    Err(join_err) => {
                        error!(target: "worker", error = ?join_err, "server task panicked");
                        break Err(join_err.into());
                    }
                }
            }
            _ = interval.tick() => {
                tick.0 += 1;
                on_tick(tick).await;
            }
            _ = tokio::signal::ctrl_c() => {
                info!(target: "worker", "shutdown signal received");
                break Ok(());
            }
        }
    };

    info!(target: "worker", "tessera-worker stopped");
    // Stop server task if still running
    server.abort();
    result
}

async fn fetch_assignments(worker_id: &str, advertise_addr: &str) -> Result<Vec<CellId>> {
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
            addr: advertise_addr.to_string(),
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

async fn load_assignments<F, Fut>(
    worker_id: &str,
    bind_addr: SocketAddr,
    advertise_addr: &str,
    fetcher: F,
) -> (Vec<CellId>, bool)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Vec<CellId>>>,
{
    match fetcher().await {
        Ok(cells) => {
            if cells.is_empty() {
                warn!(
                    target: "worker",
                    worker_id,
                    bind_addr = %bind_addr,
                    advertise_addr,
                    "orchestrator returned no assignments; worker will idle"
                );
            }
            (cells, false)
        }
        Err(e) => {
            warn!(
                target: "worker",
                worker_id,
                bind_addr = %bind_addr,
                advertise_addr,
                error = ?e,
                "failed to fetch assignments; falling back to default cell"
            );
            (default_assignments(), true)
        }
    }
}

/// 오케스트레이터와 통신하지 못할 때 개발/데모 용도로 사용하는 기본 셀 매핑
fn default_assignments() -> Vec<CellId> {
    vec![CellId::grid(0, 0, 0)]
}

fn resolve_advertise_addr(bind_addr: SocketAddr) -> Result<String> {
    let raw = std::env::var("TESSERA_WORKER_ADVERTISE_ADDR").ok();
    resolve_advertise_addr_inner(bind_addr, raw.as_deref())
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

fn resolve_advertise_addr_inner(bind_addr: SocketAddr, raw: Option<&str>) -> Result<String> {
    if let Some(raw) = raw {
        let addr = raw.trim();
        if !addr.is_empty() {
            if let Ok(parsed) = addr.parse::<SocketAddr>()
                && parsed.ip().is_unspecified()
            {
                return Err(anyhow!(
                    "TESSERA_WORKER_ADVERTISE_ADDR must not be an unspecified address"
                ));
            }
            validate_addr_with_port(addr, "advertise addr")?;
            return Ok(addr.to_string());
        }
    }

    if bind_addr.ip().is_unspecified() || bind_addr.port() == 0 {
        return Err(anyhow!(
            "TESSERA_WORKER_ADVERTISE_ADDR must be set when binding to {}",
            bind_addr
        ));
    }

    let addr = bind_addr.to_string();
    validate_addr_with_port(&addr, "advertise addr")?;
    Ok(addr)
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

fn assignment_retry_interval() -> Duration {
    const DEFAULT_SECS: u64 = 5;
    let secs = std::env::var("TESSERA_WORKER_REFRESH_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|val| *val > 0)
        .unwrap_or(DEFAULT_SECS);
    Duration::from_secs(secs)
}

async fn retry_assignments_until_registered<F, Fut>(
    worker_id: String,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
    state: Arc<SharedState>,
    mut fetcher: F,
    retry_interval: Duration,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<Vec<CellId>>> + Send,
{
    let mut ticker = time::interval(retry_interval);
    loop {
        ticker.tick().await;
        match fetcher().await {
            Ok(cells) => {
                let updated = cells.iter().copied().collect::<HashSet<_>>();
                let removed = {
                    let mut guard = owned_cells.write().await;
                    let removed = guard.difference(&updated).copied().collect::<HashSet<_>>();
                    if *guard != updated {
                        *guard = updated;
                    }
                    removed
                };
                if !removed.is_empty() {
                    state.drop_cells(&removed).await;
                }
                if cells.is_empty() {
                    warn!(
                        target: "worker",
                        worker_id = worker_id.as_str(),
                        "orchestrator returned no assignments; worker will idle"
                    );
                } else {
                    info!(
                        target: "worker",
                        worker_id = worker_id.as_str(),
                        cells = ?cells,
                        "assignments refreshed after orchestrator recovery"
                    );
                }
                break;
            }
            Err(e) => {
                warn!(
                    target: "worker",
                    worker_id = worker_id.as_str(),
                    error = ?e,
                    "failed to refresh assignments; retrying"
                );
            }
        }
    }
}

async fn on_tick(tick: Tick) {
    // TODO: cell queues, AOI broadcasting, metrics
    if tick.0 % 30 == 0 {
        info!(target: "worker", tick = tick.0, "tick heartbeat");
    }
}

fn position_is_finite(pos: &Position) -> bool {
    pos.x.is_finite() && pos.y.is_finite()
}

fn delta_is_finite(dx: f32, dy: f32) -> bool {
    dx.is_finite() && dy.is_finite()
}

fn make_error(code: &str, message: impl Into<String>) -> ServerMsg {
    ServerMsg::Error {
        code: code.to_string(),
        message: message.into(),
    }
}

fn enqueue_error(
    tx: &Sender<OutboundMsg>,
    cell: CellId,
    epoch: u32,
    code: &str,
    message: impl Into<String>,
) -> Result<(), ()> {
    tx.try_send((cell, Some(epoch), make_error(code, message)))
        .map_err(|_| ())
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
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
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
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
) -> Result<()> {
    let (tx, rx) = channel::<OutboundMsg>(OUTBOUND_CHANNEL_CAPACITY);
    let (reader, writer) = stream.into_split();
    handle_upstream_inner(reader, writer, peer, state, owned_cells, tx, rx, true).await
}

#[allow(clippy::too_many_arguments)]
async fn handle_upstream_inner(
    mut reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    peer: SocketAddr,
    state: Arc<SharedState>,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
    tx: Sender<OutboundMsg>,
    mut rx: Receiver<OutboundMsg>,
    spawn_writer: bool,
) -> Result<()> {
    let epoch = Arc::new(AtomicU32::new(0));
    let writer_epoch = Arc::clone(&epoch);
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut owned_actors: HashSet<(CellId, EntityId)> = HashSet::new();

    if spawn_writer {
        tokio::spawn(async move {
            let mut seq_out: u64 = 0;
            let mut writer = writer;
            while let Some((cell, epoch_override, msg)) = rx.recv().await {
                let env_out = Envelope {
                    cell,
                    seq: seq_out,
                    epoch: epoch_override.unwrap_or_else(|| writer_epoch.load(Ordering::Relaxed)),
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
    } else {
        drop(rx);
        drop(writer);
    }

    let result = loop {
        // Read one frame from Gateway
        let mut len_buf = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut len_buf).await {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
                info!(target: "worker", %peer, "upstream closed");
                break Ok(());
            }
            break Err(e.into());
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_LEN {
            warn!(target: "worker", %peer, len, max = MAX_FRAME_LEN, "frame too large");
            break Ok(());
        }
        let mut payload = vec![0u8; len];
        if let Err(e) = reader.read_exact(&mut payload).await {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
                info!(target: "worker", %peer, "upstream closed");
                break Ok(());
            }
            break Err(e.into());
        }

        // Decode as Envelope<ClientMsg>
        let mut buf = BytesMut::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(&payload);
        let env_in = match try_decode_frame::<Envelope<ClientMsg>>(&mut buf) {
            Ok(Some(env_in)) => env_in,
            Ok(None) => {
                warn!(
                    target: "worker",
                    %peer,
                    "incomplete frame; closing connection"
                );
                break Ok(());
            }
            Err(e) => {
                warn!(
                    target: "worker",
                    %peer,
                    error = ?e,
                    "failed to decode frame; closing connection"
                );
                break Ok(());
            }
        };
        let cell = env_in.cell;
        // 할당 갱신과 메시지 처리가 엇갈리면 해제된 셀에 상태가 다시 생길 수 있어,
        // 처리 중에는 소유 셀 읽기 락을 유지해 레이스를 막는다.
        let owned_guard = owned_cells.read().await;
        if !owned_guard.contains(&cell) {
            warn!(
                target: "worker",
                %peer,
                cell = ?cell,
                "received message for cell not owned by this worker"
            );
            if enqueue_error(
                &tx,
                cell,
                env_in.epoch,
                "cell_not_owned",
                "cell not owned by this worker",
            )
            .is_err()
            {
                warn!(
                    target: "worker",
                    %peer,
                    "client response channel closed; closing connection"
                );
                break Ok(());
            }
            continue;
        }

        match env_in.payload {
            ClientMsg::Ping { ts } => {
                epoch.store(env_in.epoch, Ordering::Relaxed);
                if tx
                    .try_send((cell, Some(env_in.epoch), ServerMsg::Pong { ts }))
                    .is_err()
                {
                    warn!(
                        target: "worker",
                        %peer,
                        "client response channel closed; closing connection"
                    );
                    break Ok(());
                }
            }
            ClientMsg::Join { actor, pos } => {
                if !position_is_finite(&pos) {
                    warn!(
                        target: "worker",
                        %peer,
                        cell = ?cell,
                        actor = actor.0,
                        "join rejected: non-finite position"
                    );
                    if enqueue_error(
                        &tx,
                        cell,
                        env_in.epoch,
                        "invalid_position",
                        "join rejected: invalid position",
                    )
                    .is_err()
                    {
                        warn!(
                            target: "worker",
                            %peer,
                            "client response channel closed; closing connection"
                        );
                        break Ok(());
                    }
                    continue;
                }
                let claim = state.claim_owner(cell, actor, client_id).await;
                if let ClaimOutcome::Denied(existing) = claim {
                    warn!(
                        target: "worker",
                        %peer,
                        cell = ?cell,
                        actor = actor.0,
                        existing_owner = existing,
                        "join rejected: actor owned by different client"
                    );
                    if enqueue_error(
                        &tx,
                        cell,
                        env_in.epoch,
                        "actor_owned_by_other",
                        "join rejected: actor owned by another client",
                    )
                    .is_err()
                    {
                        warn!(
                            target: "worker",
                            %peer,
                            "client response channel closed; closing connection"
                        );
                        break Ok(());
                    }
                    continue;
                }

                epoch.store(env_in.epoch, Ordering::Relaxed);

                // 스냅샷 처리 중 델타 누락을 막기 위해 구독을 같은 락 구간에서 갱신한다.
                let inserted_new = match state
                    .snapshot_and_subscribe(cell, actor, pos, client_id, &tx, env_in.epoch)
                    .await
                {
                    Ok(inserted_new) => inserted_new,
                    Err(()) => {
                        if matches!(claim, ClaimOutcome::GrantedNew) {
                            state.release_owner(cell, actor, client_id).await;
                        }
                        warn!(
                            target: "worker",
                            %peer,
                            "client response channel closed; closing connection"
                        );
                        break Ok(());
                    }
                };
                if inserted_new || matches!(claim, ClaimOutcome::GrantedNew) {
                    owned_actors.insert((cell, actor));
                }

                let moved = ActorState { id: actor, pos };
                state
                    .broadcast(
                        cell,
                        ServerMsg::Delta {
                            cell,
                            moved: vec![moved],
                        },
                        Some(client_id),
                        Some(env_in.epoch),
                    )
                    .await;
            }
            ClientMsg::Move { actor, dx, dy } => {
                if !delta_is_finite(dx, dy) {
                    warn!(
                        target: "worker",
                        %peer,
                        cell = ?cell,
                        actor = actor.0,
                        "move rejected: non-finite delta"
                    );
                    if enqueue_error(
                        &tx,
                        cell,
                        env_in.epoch,
                        "invalid_delta",
                        "move rejected: invalid delta",
                    )
                    .is_err()
                    {
                        warn!(
                            target: "worker",
                            %peer,
                            "client response channel closed; closing connection"
                        );
                        break Ok(());
                    }
                    continue;
                }
                match state.owner_for(&cell, &actor).await {
                    Some(owner) if owner == client_id => {}
                    Some(owner) => {
                        warn!(
                            target: "worker",
                            %peer,
                            cell = ?cell,
                            actor = actor.0,
                            owner,
                            client_id,
                            "move rejected: actor owned by different client"
                        );
                        if enqueue_error(
                            &tx,
                            cell,
                            env_in.epoch,
                            "actor_owned_by_other",
                            "move rejected: actor owned by another client",
                        )
                        .is_err()
                        {
                            warn!(
                                target: "worker",
                                %peer,
                                "client response channel closed; closing connection"
                            );
                            break Ok(());
                        }
                        continue;
                    }
                    None => {
                        warn!(
                            target: "worker",
                            %peer,
                            cell = ?cell,
                            actor = actor.0,
                            "move rejected: actor not joined"
                        );
                        if enqueue_error(
                            &tx,
                            cell,
                            env_in.epoch,
                            "actor_not_joined",
                            "move rejected: actor not joined",
                        )
                        .is_err()
                        {
                            warn!(
                                target: "worker",
                                %peer,
                                "client response channel closed; closing connection"
                            );
                            break Ok(());
                        }
                        continue;
                    }
                }

                state.subscribe(cell, client_id, tx.clone()).await;

                let moved = {
                    let mut actors = state.actors.lock().await;
                    let cell_actors = match actors.get_mut(&cell) {
                        Some(map) => map,
                        None => {
                            warn!(
                                target: "worker",
                                %peer,
                                cell = ?cell,
                                actor = actor.0,
                                "move rejected: cell has no actors"
                            );
                            if enqueue_error(
                                &tx,
                                cell,
                                env_in.epoch,
                                "actor_not_joined",
                                "move rejected: actor not joined",
                            )
                            .is_err()
                            {
                                warn!(
                                    target: "worker",
                                    %peer,
                                    "client response channel closed; closing connection"
                                );
                                break Ok(());
                            }
                            continue;
                        }
                    };
                    let entry = match cell_actors.get_mut(&actor) {
                        Some(pos) => pos,
                        None => {
                            warn!(
                                target: "worker",
                                %peer,
                                cell = ?cell,
                                actor = actor.0,
                                "move rejected: actor not found in cell"
                            );
                            if enqueue_error(
                                &tx,
                                cell,
                                env_in.epoch,
                                "actor_not_joined",
                                "move rejected: actor not joined",
                            )
                            .is_err()
                            {
                                warn!(
                                    target: "worker",
                                    %peer,
                                    "client response channel closed; closing connection"
                                );
                                break Ok(());
                            }
                            continue;
                        }
                    };
                    let new_x = entry.x + dx;
                    let new_y = entry.y + dy;
                    if !(new_x.is_finite() && new_y.is_finite()) {
                        warn!(
                            target: "worker",
                            %peer,
                            cell = ?cell,
                            actor = actor.0,
                            "move rejected: position overflow"
                        );
                        if enqueue_error(
                            &tx,
                            cell,
                            env_in.epoch,
                            "position_overflow",
                            "move rejected: position overflow",
                        )
                        .is_err()
                        {
                            warn!(
                                target: "worker",
                                %peer,
                                "client response channel closed; closing connection"
                            );
                            break Ok(());
                        }
                        continue;
                    }
                    entry.x = new_x;
                    entry.y = new_y;
                    ActorState {
                        id: actor,
                        pos: *entry,
                    }
                };

                epoch.store(env_in.epoch, Ordering::Relaxed);

                let delta = ServerMsg::Delta {
                    cell,
                    moved: vec![moved],
                };
                let mut outbound_failed = false;
                if tx
                    .try_send((cell, Some(env_in.epoch), delta.clone()))
                    .is_err()
                {
                    warn!(
                        target: "worker",
                        %peer,
                        "client response channel closed; closing connection"
                    );
                    outbound_failed = true;
                }
                state
                    .broadcast(cell, delta, Some(client_id), Some(env_in.epoch))
                    .await;
                if outbound_failed {
                    break Ok(());
                }
            }
        }
    };

    state.remove_client_subscriptions(client_id).await;
    state
        .remove_owned_actors(
            client_id,
            &owned_actors,
            Some(epoch.load(Ordering::Relaxed)),
        )
        .await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

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

    #[tokio::test]
    async fn claim_owner_rejects_conflicts() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(1);

        assert_eq!(
            state.claim_owner(cell, actor, 1).await,
            ClaimOutcome::GrantedNew
        );
        assert_eq!(
            state.claim_owner(cell, actor, 2).await,
            ClaimOutcome::Denied(1)
        );
        assert_eq!(state.owner_for(&cell, &actor).await, Some(1));
    }

    #[test]
    fn resolve_advertise_addr_rejects_unspecified_bind_without_override() {
        let bind: SocketAddr = "0.0.0.0:5001".parse().unwrap();
        let err = resolve_advertise_addr_inner(bind, None).expect_err("should reject");
        assert!(
            err.to_string()
                .contains("TESSERA_WORKER_ADVERTISE_ADDR must be set")
        );
    }

    #[test]
    fn resolve_advertise_addr_rejects_unspecified_override() {
        let bind: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let err = resolve_advertise_addr_inner(bind, Some("0.0.0.0:5001"))
            .expect_err("should reject unspecified override");
        assert!(
            err.to_string()
                .contains("must not be an unspecified address")
        );
    }

    #[test]
    fn resolve_advertise_addr_accepts_hostname_override() {
        let bind: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let addr = resolve_advertise_addr_inner(bind, Some("worker-a:5001"))
            .expect("should accept hostname");
        assert_eq!(addr, "worker-a:5001");
    }

    #[test]
    fn resolve_advertise_addr_rejects_missing_port_override() {
        let bind: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let err = resolve_advertise_addr_inner(bind, Some("worker-a"))
            .expect_err("should reject missing port");
        assert!(err.to_string().contains("port"));
    }

    #[test]
    fn resolve_advertise_addr_ignores_empty_override() {
        let bind: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let addr =
            resolve_advertise_addr_inner(bind, Some("   ")).expect("should ignore empty override");
        assert_eq!(addr, "127.0.0.1:5001");
    }

    #[test]
    fn resolve_advertise_addr_requires_override_for_port_zero() {
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let err = resolve_advertise_addr_inner(bind, None).expect_err("should reject port 0");
        assert!(
            err.to_string()
                .contains("TESSERA_WORKER_ADVERTISE_ADDR must be set")
        );
    }

    #[test]
    fn resolve_advertise_addr_defaults_to_bind_addr() {
        let bind: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let addr = resolve_advertise_addr_inner(bind, None).expect("should default");
        assert_eq!(addr, "127.0.0.1:5001");
    }

    #[tokio::test]
    async fn remove_owned_actors_clears_owners() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(2);

        state.claim_owner(cell, actor, 42).await;
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(cell)
                .or_default()
                .insert(actor, Position { x: 0.0, y: 0.0 });
        }
        let owned = HashSet::from([(cell, actor)]);

        state.remove_owned_actors(42, &owned, None).await;
        assert!(state.owner_for(&cell, &actor).await.is_none());
        let actors = state.actors.lock().await;
        assert!(!actors.contains_key(&cell));
    }

    #[tokio::test]
    async fn remove_owned_actors_skips_if_owner_changed() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(9);

        state.claim_owner(cell, actor, 1).await;
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(cell)
                .or_default()
                .insert(actor, Position { x: 0.0, y: 0.0 });
        }
        {
            let mut owners = state.owners.lock().await;
            owners.entry(cell).or_default().insert(actor, 2);
        }

        let owned = HashSet::from([(cell, actor)]);
        state.remove_owned_actors(1, &owned, None).await;

        assert_eq!(state.owner_for(&cell, &actor).await, Some(2));
        let actors = state.actors.lock().await;
        assert!(
            actors
                .get(&cell)
                .and_then(|cell_map| cell_map.get(&actor))
                .is_some()
        );
    }

    #[tokio::test]
    async fn load_assignments_uses_fallback_on_error() {
        let (cells, used_fallback) = load_assignments(
            "worker-a",
            "127.0.0.1:5001".parse().unwrap(),
            "127.0.0.1:5001",
            || async { Err(anyhow!("boom")) },
        )
        .await;
        assert_eq!(cells, vec![CellId::grid(0, 0, 0)]);
        assert!(used_fallback);
    }

    #[tokio::test]
    async fn load_assignments_allows_empty_list() {
        let (cells, used_fallback) = load_assignments(
            "worker-b",
            "127.0.0.1:5001".parse().unwrap(),
            "127.0.0.1:5001",
            || async { Ok(vec![]) },
        )
        .await;
        assert!(cells.is_empty());
        assert!(!used_fallback);
    }

    #[tokio::test]
    async fn load_assignments_keeps_remote_cells_when_present() {
        let expected = vec![CellId::grid(1, 2, 3)];
        let (cells, used_fallback) = load_assignments(
            "worker-c",
            "127.0.0.1:5001".parse().unwrap(),
            "127.0.0.1:5001",
            || {
                let expected = expected.clone();
                async move { Ok(expected) }
            },
        )
        .await;
        assert_eq!(cells, vec![CellId::grid(1, 2, 3)]);
        assert!(!used_fallback);
    }

    #[tokio::test]
    async fn retry_assignments_replaces_fallback_cells() {
        let state = Arc::new(SharedState::default());
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(CellId::grid(0, 0, 0))
                .or_default()
                .insert(EntityId(1), Position { x: 0.0, y: 0.0 });
        }
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::clone(&attempts);

        retry_assignments_until_registered(
            "worker-retry".to_string(),
            Arc::clone(&owned_cells),
            Arc::clone(&state),
            move || {
                let attempts = Arc::clone(&attempts_clone);
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(anyhow!("orch unreachable"))
                    } else {
                        Ok(vec![CellId::grid(1, 0, 0)])
                    }
                }
            },
            Duration::from_millis(10),
        )
        .await;

        let guard = owned_cells.read().await;
        assert_eq!(*guard, HashSet::from([CellId::grid(1, 0, 0)]));

        let actors = state.actors.lock().await;
        assert!(!actors.contains_key(&CellId::grid(0, 0, 0)));
    }

    #[tokio::test]
    async fn retry_assignments_accepts_empty_list() {
        let state = Arc::new(SharedState::default());
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(CellId::grid(0, 0, 0))
                .or_default()
                .insert(EntityId(1), Position { x: 0.0, y: 0.0 });
        }
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));

        retry_assignments_until_registered(
            "worker-retry-empty".to_string(),
            Arc::clone(&owned_cells),
            Arc::clone(&state),
            || async { Ok(vec![]) },
            Duration::from_millis(10),
        )
        .await;

        let guard = owned_cells.read().await;
        assert!(guard.is_empty());

        let actors = state.actors.lock().await;
        assert!(actors.is_empty());
    }

    #[tokio::test]
    async fn responses_echo_client_epoch() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, state, owned_cells)
                .await
                .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        let env = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 7,
            epoch: 99,
            payload: ClientMsg::Ping { ts: 123 },
        };
        let frame = encode_frame(&env);
        client.write_all(&frame).await.expect("send ping");

        let reply: Envelope<ServerMsg> = {
            let mut len_buf = [0u8; 4];
            client.read_exact(&mut len_buf).await.expect("read len");
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            client.read_exact(&mut payload).await.expect("read payload");
            serde_json::from_slice(&payload).expect("decode reply")
        };
        assert_eq!(reply.epoch, 99);
        assert_eq!(reply.seq, 0);
        assert!(matches!(reply.payload, ServerMsg::Pong { ts } if ts == 123));

        drop(client);
        let _ = server.await;
    }

    #[tokio::test]
    async fn responses_use_request_epoch_per_message() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, state, owned_cells)
                .await
                .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        let env1 = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 1,
            payload: ClientMsg::Ping { ts: 1 },
        };
        let env2 = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 1,
            epoch: 2,
            payload: ClientMsg::Ping { ts: 2 },
        };
        client
            .write_all(&encode_frame(&env1))
            .await
            .expect("send first ping");
        client
            .write_all(&encode_frame(&env2))
            .await
            .expect("send second ping");

        let reply1 = read_env(&mut client).await;
        let reply2 = read_env(&mut client).await;

        assert_eq!(reply1.epoch, 1);
        assert!(matches!(reply1.payload, ServerMsg::Pong { ts } if ts == 1));
        assert_eq!(reply2.epoch, 2);
        assert!(matches!(reply2.payload, ServerMsg::Pong { ts } if ts == 2));

        drop(client);
        let _ = server.await;
    }

    #[tokio::test]
    async fn broadcast_uses_request_epoch() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let cell = CellId::grid(0, 0, 0);
        let mut client_a = TcpStream::connect(addr)
            .await
            .expect("connect first client");
        let mut client_b = TcpStream::connect(addr)
            .await
            .expect("connect second client");

        let join_a = Envelope {
            cell,
            seq: 0,
            epoch: 1,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        client_a
            .write_all(&encode_frame(&join_a))
            .await
            .expect("send first join");
        let _ = read_env(&mut client_a).await;

        let join_b = Envelope {
            cell,
            seq: 0,
            epoch: 100,
            payload: ClientMsg::Join {
                actor: EntityId(2),
                pos: Position { x: 1.0, y: 1.0 },
            },
        };
        client_b
            .write_all(&encode_frame(&join_b))
            .await
            .expect("send second join");
        let _ = read_env(&mut client_b).await;

        let _ = read_env(&mut client_a).await;

        let move_a = Envelope {
            cell,
            seq: 1,
            epoch: 7,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: 1.0,
                dy: 0.0,
            },
        };
        client_a
            .write_all(&encode_frame(&move_a))
            .await
            .expect("send move");
        let reply_a = read_env(&mut client_a).await;
        assert_eq!(reply_a.epoch, 7);

        let delta_b = read_env(&mut client_b).await;
        assert_eq!(delta_b.epoch, 7);
        assert!(matches!(delta_b.payload, ServerMsg::Delta { .. }));

        drop(client_a);
        drop(client_b);
        let _ = server.await;
    }

    #[tokio::test]
    async fn move_without_join_returns_error() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, state, owned_cells)
                .await
                .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        let env = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 7,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: 1.0,
                dy: 0.0,
            },
        };
        client
            .write_all(&encode_frame(&env))
            .await
            .expect("send move");

        let reply = read_env(&mut client).await;
        assert_eq!(reply.epoch, 7);
        assert!(matches!(
            reply.payload,
            ServerMsg::Error { ref code, .. } if code == "actor_not_joined"
        ));

        drop(client);
        let _ = server.await;
    }

    #[test]
    fn position_is_finite_checks_values() {
        assert!(!position_is_finite(&Position {
            x: f32::NAN,
            y: 0.0
        }));
        assert!(!position_is_finite(&Position {
            x: f32::INFINITY,
            y: 1.0
        }));
        assert!(position_is_finite(&Position { x: 1.0, y: 2.0 }));
    }

    #[test]
    fn delta_is_finite_checks_values() {
        assert!(!delta_is_finite(f32::NAN, 0.0));
        assert!(!delta_is_finite(0.0, f32::NEG_INFINITY));
        assert!(delta_is_finite(0.5, -1.25));
    }

    #[tokio::test]
    async fn duplicate_join_rejected_for_different_client() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let mut first = TcpStream::connect(addr)
            .await
            .expect("connect first client");
        let mut second = TcpStream::connect(addr)
            .await
            .expect("connect second client");
        let cell = CellId::grid(0, 0, 0);

        let first_join = Envelope {
            cell,
            seq: 0,
            epoch: 1,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        first
            .write_all(&encode_frame(&first_join))
            .await
            .expect("send first join");
        let _ = read_env(&mut first).await;

        let second_join = Envelope {
            cell,
            seq: 0,
            epoch: 2,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 1.0, y: 1.0 },
            },
        };
        second
            .write_all(&encode_frame(&second_join))
            .await
            .expect("send second join");
        let reply = read_env(&mut second).await;
        assert!(matches!(
            reply.payload,
            ServerMsg::Error { ref code, .. } if code == "actor_owned_by_other"
        ));

        drop(first);
        drop(second);
        let _ = server.await;
    }

    #[tokio::test]
    async fn move_rejected_when_actor_owned_by_other() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let mut owner = TcpStream::connect(addr).await.expect("connect owner");
        let mut other = TcpStream::connect(addr)
            .await
            .expect("connect other client");
        let cell = CellId::grid(0, 0, 0);

        let owner_join = Envelope {
            cell,
            seq: 0,
            epoch: 1,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        owner
            .write_all(&encode_frame(&owner_join))
            .await
            .expect("owner join");
        let _ = read_env(&mut owner).await;

        let other_move = Envelope {
            cell,
            seq: 0,
            epoch: 2,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: 1.0,
                dy: 1.0,
            },
        };
        other
            .write_all(&encode_frame(&other_move))
            .await
            .expect("other move");
        let reply = read_env(&mut other).await;
        assert!(matches!(
            reply.payload,
            ServerMsg::Error { ref code, .. } if code == "actor_owned_by_other"
        ));

        drop(owner);
        drop(other);
        let _ = server.await;
    }

    #[tokio::test]
    async fn move_rejects_position_overflow() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let server_state = Arc::clone(&state);
        let server_owned = Arc::clone(&owned_cells);
        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, server_state, server_owned)
                .await
                .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        let cell = CellId::grid(0, 0, 0);

        let join = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position {
                    x: f32::MAX,
                    y: 0.0,
                },
            },
        };
        client
            .write_all(&encode_frame(&join))
            .await
            .expect("send join");
        let _ = read_env(&mut client).await;

        let move_env = Envelope {
            cell,
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: f32::MAX,
                dy: 0.0,
            },
        };
        client
            .write_all(&encode_frame(&move_env))
            .await
            .expect("send move");
        let reply = read_env(&mut client).await;
        assert!(matches!(
            reply.payload,
            ServerMsg::Error { ref code, .. } if code == "position_overflow"
        ));

        {
            let actors = state.actors.lock().await;
            let cell_actors = actors.get(&cell).expect("cell actors");
            let pos = cell_actors.get(&EntityId(1)).expect("actor");
            assert_eq!(pos.x, f32::MAX);
            assert_eq!(pos.y, 0.0);
        }

        drop(client);
        timeout(Duration::from_millis(500), server)
            .await
            .expect("server timeout")
            .expect("server join");
    }

    #[tokio::test]
    async fn unauthorized_client_does_not_receive_broadcast() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let mut unauthorized = TcpStream::connect(addr)
            .await
            .expect("connect unauthorized client");
        let mut authorized = TcpStream::connect(addr)
            .await
            .expect("connect authorized client");

        let unauthorized_move = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Move {
                actor: EntityId(99),
                dx: 1.0,
                dy: 1.0,
            },
        };
        unauthorized
            .write_all(&encode_frame(&unauthorized_move))
            .await
            .expect("send unauthorized move");
        let unauthorized_reply = read_env(&mut unauthorized).await;
        assert!(matches!(
            unauthorized_reply.payload,
            ServerMsg::Error { ref code, .. } if code == "actor_not_joined"
        ));

        let join_env = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        authorized
            .write_all(&encode_frame(&join_env))
            .await
            .expect("send join");

        let _snapshot = read_env(&mut authorized).await;

        let move_env = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 1,
            epoch: 0,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: 1.0,
                dy: 0.0,
            },
        };
        authorized
            .write_all(&encode_frame(&move_env))
            .await
            .expect("send move");
        let _delta = read_env(&mut authorized).await;

        let leaked = timeout(Duration::from_millis(200), async {
            read_env(&mut unauthorized).await
        })
        .await;

        drop(authorized);
        drop(unauthorized);
        let _ = server.await;

        assert!(
            leaked.is_err(),
            "unauthorized client should not receive broadcast without joining"
        );
    }

    #[tokio::test]
    async fn joiner_does_not_receive_self_delta() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let mut first = TcpStream::connect(addr)
            .await
            .expect("connect first client");
        let mut second = TcpStream::connect(addr)
            .await
            .expect("connect second client");

        let first_join = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 1,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        first
            .write_all(&encode_frame(&first_join))
            .await
            .expect("send first join");
        let _ = read_env(&mut first).await; // snapshot
        let _ = timeout(Duration::from_millis(100), async {
            read_env(&mut first).await
        })
        .await;

        let second_join = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 2,
            payload: ClientMsg::Join {
                actor: EntityId(2),
                pos: Position { x: 1.0, y: 1.0 },
            },
        };
        second
            .write_all(&encode_frame(&second_join))
            .await
            .expect("send second join");

        let snapshot_second = read_env(&mut second).await;
        assert!(matches!(
            snapshot_second.payload,
            ServerMsg::Snapshot { .. }
        ));

        let extra = timeout(Duration::from_millis(150), async {
            read_env(&mut second).await
        })
        .await;
        assert!(
            extra.is_err(),
            "joiner should not receive delta for its own join"
        );

        let delta_for_first = read_env(&mut first).await;
        match delta_for_first.payload {
            ServerMsg::Delta { moved, .. } => {
                assert!(
                    moved.iter().any(|actor| actor.id == EntityId(2)),
                    "expected delta about second client join"
                );
            }
            other => panic!("expected delta for first client, got {other:?}"),
        }

        drop(first);
        drop(second);
        let _ = server.await;
    }

    #[tokio::test]
    async fn disconnecting_owner_broadcasts_despawn() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let mut owner = TcpStream::connect(addr).await.expect("connect owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");

        let owner_join = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        owner
            .write_all(&encode_frame(&owner_join))
            .await
            .expect("owner join");
        let _ = read_env(&mut owner).await;

        let watcher_join = Envelope {
            cell: CellId::grid(0, 0, 0),
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(2),
                pos: Position { x: 1.0, y: 1.0 },
            },
        };
        watcher
            .write_all(&encode_frame(&watcher_join))
            .await
            .expect("watcher join");
        let _ = read_env(&mut watcher).await;

        drop(owner);

        let despawn = timeout(Duration::from_millis(300), async {
            read_env(&mut watcher).await
        })
        .await
        .expect("watcher should receive despawn after owner disconnect");

        match despawn.payload {
            ServerMsg::Despawn { actors, .. } => {
                assert!(
                    actors.contains(&EntityId(1)),
                    "despawn should include owner actor"
                );
            }
            other => panic!("expected despawn, got {other:?}"),
        }

        drop(watcher);
        let _ = server.await;
    }

    #[tokio::test]
    async fn despawn_uses_last_owner_epoch() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let mut owner = TcpStream::connect(addr).await.expect("connect owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");
        let cell = CellId::grid(0, 0, 0);

        let owner_join = Envelope {
            cell,
            seq: 0,
            epoch: 10,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        owner
            .write_all(&encode_frame(&owner_join))
            .await
            .expect("owner join");
        let _ = read_env(&mut owner).await;

        let watcher_join = Envelope {
            cell,
            seq: 0,
            epoch: 20,
            payload: ClientMsg::Join {
                actor: EntityId(2),
                pos: Position { x: 1.0, y: 1.0 },
            },
        };
        watcher
            .write_all(&encode_frame(&watcher_join))
            .await
            .expect("watcher join");
        let _ = read_env(&mut watcher).await;

        let _ = read_env(&mut owner).await;

        let owner_move = Envelope {
            cell,
            seq: 1,
            epoch: 123,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: 1.0,
                dy: 0.0,
            },
        };
        owner
            .write_all(&encode_frame(&owner_move))
            .await
            .expect("owner move");
        let owner_delta = read_env(&mut owner).await;
        assert_eq!(owner_delta.epoch, 123);
        let watcher_delta = read_env(&mut watcher).await;
        assert_eq!(watcher_delta.epoch, 123);

        drop(owner);

        let despawn = timeout(Duration::from_millis(300), async {
            read_env(&mut watcher).await
        })
        .await
        .expect("watcher should receive despawn after owner disconnect");

        assert_eq!(despawn.epoch, 123);
        match despawn.payload {
            ServerMsg::Despawn { actors, .. } => {
                assert!(
                    actors.contains(&EntityId(1)),
                    "despawn should include owner actor"
                );
            }
            other => panic!("expected despawn, got {other:?}"),
        }

        drop(watcher);
        let _ = server.await;
    }

    #[tokio::test]
    async fn invalid_move_does_not_advance_epoch_for_despawn() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..2 {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&accept_state);
                let owned = Arc::clone(&accept_owned);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_upstream(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        });

        let mut owner = TcpStream::connect(addr).await.expect("connect owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");
        let cell = CellId::grid(0, 0, 0);

        let owner_join = Envelope {
            cell,
            seq: 0,
            epoch: 10,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position {
                    x: f32::MAX,
                    y: 0.0,
                },
            },
        };
        owner
            .write_all(&encode_frame(&owner_join))
            .await
            .expect("owner join");
        let _ = read_env(&mut owner).await;

        let watcher_join = Envelope {
            cell,
            seq: 0,
            epoch: 20,
            payload: ClientMsg::Join {
                actor: EntityId(2),
                pos: Position { x: 1.0, y: 1.0 },
            },
        };
        watcher
            .write_all(&encode_frame(&watcher_join))
            .await
            .expect("watcher join");
        let _ = read_env(&mut watcher).await;
        let _ = read_env(&mut owner).await;

        let invalid_move = Envelope {
            cell,
            seq: 1,
            epoch: 999,
            payload: ClientMsg::Move {
                actor: EntityId(1),
                dx: f32::MAX,
                dy: 0.0,
            },
        };
        owner
            .write_all(&encode_frame(&invalid_move))
            .await
            .expect("send invalid move");
        let reply = read_env(&mut owner).await;
        assert!(matches!(
            reply.payload,
            ServerMsg::Error { ref code, .. } if code == "position_overflow"
        ));

        drop(owner);

        let despawn = timeout(Duration::from_millis(300), async {
            read_env(&mut watcher).await
        })
        .await
        .expect("watcher should receive despawn after owner disconnect");

        assert_eq!(despawn.epoch, 10);
        match despawn.payload {
            ServerMsg::Despawn { actors, .. } => {
                assert!(
                    actors.contains(&EntityId(1)),
                    "despawn should include owner actor"
                );
            }
            other => panic!("expected despawn, got {other:?}"),
        }

        drop(watcher);
        let _ = server.await;
    }

    #[tokio::test]
    async fn subscribe_is_idempotent_and_recoverable() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);

        let (tx, _rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        state.subscribe(cell, 10, tx).await;
        let (tx2, _rx2) = channel(OUTBOUND_CHANNEL_CAPACITY);
        state.subscribe(cell, 10, tx2).await;

        {
            let subs = state.subscribers.lock().await;
            assert_eq!(subs.get(&cell).map(|v| v.len()), Some(1));
        }

        let removed = HashSet::from([cell]);
        state.drop_cells(&removed).await;

        let (tx3, _rx3) = channel(OUTBOUND_CHANNEL_CAPACITY);
        state.subscribe(cell, 10, tx3).await;

        let subs = state.subscribers.lock().await;
        assert_eq!(subs.get(&cell).map(|v| v.len()), Some(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn join_snapshot_locks_actors_before_subscribers() {
        let state = Arc::new(SharedState::default());
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(1);
        let pos = Position { x: 0.0, y: 0.0 };

        let (tx, _rx) = channel(OUTBOUND_CHANNEL_CAPACITY);

        let actors_guard = state.actors.lock().await;

        let state_clone = Arc::clone(&state);
        let join_handle = tokio::spawn(async move {
            state_clone
                .snapshot_and_subscribe(cell, actor, pos, 1, &tx, 0)
                .await
                .expect("snapshot");
        });

        tokio::task::yield_now().await;

        let subs_guard = timeout(Duration::from_millis(50), state.subscribers.lock())
            .await
            .expect("subscribers lock should be available");
        drop(subs_guard);
        drop(actors_guard);

        timeout(Duration::from_millis(200), join_handle)
            .await
            .expect("join task timeout")
            .expect("join task failed");
    }

    #[tokio::test]
    async fn broadcast_removes_closed_subscribers() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);

        let (tx, rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        state.subscribe(cell, 1, tx).await;
        drop(rx);

        state
            .broadcast(cell, ServerMsg::Pong { ts: 1 }, None, None)
            .await;

        let subs = state.subscribers.lock().await;
        assert!(subs.is_empty());
    }

    #[tokio::test]
    async fn broadcast_prunes_closed_excluded_subscribers() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);

        let (tx, rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        state.subscribe(cell, 1, tx).await;
        drop(rx);

        state
            .broadcast(cell, ServerMsg::Pong { ts: 2 }, Some(1), None)
            .await;

        let subs = state.subscribers.lock().await;
        assert!(subs.is_empty());
    }

    #[tokio::test]
    async fn cell_not_owned_returns_error() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let server_state = Arc::clone(&state);
        let server_owned = Arc::clone(&owned_cells);
        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, server_state, server_owned)
                .await
                .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        let env = Envelope {
            cell: CellId::grid(9, 0, 0),
            seq: 0,
            epoch: 42,
            payload: ClientMsg::Ping { ts: 1 },
        };
        client
            .write_all(&encode_frame(&env))
            .await
            .expect("send ping");

        let reply = read_env(&mut client).await;
        assert_eq!(reply.epoch, 42);
        assert!(matches!(
            reply.payload,
            ServerMsg::Error { ref code, .. } if code == "cell_not_owned"
        ));

        drop(client);
        timeout(Duration::from_millis(500), server)
            .await
            .expect("server timeout")
            .expect("server join");

        let subs = state.subscribers.lock().await;
        assert!(subs.is_empty());
        let actors = state.actors.lock().await;
        assert!(actors.is_empty());
    }

    #[tokio::test]
    async fn disconnect_mid_frame_is_clean_close() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let server_state = Arc::clone(&state);
        let server_owned = Arc::clone(&owned_cells);
        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, server_state, server_owned).await
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        client
            .write_all(&32u32.to_be_bytes())
            .await
            .expect("send length");
        client
            .write_all(&[0u8; 4])
            .await
            .expect("send partial payload");
        drop(client);

        let result = timeout(Duration::from_millis(500), server)
            .await
            .expect("server timeout")
            .expect("server join");
        assert!(result.is_ok());

        let subs = state.subscribers.lock().await;
        assert!(subs.is_empty());
        let actors = state.actors.lock().await;
        assert!(actors.is_empty());
    }

    #[tokio::test]
    async fn disconnect_removes_subscriptions() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let server_state = Arc::clone(&state);
        let server_owned = Arc::clone(&owned_cells);
        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, server_state, server_owned)
                .await
                .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
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
        let _ = read_env(&mut client).await;

        drop(client);
        timeout(Duration::from_millis(500), server)
            .await
            .expect("server timeout")
            .expect("server join");

        let subs = state.subscribers.lock().await;
        assert!(subs.is_empty());
        let actors = state.actors.lock().await;
        assert!(actors.is_empty());
        let owners = state.owners.lock().await;
        assert!(owners.is_empty());
    }

    #[tokio::test]
    async fn join_snapshot_failure_cleans_ownership() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            let (reader, writer) = sock.into_split();
            let (tx, rx) = channel::<OutboundMsg>(1);
            handle_upstream_inner(
                reader,
                writer,
                peer,
                accept_state,
                accept_owned,
                tx,
                rx,
                false,
            )
            .await
            .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
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
        drop(client);

        timeout(Duration::from_millis(500), server)
            .await
            .expect("server timeout")
            .expect("server join");

        let owners = state.owners.lock().await;
        assert!(owners.is_empty());
        let actors = state.actors.lock().await;
        assert!(actors.is_empty());
    }

    #[tokio::test]
    async fn join_snapshot_failure_does_not_broadcast_despawn() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let owned_cells = Arc::new(RwLock::new(HashSet::from([CellId::grid(0, 0, 0)])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let (watcher_sock, watcher_peer) = listener.accept().await.expect("accept watcher");
            let watcher_state = Arc::clone(&accept_state);
            let watcher_owned = Arc::clone(&accept_owned);
            let watcher_task = tokio::spawn(async move {
                let _ =
                    handle_upstream(watcher_sock, watcher_peer, watcher_state, watcher_owned).await;
            });

            let (joiner_sock, joiner_peer) = listener.accept().await.expect("accept joiner");
            let joiner_state = Arc::clone(&accept_state);
            let joiner_owned = Arc::clone(&accept_owned);
            let joiner_task = tokio::spawn(async move {
                let (reader, writer) = joiner_sock.into_split();
                let (tx, rx) = channel::<OutboundMsg>(1);
                let _ = handle_upstream_inner(
                    reader,
                    writer,
                    joiner_peer,
                    joiner_state,
                    joiner_owned,
                    tx,
                    rx,
                    false,
                )
                .await;
            });

            let _ = watcher_task.await;
            let _ = joiner_task.await;
        });

        let cell = CellId::grid(0, 0, 0);

        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");
        let watcher_join = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        watcher
            .write_all(&encode_frame(&watcher_join))
            .await
            .expect("send watcher join");
        let _ = read_env(&mut watcher).await;

        let mut joiner = TcpStream::connect(addr).await.expect("connect joiner");
        let joiner_join = Envelope {
            cell,
            seq: 0,
            epoch: 0,
            payload: ClientMsg::Join {
                actor: EntityId(2),
                pos: Position { x: 1.0, y: 1.0 },
            },
        };
        joiner
            .write_all(&encode_frame(&joiner_join))
            .await
            .expect("send joiner join");

        let leaked = timeout(Duration::from_millis(200), read_env(&mut watcher)).await;
        assert!(
            leaked.is_err(),
            "watcher should not receive despawn for failed join"
        );

        drop(joiner);
        drop(watcher);
        timeout(Duration::from_millis(500), server)
            .await
            .expect("server timeout")
            .expect("server join");
    }

    async fn read_env(stream: &mut TcpStream) -> Envelope<ServerMsg> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.expect("read len");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.expect("read payload");
        serde_json::from_slice(&payload).expect("decode frame")
    }
}
