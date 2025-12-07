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
    ActorState, CellId, ClientMsg, EntityId, Envelope, Position, ServerMsg, Tick, encode_frame,
    try_decode_frame,
};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{Assignment, WorkerRegistration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    Mutex, RwLock,
    mpsc::{UnboundedSender, unbounded_channel},
};
use tokio::time;
use tracing::{error, info, warn};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
type OutboundMsg = (CellId, Option<u32>, ServerMsg);
type CellSubscribers = HashMap<CellId, Vec<(u64, UnboundedSender<OutboundMsg>)>>;
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
    async fn subscribe(&self, cell: CellId, client_id: u64, tx: UnboundedSender<OutboundMsg>) {
        let mut subs = self.subscribers.lock().await;
        let entry = subs.entry(cell).or_default();
        if entry.iter().any(|(id, _)| *id == client_id) {
            return;
        }
        entry.push((client_id, tx));
    }

    async fn remove_client_subscriptions(&self, client_id: u64) {
        let mut subs = self.subscribers.lock().await;
        subs.values_mut().for_each(|entries| {
            entries.retain(|(id, _)| *id != client_id);
        });
        subs.retain(|_, entries| !entries.is_empty());
    }

    async fn broadcast(&self, cell: CellId, msg: ServerMsg, exclude: Option<u64>) {
        let mut subs = self.subscribers.lock().await;
        if let Some(entries) = subs.get_mut(&cell) {
            entries.retain(|(id, tx)| {
                if exclude.is_some_and(|excluded| excluded == *id) {
                    return true;
                }
                tx.send((cell, None, msg.clone())).is_ok()
            });
            if entries.is_empty() {
                subs.remove(&cell);
            }
        }
    }

    async fn remove_owned_actors(&self, client_id: u64, owned: &HashSet<(CellId, EntityId)>) {
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
        drop(owners);
        drop(actors);

        for (cell, actors) in removed_by_cell {
            if !actors.is_empty() {
                self.broadcast(cell, ServerMsg::Despawn { cell, actors }, None)
                    .await;
            }
        }
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
            self.broadcast(cell, ServerMsg::Despawn { cell, actors }, None)
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

    let addr: SocketAddr = std::env::var("TESSERA_WORKER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5001".to_string())
        .parse()
        .expect("invalid TESSERA_WORKER_ADDR");
    let worker_id =
        std::env::var("TESSERA_WORKER_ID").unwrap_or_else(|_| "worker-local".to_string());

    let (assignments, used_fallback) =
        load_assignments(&worker_id, addr, || fetch_assignments(&worker_id, addr)).await;

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
        let fetcher = move || {
            let worker_id_for_retry = worker_id_for_retry.clone();
            async move { fetch_assignments(&worker_id_for_retry, addr).await }
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

async fn load_assignments<F, Fut>(
    worker_id: &str,
    worker_addr: SocketAddr,
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
                    addr = %worker_addr,
                    "orchestrator returned no assignments; worker will idle"
                );
            }
            (cells, false)
        }
        Err(e) => {
            warn!(
                target: "worker",
                worker_id,
                addr = %worker_addr,
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
    let (mut reader, mut writer) = stream.into_split();
    let (tx, mut rx) = unbounded_channel::<OutboundMsg>();
    let epoch = Arc::new(AtomicU32::new(0));
    let writer_epoch = Arc::clone(&epoch);
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut owned_actors: HashSet<(CellId, EntityId)> = HashSet::new();

    tokio::spawn(async move {
        let mut seq_out: u64 = 0;
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
        if len > 1_000_000 {
            warn!(target: "worker", %peer, len, "frame too large");
            break Ok(());
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
        epoch.store(env_in.epoch, Ordering::Relaxed);
        let cell = env_in.cell;
        if !owned_cells.read().await.contains(&cell) {
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
                if tx
                    .send((cell, Some(env_in.epoch), ServerMsg::Pong { ts }))
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
                    continue;
                }

                let (snapshot, inserted_new) = {
                    let mut actors = state.actors.lock().await;
                    let cell_actors = actors.entry(cell).or_default();
                    let is_new = cell_actors.insert(actor, pos).is_none();
                    let snapshot = cell_actors
                        .iter()
                        .map(|(id, pos)| ActorState { id: *id, pos: *pos })
                        .collect::<Vec<_>>();
                    (snapshot, is_new)
                };
                if inserted_new || matches!(claim, ClaimOutcome::GrantedNew) {
                    owned_actors.insert((cell, actor));
                }
                if tx
                    .send((
                        cell,
                        Some(env_in.epoch),
                        ServerMsg::Snapshot {
                            cell,
                            actors: snapshot,
                        },
                    ))
                    .is_err()
                {
                    warn!(
                        target: "worker",
                        %peer,
                        "client response channel closed; closing connection"
                    );
                    break Ok(());
                }
                // 조인 클라이언트는 스냅샷을 먼저 받아야 하므로 전송 후 구독한다.
                state.subscribe(cell, client_id, tx.clone()).await;

                let moved = ActorState { id: actor, pos };
                state
                    .broadcast(
                        cell,
                        ServerMsg::Delta {
                            cell,
                            moved: vec![moved],
                        },
                        Some(client_id),
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
                        continue;
                    }
                    entry.x = new_x;
                    entry.y = new_y;
                    ActorState {
                        id: actor,
                        pos: *entry,
                    }
                };

                let delta = ServerMsg::Delta {
                    cell,
                    moved: vec![moved],
                };
                if tx.send((cell, Some(env_in.epoch), delta.clone())).is_err() {
                    warn!(
                        target: "worker",
                        %peer,
                        "client response channel closed; closing connection"
                    );
                    break Ok(());
                }
                state.broadcast(cell, delta, Some(client_id)).await;
            }
        }
    };

    state.remove_client_subscriptions(client_id).await;
    state.remove_owned_actors(client_id, &owned_actors).await;
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

        state.remove_owned_actors(42, &owned).await;
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
        state.remove_owned_actors(1, &owned).await;

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
        let (cells, used_fallback) =
            load_assignments("worker-a", "127.0.0.1:5001".parse().unwrap(), || async {
                Err(anyhow!("boom"))
            })
            .await;
        assert_eq!(cells, vec![CellId::grid(0, 0, 0)]);
        assert!(used_fallback);
    }

    #[tokio::test]
    async fn load_assignments_allows_empty_list() {
        let (cells, used_fallback) =
            load_assignments("worker-b", "127.0.0.1:5001".parse().unwrap(), || async {
                Ok(vec![])
            })
            .await;
        assert!(cells.is_empty());
        assert!(!used_fallback);
    }

    #[tokio::test]
    async fn load_assignments_keeps_remote_cells_when_present() {
        let expected = vec![CellId::grid(1, 2, 3)];
        let (cells, used_fallback) =
            load_assignments("worker-c", "127.0.0.1:5001".parse().unwrap(), || {
                let expected = expected.clone();
                async move { Ok(expected) }
            })
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
    async fn subscribe_is_idempotent_and_recoverable() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);

        let (tx, _rx) = unbounded_channel();
        state.subscribe(cell, 10, tx).await;
        let (tx2, _rx2) = unbounded_channel();
        state.subscribe(cell, 10, tx2).await;

        {
            let subs = state.subscribers.lock().await;
            assert_eq!(subs.get(&cell).map(|v| v.len()), Some(1));
        }

        let removed = HashSet::from([cell]);
        state.drop_cells(&removed).await;

        let (tx3, _rx3) = unbounded_channel();
        state.subscribe(cell, 10, tx3).await;

        let subs = state.subscribers.lock().await;
        assert_eq!(subs.get(&cell).map(|v| v.len()), Some(1));
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
