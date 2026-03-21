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
    WorkerRelayMsg, encode_frame, try_decode_frame,
};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{
    Assignment, AssignmentListing, ListAssignmentsRequest, WatchAssignmentsRequest,
    WorkerRegistration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{
    Mutex, RwLock,
    mpsc::{Receiver, Sender, channel},
    watch,
};
use tokio::time;
use tracing::{error, info, warn};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_RELAY_SUBSCRIBER_ID: AtomicU64 = AtomicU64::new(1);
const OUTBOUND_CHANNEL_CAPACITY: usize = 256;
const DEFAULT_AOI_RADIUS_CELLS: i32 = 1;
const DEFAULT_AOI_CELL_SPAN_UNITS: f32 = 32.0;
type OutboundMsg = (CellId, Option<u32>, ServerMsg);
type RelayOutboundMsg = (CellId, Option<u32>, WorkerRelayMsg);
type CellSubscribers = HashMap<CellId, Vec<CellSubscriber>>;
type RelaySubscribers = HashMap<CellId, Vec<RelaySubscriber>>;
type CellOwners = HashMap<CellId, HashMap<EntityId, u64>>;
type PendingMoveQueues = HashMap<CellId, HashMap<EntityId, PendingMoveBroadcast>>;
type OwnedActors = HashSet<(CellId, EntityId)>;
type PeerRoutes = HashMap<CellId, PeerRoute>;
type RemoteInterestMap = HashMap<u64, HashMap<CellId, PeerRoute>>;
type RemotePeerSessionMap = HashMap<u64, HashMap<String, RemotePeerSessionHandle>>;

#[derive(Clone)]
struct CellSubscriber {
    client_id: u64,
    tx: Sender<OutboundMsg>,
    disconnect_tx: watch::Sender<()>,
}

#[derive(Clone)]
struct RelaySubscriber {
    connection_id: u64,
    tx: Sender<RelayOutboundMsg>,
    disconnect_tx: watch::Sender<()>,
}

#[derive(Clone)]
struct ClientSession {
    subscriber: CellSubscriber,
    root_actors: OwnedActors,
    last_epoch: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimOutcome {
    GrantedNew,
    GrantedExisting,
    Denied(u64),
}

#[derive(Debug, Clone)]
struct PendingMoveBroadcast {
    exclude_client_id: u64,
    epoch: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerRoute {
    worker_id: String,
    addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemotePeerDesired {
    cells: HashSet<CellId>,
    epoch: u32,
}

#[derive(Clone)]
struct RemotePeerSessionHandle {
    desired_tx: watch::Sender<RemotePeerDesired>,
}

struct SharedState {
    actors: Mutex<HashMap<CellId, HashMap<EntityId, Position>>>,
    owners: Mutex<CellOwners>,
    subscribers: Mutex<CellSubscribers>,
    relay_subscribers: Mutex<RelaySubscribers>,
    pending_moves: Mutex<PendingMoveQueues>,
    client_sessions: Mutex<HashMap<u64, ClientSession>>,
    peer_routes: RwLock<PeerRoutes>,
    remote_interests: Mutex<RemoteInterestMap>,
    remote_peer_sessions: Mutex<RemotePeerSessionMap>,
    aoi_radius_cells: i32,
    aoi_cell_span_units: f32,
    aoi_edge_margin_units: Option<f32>,
}

impl SharedState {
    fn with_aoi_config(
        aoi_radius_cells: i32,
        aoi_cell_span_units: f32,
        aoi_edge_margin_units: Option<f32>,
    ) -> Self {
        Self {
            actors: Mutex::new(HashMap::new()),
            owners: Mutex::new(HashMap::new()),
            subscribers: Mutex::new(HashMap::new()),
            relay_subscribers: Mutex::new(HashMap::new()),
            pending_moves: Mutex::new(HashMap::new()),
            client_sessions: Mutex::new(HashMap::new()),
            peer_routes: RwLock::new(HashMap::new()),
            remote_interests: Mutex::new(HashMap::new()),
            remote_peer_sessions: Mutex::new(HashMap::new()),
            aoi_radius_cells,
            aoi_cell_span_units,
            aoi_edge_margin_units,
        }
    }

    fn new(aoi_radius_cells: i32) -> Self {
        Self::with_aoi_config(aoi_radius_cells, DEFAULT_AOI_CELL_SPAN_UNITS, None)
    }

    #[cfg(test)]
    async fn subscribe(&self, cell: CellId, subscriber: &CellSubscriber) {
        let mut subs = self.subscribers.lock().await;
        let entry = subs.entry(cell).or_default();
        if entry
            .iter()
            .any(|existing| existing.client_id == subscriber.client_id)
        {
            return;
        }
        entry.push(subscriber.clone());
    }

    async fn snapshot_and_subscribe(
        &self,
        cell: CellId,
        actor: EntityId,
        pos: Position,
        subscriber: &CellSubscriber,
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

        if subscriber
            .tx
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
        if !entry
            .iter()
            .any(|existing| existing.client_id == subscriber.client_id)
        {
            entry.push(subscriber.clone());
        }

        Ok(inserted_new)
    }

    async fn subscribe_with_snapshot(
        &self,
        cell: CellId,
        subscriber: &CellSubscriber,
        epoch: u32,
    ) -> Result<bool, ()> {
        let actors = self.actors.lock().await;
        let mut subs = self.subscribers.lock().await;
        let snapshot = actors
            .get(&cell)
            .map(|cell_actors| {
                cell_actors
                    .iter()
                    .map(|(id, pos)| ActorState { id: *id, pos: *pos })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if snapshot.is_empty() {
            let entry = subs.entry(cell).or_default();
            let already_subscribed = entry
                .iter()
                .any(|existing| existing.client_id == subscriber.client_id);
            if !already_subscribed {
                entry.push(subscriber.clone());
            }
            return Ok(!already_subscribed);
        }

        if subscriber
            .tx
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

        let entry = subs.entry(cell).or_default();
        let already_subscribed = entry
            .iter()
            .any(|existing| existing.client_id == subscriber.client_id);
        if !already_subscribed {
            entry.push(subscriber.clone());
        }

        Ok(!already_subscribed)
    }

    async fn relay_subscribe_with_snapshot(
        &self,
        cell: CellId,
        subscriber: &RelaySubscriber,
        epoch: u32,
    ) -> Result<bool, ()> {
        let actors = self.actors.lock().await;
        let mut subs = self.relay_subscribers.lock().await;
        let snapshot = actors
            .get(&cell)
            .map(|cell_actors| {
                cell_actors
                    .iter()
                    .map(|(id, pos)| ActorState { id: *id, pos: *pos })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if !snapshot.is_empty()
            && subscriber
                .tx
                .try_send((
                    cell,
                    Some(epoch),
                    WorkerRelayMsg::Snapshot {
                        cell,
                        actors: snapshot,
                    },
                ))
                .is_err()
        {
            return Err(());
        }

        let entry = subs.entry(cell).or_default();
        let already_subscribed = entry
            .iter()
            .any(|existing| existing.connection_id == subscriber.connection_id);
        if !already_subscribed {
            entry.push(subscriber.clone());
        }

        Ok(!already_subscribed)
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
        {
            let mut sessions = self.client_sessions.lock().await;
            sessions.remove(&client_id);
        }
        {
            let mut remote_interests = self.remote_interests.lock().await;
            remote_interests.remove(&client_id);
        }
        let mut subs = self.subscribers.lock().await;
        subs.values_mut().for_each(|entries| {
            entries.retain(|subscriber| subscriber.client_id != client_id);
        });
        subs.retain(|_, entries| !entries.is_empty());

        self.remove_remote_peer_sessions(client_id).await;
    }

    async fn remove_subscriptions_for_cells(&self, client_id: u64, cells: &HashSet<CellId>) {
        if cells.is_empty() {
            return;
        }

        let mut subs = self.subscribers.lock().await;
        for cell in cells {
            let mut remove_cell = false;
            if let Some(entries) = subs.get_mut(cell) {
                entries.retain(|subscriber| subscriber.client_id != client_id);
                remove_cell = entries.is_empty();
            }
            if remove_cell {
                subs.remove(cell);
            }
        }
    }

    async fn remove_relay_subscriptions(&self, connection_id: u64) {
        let mut subs = self.relay_subscribers.lock().await;
        subs.values_mut().for_each(|entries| {
            entries.retain(|subscriber| subscriber.connection_id != connection_id);
        });
        subs.retain(|_, entries| !entries.is_empty());
    }

    async fn remove_relay_subscriptions_for_cells(
        &self,
        connection_id: u64,
        cells: &HashSet<CellId>,
    ) {
        if cells.is_empty() {
            return;
        }

        let mut subs = self.relay_subscribers.lock().await;
        for cell in cells {
            let mut remove_cell = false;
            if let Some(entries) = subs.get_mut(cell) {
                entries.retain(|subscriber| subscriber.connection_id != connection_id);
                remove_cell = entries.is_empty();
            }
            if remove_cell {
                subs.remove(cell);
            }
        }
    }

    async fn upsert_client_root(
        &self,
        subscriber: &CellSubscriber,
        cell: CellId,
        actor: EntityId,
        epoch: u32,
    ) {
        let mut sessions = self.client_sessions.lock().await;
        let entry = sessions
            .entry(subscriber.client_id)
            .or_insert_with(|| ClientSession {
                subscriber: subscriber.clone(),
                root_actors: HashSet::new(),
                last_epoch: epoch,
            });
        entry.subscriber = subscriber.clone();
        entry.root_actors.insert((cell, actor));
        entry.last_epoch = epoch;
    }

    async fn touch_client_epoch(&self, subscriber: &CellSubscriber, epoch: u32) {
        let mut sessions = self.client_sessions.lock().await;
        if let Some(entry) = sessions.get_mut(&subscriber.client_id) {
            entry.subscriber = subscriber.clone();
            entry.last_epoch = epoch;
        }
    }

    async fn prune_client_roots(&self, removed_cells: &HashSet<CellId>) {
        if removed_cells.is_empty() {
            return;
        }

        let removed_clients = {
            let mut sessions = self.client_sessions.lock().await;
            let mut removed_clients = HashSet::new();
            sessions.retain(|client_id, session| {
                session
                    .root_actors
                    .retain(|(cell, _)| !removed_cells.contains(cell));
                let keep = !session.root_actors.is_empty();
                if !keep {
                    removed_clients.insert(*client_id);
                }
                keep
            });
            removed_clients
        };

        if removed_clients.is_empty() {
            return;
        }

        {
            let mut remote_interests = self.remote_interests.lock().await;
            for client_id in &removed_clients {
                remote_interests.remove(client_id);
            }
        }

        for client_id in &removed_clients {
            self.remove_remote_peer_sessions(*client_id).await;
        }

        let mut subs = self.subscribers.lock().await;
        subs.values_mut().for_each(|entries| {
            entries.retain(|subscriber| !removed_clients.contains(&subscriber.client_id));
        });
        subs.retain(|_, entries| !entries.is_empty());
    }

    async fn actor_positions(&self, actors: &OwnedActors) -> Vec<(CellId, Position)> {
        let actor_map = self.actors.lock().await;
        actors
            .iter()
            .filter_map(|(cell, actor)| {
                actor_map
                    .get(cell)
                    .and_then(|cell_actors| cell_actors.get(actor))
                    .copied()
                    .map(|pos| (*cell, pos))
            })
            .collect::<Vec<_>>()
    }

    async fn update_peer_routes(&self, routes: PeerRoutes) -> bool {
        let mut peer_routes = self.peer_routes.write().await;
        if *peer_routes == routes {
            return false;
        }
        *peer_routes = routes;
        true
    }

    async fn sync_remote_interests(
        self: &Arc<Self>,
        subscriber: &CellSubscriber,
        client_id: u64,
        desired_targets: &HashSet<CellId>,
        owned_cells: &HashSet<CellId>,
        epoch: u32,
    ) -> bool {
        let desired_remote = {
            let peer_routes = self.peer_routes.read().await;
            desired_targets
                .iter()
                .filter(|cell| !owned_cells.contains(cell))
                .filter_map(|cell| peer_routes.get(cell).cloned().map(|route| (*cell, route)))
                .collect::<HashMap<_, _>>()
        };

        let mut remote_interests = self.remote_interests.lock().await;
        let changed = remote_interests.get(&client_id) != Some(&desired_remote);
        let desired_remote_for_relay = desired_remote.clone();
        if desired_remote.is_empty() {
            remote_interests.remove(&client_id);
        } else {
            remote_interests.insert(client_id, desired_remote);
        }
        drop(remote_interests);

        self.sync_remote_peer_sessions(
            subscriber.clone(),
            client_id,
            desired_remote_for_relay,
            epoch,
        )
        .await;
        changed
    }

    #[cfg(test)]
    async fn remote_interest_cells(&self, client_id: u64) -> HashSet<CellId> {
        let remote_interests = self.remote_interests.lock().await;
        remote_interests
            .get(&client_id)
            .map(|cells| cells.keys().copied().collect::<HashSet<_>>())
            .unwrap_or_default()
    }

    async fn client_subscribed_cells(&self, client_id: u64) -> HashSet<CellId> {
        let subs = self.subscribers.lock().await;
        subs.iter()
            .filter_map(|(cell, entries)| {
                entries
                    .iter()
                    .any(|subscriber| subscriber.client_id == client_id)
                    .then_some(*cell)
            })
            .collect::<HashSet<_>>()
    }

    async fn remove_remote_peer_sessions(&self, client_id: u64) {
        let mut sessions = self.remote_peer_sessions.lock().await;
        sessions.remove(&client_id);
    }

    async fn sync_remote_peer_sessions(
        self: &Arc<Self>,
        subscriber: CellSubscriber,
        client_id: u64,
        desired_remote: HashMap<CellId, PeerRoute>,
        epoch: u32,
    ) {
        let mut desired_by_route: HashMap<String, (PeerRoute, HashSet<CellId>)> = HashMap::new();
        for (cell, route) in desired_remote {
            let key = peer_route_key(&route);
            let (_, cells) = desired_by_route
                .entry(key)
                .or_insert_with(|| (route, HashSet::new()));
            cells.insert(cell);
        }

        let mut sessions = self.remote_peer_sessions.lock().await;
        let client_sessions = sessions.entry(client_id).or_default();

        client_sessions.retain(|route_key, handle| {
            if let Some((_, cells)) = desired_by_route.get(route_key) {
                handle
                    .desired_tx
                    .send(RemotePeerDesired {
                        cells: cells.clone(),
                        epoch,
                    })
                    .is_ok()
            } else {
                false
            }
        });

        for (route_key, (route, cells)) in desired_by_route {
            if client_sessions.contains_key(&route_key) {
                continue;
            }
            let desired = RemotePeerDesired { cells, epoch };
            let (desired_tx, desired_rx) = watch::channel(desired);
            tokio::spawn(remote_peer_session_loop(
                Arc::clone(self),
                subscriber.clone(),
                route,
                desired_rx,
            ));
            client_sessions.insert(route_key, RemotePeerSessionHandle { desired_tx });
        }

        if client_sessions.is_empty() {
            sessions.remove(&client_id);
        }
    }

    async fn resync_client_interests(self: &Arc<Self>, owned_cells: &HashSet<CellId>) {
        let sessions = {
            let sessions = self.client_sessions.lock().await;
            sessions
                .iter()
                .map(|(client_id, session)| {
                    (
                        *client_id,
                        session.subscriber.clone(),
                        session.root_actors.clone(),
                        session.last_epoch,
                    )
                })
                .collect::<Vec<_>>()
        };

        for (client_id, subscriber, root_actors, epoch) in sessions {
            let root_positions = self.actor_positions(&root_actors).await;
            let desired_targets = desired_aoi_targets_for_positions(
                &root_positions,
                self.aoi_radius_cells,
                self.aoi_cell_span_units,
                self.aoi_edge_margin_units,
            );
            let desired_cells = desired_targets
                .intersection(owned_cells)
                .copied()
                .collect::<HashSet<_>>();
            let mut subscribed_cells = self.client_subscribed_cells(client_id).await;
            if sync_interest_cells(
                self,
                &subscriber,
                &mut subscribed_cells,
                &desired_cells,
                epoch,
            )
            .await
            .is_err()
            {
                self.remove_client_subscriptions(client_id).await;
                continue;
            }
            if self
                .sync_remote_interests(&subscriber, client_id, &desired_targets, owned_cells, epoch)
                .await
            {
                info!(
                    target: "worker",
                    client_id,
                    "remote AOI interests updated"
                );
            }
        }
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
            entries.retain(|subscriber| {
                if exclude.is_some_and(|excluded| excluded == subscriber.client_id) {
                    return !subscriber.tx.is_closed();
                }
                match subscriber.tx.try_send((cell, epoch_override, msg.clone())) {
                    Ok(()) => true,
                    Err(TrySendError::Full(_)) => {
                        warn!(
                            target: "worker",
                            cell = ?cell,
                            client_id = subscriber.client_id,
                            "outbound channel full; disconnecting slow subscriber"
                        );
                        let _ = subscriber.disconnect_tx.send(());
                        false
                    }
                    Err(TrySendError::Closed(_)) => false,
                }
            });
            if entries.is_empty() {
                subs.remove(&cell);
            }
        }
    }

    async fn broadcast_relay(
        &self,
        cell: CellId,
        msg: WorkerRelayMsg,
        epoch_override: Option<u32>,
    ) {
        let mut subs = self.relay_subscribers.lock().await;
        if let Some(entries) = subs.get_mut(&cell) {
            entries.retain(|subscriber| {
                match subscriber.tx.try_send((cell, epoch_override, msg.clone())) {
                    Ok(()) => true,
                    Err(TrySendError::Full(_)) => {
                        warn!(
                            target: "worker",
                            cell = ?cell,
                            connection_id = subscriber.connection_id,
                            "relay outbound channel full; disconnecting slow peer"
                        );
                        let _ = subscriber.disconnect_tx.send(());
                        false
                    }
                    Err(TrySendError::Closed(_)) => false,
                }
            });
            if entries.is_empty() {
                subs.remove(&cell);
            }
        }
    }

    async fn enqueue_move_broadcast(
        &self,
        cell: CellId,
        actor: EntityId,
        exclude_client_id: u64,
        epoch: u32,
    ) {
        let mut pending = self.pending_moves.lock().await;
        pending.entry(cell).or_default().insert(
            actor,
            PendingMoveBroadcast {
                exclude_client_id,
                epoch,
            },
        );
    }

    async fn drain_move_broadcasts(&self) -> PendingMoveQueues {
        let mut pending = self.pending_moves.lock().await;
        std::mem::take(&mut *pending)
    }

    async fn drop_pending_moves_for_actors(
        &self,
        removed_by_cell: &HashMap<CellId, Vec<EntityId>>,
    ) {
        let mut pending = self.pending_moves.lock().await;
        pending.retain(|cell, queued| {
            if let Some(removed) = removed_by_cell.get(cell) {
                for entity in removed {
                    queued.remove(entity);
                }
            }
            !queued.is_empty()
        });
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

        self.drop_pending_moves_for_actors(&removed_by_cell).await;

        // 소유권 맵을 잠근 상태에서 despawn을 전파해 재조인 경쟁으로 인한 순서 역전을 막는다.
        for (cell, actors) in removed_by_cell {
            if !actors.is_empty() {
                let relay_actors = actors.clone();
                self.broadcast(
                    cell,
                    ServerMsg::Despawn { cell, actors },
                    None,
                    epoch_override,
                )
                .await;
                self.broadcast_relay(
                    cell,
                    WorkerRelayMsg::Despawn {
                        cell,
                        actors: relay_actors,
                    },
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
            let relay_actors = actors.clone();
            self.broadcast(cell, ServerMsg::Despawn { cell, actors }, None, None)
                .await;
            self.broadcast_relay(
                cell,
                WorkerRelayMsg::Despawn {
                    cell,
                    actors: relay_actors,
                },
                None,
            )
            .await;
        }
        {
            let mut pending = self.pending_moves.lock().await;
            pending.retain(|cell, _| !cells.contains(cell));
        }
        let mut subs = self.subscribers.lock().await;
        subs.retain(|cell, _| !cells.contains(cell));
        drop(subs);
        let mut relay_subs = self.relay_subscribers.lock().await;
        relay_subs.retain(|cell, _| !cells.contains(cell));
        drop(relay_subs);

        self.prune_client_roots(cells).await;
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

impl Default for SharedState {
    fn default() -> Self {
        Self::new(DEFAULT_AOI_RADIUS_CELLS)
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
    let aoi_radius_cells = load_aoi_radius_cells().context("load worker AOI radius")?;
    let aoi_cell_span_units = load_aoi_cell_span_units().context("load worker AOI cell span")?;
    let aoi_edge_margin_units =
        load_aoi_edge_margin_units().context("load worker AOI edge margin")?;

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

    let state = Arc::new(SharedState::with_aoi_config(
        aoi_radius_cells,
        aoi_cell_span_units,
        aoi_edge_margin_units,
    ));
    match fetch_assignment_listing().await {
        Ok(listing) => {
            if let Err(e) =
                apply_peer_routes_update(&state, &owned_cells, &worker_id, listing).await
            {
                warn!(
                    target: "worker",
                    worker_id = worker_id.as_str(),
                    error = ?e,
                    "failed to apply initial peer routes"
                );
            }
        }
        Err(e) => {
            warn!(
                target: "worker",
                worker_id = worker_id.as_str(),
                error = ?e,
                "failed to fetch initial peer routes"
            );
        }
    }
    let server_state = state.clone();
    let owned_for_server = owned_cells.clone();
    let mut server = tokio::spawn(run_server(addr, server_state, owned_for_server));
    tokio::spawn(peer_routes_watch_loop(
        state.clone(),
        owned_cells.clone(),
        worker_id.clone(),
    ));
    tokio::spawn(peer_routes_refresh_loop(
        state.clone(),
        owned_cells.clone(),
        worker_id.clone(),
    ));

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
                on_tick(tick, state.as_ref()).await;
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
    let endpoint = orchestrator_endpoint();
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

async fn fetch_assignment_listing() -> Result<AssignmentListing> {
    let endpoint = orchestrator_endpoint();
    let mut client = OrchestratorClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connect orchestrator at {}", endpoint))?;
    let response = client
        .list_assignments(ListAssignmentsRequest {})
        .await
        .context("list assignments")?
        .into_inner();
    Ok(response)
}

async fn watch_assignment_listing_once(
    state: Arc<SharedState>,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
    worker_id: String,
) -> Result<()> {
    let endpoint = orchestrator_endpoint();
    let mut client = OrchestratorClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connect orchestrator at {}", endpoint))?;
    let response = client
        .watch_assignments(WatchAssignmentsRequest {})
        .await
        .context("subscribe to watch_assignments")?;
    let mut stream = response.into_inner();
    while let Some(listing) = stream
        .message()
        .await
        .context("receive assignment listing from watch stream")?
    {
        apply_peer_routes_update(&state, &owned_cells, &worker_id, listing).await?;
    }
    Ok(())
}

async fn peer_routes_watch_loop(
    state: Arc<SharedState>,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
    worker_id: String,
) {
    loop {
        match watch_assignment_listing_once(
            Arc::clone(&state),
            Arc::clone(&owned_cells),
            worker_id.clone(),
        )
        .await
        {
            Ok(()) => {
                warn!(
                    target: "worker",
                    worker_id = worker_id.as_str(),
                    "assignment listing watch ended; reconnecting after backoff"
                );
            }
            Err(e) => {
                warn!(
                    target: "worker",
                    worker_id = worker_id.as_str(),
                    error = ?e,
                    "assignment listing watch failed; reconnecting after backoff"
                );
            }
        }
        time::sleep(Duration::from_secs(1)).await;
    }
}

async fn peer_routes_refresh_loop(
    state: Arc<SharedState>,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
    worker_id: String,
) {
    let mut ticker = time::interval(assignment_retry_interval());
    loop {
        ticker.tick().await;
        match fetch_assignment_listing().await {
            Ok(listing) => {
                if let Err(e) =
                    apply_peer_routes_update(&state, &owned_cells, &worker_id, listing).await
                {
                    warn!(
                        target: "worker",
                        worker_id = worker_id.as_str(),
                        error = ?e,
                        "failed to refresh peer routes from listing"
                    );
                }
            }
            Err(e) => {
                warn!(
                    target: "worker",
                    worker_id = worker_id.as_str(),
                    error = ?e,
                    "failed to refresh peer routes; keeping existing routes"
                );
            }
        }
    }
}

async fn apply_peer_routes_update(
    state: &Arc<SharedState>,
    owned_cells: &Arc<RwLock<HashSet<CellId>>>,
    worker_id: &str,
    listing: AssignmentListing,
) -> Result<bool> {
    let routes = peer_routes_from_listing(&listing, worker_id)?;
    let route_count = routes.len();
    let changed = state.update_peer_routes(routes).await;
    if changed {
        let owned_snapshot = owned_cells.read().await.clone();
        state.resync_client_interests(&owned_snapshot).await;
        if route_count == 0 {
            warn!(
                target: "worker",
                worker_id,
                "peer route table updated; no remote cells available"
            );
        } else {
            info!(
                target: "worker",
                worker_id,
                remote_cells = route_count,
                "peer route table updated from orchestrator listing"
            );
        }
    }
    Ok(changed)
}

fn peer_routes_from_listing(
    listing: &AssignmentListing,
    self_worker_id: &str,
) -> Result<PeerRoutes> {
    let mut routes = HashMap::new();
    for bundle in &listing.workers {
        if bundle.worker_id == self_worker_id {
            continue;
        }
        let addr = bundle.addr.trim();
        if addr.is_empty() {
            return Err(anyhow!("worker addr empty for {}", bundle.worker_id));
        }
        for assignment in &bundle.cells {
            let cell = assignment_to_cell_ref(assignment)?;
            let prev = routes.insert(
                cell,
                PeerRoute {
                    worker_id: bundle.worker_id.clone(),
                    addr: addr.to_string(),
                },
            );
            if let Some(prev) = prev {
                return Err(anyhow!(
                    "duplicate remote route for {:?}: {} and {}",
                    cell,
                    prev.worker_id,
                    bundle.worker_id
                ));
            }
        }
    }
    Ok(routes)
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

fn assignment_to_cell(assignment: Assignment) -> Result<CellId> {
    assignment_to_cell_parts(
        assignment.world,
        assignment.cx,
        assignment.cy,
        assignment.depth,
        assignment.sub,
    )
}

fn assignment_to_cell_ref(assignment: &Assignment) -> Result<CellId> {
    assignment_to_cell_parts(
        assignment.world,
        assignment.cx,
        assignment.cy,
        assignment.depth,
        assignment.sub,
    )
}

fn assignment_to_cell_parts(world: u32, cx: i32, cy: i32, depth: u32, sub: u32) -> Result<CellId> {
    let depth =
        u8::try_from(depth).map_err(|_| anyhow!("assignment depth {} out of range", depth))?;
    let sub = u8::try_from(sub).map_err(|_| anyhow!("assignment sub {} out of range", sub))?;
    Ok(CellId {
        world,
        cx,
        cy,
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

fn load_aoi_radius_cells() -> Result<i32> {
    let raw = std::env::var("TESSERA_WORKER_AOI_RADIUS_CELLS").ok();
    parse_aoi_radius_cells(raw.as_deref())
}

fn load_aoi_cell_span_units() -> Result<f32> {
    let raw = std::env::var("TESSERA_WORKER_AOI_CELL_SPAN_UNITS").ok();
    parse_aoi_cell_span_units(raw.as_deref())
}

fn load_aoi_edge_margin_units() -> Result<Option<f32>> {
    let raw = std::env::var("TESSERA_WORKER_AOI_EDGE_MARGIN_UNITS").ok();
    parse_aoi_edge_margin_units(raw.as_deref())
}

fn parse_aoi_radius_cells(raw: Option<&str>) -> Result<i32> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_AOI_RADIUS_CELLS);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_AOI_RADIUS_CELLS);
    }

    let radius = trimmed
        .parse::<i32>()
        .with_context(|| format!("invalid AOI radius `{trimmed}`"))?;
    if radius < 0 {
        return Err(anyhow!("AOI radius must be >= 0"));
    }
    Ok(radius)
}

fn parse_aoi_cell_span_units(raw: Option<&str>) -> Result<f32> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_AOI_CELL_SPAN_UNITS);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_AOI_CELL_SPAN_UNITS);
    }

    let span = trimmed
        .parse::<f32>()
        .with_context(|| format!("invalid AOI cell span `{trimmed}`"))?;
    if !span.is_finite() {
        return Err(anyhow!("AOI cell span must be finite"));
    }
    if span <= 0.0 {
        return Err(anyhow!("AOI cell span must be > 0"));
    }
    Ok(span)
}

fn parse_aoi_edge_margin_units(raw: Option<&str>) -> Result<Option<f32>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let margin = trimmed
        .parse::<f32>()
        .with_context(|| format!("invalid AOI edge margin `{trimmed}`"))?;
    if !margin.is_finite() {
        return Err(anyhow!("AOI edge margin must be finite"));
    }
    if margin < 0.0 {
        return Err(anyhow!("AOI edge margin must be >= 0"));
    }
    Ok(Some(margin))
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
                let (changed, removed) = {
                    let mut guard = owned_cells.write().await;
                    let removed = guard.difference(&updated).copied().collect::<HashSet<_>>();
                    let changed = *guard != updated;
                    if changed {
                        *guard = updated.clone();
                    }
                    (changed, removed)
                };
                if !removed.is_empty() {
                    state.drop_cells(&removed).await;
                }
                if changed {
                    state.resync_client_interests(&updated).await;
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

async fn on_tick(tick: Tick, state: &SharedState) {
    let pending = state.drain_move_broadcasts().await;
    let queued_cells = pending.len();
    let queued_moves = pending.values().map(HashMap::len).sum::<usize>();

    for (cell, queued) in pending {
        let queued_events = {
            let actors = state.actors.lock().await;
            let Some(cell_actors) = actors.get(&cell) else {
                continue;
            };
            queued
                .into_iter()
                .filter_map(|(actor_id, event)| {
                    cell_actors.get(&actor_id).map(|pos| {
                        (
                            ActorState {
                                id: actor_id,
                                pos: *pos,
                            },
                            event,
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (actor, event) in queued_events {
            let relay_actor = actor.clone();
            state
                .broadcast(
                    cell,
                    ServerMsg::Delta {
                        cell,
                        moved: vec![actor],
                    },
                    Some(event.exclude_client_id),
                    Some(event.epoch),
                )
                .await;
            state
                .broadcast_relay(
                    cell,
                    WorkerRelayMsg::Delta {
                        cell,
                        moved: vec![relay_actor],
                    },
                    Some(event.epoch),
                )
                .await;
        }
    }

    if tick.0 % 30 == 0 {
        info!(
            target: "worker",
            tick = tick.0,
            queued_cells,
            queued_moves,
            "tick heartbeat"
        );
    }
}

fn position_is_finite(pos: &Position) -> bool {
    pos.x.is_finite() && pos.y.is_finite()
}

fn delta_is_finite(dx: f32, dy: f32) -> bool {
    dx.is_finite() && dy.is_finite()
}

fn aoi_cells_for(cell: CellId, radius_cells: i32) -> Vec<CellId> {
    let mut visible = Vec::with_capacity(((radius_cells * 2 + 1).pow(2)) as usize);
    for dy in -radius_cells..=radius_cells {
        for dx in -radius_cells..=radius_cells {
            visible.push(CellId {
                world: cell.world,
                cx: cell.cx + dx,
                cy: cell.cy + dy,
                depth: cell.depth,
                sub: cell.sub,
            });
        }
    }
    visible
}

fn aoi_cells_for_position(
    cell: CellId,
    pos: Position,
    radius_cells: i32,
    cell_span_units: f32,
    edge_margin_units: Option<f32>,
) -> Vec<CellId> {
    let Some(edge_margin_units) = edge_margin_units else {
        return aoi_cells_for(cell, radius_cells);
    };
    // 현재 프로토콜의 위치는 셀 로컬 좌표로 보고 `0..cell_span_units` 경계를 기준으로
    // 인접 방향 셀 구독 여부를 결정한다.
    let edge_margin_units = edge_margin_units.min(cell_span_units);
    let west = pos.x <= edge_margin_units;
    let east = pos.x >= cell_span_units - edge_margin_units;
    let south = pos.y <= edge_margin_units;
    let north = pos.y >= cell_span_units - edge_margin_units;

    let mut visible = Vec::with_capacity(((radius_cells * 2 + 1).pow(2)) as usize);
    for dy in -radius_cells..=radius_cells {
        if (dy < 0 && !south) || (dy > 0 && !north) {
            continue;
        }
        for dx in -radius_cells..=radius_cells {
            if (dx < 0 && !west) || (dx > 0 && !east) {
                continue;
            }
            visible.push(CellId {
                world: cell.world,
                cx: cell.cx + dx,
                cy: cell.cy + dy,
                depth: cell.depth,
                sub: cell.sub,
            });
        }
    }
    visible
}

#[cfg(test)]
fn desired_aoi_cells_for_positions(
    root_positions: &[(CellId, Position)],
    owned_cells: &HashSet<CellId>,
    radius_cells: i32,
    cell_span_units: f32,
    edge_margin_units: Option<f32>,
) -> HashSet<CellId> {
    let mut desired = HashSet::new();
    for (cell, pos) in root_positions {
        for visible in aoi_cells_for_position(
            *cell,
            *pos,
            radius_cells,
            cell_span_units,
            edge_margin_units,
        ) {
            if owned_cells.contains(&visible) {
                desired.insert(visible);
            }
        }
    }
    desired
}

fn desired_aoi_targets_for_positions(
    root_positions: &[(CellId, Position)],
    radius_cells: i32,
    cell_span_units: f32,
    edge_margin_units: Option<f32>,
) -> HashSet<CellId> {
    let mut desired = HashSet::new();
    for (cell, pos) in root_positions {
        desired.extend(aoi_cells_for_position(
            *cell,
            *pos,
            radius_cells,
            cell_span_units,
            edge_margin_units,
        ));
    }
    desired
}

#[cfg(test)]
fn desired_aoi_cells(
    owned_actor_positions: &[(CellId, Position)],
    owned_cells: &HashSet<CellId>,
    radius_cells: i32,
    cell_span_units: f32,
    edge_margin_units: Option<f32>,
) -> HashSet<CellId> {
    desired_aoi_cells_for_positions(
        owned_actor_positions,
        owned_cells,
        radius_cells,
        cell_span_units,
        edge_margin_units,
    )
}

async fn sync_interest_cells(
    state: &SharedState,
    subscriber: &CellSubscriber,
    subscribed_cells: &mut HashSet<CellId>,
    desired_cells: &HashSet<CellId>,
    epoch: u32,
) -> Result<(), ()> {
    let mut to_unsubscribe = subscribed_cells
        .difference(desired_cells)
        .copied()
        .collect::<Vec<_>>();
    to_unsubscribe.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
    let to_unsubscribe_set = to_unsubscribe.iter().copied().collect::<HashSet<_>>();
    state
        .remove_subscriptions_for_cells(subscriber.client_id, &to_unsubscribe_set)
        .await;
    for cell in &to_unsubscribe {
        subscribed_cells.remove(cell);
    }

    let mut to_subscribe = desired_cells
        .difference(subscribed_cells)
        .copied()
        .collect::<Vec<_>>();
    to_subscribe.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
    for cell in to_subscribe {
        state
            .subscribe_with_snapshot(cell, subscriber, epoch)
            .await?;
        subscribed_cells.insert(cell);
    }

    Ok(())
}

async fn sync_relay_interest_cells(
    state: &SharedState,
    subscriber: &RelaySubscriber,
    subscribed_cells: &mut HashSet<CellId>,
    desired_cells: &HashSet<CellId>,
    epoch: u32,
) -> Result<(), ()> {
    let mut to_unsubscribe = subscribed_cells
        .difference(desired_cells)
        .copied()
        .collect::<Vec<_>>();
    to_unsubscribe.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
    let to_unsubscribe_set = to_unsubscribe.iter().copied().collect::<HashSet<_>>();
    state
        .remove_relay_subscriptions_for_cells(subscriber.connection_id, &to_unsubscribe_set)
        .await;
    for cell in &to_unsubscribe {
        subscribed_cells.remove(cell);
    }

    let mut to_subscribe = desired_cells
        .difference(subscribed_cells)
        .copied()
        .collect::<Vec<_>>();
    to_subscribe.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
    for cell in to_subscribe {
        state
            .relay_subscribe_with_snapshot(cell, subscriber, epoch)
            .await?;
        subscribed_cells.insert(cell);
    }

    Ok(())
}

fn peer_route_key(route: &PeerRoute) -> String {
    format!("{}@{}", route.worker_id, route.addr)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadExactOutcome {
    Read,
    DisconnectRequested,
}

async fn read_exact_or_disconnect(
    reader: &mut OwnedReadHalf,
    buf: &mut [u8],
    disconnect_rx: &mut watch::Receiver<()>,
) -> std::io::Result<ReadExactOutcome> {
    tokio::select! {
        biased;
        changed = disconnect_rx.changed() => {
            match changed {
                Ok(()) | Err(_) => Ok(ReadExactOutcome::DisconnectRequested),
            }
        }
        read = reader.read_exact(buf) => read.map(|_| ReadExactOutcome::Read),
    }
}

async fn read_frame_bytes(reader: &mut OwnedReadHalf) -> Result<Option<BytesMut>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(anyhow!(
            "frame length {} exceeds max {}",
            len,
            MAX_FRAME_LEN
        ));
    }

    let mut payload = vec![0u8; len];
    match reader.read_exact(&mut payload).await {
        Ok(_) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    let mut buf = BytesMut::with_capacity(4 + len);
    buf.extend_from_slice(&len_buf);
    buf.extend_from_slice(&payload);
    Ok(Some(buf))
}

async fn read_frame_bytes_or_disconnect(
    reader: &mut OwnedReadHalf,
    disconnect_rx: &mut watch::Receiver<()>,
) -> Result<Option<BytesMut>> {
    let mut len_buf = [0u8; 4];
    match read_exact_or_disconnect(reader, &mut len_buf, disconnect_rx).await {
        Ok(ReadExactOutcome::DisconnectRequested) => return Ok(None),
        Ok(ReadExactOutcome::Read) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(anyhow!(
            "frame length {} exceeds max {}",
            len,
            MAX_FRAME_LEN
        ));
    }

    let mut payload = vec![0u8; len];
    match read_exact_or_disconnect(reader, &mut payload, disconnect_rx).await {
        Ok(ReadExactOutcome::DisconnectRequested) => return Ok(None),
        Ok(ReadExactOutcome::Read) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    let mut buf = BytesMut::with_capacity(4 + len);
    buf.extend_from_slice(&len_buf);
    buf.extend_from_slice(&payload);
    Ok(Some(buf))
}

fn decode_client_frame_from_bytes(frame: &BytesMut) -> Result<Envelope<ClientMsg>> {
    let mut buf = frame.clone();
    match try_decode_frame::<Envelope<ClientMsg>>(&mut buf) {
        Ok(Some(env)) => Ok(env),
        Ok(None) => Err(anyhow!("incomplete client frame")),
        Err(e) => Err(e.into()),
    }
}

fn decode_worker_relay_frame_from_bytes(frame: &BytesMut) -> Result<Envelope<WorkerRelayMsg>> {
    let mut buf = frame.clone();
    match try_decode_frame::<Envelope<WorkerRelayMsg>>(&mut buf) {
        Ok(Some(env)) => Ok(env),
        Ok(None) => Err(anyhow!("incomplete worker relay frame")),
        Err(e) => Err(e.into()),
    }
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
            if let Err(e) = handle_incoming(sock, peer, st, owned).await {
                error!(target: "worker", %peer, error = ?e, "upstream connection error");
            }
        });
    }
}

async fn handle_incoming(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<SharedState>,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
) -> Result<()> {
    let (mut reader, writer) = stream.into_split();
    let Some(first_frame) = read_frame_bytes(&mut reader).await? else {
        info!(target: "worker", %peer, "upstream closed before first frame");
        return Ok(());
    };

    if let Ok(first_env) = decode_client_frame_from_bytes(&first_frame) {
        let (tx, rx) = channel::<OutboundMsg>(OUTBOUND_CHANNEL_CAPACITY);
        return handle_upstream_inner(
            reader,
            writer,
            peer,
            state,
            owned_cells,
            tx,
            rx,
            true,
            Some(first_env),
        )
        .await;
    }

    if let Ok(first_env) = decode_worker_relay_frame_from_bytes(&first_frame) {
        let (tx, rx) = channel::<RelayOutboundMsg>(OUTBOUND_CHANNEL_CAPACITY);
        return handle_worker_relay_inner(
            reader,
            writer,
            peer,
            state,
            owned_cells,
            tx,
            rx,
            true,
            Some(first_env),
        )
        .await;
    }

    warn!(
        target: "worker",
        %peer,
        "failed to decode first frame as client or worker relay; closing connection"
    );
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
async fn handle_upstream(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<SharedState>,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
) -> Result<()> {
    let (tx, rx) = channel::<OutboundMsg>(OUTBOUND_CHANNEL_CAPACITY);
    let (reader, writer) = stream.into_split();
    handle_upstream_inner(reader, writer, peer, state, owned_cells, tx, rx, true, None).await
}

async fn forward_worker_relay_to_client(
    subscriber: &CellSubscriber,
    env_in: Envelope<WorkerRelayMsg>,
) -> bool {
    let msg = match env_in.payload {
        WorkerRelayMsg::Snapshot { cell, actors } => ServerMsg::Snapshot { cell, actors },
        WorkerRelayMsg::Delta { cell, moved } => ServerMsg::Delta { cell, moved },
        WorkerRelayMsg::Despawn { cell, actors } => ServerMsg::Despawn { cell, actors },
        WorkerRelayMsg::Subscribe { .. } | WorkerRelayMsg::Unsubscribe { .. } => {
            return true;
        }
    };

    match subscriber
        .tx
        .try_send((env_in.cell, Some(env_in.epoch), msg))
    {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            warn!(
                target: "worker",
                client_id = subscriber.client_id,
                cell = ?env_in.cell,
                "relay forward channel full; disconnecting slow client"
            );
            let _ = subscriber.disconnect_tx.send(());
            false
        }
        Err(TrySendError::Closed(_)) => false,
    }
}

async fn send_relay_interest_delta(
    tx: &Sender<RelayOutboundMsg>,
    previous: &HashSet<CellId>,
    next: &HashSet<CellId>,
    epoch: u32,
) -> Result<()> {
    let mut to_unsubscribe = previous.difference(next).copied().collect::<Vec<_>>();
    to_unsubscribe.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
    if !to_unsubscribe.is_empty() {
        tx.send((
            to_unsubscribe[0],
            Some(epoch),
            WorkerRelayMsg::Unsubscribe {
                cells: to_unsubscribe,
            },
        ))
        .await
        .map_err(|_| anyhow!("relay writer closed before unsubscribe"))?;
    }

    let mut to_subscribe = next.difference(previous).copied().collect::<Vec<_>>();
    to_subscribe.sort_by_key(|cell| (cell.world, cell.cy, cell.cx, cell.depth, cell.sub));
    if !to_subscribe.is_empty() {
        tx.send((
            to_subscribe[0],
            Some(epoch),
            WorkerRelayMsg::Subscribe {
                cells: to_subscribe,
            },
        ))
        .await
        .map_err(|_| anyhow!("relay writer closed before subscribe"))?;
    }

    Ok(())
}

async fn remote_peer_session_loop(
    state: Arc<SharedState>,
    subscriber: CellSubscriber,
    route: PeerRoute,
    mut desired_rx: watch::Receiver<RemotePeerDesired>,
) {
    let mut desired = desired_rx.borrow().clone();

    loop {
        if desired.cells.is_empty() {
            match desired_rx.changed().await {
                Ok(()) => {
                    desired = desired_rx.borrow().clone();
                    continue;
                }
                Err(_) => break,
            }
        }

        match TcpStream::connect(route.addr.as_str()).await {
            Ok(stream) => {
                info!(
                    target: "worker",
                    client_id = subscriber.client_id,
                    peer_worker = route.worker_id.as_str(),
                    peer_addr = route.addr.as_str(),
                    "connected remote ghost relay peer"
                );
                if let Err(e) = run_remote_peer_session(
                    stream,
                    &subscriber,
                    &route,
                    &mut desired_rx,
                    &mut desired,
                )
                .await
                {
                    warn!(
                        target: "worker",
                        client_id = subscriber.client_id,
                        peer_worker = route.worker_id.as_str(),
                        peer_addr = route.addr.as_str(),
                        error = ?e,
                        "remote ghost relay session ended; reconnecting"
                    );
                }
            }
            Err(e) => {
                warn!(
                    target: "worker",
                    client_id = subscriber.client_id,
                    peer_worker = route.worker_id.as_str(),
                    peer_addr = route.addr.as_str(),
                    error = ?e,
                    "failed to connect remote ghost relay peer"
                );
            }
        }

        let removed = {
            let remote_interests = state.remote_interests.lock().await;
            !remote_interests.contains_key(&subscriber.client_id)
        };
        if removed {
            break;
        }

        tokio::select! {
            _ = time::sleep(Duration::from_millis(250)) => {}
            changed = desired_rx.changed() => {
                match changed {
                    Ok(()) => desired = desired_rx.borrow().clone(),
                    Err(_) => break,
                }
            }
        }
    }
}

async fn run_remote_peer_session(
    stream: TcpStream,
    subscriber: &CellSubscriber,
    route: &PeerRoute,
    desired_rx: &mut watch::Receiver<RemotePeerDesired>,
    desired: &mut RemotePeerDesired,
) -> Result<()> {
    let (mut reader, writer) = stream.into_split();
    let (tx, mut rx) = channel::<RelayOutboundMsg>(OUTBOUND_CHANNEL_CAPACITY);

    let writer_task = tokio::spawn(async move {
        let mut seq_out: u64 = 0;
        let mut writer = writer;
        while let Some((cell, epoch_override, msg)) = rx.recv().await {
            let env_out = Envelope {
                cell,
                seq: seq_out,
                epoch: epoch_override.unwrap_or(0),
                payload: msg,
            };
            seq_out = seq_out.wrapping_add(1);
            let frame = encode_frame(&env_out);
            if let Err(e) = writer.write_all(&frame).await {
                return Err(anyhow!(e).context("write remote relay frame"));
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let mut subscribed_cells = HashSet::new();
    send_relay_interest_delta(&tx, &subscribed_cells, &desired.cells, desired.epoch).await?;
    subscribed_cells = desired.cells.clone();

    let result = loop {
        tokio::select! {
            frame = read_frame_bytes(&mut reader) => {
                let Some(frame) = frame? else {
                    break Ok(());
                };
                let env_in = decode_worker_relay_frame_from_bytes(&frame)
                    .context("decode worker relay frame from peer")?;
                if !subscribed_cells.contains(&env_in.cell) {
                    continue;
                }
                if !forward_worker_relay_to_client(subscriber, env_in).await {
                    break Ok(());
                }
            }
            changed = desired_rx.changed() => {
                match changed {
                    Ok(()) => {
                        let next = desired_rx.borrow().clone();
                        send_relay_interest_delta(&tx, &subscribed_cells, &next.cells, next.epoch).await?;
                        *desired = next.clone();
                        subscribed_cells = next.cells;
                        if subscribed_cells.is_empty() {
                            break Ok(());
                        }
                    }
                    Err(_) => break Ok(()),
                }
            }
        }
    };

    drop(tx);
    match writer_task.await {
        Ok(Ok(())) => result,
        Ok(Err(e)) => Err(e),
        Err(join_err) => Err(anyhow!(join_err).context(format!(
            "join remote relay writer task for {} / client {}",
            route.addr, subscriber.client_id
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_worker_relay_inner(
    mut reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    peer: SocketAddr,
    state: Arc<SharedState>,
    owned_cells: Arc<RwLock<HashSet<CellId>>>,
    tx: Sender<RelayOutboundMsg>,
    mut rx: Receiver<RelayOutboundMsg>,
    spawn_writer: bool,
    mut first_env: Option<Envelope<WorkerRelayMsg>>,
) -> Result<()> {
    let epoch = Arc::new(AtomicU32::new(0));
    let writer_epoch = Arc::clone(&epoch);
    let connection_id = NEXT_RELAY_SUBSCRIBER_ID.fetch_add(1, Ordering::Relaxed);
    let (disconnect_tx, mut disconnect_rx) = watch::channel(());
    let subscriber = RelaySubscriber {
        connection_id,
        tx: tx.clone(),
        disconnect_tx: disconnect_tx.clone(),
    };
    let mut subscribed_cells: HashSet<CellId> = HashSet::new();

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
                    error!(target: "worker", %peer, error = ?e, "relay write error");
                    break;
                }
            }
            info!(target: "worker", %peer, "relay writer task ended");
        });
    } else {
        drop(rx);
        drop(writer);
    }

    let result = loop {
        let env_in = if let Some(env_in) = first_env.take() {
            env_in
        } else {
            let Some(frame) =
                read_frame_bytes_or_disconnect(&mut reader, &mut disconnect_rx).await?
            else {
                break Ok(());
            };
            match decode_worker_relay_frame_from_bytes(&frame) {
                Ok(env_in) => env_in,
                Err(e) => {
                    warn!(
                        target: "worker",
                        %peer,
                        error = ?e,
                        "failed to decode worker relay frame; closing connection"
                    );
                    break Ok(());
                }
            }
        };

        epoch.store(env_in.epoch, Ordering::Relaxed);

        match env_in.payload {
            WorkerRelayMsg::Subscribe { cells } => {
                let owned_guard = owned_cells.read().await;
                let mut desired_cells = subscribed_cells.clone();
                for cell in cells {
                    if owned_guard.contains(&cell) {
                        desired_cells.insert(cell);
                    } else {
                        warn!(
                            target: "worker",
                            %peer,
                            cell = ?cell,
                            "relay subscribe requested for cell not owned by this worker"
                        );
                    }
                }
                if sync_relay_interest_cells(
                    state.as_ref(),
                    &subscriber,
                    &mut subscribed_cells,
                    &desired_cells,
                    env_in.epoch,
                )
                .await
                .is_err()
                {
                    break Ok(());
                }
            }
            WorkerRelayMsg::Unsubscribe { cells } => {
                let removed = cells.into_iter().collect::<HashSet<_>>();
                state
                    .remove_relay_subscriptions_for_cells(connection_id, &removed)
                    .await;
                for cell in removed {
                    subscribed_cells.remove(&cell);
                }
            }
            WorkerRelayMsg::Snapshot { .. }
            | WorkerRelayMsg::Delta { .. }
            | WorkerRelayMsg::Despawn { .. } => {
                warn!(
                    target: "worker",
                    %peer,
                    "unexpected worker relay payload from subscriber connection"
                );
            }
        }
    };

    state.remove_relay_subscriptions(connection_id).await;
    result
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
    mut first_env: Option<Envelope<ClientMsg>>,
) -> Result<()> {
    let epoch = Arc::new(AtomicU32::new(0));
    let writer_epoch = Arc::clone(&epoch);
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut owned_actors: HashSet<(CellId, EntityId)> = HashSet::new();
    let mut subscribed_cells: HashSet<CellId> = HashSet::new();
    let (disconnect_tx, mut disconnect_rx) = watch::channel(());
    let subscriber = CellSubscriber {
        client_id,
        tx: tx.clone(),
        disconnect_tx: disconnect_tx.clone(),
    };

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
        let env_in = if let Some(env_in) = first_env.take() {
            env_in
        } else {
            let Some(frame) =
                read_frame_bytes_or_disconnect(&mut reader, &mut disconnect_rx).await?
            else {
                info!(target: "worker", %peer, "upstream closed");
                break Ok(());
            };
            match decode_client_frame_from_bytes(&frame) {
                Ok(env_in) => env_in,
                Err(e) => {
                    warn!(
                        target: "worker",
                        %peer,
                        error = ?e,
                        "failed to decode client frame; closing connection"
                    );
                    break Ok(());
                }
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
                state.touch_client_epoch(&subscriber, env_in.epoch).await;
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
                    .snapshot_and_subscribe(cell, actor, pos, &subscriber, env_in.epoch)
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
                subscribed_cells.insert(cell);

                let owned_actor_positions = state.actor_positions(&owned_actors).await;
                let desired_targets = desired_aoi_targets_for_positions(
                    &owned_actor_positions,
                    state.aoi_radius_cells,
                    state.aoi_cell_span_units,
                    state.aoi_edge_margin_units,
                );
                let desired_cells = desired_targets
                    .intersection(&owned_guard)
                    .copied()
                    .collect::<HashSet<_>>();
                if sync_interest_cells(
                    state.as_ref(),
                    &subscriber,
                    &mut subscribed_cells,
                    &desired_cells,
                    env_in.epoch,
                )
                .await
                .is_err()
                {
                    warn!(
                        target: "worker",
                        %peer,
                        "client response channel closed while syncing AOI snapshots"
                    );
                    break Ok(());
                }
                state
                    .upsert_client_root(&subscriber, cell, actor, env_in.epoch)
                    .await;
                let _ = state
                    .sync_remote_interests(
                        &subscriber,
                        client_id,
                        &desired_targets,
                        &owned_guard,
                        env_in.epoch,
                    )
                    .await;

                let moved = ActorState { id: actor, pos };
                let relay_moved = moved.clone();
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
                state
                    .broadcast_relay(
                        cell,
                        WorkerRelayMsg::Delta {
                            cell,
                            moved: vec![relay_moved],
                        },
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
                    moved: vec![moved.clone()],
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
                    .enqueue_move_broadcast(cell, actor, client_id, env_in.epoch)
                    .await;
                if outbound_failed {
                    break Ok(());
                }
                state.touch_client_epoch(&subscriber, env_in.epoch).await;
                let owned_actor_positions = state.actor_positions(&owned_actors).await;
                let desired_targets = desired_aoi_targets_for_positions(
                    &owned_actor_positions,
                    state.aoi_radius_cells,
                    state.aoi_cell_span_units,
                    state.aoi_edge_margin_units,
                );
                let desired_cells = desired_targets
                    .intersection(&owned_guard)
                    .copied()
                    .collect::<HashSet<_>>();
                if sync_interest_cells(
                    state.as_ref(),
                    &subscriber,
                    &mut subscribed_cells,
                    &desired_cells,
                    env_in.epoch,
                )
                .await
                .is_err()
                {
                    warn!(
                        target: "worker",
                        %peer,
                        "client response channel closed while syncing AOI after move"
                    );
                    break Ok(());
                }
                let _ = state
                    .sync_remote_interests(
                        &subscriber,
                        client_id,
                        &desired_targets,
                        &owned_guard,
                        env_in.epoch,
                    )
                    .await;
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
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    fn test_subscriber(
        client_id: u64,
        tx: Sender<OutboundMsg>,
        disconnect_tx: &watch::Sender<()>,
    ) -> CellSubscriber {
        CellSubscriber {
            client_id,
            tx,
            disconnect_tx: disconnect_tx.clone(),
        }
    }

    fn spawn_test_tick_loop(state: Arc<SharedState>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = Tick(0);
            let mut interval = time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                tick.0 += 1;
                on_tick(tick, state.as_ref()).await;
            }
        })
    }

    fn spawn_incoming_test_server(
        listener: TcpListener,
        state: Arc<SharedState>,
        owned_cells: Arc<RwLock<HashSet<CellId>>>,
        accept_count: usize,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tasks = Vec::new();
            for _ in 0..accept_count {
                let (sock, peer) = listener.accept().await.expect("accept connection");
                let st = Arc::clone(&state);
                let owned = Arc::clone(&owned_cells);
                tasks.push(tokio::spawn(async move {
                    let _ = handle_incoming(sock, peer, st, owned).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        })
    }

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

    #[test]
    fn parse_aoi_radius_cells_defaults_and_rejects_negative() {
        assert_eq!(
            parse_aoi_radius_cells(None).unwrap(),
            DEFAULT_AOI_RADIUS_CELLS
        );
        assert_eq!(
            parse_aoi_radius_cells(Some("   ")).unwrap(),
            DEFAULT_AOI_RADIUS_CELLS
        );
        assert_eq!(parse_aoi_radius_cells(Some("2")).unwrap(), 2);
        let err = parse_aoi_radius_cells(Some("-1")).expect_err("negative radius should fail");
        assert!(err.to_string().contains(">= 0"));
    }

    #[test]
    fn parse_aoi_edge_settings_defaults_and_reject_invalid() {
        assert_eq!(
            parse_aoi_cell_span_units(None).unwrap(),
            DEFAULT_AOI_CELL_SPAN_UNITS
        );
        assert_eq!(
            parse_aoi_cell_span_units(Some("  ")).unwrap(),
            DEFAULT_AOI_CELL_SPAN_UNITS
        );
        assert_eq!(parse_aoi_cell_span_units(Some("48")).unwrap(), 48.0);
        let err = parse_aoi_cell_span_units(Some("0")).expect_err("zero span should fail");
        assert!(err.to_string().contains("> 0"));

        assert_eq!(parse_aoi_edge_margin_units(None).unwrap(), None);
        assert_eq!(parse_aoi_edge_margin_units(Some("")).unwrap(), None);
        assert_eq!(parse_aoi_edge_margin_units(Some("4")).unwrap(), Some(4.0));
        let err = parse_aoi_edge_margin_units(Some("-1")).expect_err("negative margin should fail");
        assert!(err.to_string().contains(">= 0"));
    }

    #[test]
    fn desired_aoi_cells_respects_radius() {
        let origin = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        let far = CellId::grid(0, 3, 0);
        let root_positions = vec![(origin, Position { x: 16.0, y: 16.0 })];
        let owned_cells = HashSet::from([origin, adjacent, far]);

        assert_eq!(
            desired_aoi_cells(
                &root_positions,
                &owned_cells,
                0,
                DEFAULT_AOI_CELL_SPAN_UNITS,
                None,
            ),
            HashSet::from([origin])
        );
        assert_eq!(
            desired_aoi_cells(
                &root_positions,
                &owned_cells,
                1,
                DEFAULT_AOI_CELL_SPAN_UNITS,
                None,
            ),
            HashSet::from([origin, adjacent])
        );
    }

    #[test]
    fn desired_aoi_cells_respects_edge_margin() {
        let origin = CellId::grid(0, 0, 0);
        let east = CellId::grid(0, 1, 0);
        let north = CellId::grid(0, 0, 1);
        let northeast = CellId::grid(0, 1, 1);
        let owned_cells = HashSet::from([origin, east, north, northeast]);

        let centered = vec![(origin, Position { x: 16.0, y: 16.0 })];
        assert_eq!(
            desired_aoi_cells(&centered, &owned_cells, 1, 32.0, Some(4.0)),
            HashSet::from([origin])
        );

        let edge = vec![(origin, Position { x: 30.0, y: 30.0 })];
        assert_eq!(
            desired_aoi_cells(&edge, &owned_cells, 1, 32.0, Some(4.0)),
            HashSet::from([origin, east, north, northeast])
        );
    }

    #[test]
    fn peer_routes_from_listing_excludes_self_and_maps_remote_cells() {
        let listing = AssignmentListing {
            workers: vec![
                tessera_proto::orch::v1::AssignmentBundle {
                    worker_id: "worker-a".into(),
                    addr: "127.0.0.1:5001".into(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 0,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
                tessera_proto::orch::v1::AssignmentBundle {
                    worker_id: "worker-b".into(),
                    addr: "127.0.0.1:5002".into(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 1,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
            ],
        };

        let routes = peer_routes_from_listing(&listing, "worker-a").expect("peer routes");
        assert_eq!(routes.len(), 1);
        assert_eq!(
            routes.get(&CellId::grid(0, 1, 0)),
            Some(&PeerRoute {
                worker_id: "worker-b".into(),
                addr: "127.0.0.1:5002".into(),
            })
        );
        assert!(!routes.contains_key(&CellId::grid(0, 0, 0)));
    }

    #[test]
    fn peer_routes_from_listing_rejects_duplicate_remote_cells() {
        let listing = AssignmentListing {
            workers: vec![
                tessera_proto::orch::v1::AssignmentBundle {
                    worker_id: "worker-b".into(),
                    addr: "127.0.0.1:5002".into(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 1,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
                tessera_proto::orch::v1::AssignmentBundle {
                    worker_id: "worker-c".into(),
                    addr: "127.0.0.1:5003".into(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 1,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
            ],
        };

        let err = peer_routes_from_listing(&listing, "worker-a").expect_err("duplicate route");
        assert!(err.to_string().contains("duplicate remote route"));
    }

    #[tokio::test]
    async fn resync_client_interests_tracks_remote_cells() {
        let state = Arc::new(SharedState::default());
        let primary = CellId::grid(0, 0, 0);
        let remote = CellId::grid(0, 1, 0);
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(primary)
                .or_default()
                .insert(EntityId(1), Position { x: 16.0, y: 16.0 });
        }
        state
            .update_peer_routes(HashMap::from([(
                remote,
                PeerRoute {
                    worker_id: "worker-b".into(),
                    addr: "127.0.0.1:5002".into(),
                },
            )]))
            .await;

        let (watcher_tx, _watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(51, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(primary, &watcher).await;
        state
            .upsert_client_root(&watcher, primary, EntityId(1), 10)
            .await;

        state
            .resync_client_interests(&HashSet::from([primary]))
            .await;

        assert_eq!(
            state.remote_interest_cells(watcher.client_id).await,
            HashSet::from([remote])
        );
        assert_eq!(
            state.client_subscribed_cells(watcher.client_id).await,
            HashSet::from([primary])
        );
    }

    #[tokio::test]
    async fn apply_peer_routes_update_resyncs_remote_interests() {
        let state = Arc::new(SharedState::default());
        let primary = CellId::grid(0, 0, 0);
        let remote = CellId::grid(0, 1, 0);
        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary])));
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(primary)
                .or_default()
                .insert(EntityId(1), Position { x: 16.0, y: 16.0 });
        }

        let (watcher_tx, _watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(52, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(primary, &watcher).await;
        state
            .upsert_client_root(&watcher, primary, EntityId(1), 11)
            .await;

        let listing = AssignmentListing {
            workers: vec![
                tessera_proto::orch::v1::AssignmentBundle {
                    worker_id: "worker-a".into(),
                    addr: "127.0.0.1:5001".into(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 0,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
                tessera_proto::orch::v1::AssignmentBundle {
                    worker_id: "worker-b".into(),
                    addr: "127.0.0.1:5002".into(),
                    cells: vec![Assignment {
                        world: 0,
                        cx: 1,
                        cy: 0,
                        depth: 0,
                        sub: 0,
                    }],
                },
            ],
        };

        apply_peer_routes_update(&state, &owned_cells, "worker-a", listing)
            .await
            .expect("apply peer routes");

        assert_eq!(
            state.remote_interest_cells(watcher.client_id).await,
            HashSet::from([remote])
        );
    }

    #[tokio::test]
    async fn remove_client_subscriptions_clears_remote_interests() {
        let state = SharedState::default();
        let remote = CellId::grid(0, 1, 0);
        {
            let mut remote_interests = state.remote_interests.lock().await;
            remote_interests.insert(
                77,
                HashMap::from([(
                    remote,
                    PeerRoute {
                        worker_id: "worker-b".into(),
                        addr: "127.0.0.1:5002".into(),
                    },
                )]),
            );
        }

        state.remove_client_subscriptions(77).await;
        assert!(state.remote_interest_cells(77).await.is_empty());
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
    async fn retry_assignments_resyncs_existing_client_aoi() {
        let state = Arc::new(SharedState::default());
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(primary)
                .or_default()
                .insert(EntityId(1), Position { x: 16.0, y: 16.0 });
            actors
                .entry(adjacent)
                .or_default()
                .insert(EntityId(2), Position { x: 3.0, y: 4.0 });
        }

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(41, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(primary, &watcher).await;
        state
            .upsert_client_root(&watcher, primary, EntityId(1), 77)
            .await;

        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary])));
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::clone(&attempts);

        retry_assignments_until_registered(
            "worker-retry-aoi".to_string(),
            Arc::clone(&owned_cells),
            Arc::clone(&state),
            move || {
                let attempts = Arc::clone(&attempts_clone);
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        Err(anyhow!("orch unreachable"))
                    } else {
                        Ok(vec![primary, adjacent])
                    }
                }
            },
            Duration::from_millis(10),
        )
        .await;

        let snapshot = watcher_rx
            .try_recv()
            .expect("existing client should receive adjacent snapshot after resync");
        assert_eq!(snapshot.0, adjacent);
        assert_eq!(snapshot.1, Some(77));
        match snapshot.2 {
            ServerMsg::Snapshot { cell, actors } => {
                assert_eq!(cell, adjacent);
                assert_eq!(actors.len(), 1);
                assert_eq!(actors[0].id, EntityId(2));
                assert_eq!(actors[0].pos, Position { x: 3.0, y: 4.0 });
            }
            other => panic!("expected adjacent snapshot, got {other:?}"),
        }

        let subscribed = state.client_subscribed_cells(watcher.client_id).await;
        assert_eq!(subscribed, HashSet::from([primary, adjacent]));
    }

    #[tokio::test]
    async fn drop_cells_prunes_client_roots_before_future_resync() {
        let state = Arc::new(SharedState::default());
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(primary)
                .or_default()
                .insert(EntityId(1), Position { x: 16.0, y: 16.0 });
            actors
                .entry(adjacent)
                .or_default()
                .insert(EntityId(2), Position { x: 9.0, y: 1.0 });
        }

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(42, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(primary, &watcher).await;
        state
            .upsert_client_root(&watcher, primary, EntityId(1), 88)
            .await;

        state
            .resync_client_interests(&HashSet::from([primary, adjacent]))
            .await;
        let initial_snapshot = watcher_rx
            .try_recv()
            .expect("initial resync should subscribe adjacent cell");
        assert_eq!(initial_snapshot.0, adjacent);

        state.drop_cells(&HashSet::from([primary])).await;
        let despawn = watcher_rx
            .try_recv()
            .expect("dropping the root cell should broadcast despawn");
        assert_eq!(despawn.0, primary);
        assert!(matches!(
            despawn.2,
            ServerMsg::Despawn { ref actors, .. } if actors == &vec![EntityId(1)]
        ));
        state
            .resync_client_interests(&HashSet::from([adjacent]))
            .await;

        let subscribed = state.client_subscribed_cells(watcher.client_id).await;
        assert!(
            subscribed.is_empty(),
            "rootless client should be unsubscribed"
        );
        let sessions = state.client_sessions.lock().await;
        assert!(
            !sessions.contains_key(&watcher.client_id),
            "dropping a root cell should forget the client session"
        );
        drop(sessions);

        state
            .resync_client_interests(&HashSet::from([primary, adjacent]))
            .await;
        assert!(
            watcher_rx.try_recv().is_err(),
            "stale roots must not resubscribe ghost cells after reassignment"
        );
    }

    #[tokio::test]
    async fn resync_client_interests_respects_edge_margin() {
        let state = Arc::new(SharedState::with_aoi_config(1, 32.0, Some(4.0)));
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(primary)
                .or_default()
                .insert(EntityId(1), Position { x: 16.0, y: 16.0 });
            actors
                .entry(adjacent)
                .or_default()
                .insert(EntityId(2), Position { x: 7.0, y: 8.0 });
        }

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(43, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(primary, &watcher).await;
        state
            .upsert_client_root(&watcher, primary, EntityId(1), 90)
            .await;

        state
            .resync_client_interests(&HashSet::from([primary, adjacent]))
            .await;
        assert!(
            watcher_rx.try_recv().is_err(),
            "centered actor should not subscribe adjacent cell under edge AOI"
        );

        {
            let mut actors = state.actors.lock().await;
            let pos = actors
                .get_mut(&primary)
                .and_then(|cell_actors| cell_actors.get_mut(&EntityId(1)))
                .expect("primary actor");
            pos.x = 29.0;
        }
        state.touch_client_epoch(&watcher, 91).await;
        state
            .resync_client_interests(&HashSet::from([primary, adjacent]))
            .await;

        let snapshot = watcher_rx
            .try_recv()
            .expect("edge actor should receive adjacent snapshot");
        assert_eq!(snapshot.0, adjacent);
        assert_eq!(snapshot.1, Some(91));
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
    async fn aoi_resync_uses_latest_client_epoch_after_ping() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary])));
        let accept_state = Arc::clone(&state);
        let accept_owned = Arc::clone(&owned_cells);

        let server = tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.expect("accept connection");
            handle_upstream(sock, peer, accept_state, accept_owned)
                .await
                .expect("handle upstream");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect client");
        client
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 0,
                epoch: 10,
                payload: ClientMsg::Join {
                    actor: EntityId(1),
                    pos: Position { x: 0.0, y: 0.0 },
                },
            }))
            .await
            .expect("send join");
        let join_snapshot = read_env(&mut client).await;
        assert_eq!(join_snapshot.epoch, 10);

        client
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 1,
                epoch: 25,
                payload: ClientMsg::Ping { ts: 1234 },
            }))
            .await
            .expect("send ping");
        let pong = read_env(&mut client).await;
        assert_eq!(pong.epoch, 25);
        assert!(matches!(pong.payload, ServerMsg::Pong { ts } if ts == 1234));

        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(adjacent)
                .or_default()
                .insert(EntityId(2), Position { x: 7.0, y: 8.0 });
        }
        let updated = {
            let mut guard = owned_cells.write().await;
            *guard = HashSet::from([primary, adjacent]);
            guard.clone()
        };
        state.resync_client_interests(&updated).await;

        let ghost_snapshot = read_env(&mut client).await;
        assert_eq!(ghost_snapshot.epoch, 25);
        match ghost_snapshot.payload {
            ServerMsg::Snapshot { cell, actors } => {
                assert_eq!(cell, adjacent);
                assert_eq!(actors.len(), 1);
                assert_eq!(actors[0].id, EntityId(2));
                assert_eq!(actors[0].pos, Position { x: 7.0, y: 8.0 });
            }
            other => panic!("expected ghost snapshot, got {other:?}"),
        }

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
        let ticker = spawn_test_tick_loop(Arc::clone(&state));
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
        ticker.abort();
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
    async fn join_receives_adjacent_owned_cell_snapshot() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary, adjacent])));
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

        let mut adjacent_owner = TcpStream::connect(addr)
            .await
            .expect("connect adjacent owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");

        let adjacent_join = Envelope {
            cell: adjacent,
            seq: 0,
            epoch: 10,
            payload: ClientMsg::Join {
                actor: EntityId(2),
                pos: Position { x: 10.0, y: 0.0 },
            },
        };
        adjacent_owner
            .write_all(&encode_frame(&adjacent_join))
            .await
            .expect("adjacent join");
        let adjacent_snapshot = read_env(&mut adjacent_owner).await;
        assert!(matches!(
            adjacent_snapshot.payload,
            ServerMsg::Snapshot { cell, .. } if cell == adjacent
        ));

        let watcher_join = Envelope {
            cell: primary,
            seq: 0,
            epoch: 11,
            payload: ClientMsg::Join {
                actor: EntityId(1),
                pos: Position { x: 0.0, y: 0.0 },
            },
        };
        watcher
            .write_all(&encode_frame(&watcher_join))
            .await
            .expect("watcher join");

        let own_snapshot = read_env(&mut watcher).await;
        assert!(matches!(
            own_snapshot.payload,
            ServerMsg::Snapshot { cell, .. } if cell == primary
        ));

        let ghost_snapshot = read_env(&mut watcher).await;
        match ghost_snapshot.payload {
            ServerMsg::Snapshot { cell, actors } => {
                assert_eq!(cell, adjacent);
                assert_eq!(ghost_snapshot.epoch, 11);
                assert!(
                    actors.iter().any(|actor| actor.id == EntityId(2)),
                    "adjacent cell snapshot should include existing neighbor actor"
                );
            }
            other => panic!("expected adjacent snapshot, got {other:?}"),
        }

        drop(adjacent_owner);
        drop(watcher);
        let _ = server.await;
    }

    #[tokio::test]
    async fn cross_worker_relay_forwards_snapshot_delta_and_despawn() {
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);

        let remote_state = Arc::new(SharedState::default());
        let remote_ticker = spawn_test_tick_loop(Arc::clone(&remote_state));
        let remote_owned = Arc::new(RwLock::new(HashSet::from([adjacent])));
        let remote_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind remote listener");
        let remote_addr = remote_listener.local_addr().expect("remote addr");
        let remote_server = spawn_incoming_test_server(
            remote_listener,
            Arc::clone(&remote_state),
            Arc::clone(&remote_owned),
            2,
        );

        let local_state = Arc::new(SharedState::default());
        let local_ticker = spawn_test_tick_loop(Arc::clone(&local_state));
        let local_owned = Arc::new(RwLock::new(HashSet::from([primary])));
        local_state
            .update_peer_routes(HashMap::from([(
                adjacent,
                PeerRoute {
                    worker_id: "worker-b".into(),
                    addr: remote_addr.to_string(),
                },
            )]))
            .await;
        let local_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local listener");
        let local_addr = local_listener.local_addr().expect("local addr");
        let local_server = spawn_incoming_test_server(
            local_listener,
            Arc::clone(&local_state),
            Arc::clone(&local_owned),
            1,
        );

        let mut remote_owner = TcpStream::connect(remote_addr)
            .await
            .expect("connect remote owner");
        remote_owner
            .write_all(&encode_frame(&Envelope {
                cell: adjacent,
                seq: 0,
                epoch: 50,
                payload: ClientMsg::Join {
                    actor: EntityId(2),
                    pos: Position { x: 4.0, y: 8.0 },
                },
            }))
            .await
            .expect("remote join");
        let _ = read_env(&mut remote_owner).await;

        let mut watcher = TcpStream::connect(local_addr)
            .await
            .expect("connect watcher");
        watcher
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 0,
                epoch: 51,
                payload: ClientMsg::Join {
                    actor: EntityId(1),
                    pos: Position { x: 0.0, y: 0.0 },
                },
            }))
            .await
            .expect("watcher join");

        let own_snapshot = read_env(&mut watcher).await;
        assert!(matches!(
            own_snapshot.payload,
            ServerMsg::Snapshot { cell, .. } if cell == primary
        ));

        let remote_snapshot = timeout(Duration::from_secs(1), read_env(&mut watcher))
            .await
            .expect("wait for remote snapshot");
        match remote_snapshot.payload {
            ServerMsg::Snapshot { cell, actors } => {
                assert_eq!(cell, adjacent);
                assert_eq!(remote_snapshot.epoch, 51);
                assert_eq!(actors.len(), 1);
                assert_eq!(actors[0].id, EntityId(2));
                assert_eq!(actors[0].pos, Position { x: 4.0, y: 8.0 });
            }
            other => panic!("expected relayed snapshot, got {other:?}"),
        }

        remote_owner
            .write_all(&encode_frame(&Envelope {
                cell: adjacent,
                seq: 1,
                epoch: 52,
                payload: ClientMsg::Move {
                    actor: EntityId(2),
                    dx: 3.0,
                    dy: -2.0,
                },
            }))
            .await
            .expect("remote move");
        let _ = read_env(&mut remote_owner).await;

        let remote_delta = timeout(Duration::from_secs(1), read_env(&mut watcher))
            .await
            .expect("wait for remote delta");
        match remote_delta.payload {
            ServerMsg::Delta { cell, moved } => {
                assert_eq!(cell, adjacent);
                assert_eq!(remote_delta.epoch, 52);
                assert_eq!(moved.len(), 1);
                assert_eq!(moved[0].id, EntityId(2));
                assert_eq!(moved[0].pos, Position { x: 7.0, y: 6.0 });
            }
            other => panic!("expected relayed delta, got {other:?}"),
        }

        drop(remote_owner);
        let remote_despawn = timeout(Duration::from_secs(1), read_env(&mut watcher))
            .await
            .expect("wait for remote despawn");
        match remote_despawn.payload {
            ServerMsg::Despawn { cell, actors } => {
                assert_eq!(cell, adjacent);
                assert_eq!(remote_despawn.epoch, 52);
                assert_eq!(actors, vec![EntityId(2)]);
            }
            other => panic!("expected relayed despawn, got {other:?}"),
        }

        drop(watcher);
        timeout(Duration::from_secs(1), local_server)
            .await
            .expect("local server timeout")
            .expect("local server join");
        timeout(Duration::from_secs(1), remote_server)
            .await
            .expect("remote server timeout")
            .expect("remote server join");

        let relay_subscribers = remote_state.relay_subscribers.lock().await;
        assert!(relay_subscribers.is_empty());

        local_ticker.abort();
        remote_ticker.abort();
    }

    #[tokio::test]
    async fn move_resyncs_edge_based_aoi_subscriptions() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::with_aoi_config(1, 32.0, Some(4.0)));
        let ticker = spawn_test_tick_loop(Arc::clone(&state));
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary, adjacent])));
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

        let mut adjacent_owner = TcpStream::connect(addr)
            .await
            .expect("connect adjacent owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");

        adjacent_owner
            .write_all(&encode_frame(&Envelope {
                cell: adjacent,
                seq: 0,
                epoch: 70,
                payload: ClientMsg::Join {
                    actor: EntityId(2),
                    pos: Position { x: 10.0, y: 10.0 },
                },
            }))
            .await
            .expect("adjacent join");
        let _ = read_env(&mut adjacent_owner).await;

        watcher
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 0,
                epoch: 71,
                payload: ClientMsg::Join {
                    actor: EntityId(1),
                    pos: Position { x: 16.0, y: 16.0 },
                },
            }))
            .await
            .expect("watcher join");
        let own_snapshot = read_env(&mut watcher).await;
        assert!(matches!(
            own_snapshot.payload,
            ServerMsg::Snapshot { cell, .. } if cell == primary
        ));
        let leaked_snapshot = timeout(Duration::from_millis(150), read_env(&mut watcher)).await;
        assert!(
            leaked_snapshot.is_err(),
            "centered actor should not receive adjacent ghost snapshot"
        );

        watcher
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 1,
                epoch: 72,
                payload: ClientMsg::Move {
                    actor: EntityId(1),
                    dx: 13.0,
                    dy: 0.0,
                },
            }))
            .await
            .expect("watcher move to edge");
        let self_delta = read_env(&mut watcher).await;
        assert!(matches!(
            self_delta.payload,
            ServerMsg::Delta { cell, moved }
            if cell == primary && moved.iter().any(|actor| actor.id == EntityId(1))
        ));
        let ghost_snapshot = read_env(&mut watcher).await;
        assert_eq!(ghost_snapshot.epoch, 72);
        assert!(matches!(
            ghost_snapshot.payload,
            ServerMsg::Snapshot { cell, ref actors }
            if cell == adjacent && actors.iter().any(|actor| actor.id == EntityId(2))
        ));

        watcher
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 2,
                epoch: 73,
                payload: ClientMsg::Move {
                    actor: EntityId(1),
                    dx: -20.0,
                    dy: 0.0,
                },
            }))
            .await
            .expect("watcher move away from edge");
        let self_delta = read_env(&mut watcher).await;
        assert!(matches!(
            self_delta.payload,
            ServerMsg::Delta { cell, moved }
            if cell == primary && moved.iter().any(|actor| actor.id == EntityId(1))
        ));

        adjacent_owner
            .write_all(&encode_frame(&Envelope {
                cell: adjacent,
                seq: 1,
                epoch: 74,
                payload: ClientMsg::Move {
                    actor: EntityId(2),
                    dx: 1.0,
                    dy: 0.0,
                },
            }))
            .await
            .expect("adjacent move");
        let _ = read_env(&mut adjacent_owner).await;

        let leaked_delta = timeout(Duration::from_millis(200), read_env(&mut watcher)).await;
        assert!(
            leaked_delta.is_err(),
            "moving away from the edge should unsubscribe adjacent AOI updates"
        );

        drop(adjacent_owner);
        drop(watcher);
        let _ = server.await;
        ticker.abort();
    }

    #[tokio::test]
    async fn adjacent_cell_move_reaches_aoi_subscriber() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let ticker = spawn_test_tick_loop(Arc::clone(&state));
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary, adjacent])));
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

        let mut adjacent_owner = TcpStream::connect(addr)
            .await
            .expect("connect adjacent owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");

        adjacent_owner
            .write_all(&encode_frame(&Envelope {
                cell: adjacent,
                seq: 0,
                epoch: 20,
                payload: ClientMsg::Join {
                    actor: EntityId(2),
                    pos: Position { x: 10.0, y: 0.0 },
                },
            }))
            .await
            .expect("adjacent join");
        let _ = read_env(&mut adjacent_owner).await;

        watcher
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 0,
                epoch: 21,
                payload: ClientMsg::Join {
                    actor: EntityId(1),
                    pos: Position { x: 0.0, y: 0.0 },
                },
            }))
            .await
            .expect("watcher join");
        let _ = read_env(&mut watcher).await;
        let _ = read_env(&mut watcher).await;
        let owner_ghost_delta = read_env(&mut adjacent_owner).await;
        assert_eq!(owner_ghost_delta.epoch, 21);
        assert!(matches!(
            owner_ghost_delta.payload,
            ServerMsg::Delta { cell, moved }
            if cell == primary && moved.iter().any(|actor| actor.id == EntityId(1))
        ));

        adjacent_owner
            .write_all(&encode_frame(&Envelope {
                cell: adjacent,
                seq: 1,
                epoch: 22,
                payload: ClientMsg::Move {
                    actor: EntityId(2),
                    dx: 1.0,
                    dy: 0.5,
                },
            }))
            .await
            .expect("adjacent move");

        let owner_delta = read_env(&mut adjacent_owner).await;
        assert_eq!(owner_delta.epoch, 22);

        let mut watcher_delta = None;
        for _ in 0..3 {
            let candidate = read_env(&mut watcher).await;
            if candidate.epoch == 22 {
                watcher_delta = Some(candidate);
                break;
            }
        }
        let watcher_delta = watcher_delta.expect("watcher should receive adjacent move delta");
        assert_eq!(watcher_delta.epoch, 22);
        match watcher_delta.payload {
            ServerMsg::Delta { cell, moved } => {
                assert_eq!(cell, adjacent);
                assert_eq!(moved.len(), 1);
                assert_eq!(moved[0].id, EntityId(2));
                assert_eq!(moved[0].pos, Position { x: 11.0, y: 0.5 });
            }
            other => panic!("expected adjacent delta, got {other:?}"),
        }

        drop(adjacent_owner);
        drop(watcher);
        let _ = server.await;
        ticker.abort();
    }

    #[tokio::test]
    async fn adjacent_cell_despawn_reaches_aoi_subscriber() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let primary = CellId::grid(0, 0, 0);
        let adjacent = CellId::grid(0, 1, 0);
        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary, adjacent])));
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

        let mut adjacent_owner = TcpStream::connect(addr)
            .await
            .expect("connect adjacent owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");

        adjacent_owner
            .write_all(&encode_frame(&Envelope {
                cell: adjacent,
                seq: 0,
                epoch: 40,
                payload: ClientMsg::Join {
                    actor: EntityId(2),
                    pos: Position { x: 10.0, y: 0.0 },
                },
            }))
            .await
            .expect("adjacent join");
        let _ = read_env(&mut adjacent_owner).await;

        watcher
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 0,
                epoch: 41,
                payload: ClientMsg::Join {
                    actor: EntityId(1),
                    pos: Position { x: 0.0, y: 0.0 },
                },
            }))
            .await
            .expect("watcher join");
        let _ = read_env(&mut watcher).await;
        let _ = read_env(&mut watcher).await;
        let _ = read_env(&mut adjacent_owner).await;

        drop(adjacent_owner);

        let despawn = read_env(&mut watcher).await;
        assert_eq!(despawn.epoch, 40);
        match despawn.payload {
            ServerMsg::Despawn { cell, actors } => {
                assert_eq!(cell, adjacent);
                assert_eq!(actors, vec![EntityId(2)]);
            }
            other => panic!("expected adjacent despawn, got {other:?}"),
        }

        drop(watcher);
        let _ = server.await;
    }

    #[tokio::test]
    async fn distant_cell_updates_do_not_reach_non_aoi_subscriber() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let state = Arc::new(SharedState::default());
        let ticker = spawn_test_tick_loop(Arc::clone(&state));
        let primary = CellId::grid(0, 0, 0);
        let distant = CellId::grid(0, 3, 0);
        let owned_cells = Arc::new(RwLock::new(HashSet::from([primary, distant])));
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

        let mut distant_owner = TcpStream::connect(addr)
            .await
            .expect("connect distant owner");
        let mut watcher = TcpStream::connect(addr).await.expect("connect watcher");

        distant_owner
            .write_all(&encode_frame(&Envelope {
                cell: distant,
                seq: 0,
                epoch: 30,
                payload: ClientMsg::Join {
                    actor: EntityId(2),
                    pos: Position { x: 30.0, y: 0.0 },
                },
            }))
            .await
            .expect("distant join");
        let _ = read_env(&mut distant_owner).await;

        watcher
            .write_all(&encode_frame(&Envelope {
                cell: primary,
                seq: 0,
                epoch: 31,
                payload: ClientMsg::Join {
                    actor: EntityId(1),
                    pos: Position { x: 0.0, y: 0.0 },
                },
            }))
            .await
            .expect("watcher join");
        let _ = read_env(&mut watcher).await;

        let leaked_snapshot = timeout(Duration::from_millis(150), read_env(&mut watcher)).await;
        assert!(
            leaked_snapshot.is_err(),
            "non-adjacent cell should not be subscribed as AOI"
        );

        distant_owner
            .write_all(&encode_frame(&Envelope {
                cell: distant,
                seq: 1,
                epoch: 32,
                payload: ClientMsg::Move {
                    actor: EntityId(2),
                    dx: 1.0,
                    dy: 0.0,
                },
            }))
            .await
            .expect("distant move");
        let _ = read_env(&mut distant_owner).await;

        let leaked_delta = timeout(Duration::from_millis(150), read_env(&mut watcher)).await;
        assert!(
            leaked_delta.is_err(),
            "non-adjacent cell delta should not leak to watcher"
        );

        drop(distant_owner);
        drop(watcher);
        let _ = server.await;
        ticker.abort();
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
        let ticker = spawn_test_tick_loop(Arc::clone(&state));
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
        ticker.abort();
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
        let (disconnect_tx, _disconnect_rx) = watch::channel(());
        let subscriber = test_subscriber(10, tx, &disconnect_tx);
        state.subscribe(cell, &subscriber).await;
        let (tx2, _rx2) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let duplicate_subscriber = test_subscriber(10, tx2, &disconnect_tx);
        state.subscribe(cell, &duplicate_subscriber).await;

        {
            let subs = state.subscribers.lock().await;
            assert_eq!(subs.get(&cell).map(|v| v.len()), Some(1));
        }

        let removed = HashSet::from([cell]);
        state.drop_cells(&removed).await;

        let (tx3, _rx3) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let resubscribed = test_subscriber(10, tx3, &disconnect_tx);
        state.subscribe(cell, &resubscribed).await;

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
        let (disconnect_tx, _disconnect_rx) = watch::channel(());
        let subscriber = test_subscriber(1, tx, &disconnect_tx);

        let actors_guard = state.actors.lock().await;

        let state_clone = Arc::clone(&state);
        let join_handle = tokio::spawn(async move {
            state_clone
                .snapshot_and_subscribe(cell, actor, pos, &subscriber, 0)
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
        let (disconnect_tx, _disconnect_rx) = watch::channel(());
        let subscriber = test_subscriber(1, tx, &disconnect_tx);
        state.subscribe(cell, &subscriber).await;
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
        let (disconnect_tx, _disconnect_rx) = watch::channel(());
        let subscriber = test_subscriber(1, tx, &disconnect_tx);
        state.subscribe(cell, &subscriber).await;
        drop(rx);

        state
            .broadcast(cell, ServerMsg::Pong { ts: 2 }, Some(1), None)
            .await;

        let subs = state.subscribers.lock().await;
        assert!(subs.is_empty());
    }

    #[tokio::test]
    async fn broadcast_full_subscriber_requests_disconnect() {
        let state = SharedState::default();
        let cell = CellId::grid(0, 0, 0);

        let (tx, mut rx) = channel(1);
        let tx_for_state = tx.clone();
        let (disconnect_tx, mut disconnect_rx) = watch::channel(());
        let subscriber = test_subscriber(1, tx_for_state, &disconnect_tx);
        state.subscribe(cell, &subscriber).await;

        tx.try_send((cell, None, ServerMsg::Pong { ts: 0 }))
            .expect("fill outbound channel");
        state
            .broadcast(cell, ServerMsg::Pong { ts: 1 }, None, None)
            .await;

        disconnect_rx
            .changed()
            .await
            .expect("disconnect signal should be sent");
        assert!(
            rx.try_recv().is_ok(),
            "existing queued message should remain until connection is closed"
        );

        let subs = state.subscribers.lock().await;
        assert!(subs.is_empty());
    }

    #[tokio::test]
    async fn queued_move_broadcast_flushes_on_tick() {
        let state = Arc::new(SharedState::default());
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(7);

        let (owner_tx, mut owner_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (owner_disconnect_tx, _owner_disconnect_rx) = watch::channel(());
        let owner = test_subscriber(1, owner_tx, &owner_disconnect_tx);
        state.subscribe(cell, &owner).await;

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(2, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(cell, &watcher).await;

        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(cell)
                .or_default()
                .insert(actor, Position { x: 3.0, y: 4.0 });
        }
        state.enqueue_move_broadcast(cell, actor, 1, 55).await;

        assert!(watcher_rx.try_recv().is_err());
        assert!(owner_rx.try_recv().is_err());

        on_tick(Tick(1), state.as_ref()).await;

        let queued = watcher_rx.try_recv().expect("watcher delta should flush");
        assert_eq!(queued.0, cell);
        assert_eq!(queued.1, Some(55));
        match queued.2 {
            ServerMsg::Delta {
                cell: delta_cell,
                moved,
            } => {
                assert_eq!(delta_cell, cell);
                assert_eq!(moved.len(), 1);
                assert_eq!(moved[0].id, actor);
                assert_eq!(moved[0].pos, Position { x: 3.0, y: 4.0 });
            }
            other => panic!("expected queued delta, got {other:?}"),
        }

        assert!(
            owner_rx.try_recv().is_err(),
            "origin client should stay excluded from queued move broadcast"
        );
    }

    #[tokio::test]
    async fn queued_move_broadcast_keeps_latest_actor_state_per_tick() {
        let state = Arc::new(SharedState::default());
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(7);

        let (owner_tx, mut owner_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (owner_disconnect_tx, _owner_disconnect_rx) = watch::channel(());
        let owner = test_subscriber(1, owner_tx, &owner_disconnect_tx);
        state.subscribe(cell, &owner).await;

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(2, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(cell, &watcher).await;

        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(cell)
                .or_default()
                .insert(actor, Position { x: 1.0, y: 1.0 });
        }
        state.enqueue_move_broadcast(cell, actor, 1, 10).await;
        {
            let mut actors = state.actors.lock().await;
            let cell_actors = actors.get_mut(&cell).expect("cell actors");
            let pos = cell_actors.get_mut(&actor).expect("actor");
            pos.x = 5.0;
            pos.y = 8.0;
        }
        state.enqueue_move_broadcast(cell, actor, 1, 11).await;

        on_tick(Tick(1), state.as_ref()).await;

        let queued = watcher_rx
            .try_recv()
            .expect("watcher should receive coalesced delta");
        assert_eq!(queued.1, Some(11));
        match queued.2 {
            ServerMsg::Delta { moved, .. } => {
                assert_eq!(moved.len(), 1);
                assert_eq!(moved[0].id, actor);
                assert_eq!(moved[0].pos, Position { x: 5.0, y: 8.0 });
            }
            other => panic!("expected coalesced delta, got {other:?}"),
        }

        assert!(
            watcher_rx.try_recv().is_err(),
            "watcher should only receive the latest state once per actor"
        );
        assert!(
            owner_rx.try_recv().is_err(),
            "origin client should stay excluded from queued move broadcast"
        );
    }

    #[tokio::test]
    async fn drop_cells_clears_pending_move_broadcasts() {
        let state = Arc::new(SharedState::default());
        let cell = CellId::grid(0, 0, 0);

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(2, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(cell, &watcher).await;

        state.enqueue_move_broadcast(cell, EntityId(7), 1, 21).await;

        state.drop_cells(&HashSet::from([cell])).await;
        on_tick(Tick(1), state.as_ref()).await;

        assert!(
            watcher_rx.try_recv().is_err(),
            "dropped cell should not flush stale pending moves"
        );
    }

    #[tokio::test]
    async fn remove_owned_actors_clears_pending_moves_before_despawn() {
        let state = Arc::new(SharedState::default());
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(7);

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(2, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(cell, &watcher).await;

        state.claim_owner(cell, actor, 1).await;
        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(cell)
                .or_default()
                .insert(actor, Position { x: 0.0, y: 0.0 });
        }
        state.enqueue_move_broadcast(cell, actor, 1, 31).await;

        state
            .remove_owned_actors(1, &HashSet::from([(cell, actor)]), Some(30))
            .await;

        let despawn = watcher_rx.try_recv().expect("despawn should be broadcast");
        assert_eq!(despawn.0, cell);
        assert_eq!(despawn.1, Some(30));
        assert!(matches!(
            despawn.2,
            ServerMsg::Despawn { ref actors, .. } if actors == &vec![actor]
        ));

        on_tick(Tick(1), state.as_ref()).await;
        assert!(
            watcher_rx.try_recv().is_err(),
            "despawn should remove stale pending move broadcasts"
        );
    }

    #[tokio::test]
    async fn queued_move_broadcast_skips_removed_actor() {
        let state = Arc::new(SharedState::default());
        let cell = CellId::grid(0, 0, 0);
        let actor = EntityId(9);

        let (watcher_tx, mut watcher_rx) = channel(OUTBOUND_CHANNEL_CAPACITY);
        let (watcher_disconnect_tx, _watcher_disconnect_rx) = watch::channel(());
        let watcher = test_subscriber(2, watcher_tx, &watcher_disconnect_tx);
        state.subscribe(cell, &watcher).await;

        {
            let mut actors = state.actors.lock().await;
            actors
                .entry(cell)
                .or_default()
                .insert(actor, Position { x: 2.0, y: 2.0 });
        }
        state.enqueue_move_broadcast(cell, actor, 1, 22).await;

        {
            let mut actors = state.actors.lock().await;
            let cell_actors = actors.get_mut(&cell).expect("cell actors");
            cell_actors.remove(&actor);
        }

        on_tick(Tick(1), state.as_ref()).await;

        assert!(
            watcher_rx.try_recv().is_err(),
            "removed actor should not emit stale queued move"
        );
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
                None,
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
                    None,
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
