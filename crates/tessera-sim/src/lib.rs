use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::time::Duration;
use tessera_core::{
    CellId, ClientEnvelope, ClientMsg, EntityId, MAX_FRAME_LEN, Position, ServerEnvelope,
    ServerMsg, encode_frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
    time,
};

pub const PLAN_SCHEMA_VERSION: &str = "tessera.sim.plan.v1";
pub const DEFAULT_CLIENTS: u32 = 4;
pub const DEFAULT_CELLS: u32 = 1;
pub const DEFAULT_MOVES_PER_CLIENT: u32 = 3;
pub const MAX_CLIENTS: u32 = 10_000;
pub const MAX_CELLS: u32 = 4_096;
pub const MAX_MOVES_PER_CLIENT: u32 = 10_000;
pub const MAX_OPERATIONS: u64 = 1_000_000;
pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_MAX_CONCURRENCY: usize = 16;
pub const MAX_OPERATION_TIMEOUT_MS: u64 = 60_000;
pub const MAX_CONCURRENCY: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioConfig {
    pub seed: u64,
    pub clients: u32,
    pub cells: u32,
    pub moves_per_client: u32,
    pub actor_base: u64,
    pub world: u32,
    pub start_cx: i32,
    pub cy: i32,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            clients: DEFAULT_CLIENTS,
            cells: DEFAULT_CELLS,
            moves_per_client: DEFAULT_MOVES_PER_CLIENT,
            actor_base: 1,
            world: 0,
            start_cx: 0,
            cy: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioPlan {
    pub schema_version: String,
    pub seed: u64,
    pub client_count: u32,
    pub cell_count: u32,
    pub moves_per_client: u32,
    pub operation_count: u64,
    pub players: Vec<PlayerPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerPlan {
    pub client_index: u32,
    pub actor_id: u64,
    pub cell: CellId,
    pub steps: Vec<PlannedStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlannedStep {
    Join { x_milli: i32, y_milli: i32 },
    Move { dx_milli: i32, dy_milli: i32 },
    Ping { ts: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionConfig {
    pub addr: String,
    pub operation_timeout_ms: u64,
    pub max_concurrency: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4000".to_owned(),
            operation_timeout_ms: DEFAULT_OPERATION_TIMEOUT_MS,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionFailureKind {
    Connect,
    Protocol,
    Timeout,
    ServerClose,
}

impl ExecutionFailureKind {
    pub const ALL: [Self; 4] = [
        Self::Connect,
        Self::Protocol,
        Self::Timeout,
        Self::ServerClose,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Protocol => "protocol",
            Self::Timeout => "timeout",
            Self::ServerClose => "server_close",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientExecution {
    pub client_index: u32,
    pub actor_id: u64,
    pub operations_completed: u64,
    pub failure: Option<ExecutionFailureKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub client_count: u32,
    pub completed_clients: u32,
    pub failed_clients: u32,
    pub operations_completed: u64,
    pub clients: Vec<ClientExecution>,
}

impl ExecutionSummary {
    pub fn failure_count(&self, kind: ExecutionFailureKind) -> usize {
        self.clients
            .iter()
            .filter(|client| client.failure == Some(kind))
            .count()
    }
}

pub fn build_plan(config: &ScenarioConfig) -> Result<ScenarioPlan> {
    validate_config(config)?;

    let operations_per_client = u64::from(config.moves_per_client) + 2;
    let operation_count = u64::from(config.clients) * operations_per_client;
    let mut rng = SplitMix64::new(config.seed);
    let mut players = Vec::with_capacity(config.clients as usize);

    for client_index in 0..config.clients {
        let cell_offset = ((u64::from(client_index) + config.seed % u64::from(config.cells))
            % u64::from(config.cells)) as i32;
        let cell = CellId::grid(config.world, config.start_cx + cell_offset, config.cy);
        let actor_id = config.actor_base + u64::from(client_index);
        let mut steps = Vec::with_capacity(config.moves_per_client as usize + 2);
        steps.push(PlannedStep::Join {
            x_milli: rng.signed_milli(900),
            y_milli: rng.signed_milli(900),
        });
        for _ in 0..config.moves_per_client {
            let mut dx_milli = rng.signed_milli(250);
            let dy_milli = rng.signed_milli(250);
            if dx_milli == 0 && dy_milli == 0 {
                dx_milli = 1;
            }
            steps.push(PlannedStep::Move { dx_milli, dy_milli });
        }
        steps.push(PlannedStep::Ping { ts: rng.next() });
        players.push(PlayerPlan {
            client_index,
            actor_id,
            cell,
            steps,
        });
    }

    Ok(ScenarioPlan {
        schema_version: PLAN_SCHEMA_VERSION.to_owned(),
        seed: config.seed,
        client_count: config.clients,
        cell_count: config.cells,
        moves_per_client: config.moves_per_client,
        operation_count,
        players,
    })
}

pub async fn execute_plan(
    plan: &ScenarioPlan,
    config: &ExecutionConfig,
) -> Result<ExecutionSummary> {
    validate_execution_config(config)?;
    ensure!(
        plan.players.len() == plan.client_count as usize,
        "plan client count does not match player entries"
    );

    let mut pending = plan.players.clone().into_iter();
    let mut tasks = JoinSet::new();
    for _ in 0..config.max_concurrency.min(plan.players.len()) {
        if let Some(player) = pending.next() {
            spawn_player(&mut tasks, player, config);
        }
    }

    let mut clients = Vec::with_capacity(plan.players.len());
    while let Some(joined) = tasks.join_next().await {
        clients.push(joined.context("simulated client task failed")?);
        if let Some(player) = pending.next() {
            spawn_player(&mut tasks, player, config);
        }
    }
    clients.sort_by_key(|client| client.client_index);

    let completed_clients = clients
        .iter()
        .filter(|client| client.failure.is_none())
        .count() as u32;
    let failed_clients = plan.client_count - completed_clients;
    let operations_completed = clients
        .iter()
        .map(|client| client.operations_completed)
        .sum();

    Ok(ExecutionSummary {
        client_count: plan.client_count,
        completed_clients,
        failed_clients,
        operations_completed,
        clients,
    })
}

fn spawn_player(
    tasks: &mut JoinSet<ClientExecution>,
    player: PlayerPlan,
    config: &ExecutionConfig,
) {
    let addr = config.addr.clone();
    let operation_timeout = Duration::from_millis(config.operation_timeout_ms);
    tasks.spawn(async move { execute_player(player, &addr, operation_timeout).await });
}

async fn execute_player(
    player: PlayerPlan,
    addr: &str,
    operation_timeout: Duration,
) -> ClientExecution {
    let mut stream = match time::timeout(operation_timeout, TcpStream::connect(addr)).await {
        Err(_) => return failed_client(&player, 0, ExecutionFailureKind::Timeout),
        Ok(Err(_)) => return failed_client(&player, 0, ExecutionFailureKind::Connect),
        Ok(Ok(stream)) => stream,
    };

    let mut operations_completed = 0;
    for (seq, step) in player.steps.iter().enumerate() {
        match time::timeout(
            operation_timeout,
            execute_step(&mut stream, &player, seq as u64, step),
        )
        .await
        {
            Err(_) => {
                return failed_client(&player, operations_completed, ExecutionFailureKind::Timeout);
            }
            Ok(Err(kind)) => return failed_client(&player, operations_completed, kind),
            Ok(Ok(())) => operations_completed += 1,
        }
    }

    ClientExecution {
        client_index: player.client_index,
        actor_id: player.actor_id,
        operations_completed,
        failure: None,
    }
}

fn failed_client(
    player: &PlayerPlan,
    operations_completed: u64,
    failure: ExecutionFailureKind,
) -> ClientExecution {
    ClientExecution {
        client_index: player.client_index,
        actor_id: player.actor_id,
        operations_completed,
        failure: Some(failure),
    }
}

async fn execute_step(
    stream: &mut TcpStream,
    player: &PlayerPlan,
    seq: u64,
    step: &PlannedStep,
) -> std::result::Result<(), ExecutionFailureKind> {
    let payload = match step {
        PlannedStep::Join { x_milli, y_milli } => ClientMsg::Join {
            actor: EntityId(player.actor_id),
            pos: Position::new(*x_milli as f32 / 1_000.0, *y_milli as f32 / 1_000.0),
        },
        PlannedStep::Move { dx_milli, dy_milli } => ClientMsg::Move {
            actor: EntityId(player.actor_id),
            dx: *dx_milli as f32 / 1_000.0,
            dy: *dy_milli as f32 / 1_000.0,
        },
        PlannedStep::Ping { ts } => ClientMsg::Ping { ts: *ts },
    };
    let frame = encode_frame(&ClientEnvelope {
        cell: player.cell,
        seq,
        epoch: 0,
        session: None,
        request_id: Some(seq + 1),
        payload,
    });
    stream
        .write_all(&frame)
        .await
        .map_err(|_| ExecutionFailureKind::ServerClose)?;

    loop {
        let reply: ServerEnvelope = read_frame(stream).await?;
        match reply_matches(step, player, &reply)? {
            true => return Ok(()),
            false => continue,
        }
    }
}

fn reply_matches(
    step: &PlannedStep,
    player: &PlayerPlan,
    reply: &ServerEnvelope,
) -> std::result::Result<bool, ExecutionFailureKind> {
    if matches!(reply.payload, ServerMsg::Error { .. }) {
        return Err(ExecutionFailureKind::Protocol);
    }

    match (step, &reply.payload) {
        (PlannedStep::Join { .. }, ServerMsg::Snapshot { cell, actors })
            if reply.request_id.is_some() =>
        {
            if *cell == player.cell
                && actors
                    .iter()
                    .any(|actor| actor.id == EntityId(player.actor_id))
            {
                Ok(true)
            } else {
                Err(ExecutionFailureKind::Protocol)
            }
        }
        (PlannedStep::Move { .. }, ServerMsg::Delta { cell, moved })
            if reply.request_id.is_some() =>
        {
            if *cell == player.cell
                && moved
                    .iter()
                    .any(|actor| actor.id == EntityId(player.actor_id))
            {
                Ok(true)
            } else {
                Err(ExecutionFailureKind::Protocol)
            }
        }
        (PlannedStep::Ping { ts }, ServerMsg::Pong { ts: reply_ts }) => {
            if ts == reply_ts {
                Ok(true)
            } else {
                Err(ExecutionFailureKind::Protocol)
            }
        }
        (_, _) if reply.request_id.is_some() => Err(ExecutionFailureKind::Protocol),
        _ => Ok(false),
    }
}

async fn read_frame<T: DeserializeOwned>(
    stream: &mut TcpStream,
) -> std::result::Result<T, ExecutionFailureKind> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|_| ExecutionFailureKind::ServerClose)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(ExecutionFailureKind::Protocol);
    }
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|_| ExecutionFailureKind::ServerClose)?;
    serde_json::from_slice(&payload).map_err(|_| ExecutionFailureKind::Protocol)
}

fn validate_config(config: &ScenarioConfig) -> Result<()> {
    ensure!(config.clients > 0, "clients must be greater than zero");
    ensure!(
        config.clients <= MAX_CLIENTS,
        "clients must not exceed {MAX_CLIENTS}"
    );
    ensure!(config.cells > 0, "cells must be greater than zero");
    ensure!(
        config.cells <= MAX_CELLS,
        "cells must not exceed {MAX_CELLS}"
    );
    ensure!(
        config.cells <= config.clients,
        "cells must not exceed clients"
    );
    ensure!(
        config.moves_per_client <= MAX_MOVES_PER_CLIENT,
        "moves per client must not exceed {MAX_MOVES_PER_CLIENT}"
    );

    let operations_per_client = u64::from(config.moves_per_client) + 2;
    let operation_count = u64::from(config.clients) * operations_per_client;
    ensure!(
        operation_count <= MAX_OPERATIONS,
        "planned operations must not exceed {MAX_OPERATIONS}"
    );
    ensure!(
        config
            .actor_base
            .checked_add(u64::from(config.clients - 1))
            .is_some(),
        "actor id range overflows u64"
    );
    ensure!(
        config
            .start_cx
            .checked_add((config.cells - 1) as i32)
            .is_some(),
        "cell x range overflows i32"
    );

    Ok(())
}

fn validate_execution_config(config: &ExecutionConfig) -> Result<()> {
    ensure!(
        !config.addr.trim().is_empty(),
        "gateway address is required"
    );
    ensure!(
        config.operation_timeout_ms > 0,
        "operation timeout must be greater than zero"
    );
    ensure!(
        config.operation_timeout_ms <= MAX_OPERATION_TIMEOUT_MS,
        "operation timeout must not exceed {MAX_OPERATION_TIMEOUT_MS} ms"
    );
    ensure!(
        config.max_concurrency > 0,
        "max concurrency must be greater than zero"
    );
    ensure!(
        config.max_concurrency <= MAX_CONCURRENCY,
        "max concurrency must not exceed {MAX_CONCURRENCY}"
    );
    Ok(())
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn signed_milli(&mut self, limit: i32) -> i32 {
        let width = u64::try_from(limit * 2 + 1).expect("positive bounded width");
        i32::try_from(self.next() % width).expect("bounded sample fits i32") - limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashSet,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tessera_core::ActorState;
    use tokio::{net::TcpListener, task::JoinHandle};

    #[test]
    fn default_plan_is_safe_and_versioned() {
        let plan = build_plan(&ScenarioConfig::default()).expect("build default plan");

        assert_eq!(plan.schema_version, PLAN_SCHEMA_VERSION);
        assert_eq!(plan.client_count, DEFAULT_CLIENTS);
        assert_eq!(plan.cell_count, DEFAULT_CELLS);
        assert_eq!(plan.operation_count, 20);
        assert_eq!(plan.players.len(), DEFAULT_CLIENTS as usize);
        assert!(plan.players.iter().all(|player| player.steps.len() == 5));
    }

    #[test]
    fn identical_inputs_produce_byte_stable_json() {
        let config = ScenarioConfig {
            seed: 42,
            clients: 8,
            cells: 4,
            moves_per_client: 5,
            ..ScenarioConfig::default()
        };

        let first = serde_json::to_vec(&build_plan(&config).expect("first plan"))
            .expect("serialize first plan");
        let second = serde_json::to_vec(&build_plan(&config).expect("second plan"))
            .expect("serialize second plan");

        assert_eq!(first, second);
    }

    #[test]
    fn seed_changes_generated_scenario_data() {
        let first = build_plan(&ScenarioConfig::default()).expect("first plan");
        let second = build_plan(&ScenarioConfig {
            seed: 2,
            ..ScenarioConfig::default()
        })
        .expect("second plan");

        assert_ne!(first.players, second.players);
    }

    #[test]
    fn actor_and_cell_mapping_is_bounded_and_complete() {
        let config = ScenarioConfig {
            seed: 7,
            clients: 8,
            cells: 4,
            actor_base: 100,
            world: 3,
            start_cx: -2,
            cy: 5,
            ..ScenarioConfig::default()
        };
        let plan = build_plan(&config).expect("build plan");
        let actors = plan
            .players
            .iter()
            .map(|player| player.actor_id)
            .collect::<Vec<_>>();
        let cells = plan
            .players
            .iter()
            .map(|player| player.cell)
            .collect::<HashSet<_>>();

        assert_eq!(actors, (100..108).collect::<Vec<_>>());
        assert_eq!(cells.len(), 4);
        assert!(
            cells
                .iter()
                .all(|cell| { cell.world == 3 && (-2..=1).contains(&cell.cx) && cell.cy == 5 })
        );
    }

    #[test]
    fn invalid_boundaries_fail_closed() {
        for invalid in [
            ScenarioConfig {
                clients: 0,
                ..ScenarioConfig::default()
            },
            ScenarioConfig {
                clients: 2,
                cells: 3,
                ..ScenarioConfig::default()
            },
            ScenarioConfig {
                clients: MAX_CLIENTS,
                moves_per_client: MAX_MOVES_PER_CLIENT,
                ..ScenarioConfig::default()
            },
            ScenarioConfig {
                actor_base: u64::MAX,
                clients: 2,
                ..ScenarioConfig::default()
            },
            ScenarioConfig {
                start_cx: i32::MAX,
                cells: 2,
                ..ScenarioConfig::default()
            },
        ] {
            assert!(
                build_plan(&invalid).is_err(),
                "accepted invalid config: {invalid:?}"
            );
        }
    }

    #[tokio::test]
    async fn multi_client_execution_is_independent_and_concurrency_bounded() {
        let scenario = ScenarioConfig {
            clients: 3,
            moves_per_client: 1,
            ..ScenarioConfig::default()
        };
        let plan = build_plan(&scenario).expect("build plan");
        let (addr, server, max_active) = start_success_server(3, 3).await;

        let summary = execute_plan(
            &plan,
            &ExecutionConfig {
                addr,
                operation_timeout_ms: 500,
                max_concurrency: 2,
            },
        )
        .await
        .expect("execute plan");
        server.await.expect("success server");

        assert_eq!(summary.client_count, 3);
        assert_eq!(summary.completed_clients, 3);
        assert_eq!(summary.failed_clients, 0);
        assert_eq!(summary.operations_completed, 9);
        assert!(
            summary
                .clients
                .iter()
                .all(|client| client.failure.is_none())
        );
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn execution_classifies_connect_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr").to_string();
        drop(listener);

        let summary = execute_single_client(addr, 100).await;

        assert_single_failure(&summary, ExecutionFailureKind::Connect);
    }

    #[tokio::test]
    async fn execution_classifies_protocol_failure() {
        let summary = execute_with_failure_server(FailureMode::Protocol, 100).await;

        assert_single_failure(&summary, ExecutionFailureKind::Protocol);
    }

    #[tokio::test]
    async fn execution_classifies_operation_timeout() {
        let summary = execute_with_failure_server(FailureMode::Timeout, 20).await;

        assert_single_failure(&summary, ExecutionFailureKind::Timeout);
    }

    #[tokio::test]
    async fn execution_classifies_server_close() {
        let summary = execute_with_failure_server(FailureMode::ServerClose, 100).await;

        assert_single_failure(&summary, ExecutionFailureKind::ServerClose);
    }

    #[tokio::test]
    async fn invalid_execution_bounds_fail_before_connect() {
        let plan = build_plan(&ScenarioConfig::default()).expect("build plan");
        for invalid in [
            ExecutionConfig {
                addr: String::new(),
                ..ExecutionConfig::default()
            },
            ExecutionConfig {
                operation_timeout_ms: 0,
                ..ExecutionConfig::default()
            },
            ExecutionConfig {
                max_concurrency: 0,
                ..ExecutionConfig::default()
            },
        ] {
            assert!(
                execute_plan(&plan, &invalid).await.is_err(),
                "accepted invalid execution config: {invalid:?}"
            );
        }
    }

    async fn start_success_server(
        clients: usize,
        operations_per_client: usize,
    ) -> (String, JoinHandle<()>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr").to_string();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let server_active = Arc::clone(&active);
        let server_max = Arc::clone(&max_active);
        let server = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            for _ in 0..clients {
                let (stream, _) = listener.accept().await.expect("accept");
                let active = Arc::clone(&server_active);
                let max_active = Arc::clone(&server_max);
                connections.spawn(async move {
                    serve_success_connection(stream, operations_per_client, active, max_active)
                        .await;
                });
            }
            while let Some(connection) = connections.join_next().await {
                connection.expect("connection task");
            }
        });
        (addr, server, max_active)
    }

    async fn serve_success_connection(
        mut stream: TcpStream,
        operations: usize,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    ) {
        let active_now = active.fetch_add(1, Ordering::SeqCst) + 1;
        max_active.fetch_max(active_now, Ordering::SeqCst);
        time::sleep(Duration::from_millis(10)).await;

        for _ in 0..operations {
            let request: ClientEnvelope = read_frame(&mut stream).await.expect("client frame");
            if request.seq == 0 {
                let push = ServerEnvelope {
                    cell: request.cell,
                    seq: 0,
                    epoch: request.epoch,
                    request_id: None,
                    payload: ServerMsg::Delta {
                        cell: request.cell,
                        moved: Vec::new(),
                    },
                };
                write_server_frame(&mut stream, &push).await;
            }

            let (request_id, payload) = match request.payload {
                ClientMsg::Join { actor, pos } => (
                    Some(request.seq + 100),
                    ServerMsg::Snapshot {
                        cell: request.cell,
                        actors: vec![ActorState { id: actor, pos }],
                    },
                ),
                ClientMsg::Move { actor, .. } => (
                    Some(request.seq + 100),
                    ServerMsg::Delta {
                        cell: request.cell,
                        moved: vec![ActorState {
                            id: actor,
                            pos: Position::new(0.0, 0.0),
                        }],
                    },
                ),
                ClientMsg::Ping { ts } => (None, ServerMsg::Pong { ts }),
            };
            let reply = ServerEnvelope {
                cell: request.cell,
                seq: request.seq,
                epoch: request.epoch,
                request_id,
                payload,
            };
            write_server_frame(&mut stream, &reply).await;
        }

        active.fetch_sub(1, Ordering::SeqCst);
    }

    enum FailureMode {
        Protocol,
        Timeout,
        ServerClose,
    }

    async fn execute_with_failure_server(
        mode: FailureMode,
        operation_timeout_ms: u64,
    ) -> ExecutionSummary {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr").to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let request: ClientEnvelope = read_frame(&mut stream).await.expect("client frame");
            match mode {
                FailureMode::Protocol => {
                    let reply = ServerEnvelope {
                        cell: request.cell,
                        seq: request.seq,
                        epoch: request.epoch,
                        request_id: Some(1),
                        payload: ServerMsg::Pong { ts: 0 },
                    };
                    write_server_frame(&mut stream, &reply).await;
                }
                FailureMode::Timeout => time::sleep(Duration::from_millis(60)).await,
                FailureMode::ServerClose => {}
            }
        });

        let summary = execute_single_client(addr, operation_timeout_ms).await;
        server.await.expect("failure server");
        summary
    }

    async fn execute_single_client(addr: String, operation_timeout_ms: u64) -> ExecutionSummary {
        let plan = build_plan(&ScenarioConfig {
            clients: 1,
            moves_per_client: 0,
            ..ScenarioConfig::default()
        })
        .expect("build plan");
        execute_plan(
            &plan,
            &ExecutionConfig {
                addr,
                operation_timeout_ms,
                max_concurrency: 1,
            },
        )
        .await
        .expect("execute plan")
    }

    async fn write_server_frame(stream: &mut TcpStream, reply: &ServerEnvelope) {
        stream
            .write_all(&encode_frame(reply))
            .await
            .expect("write server frame");
    }

    fn assert_single_failure(summary: &ExecutionSummary, kind: ExecutionFailureKind) {
        assert_eq!(summary.client_count, 1);
        assert_eq!(summary.completed_clients, 0);
        assert_eq!(summary.failed_clients, 1);
        assert_eq!(summary.operations_completed, 0);
        assert_eq!(summary.failure_count(kind), 1);
    }
}
