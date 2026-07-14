use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use tessera_core::CellId;

pub const PLAN_SCHEMA_VERSION: &str = "tessera.sim.plan.v1";
pub const DEFAULT_CLIENTS: u32 = 4;
pub const DEFAULT_CELLS: u32 = 1;
pub const DEFAULT_MOVES_PER_CLIENT: u32 = 3;
pub const MAX_CLIENTS: u32 = 10_000;
pub const MAX_CELLS: u32 = 4_096;
pub const MAX_MOVES_PER_CLIENT: u32 = 10_000;
pub const MAX_OPERATIONS: u64 = 1_000_000;

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
    use std::collections::HashSet;

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
}
