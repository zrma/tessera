use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand};
use tessera_sim::{
    DEFAULT_CELLS, DEFAULT_CLIENTS, DEFAULT_MAX_CONCURRENCY, DEFAULT_MOVES_PER_CLIENT,
    DEFAULT_OPERATION_TIMEOUT_MS, ExecutionConfig, ExecutionFailureKind, ScenarioConfig,
    build_plan, execute_plan,
};

#[derive(Parser, Debug)]
#[command(
    name = "tessera-sim",
    about = "Bounded deterministic simulation harness for Tessera"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Build a deterministic scenario without opening a network connection.
    Plan {
        #[command(flatten)]
        scenario: ScenarioArgs,
        /// Pretty-print the versioned JSON plan.
        #[arg(long)]
        pretty: bool,
    },
    /// Execute a bounded scenario against a Gateway.
    Run {
        #[command(flatten)]
        scenario: ScenarioArgs,
        /// Gateway address (host:port).
        #[arg(long, default_value = "127.0.0.1:4000")]
        addr: String,
        /// Per-connect and per-operation timeout in milliseconds.
        #[arg(long, default_value_t = DEFAULT_OPERATION_TIMEOUT_MS)]
        operation_timeout_ms: u64,
        /// Maximum number of active client sessions.
        #[arg(long, default_value_t = DEFAULT_MAX_CONCURRENCY)]
        max_concurrency: usize,
    },
}

#[derive(Args, Debug)]
struct ScenarioArgs {
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = DEFAULT_CLIENTS)]
    clients: u32,
    #[arg(long, default_value_t = DEFAULT_CELLS)]
    cells: u32,
    #[arg(long, default_value_t = DEFAULT_MOVES_PER_CLIENT)]
    moves_per_client: u32,
    #[arg(long, default_value_t = 1)]
    actor_base: u64,
    #[arg(long, default_value_t = 0)]
    world: u32,
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    start_cx: i32,
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    cy: i32,
}

impl From<ScenarioArgs> for ScenarioConfig {
    fn from(args: ScenarioArgs) -> Self {
        Self {
            seed: args.seed,
            clients: args.clients,
            cells: args.cells,
            moves_per_client: args.moves_per_client,
            actor_base: args.actor_base,
            world: args.world,
            start_cx: args.start_cx,
            cy: args.cy,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Plan { scenario, pretty } => {
            let plan = build_plan(&scenario.into())?;
            let json = if pretty {
                serde_json::to_string_pretty(&plan)?
            } else {
                serde_json::to_string(&plan)?
            };
            println!("{json}");
        }
        Command::Run {
            scenario,
            addr,
            operation_timeout_ms,
            max_concurrency,
        } => {
            let plan = build_plan(&scenario.into())?;
            let summary = execute_plan(
                &plan,
                &ExecutionConfig {
                    addr,
                    operation_timeout_ms,
                    max_concurrency,
                },
            )
            .await?;
            let failures = ExecutionFailureKind::ALL
                .iter()
                .map(|kind| format!("{}:{}", kind.as_str(), summary.failure_count(*kind)))
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "simulation complete: clients={} completed={} failed={} operations={}/{} failures={}",
                summary.client_count,
                summary.completed_clients,
                summary.failed_clients,
                summary.operations_completed,
                plan.operation_count,
                failures
            );
            if summary.failed_clients > 0 {
                bail!(
                    "simulation completed with {} failed clients",
                    summary.failed_clients
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plan_defaults() {
        let cli = Cli::try_parse_from(["tessera-sim", "plan"]).expect("parse defaults");
        let Command::Plan { scenario, pretty } = cli.command else {
            panic!("expected plan command");
        };

        assert_eq!(scenario.seed, 1);
        assert_eq!(scenario.clients, DEFAULT_CLIENTS);
        assert_eq!(scenario.cells, DEFAULT_CELLS);
        assert_eq!(scenario.moves_per_client, DEFAULT_MOVES_PER_CLIENT);
        assert!(!pretty);
    }

    #[test]
    fn parse_plan_overrides() {
        let cli = Cli::try_parse_from([
            "tessera-sim",
            "plan",
            "--seed",
            "42",
            "--clients",
            "8",
            "--cells",
            "4",
            "--moves-per-client",
            "5",
            "--actor-base",
            "100",
            "--world",
            "3",
            "--start-cx",
            "-2",
            "--cy",
            "5",
            "--pretty",
        ])
        .expect("parse overrides");
        let Command::Plan { scenario, pretty } = cli.command else {
            panic!("expected plan command");
        };

        assert_eq!(scenario.seed, 42);
        assert_eq!(scenario.clients, 8);
        assert_eq!(scenario.cells, 4);
        assert_eq!(scenario.moves_per_client, 5);
        assert_eq!(scenario.actor_base, 100);
        assert_eq!(scenario.world, 3);
        assert_eq!(scenario.start_cx, -2);
        assert_eq!(scenario.cy, 5);
        assert!(pretty);
    }

    #[test]
    fn parse_run_defaults_and_bounds() {
        let cli = Cli::try_parse_from([
            "tessera-sim",
            "run",
            "--clients",
            "8",
            "--cells",
            "4",
            "--operation-timeout-ms",
            "500",
            "--max-concurrency",
            "2",
        ])
        .expect("parse run");
        let Command::Run {
            scenario,
            addr,
            operation_timeout_ms,
            max_concurrency,
        } = cli.command
        else {
            panic!("expected run command");
        };

        assert_eq!(scenario.clients, 8);
        assert_eq!(scenario.cells, 4);
        assert_eq!(addr, "127.0.0.1:4000");
        assert_eq!(operation_timeout_ms, 500);
        assert_eq!(max_concurrency, 2);
    }
}
