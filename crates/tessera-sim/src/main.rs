use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use tessera_sim::{
    DEFAULT_CELLS, DEFAULT_CLIENTS, DEFAULT_MOVES_PER_CLIENT, ScenarioConfig, build_plan,
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

fn main() -> Result<()> {
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
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plan_defaults() {
        let cli = Cli::try_parse_from(["tessera-sim", "plan"]).expect("parse defaults");
        let Command::Plan { scenario, pretty } = cli.command;

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
        let Command::Plan { scenario, pretty } = cli.command;

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
}
