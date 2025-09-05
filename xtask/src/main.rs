use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Parser)]
#[command(name = "xtask", version, about = "tessera workspace helper")]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run cargo fmt, clippy, check in order (default)
    Verify,
    Fmt,
    Clippy,
    Check,
    /// Dev helpers: up/down worker+gateway
    Dev {
        #[command(subcommand)]
        sub: DevSub,
    },
}

#[derive(Subcommand)]
enum DevSub {
    /// Build and start worker+gateway in background
    Up,
    /// Stop worker+gateway using recorded PIDs
    Down,
    /// Tail logs in .dev/logs (gateway/worker/all)
    Logs {
        /// Target to tail: gateway|worker|all
        #[arg(long, value_parser = ["gateway","worker","all"], default_value = "all")]
        target: String,
        /// Follow (like tail -f)
        #[arg(long, default_value_t = false)]
        follow: bool,
        /// Number of lines to show (tail -n)
        #[arg(long)]
        lines: Option<usize>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd.unwrap_or(Cmd::Verify) {
        Cmd::Verify => {
            fmt()?;
            clippy()?;
            check()?;
        }
        Cmd::Fmt => fmt()?,
        Cmd::Clippy => clippy()?,
        Cmd::Check => check()?,
        Cmd::Dev { sub } => match sub {
            DevSub::Up => dev_up()?,
            DevSub::Down => dev_down()?,
            DevSub::Logs {
                target,
                follow,
                lines,
            } => dev_logs(&target, follow, lines)?,
        },
    }
    Ok(())
}

fn fmt() -> Result<()> {
    run(Command::new("cargo").args(["fmt", "--all"]))
}
fn clippy() -> Result<()> {
    run(Command::new("cargo").args(["clippy", "--workspace", "--", "-D", "warnings"]))
}
fn check() -> Result<()> {
    run(Command::new("cargo").args(["check", "--workspace"]))
}

fn run(cmd: &mut Command) -> Result<()> {
    let status = cmd
        .current_dir(workspace_root())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;
    if !status.success() {
        bail!("command failed: {:?}", cmd);
    }
    Ok(())
}

/// 워크스페이스 루트를 찾는다: 현재(xtask)에서 상위로 올라가며 [workspace]가 있는 Cargo.toml 탐색
fn workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for _ in 0..4 {
        if has_workspace_toml(dir.join("Cargo.toml")) {
            return dir;
        }
        if !dir.pop() {
            break;
        }
    }
    PathBuf::from(".")
}

fn has_workspace_toml(p: impl AsRef<Path>) -> bool {
    fs::read_to_string(p)
        .map(|s| s.contains("[workspace]"))
        .unwrap_or(false)
}

fn dev_dirs() -> (PathBuf, PathBuf, PathBuf) {
    let root = workspace_root();
    let dev = root.join(".dev");
    let logs = dev.join("logs");
    let pids = dev.join("pids");
    (dev, logs, pids)
}

fn dev_up() -> Result<()> {
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;

    let gw_pid = pids.join("gateway.pid");
    let wk_pid = pids.join("worker.pid");
    if gw_pid.exists() || wk_pid.exists() {
        bail!("pid files exist (.dev/pids). Run `cargo xt dev down` first.");
    }

    // Build binaries first for faster start and stable exec paths
    run(Command::new("cargo").args([
        "build",
        "--bin",
        "tessera-worker",
        "--bin",
        "tessera-gateway",
    ]))?;

    let worker_bin = root.join("target/debug/tessera-worker");
    let gateway_bin = root.join("target/debug/tessera-gateway");

    // Start worker
    let worker_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(logs.join("worker.log"))?;
    let mut wcmd = Command::new(worker_bin);
    let wchild = wcmd
        .current_dir(&root)
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        )
        .env(
            "TESSERA_WORKER_ADDR",
            std::env::var("TESSERA_WORKER_ADDR").unwrap_or_else(|_| "127.0.0.1:5001".into()),
        )
        .stdout(worker_log.try_clone()?)
        .stderr(worker_log)
        .spawn()?;
    fs::write(&wk_pid, format!("{}\n", wchild.id()))?;

    // Start gateway
    let gateway_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(logs.join("gateway.log"))?;
    let mut gcmd = Command::new(gateway_bin);
    let gchild = gcmd
        .current_dir(&root)
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        )
        .env(
            "TESSERA_GW_ADDR",
            std::env::var("TESSERA_GW_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".into()),
        )
        .env(
            "TESSERA_WORKER_ADDR",
            std::env::var("TESSERA_WORKER_ADDR").unwrap_or_else(|_| "127.0.0.1:5001".into()),
        )
        .stdout(gateway_log.try_clone()?)
        .stderr(gateway_log)
        .spawn()?;
    fs::write(&gw_pid, format!("{}\n", gchild.id()))?;

    println!(
        "dev up: started worker(pid={}) and gateway(pid={})",
        wchild.id(),
        gchild.id()
    );
    println!("logs: .dev/logs/worker.log, .dev/logs/gateway.log");
    Ok(())
}

fn dev_down() -> Result<()> {
    let (_dev, _logs, pids) = dev_dirs();
    let gw_pid = pids.join("gateway.pid");
    let wk_pid = pids.join("worker.pid");

    let mut killed_any = false;
    if wk_pid.exists() {
        if let Ok(pid_str) = fs::read_to_string(&wk_pid) {
            let pid = pid_str.trim();
            let _ = Command::new("kill").args(["-TERM", pid]).status();
            killed_any = true;
        }
        let _ = fs::remove_file(&wk_pid);
    }
    if gw_pid.exists() {
        if let Ok(pid_str) = fs::read_to_string(&gw_pid) {
            let pid = pid_str.trim();
            let _ = Command::new("kill").args(["-TERM", pid]).status();
            killed_any = true;
        }
        let _ = fs::remove_file(&gw_pid);
    }
    if killed_any {
        println!("dev down: sent TERM to recorded PIDs");
    } else {
        println!("dev down: no pid files found");
    }
    Ok(())
}

fn dev_logs(target: &str, follow: bool, lines: Option<usize>) -> Result<()> {
    let (_dev, logs, _pids) = dev_dirs();
    let gw = logs.join("gateway.log");
    let wk = logs.join("worker.log");

    // Ensure files exist so tail -f works even before first write
    let _ = OpenOptions::new().create(true).append(true).open(&gw);
    let _ = OpenOptions::new().create(true).append(true).open(&wk);

    let mut args: Vec<String> = Vec::new();
    if let Some(n) = lines {
        args.push("-n".into());
        args.push(n.to_string());
    }
    if follow {
        args.push("-f".into());
    }

    match target {
        "gateway" => args.push(gw.to_string_lossy().into_owned()),
        "worker" => args.push(wk.to_string_lossy().into_owned()),
        _ => {
            args.push(wk.to_string_lossy().into_owned());
            args.push(gw.to_string_lossy().into_owned());
        }
    }

    let mut cmd = Command::new("tail");
    cmd.args(&args);
    run(&mut cmd)
}
