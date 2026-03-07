use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use std::fs;
use std::fs::OpenOptions;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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
    Up {
        /// Also start tessera-orch alongside worker/gateway
        #[arg(long, default_value_t = false)]
        with_orch: bool,
        /// Optional orchestrator config path (passed via TESSERA_ORCH_CONFIG)
        #[arg(long)]
        orch_config: Option<PathBuf>,
    },
    /// Stop worker+gateway using recorded PIDs
    Down {
        /// Also stop tessera-orch (if started with --with-orch)
        #[arg(long, default_value_t = false)]
        with_orch: bool,
    },
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
            DevSub::Up {
                with_orch,
                orch_config,
            } => dev_up(with_orch, orch_config)?,
            DevSub::Down { with_orch } => dev_down(with_orch)?,
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

fn dev_up(with_orch: bool, orch_config: Option<PathBuf>) -> Result<()> {
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;

    let gw_pid = pids.join("gateway.pid");
    let wk_pid = pids.join("worker.pid");
    let orch_pid = pids.join("orch.pid");
    if gw_pid.exists() || wk_pid.exists() || (with_orch && orch_pid.exists()) {
        bail!("pid files exist (.dev/pids). Run `cargo xt dev down` first.");
    }

    let worker_addr =
        std::env::var("TESSERA_WORKER_ADDR").unwrap_or_else(|_| "127.0.0.1:5001".into());
    let gateway_addr = std::env::var("TESSERA_GW_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".into());
    let orch_addr = std::env::var("TESSERA_ORCH_ADDR").unwrap_or_else(|_| "127.0.0.1:6000".into());

    let worker_ready_addr = readiness_addr(&worker_addr)?;
    let gateway_ready_addr = readiness_addr(&gateway_addr)?;
    let orch_ready_addr = readiness_addr(&orch_addr)?;

    // Build binaries first for faster start and stable exec paths.
    let mut build = Command::new("cargo");
    build.args([
        "build",
        "--bin",
        "tessera-worker",
        "--bin",
        "tessera-gateway",
    ]);
    if with_orch {
        build.args(["--bin", "tessera-orch"]);
    }
    run(&mut build)?;

    let worker_bin = root.join("target/debug/tessera-worker");
    let gateway_bin = root.join("target/debug/tessera-gateway");
    let orchestrator_bin = root.join("target/debug/tessera-orch");

    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());

    let mut orch_child = None;
    let mut worker_child = None;
    let mut gateway_child = None;

    let startup = (|| -> Result<()> {
        if with_orch {
            let orch_log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(logs.join("orch.log"))?;
            let mut ocmd = Command::new(&orchestrator_bin);
            ocmd.current_dir(&root).env("RUST_LOG", &rust_log);
            if let Some(cfg) = orch_config.as_ref() {
                let cfg_path = if cfg.is_absolute() {
                    cfg.clone()
                } else {
                    root.join(cfg)
                };
                ocmd.env("TESSERA_ORCH_CONFIG", cfg_path);
            }
            let mut child = ocmd
                .stdout(orch_log.try_clone()?)
                .stderr(orch_log)
                .spawn()?;
            wait_for_service_ready("orchestrator", &mut child, orch_ready_addr)?;
            fs::write(&orch_pid, format!("{}\n", child.id()))?;
            orch_child = Some(child);
        }

        let worker_log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(logs.join("worker.log"))?;
        let mut wcmd = Command::new(&worker_bin);
        let mut child = wcmd
            .current_dir(&root)
            .env("RUST_LOG", &rust_log)
            .env("TESSERA_WORKER_ADDR", &worker_addr)
            .stdout(worker_log.try_clone()?)
            .stderr(worker_log)
            .spawn()?;
        wait_for_service_ready("worker", &mut child, worker_ready_addr)?;
        fs::write(&wk_pid, format!("{}\n", child.id()))?;
        worker_child = Some(child);

        let gateway_log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(logs.join("gateway.log"))?;
        let mut gcmd = Command::new(&gateway_bin);
        let mut child = gcmd
            .current_dir(&root)
            .env("RUST_LOG", &rust_log)
            .env("TESSERA_GW_ADDR", &gateway_addr)
            .env("TESSERA_WORKER_ADDR", &worker_addr)
            .stdout(gateway_log.try_clone()?)
            .stderr(gateway_log)
            .spawn()?;
        wait_for_service_ready("gateway", &mut child, gateway_ready_addr)?;
        fs::write(&gw_pid, format!("{}\n", child.id()))?;
        gateway_child = Some(child);

        Ok(())
    })();

    if let Err(err) = startup {
        terminate_child(gateway_child.as_mut(), &gw_pid);
        terminate_child(worker_child.as_mut(), &wk_pid);
        terminate_child(orch_child.as_mut(), &orch_pid);
        return Err(err);
    }

    match orch_child.as_ref() {
        Some(ochild) => {
            println!(
                "dev up: started worker(pid={}), gateway(pid={}), orchestrator(pid={})",
                worker_child.as_ref().expect("worker child").id(),
                gateway_child.as_ref().expect("gateway child").id(),
                ochild.id()
            );
            println!("logs: .dev/logs/worker.log, .dev/logs/gateway.log, .dev/logs/orch.log");
        }
        None => {
            println!(
                "dev up: started worker(pid={}) and gateway(pid={})",
                worker_child.as_ref().expect("worker child").id(),
                gateway_child.as_ref().expect("gateway child").id()
            );
            println!("logs: .dev/logs/worker.log, .dev/logs/gateway.log");
        }
    }
    Ok(())
}

fn readiness_addr(raw: &str) -> Result<SocketAddr> {
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(match addr {
            SocketAddr::V4(v4) if v4.ip().is_unspecified() => {
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4.port())
            }
            SocketAddr::V6(v6) if v6.ip().is_unspecified() => {
                SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.port())
            }
            other => other,
        });
    }

    let mut addrs = raw
        .to_socket_addrs()
        .map_err(|e| anyhow::anyhow!("resolve readiness addr {raw}: {e}"))?;
    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no readiness addr resolved for {raw}"))
}

fn wait_for_service_ready(name: &str, child: &mut Child, addr: SocketAddr) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let connect_timeout = Duration::from_millis(200);

    loop {
        if let Some(status) = child.try_wait()? {
            bail!("{name} exited before becoming ready: {status}");
        }

        match TcpStream::connect_timeout(&addr, connect_timeout) {
            Ok(stream) => {
                let _ = stream.shutdown(Shutdown::Both);
                return Ok(());
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::AddrNotAvailable
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::NotConnected
                ) =>
            {
                if Instant::now() >= deadline {
                    bail!("{name} did not become ready at {addr} within 5s");
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => {
                bail!("failed while waiting for {name} at {addr}: {err}");
            }
        }
    }
}

fn terminate_child(child: Option<&mut Child>, pid_path: &Path) {
    if let Some(child) = child {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = fs::remove_file(pid_path);
}

fn dev_down(with_orch: bool) -> Result<()> {
    let (_dev, _logs, pids) = dev_dirs();
    let gw_pid = pids.join("gateway.pid");
    let wk_pid = pids.join("worker.pid");
    let orch_pid = pids.join("orch.pid");

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
    if with_orch && orch_pid.exists() {
        if let Ok(pid_str) = fs::read_to_string(&orch_pid) {
            let pid = pid_str.trim();
            let _ = Command::new("kill").args(["-TERM", pid]).status();
            killed_any = true;
        }
        let _ = fs::remove_file(&orch_pid);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_addr_maps_unspecified_ipv4_to_loopback() {
        let addr = readiness_addr("0.0.0.0:6000").expect("resolve addr");
        assert_eq!(
            addr,
            "127.0.0.1:6000"
                .parse::<SocketAddr>()
                .expect("parse socket addr")
        );
    }

    #[test]
    fn readiness_addr_maps_unspecified_ipv6_to_loopback() {
        let addr = readiness_addr("[::]:6000").expect("resolve addr");
        assert_eq!(
            addr,
            "[::1]:6000"
                .parse::<SocketAddr>()
                .expect("parse socket addr")
        );
    }

    #[test]
    fn readiness_addr_keeps_specific_socket_addr() {
        let addr = readiness_addr("127.0.0.1:4000").expect("resolve addr");
        assert_eq!(
            addr,
            "127.0.0.1:4000"
                .parse::<SocketAddr>()
                .expect("parse socket addr")
        );
    }
}
