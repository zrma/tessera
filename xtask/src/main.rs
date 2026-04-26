use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

#[derive(Parser)]
#[command(name = "xtask", version, about = "tessera workspace helper")]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run cargo fmt, clippy, check, and harness in order (default)
    Verify,
    Fmt,
    Clippy,
    Check,
    /// Verify repo-local agent harness docs, CI, and architecture guardrails
    Harness,
    /// Dev helpers: up/down worker+gateway, optionally with orchestrator
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
    /// Tail logs in .dev/logs (gateway/worker/orch/all)
    Logs {
        /// Target to tail: gateway|worker|orch|all
        #[arg(long, value_parser = ["gateway","worker","orch","all"], default_value = "all")]
        target: String,
        /// Follow (like tail -f)
        #[arg(long, default_value_t = false)]
        follow: bool,
        /// Number of lines to show (tail -n)
        #[arg(long)]
        lines: Option<usize>,
    },
    /// Start dev stack with metrics enabled and assert /metrics responses
    MetricsSmoke,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd.unwrap_or(Cmd::Verify) {
        Cmd::Verify => {
            fmt()?;
            clippy()?;
            check()?;
            harness()?;
        }
        Cmd::Fmt => fmt()?,
        Cmd::Clippy => clippy()?,
        Cmd::Check => check()?,
        Cmd::Harness => harness()?,
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
            DevSub::MetricsSmoke => dev_metrics_smoke()?,
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

const INTERNAL_CRATES: &[&str] = &[
    "tessera-client",
    "tessera-core",
    "tessera-gateway",
    "tessera-orch",
    "tessera-proto",
    "tessera-sim",
    "tessera-worker",
];

struct CrateBoundary<'a> {
    manifest: &'a str,
    allowed_internal_deps: &'a [&'a str],
}

fn harness() -> Result<()> {
    let root = workspace_root();
    check_harness_docs(&root)?;
    check_crate_boundaries(&root)?;
    println!("harness: docs, CI, and crate dependency guardrails are valid");
    Ok(())
}

fn check_harness_docs(root: &Path) -> Result<()> {
    let required_files: &[(&str, &[&str])] = &[
        (
            "README.md",
            &[
                "## Run Locally",
                "## Status Snapshot",
                "## Design Overview",
                "## Automation Harness",
                "cargo xt harness",
                "cargo xt dev up --with-orch",
                "cargo run -p tessera-client -- ping --ts 123",
                "자동화 에이전트",
            ],
        ),
        (
            "AGENTS.md",
            &[
                "## 자율 수행 원칙",
                "기본값은 자율 진행",
                "사용자를 호출하는 경우",
                "cargo xt harness",
                "cargo test",
                "jj status",
            ],
        ),
        (
            "docs/quality.md",
            &[
                "# Tessera Quality Harness",
                "Last verified: 2026-04-26",
                "Autonomy contract",
                "Feedback loops",
                "Crate boundary policy",
                "cargo xt harness",
            ],
        ),
        (
            ".github/workflows/ci.yml",
            &[
                "cargo xt",
                "cargo test",
                "cargo xt dev up --with-orch",
                "cargo run -p tessera-client -- ping --ts 123",
                "cargo xt dev down --with-orch",
            ],
        ),
    ];

    for (relative_path, needles) in required_files {
        check_file_contains(root, relative_path, needles)?;
    }

    Ok(())
}

fn check_file_contains(root: &Path, relative_path: &str, needles: &[&str]) -> Result<()> {
    let path = root.join(relative_path);
    let contents = fs::read_to_string(&path)
        .map_err(|err| anyhow::anyhow!("harness check failed: read {relative_path}: {err}"))?;

    for needle in needles {
        if !contents.contains(needle) {
            bail!(
                "harness check failed: {relative_path} must mention `{needle}` for agent legibility"
            );
        }
    }

    Ok(())
}

fn check_crate_boundaries(root: &Path) -> Result<()> {
    let boundaries = [
        CrateBoundary {
            manifest: "crates/tessera-core/Cargo.toml",
            allowed_internal_deps: &[],
        },
        CrateBoundary {
            manifest: "crates/tessera-proto/Cargo.toml",
            allowed_internal_deps: &[],
        },
        CrateBoundary {
            manifest: "crates/tessera-gateway/Cargo.toml",
            allowed_internal_deps: &["tessera-core", "tessera-proto"],
        },
        CrateBoundary {
            manifest: "crates/tessera-worker/Cargo.toml",
            allowed_internal_deps: &["tessera-core", "tessera-proto"],
        },
        CrateBoundary {
            manifest: "crates/tessera-orch/Cargo.toml",
            allowed_internal_deps: &["tessera-core", "tessera-proto"],
        },
        CrateBoundary {
            manifest: "crates/tessera-client/Cargo.toml",
            allowed_internal_deps: &["tessera-core"],
        },
        CrateBoundary {
            manifest: "crates/tessera-sim/Cargo.toml",
            allowed_internal_deps: &["tessera-core", "tessera-client"],
        },
    ];

    for boundary in boundaries {
        let contents = fs::read_to_string(root.join(boundary.manifest)).map_err(|err| {
            anyhow::anyhow!("harness check failed: read {}: {err}", boundary.manifest)
        })?;
        let disallowed =
            disallowed_internal_dependencies(&contents, boundary.allowed_internal_deps);
        if !disallowed.is_empty() {
            bail!(
                "harness check failed: {} has disallowed internal deps {:?}; allowed deps are {:?}. Update README Design Overview and xtask harness if this edge is intentional.",
                boundary.manifest,
                disallowed,
                boundary.allowed_internal_deps
            );
        }
    }

    Ok(())
}

fn disallowed_internal_dependencies(
    manifest_contents: &str,
    allowed: &[&str],
) -> Vec<&'static str> {
    internal_crate_dependencies(manifest_contents)
        .into_iter()
        .filter(|dep| !allowed.contains(dep))
        .collect()
}

fn internal_crate_dependencies(manifest_contents: &str) -> Vec<&'static str> {
    let mut deps = Vec::new();

    for line in manifest_contents.lines() {
        let trimmed = line.trim_start();
        for &name in INTERNAL_CRATES {
            if is_dependency_line(trimmed, name) && !deps.contains(&name) {
                deps.push(name);
            }
        }
    }

    deps
}

fn is_dependency_line(line: &str, dependency_name: &str) -> bool {
    let Some(rest) = line.strip_prefix(dependency_name) else {
        return false;
    };
    let rest = rest.trim_start();
    rest.starts_with('=') || rest.starts_with('.')
}

fn dev_dirs() -> (PathBuf, PathBuf, PathBuf) {
    let root = workspace_root();
    let dev = root.join(".dev");
    let logs = dev.join("logs");
    let pids = dev.join("pids");
    (dev, logs, pids)
}

#[derive(Debug, Clone, Default)]
struct DevLaunchOptions {
    gateway_metrics_addr: Option<String>,
    worker_metrics_addr: Option<String>,
    orch_metrics_addr: Option<String>,
}

fn dev_up(with_orch: bool, orch_config: Option<PathBuf>) -> Result<()> {
    dev_up_inner(with_orch, orch_config, DevLaunchOptions::default())
}

fn dev_up_inner(
    with_orch: bool,
    orch_config: Option<PathBuf>,
    options: DevLaunchOptions,
) -> Result<()> {
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
            let orch_log = open_dev_log(&logs, "orch")?;
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
            if let Some(addr) = options.orch_metrics_addr.as_ref() {
                ocmd.env("TESSERA_ORCH_METRICS_ADDR", addr);
            }
            detach_background_process(&mut ocmd);
            let mut child = ocmd
                .stdout(orch_log.try_clone()?)
                .stderr(orch_log)
                .spawn()?;
            wait_for_service_ready("orchestrator", &mut child, orch_ready_addr)?;
            fs::write(&orch_pid, format!("{}\n", child.id()))?;
            orch_child = Some(child);
        }

        let worker_log = open_dev_log(&logs, "worker")?;
        let mut wcmd = Command::new(&worker_bin);
        wcmd.current_dir(&root)
            .env("RUST_LOG", &rust_log)
            .env("TESSERA_WORKER_ADDR", &worker_addr);
        if let Some(addr) = options.worker_metrics_addr.as_ref() {
            wcmd.env("TESSERA_WORKER_METRICS_ADDR", addr);
        }
        detach_background_process(&mut wcmd);
        let mut child = wcmd
            .stdout(worker_log.try_clone()?)
            .stderr(worker_log)
            .spawn()?;
        wait_for_service_ready("worker", &mut child, worker_ready_addr)?;
        fs::write(&wk_pid, format!("{}\n", child.id()))?;
        worker_child = Some(child);

        let gateway_log = open_dev_log(&logs, "gateway")?;
        let mut gcmd = Command::new(&gateway_bin);
        gcmd.current_dir(&root)
            .env("RUST_LOG", &rust_log)
            .env("TESSERA_GW_ADDR", &gateway_addr)
            .env("TESSERA_WORKER_ADDR", &worker_addr);
        if let Some(addr) = options.gateway_metrics_addr.as_ref() {
            gcmd.env("TESSERA_GW_METRICS_ADDR", addr);
        }
        detach_background_process(&mut gcmd);
        let mut child = gcmd
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

fn open_dev_log(logs: &Path, service: &str) -> Result<std::fs::File> {
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(logs.join(format!("{service}.log")))?;
    writeln!(
        log,
        "\n--- dev up service={service} unix_ts={} ---",
        unix_timestamp_secs()
    )?;
    log.flush()?;
    Ok(log)
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn detach_background_process(cmd: &mut Command) {
    #[cfg(unix)]
    {
        // Keep dev services alive after the `cargo xt dev up` process exits.
        cmd.process_group(0);
    }
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

fn dev_metrics_smoke() -> Result<()> {
    let gateway_metrics_addr = "127.0.0.1:4100";
    let worker_metrics_addr = "127.0.0.1:5100";
    let orch_metrics_addr = "127.0.0.1:6100";
    let options = DevLaunchOptions {
        gateway_metrics_addr: Some(gateway_metrics_addr.to_string()),
        worker_metrics_addr: Some(worker_metrics_addr.to_string()),
        orch_metrics_addr: Some(orch_metrics_addr.to_string()),
    };

    dev_up_inner(true, None, options)?;
    let smoke_result = (|| -> Result<()> {
        assert_metrics_endpoint_body(
            "gateway",
            gateway_metrics_addr,
            &[
                "tessera_gateway_ready",
                "tessera_gateway_routes",
                "tessera_gateway_accepted_connections_total",
                "tessera_gateway_upstream_connect_attempts_total",
                "tessera_gateway_ping_roundtrip_seconds_count",
                "tessera_gateway_client_closes_no_route_total",
            ],
        )?;
        assert_http_status_endpoint(
            "gateway readiness",
            gateway_metrics_addr,
            "/ready",
            "200 OK",
        )?;
        assert_metrics_endpoint_body(
            "worker",
            worker_metrics_addr,
            &[
                "tessera_worker_client_subscribers",
                "tessera_worker_accepted_connections_total",
            ],
        )?;
        assert_metrics_endpoint_body(
            "orchestrator",
            orch_metrics_addr,
            &[
                "tessera_orch_configured_workers",
                "tessera_orch_registration_attempts_total",
            ],
        )?;
        run_client_ping(987)?;
        let gateway_metrics = assert_metrics_endpoint_body(
            "gateway",
            gateway_metrics_addr,
            &["tessera_gateway_ping_roundtrip_seconds_count"],
        )?;
        assert_prometheus_sample_at_least(
            "gateway",
            &gateway_metrics,
            "tessera_gateway_ping_roundtrip_seconds_count",
            1.0,
        )?;
        Ok(())
    })();
    let down_result = dev_down(true);

    smoke_result?;
    down_result?;
    println!(
        "metrics smoke: gateway, worker, orchestrator /metrics, gateway /ready, and ping latency histogram are valid"
    );
    Ok(())
}

fn assert_metrics_endpoint_body(
    service: &str,
    raw_addr: &str,
    required_metrics: &[&str],
) -> Result<String> {
    let addr = readiness_addr(raw_addr)?;
    let response = http_get(addr, "/metrics")?;
    assert_prometheus_response(service, &response, required_metrics)?;
    let Some((_, body)) = response.split_once("\r\n\r\n") else {
        bail!("{service} metrics smoke failed: missing HTTP body separator");
    };
    Ok(body.to_string())
}

fn run_client_ping(ts: u64) -> Result<()> {
    let root = workspace_root();
    let mut build = Command::new("cargo");
    build.args(["build", "--bin", "tessera-client"]);
    run(&mut build)?;

    let client = root.join("target/debug/tessera-client");
    let mut ping = Command::new(client);
    ping.args(["ping", "--ts", &ts.to_string()]);
    run(&mut ping)
}

fn assert_http_status_endpoint(
    service: &str,
    raw_addr: &str,
    path: &str,
    expected_status: &str,
) -> Result<()> {
    let addr = readiness_addr(raw_addr)?;
    let response = http_get(addr, path)?;
    let expected = format!("HTTP/1.1 {expected_status}");
    if !response.starts_with(&expected) {
        bail!("{service} smoke failed: expected HTTP {expected_status} response");
    }
    Ok(())
}

fn assert_prometheus_sample_at_least(
    service: &str,
    body: &str,
    metric_name: &str,
    min: f64,
) -> Result<()> {
    let value = prometheus_sample_value(body, metric_name)?;
    if value < min {
        bail!(
            "{service} metrics smoke failed: metric `{metric_name}` value {value} is below {min}"
        );
    }
    Ok(())
}

fn prometheus_sample_value(body: &str, metric_name: &str) -> Result<f64> {
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(name) = parts.next() else {
            continue;
        };
        if name != metric_name {
            continue;
        }
        let Some(value) = parts.next() else {
            bail!("metric `{metric_name}` has no value");
        };
        return value
            .parse::<f64>()
            .map_err(|err| anyhow::anyhow!("metric `{metric_name}` has invalid value: {err}"));
    }
    bail!("metric `{metric_name}` not found")
}

fn http_get(addr: SocketAddr, path: &str) -> Result<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn assert_prometheus_response(
    service: &str,
    response: &str,
    required_metrics: &[&str],
) -> Result<()> {
    if !response.starts_with("HTTP/1.1 200 OK") {
        bail!("{service} metrics smoke failed: expected HTTP 200 response");
    }
    let Some((_, body)) = response.split_once("\r\n\r\n") else {
        bail!("{service} metrics smoke failed: missing HTTP body separator");
    };
    for required in required_metrics {
        if !body.contains(required) {
            bail!("{service} metrics smoke failed: missing metric `{required}`");
        }
    }
    let mut samples = 0_usize;
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(name) = parts.next() else {
            continue;
        };
        let Some(value) = parts.next() else {
            bail!("{service} metrics smoke failed: metric `{name}` has no value");
        };
        value.parse::<f64>().map_err(|err| {
            anyhow::anyhow!(
                "{service} metrics smoke failed: metric `{name}` has invalid value `{value}`: {err}"
            )
        })?;
        samples += 1;
    }
    if samples == 0 {
        bail!("{service} metrics smoke failed: no metric samples found");
    }
    Ok(())
}

fn dev_logs(target: &str, follow: bool, lines: Option<usize>) -> Result<()> {
    let (_dev, logs, _pids) = dev_dirs();
    let gw = logs.join("gateway.log");
    let wk = logs.join("worker.log");
    let orch = logs.join("orch.log");

    // Ensure files exist so tail -f works even before first write
    let _ = OpenOptions::new().create(true).append(true).open(&gw);
    let _ = OpenOptions::new().create(true).append(true).open(&wk);
    let _ = OpenOptions::new().create(true).append(true).open(&orch);

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
        "orch" => args.push(orch.to_string_lossy().into_owned()),
        _ => {
            args.push(wk.to_string_lossy().into_owned());
            args.push(gw.to_string_lossy().into_owned());
            args.push(orch.to_string_lossy().into_owned());
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

    #[test]
    fn prometheus_response_requires_metric_samples() {
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 30\r\n\r\n# TYPE demo gauge\ndemo 1\n";
        assert_prometheus_response("demo", response, &["demo"]).expect("valid metrics");

        let missing = "HTTP/1.1 200 OK\r\n\r\n# TYPE other gauge\nother 1\n";
        let err = assert_prometheus_response("demo", missing, &["demo"])
            .expect_err("missing required metric should fail");
        assert!(err.to_string().contains("missing metric"));

        let invalid = "HTTP/1.1 200 OK\r\n\r\n# TYPE demo gauge\ndemo nope\n";
        let err = assert_prometheus_response("demo", invalid, &["demo"])
            .expect_err("invalid metric value should fail");
        assert!(err.to_string().contains("invalid value"));
    }

    #[test]
    fn prometheus_sample_value_reads_plain_metric() {
        let body = r#"
# TYPE demo_count counter
demo_bucket{le="0.1"} 3
demo_count 4
"#;

        assert_eq!(
            prometheus_sample_value(body, "demo_count").expect("metric value"),
            4.0
        );

        let err =
            prometheus_sample_value(body, "missing_count").expect_err("missing metric should fail");
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn internal_crate_dependencies_extracts_unique_manifest_edges() {
        let manifest = r#"
[package]
name = "tessera-gateway"

[dependencies]
tessera-core = { path = "../tessera-core" }
tessera-proto = { path = "../tessera-proto" }
tessera-core = { path = "../tessera-core" }
        "#;

        assert_eq!(
            internal_crate_dependencies(manifest),
            vec!["tessera-core", "tessera-proto"]
        );
    }

    #[test]
    fn disallowed_internal_dependencies_rejects_runtime_cycles() {
        let manifest = r#"
[dependencies]
tessera-core = { path = "../tessera-core" }
tessera-worker = { path = "../tessera-worker" }
        "#;

        assert_eq!(
            disallowed_internal_dependencies(manifest, &["tessera-core"]),
            vec!["tessera-worker"]
        );
    }
}
