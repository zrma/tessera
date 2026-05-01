use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tessera_core::{
    CellId, ClientMsg, EntityId, Envelope, MAX_FRAME_LEN, Position, ServerEnvelope, ServerMsg,
    encode_frame,
};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{
    Assignment, AssignmentListing, HealthCheckRequest, ListAssignmentsRequest, OrchestratorHealth,
    SplitActivationRequest, SplitActivationResponse, SplitActivationState, SplitChildTarget,
};

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
    /// Submit a default-off manual split activation to an orchestrator
    SplitActivation {
        /// Orchestrator gRPC address or endpoint
        #[arg(long, default_value = "127.0.0.1:6000")]
        orch_addr: String,
        /// Operator-chosen operation id
        #[arg(long)]
        operation_id: String,
        /// Parent world id
        #[arg(long, default_value_t = 0)]
        world: u32,
        /// Parent grid x coordinate
        #[arg(long, default_value_t = 0)]
        cx: i32,
        /// Parent grid y coordinate
        #[arg(long, default_value_t = 0)]
        cy: i32,
        /// Child target mapping as sub=worker-id; pass exactly four, sub 0..3
        #[arg(long = "target", required = true)]
        targets: Vec<String>,
    },
    /// Build a read-only operator plan from split/merge preview output
    SplitActivationPlan {
        /// Orchestrator gRPC address or endpoint for health/listing checks
        #[arg(long, default_value = "127.0.0.1:6000")]
        orch_addr: String,
        /// Orchestrator metrics HTTP address that serves /split-merge/preview
        #[arg(long, default_value = "127.0.0.1:6100")]
        preview_addr: String,
        /// Operator-chosen operation id; defaults to a timestamped id
        #[arg(long)]
        operation_id: Option<String>,
        /// Optional explicit child target mapping as sub=worker-id
        #[arg(long = "target")]
        targets: Vec<String>,
        /// Output JSON path. Defaults to .dev/reports/split-activation-plan-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Kubernetes helpers for the internal MicroK8s runtime smoke gate
    K8s {
        #[command(subcommand)]
        sub: Box<K8sSub>,
    },
    /// Dev helpers: up/down worker+gateway, optionally with orchestrator
    Dev {
        #[command(subcommand)]
        sub: DevSub,
    },
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum K8sSub {
    /// Run the port-forwarded internal split activation smoke against a cluster
    ActivationSmoke {
        /// Kubernetes namespace that contains the Tessera runtime
        #[arg(long, default_value = "tessera")]
        namespace: String,
        /// Optional kube context. If unset, the current context is used.
        #[arg(long)]
        context: Option<String>,
        /// Orchestrator service name
        #[arg(long, default_value = "tessera-orch")]
        orch_service: String,
        /// Orchestrator deployment name used for image preflight
        #[arg(long, default_value = "tessera-orch")]
        orch_deploy: String,
        /// Gateway service name
        #[arg(long, default_value = "tessera-gateway")]
        gateway_service: String,
        /// Gateway deployment name used for image preflight
        #[arg(long, default_value = "tessera-gateway")]
        gateway_deploy: String,
        /// Source Worker deployment name used for image preflight
        #[arg(long, default_value = "tessera-worker")]
        source_worker_deploy: String,
        /// Target Worker deployment used for optional failure/recovery smoke
        #[arg(long, default_value = "tessera-worker-b")]
        target_worker_deploy: String,
        /// Worker id represented by --target-worker-deploy
        #[arg(long, default_value = "worker-b")]
        target_worker_id: String,
        /// Require the target Worker deployment/image during read-only preflight
        #[arg(long, default_value_t = false)]
        require_target_worker: bool,
        /// ArgoCD Application namespace used for Synced/Healthy preflight
        #[arg(long, default_value = "argocd")]
        argocd_namespace: String,
        /// ArgoCD Application name used for Synced/Healthy preflight
        #[arg(long, default_value = "tessera")]
        argocd_app: String,
        /// Skip the ArgoCD Application Synced/Healthy preflight
        #[arg(long, default_value_t = false)]
        skip_argocd_check: bool,
        /// Expected runtime image for all Tessera deployments before activation
        #[arg(long)]
        expected_image: Option<String>,
        /// Local Orchestrator gRPC port for kubectl port-forward
        #[arg(long, default_value_t = 6000)]
        local_orch_port: u16,
        /// Local Orchestrator metrics/preview port for kubectl port-forward
        #[arg(long, default_value_t = 6100)]
        local_orch_metrics_port: u16,
        /// Local Gateway TCP port for kubectl port-forward
        #[arg(long, default_value_t = 4000)]
        local_gateway_port: u16,
        /// Local Gateway metrics/readiness port for kubectl port-forward
        #[arg(long, default_value_t = 4100)]
        local_gateway_metrics_port: u16,
        /// Operator-chosen operation id; defaults to a timestamped internal smoke id
        #[arg(long)]
        operation_id: Option<String>,
        /// Optional explicit child target mapping as sub=worker-id
        #[arg(long = "target")]
        targets: Vec<String>,
        /// Actually submit SubmitSplitActivation after writing the read-only plan
        #[arg(long, default_value_t = false)]
        allow_activation: bool,
        /// Also scale the target Worker down/up to verify failure and recovery
        #[arg(long, default_value_t = false)]
        with_failure: bool,
        /// Required with --with-failure because it mutates the target deployment replica count
        #[arg(long, default_value_t = false)]
        allow_scale: bool,
        /// Output JSON path. Defaults to .dev/reports/internal-microk8s-activation-smoke-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Validate an internal activation smoke report JSON
    ActivationReportCheck {
        /// Report JSON path. Defaults to .dev/reports/internal-microk8s-activation-smoke-latest.json
        #[arg(long)]
        report: Option<PathBuf>,
        /// Require a successful split publish report
        #[arg(long, default_value_t = false)]
        require_published: bool,
        /// Require post-publish target outage and restart recovery evidence
        #[arg(long, default_value_t = false)]
        require_failure: bool,
        /// Require all recorded deployment images to match this image
        #[arg(long)]
        expected_image: Option<String>,
        /// Require a blocked/preflight report to contain this substring in preflight_errors[]
        #[arg(long = "expect-preflight-error")]
        expect_preflight_errors: Vec<String>,
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
    /// Start a two-worker dev stack and prove manual split activation converges
    ActivationSmoke,
    /// Start a two-worker dev stack and prove post-publish failure is detected and recoverable
    ActivationFailureSmoke,
    /// Start a two-worker dev stack and prove preview can become an operator plan without mutation
    ActivationPlanSmoke,
    /// Start a two-worker dev stack and run sustained traffic after split activation
    ActivationSoak {
        /// Per-child ping/move iterations after activation publish
        #[arg(long, default_value_t = 32)]
        iterations: u32,
        /// Delay between iterations, in milliseconds
        #[arg(long, default_value_t = 10)]
        sleep_ms: u64,
    },
    /// Validate the latest local split activation plan/smoke/failure/soak reports
    ActivationReportCheck {
        /// Split activation plan report path
        #[arg(long)]
        plan_report: Option<PathBuf>,
        /// Activation success smoke report path
        #[arg(long)]
        activation_report: Option<PathBuf>,
        /// Activation failure/recovery smoke report path
        #[arg(long)]
        failure_report: Option<PathBuf>,
        /// Activation soak report path
        #[arg(long)]
        soak_report: Option<PathBuf>,
        /// Minimum per-child soak iterations expected in the soak report
        #[arg(long, default_value_t = 32)]
        min_soak_iterations: u32,
    },
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
        Cmd::SplitActivation {
            orch_addr,
            operation_id,
            world,
            cx,
            cy,
            targets,
        } => run_split_activation_operator(
            &orch_addr,
            operation_id,
            CellId {
                world,
                cx,
                cy,
                depth: 0,
                sub: 0,
            },
            &targets,
        )?,
        Cmd::SplitActivationPlan {
            orch_addr,
            preview_addr,
            operation_id,
            targets,
            out,
        } => run_split_activation_plan_operator(
            &orch_addr,
            &preview_addr,
            operation_id,
            &targets,
            out.as_deref(),
        )?,
        Cmd::K8s { sub } => match *sub {
            K8sSub::ActivationSmoke {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                source_worker_deploy,
                target_worker_deploy,
                target_worker_id,
                require_target_worker,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                operation_id,
                targets,
                allow_activation,
                with_failure,
                allow_scale,
                out,
            } => run_k8s_activation_smoke(K8sActivationSmokeOptions {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                source_worker_deploy,
                target_worker_deploy,
                target_worker_id,
                require_target_worker,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                operation_id,
                targets,
                allow_activation,
                with_failure,
                allow_scale,
                out,
            })?,
            K8sSub::ActivationReportCheck {
                report,
                require_published,
                require_failure,
                expected_image,
                expect_preflight_errors,
            } => run_k8s_activation_report_check(
                report.as_deref(),
                require_published,
                require_failure,
                expected_image.as_deref(),
                &expect_preflight_errors,
            )?,
        },
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
            DevSub::ActivationSmoke => dev_activation_smoke()?,
            DevSub::ActivationFailureSmoke => dev_activation_failure_smoke()?,
            DevSub::ActivationPlanSmoke => dev_activation_plan_smoke()?,
            DevSub::ActivationSoak {
                iterations,
                sleep_ms,
            } => dev_activation_soak(iterations, sleep_ms)?,
            DevSub::ActivationReportCheck {
                plan_report,
                activation_report,
                failure_report,
                soak_report,
                min_soak_iterations,
            } => run_dev_activation_report_check(DevActivationReportCheckOptions {
                plan_report: plan_report.as_deref(),
                activation_report: activation_report.as_deref(),
                failure_report: failure_report.as_deref(),
                soak_report: soak_report.as_deref(),
                min_soak_iterations,
            })?,
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
    orch_split_merge_preview_json: Option<String>,
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
            if let Some(raw) = options.orch_split_merge_preview_json.as_ref() {
                ocmd.env("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", raw);
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
    let split_merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0},"actor_count":150,"move_queue_pressure":80,"high_pressure_windows":3,"cell_age_secs":120,"owner_worker_id":"worker-local"}]}"#;
    let options = DevLaunchOptions {
        gateway_metrics_addr: Some(gateway_metrics_addr.to_string()),
        worker_metrics_addr: Some(worker_metrics_addr.to_string()),
        orch_metrics_addr: Some(orch_metrics_addr.to_string()),
        orch_split_merge_preview_json: Some(split_merge_preview_json.to_string()),
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
                "tessera_gateway_request_roundtrip_seconds_count",
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
        assert_json_endpoint_contains(
            "orchestrator split/merge preview",
            orch_metrics_addr,
            "/split-merge/preview",
            &[
                "\"mode\": \"dry_run\"",
                "\"source\": \"env:TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON\"",
                "\"assignments_changed\": false",
                "\"kind\": \"split\"",
                "\"cells_moved\": 1",
            ],
        )?;
        run_client_ping(987)?;
        run_client_demo(42)?;
        let gateway_metrics = assert_metrics_endpoint_body(
            "gateway",
            gateway_metrics_addr,
            &[
                "tessera_gateway_ping_roundtrip_seconds_count",
                "tessera_gateway_request_roundtrip_seconds_count",
            ],
        )?;
        assert_prometheus_sample_at_least(
            "gateway",
            &gateway_metrics,
            "tessera_gateway_ping_roundtrip_seconds_count",
            1.0,
        )?;
        assert_prometheus_sample_at_least(
            "gateway",
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
            1.0,
        )?;
        assert_prometheus_sample_at_least(
            "gateway",
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
            1.0,
        )?;
        Ok(())
    })();
    let down_result = dev_down(true);

    smoke_result?;
    down_result?;
    println!(
        "metrics smoke: gateway, worker, orchestrator /metrics, gateway /ready, orchestrator split/merge preview, and ping/non-ping latency histograms are valid"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivationSmokeMode {
    Plan,
    Success,
    PostPublishFailure,
    Soak { iterations: u32, sleep_ms: u64 },
}

fn dev_activation_plan_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::Plan)
}

fn dev_activation_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::Success)
}

fn dev_activation_failure_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::PostPublishFailure)
}

fn dev_activation_soak(iterations: u32, sleep_ms: u64) -> Result<()> {
    if iterations == 0 {
        bail!("activation soak requires --iterations > 0");
    }
    dev_activation_smoke_inner(ActivationSmokeMode::Soak {
        iterations,
        sleep_ms,
    })
}

fn dev_activation_smoke_inner(mode: ActivationSmokeMode) -> Result<()> {
    let gateway_addr = "127.0.0.1:4300";
    let gateway_metrics_addr = "127.0.0.1:4301";
    let worker_a_addr = "127.0.0.1:5301";
    let worker_b_addr = "127.0.0.1:5302";
    let worker_a_metrics_addr = "127.0.0.1:5303";
    let worker_b_metrics_addr = "127.0.0.1:5304";
    let orch_addr = "127.0.0.1:6300";
    let orch_metrics_addr = "127.0.0.1:6301";
    let orch_endpoint = format!("http://{orch_addr}");
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;

    let mut build = Command::new("cargo");
    build.args([
        "build",
        "--bin",
        "tessera-worker",
        "--bin",
        "tessera-gateway",
        "--bin",
        "tessera-orch",
    ]);
    run(&mut build)?;

    let worker_bin = root.join("target/debug/tessera-worker");
    let gateway_bin = root.join("target/debug/tessera-gateway");
    let orchestrator_bin = root.join("target/debug/tessera-orch");
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
    let orch_config_json = format!(
        r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
    );
    let split_merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0},"actor_count":140,"move_queue_pressure":70,"high_pressure_windows":3,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let mut orch_envs = vec![
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];
    if mode == ActivationSmokeMode::Plan {
        orch_envs.push((
            "TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON",
            split_merge_preview_json,
        ));
    }

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "activation-orch",
            bin: &orchestrator_bin,
            ready_addr: orch_addr,
            envs: &orch_envs,
        },
    )?;
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "activation-worker-a",
            bin: &worker_bin,
            ready_addr: worker_a_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-a"),
                ("TESSERA_WORKER_ADDR", worker_a_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_a_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_a_metrics_addr),
                ("TESSERA_ORCH_ADDR", orch_addr),
            ],
        },
    )?;
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "activation-worker-b",
            bin: &worker_bin,
            ready_addr: worker_b_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-b"),
                ("TESSERA_WORKER_ADDR", worker_b_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                ("TESSERA_ORCH_ADDR", orch_addr),
            ],
        },
    )?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;

    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "activation-gateway",
            bin: &gateway_bin,
            ready_addr: gateway_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_GW_ADDR", gateway_addr),
                ("TESSERA_GW_METRICS_ADDR", gateway_metrics_addr),
                ("TESSERA_GW_REFRESH_SECS", "1"),
                ("TESSERA_WORKER_ADDR", worker_a_addr),
                ("TESSERA_ORCH_ADDR", orch_addr),
            ],
        },
    )?;

    assert_http_status_endpoint(
        "activation gateway readiness",
        gateway_metrics_addr,
        "/ready",
        "200 OK",
    )?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;

    if mode == ActivationSmokeMode::Plan {
        let plan_path = default_split_activation_plan_path();
        let plan = build_split_activation_plan(
            &orch_endpoint,
            orch_metrics_addr,
            Some(format!("activation-plan-smoke-{}", unix_timestamp_secs())),
            &[],
        )?;
        if plan.status != "ready" {
            bail!(
                "activation plan smoke expected ready plan, got status={} reason={}",
                plan.status,
                plan.reason
            );
        }
        if plan.recommended_targets.len() != 4
            || plan.recommended_targets[0].worker_id != "worker-a"
            || plan.recommended_targets[1].worker_id != "worker-b"
            || plan.recommended_targets[2].worker_id != "worker-a"
            || plan.recommended_targets[3].worker_id != "worker-b"
        {
            bail!(
                "activation plan smoke produced unexpected targets: {:?}",
                plan.recommended_targets
            );
        }
        let report_path = write_split_activation_plan_report(&plan, Some(&plan_path))?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(CellId::grid(0, 0, 0), "worker-a")],
        ))?;
        println!(
            "activation plan smoke: preview split candidate became a ready operator plan without assignment mutation, report={}",
            report_path.display()
        );
        return Ok(());
    }

    let parent = CellId::grid(0, 0, 0);
    let actor = EntityId(7001);
    let mut session = GatewaySession::connect(gateway_addr)?;
    let joined = session.request(
        parent,
        ClientMsg::Join {
            actor,
            pos: Position { x: 24.0, y: 24.0 },
        },
    )?;
    assert_snapshot_contains("activation parent join", joined, parent, actor)?;

    let operation_prefix = match mode {
        ActivationSmokeMode::Plan => "activation-plan-smoke",
        ActivationSmokeMode::Success => "activation-smoke",
        ActivationSmokeMode::PostPublishFailure => "activation-failure-smoke",
        ActivationSmokeMode::Soak { .. } => "activation-soak",
    };
    let operation_id = format!("{operation_prefix}-{}", unix_timestamp_secs());
    let smoke_targets = vec![
        (0, "worker-a".to_string()),
        (1, "worker-b".to_string()),
        (2, "worker-a".to_string()),
        (3, "worker-b".to_string()),
    ];
    let response = runtime.block_on(submit_split_activation(
        &orch_endpoint,
        operation_id.clone(),
        parent,
        &smoke_targets,
    ))?;
    assert_split_activation_published(&response)?;
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[
            (activation_child_cell(0), "worker-a"),
            (activation_child_cell(1), "worker-b"),
            (activation_child_cell(2), "worker-a"),
            (activation_child_cell(3), "worker-b"),
        ],
    ))?;

    if mode == ActivationSmokeMode::PostPublishFailure {
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        stack.terminate_named("activation-worker-b")?;
        let failure_probe = probe_split_convergence(gateway_addr, 8_100);
        failure_probe.assert_failed_only(&[1, 3])?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[
                (activation_child_cell(0), "worker-a"),
                (activation_child_cell(1), "worker-b"),
                (activation_child_cell(2), "worker-a"),
                (activation_child_cell(3), "worker-b"),
            ],
        ))?;
        let gateway_metrics_after_failure = assert_metrics_endpoint_body(
            "activation gateway",
            gateway_metrics_addr,
            &["tessera_gateway_routes"],
        )?;
        let gateway_routes_after_failure =
            prometheus_sample_value(&gateway_metrics_after_failure, "tessera_gateway_routes")?;

        stack.spawn(
            &root,
            &logs,
            &pids,
            DevProcessSpec {
                name: "activation-worker-b",
                bin: &worker_bin,
                ready_addr: worker_b_addr,
                envs: &[
                    ("RUST_LOG", rust_log.as_str()),
                    ("TESSERA_WORKER_ID", "worker-b"),
                    ("TESSERA_WORKER_ADDR", worker_b_addr),
                    ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                    ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                    ("TESSERA_ORCH_ADDR", orch_addr),
                ],
            },
        )?;
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        let recovery_probe = probe_split_convergence(gateway_addr, 8_200);
        recovery_probe.assert_success()?;
        let report_path = write_activation_failure_smoke_report(ActivationFailureSmokeReport {
            operation_id: &operation_id,
            gateway_addr,
            gateway_metrics_addr,
            orch_addr,
            worker_a_addr,
            worker_b_addr,
            gateway_routes: gateway_routes_after_failure,
            failure_probe: &failure_probe,
            recovery_probe: &recovery_probe,
        })?;
        println!(
            "activation failure smoke: post-publish target outage was detected, child assignments stayed published, worker restart recovered convergence, report={}",
            report_path.display()
        );
        return Ok(());
    }

    if let ActivationSmokeMode::Soak {
        iterations,
        sleep_ms,
    } = mode
    {
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        let stats =
            run_activation_soak_loop(gateway_addr, iterations, Duration::from_millis(sleep_ms))?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[
                (activation_child_cell(0), "worker-a"),
                (activation_child_cell(1), "worker-b"),
                (activation_child_cell(2), "worker-a"),
                (activation_child_cell(3), "worker-b"),
            ],
        ))?;
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;

        let gateway_metrics = assert_metrics_endpoint_body_until(
            "activation gateway",
            gateway_metrics_addr,
            &[
                "tessera_gateway_routes",
                "tessera_gateway_ping_roundtrip_seconds_count",
                "tessera_gateway_request_roundtrip_seconds_count",
                "tessera_gateway_client_closes_no_route_total",
                "tessera_gateway_client_closes_upstream_retry_exhausted_total",
                "tessera_gateway_client_closes_ambiguous_upstream_total",
            ],
        )?;
        assert_prometheus_sample_at_least(
            "activation gateway",
            &gateway_metrics,
            "tessera_gateway_routes",
            4.0,
        )?;
        let expected_child_requests = f64::from(iterations) * 4.0;
        assert_prometheus_sample_at_least(
            "activation gateway",
            &gateway_metrics,
            "tessera_gateway_ping_roundtrip_seconds_count",
            expected_child_requests,
        )?;
        assert_prometheus_sample_at_least(
            "activation gateway",
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
            expected_child_requests,
        )?;
        assert_prometheus_sample_at_least(
            "activation gateway",
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
            5.0,
        )?;
        assert_prometheus_sample_eq(
            "activation gateway",
            &gateway_metrics,
            "tessera_gateway_client_closes_no_route_total",
            0.0,
        )?;
        assert_prometheus_sample_eq(
            "activation gateway",
            &gateway_metrics,
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            0.0,
        )?;
        assert_prometheus_sample_eq(
            "activation gateway",
            &gateway_metrics,
            "tessera_gateway_client_closes_ambiguous_upstream_total",
            0.0,
        )?;
        let gateway_routes = prometheus_sample_value(&gateway_metrics, "tessera_gateway_routes")?;
        let gateway_ping_roundtrips = prometheus_sample_value(
            &gateway_metrics,
            "tessera_gateway_ping_roundtrip_seconds_count",
        )?;
        let gateway_join_roundtrips = prometheus_sample_value(
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
        )?;
        let gateway_move_roundtrips = prometheus_sample_value(
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        )?;

        let worker_a_metrics = assert_metrics_endpoint_body_until(
            "activation worker-a",
            worker_a_metrics_addr,
            &[
                "tessera_worker_accepted_connections_total",
                "tessera_worker_relay_connections_total",
            ],
        )?;
        let worker_b_metrics = assert_metrics_endpoint_body_until(
            "activation worker-b",
            worker_b_metrics_addr,
            &[
                "tessera_worker_accepted_connections_total",
                "tessera_worker_relay_connections_total",
            ],
        )?;
        let report_path = write_activation_soak_report(ActivationSoakReport {
            operation_id: &operation_id,
            gateway_addr,
            gateway_metrics_addr,
            orch_addr,
            worker_a_addr,
            worker_b_addr,
            iterations,
            sleep_ms,
            stats,
            gateway_routes,
            gateway_ping_roundtrips,
            gateway_join_roundtrips,
            gateway_move_roundtrips,
            gateway_no_route_closes: prometheus_sample_value(
                &gateway_metrics,
                "tessera_gateway_client_closes_no_route_total",
            )?,
            gateway_retry_exhausted_closes: prometheus_sample_value(
                &gateway_metrics,
                "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            )?,
            gateway_ambiguous_upstream_closes: prometheus_sample_value(
                &gateway_metrics,
                "tessera_gateway_client_closes_ambiguous_upstream_total",
            )?,
            worker_a_accepted_connections: prometheus_sample_value(
                &worker_a_metrics,
                "tessera_worker_accepted_connections_total",
            )?,
            worker_b_accepted_connections: prometheus_sample_value(
                &worker_b_metrics,
                "tessera_worker_accepted_connections_total",
            )?,
            worker_a_relay_connections: prometheus_sample_value(
                &worker_a_metrics,
                "tessera_worker_relay_connections_total",
            )?,
            worker_b_relay_connections: prometheus_sample_value(
                &worker_b_metrics,
                "tessera_worker_relay_connections_total",
            )?,
        })?;

        println!(
            "activation soak: manual split stayed converged across {iterations} per-child ping/move iterations, remote AOI frames observed, report={}",
            report_path.display()
        );
        return Ok(());
    }

    assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
    for sub in 0..4 {
        assert_gateway_ping_until(
            gateway_addr,
            activation_child_cell(sub),
            7_100 + u64::from(sub),
        )?;
    }

    let remote_aoi_actor = EntityId(7002);
    let _remote_aoi_session = open_gateway_join_until_snapshot(
        gateway_addr,
        activation_child_cell(2),
        remote_aoi_actor,
        Position { x: 8.0, y: 24.0 },
    )?;

    let (moved, remote_aoi_snapshot) = request_move_until_delta_and_snapshot(
        &mut session,
        activation_child_cell(3),
        actor,
        activation_child_cell(2),
        remote_aoi_actor,
    )?;
    assert_delta_contains(
        "activation moved replayed actor",
        moved,
        activation_child_cell(3),
        actor,
    )?;
    assert_snapshot_contains(
        "activation live AOI resync snapshot",
        remote_aoi_snapshot,
        activation_child_cell(2),
        remote_aoi_actor,
    )?;

    let gateway_metrics = assert_metrics_endpoint_body(
        "activation gateway",
        gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_upstream_route_change_reconnects_total",
        ],
    )?;
    assert_prometheus_sample_at_least(
        "activation gateway",
        &gateway_metrics,
        "tessera_gateway_routes",
        4.0,
    )?;
    let gateway_routes = prometheus_sample_value(&gateway_metrics, "tessera_gateway_routes")?;
    assert_prometheus_sample_at_least(
        "activation gateway",
        &gateway_metrics,
        "tessera_gateway_upstream_route_change_reconnects_total",
        1.0,
    )?;
    let gateway_route_change_reconnects = prometheus_sample_value(
        &gateway_metrics,
        "tessera_gateway_upstream_route_change_reconnects_total",
    )?;

    let worker_b_metrics = assert_metrics_endpoint_body(
        "activation worker-b",
        worker_b_metrics_addr,
        &[
            "tessera_worker_relay_connections_total",
            "tessera_worker_accepted_connections_total",
        ],
    )?;
    assert_prometheus_sample_at_least(
        "activation worker-b",
        &worker_b_metrics,
        "tessera_worker_relay_connections_total",
        1.0,
    )?;
    let worker_b_relay_connections =
        prometheus_sample_value(&worker_b_metrics, "tessera_worker_relay_connections_total")?;

    let report_path = write_activation_smoke_report(ActivationSmokeReport {
        operation_id: &operation_id,
        gateway_addr,
        gateway_metrics_addr,
        orch_addr,
        worker_a_addr,
        worker_b_addr,
        gateway_routes,
        gateway_route_change_reconnects,
        worker_b_relay_connections,
    })?;

    println!(
        "activation smoke: manual split published, gateway routes converged to 4 children, source/target workers accepted child traffic, stable-session post-split move and remote AOI resync succeeded, report={}",
        report_path.display()
    );
    Ok(())
}

#[derive(Default)]
struct ManagedDevStack {
    processes: Vec<ManagedDevProcess>,
}

impl ManagedDevStack {
    fn spawn(
        &mut self,
        root: &Path,
        logs: &Path,
        pids: &Path,
        spec: DevProcessSpec<'_>,
    ) -> Result<()> {
        let name = spec.name;
        let pid_path = pids.join(format!("{name}.pid"));
        if pid_path.exists() {
            bail!(
                "pid file exists: {}. Stop the old activation smoke first.",
                pid_path.display()
            );
        }
        let log = open_dev_log(logs, name)?;
        let mut cmd = Command::new(spec.bin);
        cmd.current_dir(root);
        for (key, value) in spec.envs {
            cmd.env(key, value);
        }
        detach_background_process(&mut cmd);
        let mut child = cmd.stdout(log.try_clone()?).stderr(log).spawn()?;
        wait_for_service_ready(name, &mut child, readiness_addr(spec.ready_addr)?)?;
        fs::write(&pid_path, format!("{}\n", child.id()))?;
        self.processes.push(ManagedDevProcess {
            name: name.to_string(),
            child,
            pid_path,
        });
        Ok(())
    }

    fn terminate_named(&mut self, name: &str) -> Result<()> {
        let Some(index) = self
            .processes
            .iter()
            .position(|process| process.name == name)
        else {
            bail!("activation dev process `{name}` is not running");
        };
        let mut process = self.processes.remove(index);
        process.terminate();
        Ok(())
    }
}

struct DevProcessSpec<'a> {
    name: &'a str,
    bin: &'a Path,
    ready_addr: &'a str,
    envs: &'a [(&'a str, &'a str)],
}

impl Drop for ManagedDevStack {
    fn drop(&mut self) {
        while let Some(mut process) = self.processes.pop() {
            process.terminate();
        }
    }
}

struct ManagedDevProcess {
    name: String,
    child: Child,
    pid_path: PathBuf,
}

impl ManagedDevProcess {
    fn terminate(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        let _ = fs::remove_file(&self.pid_path);
        println!("dev activation smoke: stopped {}", self.name);
    }
}

struct GatewaySession {
    stream: TcpStream,
    seq: u64,
}

impl GatewaySession {
    fn connect(raw_addr: &str) -> Result<Self> {
        let addr = readiness_addr(raw_addr)?;
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
            .with_context(|| format!("connect gateway at {addr}"))?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(Duration::from_secs(3)))?;
        Ok(Self { stream, seq: 0 })
    }

    fn request(&mut self, cell: CellId, payload: ClientMsg) -> Result<ServerEnvelope> {
        self.send(cell, payload)?;
        self.recv()
    }

    fn send(&mut self, cell: CellId, payload: ClientMsg) -> Result<()> {
        let env = Envelope {
            cell,
            seq: self.seq,
            epoch: 0,
            payload,
        };
        self.seq = self.seq.wrapping_add(1);
        let frame = encode_frame(&env);
        self.stream.write_all(frame.as_ref())?;
        Ok(())
    }

    fn recv(&mut self) -> Result<ServerEnvelope> {
        read_server_envelope(&mut self.stream)
    }
}

fn read_server_envelope(stream: &mut TcpStream) -> Result<ServerEnvelope> {
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        bail!("gateway frame length {len} exceeds max {MAX_FRAME_LEN}");
    }
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload)?;
    Ok(serde_json::from_slice(&payload)?)
}

fn activation_child_cell(sub: u8) -> CellId {
    CellId {
        world: 0,
        cx: 0,
        cy: 0,
        depth: 1,
        sub,
    }
}

fn assert_snapshot_contains(
    context: &str,
    envelope: ServerEnvelope,
    expected_cell: CellId,
    expected_actor: EntityId,
) -> Result<()> {
    if envelope.cell != expected_cell {
        bail!(
            "{context}: reply cell {:?} did not match {:?}",
            envelope.cell,
            expected_cell
        );
    }
    match envelope.payload {
        ServerMsg::Snapshot { cell, actors } if cell == expected_cell => {
            if actors.iter().any(|actor| actor.id == expected_actor) {
                Ok(())
            } else {
                bail!(
                    "{context}: snapshot did not include actor {:?}",
                    expected_actor
                )
            }
        }
        ServerMsg::Error { code, message } => bail!("{context}: worker returned {code}: {message}"),
        other => bail!("{context}: expected snapshot, got {other:?}"),
    }
}

fn assert_delta_contains(
    context: &str,
    envelope: ServerEnvelope,
    expected_cell: CellId,
    expected_actor: EntityId,
) -> Result<()> {
    if envelope.cell != expected_cell {
        bail!(
            "{context}: reply cell {:?} did not match {:?}",
            envelope.cell,
            expected_cell
        );
    }
    match envelope.payload {
        ServerMsg::Delta { cell, moved } if cell == expected_cell => {
            if moved.iter().any(|actor| actor.id == expected_actor) {
                Ok(())
            } else {
                bail!(
                    "{context}: delta did not include actor {:?}",
                    expected_actor
                )
            }
        }
        ServerMsg::Error { code, message } => bail!("{context}: worker returned {code}: {message}"),
        other => bail!("{context}: expected delta, got {other:?}"),
    }
}

fn assert_gateway_ping_until(raw_addr: &str, cell: CellId, ts: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match GatewaySession::connect(raw_addr)
            .and_then(|mut session| session.request(cell, ClientMsg::Ping { ts }))
            .and_then(|reply| assert_pong("activation child ping", reply, cell, ts))
        {
            Ok(()) => return Ok(()),
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
}

fn open_gateway_join_until_snapshot(
    raw_addr: &str,
    cell: CellId,
    actor: EntityId,
    pos: Position,
) -> Result<GatewaySession> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match GatewaySession::connect(raw_addr).and_then(|mut session| {
            let reply = session.request(cell, ClientMsg::Join { actor, pos })?;
            assert_snapshot_contains("activation child join", reply, cell, actor)?;
            Ok(session)
        }) {
            Ok(session) => return Ok(session),
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
}

fn assert_pong(
    context: &str,
    envelope: ServerEnvelope,
    expected_cell: CellId,
    expected_ts: u64,
) -> Result<()> {
    if envelope.cell != expected_cell {
        bail!(
            "{context}: reply cell {:?} did not match {:?}",
            envelope.cell,
            expected_cell
        );
    }
    match envelope.payload {
        ServerMsg::Pong { ts } if ts == expected_ts => Ok(()),
        ServerMsg::Error { code, message } => bail!("{context}: worker returned {code}: {message}"),
        other => bail!("{context}: expected pong ts={expected_ts}, got {other:?}"),
    }
}

fn request_move_until_delta_and_snapshot(
    session: &mut GatewaySession,
    move_cell: CellId,
    actor: EntityId,
    snapshot_cell: CellId,
    snapshot_actor: EntityId,
) -> Result<(ServerEnvelope, ServerEnvelope)> {
    let deadline = Instant::now() + Duration::from_secs(5);
    session.send(
        move_cell,
        ClientMsg::Move {
            actor,
            dx: 1.0,
            dy: 1.0,
        },
    )?;
    let mut moved = None;
    let mut snapshot = None;
    loop {
        if moved.is_some() && snapshot.is_some() {
            return Ok((
                moved.take().expect("moved checked"),
                snapshot.take().expect("snapshot checked"),
            ));
        }
        let reply = match session.recv() {
            Ok(reply) => reply,
            Err(err) if is_temporary_io_error(&err) && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(err) => return Err(err),
        };
        if reply_is_correlated_delta_for_actor(&reply, actor) {
            moved = Some(reply);
            continue;
        }
        if reply_is_snapshot_for_actor(&reply, snapshot_cell, snapshot_actor) {
            snapshot = Some(reply);
            continue;
        }
        match &reply.payload {
            ServerMsg::Delta { moved, .. } if moved.iter().any(|moved| moved.id == actor) => {
                if Instant::now() >= deadline {
                    bail!("activation child move: timed out waiting for correlated move reply");
                }
            }
            ServerMsg::Error { code, .. }
                if code == "cell_not_owned" && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(100));
                session.send(
                    move_cell,
                    ClientMsg::Move {
                        actor,
                        dx: 1.0,
                        dy: 1.0,
                    },
                )?;
            }
            ServerMsg::Error { code, message } => {
                bail!("activation child move: worker returned {code}: {message}");
            }
            other if Instant::now() < deadline => {
                let _ = other;
                thread::sleep(Duration::from_millis(100));
            }
            other => bail!("activation child move: expected delta, got {other:?}"),
        }
    }
}

fn is_temporary_io_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>()
        .is_some_and(|io_err| matches!(io_err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut))
}

fn reply_is_correlated_delta_for_actor(envelope: &ServerEnvelope, actor: EntityId) -> bool {
    matches!(
        &envelope.payload,
        ServerMsg::Delta { moved, .. }
            if envelope.request_id.is_some() && moved.iter().any(|moved| moved.id == actor)
    )
}

fn reply_is_snapshot_for_actor(
    envelope: &ServerEnvelope,
    expected_cell: CellId,
    expected_actor: EntityId,
) -> bool {
    if envelope.cell != expected_cell {
        return false;
    }
    matches!(
        &envelope.payload,
        ServerMsg::Snapshot { cell, actors }
            if *cell == expected_cell && actors.iter().any(|actor| actor.id == expected_actor)
    )
}

#[derive(Debug, Clone, Copy, Default)]
struct ActivationSoakStats {
    pings_ok: u64,
    moves_ok: u64,
    ignored_frames: u64,
    remote_delta_frames: u64,
    remote_snapshot_frames: u64,
}

impl ActivationSoakStats {
    fn add(&mut self, observed: ActivationSoakObservedFrames) {
        self.ignored_frames += observed.ignored_frames;
        self.remote_delta_frames += observed.remote_delta_frames;
        self.remote_snapshot_frames += observed.remote_snapshot_frames;
    }

    fn assert_remote_aoi_observed(&self) -> Result<()> {
        if self.remote_delta_frames == 0 && self.remote_snapshot_frames == 0 {
            bail!("activation soak did not observe any remote AOI frames during sustained traffic");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ActivationSoakObservedFrames {
    ignored_frames: u64,
    remote_delta_frames: u64,
    remote_snapshot_frames: u64,
}

impl ActivationSoakObservedFrames {
    fn record_ignored(&mut self, envelope: &ServerEnvelope, expected_cell: CellId) {
        self.ignored_frames += 1;
        if envelope.cell == expected_cell {
            return;
        }
        match &envelope.payload {
            ServerMsg::Delta { .. } => self.remote_delta_frames += 1,
            ServerMsg::Snapshot { .. } => self.remote_snapshot_frames += 1,
            _ => {}
        }
    }
}

struct ActivationSoakSession {
    cell: CellId,
    actor: EntityId,
    session: GatewaySession,
}

fn run_activation_soak_loop(
    gateway_addr: &str,
    iterations: u32,
    sleep: Duration,
) -> Result<ActivationSoakStats> {
    let mut sessions = Vec::new();
    for sub in 0..4 {
        let cell = activation_child_cell(sub);
        let actor = EntityId(7_300 + u64::from(sub));
        let session = open_gateway_join_until_snapshot(
            gateway_addr,
            cell,
            actor,
            activation_soak_position(sub),
        )?;
        sessions.push(ActivationSoakSession {
            cell,
            actor,
            session,
        });
    }

    let mut stats = ActivationSoakStats::default();
    for iteration in 0..iterations {
        for session in &mut sessions {
            let ts = 9_000 + u64::from(iteration) * 10 + u64::from(session.cell.sub);
            let observed = request_ping_until_pong(
                &mut session.session,
                session.cell,
                ts,
                "activation soak ping",
            )?;
            stats.add(observed);
            stats.pings_ok += 1;

            let step = if iteration % 2 == 0 { 0.25 } else { -0.25 };
            let observed = request_move_until_delta(
                &mut session.session,
                session.cell,
                session.actor,
                step,
                step,
                "activation soak move",
            )?;
            stats.add(observed);
            stats.moves_ok += 1;
        }
        if !sleep.is_zero() {
            thread::sleep(sleep);
        }
    }

    stats.assert_remote_aoi_observed()?;
    Ok(stats)
}

fn activation_soak_position(sub: u8) -> Position {
    match sub {
        0 => Position { x: 8.0, y: 8.0 },
        1 => Position { x: 24.0, y: 8.0 },
        2 => Position { x: 8.0, y: 24.0 },
        _ => Position { x: 24.0, y: 24.0 },
    }
}

fn request_ping_until_pong(
    session: &mut GatewaySession,
    cell: CellId,
    ts: u64,
    context: &str,
) -> Result<ActivationSoakObservedFrames> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = ActivationSoakObservedFrames::default();
    session.send(cell, ClientMsg::Ping { ts })?;
    loop {
        let reply = match session.recv() {
            Ok(reply) => reply,
            Err(err) if is_temporary_io_error(&err) && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(err) => return Err(err),
        };
        if reply.cell == cell && matches!(reply.payload, ServerMsg::Pong { ts: got } if got == ts) {
            return Ok(observed);
        }
        if let ServerMsg::Error { code, message } = &reply.payload {
            bail!("{context}: worker returned {code}: {message}");
        }
        observed.record_ignored(&reply, cell);
        if Instant::now() >= deadline {
            bail!("{context}: timed out waiting for pong ts={ts}");
        }
    }
}

fn request_move_until_delta(
    session: &mut GatewaySession,
    cell: CellId,
    actor: EntityId,
    dx: f32,
    dy: f32,
    context: &str,
) -> Result<ActivationSoakObservedFrames> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = ActivationSoakObservedFrames::default();
    session.send(cell, ClientMsg::Move { actor, dx, dy })?;
    loop {
        let reply = match session.recv() {
            Ok(reply) => reply,
            Err(err) if is_temporary_io_error(&err) && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(err) => return Err(err),
        };
        if reply.cell == cell && reply_is_correlated_delta_for_actor(&reply, actor) {
            return Ok(observed);
        }
        if let ServerMsg::Error { code, message } = &reply.payload {
            bail!("{context}: worker returned {code}: {message}");
        }
        observed.record_ignored(&reply, cell);
        if Instant::now() >= deadline {
            bail!("{context}: timed out waiting for correlated delta for actor {actor:?}");
        }
    }
}

fn assert_gateway_ready_routes(raw_addr: &str, expected_routes: u64) -> Result<()> {
    let addr = readiness_addr(raw_addr)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match http_get(addr, "/ready").and_then(|response| readiness_routes(&response)) {
            Ok(routes) if routes == expected_routes => return Ok(()),
            Ok(_) | Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Ok(routes) => bail!(
                "gateway readiness smoke failed: expected {expected_routes} routes, got {routes}"
            ),
            Err(err) => return Err(err),
        }
    }
}

fn readiness_routes(response: &str) -> Result<u64> {
    if !response.starts_with("HTTP/1.1 200 OK") {
        bail!("gateway readiness smoke failed: expected HTTP 200 response");
    }
    let Some((_, body)) = response.split_once("\r\n\r\n") else {
        bail!("gateway readiness smoke failed: missing HTTP body separator");
    };
    for line in body.lines() {
        let Some(raw_routes) = line.strip_prefix("routes ") else {
            continue;
        };
        return raw_routes.parse::<u64>().map_err(|err| {
            anyhow::anyhow!("invalid readiness routes value `{raw_routes}`: {err}")
        });
    }
    bail!("gateway readiness smoke failed: missing routes line")
}

async fn wait_for_orchestrator_registered(endpoint: &str, expected_registered: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match OrchestratorClient::connect(endpoint.to_string()).await {
            Ok(mut client) => {
                match client
                    .get_health(tonic::Request::new(HealthCheckRequest {}))
                    .await
                {
                    Ok(response) => {
                        let health = response.into_inner();
                        if health.registered_workers == expected_registered
                            && health.status == "SERVING"
                        {
                            return Ok(());
                        }
                    }
                    Err(err) if Instant::now() >= deadline => return Err(err.into()),
                    Err(_) => {}
                }
            }
            Err(err) if Instant::now() >= deadline => return Err(err.into()),
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            bail!(
                "orchestrator did not report {expected_registered} registered workers before timeout"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Debug)]
struct K8sActivationSmokeOptions {
    namespace: String,
    context: Option<String>,
    orch_service: String,
    orch_deploy: String,
    gateway_service: String,
    gateway_deploy: String,
    source_worker_deploy: String,
    target_worker_deploy: String,
    target_worker_id: String,
    require_target_worker: bool,
    argocd_namespace: String,
    argocd_app: String,
    skip_argocd_check: bool,
    expected_image: Option<String>,
    local_orch_port: u16,
    local_orch_metrics_port: u16,
    local_gateway_port: u16,
    local_gateway_metrics_port: u16,
    operation_id: Option<String>,
    targets: Vec<String>,
    allow_activation: bool,
    with_failure: bool,
    allow_scale: bool,
    out: Option<PathBuf>,
}

fn run_k8s_activation_smoke(options: K8sActivationSmokeOptions) -> Result<()> {
    if options.with_failure && !options.allow_scale {
        bail!(
            "internal k8s failure smoke scales a deployment; pass --allow-scale with --with-failure"
        );
    }

    let context = resolve_kube_context(options.context.as_deref())?;
    let operation_id = options
        .operation_id
        .clone()
        .unwrap_or_else(|| format!("internal-smoke-{}", unix_timestamp_secs()));
    let preflight_result = k8s_activation_preflight(&context, &options);
    let preflight = preflight_result.preflight;
    if !preflight_result.errors.is_empty() {
        let reason = format!(
            "internal k8s activation preflight failed: {}",
            preflight_result.errors.join("; ")
        );
        let report_path = write_internal_k8s_activation_preflight_report(
            InternalK8sActivationPreflightReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                target_worker_deploy: &options.target_worker_deploy,
                target_worker_id: &options.target_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: None,
                plan: None,
                stage: "blocked_before_plan",
                activation_allowed: options.allow_activation,
                reason: &reason,
                preflight_errors: &preflight_result.errors,
            },
            options.out.as_deref(),
        )?;
        bail!("{reason}; internal report={}", report_path.display());
    }
    let image_validation = options.expected_image.as_ref().map(|expected_image| {
        validate_k8s_deployment_images(&preflight.deployment_images, expected_image)
    });
    if let Some(Err(err)) = image_validation {
        let reason = err.to_string();
        let report_path = write_internal_k8s_activation_preflight_report(
            InternalK8sActivationPreflightReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                target_worker_deploy: &options.target_worker_deploy,
                target_worker_id: &options.target_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: None,
                plan: None,
                stage: "blocked_before_plan",
                activation_allowed: options.allow_activation,
                reason: &reason,
                preflight_errors: &[],
            },
            options.out.as_deref(),
        )?;
        bail!("{reason}; internal report={}", report_path.display());
    }

    let local_orch_addr = format!("127.0.0.1:{}", options.local_orch_port);
    let local_orch_metrics_addr = format!("127.0.0.1:{}", options.local_orch_metrics_port);
    let local_gateway_addr = format!("127.0.0.1:{}", options.local_gateway_port);
    let local_gateway_metrics_addr = format!("127.0.0.1:{}", options.local_gateway_metrics_port);
    let mut forwards = ManagedK8sPortForwards::default();
    forwards.spawn(
        &context,
        &options.namespace,
        &options.orch_service,
        "orchestrator",
        &[
            (options.local_orch_port, 6000),
            (options.local_orch_metrics_port, 6100),
        ],
    )?;
    forwards.spawn(
        &context,
        &options.namespace,
        &options.gateway_service,
        "gateway",
        &[
            (options.local_gateway_port, 4000),
            (options.local_gateway_metrics_port, 4100),
        ],
    )?;

    let orch_endpoint = grpc_endpoint(&local_orch_addr);
    let plan = build_split_activation_plan(
        &orch_endpoint,
        &local_orch_metrics_addr,
        Some(operation_id.clone()),
        &options.targets,
    )?;
    let plan_report_path = write_split_activation_plan_report(&plan, None)?;
    if plan.status != "ready" {
        let blocked_report = write_internal_k8s_activation_preflight_report(
            InternalK8sActivationPreflightReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                target_worker_deploy: &options.target_worker_deploy,
                target_worker_id: &options.target_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: Some(&plan_report_path),
                plan: Some(&plan),
                stage: "blocked_before_activation",
                activation_allowed: options.allow_activation,
                reason: &plan.reason,
                preflight_errors: &[],
            },
            options.out.as_deref(),
        )?;
        bail!(
            "internal k8s activation smoke blocked before mutation: status={} reason={}; plan report={}; internal report={}",
            plan.status,
            plan.reason,
            plan_report_path.display(),
            blocked_report.display()
        );
    }
    println!(
        "internal k8s activation plan: ready without mutation, report={}",
        plan_report_path.display()
    );

    if !options.allow_activation {
        let preflight_report = write_internal_k8s_activation_preflight_report(
            InternalK8sActivationPreflightReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                target_worker_deploy: &options.target_worker_deploy,
                target_worker_id: &options.target_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: Some(&plan_report_path),
                plan: Some(&plan),
                stage: "planned_without_activation",
                activation_allowed: false,
                reason: "plan is ready; --allow-activation was not provided",
                preflight_errors: &[],
            },
            options.out.as_deref(),
        )?;
        println!(
            "internal k8s activation smoke stopped before mutation; rerun with --allow-activation during the controlled smoke window, report={}",
            preflight_report.display()
        );
        return Ok(());
    }

    assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
    let gateway_metrics_before = assert_metrics_endpoint_body_until(
        "internal k8s gateway",
        &local_gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_close_before = gateway_close_counters_from_metrics(&gateway_metrics_before)?;

    let parent = plan
        .parent
        .ok_or_else(|| anyhow::anyhow!("ready split activation plan had no parent cell"))?;
    let targets = plan
        .recommended_targets
        .iter()
        .map(|target| (target.sub, target.worker_id.clone()))
        .collect::<Vec<_>>();
    let expected_listing = plan
        .recommended_targets
        .iter()
        .map(|target| {
            (
                activation_child_cell(target.sub as u8),
                target.worker_id.as_str(),
            )
        })
        .collect::<Vec<_>>();
    let runtime = tokio::runtime::Runtime::new()?;
    let response = runtime.block_on(submit_split_activation(
        &orch_endpoint,
        operation_id.clone(),
        parent,
        &targets,
    ))?;
    print_split_activation_response(&response)?;
    assert_split_activation_published(&response)?;
    runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
    assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
    let success_probe = probe_split_convergence(&local_gateway_addr, 12_000);
    success_probe.assert_success()?;

    let gateway_metrics_after_success = assert_metrics_endpoint_body_until(
        "internal k8s gateway",
        &local_gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_close_after_success =
        gateway_close_counters_from_metrics(&gateway_metrics_after_success)?;
    assert_gateway_close_counters_not_increased(
        "internal k8s gateway success smoke",
        gateway_close_before,
        gateway_close_after_success,
    )?;

    let mut failure_probe = None;
    let mut recovery_probe = None;
    if options.with_failure {
        let expected_failed_subs =
            target_subs_for_worker(&plan.recommended_targets, &options.target_worker_id)?;
        let original_replicas = kubectl_deploy_spec_replicas(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
        )?;
        if original_replicas == 0 {
            bail!(
                "target deployment {} already has 0 replicas; cannot run failure/recovery smoke",
                options.target_worker_deploy
            );
        }
        kubectl_scale_deploy(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            0,
        )?;
        let mut restore = K8sScaleRestore::new(
            context.clone(),
            options.namespace.clone(),
            options.target_worker_deploy.clone(),
            original_replicas,
        );
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            0,
        )?;
        let observed_failure = probe_split_convergence(&local_gateway_addr, 12_100);
        let failure_assert = observed_failure.assert_failed_only(&expected_failed_subs);
        runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;

        kubectl_scale_deploy(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            original_replicas,
        )?;
        restore.disarm();
        kubectl_rollout_status(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            original_replicas,
        )?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
        let observed_recovery = probe_split_convergence(&local_gateway_addr, 12_200);
        observed_recovery.assert_success()?;

        failure_assert?;
        failure_probe = Some(observed_failure);
        recovery_probe = Some(observed_recovery);
    }

    let report_path = write_internal_k8s_activation_report(
        InternalK8sActivationReport {
            context: &context,
            namespace: &options.namespace,
            orch_service: &options.orch_service,
            gateway_service: &options.gateway_service,
            target_worker_deploy: &options.target_worker_deploy,
            target_worker_id: &options.target_worker_id,
            argocd_namespace: &options.argocd_namespace,
            argocd_app: &options.argocd_app,
            argocd_status: preflight.argocd_status.as_ref(),
            deployment_images: &preflight.deployment_images,
            expected_image: options.expected_image.as_deref(),
            operation_id: &operation_id,
            plan_report_path: &plan_report_path,
            plan: &plan,
            response: &response,
            success_probe: &success_probe,
            failure_probe: failure_probe.as_ref(),
            recovery_probe: recovery_probe.as_ref(),
            gateway_close_before,
            gateway_close_after_success,
            with_failure: options.with_failure,
        },
        options.out.as_deref(),
    )?;
    println!(
        "internal k8s activation smoke: split published and converged{}; report={}",
        if options.with_failure {
            ", failure/recovery path verified"
        } else {
            ""
        },
        report_path.display()
    );
    Ok(())
}

fn resolve_kube_context(context: Option<&str>) -> Result<String> {
    if let Some(context) = context {
        if context.trim().is_empty() {
            bail!("--context must not be empty");
        }
        return Ok(context.to_string());
    }
    let mut cmd = Command::new("kubectl");
    cmd.args(["config", "current-context"]);
    let output = command_stdout(&mut cmd)?;
    let context = output.trim();
    if context.is_empty() {
        bail!("kubectl current-context returned an empty context");
    }
    Ok(context.to_string())
}

fn k8s_activation_preflight(
    context: &str,
    options: &K8sActivationSmokeOptions,
) -> K8sActivationPreflightResult {
    let mut errors = Vec::new();
    let argocd_status = if options.skip_argocd_check {
        None
    } else {
        match kubectl_argocd_app_status(context, &options.argocd_namespace, &options.argocd_app) {
            Ok(status) => {
                if let Err(err) = validate_argocd_app_ready(&status) {
                    errors.push(err.to_string());
                }
                Some(status)
            }
            Err(err) => {
                errors.push(format!(
                    "ArgoCD application status preflight failed: {err:#}"
                ));
                None
            }
        }
    };
    let (deployment_images, deployment_errors) =
        collect_k8s_runtime_deployment_images(context, options);
    errors.extend(deployment_errors);
    for resource in [
        format!("svc/{}", options.orch_service),
        format!("svc/{}", options.gateway_service),
    ] {
        if let Err(err) = kubectl_resource_name(context, &options.namespace, &resource) {
            errors.push(format!(
                "required resource {resource} is not ready: {err:#}"
            ));
        }
    }
    if options.with_failure || options.require_target_worker {
        let resource = format!("deploy/{}", options.target_worker_deploy);
        if let Err(err) = kubectl_resource_name(context, &options.namespace, &resource) {
            errors.push(format!(
                "required resource {resource} is not ready: {err:#}"
            ));
        }
    }
    K8sActivationPreflightResult {
        preflight: K8sActivationPreflight {
            argocd_status,
            deployment_images,
        },
        errors,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArgoCdAppStatus {
    sync: String,
    health: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sActivationPreflight {
    argocd_status: Option<ArgoCdAppStatus>,
    deployment_images: Vec<K8sDeploymentImage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sActivationPreflightResult {
    preflight: K8sActivationPreflight,
    errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sDeploymentImage {
    role: &'static str,
    deployment: String,
    image: String,
}

fn kubectl_argocd_app_status(context: &str, namespace: &str, app: &str) -> Result<ArgoCdAppStatus> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args([
        "get",
        "application",
        app,
        "-o",
        "jsonpath={.status.sync.status} {.status.health.status}",
    ]);
    parse_argocd_app_status(&command_stdout(&mut cmd)?)
}

fn parse_argocd_app_status(raw: &str) -> Result<ArgoCdAppStatus> {
    let mut parts = raw.split_whitespace();
    let Some(sync) = parts.next() else {
        bail!("ArgoCD application status is missing sync status");
    };
    let Some(health) = parts.next() else {
        bail!("ArgoCD application status is missing health status");
    };
    Ok(ArgoCdAppStatus {
        sync: sync.to_string(),
        health: health.to_string(),
    })
}

fn validate_argocd_app_ready(status: &ArgoCdAppStatus) -> Result<()> {
    if status.sync == "Synced" && status.health == "Healthy" {
        return Ok(());
    }
    bail!(
        "ArgoCD application must be Synced and Healthy before internal activation smoke; got sync={} health={}",
        status.sync,
        status.health
    )
}

fn collect_k8s_runtime_deployment_images(
    context: &str,
    options: &K8sActivationSmokeOptions,
) -> (Vec<K8sDeploymentImage>, Vec<String>) {
    let mut deployments = vec![
        ("orchestrator", options.orch_deploy.as_str()),
        ("gateway", options.gateway_deploy.as_str()),
        ("source_worker", options.source_worker_deploy.as_str()),
    ];
    if options.allow_activation || options.with_failure || options.require_target_worker {
        deployments.push(("target_worker", options.target_worker_deploy.as_str()));
    }

    let mut images = Vec::new();
    let mut errors = Vec::new();
    for (role, deployment) in deployments {
        match kubectl_deploy_first_container_image(context, &options.namespace, deployment)
            .with_context(|| format!("read image for deployment {deployment}"))
        {
            Ok(image) => images.push(K8sDeploymentImage {
                role,
                deployment: deployment.to_string(),
                image,
            }),
            Err(err) => errors.push(format!("{role}={deployment}: {err:#}")),
        }
    }
    (images, errors)
}

fn kubectl_deploy_first_container_image(
    context: &str,
    namespace: &str,
    deploy: &str,
) -> Result<String> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args([
        "get",
        "deploy",
        deploy,
        "-o",
        "jsonpath={.spec.template.spec.containers[0].image}",
    ]);
    let image = command_stdout(&mut cmd)?.trim().to_string();
    if image.is_empty() {
        bail!("deployment {deploy} has no first container image");
    }
    Ok(image)
}

fn validate_k8s_deployment_images(
    images: &[K8sDeploymentImage],
    expected_image: &str,
) -> Result<()> {
    let mismatches = images
        .iter()
        .filter(|image| image.image != expected_image)
        .map(|image| format!("{}={} uses {}", image.role, image.deployment, image.image))
        .collect::<Vec<_>>();
    if mismatches.is_empty() {
        return Ok(());
    }
    bail!(
        "runtime deployment image preflight failed: expected {expected_image}; mismatches: {}",
        mismatches.join(", ")
    )
}

#[derive(Default)]
struct ManagedK8sPortForwards {
    forwards: Vec<ManagedK8sPortForward>,
}

impl ManagedK8sPortForwards {
    fn spawn(
        &mut self,
        context: &str,
        namespace: &str,
        service: &str,
        name: &str,
        ports: &[(u16, u16)],
    ) -> Result<()> {
        let (_dev, logs, _pids) = dev_dirs();
        fs::create_dir_all(&logs)?;
        let log = open_dev_log(&logs, &format!("k8s-{name}-port-forward"))?;
        let mut cmd = kubectl_cmd(context, Some(namespace));
        cmd.arg("port-forward").arg(format!("svc/{service}")).args(
            ports
                .iter()
                .map(|(local, remote)| format!("{local}:{remote}")),
        );
        detach_background_process(&mut cmd);
        let mut child = cmd.stdout(log.try_clone()?).stderr(log).spawn()?;
        wait_for_port_forward(name, &mut child, ports)?;
        self.forwards.push(ManagedK8sPortForward {
            name: name.to_string(),
            child,
        });
        Ok(())
    }
}

impl Drop for ManagedK8sPortForwards {
    fn drop(&mut self) {
        while let Some(mut forward) = self.forwards.pop() {
            forward.terminate();
        }
    }
}

struct ManagedK8sPortForward {
    name: String,
    child: Child,
}

impl ManagedK8sPortForward {
    fn terminate(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        println!("k8s activation smoke: stopped {} port-forward", self.name);
    }
}

fn wait_for_port_forward(name: &str, child: &mut Child, ports: &[(u16, u16)]) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            bail!("{name} port-forward exited before becoming ready: {status}");
        }
        let all_ready = ports.iter().all(|(local, _)| {
            TcpStream::connect_timeout(
                &SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), *local),
                Duration::from_millis(200),
            )
            .map(|stream| {
                let _ = stream.shutdown(Shutdown::Both);
            })
            .is_ok()
        });
        if all_ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("{name} port-forward did not expose local ports within 10s");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn kubectl_cmd(context: &str, namespace: Option<&str>) -> Command {
    let mut cmd = Command::new("kubectl");
    cmd.args(["--context", context]);
    if let Some(namespace) = namespace {
        cmd.args(["-n", namespace]);
    }
    cmd
}

fn command_stdout(cmd: &mut Command) -> Result<String> {
    let debug = format!("{cmd:?}");
    let output = cmd.current_dir(workspace_root()).output()?;
    if !output.status.success() {
        bail!(
            "command failed: {debug}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn kubectl_resource_name(context: &str, namespace: &str, resource: &str) -> Result<String> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args(["get", resource, "-o", "name"]);
    let output = command_stdout(&mut cmd)?;
    let name = output.trim();
    if name.is_empty() {
        bail!("kubectl did not return a resource name for {resource}");
    }
    Ok(name.to_string())
}

fn kubectl_deploy_spec_replicas(context: &str, namespace: &str, deploy: &str) -> Result<u32> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args(["get", "deploy", deploy, "-o", "jsonpath={.spec.replicas}"]);
    let output = command_stdout(&mut cmd)?;
    parse_kubectl_replicas(output.trim(), 1)
}

fn kubectl_deploy_available_replicas(context: &str, namespace: &str, deploy: &str) -> Result<u32> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args([
        "get",
        "deploy",
        deploy,
        "-o",
        "jsonpath={.status.availableReplicas}",
    ]);
    let output = command_stdout(&mut cmd)?;
    parse_kubectl_replicas(output.trim(), 0)
}

fn parse_kubectl_replicas(raw: &str, empty_default: u32) -> Result<u32> {
    if raw.trim().is_empty() {
        return Ok(empty_default);
    }
    raw.trim()
        .parse::<u32>()
        .map_err(|err| anyhow::anyhow!("invalid Kubernetes replica value `{raw}`: {err}"))
}

fn kubectl_scale_deploy(context: &str, namespace: &str, deploy: &str, replicas: u32) -> Result<()> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args([
        "scale",
        &format!("deploy/{deploy}"),
        &format!("--replicas={replicas}"),
    ]);
    run(&mut cmd)
}

fn wait_for_kubectl_deploy_available_replicas(
    context: &str,
    namespace: &str,
    deploy: &str,
    expected: u32,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let available = kubectl_deploy_available_replicas(context, namespace, deploy)?;
        if available == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "deployment {deploy} did not reach availableReplicas={expected} before timeout; last availableReplicas={available}"
            );
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn kubectl_rollout_status(
    context: &str,
    namespace: &str,
    deploy: &str,
    timeout: Duration,
) -> Result<()> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args([
        "rollout",
        "status",
        &format!("deploy/{deploy}"),
        &format!("--timeout={}s", timeout.as_secs()),
    ]);
    run(&mut cmd)
}

struct K8sScaleRestore {
    context: String,
    namespace: String,
    deploy: String,
    replicas: u32,
    active: bool,
}

impl K8sScaleRestore {
    fn new(context: String, namespace: String, deploy: String, replicas: u32) -> Self {
        Self {
            context,
            namespace,
            deploy,
            replicas,
            active: true,
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for K8sScaleRestore {
    fn drop(&mut self) {
        if self.active {
            let mut cmd = kubectl_cmd(&self.context, Some(&self.namespace));
            cmd.args([
                "scale",
                &format!("deploy/{}", self.deploy),
                &format!("--replicas={}", self.replicas),
            ]);
            let _ = cmd.status();
        }
    }
}

fn target_subs_for_worker(
    targets: &[SplitActivationPlanTarget],
    worker_id: &str,
) -> Result<Vec<u8>> {
    let subs = targets
        .iter()
        .filter(|target| target.worker_id == worker_id)
        .map(|target| {
            u8::try_from(target.sub)
                .map_err(|_| anyhow::anyhow!("target sub {} is out of range", target.sub))
        })
        .collect::<Result<Vec<_>>>()?;
    if subs.is_empty() {
        bail!("target worker id {worker_id} owns no children in the activation plan");
    }
    Ok(subs)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct GatewayCloseCounters {
    no_route: f64,
    upstream_retry_exhausted: f64,
    ambiguous_upstream: f64,
}

fn gateway_close_counters_from_metrics(body: &str) -> Result<GatewayCloseCounters> {
    Ok(GatewayCloseCounters {
        no_route: prometheus_sample_value(body, "tessera_gateway_client_closes_no_route_total")?,
        upstream_retry_exhausted: prometheus_sample_value(
            body,
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
        )?,
        ambiguous_upstream: prometheus_sample_value(
            body,
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        )?,
    })
}

fn assert_gateway_close_counters_not_increased(
    service: &str,
    before: GatewayCloseCounters,
    after: GatewayCloseCounters,
) -> Result<()> {
    let increased = after.no_route > before.no_route
        || after.upstream_retry_exhausted > before.upstream_retry_exhausted
        || after.ambiguous_upstream > before.ambiguous_upstream;
    if increased {
        bail!(
            "{service}: gateway client close counters increased during success smoke: before={before:?} after={after:?}"
        );
    }
    Ok(())
}

struct InternalK8sActivationReport<'a> {
    context: &'a str,
    namespace: &'a str,
    orch_service: &'a str,
    gateway_service: &'a str,
    target_worker_deploy: &'a str,
    target_worker_id: &'a str,
    argocd_namespace: &'a str,
    argocd_app: &'a str,
    argocd_status: Option<&'a ArgoCdAppStatus>,
    deployment_images: &'a [K8sDeploymentImage],
    expected_image: Option<&'a str>,
    operation_id: &'a str,
    plan_report_path: &'a Path,
    plan: &'a SplitActivationOperatorPlan,
    response: &'a SplitActivationResponse,
    success_probe: &'a SplitConvergenceProbe,
    failure_probe: Option<&'a SplitConvergenceProbe>,
    recovery_probe: Option<&'a SplitConvergenceProbe>,
    gateway_close_before: GatewayCloseCounters,
    gateway_close_after_success: GatewayCloseCounters,
    with_failure: bool,
}

struct InternalK8sActivationPreflightReport<'a> {
    context: &'a str,
    namespace: &'a str,
    orch_service: &'a str,
    gateway_service: &'a str,
    target_worker_deploy: &'a str,
    target_worker_id: &'a str,
    argocd_namespace: &'a str,
    argocd_app: &'a str,
    argocd_status: Option<&'a ArgoCdAppStatus>,
    deployment_images: &'a [K8sDeploymentImage],
    expected_image: Option<&'a str>,
    operation_id: &'a str,
    plan_report_path: Option<&'a Path>,
    plan: Option<&'a SplitActivationOperatorPlan>,
    stage: &'a str,
    activation_allowed: bool,
    reason: &'a str,
    preflight_errors: &'a [String],
}

fn p5_rollback_policy_json() -> serde_json::Value {
    serde_json::json!({
        "policy_id": "operator_recovery_no_automatic_merge_rollback_v1",
        "automatic_rollback": "disabled",
        "failure_recovery": "operator restarts or restores the failed target Worker, then reruns convergence checks",
        "gitops_backout": "revert the controlled smoke GitOps slice, including image tag, second Worker topology, preview fixture, and manual activation flag, then wait for ArgoCD Synced/Healthy",
        "merge_activation": "deferred outside the P5 split-activation completion boundary"
    })
}

fn write_internal_k8s_activation_preflight_report(
    input: InternalK8sActivationPreflightReport<'_>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_activation_smoke_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let report = serde_json::json!({
        "schema": "tessera.internal_microk8s_activation_smoke.v1",
        "unix_ts": unix_timestamp_secs(),
        "operation_id": input.operation_id,
        "stage": input.stage,
        "activation_mode": "manual",
        "activation_mutated": false,
        "activation_allowed": input.activation_allowed,
        "reason": input.reason,
        "preflight_errors": input.preflight_errors,
        "cluster": {
            "context": input.context,
            "namespace": input.namespace,
            "orchestrator_service": input.orch_service,
            "gateway_service": input.gateway_service,
            "target_worker_deployment": input.target_worker_deploy,
            "target_worker_id": input.target_worker_id,
            "argocd": argocd_status_json(
                input.argocd_namespace,
                input.argocd_app,
                input.argocd_status
            ),
            "expected_image": input.expected_image,
            "deployment_images": k8s_deployment_images_json(input.deployment_images)
        },
        "plan": internal_k8s_preflight_plan_json(
            input.plan_report_path,
            input.plan,
            input.reason
        ),
        "checks": {
            "split_published": false,
            "post_publish_failure_smoke_ran": false,
            "target_worker_restart_recovered_convergence": false,
            "automatic_rollback_observed": false
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": [
            "internal_microk8s_activation_publish",
            "internal_microk8s_failure_recovery_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_path.with_file_name(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn internal_k8s_preflight_plan_json(
    report_path: Option<&Path>,
    plan: Option<&SplitActivationOperatorPlan>,
    reason: &str,
) -> serde_json::Value {
    let Some(plan) = plan else {
        return serde_json::json!({
            "report_path": report_path.map(|path| path.display().to_string()),
            "status": "not_run",
            "reason": reason,
            "activation_mutated": false
        });
    };
    serde_json::json!({
        "report_path": report_path.map(|path| path.display().to_string()),
        "status": plan.status,
        "reason": plan.reason.as_str(),
        "activation_mutated": false,
        "preview": {
            "addr": plan.preview_addr.as_str(),
            "mode": plan.preview_mode.as_str(),
            "source": plan.preview_source.as_str(),
            "assignments_changed": false,
            "plan_count": plan.preview_plan_count
        },
        "orchestrator": {
            "addr": plan.orch_addr.as_str(),
            "status": plan.health.status.as_str(),
            "configured_workers": plan.health.configured_workers,
            "registered_workers": plan.health.registered_workers,
            "assigned_cells": plan.health.assigned_cells
        },
        "workers": plan.workers.iter().map(|worker| {
            serde_json::json!({
                "worker_id": worker.worker_id.as_str(),
                "addr": worker.addr.as_str(),
                "cell_count": worker.cell_count,
                "registered": worker.registered,
                "active_handover": worker.active_handover
            })
        }).collect::<Vec<_>>(),
        "recommendation": {
            "parent": plan.parent,
            "source_worker_id": plan.source_worker_id.as_deref(),
            "targets": plan.recommended_targets.iter().map(|target| {
                serde_json::json!({
                    "sub": target.sub,
                    "worker_id": target.worker_id.as_str()
                })
            }).collect::<Vec<_>>(),
            "submission_command": plan.submission_command.as_deref()
        }
    })
}

fn write_internal_k8s_activation_report(
    input: InternalK8sActivationReport<'_>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_activation_smoke_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let remaining_uncovered = if input.with_failure {
        Vec::<&str>::new()
    } else {
        vec!["internal_microk8s_failure_recovery_smoke"]
    };
    let report = serde_json::json!({
        "schema": "tessera.internal_microk8s_activation_smoke.v1",
        "unix_ts": unix_timestamp_secs(),
        "operation_id": input.operation_id,
        "stage": "published",
        "activation_mode": "manual",
        "activation_mutated": true,
        "activation_allowed": true,
        "cluster": {
            "context": input.context,
            "namespace": input.namespace,
            "orchestrator_service": input.orch_service,
            "gateway_service": input.gateway_service,
            "target_worker_deployment": input.target_worker_deploy,
            "target_worker_id": input.target_worker_id,
            "argocd": argocd_status_json(
                input.argocd_namespace,
                input.argocd_app,
                input.argocd_status
            ),
            "expected_image": input.expected_image,
            "deployment_images": k8s_deployment_images_json(input.deployment_images)
        },
        "plan": {
            "report_path": input.plan_report_path.display().to_string(),
            "status": input.plan.status,
            "reason": input.plan.reason.as_str(),
            "activation_mutated": false,
            "source_worker_id": input.plan.source_worker_id.as_deref(),
            "targets": input.plan.recommended_targets.iter().map(|target| {
                serde_json::json!({
                    "sub": target.sub,
                    "worker_id": target.worker_id.as_str()
                })
            }).collect::<Vec<_>>()
        },
        "split_activation": {
            "accepted": input.response.accepted,
            "state": split_activation_state_name(input.response.state),
            "assignments_changed": input.response.assignments_changed,
            "source_worker_id": input.response.source_worker_id.as_str(),
            "reason": input.response.reason.as_str(),
            "children": input.response.staged_children.iter().map(|child| {
                serde_json::json!({
                    "cell": child.cell.as_ref().map(|cell| serde_json::json!({
                        "world": cell.world,
                        "cx": cell.cx,
                        "cy": cell.cy,
                        "depth": cell.depth,
                        "sub": cell.sub
                    })),
                    "worker_id": child.target_worker_id.as_str()
                })
            }).collect::<Vec<_>>()
        },
        "checks": {
            "split_published": true,
            "gateway_ready_routes": 4,
            "child_ping_all_routes": input.success_probe.failures.is_empty() && input.success_probe.succeeded.len() == 4,
            "gateway_close_counters_success_delta_zero": true,
            "post_publish_failure_smoke_ran": input.with_failure,
            "target_worker_restart_recovered_convergence": input.recovery_probe.is_some_and(|probe| probe.failures.is_empty() && probe.succeeded.len() == 4),
            "automatic_rollback_observed": false
        },
        "success_probe": split_convergence_probe_json(input.success_probe),
        "failure_probe": input.failure_probe.map(split_convergence_probe_json),
        "recovery_probe": input.recovery_probe.map(split_convergence_probe_json),
        "gateway_close_counters": {
            "before_success": gateway_close_counters_json(input.gateway_close_before),
            "after_success": gateway_close_counters_json(input.gateway_close_after_success)
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": remaining_uncovered
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_path.with_file_name(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn default_internal_k8s_activation_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("internal-microk8s-activation-smoke-latest.json")
}

fn run_k8s_activation_report_check(
    report: Option<&Path>,
    require_published: bool,
    require_failure: bool,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_activation_smoke_path);
    let body = fs::read_to_string(&report_path)
        .with_context(|| format!("read activation report {}", report_path.display()))?;
    let report: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parse activation report {}", report_path.display()))?;
    validate_internal_k8s_activation_report(
        &report,
        require_published,
        require_failure,
        expected_image,
        expect_preflight_errors,
    )?;
    println!(
        "internal k8s activation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

struct DevActivationReportCheckOptions<'a> {
    plan_report: Option<&'a Path>,
    activation_report: Option<&'a Path>,
    failure_report: Option<&'a Path>,
    soak_report: Option<&'a Path>,
    min_soak_iterations: u32,
}

fn run_dev_activation_report_check(options: DevActivationReportCheckOptions<'_>) -> Result<()> {
    let report_dir = workspace_root().join(".dev/reports");
    let plan_path = options
        .plan_report
        .map(Path::to_path_buf)
        .unwrap_or_else(|| report_dir.join("split-activation-plan-latest.json"));
    let activation_path = options
        .activation_report
        .map(Path::to_path_buf)
        .unwrap_or_else(|| report_dir.join("activation-smoke-latest.json"));
    let failure_path = options
        .failure_report
        .map(Path::to_path_buf)
        .unwrap_or_else(|| report_dir.join("activation-failure-smoke-latest.json"));
    let soak_path = options
        .soak_report
        .map(Path::to_path_buf)
        .unwrap_or_else(|| report_dir.join("activation-soak-latest.json"));

    let plan = read_json_report(&plan_path)?;
    validate_split_activation_plan_report(&plan)?;
    let activation = read_json_report(&activation_path)?;
    validate_activation_smoke_report(&activation)?;
    let failure = read_json_report(&failure_path)?;
    validate_activation_failure_smoke_report(&failure)?;
    let soak = read_json_report(&soak_path)?;
    validate_activation_soak_report(&soak, options.min_soak_iterations)?;

    println!(
        "local activation reports are valid: {}, {}, {}, {}",
        plan_path.display(),
        activation_path.display(),
        failure_path.display(),
        soak_path.display()
    );
    Ok(())
}

fn read_json_report(path: &Path) -> Result<serde_json::Value> {
    let body =
        fs::read_to_string(path).with_context(|| format!("read report {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("parse report {}", path.display()))
}

fn validate_split_activation_plan_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.split_activation_plan.v1")?;
    assert_json_str_eq(report, &["status"], "ready")?;
    assert_json_bool_eq(report, &["activation_mutated"], false)?;
    assert_json_bool_eq(report, &["preview", "assignments_changed"], false)?;
    assert_json_array_len(report, &["recommendation", "targets"], 4)?;
    assert_remaining_uncovered_only(report, "internal_microk8s_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_activation_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.activation_smoke.v1")?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_split_activation_published"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "orchestrator_listing_child_routes"],
        4.0,
    )?;
    assert_json_number_at_least(report, &["checks", "gateway_ready_routes"], 4.0)?;
    assert_json_bool_eq(report, &["checks", "child_ping_all_routes"], true)?;
    assert_json_bool_eq(report, &["checks", "stable_session_post_split_move"], true)?;
    assert_json_bool_eq(report, &["checks", "live_remote_aoi_resync_snapshot"], true)?;
    assert_json_number_at_least(report, &["checks", "target_worker_relay_connections"], 1.0)?;
    assert_remaining_uncovered_only(report, "internal_microk8s_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_activation_failure_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.activation_failure_smoke.v1")?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_split_activation_published"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "orchestrator_listing_child_routes_after_failure"],
        4.0,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ready_routes_after_failure"],
        4.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "post_publish_target_outage_detected"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
    assert_json_bool_eq(report, &["checks", "operator_recovery_required"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "target_worker_restart_recovered_convergence"],
        true,
    )?;
    assert_json_array_nonempty(report, &["checks", "failed_child_subs"])?;
    assert_json_array_nonempty(report, &["checks", "succeeded_child_subs_during_failure"])?;
    assert_json_array_len(report, &["checks", "recovered_child_subs"], 4)?;
    assert_remaining_uncovered_only(report, "internal_microk8s_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_activation_soak_report(
    report: &serde_json::Value,
    min_soak_iterations: u32,
) -> Result<()> {
    let min_total = f64::from(min_soak_iterations) * 4.0;
    assert_json_str_eq(report, &["schema"], "tessera.activation_soak.v1")?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_split_activation_published"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "orchestrator_listing_child_routes"],
        4.0,
    )?;
    assert_json_number_at_least(report, &["checks", "gateway_ready_routes_after_soak"], 4.0)?;
    assert_json_number_at_least(
        report,
        &["traffic", "iterations_per_child"],
        f64::from(min_soak_iterations),
    )?;
    assert_json_number_at_least(report, &["checks", "child_ping_iterations"], min_total)?;
    assert_json_number_at_least(report, &["checks", "child_move_iterations"], min_total)?;
    assert_json_number_at_least(report, &["checks", "remote_aoi_frames_observed"], 1.0)?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ping_roundtrip_count"],
        min_total,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_move_roundtrip_count"],
        min_total,
    )?;
    assert_json_number_eq(
        report,
        &["checks", "gateway_client_closes_no_route_total"],
        0.0,
    )?;
    assert_json_number_eq(
        report,
        &[
            "checks",
            "gateway_client_closes_upstream_retry_exhausted_total",
        ],
        0.0,
    )?;
    assert_json_number_eq(
        report,
        &["checks", "gateway_client_closes_ambiguous_upstream_total"],
        0.0,
    )?;
    assert_remaining_uncovered_only(report, "internal_microk8s_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_internal_k8s_activation_report(
    report: &serde_json::Value,
    require_published: bool,
    require_failure: bool,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.internal_microk8s_activation_smoke.v1",
    )?;
    let stage = json_str(report, &["stage"])?;
    match stage {
        "blocked_before_plan"
        | "blocked_before_activation"
        | "planned_without_activation"
        | "published" => {}
        other => bail!("internal activation report has unknown stage `{other}`"),
    }
    if stage != "published" {
        assert_json_bool_eq(report, &["activation_mutated"], false)?;
    }
    if !expect_preflight_errors.is_empty() {
        assert_preflight_errors_contain(report, expect_preflight_errors)?;
    }
    if let Some(expected_image) = expected_image {
        assert_report_images_match(report, expected_image)?;
    }
    if require_failure {
        validate_internal_k8s_published_report(report, true)?;
        return Ok(());
    }
    if require_published {
        validate_internal_k8s_published_report(report, false)?;
    }
    Ok(())
}

fn validate_internal_k8s_published_report(
    report: &serde_json::Value,
    require_failure: bool,
) -> Result<()> {
    assert_json_str_eq(report, &["stage"], "published")?;
    assert_json_bool_eq(report, &["activation_mutated"], true)?;
    assert_json_bool_eq(report, &["cluster", "argocd", "checked"], true)?;
    assert_json_str_eq(report, &["cluster", "argocd", "sync"], "Synced")?;
    assert_json_str_eq(report, &["cluster", "argocd", "health"], "Healthy")?;
    assert_json_array_empty_or_missing(report, &["preflight_errors"])?;
    assert_json_str_eq(report, &["plan", "status"], "ready")?;
    assert_json_bool_eq(report, &["plan", "activation_mutated"], false)?;
    assert_json_array_len(report, &["plan", "targets"], 4)?;
    assert_json_bool_eq(report, &["split_activation", "accepted"], true)?;
    assert_json_str_eq(report, &["split_activation", "state"], "published")?;
    assert_json_bool_eq(report, &["split_activation", "assignments_changed"], true)?;
    assert_json_array_len(report, &["split_activation", "children"], 4)?;
    assert_json_bool_eq(report, &["checks", "split_published"], true)?;
    assert_json_number_at_least(report, &["checks", "gateway_ready_routes"], 4.0)?;
    assert_json_bool_eq(report, &["checks", "child_ping_all_routes"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "gateway_close_counters_success_delta_zero"],
        true,
    )?;
    assert_probe_succeeded_all(report, &["success_probe"])?;
    assert_report_deployment_roles(
        report,
        &["orchestrator", "gateway", "source_worker", "target_worker"],
    )?;
    assert_remaining_uncovered_absent(report, "internal_microk8s_activation_publish")?;

    if require_failure {
        assert_json_bool_eq(report, &["checks", "post_publish_failure_smoke_ran"], true)?;
        assert_json_bool_eq(
            report,
            &["checks", "target_worker_restart_recovered_convergence"],
            true,
        )?;
        assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
        assert_json_array_nonempty(report, &["failure_probe", "failures"])?;
        assert_json_array_nonempty(report, &["recovery_probe", "succeeded"])?;
        assert_failure_probe_matches_target_map(report)?;
        assert_probe_succeeded_all(report, &["recovery_probe"])?;
        assert_remaining_uncovered_absent(report, "internal_microk8s_failure_recovery_smoke")?;
        assert_remaining_uncovered_absent(report, "merge_rollback_policy_gate")?;
        assert_remaining_uncovered_empty(report)?;
        validate_p5_rollback_policy(report)?;
    }
    Ok(())
}

fn validate_p5_rollback_policy(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["rollback_policy", "policy_id"],
        "operator_recovery_no_automatic_merge_rollback_v1",
    )?;
    assert_json_str_eq(
        report,
        &["rollback_policy", "automatic_rollback"],
        "disabled",
    )?;
    assert_json_str_eq(
        report,
        &["rollback_policy", "merge_activation"],
        "deferred outside the P5 split-activation completion boundary",
    )?;
    let failure_recovery = json_str(report, &["rollback_policy", "failure_recovery"])?;
    if !failure_recovery.contains("convergence checks") {
        bail!("rollback policy does not describe recovery convergence checks");
    }
    let gitops_backout = json_str(report, &["rollback_policy", "gitops_backout"])?;
    if !gitops_backout.contains("ArgoCD Synced/Healthy") {
        bail!("rollback policy does not describe GitOps backout and ArgoCD health");
    }
    Ok(())
}

fn assert_probe_succeeded_all(report: &serde_json::Value, path: &[&str]) -> Result<()> {
    let mut succeeded_path = path.to_vec();
    succeeded_path.push("succeeded");
    let mut failures_path = path.to_vec();
    failures_path.push("failures");
    let succeeded = json_u8_array(report, &succeeded_path)?;
    let failures = json_array(report, &failures_path)?;
    if succeeded == vec![0, 1, 2, 3] && failures.is_empty() {
        return Ok(());
    }
    bail!(
        "report field {} expected succeeded [0,1,2,3] and no failures, got succeeded={succeeded:?} failures={failures:?}",
        path.join(".")
    )
}

fn assert_failure_probe_matches_target_map(report: &serde_json::Value) -> Result<()> {
    let target_worker_id = json_str(report, &["cluster", "target_worker_id"])?;
    let target_subs = target_subs_from_report_plan(report, target_worker_id)?;
    if target_subs.is_empty() {
        bail!(
            "internal activation report plan has no children assigned to target worker `{target_worker_id}`"
        );
    }
    let failed_subs = failure_subs_from_report(report, &["failure_probe", "failures"])?;
    if failed_subs != target_subs {
        bail!(
            "failure_probe failed child subs {failed_subs:?} do not match target worker `{target_worker_id}` child subs {target_subs:?}"
        );
    }
    let succeeded_subs = json_u8_array(report, &["failure_probe", "succeeded"])?;
    let mut expected_succeeded = (0_u8..=3)
        .filter(|sub| !target_subs.contains(sub))
        .collect::<Vec<_>>();
    expected_succeeded.sort_unstable();
    if succeeded_subs != expected_succeeded {
        bail!(
            "failure_probe succeeded child subs {succeeded_subs:?} do not match non-target child subs {expected_succeeded:?}"
        );
    }
    Ok(())
}

fn target_subs_from_report_plan(
    report: &serde_json::Value,
    target_worker_id: &str,
) -> Result<Vec<u8>> {
    let mut all_subs = Vec::new();
    let mut target_subs = Vec::new();
    for (index, target) in json_array(report, &["plan", "targets"])?.iter().enumerate() {
        let worker_id = target
            .get("worker_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("plan target #{index} missing worker_id"))?;
        let raw_sub = target
            .get("sub")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("plan target #{index} missing numeric sub"))?;
        let sub = u8::try_from(raw_sub)
            .map_err(|err| anyhow::anyhow!("target sub {raw_sub} out of range: {err}"))?;
        all_subs.push(sub);
        if worker_id == target_worker_id {
            target_subs.push(sub);
        }
    }
    all_subs.sort_unstable();
    if all_subs != vec![0, 1, 2, 3] {
        bail!("internal activation report plan target subs must cover [0,1,2,3], got {all_subs:?}");
    }
    target_subs.sort_unstable();
    Ok(target_subs)
}

fn failure_subs_from_report(report: &serde_json::Value, path: &[&str]) -> Result<Vec<u8>> {
    let mut subs = json_array(report, path)?
        .iter()
        .map(|failure| {
            let sub = failure
                .get("sub")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    anyhow::anyhow!("failure_probe failure entry missing numeric sub")
                })?;
            u8::try_from(sub)
                .map_err(|err| anyhow::anyhow!("failure sub {sub} out of range: {err}"))
        })
        .collect::<Result<Vec<_>>>()?;
    subs.sort_unstable();
    Ok(subs)
}

fn assert_preflight_errors_contain(
    report: &serde_json::Value,
    expected_errors: &[String],
) -> Result<()> {
    let errors = json_array(report, &["preflight_errors"])?;
    if errors.is_empty() {
        bail!("internal activation report has no preflight_errors entries");
    }
    for expected in expected_errors {
        if expected.trim().is_empty() {
            bail!("expected preflight error substring must not be empty");
        }
        let matched = errors
            .iter()
            .filter_map(serde_json::Value::as_str)
            .any(|actual| actual.contains(expected));
        if !matched {
            bail!("internal activation report preflight_errors does not contain `{expected}`");
        }
    }
    Ok(())
}

fn assert_report_images_match(report: &serde_json::Value, expected_image: &str) -> Result<()> {
    assert_json_str_eq(report, &["cluster", "expected_image"], expected_image)?;
    let images = json_array(report, &["cluster", "deployment_images"])?;
    if images.is_empty() {
        bail!("internal activation report has no recorded deployment images");
    }
    for image in images {
        let actual = json_str(image, &["image"])?;
        if actual != expected_image {
            let deployment = json_str(image, &["deployment"]).unwrap_or("<unknown>");
            bail!(
                "internal activation report image mismatch: deployment {deployment} uses {actual}, expected {expected_image}"
            );
        }
    }
    Ok(())
}

fn assert_report_deployment_roles(
    report: &serde_json::Value,
    required_roles: &[&str],
) -> Result<()> {
    let images = json_array(report, &["cluster", "deployment_images"])?;
    for required_role in required_roles {
        let present = images.iter().any(|image| {
            image.get("role").and_then(serde_json::Value::as_str) == Some(*required_role)
        });
        if !present {
            bail!("internal activation report missing deployment image role `{required_role}`");
        }
    }
    Ok(())
}

fn assert_remaining_uncovered_absent(report: &serde_json::Value, blocker: &str) -> Result<()> {
    let values = json_array(report, &["remaining_uncovered"])?;
    if values.iter().any(|value| value.as_str() == Some(blocker)) {
        bail!("internal activation report still lists blocker `{blocker}`");
    }
    Ok(())
}

fn assert_remaining_uncovered_empty(report: &serde_json::Value) -> Result<()> {
    let values = json_array(report, &["remaining_uncovered"])?;
    if !values.is_empty() {
        bail!("internal activation report still lists uncovered requirements: {values:?}");
    }
    Ok(())
}

fn assert_remaining_uncovered_only(report: &serde_json::Value, expected: &str) -> Result<()> {
    let values = json_array(report, &["remaining_uncovered"])?;
    if values.len() == 1 && values[0].as_str() == Some(expected) {
        return Ok(());
    }
    bail!("report remaining_uncovered expected only `{expected}`, got {values:?}")
}

fn assert_json_str_eq(report: &serde_json::Value, path: &[&str], expected: &str) -> Result<()> {
    let actual = json_str(report, path)?;
    if actual != expected {
        bail!(
            "report field {} expected `{expected}`, got `{actual}`",
            path.join(".")
        );
    }
    Ok(())
}

fn assert_json_bool_eq(report: &serde_json::Value, path: &[&str], expected: bool) -> Result<()> {
    let actual = json_bool(report, path)?;
    if actual != expected {
        bail!(
            "report field {} expected `{expected}`, got `{actual}`",
            path.join(".")
        );
    }
    Ok(())
}

fn assert_json_number_at_least(report: &serde_json::Value, path: &[&str], min: f64) -> Result<()> {
    let actual = json_f64(report, path)?;
    if actual < min {
        bail!(
            "report field {} expected at least {min}, got {actual}",
            path.join(".")
        );
    }
    Ok(())
}

fn assert_json_number_eq(report: &serde_json::Value, path: &[&str], expected: f64) -> Result<()> {
    let actual = json_f64(report, path)?;
    if (actual - expected).abs() > f64::EPSILON {
        bail!(
            "report field {} expected {expected}, got {actual}",
            path.join(".")
        );
    }
    Ok(())
}

fn assert_json_array_len(report: &serde_json::Value, path: &[&str], expected: usize) -> Result<()> {
    let actual = json_array(report, path)?.len();
    if actual != expected {
        bail!(
            "report field {} expected array length {expected}, got {actual}",
            path.join(".")
        );
    }
    Ok(())
}

fn assert_json_array_nonempty(report: &serde_json::Value, path: &[&str]) -> Result<()> {
    let actual = json_array(report, path)?.len();
    if actual == 0 {
        bail!("report field {} expected non-empty array", path.join("."));
    }
    Ok(())
}

fn assert_json_array_empty_or_missing(report: &serde_json::Value, path: &[&str]) -> Result<()> {
    match json_path(report, path) {
        None => Ok(()),
        Some(serde_json::Value::Array(values)) if values.is_empty() => Ok(()),
        Some(serde_json::Value::Array(values)) => {
            bail!(
                "report field {} expected empty array, got {values:?}",
                path.join(".")
            )
        }
        Some(_) => bail!("report field {} is not an array", path.join(".")),
    }
}

fn json_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn json_field<'a>(value: &'a serde_json::Value, path: &[&str]) -> Result<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current
            .get(*key)
            .ok_or_else(|| anyhow::anyhow!("report field {} is missing", path.join(".")))?;
    }
    Ok(current)
}

fn json_str<'a>(value: &'a serde_json::Value, path: &[&str]) -> Result<&'a str> {
    json_field(value, path)?
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("report field {} is not a string", path.join(".")))
}

fn json_bool(value: &serde_json::Value, path: &[&str]) -> Result<bool> {
    json_field(value, path)?
        .as_bool()
        .ok_or_else(|| anyhow::anyhow!("report field {} is not a bool", path.join(".")))
}

fn json_f64(value: &serde_json::Value, path: &[&str]) -> Result<f64> {
    json_field(value, path)?
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("report field {} is not a number", path.join(".")))
}

fn json_array<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> Result<&'a Vec<serde_json::Value>> {
    json_field(value, path)?
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("report field {} is not an array", path.join(".")))
}

fn json_u8_array(value: &serde_json::Value, path: &[&str]) -> Result<Vec<u8>> {
    let mut values = json_array(value, path)?
        .iter()
        .map(|entry| {
            let raw = entry.as_u64().ok_or_else(|| {
                anyhow::anyhow!("report field {} has non-numeric entry", path.join("."))
            })?;
            u8::try_from(raw).map_err(|err| {
                anyhow::anyhow!(
                    "report field {} entry {raw} out of range: {err}",
                    path.join(".")
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    values.sort_unstable();
    Ok(values)
}

fn split_convergence_probe_json(probe: &SplitConvergenceProbe) -> serde_json::Value {
    serde_json::json!({
        "succeeded": &probe.succeeded,
        "failures": probe.failures.iter().map(|failure| {
            serde_json::json!({
                "sub": failure.sub,
                "error": failure.error.as_str()
            })
        }).collect::<Vec<_>>()
    })
}

fn gateway_close_counters_json(counters: GatewayCloseCounters) -> serde_json::Value {
    serde_json::json!({
        "no_route": counters.no_route,
        "upstream_retry_exhausted": counters.upstream_retry_exhausted,
        "ambiguous_upstream": counters.ambiguous_upstream
    })
}

fn argocd_status_json(
    namespace: &str,
    app: &str,
    status: Option<&ArgoCdAppStatus>,
) -> serde_json::Value {
    serde_json::json!({
        "namespace": namespace,
        "application": app,
        "checked": status.is_some(),
        "sync": status.map(|status| status.sync.as_str()),
        "health": status.map(|status| status.health.as_str()),
    })
}

fn k8s_deployment_images_json(images: &[K8sDeploymentImage]) -> serde_json::Value {
    serde_json::Value::Array(
        images
            .iter()
            .map(|image| {
                serde_json::json!({
                    "role": image.role,
                    "deployment": image.deployment.as_str(),
                    "image": image.image.as_str()
                })
            })
            .collect(),
    )
}

fn run_split_activation_operator(
    orch_addr: &str,
    operation_id: String,
    parent: CellId,
    raw_targets: &[String],
) -> Result<()> {
    let targets = parse_split_activation_targets(raw_targets)?;
    let endpoint = grpc_endpoint(orch_addr);
    let runtime = tokio::runtime::Runtime::new()?;
    let response = runtime.block_on(submit_split_activation(
        &endpoint,
        operation_id,
        parent,
        &targets,
    ))?;
    print_split_activation_response(&response)?;
    assert_split_activation_published(&response)
}

fn run_split_activation_plan_operator(
    orch_addr: &str,
    preview_addr: &str,
    operation_id: Option<String>,
    raw_targets: &[String],
    out: Option<&Path>,
) -> Result<()> {
    let endpoint = grpc_endpoint(orch_addr);
    let plan = build_split_activation_plan(&endpoint, preview_addr, operation_id, raw_targets)?;
    let report_path = write_split_activation_plan_report(&plan, out)?;
    println!(
        "split activation plan: status={} reason={} report={}",
        plan.status,
        plan.reason,
        report_path.display()
    );
    if let Some(command) = plan.submission_command.as_ref() {
        println!("submission command: {command}");
    }
    Ok(())
}

fn grpc_endpoint(raw_addr: &str) -> String {
    if raw_addr.starts_with("http://") || raw_addr.starts_with("https://") {
        raw_addr.to_string()
    } else {
        format!("http://{raw_addr}")
    }
}

#[derive(Debug, Deserialize)]
struct SplitMergePreviewJson {
    mode: String,
    source: String,
    assignments_changed: bool,
    #[serde(default)]
    plans: Vec<SplitMergePreviewPlanJson>,
}

#[derive(Debug, Clone, Deserialize)]
struct SplitMergePreviewPlanJson {
    kind: String,
    cell: CellId,
    #[serde(default)]
    pressure_signals: u8,
    #[serde(default)]
    score: u64,
    #[serde(default)]
    required_handover_ops: usize,
    #[serde(default)]
    cells_moved: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SplitActivationPlanTarget {
    sub: u32,
    worker_id: String,
}

#[derive(Debug)]
struct SplitActivationOperatorPlan {
    operation_id: String,
    status: &'static str,
    reason: String,
    preview_addr: String,
    preview_mode: String,
    preview_source: String,
    preview_plan_count: usize,
    selected_plan: Option<SplitMergePreviewPlanJson>,
    orch_addr: String,
    health: OrchestratorHealth,
    workers: Vec<SplitActivationPlanWorker>,
    parent: Option<CellId>,
    source_worker_id: Option<String>,
    recommended_targets: Vec<SplitActivationPlanTarget>,
    submission_command: Option<String>,
}

#[derive(Debug, Clone)]
struct SplitActivationPlanWorker {
    worker_id: String,
    addr: String,
    cell_count: usize,
    registered: bool,
    active_handover: bool,
}

fn build_split_activation_plan(
    orch_endpoint: &str,
    preview_addr: &str,
    operation_id: Option<String>,
    raw_targets: &[String],
) -> Result<SplitActivationOperatorPlan> {
    let preview = fetch_split_merge_preview(preview_addr)?;
    if preview.assignments_changed {
        bail!("split activation plan requires dry-run preview with assignments_changed=false");
    }

    let runtime = tokio::runtime::Runtime::new()?;
    let (health, listing) = runtime.block_on(fetch_orch_health_and_listing(orch_endpoint))?;
    let workers = split_activation_plan_workers(&health, &listing);
    let operation_id =
        operation_id.unwrap_or_else(|| format!("planned-split-{}", unix_timestamp_secs()));
    let selected_plan = select_split_preview_candidate(&preview);
    let Some(selected_plan) = selected_plan else {
        return Ok(SplitActivationOperatorPlan {
            operation_id,
            status: "no_split_candidate",
            reason: "preview returned no split candidate".to_string(),
            preview_addr: preview_addr.to_string(),
            preview_mode: preview.mode,
            preview_source: preview.source,
            preview_plan_count: preview.plans.len(),
            selected_plan: None,
            orch_addr: orch_endpoint.to_string(),
            health,
            workers,
            parent: None,
            source_worker_id: None,
            recommended_targets: Vec::new(),
            submission_command: None,
        });
    };

    let parent = selected_plan.cell;
    if parent.depth != 0 || parent.sub != 0 {
        return blocked_split_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            None,
            Vec::new(),
            "selected split candidate is not a depth=0/sub=0 parent",
        );
    }
    let owners = owners_for_listing_cell(&listing, parent)?;
    let Some(source_worker_id) = owners.first().cloned() else {
        return blocked_split_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            None,
            Vec::new(),
            "selected split parent is not currently assigned",
        );
    };
    if owners.len() != 1 {
        return blocked_split_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            Some(source_worker_id),
            Vec::new(),
            "selected split parent has multiple owners",
        );
    }

    let targets = if raw_targets.is_empty() {
        infer_split_activation_targets(&workers, &source_worker_id)?
    } else {
        parse_split_activation_targets(raw_targets)?
            .into_iter()
            .map(|(sub, worker_id)| SplitActivationPlanTarget { sub, worker_id })
            .collect()
    };
    let validation = validate_split_activation_plan_targets(&workers, &source_worker_id, &targets);
    if let Err(reason) = validation {
        return blocked_split_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            Some(source_worker_id),
            targets,
            &reason,
        );
    }

    let submission_command =
        split_activation_submission_command(orch_endpoint, &operation_id, parent, &targets);
    Ok(SplitActivationOperatorPlan {
        operation_id,
        status: "ready",
        reason: "preview split candidate has an operator-reviewed target map".to_string(),
        preview_addr: preview_addr.to_string(),
        preview_mode: preview.mode,
        preview_source: preview.source,
        preview_plan_count: preview.plans.len(),
        selected_plan: Some(selected_plan),
        orch_addr: orch_endpoint.to_string(),
        health,
        workers,
        parent: Some(parent),
        source_worker_id: Some(source_worker_id),
        recommended_targets: targets,
        submission_command: Some(submission_command),
    })
}

#[allow(clippy::too_many_arguments)]
fn blocked_split_activation_plan(
    operation_id: String,
    preview_addr: &str,
    preview: SplitMergePreviewJson,
    selected_plan: SplitMergePreviewPlanJson,
    orch_endpoint: &str,
    health: OrchestratorHealth,
    workers: Vec<SplitActivationPlanWorker>,
    parent: Option<CellId>,
    source_worker_id: Option<String>,
    targets: Vec<SplitActivationPlanTarget>,
    reason: &str,
) -> Result<SplitActivationOperatorPlan> {
    Ok(SplitActivationOperatorPlan {
        operation_id,
        status: "blocked",
        reason: reason.to_string(),
        preview_addr: preview_addr.to_string(),
        preview_mode: preview.mode,
        preview_source: preview.source,
        preview_plan_count: preview.plans.len(),
        selected_plan: Some(selected_plan),
        orch_addr: orch_endpoint.to_string(),
        health,
        workers,
        parent,
        source_worker_id,
        recommended_targets: targets,
        submission_command: None,
    })
}

fn fetch_split_merge_preview(preview_addr: &str) -> Result<SplitMergePreviewJson> {
    let addr = readiness_addr(preview_addr)?;
    let response = http_get(addr, "/split-merge/preview")?;
    let body = http_response_body("split/merge preview", &response)?;
    serde_json::from_str(body).context("parse split/merge preview JSON")
}

async fn fetch_orch_health_and_listing(
    endpoint: &str,
) -> Result<(OrchestratorHealth, AssignmentListing)> {
    let mut client = OrchestratorClient::connect(endpoint.to_string()).await?;
    let health = client
        .get_health(tonic::Request::new(HealthCheckRequest {}))
        .await?
        .into_inner();
    let listing = client
        .list_assignments(tonic::Request::new(ListAssignmentsRequest {}))
        .await?
        .into_inner();
    Ok((health, listing))
}

fn select_split_preview_candidate(
    preview: &SplitMergePreviewJson,
) -> Option<SplitMergePreviewPlanJson> {
    preview
        .plans
        .iter()
        .find(|plan| plan.kind == "split")
        .cloned()
}

fn split_activation_plan_workers(
    health: &OrchestratorHealth,
    listing: &AssignmentListing,
) -> Vec<SplitActivationPlanWorker> {
    let mut workers = listing
        .workers
        .iter()
        .map(|bundle| {
            let health_worker = health
                .workers
                .iter()
                .find(|worker| worker.worker_id == bundle.worker_id);
            SplitActivationPlanWorker {
                worker_id: bundle.worker_id.clone(),
                addr: bundle.addr.clone(),
                cell_count: bundle.cells.len(),
                registered: health_worker.is_some_and(|worker| worker.registered),
                active_handover: listing.handovers.iter().any(|handover| {
                    handover.source_worker_id == bundle.worker_id
                        || handover.target_worker_id == bundle.worker_id
                }),
            }
        })
        .collect::<Vec<_>>();
    workers.sort_by(|left, right| left.worker_id.cmp(&right.worker_id));
    workers
}

fn owners_for_listing_cell(listing: &AssignmentListing, cell: CellId) -> Result<Vec<String>> {
    let mut owners = Vec::new();
    for bundle in &listing.workers {
        for assigned in &bundle.cells {
            if proto_assignment_to_cell(assigned)? == cell {
                owners.push(bundle.worker_id.clone());
            }
        }
    }
    owners.sort();
    Ok(owners)
}

fn infer_split_activation_targets(
    workers: &[SplitActivationPlanWorker],
    source_worker_id: &str,
) -> Result<Vec<SplitActivationPlanTarget>> {
    let source = workers
        .iter()
        .find(|worker| worker.worker_id == source_worker_id)
        .ok_or_else(|| anyhow::anyhow!("source worker {source_worker_id} is not in listing"))?;
    if !source.registered {
        bail!("source worker {source_worker_id} is not registered");
    }
    let mut non_source = workers
        .iter()
        .filter(|worker| worker.worker_id != source_worker_id)
        .filter(|worker| worker.registered)
        .filter(|worker| !worker.active_handover)
        .collect::<Vec<_>>();
    non_source.sort_by(|left, right| {
        left.cell_count
            .cmp(&right.cell_count)
            .then_with(|| left.worker_id.cmp(&right.worker_id))
    });
    let Some(first_target) = non_source.first() else {
        bail!("no registered non-source worker is available for split activation");
    };
    let second_target = non_source.get(1).unwrap_or(first_target);
    Ok(vec![
        SplitActivationPlanTarget {
            sub: 0,
            worker_id: source_worker_id.to_string(),
        },
        SplitActivationPlanTarget {
            sub: 1,
            worker_id: first_target.worker_id.clone(),
        },
        SplitActivationPlanTarget {
            sub: 2,
            worker_id: source_worker_id.to_string(),
        },
        SplitActivationPlanTarget {
            sub: 3,
            worker_id: second_target.worker_id.clone(),
        },
    ])
}

fn validate_split_activation_plan_targets(
    workers: &[SplitActivationPlanWorker],
    source_worker_id: &str,
    targets: &[SplitActivationPlanTarget],
) -> Result<(), String> {
    if targets.len() != 4 {
        return Err("target map must include exactly four children".to_string());
    }
    let worker_by_id = workers
        .iter()
        .map(|worker| (worker.worker_id.as_str(), worker))
        .collect::<std::collections::HashMap<_, _>>();
    let mut seen = [false; 4];
    let mut has_non_source = false;
    for target in targets {
        let Some(slot) = seen.get_mut(target.sub as usize) else {
            return Err(format!("target sub {} is out of range", target.sub));
        };
        if *slot {
            return Err(format!("target sub {} is duplicated", target.sub));
        }
        *slot = true;
        let Some(worker) = worker_by_id.get(target.worker_id.as_str()) else {
            return Err(format!(
                "target worker {} is not configured",
                target.worker_id
            ));
        };
        if !worker.registered {
            return Err(format!(
                "target worker {} is not registered",
                target.worker_id
            ));
        }
        if worker.active_handover {
            return Err(format!(
                "target worker {} has an active handover",
                target.worker_id
            ));
        }
        if target.worker_id != source_worker_id {
            has_non_source = true;
        }
    }
    if seen.iter().any(|slot| !slot) {
        return Err("target map must cover sub=0..3".to_string());
    }
    if !has_non_source {
        return Err("target map assigns all children to the source worker".to_string());
    }
    Ok(())
}

fn split_activation_submission_command(
    orch_endpoint: &str,
    operation_id: &str,
    parent: CellId,
    targets: &[SplitActivationPlanTarget],
) -> String {
    let orch_addr = orch_endpoint
        .strip_prefix("http://")
        .or_else(|| orch_endpoint.strip_prefix("https://"))
        .unwrap_or(orch_endpoint);
    let mut parts = vec![
        "cargo xt split-activation".to_string(),
        format!("--orch-addr {orch_addr}"),
        format!("--operation-id {operation_id}"),
        format!("--world {}", parent.world),
        format!("--cx {}", parent.cx),
        format!("--cy {}", parent.cy),
    ];
    let mut targets = targets.to_vec();
    targets.sort_by_key(|target| target.sub);
    parts.extend(
        targets
            .into_iter()
            .map(|target| format!("--target {}={}", target.sub, target.worker_id)),
    );
    parts.join(" ")
}

fn write_split_activation_plan_report(
    plan: &SplitActivationOperatorPlan,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_split_activation_plan_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let selected_plan = plan.selected_plan.as_ref().map(|selected| {
        serde_json::json!({
            "kind": selected.kind,
            "cell": selected.cell,
            "pressure_signals": selected.pressure_signals,
            "score": selected.score,
            "required_handover_ops": selected.required_handover_ops,
            "cells_moved": selected.cells_moved
        })
    });
    let report = serde_json::json!({
        "schema": "tessera.split_activation_plan.v1",
        "unix_ts": unix_timestamp_secs(),
        "status": plan.status,
        "reason": plan.reason,
        "operation_id": plan.operation_id,
        "activation_mutated": false,
        "preview": {
            "addr": plan.preview_addr,
            "mode": plan.preview_mode,
            "source": plan.preview_source,
            "assignments_changed": false,
            "plan_count": plan.preview_plan_count,
            "selected_plan": selected_plan
        },
        "orchestrator": {
            "addr": plan.orch_addr,
            "status": plan.health.status,
            "configured_workers": plan.health.configured_workers,
            "registered_workers": plan.health.registered_workers,
            "assigned_cells": plan.health.assigned_cells
        },
        "workers": plan.workers.iter().map(|worker| {
            serde_json::json!({
                "worker_id": worker.worker_id,
                "addr": worker.addr,
                "cell_count": worker.cell_count,
                "registered": worker.registered,
                "active_handover": worker.active_handover
            })
        }).collect::<Vec<_>>(),
        "recommendation": {
            "parent": plan.parent,
            "source_worker_id": plan.source_worker_id,
            "targets": plan.recommended_targets.iter().map(|target| {
                serde_json::json!({
                    "sub": target.sub,
                    "worker_id": target.worker_id
                })
            }).collect::<Vec<_>>(),
            "submission_command": plan.submission_command,
            "required_preconditions": [
                "operator reviewed target map and blast radius",
                "TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual is enabled only for the controlled smoke window",
                "all target workers are registered and have no active handover",
                "run activation-smoke or equivalent convergence checks after submit"
            ]
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": [
            "internal_microk8s_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn default_split_activation_plan_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("split-activation-plan-latest.json")
}

fn parse_split_activation_targets(raw_targets: &[String]) -> Result<Vec<(u32, String)>> {
    if raw_targets.len() != 4 {
        bail!(
            "split activation requires exactly four --target values, one for each child sub=0..3"
        );
    }
    let mut slots = vec![None::<String>; 4];
    for raw in raw_targets {
        let Some((sub_raw, worker_raw)) = raw.split_once('=') else {
            bail!("invalid --target `{raw}`; expected sub=worker-id");
        };
        let sub = sub_raw
            .trim()
            .parse::<usize>()
            .map_err(|err| anyhow::anyhow!("invalid target sub `{sub_raw}`: {err}"))?;
        if sub > 3 {
            bail!("target sub {sub} is out of range; expected 0..=3");
        }
        let worker_id = worker_raw.trim();
        if worker_id.is_empty() {
            bail!("target worker id must not be empty for child sub={sub}");
        }
        if slots[sub].is_some() {
            bail!("duplicate target mapping for child sub={sub}");
        }
        slots[sub] = Some(worker_id.to_string());
    }

    slots
        .into_iter()
        .enumerate()
        .map(|(sub, worker_id)| {
            Ok((
                sub as u32,
                worker_id
                    .ok_or_else(|| anyhow::anyhow!("missing target mapping for child sub={sub}"))?,
            ))
        })
        .collect()
}

async fn submit_split_activation(
    endpoint: &str,
    operation_id: String,
    parent: CellId,
    targets: &[(u32, String)],
) -> Result<SplitActivationResponse> {
    let mut client = OrchestratorClient::connect(endpoint.to_string()).await?;
    let response = client
        .submit_split_activation(tonic::Request::new(SplitActivationRequest {
            operation_id,
            parent: Some(Assignment {
                world: parent.world,
                cx: parent.cx,
                cy: parent.cy,
                depth: parent.depth as u32,
                sub: parent.sub as u32,
            }),
            targets: targets
                .iter()
                .map(|(sub, target_worker_id)| SplitChildTarget {
                    sub: *sub,
                    target_worker_id: target_worker_id.clone(),
                })
                .collect(),
        }))
        .await?
        .into_inner();

    Ok(response)
}

fn assert_split_activation_published(response: &SplitActivationResponse) -> Result<()> {
    if !response.accepted
        || response.state != SplitActivationState::Published as i32
        || !response.assignments_changed
        || response.staged_children.len() != 4
    {
        bail!(
            "split activation smoke failed: accepted={} state={} assignments_changed={} children={} reason={}",
            response.accepted,
            response.state,
            response.assignments_changed,
            response.staged_children.len(),
            response.reason
        );
    }
    Ok(())
}

fn print_split_activation_response(response: &SplitActivationResponse) -> Result<()> {
    println!(
        "split activation: accepted={} state={} assignments_changed={} source_worker_id={} reason={}",
        response.accepted,
        split_activation_state_name(response.state),
        response.assignments_changed,
        response.source_worker_id,
        response.reason
    );
    for child in &response.staged_children {
        let Some(cell) = child.cell.as_ref() else {
            bail!("split activation response contained child without cell");
        };
        println!(
            "child world={} cx={} cy={} depth={} sub={} -> {}",
            cell.world, cell.cx, cell.cy, cell.depth, cell.sub, child.target_worker_id
        );
    }
    Ok(())
}

fn split_activation_state_name(value: i32) -> &'static str {
    match SplitActivationState::try_from(value).unwrap_or(SplitActivationState::Unspecified) {
        SplitActivationState::Unspecified => "unspecified",
        SplitActivationState::Disabled => "disabled",
        SplitActivationState::Rejected => "rejected",
        SplitActivationState::Staged => "staged",
        SplitActivationState::Published => "published",
        SplitActivationState::Failed => "failed",
    }
}

async fn wait_for_split_listing(endpoint: &str, expected: &[(CellId, &str)]) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match split_listing_matches(endpoint, expected).await {
            Ok(true) => return Ok(()),
            Ok(false) | Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(false) => bail!("split activation listing did not converge before timeout"),
            Err(err) => return Err(err),
        }
    }
}

async fn split_listing_matches(endpoint: &str, expected: &[(CellId, &str)]) -> Result<bool> {
    let mut client = OrchestratorClient::connect(endpoint.to_string()).await?;
    let listing = client
        .list_assignments(tonic::Request::new(ListAssignmentsRequest {}))
        .await?
        .into_inner();
    let mut actual = Vec::new();
    for bundle in listing.workers {
        for cell in bundle.cells {
            actual.push((proto_assignment_to_cell(&cell)?, bundle.worker_id.clone()));
        }
    }
    actual.sort_by_key(|(cell, worker_id)| {
        (
            cell.world,
            cell.cy,
            cell.cx,
            cell.depth,
            cell.sub,
            worker_id.clone(),
        )
    });
    let mut expected = expected
        .iter()
        .map(|(cell, worker_id)| (*cell, (*worker_id).to_string()))
        .collect::<Vec<_>>();
    expected.sort_by_key(|(cell, worker_id)| {
        (
            cell.world,
            cell.cy,
            cell.cx,
            cell.depth,
            cell.sub,
            worker_id.clone(),
        )
    });
    Ok(actual == expected)
}

fn proto_assignment_to_cell(assignment: &Assignment) -> Result<CellId> {
    Ok(CellId {
        world: assignment.world,
        cx: assignment.cx,
        cy: assignment.cy,
        depth: u8::try_from(assignment.depth)
            .map_err(|_| anyhow::anyhow!("assignment depth {} out of range", assignment.depth))?,
        sub: u8::try_from(assignment.sub)
            .map_err(|_| anyhow::anyhow!("assignment sub {} out of range", assignment.sub))?,
    })
}

#[derive(Debug, Clone)]
struct SplitConvergenceProbe {
    succeeded: Vec<u8>,
    failures: Vec<SplitConvergenceFailure>,
}

#[derive(Debug, Clone)]
struct SplitConvergenceFailure {
    sub: u8,
    error: String,
}

impl SplitConvergenceProbe {
    fn assert_success(&self) -> Result<()> {
        if self.failures.is_empty() && self.succeeded.len() == 4 {
            return Ok(());
        }
        bail!(
            "split convergence probe expected all children to pass, succeeded={:?}, failures={:?}",
            self.succeeded,
            self.failures
        )
    }

    fn assert_failed_only(&self, expected_failed: &[u8]) -> Result<()> {
        let mut actual = self
            .failures
            .iter()
            .map(|failure| failure.sub)
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let mut expected = expected_failed.to_vec();
        expected.sort_unstable();
        if actual == expected {
            return Ok(());
        }
        bail!(
            "split convergence probe expected failed child subs {:?}, got {:?}; succeeded={:?}, failures={:?}",
            expected,
            actual,
            self.succeeded,
            self.failures
        )
    }
}

fn probe_split_convergence(gateway_addr: &str, ts_base: u64) -> SplitConvergenceProbe {
    let mut succeeded = Vec::new();
    let mut failures = Vec::new();
    for sub in 0..4 {
        match assert_gateway_ping_until(
            gateway_addr,
            activation_child_cell(sub),
            ts_base + u64::from(sub),
        ) {
            Ok(()) => succeeded.push(sub),
            Err(err) => failures.push(SplitConvergenceFailure {
                sub,
                error: err.to_string(),
            }),
        }
    }
    SplitConvergenceProbe {
        succeeded,
        failures,
    }
}

struct ActivationSmokeReport<'a> {
    operation_id: &'a str,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    gateway_routes: f64,
    gateway_route_change_reconnects: f64,
    worker_b_relay_connections: f64,
}

struct ActivationFailureSmokeReport<'a> {
    operation_id: &'a str,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    gateway_routes: f64,
    failure_probe: &'a SplitConvergenceProbe,
    recovery_probe: &'a SplitConvergenceProbe,
}

struct ActivationSoakReport<'a> {
    operation_id: &'a str,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    iterations: u32,
    sleep_ms: u64,
    stats: ActivationSoakStats,
    gateway_routes: f64,
    gateway_ping_roundtrips: f64,
    gateway_join_roundtrips: f64,
    gateway_move_roundtrips: f64,
    gateway_no_route_closes: f64,
    gateway_retry_exhausted_closes: f64,
    gateway_ambiguous_upstream_closes: f64,
    worker_a_accepted_connections: f64,
    worker_b_accepted_connections: f64,
    worker_a_relay_connections: f64,
    worker_b_relay_connections: f64,
}

fn write_activation_smoke_report(input: ActivationSmokeReport<'_>) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let report = serde_json::json!({
        "schema": "tessera.activation_smoke.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_b": input.worker_b_addr
        },
        "parent": CellId::grid(0, 0, 0),
        "children": [
            {"cell": activation_child_cell(0), "worker_id": "worker-a"},
            {"cell": activation_child_cell(1), "worker_id": "worker-b"},
            {"cell": activation_child_cell(2), "worker_id": "worker-a"},
            {"cell": activation_child_cell(3), "worker_id": "worker-b"}
        ],
        "checks": {
            "submit_split_activation_published": true,
            "orchestrator_listing_child_routes": 4,
            "gateway_ready_routes": input.gateway_routes,
            "child_ping_all_routes": true,
            "stable_session_post_split_move": true,
            "live_remote_aoi_resync_snapshot": true,
            "gateway_route_change_reconnects": input.gateway_route_change_reconnects,
            "target_worker_relay_connections": input.worker_b_relay_connections
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": [
            "internal_microk8s_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = report_dir.join("activation-smoke-latest.json");
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_activation_soak_report(input: ActivationSoakReport<'_>) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let report = serde_json::json!({
        "schema": "tessera.activation_soak.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_b": input.worker_b_addr
        },
        "parent": CellId::grid(0, 0, 0),
        "children": [
            {"cell": activation_child_cell(0), "worker_id": "worker-a"},
            {"cell": activation_child_cell(1), "worker_id": "worker-b"},
            {"cell": activation_child_cell(2), "worker_id": "worker-a"},
            {"cell": activation_child_cell(3), "worker_id": "worker-b"}
        ],
        "traffic": {
            "iterations_per_child": input.iterations,
            "sleep_ms": input.sleep_ms,
            "actors": 4,
            "pings_ok": input.stats.pings_ok,
            "moves_ok": input.stats.moves_ok,
            "ignored_frames": input.stats.ignored_frames,
            "remote_delta_frames": input.stats.remote_delta_frames,
            "remote_snapshot_frames": input.stats.remote_snapshot_frames
        },
        "checks": {
            "submit_split_activation_published": true,
            "orchestrator_listing_child_routes": 4,
            "gateway_ready_routes_after_soak": input.gateway_routes,
            "child_ping_iterations": input.stats.pings_ok,
            "child_move_iterations": input.stats.moves_ok,
            "remote_aoi_frames_observed": input.stats.remote_delta_frames + input.stats.remote_snapshot_frames,
            "gateway_ping_roundtrip_count": input.gateway_ping_roundtrips,
            "gateway_join_roundtrip_count": input.gateway_join_roundtrips,
            "gateway_move_roundtrip_count": input.gateway_move_roundtrips,
            "gateway_client_closes_no_route_total": input.gateway_no_route_closes,
            "gateway_client_closes_upstream_retry_exhausted_total": input.gateway_retry_exhausted_closes,
            "gateway_client_closes_ambiguous_upstream_total": input.gateway_ambiguous_upstream_closes,
            "worker_a_accepted_connections_total": input.worker_a_accepted_connections,
            "worker_b_accepted_connections_total": input.worker_b_accepted_connections,
            "worker_a_relay_connections_total": input.worker_a_relay_connections,
            "worker_b_relay_connections_total": input.worker_b_relay_connections
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": [
            "internal_microk8s_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = report_dir.join("activation-soak-latest.json");
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_activation_failure_smoke_report(
    input: ActivationFailureSmokeReport<'_>,
) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let failure_details = input
        .failure_probe
        .failures
        .iter()
        .map(|failure| {
            serde_json::json!({
                "sub": failure.sub,
                "error": failure.error,
            })
        })
        .collect::<Vec<_>>();
    let recovery_details = input
        .recovery_probe
        .failures
        .iter()
        .map(|failure| {
            serde_json::json!({
                "sub": failure.sub,
                "error": failure.error,
            })
        })
        .collect::<Vec<_>>();
    let failed_child_subs = input
        .failure_probe
        .failures
        .iter()
        .map(|failure| failure.sub)
        .collect::<Vec<_>>();
    let succeeded_during_failure = input.failure_probe.succeeded.clone();
    let recovered_child_subs = input.recovery_probe.succeeded.clone();
    let report = serde_json::json!({
        "schema": "tessera.activation_failure_smoke.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_b": input.worker_b_addr
        },
        "parent": CellId::grid(0, 0, 0),
        "children": [
            {"cell": activation_child_cell(0), "worker_id": "worker-a"},
            {"cell": activation_child_cell(1), "worker_id": "worker-b"},
            {"cell": activation_child_cell(2), "worker_id": "worker-a"},
            {"cell": activation_child_cell(3), "worker_id": "worker-b"}
        ],
        "checks": {
            "submit_split_activation_published": true,
            "orchestrator_listing_child_routes_after_failure": 4,
            "gateway_ready_routes_after_failure": input.gateway_routes,
            "post_publish_target_outage_detected": true,
            "failed_child_subs": failed_child_subs,
            "succeeded_child_subs_during_failure": succeeded_during_failure,
            "automatic_rollback_observed": false,
            "operator_recovery_required": true,
            "target_worker_restart_recovered_convergence": true,
            "recovered_child_subs": recovered_child_subs
        },
        "failure_probe": {
            "failures": failure_details
        },
        "recovery_probe": {
            "failures": recovery_details
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": [
            "internal_microk8s_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = report_dir.join("activation-failure-smoke-latest.json");
    fs::write(&latest, body)?;
    Ok(latest)
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

fn assert_metrics_endpoint_body_until(
    service: &str,
    raw_addr: &str,
    required_metrics: &[&str],
) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match assert_metrics_endpoint_body(service, raw_addr, required_metrics) {
            Ok(body) => return Ok(body),
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
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

fn run_client_demo(actor: u64) -> Result<()> {
    let root = workspace_root();
    let client = root.join("target/debug/tessera-client");
    let mut demo = Command::new(client);
    demo.args(["demo", "--actor", &actor.to_string()]);
    run(&mut demo)
}

fn assert_http_status_endpoint(
    service: &str,
    raw_addr: &str,
    path: &str,
    expected_status: &str,
) -> Result<()> {
    let addr = readiness_addr(raw_addr)?;
    let expected = format!("HTTP/1.1 {expected_status}");
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match http_get(addr, path) {
            Ok(response) if response.starts_with(&expected) => return Ok(()),
            Ok(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Ok(_) => {
                bail!("{service} smoke failed: expected HTTP {expected_status} response");
            }
            Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
}

fn assert_json_endpoint_contains(
    service: &str,
    raw_addr: &str,
    path: &str,
    required_fragments: &[&str],
) -> Result<()> {
    let addr = readiness_addr(raw_addr)?;
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match assert_json_response_contains(service, http_get(addr, path), required_fragments) {
            Ok(()) => return Ok(()),
            Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
}

fn assert_json_response_contains(
    service: &str,
    response: Result<String>,
    required_fragments: &[&str],
) -> Result<()> {
    let response = response?;
    if !response.starts_with("HTTP/1.1 200 OK") {
        bail!("{service} smoke failed: expected HTTP 200 response");
    }
    let Some((headers, body)) = response.split_once("\r\n\r\n") else {
        bail!("{service} smoke failed: missing HTTP body separator");
    };
    if !headers.contains("Content-Type: application/json") {
        bail!("{service} smoke failed: expected application/json content type");
    }
    for fragment in required_fragments {
        if !body.contains(fragment) {
            bail!("{service} smoke failed: missing JSON fragment `{fragment}`");
        }
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

fn assert_prometheus_sample_eq(
    service: &str,
    body: &str,
    metric_name: &str,
    expected: f64,
) -> Result<()> {
    let value = prometheus_sample_value(body, metric_name)?;
    if (value - expected).abs() > f64::EPSILON {
        bail!(
            "{service} metrics smoke failed: metric `{metric_name}` value {value} is not {expected}"
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
    let mut response = Vec::new();
    let mut buf = [0_u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&buf[..n]),
            Err(err) if err.kind() == ErrorKind::ConnectionReset && !response.is_empty() => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(String::from_utf8(response)?)
}

fn http_response_body<'a>(service: &str, response: &'a str) -> Result<&'a str> {
    if !response.starts_with("HTTP/1.1 200 OK") {
        bail!("{service} request failed: expected HTTP 200 response");
    }
    let Some((_, body)) = response.split_once("\r\n\r\n") else {
        bail!("{service} request failed: missing HTTP body separator");
    };
    Ok(body)
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
    fn prometheus_sample_eq_rejects_unexpected_value() {
        let body = "demo_total 0\n";
        assert_prometheus_sample_eq("demo", body, "demo_total", 0.0).expect("zero metric");

        let err = assert_prometheus_sample_eq("demo", body, "demo_total", 1.0)
            .expect_err("mismatched value should fail");
        assert!(err.to_string().contains("is not 1"));
    }

    #[test]
    fn kubectl_replicas_parse_empty_default() {
        assert_eq!(parse_kubectl_replicas("", 0).expect("empty status"), 0);
        assert_eq!(parse_kubectl_replicas("3", 0).expect("replicas"), 3);
        let err = parse_kubectl_replicas("nope", 0).expect_err("invalid replicas should fail");
        assert!(err.to_string().contains("invalid Kubernetes replica value"));
    }

    #[test]
    fn argocd_app_status_requires_synced_healthy() {
        let status = parse_argocd_app_status("Synced Healthy\n").expect("parse status");
        assert_eq!(
            status,
            ArgoCdAppStatus {
                sync: "Synced".to_string(),
                health: "Healthy".to_string()
            }
        );
        validate_argocd_app_ready(&status).expect("synced healthy is ready");

        let status = parse_argocd_app_status("OutOfSync Healthy").expect("parse not synced");
        let err = validate_argocd_app_ready(&status).expect_err("out of sync should fail");
        assert!(err.to_string().contains("Synced and Healthy"));

        let err = parse_argocd_app_status("Synced").expect_err("missing health should fail");
        assert!(err.to_string().contains("missing health"));
    }

    #[test]
    fn deployment_image_preflight_rejects_mismatches() {
        let images = vec![
            K8sDeploymentImage {
                role: "orchestrator",
                deployment: "tessera-orch".to_string(),
                image: "repo/tessera:v1".to_string(),
            },
            K8sDeploymentImage {
                role: "gateway",
                deployment: "tessera-gateway".to_string(),
                image: "repo/tessera:v2".to_string(),
            },
        ];

        let err = validate_k8s_deployment_images(&images, "repo/tessera:v1")
            .expect_err("mismatched image should fail");
        assert!(err.to_string().contains("tessera-gateway"));

        let images = vec![K8sDeploymentImage {
            role: "orchestrator",
            deployment: "tessera-orch".to_string(),
            image: "repo/tessera:v1".to_string(),
        }];
        validate_k8s_deployment_images(&images, "repo/tessera:v1").expect("matching images pass");
    }

    fn complete_internal_k8s_failure_report() -> serde_json::Value {
        serde_json::json!({
            "schema": "tessera.internal_microk8s_activation_smoke.v1",
            "stage": "published",
            "activation_mutated": true,
            "cluster": {
                "argocd": {
                    "checked": true,
                    "sync": "Synced",
                    "health": "Healthy"
                },
                "expected_image": "repo/tessera:v1",
                "target_worker_id": "worker-b",
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "source_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"},
                    {"role": "target_worker", "deployment": "tessera-worker-b", "image": "repo/tessera:v1"}
                ]
            },
            "plan": {
                "status": "ready",
                "activation_mutated": false,
                "targets": [
                    {"sub": 0, "worker_id": "worker-a"},
                    {"sub": 1, "worker_id": "worker-b"},
                    {"sub": 2, "worker_id": "worker-a"},
                    {"sub": 3, "worker_id": "worker-b"}
                ]
            },
            "split_activation": {
                "accepted": true,
                "state": "published",
                "assignments_changed": true,
                "children": [{}, {}, {}, {}]
            },
            "checks": {
                "split_published": true,
                "gateway_ready_routes": 4,
                "child_ping_all_routes": true,
                "gateway_close_counters_success_delta_zero": true,
                "post_publish_failure_smoke_ran": true,
                "target_worker_restart_recovered_convergence": true,
                "automatic_rollback_observed": false
            },
            "success_probe": {
                "succeeded": [0, 1, 2, 3],
                "failures": []
            },
            "failure_probe": {
                "succeeded": [0, 2],
                "failures": [
                    {"sub": 1, "error": "target down"},
                    {"sub": 3, "error": "target down"}
                ]
            },
            "recovery_probe": {
                "succeeded": [0, 1, 2, 3],
                "failures": []
            },
            "rollback_policy": {
                "policy_id": "operator_recovery_no_automatic_merge_rollback_v1",
                "automatic_rollback": "disabled",
                "failure_recovery": "operator restarts or restores the failed target Worker, then reruns convergence checks",
                "gitops_backout": "revert the controlled smoke GitOps slice and wait for ArgoCD Synced/Healthy",
                "merge_activation": "deferred outside the P5 split-activation completion boundary"
            },
            "remaining_uncovered": []
        })
    }

    #[test]
    fn activation_report_check_accepts_complete_failure_evidence() {
        let report = complete_internal_k8s_failure_report();
        validate_internal_k8s_activation_report(&report, true, true, Some("repo/tessera:v1"), &[])
            .expect("complete failure evidence should pass");
    }

    #[test]
    fn activation_report_check_rejects_failure_probe_that_misses_target_child() {
        let mut report = complete_internal_k8s_failure_report();
        report["failure_probe"] = serde_json::json!({
            "succeeded": [0, 2, 3],
            "failures": [{"sub": 1, "error": "target down"}]
        });

        let err = validate_internal_k8s_activation_report(
            &report,
            true,
            true,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect_err("failure probe must match target worker child map");
        assert!(err.to_string().contains("target worker"));
    }

    #[test]
    fn activation_report_check_rejects_unresolved_rollback_policy_gate() {
        let mut report = complete_internal_k8s_failure_report();
        report["remaining_uncovered"] = serde_json::json!(["merge_rollback_policy_gate"]);

        let err = validate_internal_k8s_activation_report(
            &report,
            true,
            true,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect_err("unresolved rollback policy gate should fail completion check");
        assert!(err.to_string().contains("merge_rollback_policy_gate"));
    }

    #[test]
    fn activation_report_check_rejects_missing_target_worker_image_role() {
        let mut report = complete_internal_k8s_failure_report();
        report["cluster"]["deployment_images"] = serde_json::json!([
            {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
            {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
            {"role": "source_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"}
        ]);

        let err = validate_internal_k8s_activation_report(
            &report,
            true,
            false,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect_err("published report without target worker image role should fail");
        assert!(err.to_string().contains("target_worker"));
    }

    #[test]
    fn activation_report_check_rejects_published_report_with_dirty_preflight() {
        let mut report = complete_internal_k8s_failure_report();
        report["preflight_errors"] = serde_json::json!(["stale target worker image"]);

        let err = validate_internal_k8s_activation_report(
            &report,
            true,
            false,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect_err("published report with preflight errors should fail");
        assert!(err.to_string().contains("preflight_errors"));
    }

    #[test]
    fn activation_report_check_rejects_blocked_report_when_publish_required() {
        let report = serde_json::json!({
            "schema": "tessera.internal_microk8s_activation_smoke.v1",
            "stage": "blocked_before_plan",
            "activation_mutated": false,
            "cluster": {
                "expected_image": "repo/tessera:v2",
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"}
                ]
            },
            "remaining_uncovered": [
                "internal_microk8s_activation_publish",
                "internal_microk8s_failure_recovery_smoke"
            ]
        });

        let err = validate_internal_k8s_activation_report(
            &report,
            true,
            false,
            Some("repo/tessera:v2"),
            &[],
        )
        .expect_err("blocked report cannot satisfy published evidence");
        assert!(err.to_string().contains("image mismatch"));
    }

    #[test]
    fn activation_report_check_matches_preflight_error_substrings() {
        let report = serde_json::json!({
            "schema": "tessera.internal_microk8s_activation_smoke.v1",
            "stage": "blocked_before_plan",
            "activation_mutated": false,
            "preflight_errors": [
                "target_worker=tessera-worker-b: read image failed: deployment not found",
                "required resource deploy/tessera-worker-b is not ready"
            ],
            "cluster": {
                "deployment_images": []
            },
            "remaining_uncovered": [
                "internal_microk8s_activation_publish",
                "internal_microk8s_failure_recovery_smoke"
            ]
        });

        validate_internal_k8s_activation_report(
            &report,
            false,
            false,
            None,
            &[
                "tessera-worker-b".to_string(),
                "deployment not found".to_string(),
            ],
        )
        .expect("matching preflight error substrings should pass");

        let err = validate_internal_k8s_activation_report(
            &report,
            false,
            false,
            None,
            &["image mismatch".to_string()],
        )
        .expect_err("missing preflight error substring should fail");
        assert!(err.to_string().contains("preflight_errors"));
    }

    #[test]
    fn dev_activation_report_check_accepts_local_evidence() {
        let rollback_policy = p5_rollback_policy_json();
        let remaining_uncovered = ["internal_microk8s_activation_smoke"];
        let plan = serde_json::json!({
            "schema": "tessera.split_activation_plan.v1",
            "status": "ready",
            "activation_mutated": false,
            "preview": {"assignments_changed": false},
            "recommendation": {"targets": [{}, {}, {}, {}]},
            "rollback_policy": rollback_policy.clone(),
            "remaining_uncovered": remaining_uncovered
        });
        validate_split_activation_plan_report(&plan).expect("valid plan report");

        let activation = serde_json::json!({
            "schema": "tessera.activation_smoke.v1",
            "checks": {
                "submit_split_activation_published": true,
                "orchestrator_listing_child_routes": 4,
                "gateway_ready_routes": 4,
                "child_ping_all_routes": true,
                "stable_session_post_split_move": true,
                "live_remote_aoi_resync_snapshot": true,
                "target_worker_relay_connections": 1
            },
            "rollback_policy": rollback_policy.clone(),
            "remaining_uncovered": remaining_uncovered
        });
        validate_activation_smoke_report(&activation).expect("valid activation report");

        let failure = serde_json::json!({
            "schema": "tessera.activation_failure_smoke.v1",
            "checks": {
                "submit_split_activation_published": true,
                "orchestrator_listing_child_routes_after_failure": 4,
                "gateway_ready_routes_after_failure": 4,
                "post_publish_target_outage_detected": true,
                "automatic_rollback_observed": false,
                "operator_recovery_required": true,
                "target_worker_restart_recovered_convergence": true,
                "failed_child_subs": [1, 3],
                "succeeded_child_subs_during_failure": [0, 2],
                "recovered_child_subs": [0, 1, 2, 3]
            },
            "rollback_policy": rollback_policy.clone(),
            "remaining_uncovered": remaining_uncovered
        });
        validate_activation_failure_smoke_report(&failure).expect("valid failure report");

        let soak = serde_json::json!({
            "schema": "tessera.activation_soak.v1",
            "traffic": {"iterations_per_child": 32},
            "checks": {
                "submit_split_activation_published": true,
                "orchestrator_listing_child_routes": 4,
                "gateway_ready_routes_after_soak": 4,
                "child_ping_iterations": 128,
                "child_move_iterations": 128,
                "remote_aoi_frames_observed": 1,
                "gateway_ping_roundtrip_count": 128,
                "gateway_move_roundtrip_count": 128,
                "gateway_client_closes_no_route_total": 0,
                "gateway_client_closes_upstream_retry_exhausted_total": 0,
                "gateway_client_closes_ambiguous_upstream_total": 0
            },
            "rollback_policy": rollback_policy,
            "remaining_uncovered": remaining_uncovered
        });
        validate_activation_soak_report(&soak, 32).expect("valid soak report");
    }

    #[test]
    fn target_subs_for_worker_selects_failure_children() {
        let targets = vec![
            SplitActivationPlanTarget {
                sub: 0,
                worker_id: "worker-a".to_string(),
            },
            SplitActivationPlanTarget {
                sub: 1,
                worker_id: "worker-b".to_string(),
            },
            SplitActivationPlanTarget {
                sub: 2,
                worker_id: "worker-a".to_string(),
            },
            SplitActivationPlanTarget {
                sub: 3,
                worker_id: "worker-b".to_string(),
            },
        ];

        assert_eq!(
            target_subs_for_worker(&targets, "worker-b").expect("target subs"),
            vec![1, 3]
        );
        let err = target_subs_for_worker(&targets, "worker-c")
            .expect_err("unknown target worker should fail");
        assert!(err.to_string().contains("owns no children"));
    }

    #[test]
    fn gateway_close_counter_guard_rejects_increase() {
        let before = GatewayCloseCounters {
            no_route: 1.0,
            upstream_retry_exhausted: 2.0,
            ambiguous_upstream: 3.0,
        };
        assert_gateway_close_counters_not_increased("gateway", before, before)
            .expect("same counters pass");

        let after = GatewayCloseCounters {
            no_route: 1.0,
            upstream_retry_exhausted: 3.0,
            ambiguous_upstream: 3.0,
        };
        let err = assert_gateway_close_counters_not_increased("gateway", before, after)
            .expect_err("increase should fail");
        assert!(err.to_string().contains("increased"));
    }

    #[test]
    fn activation_soak_observed_frames_counts_remote_aoi() {
        let expected = activation_child_cell(0);
        let remote = activation_child_cell(1);
        let mut observed = ActivationSoakObservedFrames::default();
        observed.record_ignored(
            &ServerEnvelope {
                cell: remote,
                seq: 0,
                epoch: 0,
                request_id: None,
                payload: ServerMsg::Delta {
                    cell: remote,
                    moved: vec![],
                },
            },
            expected,
        );
        observed.record_ignored(
            &ServerEnvelope {
                cell: expected,
                seq: 0,
                epoch: 0,
                request_id: None,
                payload: ServerMsg::Snapshot {
                    cell: expected,
                    actors: vec![],
                },
            },
            expected,
        );

        assert_eq!(observed.ignored_frames, 2);
        assert_eq!(observed.remote_delta_frames, 1);
        assert_eq!(observed.remote_snapshot_frames, 0);
    }

    #[test]
    fn grpc_endpoint_accepts_addr_or_url() {
        assert_eq!(grpc_endpoint("127.0.0.1:6000"), "http://127.0.0.1:6000");
        assert_eq!(
            grpc_endpoint("http://127.0.0.1:6000"),
            "http://127.0.0.1:6000"
        );
    }

    #[test]
    fn split_activation_targets_require_complete_unique_map() {
        let targets = parse_split_activation_targets(&[
            "0=worker-a".to_string(),
            "1=worker-b".to_string(),
            "2=worker-a".to_string(),
            "3=worker-b".to_string(),
        ])
        .expect("valid targets");
        assert_eq!(
            targets,
            vec![
                (0, "worker-a".to_string()),
                (1, "worker-b".to_string()),
                (2, "worker-a".to_string()),
                (3, "worker-b".to_string())
            ]
        );

        let duplicate = parse_split_activation_targets(&[
            "0=worker-a".to_string(),
            "0=worker-b".to_string(),
            "2=worker-a".to_string(),
            "3=worker-b".to_string(),
        ])
        .expect_err("duplicate sub should fail");
        assert!(duplicate.to_string().contains("duplicate"));

        let out_of_range = parse_split_activation_targets(&[
            "0=worker-a".to_string(),
            "1=worker-b".to_string(),
            "2=worker-a".to_string(),
            "4=worker-b".to_string(),
        ])
        .expect_err("out-of-range sub should fail");
        assert!(out_of_range.to_string().contains("out of range"));
    }

    #[test]
    fn split_convergence_probe_asserts_expected_failure_subs() {
        let probe = SplitConvergenceProbe {
            succeeded: vec![0, 2],
            failures: vec![
                SplitConvergenceFailure {
                    sub: 1,
                    error: "target down".to_string(),
                },
                SplitConvergenceFailure {
                    sub: 3,
                    error: "target down".to_string(),
                },
            ],
        };
        probe
            .assert_failed_only(&[3, 1])
            .expect("expected failed target children");
        assert!(
            probe.assert_success().is_err(),
            "partial convergence must not pass success assertion"
        );

        let recovered = SplitConvergenceProbe {
            succeeded: vec![0, 1, 2, 3],
            failures: vec![],
        };
        recovered.assert_success().expect("all children recovered");
    }

    #[test]
    fn split_activation_plan_infers_deterministic_targets() {
        let workers = vec![
            SplitActivationPlanWorker {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
                cell_count: 1,
                registered: true,
                active_handover: false,
            },
            SplitActivationPlanWorker {
                worker_id: "worker-c".to_string(),
                addr: "127.0.0.1:5003".to_string(),
                cell_count: 4,
                registered: true,
                active_handover: false,
            },
            SplitActivationPlanWorker {
                worker_id: "worker-b".to_string(),
                addr: "127.0.0.1:5002".to_string(),
                cell_count: 0,
                registered: true,
                active_handover: false,
            },
        ];

        let targets = infer_split_activation_targets(&workers, "worker-a").expect("infer targets");
        assert_eq!(
            targets,
            vec![
                SplitActivationPlanTarget {
                    sub: 0,
                    worker_id: "worker-a".to_string()
                },
                SplitActivationPlanTarget {
                    sub: 1,
                    worker_id: "worker-b".to_string()
                },
                SplitActivationPlanTarget {
                    sub: 2,
                    worker_id: "worker-a".to_string()
                },
                SplitActivationPlanTarget {
                    sub: 3,
                    worker_id: "worker-c".to_string()
                },
            ]
        );
        validate_split_activation_plan_targets(&workers, "worker-a", &targets)
            .expect("targets valid");
    }

    #[test]
    fn split_activation_plan_rejects_unregistered_target() {
        let workers = vec![
            SplitActivationPlanWorker {
                worker_id: "worker-a".to_string(),
                addr: "127.0.0.1:5001".to_string(),
                cell_count: 1,
                registered: true,
                active_handover: false,
            },
            SplitActivationPlanWorker {
                worker_id: "worker-b".to_string(),
                addr: "127.0.0.1:5002".to_string(),
                cell_count: 0,
                registered: false,
                active_handover: false,
            },
        ];
        let targets = vec![
            SplitActivationPlanTarget {
                sub: 0,
                worker_id: "worker-a".to_string(),
            },
            SplitActivationPlanTarget {
                sub: 1,
                worker_id: "worker-b".to_string(),
            },
            SplitActivationPlanTarget {
                sub: 2,
                worker_id: "worker-a".to_string(),
            },
            SplitActivationPlanTarget {
                sub: 3,
                worker_id: "worker-b".to_string(),
            },
        ];

        let err = validate_split_activation_plan_targets(&workers, "worker-a", &targets)
            .expect_err("unregistered target must be rejected");
        assert!(err.contains("not registered"));
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
