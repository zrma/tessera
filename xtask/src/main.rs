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
    CellChildFamilyKind, CellId, ClientMsg, EntityId, Envelope, MAX_FRAME_LEN, Position,
    ServerEnvelope, ServerMsg, encode_frame,
};
use tessera_proto::orch::v1::orchestrator_client::OrchestratorClient;
use tessera_proto::orch::v1::{
    Assignment, AssignmentListing, HealthCheckRequest, ListAssignmentsRequest,
    MergeActivationRequest, MergeActivationResponse, MergeActivationState, OrchestratorHealth,
    SplitActivationRequest, SplitActivationResponse, SplitActivationState, SplitChildTarget,
    WorkerRegistration,
};

const P5_ROLLBACK_MERGE_ACTIVATION: &str =
    "same-Worker manual merge is available in P6 but is not automatic rollback";
const P5_HISTORICAL_ROLLBACK_MERGE_ACTIVATION: &str =
    "deferred outside the P5 split-activation completion boundary";

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
    /// Audit P6+ completion gates from report JSON and fail until all gates are covered
    P6CompletionAudit {
        /// Directory that contains smoke/report JSON files
        #[arg(long, default_value = ".dev/reports")]
        report_dir: PathBuf,
        /// Print machine-readable JSON instead of text
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Validate a P7 operation ledger JSON for proposal/approval/execution evidence
    P7OperationLedgerCheck {
        /// Operation ledger JSON path. Defaults to .dev/operation-ledger.json
        #[arg(long)]
        ledger: Option<PathBuf>,
        /// Require at least one durable approval record
        #[arg(long, default_value_t = false)]
        require_approval: bool,
        /// Require at least one blocked execution phase/status
        #[arg(long, default_value_t = false)]
        require_blocked_execution: bool,
        /// Require at least one published execution phase/status
        #[arg(long, default_value_t = false)]
        require_published_execution: bool,
        /// Require at least one completed observation phase/status
        #[arg(long, default_value_t = false)]
        require_completed_observation: bool,
        /// Require at least one recovery-required observation phase/status
        #[arg(long, default_value_t = false)]
        require_recovery_required: bool,
        /// Print machine-readable JSON instead of text
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Validate a P6 GitOps rollout evidence report JSON
    P6RolloutReportCheck {
        /// Report JSON path. Defaults to .dev/reports/p6-gitops-rollout-latest.json
        #[arg(long)]
        report: Option<PathBuf>,
        /// Require the rollout report image to match this image
        #[arg(long)]
        expected_image: Option<String>,
    },
    /// Write a read-only P6 GitOps rollout evidence report draft from cluster state
    P6RolloutReport {
        /// Kubernetes namespace that contains the Tessera runtime
        #[arg(long, default_value = "tessera")]
        namespace: String,
        /// Optional kube context. If unset, the current context is used.
        #[arg(long)]
        context: Option<String>,
        /// Orchestrator deployment name used for image evidence
        #[arg(long, default_value = "tessera-orch")]
        orch_deploy: String,
        /// Gateway deployment name used for image evidence
        #[arg(long, default_value = "tessera-gateway")]
        gateway_deploy: String,
        /// Source Worker deployment name used for image evidence
        #[arg(long, default_value = "tessera-worker")]
        source_worker_deploy: String,
        /// Target Worker deployment name used for image evidence
        #[arg(long, default_value = "tessera-worker-b")]
        target_worker_deploy: String,
        /// ArgoCD Application namespace used for Synced/Healthy evidence
        #[arg(long, default_value = "argocd")]
        argocd_namespace: String,
        /// ArgoCD Application name used for Synced/Healthy evidence
        #[arg(long, default_value = "tessera")]
        argocd_app: String,
        /// Skip reading the ArgoCD Application. The generated report will remain incomplete.
        #[arg(long, default_value_t = false)]
        skip_argocd_check: bool,
        /// P6 image tag that should be running in all recorded deployments
        #[arg(long)]
        image: String,
        /// Git/GitOps revision that rolled out the P6 image and runtime settings
        #[arg(long)]
        rollout_revision: Option<String>,
        /// Git/GitOps revision that restored default-off cleanup after the smoke
        #[arg(long)]
        cleanup_revision: Option<String>,
        /// Operator assertion that the image was published by the trusted pipeline
        #[arg(long, default_value_t = false)]
        image_published: bool,
        /// Operator assertion that the GitOps rollout was approved for the controlled smoke
        #[arg(long, default_value_t = false)]
        gitops_rollout_approved: bool,
        /// Operator assertion that post-smoke cleanup restored the default-off state
        #[arg(long, default_value_t = false)]
        post_smoke_default_off_cleanup: bool,
        /// Cleanup assertion that manual activation was disabled after the smoke
        #[arg(long, default_value_t = false)]
        manual_activation_default_off: bool,
        /// Cleanup assertion that preview fixture state was removed after the smoke
        #[arg(long, default_value_t = false)]
        preview_fixture_removed: bool,
        /// Output JSON path. Defaults to .dev/reports/p6-gitops-rollout-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
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
        /// Parent depth. Non-zero depth is intended for explicit --target-cell canonical requests.
        #[arg(long, default_value_t = 0)]
        depth: u8,
        /// Parent sub value. Canonical multi-depth parents use sub=0.
        #[arg(long, default_value_t = 0)]
        sub: u8,
        /// Child target mapping as sub=worker-id; pass exactly four, sub 0..3
        #[arg(long = "target")]
        targets: Vec<String>,
        /// Explicit child target mapping as world,cx,cy,depth,sub=worker-id; pass exactly four
        #[arg(long = "target-cell")]
        target_cells: Vec<String>,
    },
    /// Submit a default-off manual same-worker merge activation to an orchestrator
    MergeActivation {
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
        /// Parent depth. Non-zero depth is intended for canonical multi-depth merge parents.
        #[arg(long, default_value_t = 0)]
        depth: u8,
        /// Worker id that currently owns all four merge siblings
        #[arg(long)]
        owner_worker_id: String,
    },
    /// Build a read-only operator plan from split/merge preview output
    SplitActivationPlan {
        /// Orchestrator gRPC address or endpoint for health/listing checks
        #[arg(long, default_value = "127.0.0.1:6000")]
        orch_addr: String,
        /// Orchestrator metrics HTTP address that serves /split-merge/preview
        #[arg(long, default_value = "127.0.0.1:6100")]
        preview_addr: String,
        /// Worker metrics endpoint mapping as worker-id=addr; when present, builds the preview from live Worker metrics instead of /split-merge/preview
        #[arg(long = "live-worker-metrics")]
        live_worker_metrics: Vec<String>,
        /// Live metric actor-count threshold for proposing split operator plans
        #[arg(long, default_value_t = 100)]
        live_actor_threshold: u64,
        /// Live metric pending-move threshold for proposing split operator plans
        #[arg(long, default_value_t = 64)]
        live_move_threshold: u64,
        /// Minimum live pressure signals needed before a split candidate is proposed
        #[arg(long, default_value_t = 2)]
        live_min_pressure_signals: u8,
        /// Cell age value recorded in live-metrics preview evidence
        #[arg(long, default_value_t = 60)]
        live_cell_age_secs: u64,
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
    /// Build a read-only operator plan for manual same-worker merge activation
    MergeActivationPlan {
        /// Orchestrator gRPC address or endpoint for health/listing checks
        #[arg(long, default_value = "127.0.0.1:6000")]
        orch_addr: String,
        /// Orchestrator metrics HTTP address that serves /split-merge/preview
        #[arg(long, default_value = "127.0.0.1:6100")]
        preview_addr: String,
        /// Operator-chosen operation id; defaults to a timestamped id
        #[arg(long)]
        operation_id: Option<String>,
        /// Output JSON path. Defaults to .dev/reports/merge-activation-plan-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Build a planner-selected activation and mutate only when the policy gate is explicitly approved
    PlannerActivation {
        /// Planner kind to apply: split or merge
        #[arg(long, default_value = "merge")]
        kind: String,
        /// Orchestrator gRPC address or endpoint for health/listing checks and optional submission
        #[arg(long, default_value = "127.0.0.1:6000")]
        orch_addr: String,
        /// Orchestrator metrics HTTP address that serves /split-merge/preview
        #[arg(long, default_value = "127.0.0.1:6100")]
        preview_addr: String,
        /// Worker metrics endpoint mapping as worker-id=addr; only supported for --kind split
        #[arg(long = "live-worker-metrics")]
        live_worker_metrics: Vec<String>,
        /// Live metric actor-count threshold for proposing split planner activations
        #[arg(long, default_value_t = 100)]
        live_actor_threshold: u64,
        /// Live metric pending-move threshold for proposing split planner activations
        #[arg(long, default_value_t = 64)]
        live_move_threshold: u64,
        /// Minimum live pressure signals needed before a split planner activation is proposed
        #[arg(long, default_value_t = 2)]
        live_min_pressure_signals: u8,
        /// Cell age value recorded in live-metrics planner evidence
        #[arg(long, default_value_t = 60)]
        live_cell_age_secs: u64,
        /// Operator-chosen operation id; defaults to a timestamped id
        #[arg(long)]
        operation_id: Option<String>,
        /// Permit the helper to submit the selected planner action
        #[arg(long, default_value_t = false)]
        allow_mutation: bool,
        /// Required policy id when --allow-mutation is set
        #[arg(long)]
        policy_id: Option<String>,
        /// Output JSON path. Defaults to .dev/reports/planner-activation-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Kubernetes helpers for the guarded Kubernetes runtime smoke gate
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
        /// Worker id represented by --source-worker-deploy
        #[arg(long, default_value = "worker-a")]
        source_worker_id: String,
        /// Source Worker service name for live metrics port-forward
        #[arg(long, default_value = "tessera-worker")]
        source_worker_service: String,
        /// Target Worker deployment used for optional failure/recovery smoke
        #[arg(long, default_value = "tessera-worker-b")]
        target_worker_deploy: String,
        /// Target Worker service name for live metrics port-forward
        #[arg(long, default_value = "tessera-worker-b")]
        target_worker_service: String,
        /// Worker id represented by --target-worker-deploy
        #[arg(long, default_value = "worker-b")]
        target_worker_id: String,
        /// Require the target Worker deployment/image during read-only preflight
        #[arg(long, default_value_t = false)]
        require_target_worker: bool,
        /// Require PVC-backed Orchestrator assignment-state storage during read-only preflight
        #[arg(long, default_value_t = false)]
        require_assignment_state_storage: bool,
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
        /// Local source Worker metrics port for kubectl port-forward when --use-live-worker-metrics is set
        #[arg(long, default_value_t = 5100)]
        local_source_worker_metrics_port: u16,
        /// Local target Worker metrics port for kubectl port-forward when --use-live-worker-metrics is set
        #[arg(long, default_value_t = 5101)]
        local_target_worker_metrics_port: u16,
        /// Build the split activation operator plan from live Worker /metrics instead of Orchestrator /split-merge/preview
        #[arg(long, default_value_t = false)]
        use_live_worker_metrics: bool,
        /// Live metric actor-count threshold for proposing split operator plans
        #[arg(long, default_value_t = 100)]
        live_actor_threshold: u64,
        /// Live metric pending-move threshold for proposing split operator plans
        #[arg(long, default_value_t = 64)]
        live_move_threshold: u64,
        /// Minimum live pressure signals needed before a split candidate is proposed
        #[arg(long, default_value_t = 2)]
        live_min_pressure_signals: u8,
        /// Cell age value recorded in live-metrics preview evidence
        #[arg(long, default_value_t = 60)]
        live_cell_age_secs: u64,
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
        /// Also restart the Orchestrator deployment and verify persisted split recovery
        #[arg(long, default_value_t = false)]
        with_restart: bool,
        /// Required with --with-restart because it mutates the Orchestrator deployment rollout
        #[arg(long, default_value_t = false)]
        allow_rollout_restart: bool,
        /// Expected Orchestrator assignment-state path in the live deployment
        #[arg(long, default_value = "/var/lib/tessera/assignment-state.json")]
        expected_assignment_state_path: String,
        /// Output JSON path. Defaults to .dev/reports/guarded-kubernetes-activation-smoke-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Validate an internal activation smoke report JSON
    ActivationReportCheck {
        /// Report JSON path. Defaults to .dev/reports/guarded-kubernetes-activation-smoke-latest.json
        #[arg(long)]
        report: Option<PathBuf>,
        /// Require a successful split publish report
        #[arg(long, default_value_t = false)]
        require_published: bool,
        /// Require post-publish target outage and restart recovery evidence
        #[arg(long, default_value_t = false)]
        require_failure: bool,
        /// Require Orchestrator restart recovery from persistent assignment state evidence
        #[arg(long, default_value_t = false)]
        require_restart: bool,
        /// Require the report plan to be sourced from live Worker metrics
        #[arg(long, default_value_t = false)]
        require_live_metrics_plan: bool,
        /// Require all recorded deployment images to match this image
        #[arg(long)]
        expected_image: Option<String>,
        /// Require a blocked/preflight report to contain this substring in preflight_errors[]
        #[arg(long = "expect-preflight-error")]
        expect_preflight_errors: Vec<String>,
    },
    /// Validate an internal planner mutation evidence report JSON
    PlannerActivationReportCheck {
        /// Report JSON path. Defaults to .dev/reports/guarded-kubernetes-planner-activation-latest.json
        #[arg(long)]
        report: Option<PathBuf>,
        /// Require all recorded deployment images to match this image
        #[arg(long)]
        expected_image: Option<String>,
        /// Require the policy-approved published report to come from live Worker metrics
        #[arg(long, default_value_t = false)]
        require_live_metrics_plan: bool,
    },
    /// Compose an internal planner mutation report from local planner reports and read-only cluster state
    PlannerActivationReport {
        /// Kubernetes namespace that contains the Tessera runtime
        #[arg(long, default_value = "tessera")]
        namespace: String,
        /// Optional kube context. If unset, the current context is used.
        #[arg(long)]
        context: Option<String>,
        /// Orchestrator deployment name used for image evidence
        #[arg(long, default_value = "tessera-orch")]
        orch_deploy: String,
        /// Gateway deployment name used for image evidence
        #[arg(long, default_value = "tessera-gateway")]
        gateway_deploy: String,
        /// Source Worker deployment name used for image evidence
        #[arg(long, default_value = "tessera-worker")]
        source_worker_deploy: String,
        /// Target Worker deployment name used for image evidence
        #[arg(long, default_value = "tessera-worker-b")]
        target_worker_deploy: String,
        /// ArgoCD Application namespace used for Synced/Healthy evidence
        #[arg(long, default_value = "argocd")]
        argocd_namespace: String,
        /// ArgoCD Application name used for Synced/Healthy evidence
        #[arg(long, default_value = "tessera")]
        argocd_app: String,
        /// Skip reading the ArgoCD Application. The generated report will remain incomplete.
        #[arg(long, default_value_t = false)]
        skip_argocd_check: bool,
        /// Expected runtime image for all Tessera deployments
        #[arg(long)]
        expected_image: Option<String>,
        /// Default-off blocked planner report. Defaults to .dev/reports/planner-activation-blocked-latest.json
        #[arg(long)]
        blocked_report: Option<PathBuf>,
        /// Policy-approved published planner report. Defaults to .dev/reports/planner-activation-latest.json
        #[arg(long)]
        published_report: Option<PathBuf>,
        /// Output JSON path. Defaults to .dev/reports/guarded-kubernetes-planner-activation-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Build a port-forwarded internal canonical multi-depth split plan and optionally run guarded mutation
    MultiDepthActivationSmoke {
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
        /// Gateway service name recorded in internal evidence
        #[arg(long, default_value = "tessera-gateway")]
        gateway_service: String,
        /// Gateway deployment name used for image preflight
        #[arg(long, default_value = "tessera-gateway")]
        gateway_deploy: String,
        /// Source Worker deployment expected to own the canonical parent
        #[arg(long, default_value = "tessera-worker")]
        source_worker_deploy: String,
        /// Worker id expected to own the canonical parent
        #[arg(long, default_value = "worker-a")]
        source_worker_id: String,
        /// Target Worker deployment expected to receive canonical child cells
        #[arg(long, default_value = "tessera-worker-b")]
        target_worker_deploy: String,
        /// Worker id represented by --target-worker-deploy
        #[arg(long, default_value = "worker-b")]
        target_worker_id: String,
        /// Parent world id
        #[arg(long, default_value_t = 0)]
        world: u32,
        /// Parent canonical leaf x coordinate
        #[arg(long, default_value_t = -2)]
        cx: i32,
        /// Parent canonical leaf y coordinate
        #[arg(long, default_value_t = 3)]
        cy: i32,
        /// Parent depth. Must be >0 for canonical multi-depth readiness.
        #[arg(long, default_value_t = 2)]
        depth: u8,
        /// Parent sub value. Canonical multi-depth parents use sub=0.
        #[arg(long = "sub", default_value_t = 0)]
        sub_cell: u8,
        /// Explicit child target mapping as world,cx,cy,depth,sub=worker-id; pass exactly four
        #[arg(long = "target-cell")]
        target_cells: Vec<String>,
        /// ArgoCD Application namespace used for Synced/Healthy preflight
        #[arg(long, default_value = "argocd")]
        argocd_namespace: String,
        /// ArgoCD Application name used for Synced/Healthy preflight
        #[arg(long, default_value = "tessera")]
        argocd_app: String,
        /// Skip the ArgoCD Application Synced/Healthy preflight
        #[arg(long, default_value_t = false)]
        skip_argocd_check: bool,
        /// Expected runtime image for all recorded Tessera deployments
        #[arg(long)]
        expected_image: Option<String>,
        /// Local Orchestrator gRPC port for kubectl port-forward
        #[arg(long, default_value_t = 6000)]
        local_orch_port: u16,
        /// Local Gateway TCP port for kubectl port-forward when --allow-activation is set
        #[arg(long, default_value_t = 4000)]
        local_gateway_port: u16,
        /// Local Gateway metrics/ready port for kubectl port-forward when --allow-activation is set
        #[arg(long, default_value_t = 4100)]
        local_gateway_metrics_port: u16,
        /// Operator-chosen operation id; defaults to a timestamped internal multi-depth id
        #[arg(long)]
        operation_id: Option<String>,
        /// Actually submit SubmitSplitActivation after writing the read-only plan
        #[arg(long, default_value_t = false)]
        allow_activation: bool,
        /// Also scale the target Worker down/up to verify failure and recovery
        #[arg(long, default_value_t = false)]
        with_failure: bool,
        /// Required with --with-failure because it mutates the target Worker deployment replica count
        #[arg(long, default_value_t = false)]
        allow_scale: bool,
        /// Also restart the Orchestrator deployment and verify persisted canonical split recovery
        #[arg(long, default_value_t = false)]
        with_restart: bool,
        /// Required with --with-restart because it mutates the Orchestrator deployment rollout
        #[arg(long, default_value_t = false)]
        allow_rollout_restart: bool,
        /// Run canonical child-route load/soak traffic after publish
        #[arg(long, default_value_t = false)]
        with_soak: bool,
        /// Child-route soak iterations per child when --with-soak is set
        #[arg(long, default_value_t = 32)]
        soak_iterations: u32,
        /// Sleep between child-route soak iterations
        #[arg(long, default_value_t = 10)]
        soak_sleep_ms: u64,
        /// Output JSON path. Defaults to .dev/reports/guarded-kubernetes-multi-depth-activation-smoke-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Validate an internal canonical multi-depth split readiness report JSON
    MultiDepthActivationReportCheck {
        /// Report JSON path. Defaults to .dev/reports/guarded-kubernetes-multi-depth-activation-smoke-latest.json
        #[arg(long)]
        report: Option<PathBuf>,
        /// Require the report to contain a ready canonical multi-depth plan
        #[arg(long, default_value_t = false)]
        require_ready_plan: bool,
        /// Require approved internal canonical multi-depth publish evidence
        #[arg(long, default_value_t = false)]
        require_published: bool,
        /// Require post-publish target Worker outage/recovery evidence
        #[arg(long, default_value_t = false)]
        require_failure: bool,
        /// Require Orchestrator restart recovery evidence after multi-depth publish
        #[arg(long, default_value_t = false)]
        require_restart: bool,
        /// Require internal canonical multi-depth load/soak evidence
        #[arg(long, default_value_t = false)]
        require_soak: bool,
        /// Require all recorded deployment images to match this image
        #[arg(long)]
        expected_image: Option<String>,
        /// Require a blocked/preflight report to contain this substring in preflight_errors[]
        #[arg(long = "expect-preflight-error")]
        expect_preflight_errors: Vec<String>,
    },
    /// Build the port-forwarded internal merge activation plan and optionally run guarded mutation
    MergeActivationSmoke {
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
        /// Gateway service name recorded in internal evidence
        #[arg(long, default_value = "tessera-gateway")]
        gateway_service: String,
        /// Gateway deployment name used for image preflight
        #[arg(long, default_value = "tessera-gateway")]
        gateway_deploy: String,
        /// Worker deployment expected to own the merge siblings
        #[arg(long, default_value = "tessera-worker")]
        owner_worker_deploy: String,
        /// Worker id expected to own all four merge siblings
        #[arg(long, default_value = "worker-a")]
        owner_worker_id: String,
        /// ArgoCD Application namespace used for Synced/Healthy preflight
        #[arg(long, default_value = "argocd")]
        argocd_namespace: String,
        /// ArgoCD Application name used for Synced/Healthy preflight
        #[arg(long, default_value = "tessera")]
        argocd_app: String,
        /// Skip the ArgoCD Application Synced/Healthy preflight
        #[arg(long, default_value_t = false)]
        skip_argocd_check: bool,
        /// Expected runtime image for all recorded Tessera deployments
        #[arg(long)]
        expected_image: Option<String>,
        /// Local Orchestrator gRPC port for kubectl port-forward
        #[arg(long, default_value_t = 6000)]
        local_orch_port: u16,
        /// Local Orchestrator metrics/preview port for kubectl port-forward
        #[arg(long, default_value_t = 6100)]
        local_orch_metrics_port: u16,
        /// Local Gateway TCP port for kubectl port-forward when --allow-activation is set
        #[arg(long, default_value_t = 4000)]
        local_gateway_port: u16,
        /// Local Gateway metrics/ready port for kubectl port-forward when --allow-activation is set
        #[arg(long, default_value_t = 4100)]
        local_gateway_metrics_port: u16,
        /// Operator-chosen operation id; defaults to a timestamped internal merge id
        #[arg(long)]
        operation_id: Option<String>,
        /// Actually submit SubmitMergeActivation after writing the read-only plan
        #[arg(long, default_value_t = false)]
        allow_activation: bool,
        /// Also scale the owner Worker down/up to verify failure and recovery
        #[arg(long, default_value_t = false)]
        with_failure: bool,
        /// Required with --with-failure because it mutates the owner Worker deployment replica count
        #[arg(long, default_value_t = false)]
        allow_scale: bool,
        /// Also restart the Orchestrator deployment and verify persisted merge recovery
        #[arg(long, default_value_t = false)]
        with_restart: bool,
        /// Required with --with-restart because it mutates the Orchestrator deployment rollout
        #[arg(long, default_value_t = false)]
        allow_rollout_restart: bool,
        /// Run parent-route load/soak traffic after merge publish
        #[arg(long, default_value_t = false)]
        with_soak: bool,
        /// Parent-route soak iterations per actor when --with-soak is set
        #[arg(long, default_value_t = 32)]
        soak_iterations: u32,
        /// Sleep between parent-route soak iterations
        #[arg(long, default_value_t = 10)]
        soak_sleep_ms: u64,
        /// Output JSON path. Defaults to .dev/reports/guarded-kubernetes-merge-activation-smoke-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Validate an internal merge activation smoke report JSON
    MergeActivationReportCheck {
        /// Report JSON path. Defaults to .dev/reports/guarded-kubernetes-merge-activation-smoke-latest.json
        #[arg(long)]
        report: Option<PathBuf>,
        /// Require the report to contain a ready merge plan
        #[arg(long, default_value_t = false)]
        require_ready_plan: bool,
        /// Require approved internal merge publish evidence
        #[arg(long, default_value_t = false)]
        require_published: bool,
        /// Require post-publish owner Worker outage/recovery evidence
        #[arg(long, default_value_t = false)]
        require_failure: bool,
        /// Require Orchestrator restart recovery evidence after merge publish
        #[arg(long, default_value_t = false)]
        require_restart: bool,
        /// Require internal merge load/soak evidence
        #[arg(long, default_value_t = false)]
        require_soak: bool,
        /// Require all recorded deployment images to match this image
        #[arg(long)]
        expected_image: Option<String>,
        /// Require a blocked/preflight report to contain this substring in preflight_errors[]
        #[arg(long = "expect-preflight-error")]
        expect_preflight_errors: Vec<String>,
    },
    /// Build a port-forwarded internal P7 operation plan and optionally run guarded execution
    OperationSmoke {
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
        /// Worker deployment expected to own the merge operation
        #[arg(long, default_value = "tessera-worker")]
        owner_worker_deploy: String,
        /// Worker service used for parent actor metrics
        #[arg(long, default_value = "tessera-worker")]
        owner_worker_service: String,
        /// Worker id expected to own the merge operation
        #[arg(long, default_value = "worker-a")]
        owner_worker_id: String,
        /// ArgoCD Application namespace used for Synced/Healthy preflight
        #[arg(long, default_value = "argocd")]
        argocd_namespace: String,
        /// ArgoCD Application name used for Synced/Healthy preflight
        #[arg(long, default_value = "tessera")]
        argocd_app: String,
        /// Skip the ArgoCD Application Synced/Healthy preflight
        #[arg(long, default_value_t = false)]
        skip_argocd_check: bool,
        /// Expected runtime image for all recorded Tessera deployments
        #[arg(long)]
        expected_image: Option<String>,
        /// Local Orchestrator gRPC port for kubectl port-forward
        #[arg(long, default_value_t = 6000)]
        local_orch_port: u16,
        /// Local Orchestrator metrics/operation port for kubectl port-forward
        #[arg(long, default_value_t = 6100)]
        local_orch_metrics_port: u16,
        /// Local Gateway TCP port for kubectl port-forward when --allow-execution is set
        #[arg(long, default_value_t = 4000)]
        local_gateway_port: u16,
        /// Local Gateway metrics/ready port for kubectl port-forward when --allow-execution is set
        #[arg(long, default_value_t = 4100)]
        local_gateway_metrics_port: u16,
        /// Local owner Worker metrics port for kubectl port-forward when --allow-execution is set
        #[arg(long, default_value_t = 5100)]
        local_owner_worker_metrics_port: u16,
        /// Approve and execute the selected P7 merge operation
        #[arg(long, default_value_t = false)]
        allow_execution: bool,
        /// Run parent-route load/soak traffic before closing the observation
        #[arg(long, default_value_t = false)]
        with_soak: bool,
        /// Scale the owner Worker down after publishing and require recovery_required observation evidence
        #[arg(long, default_value_t = false)]
        with_failure: bool,
        /// Allow the smoke helper to scale the owner Worker deployment for --with-failure
        #[arg(long, default_value_t = false)]
        allow_scale: bool,
        /// Restart the Orchestrator deployment after publish and close observation after recovery
        #[arg(long, default_value_t = false)]
        with_restart: bool,
        /// Allow the smoke helper to rollout-restart the Orchestrator deployment for --with-restart
        #[arg(long, default_value_t = false)]
        allow_rollout_restart: bool,
        /// Assignment state path expected on the Orchestrator deployment when --with-restart is set
        #[arg(
            long,
            default_value = "/var/lib/tessera/assignment-state-p7-operation-restart-20260504.json"
        )]
        expected_assignment_state_path: String,
        /// Parent-route soak iterations per actor when --with-soak is set
        #[arg(long, default_value_t = 16)]
        soak_iterations: u32,
        /// Sleep between parent-route soak iterations
        #[arg(long, default_value_t = 10)]
        soak_sleep_ms: u64,
        /// Output JSON path. Defaults to .dev/reports/guarded-kubernetes-p7-operation-smoke-latest.json
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Validate an internal P7 operation smoke report JSON
    OperationReportCheck {
        /// Report JSON path. Defaults to .dev/reports/guarded-kubernetes-p7-operation-smoke-latest.json
        #[arg(long)]
        report: Option<PathBuf>,
        /// Require approved internal operation execution evidence
        #[arg(long, default_value_t = false)]
        require_published_execution: bool,
        /// Require completed observation evidence after execution
        #[arg(long, default_value_t = false)]
        require_completed_observation: bool,
        /// Require internal parent-route soak evidence
        #[arg(long, default_value_t = false)]
        require_soak: bool,
        /// Require owner Worker outage and recovery_required operation evidence
        #[arg(long, default_value_t = false)]
        require_recovery_required: bool,
        /// Require Orchestrator rollout restart recovery evidence after execution
        #[arg(long, default_value_t = false)]
        require_restart: bool,
        /// Require all recorded deployment images to match this image
        #[arg(long)]
        expected_image: Option<String>,
        /// Require a blocked/preflight report to contain this substring in preflight_errors[]
        #[arg(long = "expect-preflight-error")]
        expect_preflight_errors: Vec<String>,
    },
}

#[allow(clippy::large_enum_variant)]
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
    /// Start a two-worker dev stack and prove split assignments survive Orchestrator restart
    ActivationRestartSmoke,
    /// Start a two-worker dev stack and prove preview can become an operator plan without mutation
    ActivationPlanSmoke,
    /// Start a two-worker dev stack and prove live Worker metrics can become an operator plan without mutation
    ActivationLivePlanSmoke,
    /// Start a two-worker dev stack and publish using a live Worker metrics operator plan
    ActivationLiveMetricsSmoke,
    /// Start a two-worker dev stack and prove live metrics planner mutation stays policy-gated
    ActivationLivePlannerMutationSmoke,
    /// Start an Orchestrator-only dev stack and prove the P7 proposal/approval/default-off execution loop
    P7OperationLoopSmoke,
    /// Start an Orchestrator-only dev stack and prove approved P7 merge execution publishes once
    P7OperationExecutionSmoke,
    /// Start a full dev stack and close an approved P7 merge execution with observation evidence
    P7OperationObservationSmoke,
    /// Start a full dev stack and prove a failed P7 observation stays recovery-required until operator recovery
    P7OperationRecoverySmoke,
    /// Start a full dev stack and prove P7 operation state survives Orchestrator restart
    P7OperationRestartSmoke,
    /// Start a full dev stack and run sustained traffic after approved P7 merge execution
    P7OperationSoakSmoke {
        /// Per-parent-route actor ping/move iterations after operation execution
        #[arg(long, default_value_t = 16)]
        iterations: u32,
        /// Delay between parent-route soak iterations, in milliseconds
        #[arg(long, default_value_t = 10)]
        sleep_ms: u64,
    },
    /// Start a two-worker dev stack and prove canonical explicit child-cell split activation converges
    MultiDepthActivationSmoke,
    /// Start a two-worker dev stack and prove canonical split target outage is recoverable
    MultiDepthActivationFailureSmoke,
    /// Start a two-worker dev stack and prove canonical split assignments survive Orchestrator restart
    MultiDepthActivationRestartSmoke,
    /// Start a two-worker dev stack and run sustained traffic after canonical split activation
    MultiDepthActivationSoak {
        /// Per-child ping/move iterations after activation publish
        #[arg(long, default_value_t = 32)]
        iterations: u32,
        /// Delay between iterations, in milliseconds
        #[arg(long, default_value_t = 10)]
        sleep_ms: u64,
    },
    /// Validate the latest local canonical explicit child-cell split activation report
    MultiDepthActivationReportCheck {
        /// Multi-depth activation smoke report path
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Validate the latest local canonical explicit child-cell split failure report
    MultiDepthActivationFailureReportCheck {
        /// Multi-depth activation failure smoke report path
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Validate the latest local canonical explicit child-cell split restart report
    MultiDepthActivationRestartReportCheck {
        /// Multi-depth activation restart smoke report path
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Validate the latest local canonical explicit child-cell split soak report
    MultiDepthActivationSoakReportCheck {
        /// Multi-depth activation soak report path
        #[arg(long)]
        report: Option<PathBuf>,
        /// Minimum per-child soak iterations expected in the soak report
        #[arg(long, default_value_t = 32)]
        min_iterations: u32,
    },
    /// Start a two-worker dev stack and prove a merge candidate can become an operator plan without mutation
    MergePlanSmoke,
    /// Start a two-worker dev stack and prove planner mutation stays policy-gated
    PlannerMutationSmoke,
    /// Start a two-worker dev stack and prove same-worker merge activation coalesces runtime state
    MergeActivationSmoke,
    /// Start a two-worker dev stack and prove canonical same-worker merge activation coalesces runtime state
    CanonicalMergeActivationSmoke,
    /// Validate the latest local canonical same-worker merge activation report
    CanonicalMergeActivationReportCheck {
        /// Canonical merge activation smoke report path
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Start a two-worker dev stack and prove canonical same-worker merge survives Orchestrator restart
    CanonicalMergeActivationRestartSmoke,
    /// Validate the latest local canonical same-worker merge restart report
    CanonicalMergeActivationRestartReportCheck {
        /// Canonical merge activation restart smoke report path
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Start a two-worker dev stack and prove canonical same-worker merge owner outage is detectable/recoverable
    CanonicalMergeActivationFailureSmoke,
    /// Validate the latest local canonical same-worker merge failure report
    CanonicalMergeActivationFailureReportCheck {
        /// Canonical merge activation failure smoke report path
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Start a two-worker dev stack and run sustained traffic after canonical same-worker merge activation
    CanonicalMergeActivationSoak {
        /// Per-actor ping/move iterations after merge publish
        #[arg(long, default_value_t = 32)]
        iterations: u32,
        /// Delay between iterations, in milliseconds
        #[arg(long, default_value_t = 10)]
        sleep_ms: u64,
    },
    /// Validate the latest local canonical same-worker merge soak report
    CanonicalMergeActivationSoakReportCheck {
        /// Canonical merge activation soak report path
        #[arg(long)]
        report: Option<PathBuf>,
        /// Minimum per-actor soak iterations expected in the soak report
        #[arg(long, default_value_t = 32)]
        min_iterations: u32,
    },
    /// Start a two-worker dev stack and prove mixed-owner merge replays remote child state
    MergeActivationCrossWorkerSmoke,
    /// Start a two-worker dev stack and prove same-worker merge survives Orchestrator restart
    MergeActivationRestartSmoke,
    /// Start a two-worker dev stack and prove same-worker merge owner outage is detectable/recoverable
    MergeActivationFailureSmoke,
    /// Start a two-worker dev stack and run sustained traffic after same-worker merge activation
    MergeActivationSoak {
        /// Per-actor ping/move iterations after merge publish
        #[arg(long, default_value_t = 32)]
        iterations: u32,
        /// Delay between iterations, in milliseconds
        #[arg(long, default_value_t = 10)]
        sleep_ms: u64,
    },
    /// Validate the latest local same-worker merge activation soak report
    MergeActivationSoakReportCheck {
        /// Merge activation soak report path
        #[arg(long)]
        report: Option<PathBuf>,
        /// Minimum per-actor soak iterations expected in the soak report
        #[arg(long, default_value_t = 32)]
        min_iterations: u32,
    },
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
        /// Activation restart recovery report path
        #[arg(long)]
        restart_report: Option<PathBuf>,
        /// Merge activation plan report path
        #[arg(long)]
        merge_plan_report: Option<PathBuf>,
        /// Merge activation smoke report path
        #[arg(long)]
        merge_activation_report: Option<PathBuf>,
        /// Cross-Worker merge activation smoke report path
        #[arg(long)]
        merge_cross_worker_report: Option<PathBuf>,
        /// Merge activation failure/recovery smoke report path
        #[arg(long)]
        merge_failure_report: Option<PathBuf>,
        /// Merge activation restart smoke report path
        #[arg(long)]
        merge_restart_report: Option<PathBuf>,
        /// Merge activation soak smoke report path
        #[arg(long)]
        merge_soak_report: Option<PathBuf>,
        /// Policy-gated planner mutation report path
        #[arg(long)]
        planner_mutation_report: Option<PathBuf>,
        /// Require the split activation plan report to come from live Worker metrics
        #[arg(long, default_value_t = false)]
        require_live_metrics_plan: bool,
        /// Require the planner mutation report to come from live Worker metrics
        #[arg(long, default_value_t = false)]
        require_planner_live_metrics: bool,
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
        Cmd::P6CompletionAudit { report_dir, json } => run_p6_completion_audit(&report_dir, json)?,
        Cmd::P7OperationLedgerCheck {
            ledger,
            require_approval,
            require_blocked_execution,
            require_published_execution,
            require_completed_observation,
            require_recovery_required,
            json,
        } => run_p7_operation_ledger_check(
            ledger.as_deref(),
            require_approval,
            require_blocked_execution,
            require_published_execution,
            require_completed_observation,
            require_recovery_required,
            json,
        )?,
        Cmd::P6RolloutReportCheck {
            report,
            expected_image,
        } => run_p6_rollout_report_check(report.as_deref(), expected_image.as_deref())?,
        Cmd::P6RolloutReport {
            namespace,
            context,
            orch_deploy,
            gateway_deploy,
            source_worker_deploy,
            target_worker_deploy,
            argocd_namespace,
            argocd_app,
            skip_argocd_check,
            image,
            rollout_revision,
            cleanup_revision,
            image_published,
            gitops_rollout_approved,
            post_smoke_default_off_cleanup,
            manual_activation_default_off,
            preview_fixture_removed,
            out,
        } => run_p6_rollout_report(P6RolloutReportOptions {
            namespace,
            context,
            orch_deploy,
            gateway_deploy,
            source_worker_deploy,
            target_worker_deploy,
            argocd_namespace,
            argocd_app,
            skip_argocd_check,
            image,
            rollout_revision,
            cleanup_revision,
            image_published,
            gitops_rollout_approved,
            post_smoke_default_off_cleanup,
            manual_activation_default_off,
            preview_fixture_removed,
            out,
        })?,
        Cmd::SplitActivation {
            orch_addr,
            operation_id,
            world,
            cx,
            cy,
            depth,
            sub,
            targets,
            target_cells,
        } => run_split_activation_operator(
            &orch_addr,
            operation_id,
            CellId {
                world,
                cx,
                cy,
                depth,
                sub,
            },
            &targets,
            &target_cells,
        )?,
        Cmd::MergeActivation {
            orch_addr,
            operation_id,
            world,
            cx,
            cy,
            depth,
            owner_worker_id,
        } => run_merge_activation_operator(
            &orch_addr,
            operation_id,
            CellId {
                world,
                cx,
                cy,
                depth,
                sub: 0,
            },
            owner_worker_id,
        )?,
        Cmd::SplitActivationPlan {
            orch_addr,
            preview_addr,
            live_worker_metrics,
            live_actor_threshold,
            live_move_threshold,
            live_min_pressure_signals,
            live_cell_age_secs,
            operation_id,
            targets,
            out,
        } => run_split_activation_plan_operator(
            &orch_addr,
            &preview_addr,
            &live_worker_metrics,
            LiveMetricsPlanPolicy {
                actor_threshold: live_actor_threshold,
                move_threshold: live_move_threshold,
                min_pressure_signals: live_min_pressure_signals,
                cell_age_secs: live_cell_age_secs,
            },
            operation_id,
            &targets,
            out.as_deref(),
        )?,
        Cmd::MergeActivationPlan {
            orch_addr,
            preview_addr,
            operation_id,
            out,
        } => run_merge_activation_plan_operator(
            &orch_addr,
            &preview_addr,
            operation_id,
            out.as_deref(),
        )?,
        Cmd::PlannerActivation {
            kind,
            orch_addr,
            preview_addr,
            live_worker_metrics,
            live_actor_threshold,
            live_move_threshold,
            live_min_pressure_signals,
            live_cell_age_secs,
            operation_id,
            allow_mutation,
            policy_id,
            out,
        } => run_planner_activation_operator(PlannerActivationOptions {
            kind: &kind,
            orch_addr: &orch_addr,
            plan_source: PlannerActivationPlanSource {
                preview_addr: &preview_addr,
                live_worker_metrics: &live_worker_metrics,
                live_policy: LiveMetricsPlanPolicy {
                    actor_threshold: live_actor_threshold,
                    move_threshold: live_move_threshold,
                    min_pressure_signals: live_min_pressure_signals,
                    cell_age_secs: live_cell_age_secs,
                },
            },
            operation_id,
            mutation: PlannerActivationMutation {
                allow_mutation,
                policy_id: policy_id.as_deref(),
                out: out.as_deref(),
            },
        })?,
        Cmd::K8s { sub } => match *sub {
            K8sSub::ActivationSmoke {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                source_worker_deploy,
                source_worker_id,
                source_worker_service,
                target_worker_deploy,
                target_worker_service,
                target_worker_id,
                require_target_worker,
                require_assignment_state_storage,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                local_source_worker_metrics_port,
                local_target_worker_metrics_port,
                use_live_worker_metrics,
                live_actor_threshold,
                live_move_threshold,
                live_min_pressure_signals,
                live_cell_age_secs,
                operation_id,
                targets,
                allow_activation,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                expected_assignment_state_path,
                out,
            } => run_k8s_activation_smoke(K8sActivationSmokeOptions {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                source_worker_deploy,
                source_worker_id,
                source_worker_service,
                target_worker_deploy,
                target_worker_service,
                target_worker_id,
                require_target_worker,
                require_assignment_state_storage,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                local_source_worker_metrics_port,
                local_target_worker_metrics_port,
                use_live_worker_metrics,
                live_actor_threshold,
                live_move_threshold,
                live_min_pressure_signals,
                live_cell_age_secs,
                operation_id,
                targets,
                allow_activation,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                expected_assignment_state_path,
                out,
            })?,
            K8sSub::ActivationReportCheck {
                report,
                require_published,
                require_failure,
                require_restart,
                require_live_metrics_plan,
                expected_image,
                expect_preflight_errors,
            } => run_k8s_activation_report_check(
                report.as_deref(),
                require_published,
                require_failure,
                require_restart,
                require_live_metrics_plan,
                expected_image.as_deref(),
                &expect_preflight_errors,
            )?,
            K8sSub::PlannerActivationReportCheck {
                report,
                expected_image,
                require_live_metrics_plan,
            } => run_k8s_planner_activation_report_check(
                report.as_deref(),
                expected_image.as_deref(),
                require_live_metrics_plan,
            )?,
            K8sSub::PlannerActivationReport {
                namespace,
                context,
                orch_deploy,
                gateway_deploy,
                source_worker_deploy,
                target_worker_deploy,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                blocked_report,
                published_report,
                out,
            } => run_k8s_planner_activation_report(K8sPlannerActivationReportOptions {
                namespace,
                context,
                orch_deploy,
                gateway_deploy,
                source_worker_deploy,
                target_worker_deploy,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                blocked_report,
                published_report,
                out,
            })?,
            K8sSub::MultiDepthActivationSmoke {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                source_worker_deploy,
                source_worker_id,
                target_worker_deploy,
                target_worker_id,
                world,
                cx,
                cy,
                depth,
                sub_cell,
                target_cells,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_gateway_port,
                local_gateway_metrics_port,
                operation_id,
                allow_activation,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                with_soak,
                soak_iterations,
                soak_sleep_ms,
                out,
            } => run_k8s_multi_depth_activation_smoke(K8sMultiDepthActivationSmokeOptions {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                source_worker_deploy,
                source_worker_id,
                target_worker_deploy,
                target_worker_id,
                parent: CellId {
                    world,
                    cx,
                    cy,
                    depth,
                    sub: sub_cell,
                },
                target_cells,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_gateway_port,
                local_gateway_metrics_port,
                operation_id,
                allow_activation,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                with_soak,
                soak_iterations,
                soak_sleep_ms,
                out,
            })?,
            K8sSub::MultiDepthActivationReportCheck {
                report,
                require_ready_plan,
                require_published,
                require_failure,
                require_restart,
                require_soak,
                expected_image,
                expect_preflight_errors,
            } => run_k8s_multi_depth_activation_report_check(
                report.as_deref(),
                require_ready_plan,
                InternalCompletionRequirements {
                    require_published,
                    require_failure,
                    require_restart,
                    require_soak,
                },
                expected_image.as_deref(),
                &expect_preflight_errors,
            )?,
            K8sSub::MergeActivationSmoke {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                owner_worker_deploy,
                owner_worker_id,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                operation_id,
                allow_activation,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                with_soak,
                soak_iterations,
                soak_sleep_ms,
                out,
            } => run_k8s_merge_activation_smoke(K8sMergeActivationSmokeOptions {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                owner_worker_deploy,
                owner_worker_id,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                operation_id,
                allow_activation,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                with_soak,
                soak_iterations,
                soak_sleep_ms,
                out,
            })?,
            K8sSub::MergeActivationReportCheck {
                report,
                require_ready_plan,
                require_published,
                require_failure,
                require_restart,
                require_soak,
                expected_image,
                expect_preflight_errors,
            } => run_k8s_merge_activation_report_check(
                report.as_deref(),
                require_ready_plan,
                InternalCompletionRequirements {
                    require_published,
                    require_failure,
                    require_restart,
                    require_soak,
                },
                expected_image.as_deref(),
                &expect_preflight_errors,
            )?,
            K8sSub::OperationSmoke {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                owner_worker_deploy,
                owner_worker_service,
                owner_worker_id,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                local_owner_worker_metrics_port,
                allow_execution,
                with_soak,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                expected_assignment_state_path,
                soak_iterations,
                soak_sleep_ms,
                out,
            } => run_k8s_operation_smoke(K8sOperationSmokeOptions {
                namespace,
                context,
                orch_service,
                orch_deploy,
                gateway_service,
                gateway_deploy,
                owner_worker_deploy,
                owner_worker_service,
                owner_worker_id,
                argocd_namespace,
                argocd_app,
                skip_argocd_check,
                expected_image,
                local_orch_port,
                local_orch_metrics_port,
                local_gateway_port,
                local_gateway_metrics_port,
                local_owner_worker_metrics_port,
                allow_execution,
                with_soak,
                with_failure,
                allow_scale,
                with_restart,
                allow_rollout_restart,
                expected_assignment_state_path,
                soak_iterations,
                soak_sleep_ms,
                out,
            })?,
            K8sSub::OperationReportCheck {
                report,
                require_published_execution,
                require_completed_observation,
                require_soak,
                require_recovery_required,
                require_restart,
                expected_image,
                expect_preflight_errors,
            } => run_k8s_operation_report_check(
                report.as_deref(),
                InternalP7OperationRequirements {
                    require_published_execution,
                    require_completed_observation,
                    require_soak,
                    require_recovery_required,
                    require_restart,
                },
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
            DevSub::ActivationRestartSmoke => dev_activation_restart_smoke()?,
            DevSub::ActivationPlanSmoke => dev_activation_plan_smoke()?,
            DevSub::ActivationLivePlanSmoke => dev_activation_live_plan_smoke()?,
            DevSub::ActivationLiveMetricsSmoke => dev_activation_live_metrics_smoke()?,
            DevSub::ActivationLivePlannerMutationSmoke => {
                dev_activation_live_planner_mutation_smoke()?
            }
            DevSub::P7OperationLoopSmoke => dev_p7_operation_loop_smoke()?,
            DevSub::P7OperationExecutionSmoke => dev_p7_operation_execution_smoke()?,
            DevSub::P7OperationObservationSmoke => dev_p7_operation_observation_smoke()?,
            DevSub::P7OperationRecoverySmoke => dev_p7_operation_recovery_smoke()?,
            DevSub::P7OperationRestartSmoke => dev_p7_operation_restart_smoke()?,
            DevSub::P7OperationSoakSmoke {
                iterations,
                sleep_ms,
            } => dev_p7_operation_soak_smoke(iterations, sleep_ms)?,
            DevSub::MultiDepthActivationSmoke => dev_multi_depth_activation_smoke()?,
            DevSub::MultiDepthActivationFailureSmoke => dev_multi_depth_activation_failure_smoke()?,
            DevSub::MultiDepthActivationRestartSmoke => dev_multi_depth_activation_restart_smoke()?,
            DevSub::MultiDepthActivationSoak {
                iterations,
                sleep_ms,
            } => dev_multi_depth_activation_soak(iterations, sleep_ms)?,
            DevSub::MultiDepthActivationReportCheck { report } => {
                dev_multi_depth_activation_report_check(report.as_deref())?
            }
            DevSub::MultiDepthActivationFailureReportCheck { report } => {
                dev_multi_depth_activation_failure_report_check(report.as_deref())?
            }
            DevSub::MultiDepthActivationRestartReportCheck { report } => {
                dev_multi_depth_activation_restart_report_check(report.as_deref())?
            }
            DevSub::MultiDepthActivationSoakReportCheck {
                report,
                min_iterations,
            } => dev_multi_depth_activation_soak_report_check(report.as_deref(), min_iterations)?,
            DevSub::MergePlanSmoke => dev_merge_plan_smoke()?,
            DevSub::PlannerMutationSmoke => dev_planner_mutation_smoke()?,
            DevSub::MergeActivationSmoke => dev_merge_activation_smoke()?,
            DevSub::CanonicalMergeActivationSmoke => dev_canonical_merge_activation_smoke()?,
            DevSub::CanonicalMergeActivationReportCheck { report } => {
                dev_canonical_merge_activation_report_check(report.as_deref())?
            }
            DevSub::CanonicalMergeActivationRestartSmoke => {
                dev_canonical_merge_activation_restart_smoke()?
            }
            DevSub::CanonicalMergeActivationRestartReportCheck { report } => {
                dev_canonical_merge_activation_restart_report_check(report.as_deref())?
            }
            DevSub::CanonicalMergeActivationFailureSmoke => {
                dev_canonical_merge_activation_failure_smoke()?
            }
            DevSub::CanonicalMergeActivationFailureReportCheck { report } => {
                dev_canonical_merge_activation_failure_report_check(report.as_deref())?
            }
            DevSub::CanonicalMergeActivationSoak {
                iterations,
                sleep_ms,
            } => dev_canonical_merge_activation_soak(iterations, sleep_ms)?,
            DevSub::CanonicalMergeActivationSoakReportCheck {
                report,
                min_iterations,
            } => {
                dev_canonical_merge_activation_soak_report_check(report.as_deref(), min_iterations)?
            }
            DevSub::MergeActivationCrossWorkerSmoke => dev_merge_activation_cross_worker_smoke()?,
            DevSub::MergeActivationRestartSmoke => dev_merge_activation_restart_smoke()?,
            DevSub::MergeActivationFailureSmoke => dev_merge_activation_failure_smoke()?,
            DevSub::MergeActivationSoak {
                iterations,
                sleep_ms,
            } => dev_merge_activation_soak(iterations, sleep_ms)?,
            DevSub::MergeActivationSoakReportCheck {
                report,
                min_iterations,
            } => dev_merge_activation_soak_report_check(report.as_deref(), min_iterations)?,
            DevSub::ActivationSoak {
                iterations,
                sleep_ms,
            } => dev_activation_soak(iterations, sleep_ms)?,
            DevSub::ActivationReportCheck {
                plan_report,
                activation_report,
                failure_report,
                soak_report,
                restart_report,
                merge_plan_report,
                merge_activation_report,
                merge_cross_worker_report,
                merge_failure_report,
                merge_restart_report,
                merge_soak_report,
                planner_mutation_report,
                require_live_metrics_plan,
                require_planner_live_metrics,
                min_soak_iterations,
            } => run_dev_activation_report_check(DevActivationReportCheckOptions {
                plan_report: plan_report.as_deref(),
                activation_report: activation_report.as_deref(),
                failure_report: failure_report.as_deref(),
                soak_report: soak_report.as_deref(),
                restart_report: restart_report.as_deref(),
                merge_plan_report: merge_plan_report.as_deref(),
                merge_activation_report: merge_activation_report.as_deref(),
                merge_cross_worker_report: merge_cross_worker_report.as_deref(),
                merge_failure_report: merge_failure_report.as_deref(),
                merge_restart_report: merge_restart_report.as_deref(),
                merge_soak_report: merge_soak_report.as_deref(),
                planner_mutation_report: planner_mutation_report.as_deref(),
                require_live_metrics_plan,
                require_planner_live_metrics,
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
                "Last verified: 2026-05-03",
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
    LiveMetricsPlan,
    LiveMetricsActivation,
    LivePlannerMutation,
    MergePlan,
    PlannerMutation,
    MergeActivation,
    CanonicalMergeActivation,
    CanonicalMergeRestart,
    CanonicalMergePostPublishFailure,
    CanonicalMergeSoak { iterations: u32, sleep_ms: u64 },
    MergeCrossWorker,
    MergeRestart,
    MergePostPublishFailure,
    MergeSoak { iterations: u32, sleep_ms: u64 },
    Success,
    PostPublishFailure,
    RestartRecovery,
    Soak { iterations: u32, sleep_ms: u64 },
}

fn is_canonical_merge_mode(mode: ActivationSmokeMode) -> bool {
    matches!(
        mode,
        ActivationSmokeMode::CanonicalMergeActivation
            | ActivationSmokeMode::CanonicalMergeRestart
            | ActivationSmokeMode::CanonicalMergePostPublishFailure
            | ActivationSmokeMode::CanonicalMergeSoak { .. }
    )
}

fn is_merge_restart_mode(mode: ActivationSmokeMode) -> bool {
    matches!(
        mode,
        ActivationSmokeMode::MergeRestart | ActivationSmokeMode::CanonicalMergeRestart
    )
}

fn is_merge_failure_mode(mode: ActivationSmokeMode) -> bool {
    matches!(
        mode,
        ActivationSmokeMode::MergePostPublishFailure
            | ActivationSmokeMode::CanonicalMergePostPublishFailure
    )
}

fn dev_activation_plan_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::Plan)
}

fn dev_activation_live_plan_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::LiveMetricsPlan)
}

fn dev_activation_live_metrics_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::LiveMetricsActivation)
}

fn dev_activation_live_planner_mutation_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::LivePlannerMutation)
}

fn dev_p7_operation_loop_smoke() -> Result<()> {
    let orch_addr = "127.0.0.1:6310";
    let orch_metrics_addr = "127.0.0.1:6311";
    let orch_endpoint = format!("http://{orch_addr}");
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    let report_dir = root.join(".dev/reports");
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;
    fs::create_dir_all(&report_dir)?;

    let ledger_path = report_dir.join("p7-operation-loop-ledger-latest.json");
    let _ = fs::remove_file(&ledger_path);
    let ledger_path_raw = ledger_path.to_string_lossy().into_owned();

    let mut build = Command::new("cargo");
    build.args(["build", "--bin", "tessera-orch"]);
    run(&mut build)?;

    let orchestrator_bin = root.join("target/debug/tessera-orch");
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());

    let cases = p7_operation_loop_smoke_cases();
    let mut case_reports = Vec::with_capacity(cases.len());
    for case in &cases {
        let case_report = run_p7_operation_loop_smoke_case(
            &root,
            &logs,
            &pids,
            &orchestrator_bin,
            rust_log.as_str(),
            orch_addr,
            orch_metrics_addr,
            orch_endpoint.as_str(),
            &ledger_path,
            ledger_path_raw.as_str(),
            case,
        )?;
        case_reports.push(case_report);
    }

    let ledger = read_json_report(&ledger_path)?;
    let ledger_summary = validate_p7_operation_ledger(&ledger, true, true, false, false, false)?;
    if ledger_summary.records < cases.len()
        || ledger_summary.approval_records < cases.len()
        || ledger_summary.blocked_execution_records < cases.len()
    {
        bail!(
            "P7 operation loop smoke expected at least {} complete operation records; got records={}, approvals={}, blocked={}",
            cases.len(),
            ledger_summary.records,
            ledger_summary.approval_records,
            ledger_summary.blocked_execution_records
        );
    }

    let report = serde_json::json!({
        "schema": "tessera.p7_operation_loop_smoke.v1",
        "unix_secs": unix_timestamp_secs(),
        "ledger": {
            "path": ledger_path_raw.as_str(),
            "records": ledger_summary.records,
            "proposal_records": ledger_summary.proposal_records,
            "approval_records": ledger_summary.approval_records,
            "blocked_execution_records": ledger_summary.blocked_execution_records
        },
        "cases": case_reports,
        "checks": {
            "split_operation_loop_smoke": true,
            "merge_operation_loop_smoke": true,
            "multi_depth_operation_loop_smoke": true,
            "proposal_approval_default_off_execution": true,
            "ledger_check_passed": true
        },
        "remaining_uncovered": [
            "approved_runtime_execution",
            "operation_observation",
            "failure_recovery",
            "restart_recovery",
            "soak",
            "guarded_kubernetes_operation_loop_smoke",
            "p7_completion_audit"
        ]
    });
    validate_p7_operation_loop_smoke_report(&report)?;
    let report_path = write_p7_operation_loop_smoke_report(&report)?;

    println!(
        "P7 operation loop smoke: split/merge/multi-depth proposal -> approval -> default-off execution blocks succeeded, report={}, ledger={}",
        report_path.display(),
        ledger_path.display()
    );
    Ok(())
}

struct P7OperationLoopSmokeCase {
    name: &'static str,
    expected_kind: &'static str,
    orch_config_json: String,
    preview_json: &'static str,
    registrations: Vec<(&'static str, &'static str)>,
}

fn p7_operation_loop_smoke_cases() -> Vec<P7OperationLoopSmokeCase> {
    let worker_a_addr = "127.0.0.1:5311";
    let worker_b_addr = "127.0.0.1:5312";
    let split_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0},"actor_count":140,"move_queue_pressure":70,"high_pressure_windows":3,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":1},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":2},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":3},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let multi_depth_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":-2,"cy":3,"depth":2,"sub":0},"actor_count":140,"move_queue_pressure":70,"high_pressure_windows":3,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let registrations = vec![("worker-a", worker_a_addr), ("worker-b", worker_b_addr)];
    vec![
        P7OperationLoopSmokeCase {
            name: "split",
            expected_kind: "split",
            orch_config_json: format!(
                r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
            ),
            preview_json: split_preview_json,
            registrations: registrations.clone(),
        },
        P7OperationLoopSmokeCase {
            name: "merge",
            expected_kind: "merge",
            orch_config_json: format!(
                r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
            ),
            preview_json: merge_preview_json,
            registrations: registrations.clone(),
        },
        P7OperationLoopSmokeCase {
            name: "multi_depth_split",
            expected_kind: "multi_depth_split",
            orch_config_json: format!(
                r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":-2,"cy":3,"depth":2,"sub":0}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
            ),
            preview_json: multi_depth_preview_json,
            registrations,
        },
    ]
}

#[allow(clippy::too_many_arguments)]
fn run_p7_operation_loop_smoke_case(
    root: &Path,
    logs: &Path,
    pids: &Path,
    orchestrator_bin: &Path,
    rust_log: &str,
    orch_addr: &str,
    orch_metrics_addr: &str,
    orch_endpoint: &str,
    ledger_path: &Path,
    ledger_path_raw: &str,
    case: &P7OperationLoopSmokeCase,
) -> Result<serde_json::Value> {
    let orch_envs = [
        ("RUST_LOG", rust_log),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", case.orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", case.preview_json),
        ("TESSERA_ORCH_OPERATION_LEDGER_PATH", ledger_path_raw),
    ];

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        root,
        logs,
        pids,
        DevProcessSpec {
            name: "p7-operation-loop-orch",
            bin: orchestrator_bin,
            ready_addr: orch_addr,
            envs: &orch_envs,
        },
    )?;

    let runtime = tokio::runtime::Runtime::new()?;
    for (worker_id, addr) in &case.registrations {
        runtime.block_on(register_orchestrator_worker(orch_endpoint, worker_id, addr))?;
    }
    runtime.block_on(wait_for_orchestrator_registered(
        orch_endpoint,
        case.registrations.len() as u64,
    ))?;
    let (before_health, before_listing) =
        runtime.block_on(fetch_orch_health_and_listing(orch_endpoint))?;

    let proposal_response = http_json_post(
        "P7 operation proposal",
        orch_metrics_addr,
        "/operations/proposals",
    )?;
    assert_json_bool_eq(&proposal_response, &["assignments_changed"], false)?;
    if json_u64(&proposal_response, &["planned_count"])? == 0 {
        bail!("P7 operation loop smoke expected at least one planner proposal");
    }
    let recorded_count = json_u64(&proposal_response, &["recorded_count"])?;
    let already_recorded_count = json_u64(&proposal_response, &["already_recorded_count"])?;
    if recorded_count + already_recorded_count == 0 {
        bail!("P7 operation loop smoke did not record or reuse a proposal");
    }
    let operation_id = json_array(&proposal_response, &["operation_ids"])?
        .first()
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("P7 operation proposal response has no operation id"))?
        .to_string();

    let proposal_snapshot = http_json_get("P7 operation ledger", orch_metrics_addr, "/operations")?;
    let proposal_record = find_p7_operation_record(&proposal_snapshot, &operation_id)?;
    let operation_kind = json_str(proposal_record, &["kind"])?.to_string();
    if operation_kind != case.expected_kind {
        bail!(
            "P7 operation loop smoke case {} expected kind {}, got {}",
            case.name,
            case.expected_kind,
            operation_kind
        );
    }
    let proposal_hash = json_str(proposal_record, &["proposal", "proposal_hash"])?.to_string();

    let policy_id = "operator_approved_dynamic_operation_v1";
    let approval_response = http_json_post(
        "P7 operation approval",
        orch_metrics_addr,
        &format!(
            "/operations/approvals?operation_id={operation_id}&policy_id={policy_id}&approver=p7-loop-smoke&expected_proposal_hash={proposal_hash}&ttl_secs=600&cooldown_key=p7-loop-smoke&budget_key=p7-loop-smoke"
        ),
    )?;
    assert_json_str_eq(&approval_response, &["status"], "approved")?;
    assert_json_bool_eq(&approval_response, &["assignments_changed"], false)?;

    let execution_response = http_json_post(
        "P7 operation execution",
        orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&policy_id={policy_id}"
        ),
    )?;
    let execution_status = json_str(&execution_response, &["status"])?;
    if !matches!(
        execution_status,
        "blocked_by_policy" | "already_blocked_by_policy"
    ) {
        bail!("P7 operation execution expected blocked_by_policy status, got {execution_status}");
    }
    assert_json_bool_eq(&execution_response, &["assignments_changed"], false)?;
    assert_json_bool_eq(&execution_response, &["mutation_attempted"], false)?;
    assert_json_bool_eq(&execution_response, &["mutation_allowed"], false)?;

    let (_after_health, after_listing) =
        runtime.block_on(fetch_orch_health_and_listing(orch_endpoint))?;
    let assignments_unchanged = before_listing == after_listing;
    if !assignments_unchanged {
        bail!(
            "P7 operation loop smoke case {} changed Orchestrator assignments",
            case.name
        );
    }

    let ledger = read_json_report(ledger_path)?;
    let final_record = find_p7_operation_record(&ledger, &operation_id)?;
    validate_p7_operation_approval(final_record)?;
    validate_p7_blocked_execution(final_record)?;

    Ok(serde_json::json!({
        "case": case.name,
        "operation": {
            "operation_id": operation_id.as_str(),
            "kind": operation_kind.as_str(),
            "proposal_hash": proposal_hash.as_str(),
            "policy_id": policy_id
        },
        "orchestrator": {
            "grpc_addr": orch_addr,
            "metrics_addr": orch_metrics_addr,
            "registered_workers": before_health.registered_workers,
            "assignment_listing_before": assignment_listing_summary_json(&before_listing)?,
            "assignment_listing_after": assignment_listing_summary_json(&after_listing)?
        },
        "responses": {
            "proposal": proposal_response,
            "approval": approval_response,
            "execution": execution_response
        },
        "checks": {
            "proposal_recorded": true,
            "approval_recorded": true,
            "execution_blocked_by_policy": true,
            "assignments_unchanged": assignments_unchanged,
            "mutation_attempted": false,
            "mutation_allowed": false,
            "ledger_check_passed": true
        }
    }))
}

fn dev_p7_operation_execution_smoke() -> Result<()> {
    let orch_addr = "127.0.0.1:6320";
    let orch_metrics_addr = "127.0.0.1:6321";
    let orch_endpoint = format!("http://{orch_addr}");
    let worker_a_addr = "127.0.0.1:5321";
    let worker_b_addr = "127.0.0.1:5322";
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    let report_dir = root.join(".dev/reports");
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;
    fs::create_dir_all(&report_dir)?;

    let ledger_path = report_dir.join("p7-operation-execution-ledger-latest.json");
    let _ = fs::remove_file(&ledger_path);
    let ledger_path_raw = ledger_path.to_string_lossy().into_owned();

    let mut build = Command::new("cargo");
    build.args(["build", "--bin", "tessera-orch"]);
    run(&mut build)?;

    let orchestrator_bin = root.join("target/debug/tessera-orch");
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
    let orch_config_json = format!(
        r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
    );
    let merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":1},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":2},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":3},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let orch_envs = [
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", merge_preview_json),
        (
            "TESSERA_ORCH_OPERATION_LEDGER_PATH",
            ledger_path_raw.as_str(),
        ),
        ("TESSERA_ORCH_OPERATION_EXECUTION", "manual"),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-execution-orch",
            bin: &orchestrator_bin,
            ready_addr: orch_addr,
            envs: &orch_envs,
        },
    )?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(register_orchestrator_worker(
        &orch_endpoint,
        "worker-a",
        worker_a_addr,
    ))?;
    runtime.block_on(register_orchestrator_worker(
        &orch_endpoint,
        "worker-b",
        worker_b_addr,
    ))?;
    runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
    let (before_health, before_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let proposal_response = http_json_post(
        "P7 operation proposal",
        orch_metrics_addr,
        "/operations/proposals",
    )?;
    let operation_id = json_array(&proposal_response, &["operation_ids"])?
        .first()
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("P7 operation proposal response has no operation id"))?
        .to_string();
    let proposal_snapshot = http_json_get("P7 operation ledger", orch_metrics_addr, "/operations")?;
    let proposal_record = find_p7_operation_record(&proposal_snapshot, &operation_id)?;
    assert_json_str_eq(proposal_record, &["kind"], "merge")?;
    let proposal_hash = json_str(proposal_record, &["proposal", "proposal_hash"])?.to_string();

    let policy_id = "operator_approved_dynamic_operation_v1";
    let approval_response = http_json_post(
        "P7 operation approval",
        orch_metrics_addr,
        &format!(
            "/operations/approvals?operation_id={operation_id}&policy_id={policy_id}&approver=p7-execution-smoke&expected_proposal_hash={proposal_hash}&ttl_secs=600&cooldown_key=p7-execution-smoke&budget_key=p7-execution-smoke"
        ),
    )?;
    assert_json_str_eq(&approval_response, &["status"], "approved")?;

    let execution_response = http_json_post(
        "P7 operation execution",
        orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&policy_id={policy_id}"
        ),
    )?;
    assert_json_str_eq(&execution_response, &["status"], "published")?;
    assert_json_bool_eq(&execution_response, &["assignments_changed"], true)?;
    assert_json_bool_eq(&execution_response, &["mutation_attempted"], true)?;
    assert_json_bool_eq(&execution_response, &["mutation_allowed"], true)?;

    let repeat_execution_response = http_json_post(
        "P7 operation execution repeat",
        orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&policy_id={policy_id}"
        ),
    )?;
    assert_json_str_eq(&repeat_execution_response, &["status"], "already_published")?;
    assert_json_bool_eq(&repeat_execution_response, &["assignments_changed"], false)?;
    assert_json_bool_eq(&repeat_execution_response, &["mutation_attempted"], false)?;

    let (_after_health, after_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;
    let before_parent_owners = listing_cell_owners(&before_listing, CellId::grid(0, 0, 0))?;
    let after_parent_owners = listing_cell_owners(&after_listing, CellId::grid(0, 0, 0))?;
    if !before_parent_owners.is_empty() || after_parent_owners != vec!["worker-a".to_string()] {
        bail!(
            "P7 operation execution smoke expected parent ownership to publish to worker-a; before={before_parent_owners:?} after={after_parent_owners:?}"
        );
    }

    let ledger = read_json_report(&ledger_path)?;
    let ledger_summary = validate_p7_operation_ledger(&ledger, true, false, true, false, false)?;
    let record = find_p7_operation_record(&ledger, &operation_id)?;
    validate_p7_published_execution(record)?;
    let report = serde_json::json!({
        "schema": "tessera.p7_operation_execution_smoke.v1",
        "unix_secs": unix_timestamp_secs(),
        "operation": {
            "operation_id": operation_id.as_str(),
            "kind": "merge",
            "proposal_hash": proposal_hash.as_str(),
            "policy_id": policy_id
        },
        "orchestrator": {
            "grpc_addr": orch_addr,
            "metrics_addr": orch_metrics_addr,
            "registered_workers": before_health.registered_workers,
            "assignment_listing_before": assignment_listing_summary_json(&before_listing)?,
            "assignment_listing_after": assignment_listing_summary_json(&after_listing)?
        },
        "ledger": {
            "path": ledger_path_raw.as_str(),
            "records": ledger_summary.records,
            "proposal_records": ledger_summary.proposal_records,
            "approval_records": ledger_summary.approval_records,
            "published_execution_records": ledger_summary.published_execution_records
        },
        "responses": {
            "proposal": proposal_response,
            "approval": approval_response,
            "execution": execution_response,
            "repeat_execution": repeat_execution_response
        },
        "checks": {
            "merge_execution_published": true,
            "parent_route_published": true,
            "repeat_execution_idempotent": true,
            "ledger_execution_published": true
        },
        "remaining_uncovered": [
            "split_runtime_execution",
            "multi_depth_runtime_execution",
            "operation_observation",
            "failure_recovery",
            "restart_recovery",
            "soak",
            "guarded_kubernetes_operation_execution_smoke",
            "p7_completion_audit"
        ]
    });
    validate_p7_operation_execution_smoke_report(&report)?;
    let report_path = write_p7_operation_execution_smoke_report(&report)?;

    println!(
        "P7 operation execution smoke: approved merge operation published once and repeat execution was idempotent, report={}, ledger={}",
        report_path.display(),
        ledger_path.display()
    );
    Ok(())
}

fn dev_p7_operation_observation_smoke() -> Result<()> {
    let gateway_addr = "127.0.0.1:4330";
    let gateway_metrics_addr = "127.0.0.1:4331";
    let worker_a_addr = "127.0.0.1:5331";
    let worker_b_addr = "127.0.0.1:5332";
    let worker_a_metrics_addr = "127.0.0.1:5333";
    let worker_b_metrics_addr = "127.0.0.1:5334";
    let orch_addr = "127.0.0.1:6330";
    let orch_metrics_addr = "127.0.0.1:6331";
    let orch_endpoint = format!("http://{orch_addr}");
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    let report_dir = root.join(".dev/reports");
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;
    fs::create_dir_all(&report_dir)?;

    let ledger_path = report_dir.join("p7-operation-observation-ledger-latest.json");
    let _ = fs::remove_file(&ledger_path);
    let ledger_path_raw = ledger_path.to_string_lossy().into_owned();

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
        r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
    );
    let merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":1},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":2},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":3},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let orch_envs = [
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", merge_preview_json),
        (
            "TESSERA_ORCH_OPERATION_LEDGER_PATH",
            ledger_path_raw.as_str(),
        ),
        ("TESSERA_ORCH_OPERATION_EXECUTION", "manual"),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-observation-orch",
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
            name: "p7-operation-observation-worker-a",
            bin: &worker_bin,
            ready_addr: worker_a_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-a"),
                ("TESSERA_WORKER_ADDR", worker_a_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_a_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_a_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
                ("TESSERA_ORCH_ADDR", orch_addr),
            ],
        },
    )?;
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-observation-worker-b",
            bin: &worker_bin,
            ready_addr: worker_b_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-b"),
                ("TESSERA_WORKER_ADDR", worker_b_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
            name: "p7-operation-observation-gateway",
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
        "P7 operation observation gateway readiness",
        gateway_metrics_addr,
        "/ready",
        "200 OK",
    )?;
    assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
    let gateway_metrics_before = assert_metrics_endpoint_body_until(
        "P7 operation observation gateway before",
        gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_close_before = gateway_close_counters_from_metrics(&gateway_metrics_before)?;
    let gateway_routes_before =
        prometheus_sample_value(&gateway_metrics_before, "tessera_gateway_routes")?;
    let (before_health, before_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let parent = CellId::grid(0, 0, 0);
    let child0 = activation_child_cell(0);
    let child3 = activation_child_cell(3);
    let actor_a = EntityId(7_701);
    let actor_b = EntityId(7_702);
    let mut child0_session = open_gateway_join_until_snapshot(
        gateway_addr,
        child0,
        actor_a,
        Position { x: 4.0, y: 4.0 },
    )?;
    let _child3_session = open_gateway_join_until_snapshot(
        gateway_addr,
        child3,
        actor_b,
        Position { x: 24.0, y: 24.0 },
    )?;

    let proposal_response = http_json_post(
        "P7 operation proposal",
        orch_metrics_addr,
        "/operations/proposals",
    )?;
    let operation_id = json_array(&proposal_response, &["operation_ids"])?
        .first()
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("P7 operation proposal response has no operation id"))?
        .to_string();
    let proposal_snapshot = http_json_get("P7 operation ledger", orch_metrics_addr, "/operations")?;
    let proposal_record = find_p7_operation_record(&proposal_snapshot, &operation_id)?;
    assert_json_str_eq(proposal_record, &["kind"], "merge")?;
    let proposal_hash = json_str(proposal_record, &["proposal", "proposal_hash"])?.to_string();

    let policy_id = "operator_approved_dynamic_operation_v1";
    let approval_response = http_json_post(
        "P7 operation approval",
        orch_metrics_addr,
        &format!(
            "/operations/approvals?operation_id={operation_id}&policy_id={policy_id}&approver=p7-observation-smoke&expected_proposal_hash={proposal_hash}&ttl_secs=600&cooldown_key=p7-observation-smoke&budget_key=p7-observation-smoke"
        ),
    )?;
    assert_json_str_eq(&approval_response, &["status"], "approved")?;

    let execution_response = http_json_post(
        "P7 operation execution",
        orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&policy_id={policy_id}"
        ),
    )?;
    assert_json_str_eq(&execution_response, &["status"], "published")?;
    assert_json_bool_eq(&execution_response, &["assignments_changed"], true)?;
    assert_json_bool_eq(&execution_response, &["mutation_attempted"], true)?;
    assert_json_bool_eq(&execution_response, &["mutation_allowed"], true)?;

    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(gateway_addr, parent, 7_730)?;
    let move_observed = request_move_until_delta(
        &mut child0_session,
        parent,
        actor_a,
        1.0,
        1.0,
        "P7 operation observation parent move",
    )?;
    let worker_parent_metric = worker_cell_actor_count_metric(parent);
    let worker_metrics = assert_metrics_endpoint_body_until(
        "P7 operation observation worker-a",
        worker_a_metrics_addr,
        &["tessera_worker_cell_actor_count"],
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation observation worker-a",
        &worker_metrics,
        worker_parent_metric.as_str(),
        2.0,
    )?;
    let worker_parent_actor_count =
        prometheus_sample_value(&worker_metrics, worker_parent_metric.as_str())?;

    let gateway_metrics_after = assert_metrics_endpoint_body_until(
        "P7 operation observation gateway after",
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
    let gateway_routes_after =
        prometheus_sample_value(&gateway_metrics_after, "tessera_gateway_routes")?;
    assert_prometheus_sample_at_least(
        "P7 operation observation gateway",
        &gateway_metrics_after,
        "tessera_gateway_ping_roundtrip_seconds_count",
        1.0,
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation observation gateway",
        &gateway_metrics_after,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
        2.0,
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation observation gateway",
        &gateway_metrics_after,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        1.0,
    )?;
    let gateway_close_after = gateway_close_counters_from_metrics(&gateway_metrics_after)?;
    assert_gateway_close_counters_not_increased(
        "P7 operation observation gateway",
        gateway_close_before,
        gateway_close_after,
    )?;
    let (_after_health, after_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let route_converged = (gateway_routes_after - 1.0).abs() < f64::EPSILON;
    let worker_refreshed = worker_parent_actor_count >= 2.0;
    let traffic_confirmed = prometheus_sample_value(
        &gateway_metrics_after,
        "tessera_gateway_ping_roundtrip_seconds_count",
    )? >= 1.0
        && prometheus_sample_value(
            &gateway_metrics_after,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        )? >= 1.0;
    let counters_clean = gateway_close_before == gateway_close_after;
    if !(route_converged && worker_refreshed && traffic_confirmed && counters_clean) {
        bail!(
            "P7 operation observation evidence incomplete: route_converged={route_converged} worker_refreshed={worker_refreshed} traffic_confirmed={traffic_confirmed} counters_clean={counters_clean}"
        );
    }

    let observation_response = http_json_post(
        "P7 operation observation",
        orch_metrics_addr,
        &format!(
            "/operations/observations?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&observer=p7-observation-smoke&route_converged=true&worker_refreshed=true&traffic_confirmed=true&counters_clean=true"
        ),
    )?;
    assert_json_str_eq(&observation_response, &["status"], "completed")?;
    assert_json_bool_eq(&observation_response, &["observation_accepted"], true)?;
    assert_json_bool_eq(&observation_response, &["assignments_changed"], false)?;

    let ledger = read_json_report(&ledger_path)?;
    let ledger_summary = validate_p7_operation_ledger(&ledger, true, false, true, true, false)?;
    let record = find_p7_operation_record(&ledger, &operation_id)?;
    validate_p7_completed_observation(record)?;
    let report = serde_json::json!({
        "schema": "tessera.p7_operation_observation_smoke.v1",
        "unix_secs": unix_timestamp_secs(),
        "operation": {
            "operation_id": operation_id.as_str(),
            "kind": "merge",
            "proposal_hash": proposal_hash.as_str(),
            "policy_id": policy_id
        },
        "orchestrator": {
            "grpc_addr": orch_addr,
            "metrics_addr": orch_metrics_addr,
            "registered_workers": before_health.registered_workers,
            "assignment_listing_before": assignment_listing_summary_json(&before_listing)?,
            "assignment_listing_after": assignment_listing_summary_json(&after_listing)?
        },
        "gateway": {
            "addr": gateway_addr,
            "metrics_addr": gateway_metrics_addr,
            "routes_before": gateway_routes_before,
            "routes_after": gateway_routes_after,
            "ping_roundtrips": prometheus_sample_value(&gateway_metrics_after, "tessera_gateway_ping_roundtrip_seconds_count")?,
            "join_roundtrips": prometheus_sample_value(&gateway_metrics_after, "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}")?,
            "move_roundtrips": prometheus_sample_value(&gateway_metrics_after, "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}")?,
            "close_counters": {
                "before": gateway_close_counters_json(gateway_close_before),
                "after": gateway_close_counters_json(gateway_close_after)
            }
        },
        "worker": {
            "worker_a_addr": worker_a_addr,
            "worker_a_metrics_addr": worker_a_metrics_addr,
            "worker_b_addr": worker_b_addr,
            "worker_b_metrics_addr": worker_b_metrics_addr,
            "parent_actor_count": worker_parent_actor_count
        },
        "ledger": {
            "path": ledger_path_raw.as_str(),
            "records": ledger_summary.records,
            "proposal_records": ledger_summary.proposal_records,
            "approval_records": ledger_summary.approval_records,
            "published_execution_records": ledger_summary.published_execution_records,
            "completed_observation_records": ledger_summary.completed_observation_records
        },
        "responses": {
            "proposal": proposal_response,
            "approval": approval_response,
            "execution": execution_response,
            "observation": observation_response
        },
        "checks": {
            "merge_execution_published": true,
            "route_converged": route_converged,
            "worker_refreshed": worker_refreshed,
            "traffic_confirmed": traffic_confirmed,
            "gateway_close_counters_clean": counters_clean,
            "stable_session_parent_move": true,
            "observation_completed": true,
            "ledger_observation_completed": true
        },
        "frames": {
            "ignored_before_parent_delta": move_observed.ignored_frames,
            "remote_delta_before_parent_delta": move_observed.remote_delta_frames,
            "remote_snapshot_before_parent_delta": move_observed.remote_snapshot_frames
        },
        "remaining_uncovered": [
            "split_runtime_execution",
            "multi_depth_runtime_execution",
            "failure_recovery",
            "restart_recovery",
            "soak",
            "guarded_kubernetes_operation_observation_smoke",
            "p7_completion_audit"
        ]
    });
    validate_p7_operation_observation_smoke_report(&report)?;
    let report_path = write_p7_operation_observation_smoke_report(&report)?;

    println!(
        "P7 operation observation smoke: approved merge execution converged through Gateway/Worker traffic evidence and completed observation, report={}, ledger={}",
        report_path.display(),
        ledger_path.display()
    );
    Ok(())
}

fn dev_p7_operation_recovery_smoke() -> Result<()> {
    let gateway_addr = "127.0.0.1:4340";
    let gateway_metrics_addr = "127.0.0.1:4341";
    let worker_a_addr = "127.0.0.1:5341";
    let worker_b_addr = "127.0.0.1:5342";
    let worker_a_metrics_addr = "127.0.0.1:5343";
    let worker_b_metrics_addr = "127.0.0.1:5344";
    let orch_addr = "127.0.0.1:6340";
    let orch_metrics_addr = "127.0.0.1:6341";
    let orch_endpoint = format!("http://{orch_addr}");
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    let report_dir = root.join(".dev/reports");
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;
    fs::create_dir_all(&report_dir)?;

    let ledger_path = report_dir.join("p7-operation-recovery-ledger-latest.json");
    let _ = fs::remove_file(&ledger_path);
    let ledger_path_raw = ledger_path.to_string_lossy().into_owned();

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
        r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
    );
    let merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":1},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":2},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":3},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let orch_envs = [
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", merge_preview_json),
        (
            "TESSERA_ORCH_OPERATION_LEDGER_PATH",
            ledger_path_raw.as_str(),
        ),
        ("TESSERA_ORCH_OPERATION_EXECUTION", "manual"),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];
    let worker_a_envs = [
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_WORKER_ID", "worker-a"),
        ("TESSERA_WORKER_ADDR", worker_a_addr),
        ("TESSERA_WORKER_ADVERTISE_ADDR", worker_a_addr),
        ("TESSERA_WORKER_METRICS_ADDR", worker_a_metrics_addr),
        ("TESSERA_WORKER_REFRESH_SECS", "1"),
        ("TESSERA_ORCH_ADDR", orch_addr),
    ];

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-recovery-orch",
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
            name: "p7-operation-recovery-worker-a",
            bin: &worker_bin,
            ready_addr: worker_a_addr,
            envs: &worker_a_envs,
        },
    )?;
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-recovery-worker-b",
            bin: &worker_bin,
            ready_addr: worker_b_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-b"),
                ("TESSERA_WORKER_ADDR", worker_b_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
            name: "p7-operation-recovery-gateway",
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
        "P7 operation recovery gateway readiness",
        gateway_metrics_addr,
        "/ready",
        "200 OK",
    )?;
    assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
    let gateway_metrics_before = assert_metrics_endpoint_body_until(
        "P7 operation recovery gateway before",
        gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_close_before = gateway_close_counters_from_metrics(&gateway_metrics_before)?;
    let (before_health, before_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let parent = CellId::grid(0, 0, 0);
    let child0 = activation_child_cell(0);
    let actor_a = EntityId(7_801);
    let mut child0_session = open_gateway_join_until_snapshot(
        gateway_addr,
        child0,
        actor_a,
        Position { x: 4.0, y: 4.0 },
    )?;

    let proposal_response = http_json_post(
        "P7 operation proposal",
        orch_metrics_addr,
        "/operations/proposals",
    )?;
    let operation_id = json_array(&proposal_response, &["operation_ids"])?
        .first()
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("P7 operation proposal response has no operation id"))?
        .to_string();
    let proposal_snapshot = http_json_get("P7 operation ledger", orch_metrics_addr, "/operations")?;
    let proposal_record = find_p7_operation_record(&proposal_snapshot, &operation_id)?;
    assert_json_str_eq(proposal_record, &["kind"], "merge")?;
    let proposal_hash = json_str(proposal_record, &["proposal", "proposal_hash"])?.to_string();

    let policy_id = "operator_approved_dynamic_operation_v1";
    let approval_response = http_json_post(
        "P7 operation approval",
        orch_metrics_addr,
        &format!(
            "/operations/approvals?operation_id={operation_id}&policy_id={policy_id}&approver=p7-recovery-smoke&expected_proposal_hash={proposal_hash}&ttl_secs=600&cooldown_key=p7-recovery-smoke&budget_key=p7-recovery-smoke"
        ),
    )?;
    assert_json_str_eq(&approval_response, &["status"], "approved")?;
    let execution_response = http_json_post(
        "P7 operation execution",
        orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&policy_id={policy_id}"
        ),
    )?;
    assert_json_str_eq(&execution_response, &["status"], "published")?;
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(gateway_addr, parent, 7_830)?;
    let _move_observed = request_move_until_delta(
        &mut child0_session,
        parent,
        actor_a,
        1.0,
        1.0,
        "P7 operation recovery pre-failure parent move",
    )?;
    drop(child0_session);

    stack.terminate_named("p7-operation-recovery-worker-a")?;
    let failure_error = match assert_gateway_ping_until(gateway_addr, parent, 7_831) {
        Ok(()) => bail!(
            "P7 operation recovery smoke expected parent Ping to fail while owner Worker was down"
        ),
        Err(err) => err.to_string(),
    };
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    let gateway_metrics_after_failure = assert_metrics_endpoint_body_until(
        "P7 operation recovery gateway after failure",
        gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_close_after_failure =
        gateway_close_counters_from_metrics(&gateway_metrics_after_failure)?;

    let observation_response = http_json_post(
        "P7 operation failed observation",
        orch_metrics_addr,
        &format!(
            "/operations/observations?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&observer=p7-recovery-smoke&route_converged=true&worker_refreshed=true&traffic_confirmed=false&counters_clean=false"
        ),
    )?;
    assert_json_str_eq(&observation_response, &["status"], "recovery_required")?;
    assert_json_bool_eq(&observation_response, &["observation_accepted"], false)?;

    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-recovery-worker-a",
            bin: &worker_bin,
            ready_addr: worker_a_addr,
            envs: &worker_a_envs,
        },
    )?;
    runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(gateway_addr, parent, 7_832)?;
    let gateway_metrics_after_recovery = assert_metrics_endpoint_body_until(
        "P7 operation recovery gateway after recovery",
        gateway_metrics_addr,
        &["tessera_gateway_routes"],
    )?;
    let (_after_health, after_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let ledger = read_json_report(&ledger_path)?;
    let ledger_summary = validate_p7_operation_ledger(&ledger, true, false, true, false, true)?;
    let record = find_p7_operation_record(&ledger, &operation_id)?;
    validate_p7_recovery_required(record)?;
    let report = serde_json::json!({
        "schema": "tessera.p7_operation_recovery_smoke.v1",
        "unix_secs": unix_timestamp_secs(),
        "operation": {
            "operation_id": operation_id.as_str(),
            "kind": "merge",
            "proposal_hash": proposal_hash.as_str(),
            "policy_id": policy_id
        },
        "orchestrator": {
            "grpc_addr": orch_addr,
            "metrics_addr": orch_metrics_addr,
            "registered_workers": before_health.registered_workers,
            "assignment_listing_before": assignment_listing_summary_json(&before_listing)?,
            "assignment_listing_after": assignment_listing_summary_json(&after_listing)?
        },
        "gateway": {
            "addr": gateway_addr,
            "metrics_addr": gateway_metrics_addr,
            "close_counters": {
                "before": gateway_close_counters_json(gateway_close_before),
                "after_failure": gateway_close_counters_json(gateway_close_after_failure)
            },
            "routes_after_recovery": prometheus_sample_value(&gateway_metrics_after_recovery, "tessera_gateway_routes")?
        },
        "ledger": {
            "path": ledger_path_raw.as_str(),
            "records": ledger_summary.records,
            "proposal_records": ledger_summary.proposal_records,
            "approval_records": ledger_summary.approval_records,
            "published_execution_records": ledger_summary.published_execution_records,
            "recovery_required_records": ledger_summary.recovery_required_records
        },
        "responses": {
            "proposal": proposal_response,
            "approval": approval_response,
            "execution": execution_response,
            "observation": observation_response
        },
        "failure": {
            "owner_worker_id": "worker-a",
            "error": failure_error
        },
        "checks": {
            "merge_execution_published": true,
            "owner_outage_detected": true,
            "observation_recovery_required": true,
            "no_automatic_rollback": true,
            "operator_recovery_confirmed": true,
            "ledger_recovery_required": true
        },
        "remaining_uncovered": [
            "split_runtime_execution",
            "multi_depth_runtime_execution",
            "restart_recovery",
            "soak",
            "guarded_kubernetes_operation_recovery_smoke",
            "p7_completion_audit"
        ]
    });
    validate_p7_operation_recovery_smoke_report(&report)?;
    let report_path = write_p7_operation_recovery_smoke_report(&report)?;

    println!(
        "P7 operation recovery smoke: owner outage marked operation recovery_required and operator Worker restart restored parent traffic, report={}, ledger={}",
        report_path.display(),
        ledger_path.display()
    );
    Ok(())
}

fn dev_p7_operation_restart_smoke() -> Result<()> {
    let gateway_addr = "127.0.0.1:4350";
    let gateway_metrics_addr = "127.0.0.1:4351";
    let worker_a_addr = "127.0.0.1:5351";
    let worker_b_addr = "127.0.0.1:5352";
    let worker_a_metrics_addr = "127.0.0.1:5353";
    let worker_b_metrics_addr = "127.0.0.1:5354";
    let orch_addr = "127.0.0.1:6350";
    let orch_metrics_addr = "127.0.0.1:6351";
    let orch_endpoint = format!("http://{orch_addr}");
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    let report_dir = root.join(".dev/reports");
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;
    fs::create_dir_all(&report_dir)?;

    let ledger_path = report_dir.join("p7-operation-restart-ledger-latest.json");
    let assignment_state_path =
        report_dir.join("p7-operation-restart-assignment-state-latest.json");
    let _ = fs::remove_file(&ledger_path);
    let _ = fs::remove_file(&assignment_state_path);
    let ledger_path_raw = ledger_path.to_string_lossy().into_owned();
    let assignment_state_path_raw = assignment_state_path.to_string_lossy().into_owned();

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
        r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
    );
    let merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":1},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":2},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":3},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let orch_envs = [
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", merge_preview_json),
        (
            "TESSERA_ORCH_OPERATION_LEDGER_PATH",
            ledger_path_raw.as_str(),
        ),
        (
            "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
            assignment_state_path_raw.as_str(),
        ),
        ("TESSERA_ORCH_OPERATION_EXECUTION", "manual"),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-restart-orch",
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
            name: "p7-operation-restart-worker-a",
            bin: &worker_bin,
            ready_addr: worker_a_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-a"),
                ("TESSERA_WORKER_ADDR", worker_a_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_a_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_a_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
                ("TESSERA_ORCH_ADDR", orch_addr),
            ],
        },
    )?;
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-restart-worker-b",
            bin: &worker_bin,
            ready_addr: worker_b_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-b"),
                ("TESSERA_WORKER_ADDR", worker_b_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
            name: "p7-operation-restart-gateway",
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
        "P7 operation restart gateway readiness",
        gateway_metrics_addr,
        "/ready",
        "200 OK",
    )?;
    assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
    let gateway_metrics_before = assert_metrics_endpoint_body_until(
        "P7 operation restart gateway before",
        gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_close_before = gateway_close_counters_from_metrics(&gateway_metrics_before)?;
    let gateway_routes_before =
        prometheus_sample_value(&gateway_metrics_before, "tessera_gateway_routes")?;
    let (before_health, before_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let parent = CellId::grid(0, 0, 0);
    let child0 = activation_child_cell(0);
    let child3 = activation_child_cell(3);
    let actor_a = EntityId(7_901);
    let actor_b = EntityId(7_902);
    let mut child0_session = open_gateway_join_until_snapshot(
        gateway_addr,
        child0,
        actor_a,
        Position { x: 4.0, y: 4.0 },
    )?;
    let _child3_session = open_gateway_join_until_snapshot(
        gateway_addr,
        child3,
        actor_b,
        Position { x: 24.0, y: 24.0 },
    )?;

    let proposal_response = http_json_post(
        "P7 operation proposal",
        orch_metrics_addr,
        "/operations/proposals",
    )?;
    let operation_id = json_array(&proposal_response, &["operation_ids"])?
        .first()
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("P7 operation proposal response has no operation id"))?
        .to_string();
    let proposal_snapshot = http_json_get("P7 operation ledger", orch_metrics_addr, "/operations")?;
    let proposal_record = find_p7_operation_record(&proposal_snapshot, &operation_id)?;
    assert_json_str_eq(proposal_record, &["kind"], "merge")?;
    let proposal_hash = json_str(proposal_record, &["proposal", "proposal_hash"])?.to_string();

    let policy_id = "operator_approved_dynamic_operation_v1";
    let approval_response = http_json_post(
        "P7 operation approval",
        orch_metrics_addr,
        &format!(
            "/operations/approvals?operation_id={operation_id}&policy_id={policy_id}&approver=p7-restart-smoke&expected_proposal_hash={proposal_hash}&ttl_secs=600&cooldown_key=p7-restart-smoke&budget_key=p7-restart-smoke"
        ),
    )?;
    assert_json_str_eq(&approval_response, &["status"], "approved")?;

    let execution_response = http_json_post(
        "P7 operation execution",
        orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&policy_id={policy_id}"
        ),
    )?;
    assert_json_str_eq(&execution_response, &["status"], "published")?;
    assert_json_bool_eq(&execution_response, &["assignments_changed"], true)?;

    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(gateway_addr, parent, 7_930)?;
    if !assignment_state_path.exists() {
        bail!(
            "P7 operation restart smoke expected assignment state file at {}",
            assignment_state_path.display()
        );
    }

    stack.terminate_named("p7-operation-restart-orch")?;
    let restart_orch_envs = [
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", merge_preview_json),
        (
            "TESSERA_ORCH_OPERATION_LEDGER_PATH",
            ledger_path_raw.as_str(),
        ),
        (
            "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
            assignment_state_path_raw.as_str(),
        ),
        ("TESSERA_ORCH_OPERATION_EXECUTION", "manual"),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-restart-orch",
            bin: &orchestrator_bin,
            ready_addr: orch_addr,
            envs: &restart_orch_envs,
        },
    )?;
    runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(gateway_addr, parent, 7_931)?;

    let restart_ledger_snapshot = http_json_get(
        "P7 operation ledger after restart",
        orch_metrics_addr,
        "/operations",
    )?;
    let restart_record = find_p7_operation_record(&restart_ledger_snapshot, &operation_id)?;
    validate_p7_published_execution(restart_record)?;

    let move_observed = request_move_until_delta(
        &mut child0_session,
        parent,
        actor_a,
        1.0,
        1.0,
        "P7 operation restart parent move",
    )?;
    let worker_parent_metric = worker_cell_actor_count_metric(parent);
    let worker_metrics_after_restart = assert_metrics_endpoint_body_until(
        "P7 operation restart worker-a",
        worker_a_metrics_addr,
        &["tessera_worker_cell_actor_count"],
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation restart worker-a",
        &worker_metrics_after_restart,
        worker_parent_metric.as_str(),
        2.0,
    )?;
    let worker_parent_actor_count =
        prometheus_sample_value(&worker_metrics_after_restart, worker_parent_metric.as_str())?;
    let gateway_metrics_after_restart = assert_metrics_endpoint_body_until(
        "P7 operation restart gateway after restart",
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
    let gateway_routes_after_restart =
        prometheus_sample_value(&gateway_metrics_after_restart, "tessera_gateway_routes")?;
    assert_prometheus_sample_at_least(
        "P7 operation restart gateway",
        &gateway_metrics_after_restart,
        "tessera_gateway_ping_roundtrip_seconds_count",
        1.0,
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation restart gateway",
        &gateway_metrics_after_restart,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        1.0,
    )?;
    let gateway_close_after_restart =
        gateway_close_counters_from_metrics(&gateway_metrics_after_restart)?;
    assert_gateway_close_counters_not_increased(
        "P7 operation restart gateway",
        gateway_close_before,
        gateway_close_after_restart,
    )?;
    let (after_restart_health, after_restart_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let route_converged_after_restart = (gateway_routes_after_restart - 1.0).abs() < f64::EPSILON;
    let worker_refreshed_after_restart = worker_parent_actor_count >= 2.0;
    let traffic_confirmed_after_restart = prometheus_sample_value(
        &gateway_metrics_after_restart,
        "tessera_gateway_ping_roundtrip_seconds_count",
    )? >= 1.0
        && prometheus_sample_value(
            &gateway_metrics_after_restart,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        )? >= 1.0;
    let counters_clean = gateway_close_before == gateway_close_after_restart;
    if !(route_converged_after_restart
        && worker_refreshed_after_restart
        && traffic_confirmed_after_restart
        && counters_clean)
    {
        bail!(
            "P7 operation restart evidence incomplete: route_converged_after_restart={route_converged_after_restart} worker_refreshed_after_restart={worker_refreshed_after_restart} traffic_confirmed_after_restart={traffic_confirmed_after_restart} counters_clean={counters_clean}"
        );
    }

    let observation_response = http_json_post(
        "P7 operation restart observation",
        orch_metrics_addr,
        &format!(
            "/operations/observations?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&observer=p7-restart-smoke&route_converged=true&worker_refreshed=true&traffic_confirmed=true&counters_clean=true"
        ),
    )?;
    assert_json_str_eq(&observation_response, &["status"], "completed")?;
    assert_json_bool_eq(&observation_response, &["observation_accepted"], true)?;

    let ledger = read_json_report(&ledger_path)?;
    let ledger_summary = validate_p7_operation_ledger(&ledger, true, false, true, true, false)?;
    let record = find_p7_operation_record(&ledger, &operation_id)?;
    validate_p7_completed_observation(record)?;
    let report = serde_json::json!({
        "schema": "tessera.p7_operation_restart_smoke.v1",
        "unix_secs": unix_timestamp_secs(),
        "operation": {
            "operation_id": operation_id.as_str(),
            "kind": "merge",
            "proposal_hash": proposal_hash.as_str(),
            "policy_id": policy_id
        },
        "orchestrator": {
            "grpc_addr": orch_addr,
            "metrics_addr": orch_metrics_addr,
            "registered_workers_before": before_health.registered_workers,
            "registered_workers_after_restart": after_restart_health.registered_workers,
            "assignment_listing_before": assignment_listing_summary_json(&before_listing)?,
            "assignment_listing_after_restart": assignment_listing_summary_json(&after_restart_listing)?,
            "assignment_state_path": assignment_state_path_raw.as_str()
        },
        "gateway": {
            "addr": gateway_addr,
            "metrics_addr": gateway_metrics_addr,
            "routes_before": gateway_routes_before,
            "routes_after_restart": gateway_routes_after_restart,
            "ping_roundtrips": prometheus_sample_value(&gateway_metrics_after_restart, "tessera_gateway_ping_roundtrip_seconds_count")?,
            "join_roundtrips": prometheus_sample_value(&gateway_metrics_after_restart, "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}")?,
            "move_roundtrips": prometheus_sample_value(&gateway_metrics_after_restart, "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}")?,
            "close_counters": {
                "before": gateway_close_counters_json(gateway_close_before),
                "after_restart": gateway_close_counters_json(gateway_close_after_restart)
            }
        },
        "worker": {
            "worker_a_addr": worker_a_addr,
            "worker_a_metrics_addr": worker_a_metrics_addr,
            "worker_b_addr": worker_b_addr,
            "worker_b_metrics_addr": worker_b_metrics_addr,
            "parent_actor_count_after_restart": worker_parent_actor_count
        },
        "ledger": {
            "path": ledger_path_raw.as_str(),
            "records": ledger_summary.records,
            "proposal_records": ledger_summary.proposal_records,
            "approval_records": ledger_summary.approval_records,
            "published_execution_records": ledger_summary.published_execution_records,
            "completed_observation_records": ledger_summary.completed_observation_records
        },
        "responses": {
            "proposal": proposal_response,
            "approval": approval_response,
            "execution": execution_response,
            "observation": observation_response
        },
        "checks": {
            "merge_execution_published": true,
            "assignment_state_persisted": true,
            "orchestrator_restarted": true,
            "ledger_execution_survived_restart": true,
            "route_converged_after_restart": route_converged_after_restart,
            "worker_refreshed_after_restart": worker_refreshed_after_restart,
            "traffic_confirmed_after_restart": traffic_confirmed_after_restart,
            "gateway_close_counters_clean": counters_clean,
            "stable_session_parent_move_after_restart": true,
            "observation_completed_after_restart": true,
            "ledger_observation_completed": true
        },
        "frames": {
            "ignored_before_parent_delta": move_observed.ignored_frames,
            "remote_delta_before_parent_delta": move_observed.remote_delta_frames,
            "remote_snapshot_before_parent_delta": move_observed.remote_snapshot_frames
        },
        "remaining_uncovered": [
            "split_runtime_execution",
            "multi_depth_runtime_execution",
            "soak",
            "guarded_kubernetes_operation_restart_smoke",
            "p7_completion_audit"
        ]
    });
    validate_p7_operation_restart_smoke_report(&report)?;
    let report_path = write_p7_operation_restart_smoke_report(&report)?;

    println!(
        "P7 operation restart smoke: approved merge execution and ledger state survived Orchestrator restart, post-restart observation completed, report={}, ledger={}",
        report_path.display(),
        ledger_path.display()
    );
    Ok(())
}

fn dev_p7_operation_soak_smoke(iterations: u32, sleep_ms: u64) -> Result<()> {
    if iterations == 0 {
        bail!("P7 operation soak requires --iterations > 0");
    }
    let gateway_addr = "127.0.0.1:4360";
    let gateway_metrics_addr = "127.0.0.1:4361";
    let worker_a_addr = "127.0.0.1:5361";
    let worker_b_addr = "127.0.0.1:5362";
    let worker_a_metrics_addr = "127.0.0.1:5363";
    let worker_b_metrics_addr = "127.0.0.1:5364";
    let orch_addr = "127.0.0.1:6360";
    let orch_metrics_addr = "127.0.0.1:6361";
    let orch_endpoint = format!("http://{orch_addr}");
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    let report_dir = root.join(".dev/reports");
    fs::create_dir_all(&logs)?;
    fs::create_dir_all(&pids)?;
    fs::create_dir_all(&report_dir)?;

    let ledger_path = report_dir.join("p7-operation-soak-ledger-latest.json");
    let _ = fs::remove_file(&ledger_path);
    let ledger_path_raw = ledger_path.to_string_lossy().into_owned();

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
        r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
    );
    let merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":1},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":2},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":3},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let orch_envs = [
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", merge_preview_json),
        (
            "TESSERA_ORCH_OPERATION_LEDGER_PATH",
            ledger_path_raw.as_str(),
        ),
        ("TESSERA_ORCH_OPERATION_EXECUTION", "manual"),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-soak-orch",
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
            name: "p7-operation-soak-worker-a",
            bin: &worker_bin,
            ready_addr: worker_a_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-a"),
                ("TESSERA_WORKER_ADDR", worker_a_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_a_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_a_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
                ("TESSERA_ORCH_ADDR", orch_addr),
            ],
        },
    )?;
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "p7-operation-soak-worker-b",
            bin: &worker_bin,
            ready_addr: worker_b_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-b"),
                ("TESSERA_WORKER_ADDR", worker_b_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
            name: "p7-operation-soak-gateway",
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
        "P7 operation soak gateway readiness",
        gateway_metrics_addr,
        "/ready",
        "200 OK",
    )?;
    assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
    let gateway_metrics_before = assert_metrics_endpoint_body_until(
        "P7 operation soak gateway before",
        gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_close_before = gateway_close_counters_from_metrics(&gateway_metrics_before)?;
    let gateway_routes_before =
        prometheus_sample_value(&gateway_metrics_before, "tessera_gateway_routes")?;
    let (before_health, before_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let proposal_response = http_json_post(
        "P7 operation proposal",
        orch_metrics_addr,
        "/operations/proposals",
    )?;
    let operation_id = json_array(&proposal_response, &["operation_ids"])?
        .first()
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("P7 operation proposal response has no operation id"))?
        .to_string();
    let proposal_snapshot = http_json_get("P7 operation ledger", orch_metrics_addr, "/operations")?;
    let proposal_record = find_p7_operation_record(&proposal_snapshot, &operation_id)?;
    assert_json_str_eq(proposal_record, &["kind"], "merge")?;
    let proposal_hash = json_str(proposal_record, &["proposal", "proposal_hash"])?.to_string();

    let policy_id = "operator_approved_dynamic_operation_v1";
    let approval_response = http_json_post(
        "P7 operation approval",
        orch_metrics_addr,
        &format!(
            "/operations/approvals?operation_id={operation_id}&policy_id={policy_id}&approver=p7-soak-smoke&expected_proposal_hash={proposal_hash}&ttl_secs=600&cooldown_key=p7-soak-smoke&budget_key=p7-soak-smoke"
        ),
    )?;
    assert_json_str_eq(&approval_response, &["status"], "approved")?;

    let execution_response = http_json_post(
        "P7 operation execution",
        orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&policy_id={policy_id}"
        ),
    )?;
    assert_json_str_eq(&execution_response, &["status"], "published")?;
    assert_json_bool_eq(&execution_response, &["assignments_changed"], true)?;

    let parent = CellId::grid(0, 0, 0);
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(gateway_addr, parent, 7_960)?;

    let stats = run_parent_activation_soak_loop(
        gateway_addr,
        parent,
        iterations,
        Duration::from_millis(sleep_ms),
    )?;
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, "worker-a")],
    ))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
    let _parent_probe_session = open_gateway_join_until_snapshot(
        gateway_addr,
        parent,
        EntityId(7_963),
        Position { x: 16.0, y: 16.0 },
    )?;

    let worker_parent_metric = worker_cell_actor_count_metric(parent);
    let worker_metrics_after_soak = assert_metrics_endpoint_body_until(
        "P7 operation soak worker-a",
        worker_a_metrics_addr,
        &[
            "tessera_worker_cell_actor_count",
            "tessera_worker_accepted_connections_total",
        ],
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation soak worker-a",
        &worker_metrics_after_soak,
        worker_parent_metric.as_str(),
        1.0,
    )?;
    let worker_parent_actor_count =
        prometheus_sample_value(&worker_metrics_after_soak, worker_parent_metric.as_str())?;
    let gateway_metrics_after_soak = assert_metrics_endpoint_body_until(
        "P7 operation soak gateway after soak",
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
    let expected_actor_requests = f64::from(iterations) * 4.0;
    assert_prometheus_sample_at_least(
        "P7 operation soak gateway",
        &gateway_metrics_after_soak,
        "tessera_gateway_ping_roundtrip_seconds_count",
        expected_actor_requests,
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation soak gateway",
        &gateway_metrics_after_soak,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
        4.0,
    )?;
    assert_prometheus_sample_at_least(
        "P7 operation soak gateway",
        &gateway_metrics_after_soak,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        expected_actor_requests,
    )?;
    let gateway_routes_after_soak =
        prometheus_sample_value(&gateway_metrics_after_soak, "tessera_gateway_routes")?;
    let gateway_close_after_soak =
        gateway_close_counters_from_metrics(&gateway_metrics_after_soak)?;
    assert_gateway_close_counters_not_increased(
        "P7 operation soak gateway",
        gateway_close_before,
        gateway_close_after_soak,
    )?;
    let (_after_health, after_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;

    let route_converged = (gateway_routes_after_soak - 1.0).abs() < f64::EPSILON;
    let worker_refreshed = worker_parent_actor_count >= 1.0;
    let traffic_confirmed =
        stats.pings_ok >= u64::from(iterations) * 4 && stats.moves_ok >= u64::from(iterations) * 4;
    let counters_clean = gateway_close_before == gateway_close_after_soak;
    if !(route_converged && worker_refreshed && traffic_confirmed && counters_clean) {
        bail!(
            "P7 operation soak evidence incomplete: route_converged={route_converged} worker_refreshed={worker_refreshed} traffic_confirmed={traffic_confirmed} counters_clean={counters_clean}"
        );
    }

    let observation_response = http_json_post(
        "P7 operation soak observation",
        orch_metrics_addr,
        &format!(
            "/operations/observations?operation_id={operation_id}&expected_proposal_hash={proposal_hash}&observer=p7-soak-smoke&route_converged=true&worker_refreshed=true&traffic_confirmed=true&counters_clean=true"
        ),
    )?;
    assert_json_str_eq(&observation_response, &["status"], "completed")?;
    assert_json_bool_eq(&observation_response, &["observation_accepted"], true)?;

    let ledger = read_json_report(&ledger_path)?;
    let ledger_summary = validate_p7_operation_ledger(&ledger, true, false, true, true, false)?;
    let record = find_p7_operation_record(&ledger, &operation_id)?;
    validate_p7_completed_observation(record)?;
    let report = serde_json::json!({
        "schema": "tessera.p7_operation_soak_smoke.v1",
        "unix_secs": unix_timestamp_secs(),
        "operation": {
            "operation_id": operation_id.as_str(),
            "kind": "merge",
            "proposal_hash": proposal_hash.as_str(),
            "policy_id": policy_id
        },
        "soak": {
            "iterations": iterations,
            "sleep_ms": sleep_ms,
            "pings_ok": stats.pings_ok,
            "moves_ok": stats.moves_ok,
            "expected_actor_requests": expected_actor_requests
        },
        "orchestrator": {
            "grpc_addr": orch_addr,
            "metrics_addr": orch_metrics_addr,
            "registered_workers": before_health.registered_workers,
            "assignment_listing_before": assignment_listing_summary_json(&before_listing)?,
            "assignment_listing_after": assignment_listing_summary_json(&after_listing)?
        },
        "gateway": {
            "addr": gateway_addr,
            "metrics_addr": gateway_metrics_addr,
            "routes_before": gateway_routes_before,
            "routes_after_soak": gateway_routes_after_soak,
            "ping_roundtrips": prometheus_sample_value(&gateway_metrics_after_soak, "tessera_gateway_ping_roundtrip_seconds_count")?,
            "join_roundtrips": prometheus_sample_value(&gateway_metrics_after_soak, "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}")?,
            "move_roundtrips": prometheus_sample_value(&gateway_metrics_after_soak, "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}")?,
            "close_counters": {
                "before": gateway_close_counters_json(gateway_close_before),
                "after_soak": gateway_close_counters_json(gateway_close_after_soak)
            }
        },
        "worker": {
            "worker_a_addr": worker_a_addr,
            "worker_a_metrics_addr": worker_a_metrics_addr,
            "worker_b_addr": worker_b_addr,
            "worker_b_metrics_addr": worker_b_metrics_addr,
            "parent_actor_count_after_soak": worker_parent_actor_count,
            "worker_a_accepted_connections": prometheus_sample_value(&worker_metrics_after_soak, "tessera_worker_accepted_connections_total")?
        },
        "ledger": {
            "path": ledger_path_raw.as_str(),
            "records": ledger_summary.records,
            "proposal_records": ledger_summary.proposal_records,
            "approval_records": ledger_summary.approval_records,
            "published_execution_records": ledger_summary.published_execution_records,
            "completed_observation_records": ledger_summary.completed_observation_records
        },
        "responses": {
            "proposal": proposal_response,
            "approval": approval_response,
            "execution": execution_response,
            "observation": observation_response
        },
        "checks": {
            "merge_execution_published": true,
            "route_converged_after_soak": route_converged,
            "worker_refreshed_after_soak": worker_refreshed,
            "traffic_confirmed_after_soak": traffic_confirmed,
            "gateway_close_counters_clean": counters_clean,
            "observation_completed_after_soak": true,
            "ledger_observation_completed": true
        },
        "frames": {
            "ignored_frames": stats.ignored_frames,
            "remote_delta_frames": stats.remote_delta_frames,
            "remote_snapshot_frames": stats.remote_snapshot_frames
        },
        "remaining_uncovered": [
            "split_runtime_execution",
            "multi_depth_runtime_execution",
            "guarded_kubernetes_operation_soak_smoke",
            "p7_completion_audit"
        ]
    });
    validate_p7_operation_soak_smoke_report(&report, iterations)?;
    let report_path = write_p7_operation_soak_smoke_report(&report)?;

    println!(
        "P7 operation soak smoke: approved merge execution stayed converged across {iterations} per-actor parent ping/move iterations and completed observation, report={}, ledger={}",
        report_path.display(),
        ledger_path.display()
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultiDepthActivationSmokeMode {
    Success,
    PostPublishFailure,
    RestartRecovery,
    Soak { iterations: u32, sleep_ms: u64 },
}

fn dev_multi_depth_activation_smoke() -> Result<()> {
    dev_multi_depth_activation_smoke_inner(MultiDepthActivationSmokeMode::Success)
}

fn dev_multi_depth_activation_failure_smoke() -> Result<()> {
    dev_multi_depth_activation_smoke_inner(MultiDepthActivationSmokeMode::PostPublishFailure)
}

fn dev_multi_depth_activation_restart_smoke() -> Result<()> {
    dev_multi_depth_activation_smoke_inner(MultiDepthActivationSmokeMode::RestartRecovery)
}

fn dev_multi_depth_activation_soak(iterations: u32, sleep_ms: u64) -> Result<()> {
    if iterations == 0 {
        bail!("multi-depth activation soak requires --iterations > 0");
    }
    dev_multi_depth_activation_smoke_inner(MultiDepthActivationSmokeMode::Soak {
        iterations,
        sleep_ms,
    })
}

fn dev_multi_depth_activation_smoke_inner(mode: MultiDepthActivationSmokeMode) -> Result<()> {
    let gateway_addr = "127.0.0.1:4310";
    let gateway_metrics_addr = "127.0.0.1:4311";
    let worker_a_addr = "127.0.0.1:5311";
    let worker_b_addr = "127.0.0.1:5312";
    let worker_a_metrics_addr = "127.0.0.1:5313";
    let worker_b_metrics_addr = "127.0.0.1:5314";
    let orch_addr = "127.0.0.1:6310";
    let orch_metrics_addr = "127.0.0.1:6311";
    let orch_endpoint = format!("http://{orch_addr}");
    let root = workspace_root();
    let (_dev, logs, pids) = dev_dirs();
    let assignment_state_path = root
        .join(".dev/reports")
        .join("multi-depth-activation-restart-assignment-state.json");
    if mode == MultiDepthActivationSmokeMode::RestartRecovery {
        if let Some(parent) = assignment_state_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let _ = fs::remove_file(&assignment_state_path);
    }
    let assignment_state_path_raw = assignment_state_path.to_string_lossy().into_owned();
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
    let parent = multi_depth_activation_parent();
    let children = parent
        .canonical_children()
        .ok_or_else(|| anyhow::anyhow!("multi-depth smoke parent must be canonical"))?;
    let orch_config_json = format!(
        r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":{},"cx":{},"cy":{},"depth":{},"sub":{}}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#,
        parent.world, parent.cx, parent.cy, parent.depth, parent.sub
    );
    let mut orch_envs = vec![
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];
    if mode == MultiDepthActivationSmokeMode::RestartRecovery {
        orch_envs.push((
            "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
            assignment_state_path_raw.as_str(),
        ));
    }

    let mut stack = ManagedDevStack::default();
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "multi-depth-activation-orch",
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
            name: "multi-depth-activation-worker-a",
            bin: &worker_bin,
            ready_addr: worker_a_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-a"),
                ("TESSERA_WORKER_ADDR", worker_a_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_a_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_a_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
                ("TESSERA_ORCH_ADDR", orch_addr),
            ],
        },
    )?;
    stack.spawn(
        &root,
        &logs,
        &pids,
        DevProcessSpec {
            name: "multi-depth-activation-worker-b",
            bin: &worker_bin,
            ready_addr: worker_b_addr,
            envs: &[
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_WORKER_ID", "worker-b"),
                ("TESSERA_WORKER_ADDR", worker_b_addr),
                ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
            name: "multi-depth-activation-gateway",
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
        "multi-depth activation gateway readiness",
        gateway_metrics_addr,
        "/ready",
        "200 OK",
    )?;
    assert_gateway_ready_routes(gateway_metrics_addr, 1)?;

    let actor = EntityId(7_501);
    let mut session = GatewaySession::connect(gateway_addr)?;
    let joined = session.request(
        parent,
        ClientMsg::Join {
            actor,
            pos: Position { x: 24.0, y: 24.0 },
        },
    )?;
    assert_snapshot_contains("multi-depth activation parent join", joined, parent, actor)?;

    let operation_prefix = match mode {
        MultiDepthActivationSmokeMode::Success => "multi-depth-activation-smoke",
        MultiDepthActivationSmokeMode::PostPublishFailure => "multi-depth-activation-failure-smoke",
        MultiDepthActivationSmokeMode::RestartRecovery => "multi-depth-activation-restart-smoke",
        MultiDepthActivationSmokeMode::Soak { .. } => "multi-depth-activation-soak",
    };
    let operation_id = format!("{operation_prefix}-{}", unix_timestamp_secs());
    let targets = vec![
        (children[0], "worker-a".to_string()),
        (children[1], "worker-b".to_string()),
        (children[2], "worker-a".to_string()),
        (children[3], "worker-b".to_string()),
    ];
    let response = runtime.block_on(submit_split_activation_with_child_cells(
        &orch_endpoint,
        operation_id.clone(),
        parent,
        &targets,
    ))?;
    assert_split_activation_published(&response)?;
    let expected_listing = targets
        .iter()
        .map(|(cell, worker_id)| (*cell, worker_id.as_str()))
        .collect::<Vec<_>>();
    runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
    assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
    for (idx, child) in children.iter().enumerate() {
        assert_gateway_ping_until(gateway_addr, *child, 7_500 + idx as u64)?;
    }

    if mode == MultiDepthActivationSmokeMode::RestartRecovery {
        if !assignment_state_path.exists() {
            bail!(
                "multi-depth activation restart smoke expected assignment state file at {}",
                assignment_state_path.display()
            );
        }
        drop(session);
        stack.terminate_named("multi-depth-activation-orch")?;
        let restart_orch_envs = vec![
            ("RUST_LOG", rust_log.as_str()),
            ("TESSERA_ORCH_ADDR", orch_addr),
            ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
            ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
            (
                "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
                assignment_state_path_raw.as_str(),
            ),
        ];
        stack.spawn(
            &root,
            &logs,
            &pids,
            DevProcessSpec {
                name: "multi-depth-activation-orch",
                bin: &orchestrator_bin,
                ready_addr: orch_addr,
                envs: &restart_orch_envs,
            },
        )?;
        runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
        runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;

        stack.terminate_named("multi-depth-activation-gateway")?;
        stack.spawn(
            &root,
            &logs,
            &pids,
            DevProcessSpec {
                name: "multi-depth-activation-gateway",
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
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        let restart_probe = probe_multi_depth_convergence(gateway_addr, &expected_listing, 8_800);
        restart_probe.assert_success()?;

        let restart_actor = EntityId(7_503);
        let remote_aoi_actor = EntityId(7_504);
        let _post_restart_session = open_gateway_join_until_snapshot(
            gateway_addr,
            children[3],
            restart_actor,
            Position { x: 16.0, y: 16.0 },
        )?;
        let _remote_aoi_session = open_gateway_join_until_snapshot(
            gateway_addr,
            children[2],
            remote_aoi_actor,
            Position { x: 16.0, y: 16.0 },
        )?;
        let worker_b_remote_interest_clients = assert_prometheus_sample_at_least_until(
            "multi-depth activation restart worker-b",
            worker_b_metrics_addr,
            "tessera_worker_remote_interest_clients",
            1.0,
        )?;
        let worker_b_remote_interest_cells = assert_prometheus_sample_at_least_until(
            "multi-depth activation restart worker-b",
            worker_b_metrics_addr,
            "tessera_worker_remote_interest_cells",
            1.0,
        )?;

        let gateway_metrics = assert_metrics_endpoint_body(
            "multi-depth activation restart gateway",
            gateway_metrics_addr,
            &["tessera_gateway_routes"],
        )?;
        let gateway_routes = prometheus_sample_value(&gateway_metrics, "tessera_gateway_routes")?;
        let report_path = write_multi_depth_activation_restart_smoke_report(
            MultiDepthActivationRestartSmokeReport {
                operation_id: &operation_id,
                assignment_state_path: &assignment_state_path,
                gateway_addr,
                gateway_metrics_addr,
                orch_addr,
                worker_a_addr,
                worker_b_addr,
                parent,
                children: &[
                    (children[0], "worker-a"),
                    (children[1], "worker-b"),
                    (children[2], "worker-a"),
                    (children[3], "worker-b"),
                ],
                gateway_routes,
                restart_probe: &restart_probe,
                worker_b_remote_interest_clients,
                worker_b_remote_interest_cells,
            },
        )?;
        let report = read_json_report(&report_path)?;
        validate_multi_depth_activation_restart_smoke_report(&report)?;
        println!(
            "multi-depth activation restart smoke: published canonical split assignments survived Orchestrator restart with manual activation disabled, Gateway routes reconverged, child traffic and remote AOI interests resynced, report={}",
            report_path.display()
        );
        return Ok(());
    }

    if mode == MultiDepthActivationSmokeMode::PostPublishFailure {
        stack.terminate_named("multi-depth-activation-worker-b")?;
        let failure_probe = probe_multi_depth_convergence(gateway_addr, &expected_listing, 8_600);
        failure_probe.assert_failed_cells_only(&[children[1], children[3]])?;
        runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
        let gateway_metrics_after_failure = assert_metrics_endpoint_body(
            "multi-depth activation gateway after failure",
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
                name: "multi-depth-activation-worker-b",
                bin: &worker_bin,
                ready_addr: worker_b_addr,
                envs: &[
                    ("RUST_LOG", rust_log.as_str()),
                    ("TESSERA_WORKER_ID", "worker-b"),
                    ("TESSERA_WORKER_ADDR", worker_b_addr),
                    ("TESSERA_WORKER_ADVERTISE_ADDR", worker_b_addr),
                    ("TESSERA_WORKER_METRICS_ADDR", worker_b_metrics_addr),
                    ("TESSERA_WORKER_REFRESH_SECS", "1"),
                    ("TESSERA_ORCH_ADDR", orch_addr),
                ],
            },
        )?;
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        let recovery_probe = probe_multi_depth_convergence(gateway_addr, &expected_listing, 8_700);
        recovery_probe.assert_success()?;
        let report_path = write_multi_depth_activation_failure_smoke_report(
            MultiDepthActivationFailureSmokeReport {
                operation_id: &operation_id,
                gateway_addr,
                gateway_metrics_addr,
                orch_addr,
                worker_a_addr,
                worker_b_addr,
                parent,
                children: &[
                    (children[0], "worker-a"),
                    (children[1], "worker-b"),
                    (children[2], "worker-a"),
                    (children[3], "worker-b"),
                ],
                gateway_routes_after_failure,
                failure_probe: &failure_probe,
                recovery_probe: &recovery_probe,
            },
        )?;
        let report = read_json_report(&report_path)?;
        validate_multi_depth_activation_failure_smoke_report(&report)?;
        println!(
            "multi-depth activation failure smoke: target worker outage was detected for canonical child routes, assignments stayed published, worker restart recovered convergence, report={}",
            report_path.display()
        );
        return Ok(());
    }

    if let MultiDepthActivationSmokeMode::Soak {
        iterations,
        sleep_ms,
    } = mode
    {
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        let stats = run_cell_activation_soak_loop(
            gateway_addr,
            &children,
            iterations,
            Duration::from_millis(sleep_ms),
        )?;
        runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;

        let gateway_metrics = assert_metrics_endpoint_body_until(
            "multi-depth activation soak gateway",
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
            "multi-depth activation soak gateway",
            &gateway_metrics,
            "tessera_gateway_routes",
            4.0,
        )?;
        let expected_child_requests = f64::from(iterations) * 4.0;
        assert_prometheus_sample_at_least(
            "multi-depth activation soak gateway",
            &gateway_metrics,
            "tessera_gateway_ping_roundtrip_seconds_count",
            expected_child_requests,
        )?;
        assert_prometheus_sample_at_least(
            "multi-depth activation soak gateway",
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
            expected_child_requests,
        )?;
        assert_prometheus_sample_at_least(
            "multi-depth activation soak gateway",
            &gateway_metrics,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
            5.0,
        )?;
        assert_prometheus_sample_eq(
            "multi-depth activation soak gateway",
            &gateway_metrics,
            "tessera_gateway_client_closes_no_route_total",
            0.0,
        )?;
        assert_prometheus_sample_eq(
            "multi-depth activation soak gateway",
            &gateway_metrics,
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            0.0,
        )?;
        assert_prometheus_sample_eq(
            "multi-depth activation soak gateway",
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
            "multi-depth activation soak worker-a",
            worker_a_metrics_addr,
            &[
                "tessera_worker_accepted_connections_total",
                "tessera_worker_relay_connections_total",
            ],
        )?;
        let worker_b_metrics = assert_metrics_endpoint_body_until(
            "multi-depth activation soak worker-b",
            worker_b_metrics_addr,
            &[
                "tessera_worker_accepted_connections_total",
                "tessera_worker_relay_connections_total",
            ],
        )?;

        let report_path =
            write_multi_depth_activation_soak_report(MultiDepthActivationSoakReport {
                operation_id: &operation_id,
                gateway_addr,
                gateway_metrics_addr,
                orch_addr,
                worker_a_addr,
                worker_b_addr,
                parent,
                children: &[
                    (children[0], "worker-a"),
                    (children[1], "worker-b"),
                    (children[2], "worker-a"),
                    (children[3], "worker-b"),
                ],
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
        let report = read_json_report(&report_path)?;
        validate_multi_depth_activation_soak_report(&report, iterations)?;
        println!(
            "multi-depth activation soak: canonical split stayed converged across {iterations} per-child ping/move iterations, remote AOI frames observed, report={}",
            report_path.display()
        );
        return Ok(());
    }

    let remote_aoi_actor = EntityId(7_502);
    let _remote_aoi_session = open_gateway_join_until_snapshot(
        gateway_addr,
        children[2],
        remote_aoi_actor,
        Position { x: 8.0, y: 24.0 },
    )?;
    let (moved, remote_aoi_snapshot) = request_move_until_delta_and_snapshot(
        &mut session,
        children[3],
        actor,
        children[2],
        remote_aoi_actor,
    )?;
    assert_delta_contains(
        "multi-depth activation moved replayed actor",
        moved,
        children[3],
        actor,
    )?;
    assert_snapshot_contains(
        "multi-depth activation AOI resync snapshot",
        remote_aoi_snapshot,
        children[2],
        remote_aoi_actor,
    )?;

    let gateway_metrics = assert_metrics_endpoint_body(
        "multi-depth activation gateway",
        gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_upstream_route_change_reconnects_total",
        ],
    )?;
    assert_prometheus_sample_at_least(
        "multi-depth activation gateway",
        &gateway_metrics,
        "tessera_gateway_routes",
        4.0,
    )?;
    let gateway_routes = prometheus_sample_value(&gateway_metrics, "tessera_gateway_routes")?;
    assert_prometheus_sample_at_least(
        "multi-depth activation gateway",
        &gateway_metrics,
        "tessera_gateway_upstream_route_change_reconnects_total",
        1.0,
    )?;
    let gateway_route_change_reconnects = prometheus_sample_value(
        &gateway_metrics,
        "tessera_gateway_upstream_route_change_reconnects_total",
    )?;

    let worker_b_metrics = assert_metrics_endpoint_body(
        "multi-depth activation worker-b",
        worker_b_metrics_addr,
        &[
            "tessera_worker_relay_connections_total",
            "tessera_worker_accepted_connections_total",
        ],
    )?;
    assert_prometheus_sample_at_least(
        "multi-depth activation worker-b",
        &worker_b_metrics,
        "tessera_worker_relay_connections_total",
        1.0,
    )?;
    let worker_b_relay_connections =
        prometheus_sample_value(&worker_b_metrics, "tessera_worker_relay_connections_total")?;

    let report_path = write_multi_depth_activation_smoke_report(MultiDepthActivationSmokeReport {
        operation_id: &operation_id,
        gateway_addr,
        gateway_metrics_addr,
        orch_addr,
        worker_a_addr,
        worker_b_addr,
        worker_a_metrics_addr,
        worker_b_metrics_addr,
        parent,
        children: &[
            (children[0], "worker-a"),
            (children[1], "worker-b"),
            (children[2], "worker-a"),
            (children[3], "worker-b"),
        ],
        gateway_routes,
        gateway_route_change_reconnects,
        worker_b_relay_connections,
    })?;
    let report = read_json_report(&report_path)?;
    validate_multi_depth_activation_smoke_report(&report)?;
    println!(
        "multi-depth activation smoke: canonical explicit-child split published, Gateway routes converged to canonical children, stable-session post-split move and remote AOI resync succeeded, report={}",
        report_path.display()
    );
    Ok(())
}

fn dev_multi_depth_activation_report_check(report: Option<&Path>) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_multi_depth_activation_smoke_path);
    let report = read_json_report(&report_path)?;
    validate_multi_depth_activation_smoke_report(&report)?;
    println!(
        "multi-depth activation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_multi_depth_activation_failure_report_check(report: Option<&Path>) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_multi_depth_activation_failure_smoke_path);
    let report = read_json_report(&report_path)?;
    validate_multi_depth_activation_failure_smoke_report(&report)?;
    println!(
        "multi-depth activation failure report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_multi_depth_activation_restart_report_check(report: Option<&Path>) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_multi_depth_activation_restart_smoke_path);
    let report = read_json_report(&report_path)?;
    validate_multi_depth_activation_restart_smoke_report(&report)?;
    println!(
        "multi-depth activation restart report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_multi_depth_activation_soak_report_check(
    report: Option<&Path>,
    min_iterations: u32,
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_multi_depth_activation_soak_path);
    let report = read_json_report(&report_path)?;
    validate_multi_depth_activation_soak_report(&report, min_iterations)?;
    println!(
        "multi-depth activation soak report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_merge_plan_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::MergePlan)
}

fn dev_planner_mutation_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::PlannerMutation)
}

fn dev_merge_activation_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::MergeActivation)
}

fn dev_canonical_merge_activation_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::CanonicalMergeActivation)
}

fn dev_canonical_merge_activation_report_check(report: Option<&Path>) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_canonical_merge_activation_path);
    let report = read_json_report(&report_path)?;
    validate_merge_activation_smoke_report(&report, false)?;
    validate_canonical_merge_activation_smoke_report(&report)?;
    println!(
        "canonical merge activation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_canonical_merge_activation_restart_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::CanonicalMergeRestart)
}

fn dev_canonical_merge_activation_restart_report_check(report: Option<&Path>) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_canonical_merge_activation_restart_path);
    let report = read_json_report(&report_path)?;
    validate_merge_activation_smoke_report(&report, true)?;
    validate_canonical_merge_activation_smoke_report(&report)?;
    println!(
        "canonical merge activation restart report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_canonical_merge_activation_failure_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::CanonicalMergePostPublishFailure)
}

fn dev_canonical_merge_activation_failure_report_check(report: Option<&Path>) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_canonical_merge_activation_failure_path);
    let report = read_json_report(&report_path)?;
    validate_merge_activation_failure_smoke_report(&report)?;
    validate_canonical_merge_activation_smoke_report(&report)?;
    println!(
        "canonical merge activation failure report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_canonical_merge_activation_soak(iterations: u32, sleep_ms: u64) -> Result<()> {
    if iterations == 0 {
        bail!("canonical merge activation soak requires --iterations > 0");
    }
    dev_activation_smoke_inner(ActivationSmokeMode::CanonicalMergeSoak {
        iterations,
        sleep_ms,
    })
}

fn dev_canonical_merge_activation_soak_report_check(
    report: Option<&Path>,
    min_iterations: u32,
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_canonical_merge_activation_soak_path);
    let report = read_json_report(&report_path)?;
    validate_merge_activation_soak_report(&report, min_iterations)?;
    validate_canonical_merge_activation_smoke_report(&report)?;
    println!(
        "canonical merge activation soak report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_merge_activation_cross_worker_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::MergeCrossWorker)
}

fn dev_merge_activation_restart_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::MergeRestart)
}

fn dev_merge_activation_failure_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::MergePostPublishFailure)
}

fn dev_merge_activation_soak(iterations: u32, sleep_ms: u64) -> Result<()> {
    if iterations == 0 {
        bail!("merge activation soak requires --iterations > 0");
    }
    dev_activation_smoke_inner(ActivationSmokeMode::MergeSoak {
        iterations,
        sleep_ms,
    })
}

fn dev_merge_activation_soak_report_check(
    report: Option<&Path>,
    min_iterations: u32,
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_merge_activation_soak_path);
    let report = read_json_report(&report_path)?;
    validate_merge_activation_soak_report(&report, min_iterations)?;
    println!(
        "merge activation soak report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn dev_activation_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::Success)
}

fn dev_activation_failure_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::PostPublishFailure)
}

fn dev_activation_restart_smoke() -> Result<()> {
    dev_activation_smoke_inner(ActivationSmokeMode::RestartRecovery)
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
    let assignment_state_path = root
        .join(".dev/reports")
        .join("activation-restart-assignment-state.json");
    if matches!(mode, ActivationSmokeMode::RestartRecovery) || is_merge_restart_mode(mode) {
        if let Some(parent) = assignment_state_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let _ = fs::remove_file(&assignment_state_path);
    }
    let assignment_state_path_raw = assignment_state_path.to_string_lossy().into_owned();
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
    let orch_config_json = if mode == ActivationSmokeMode::MergeCrossWorker {
        format!(
            r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}}]}}"#
        )
    } else if is_canonical_merge_mode(mode) {
        format!(
            r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":-4,"cy":6,"depth":3,"sub":0}},{{"world":0,"cx":-3,"cy":6,"depth":3,"sub":0}},{{"world":0,"cx":-4,"cy":7,"depth":3,"sub":0}},{{"world":0,"cx":-3,"cy":7,"depth":3,"sub":0}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
        )
    } else if matches!(
        mode,
        ActivationSmokeMode::MergePlan
            | ActivationSmokeMode::PlannerMutation
            | ActivationSmokeMode::MergeActivation
            | ActivationSmokeMode::MergeCrossWorker
            | ActivationSmokeMode::MergeRestart
            | ActivationSmokeMode::MergePostPublishFailure
            | ActivationSmokeMode::MergeSoak { .. }
    ) {
        format!(
            r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0,"depth":1,"sub":0}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":1}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":2}},{{"world":0,"cx":0,"cy":0,"depth":1,"sub":3}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
        )
    } else {
        format!(
            r#"{{"workers":[{{"id":"worker-a","addr":"{worker_a_addr}","cells":[{{"world":0,"cx":0,"cy":0}}]}},{{"id":"worker-b","addr":"{worker_b_addr}","cells":[]}}]}}"#
        )
    };
    let split_merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0},"actor_count":140,"move_queue_pressure":70,"high_pressure_windows":3,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":1},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":2},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":0,"cy":0,"depth":1,"sub":3},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let canonical_merge_preview_json = r#"{"cells":[{"cell":{"world":0,"cx":-4,"cy":6,"depth":3,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":-3,"cy":6,"depth":3,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":-4,"cy":7,"depth":3,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"},{"cell":{"world":0,"cx":-3,"cy":7,"depth":3,"sub":0},"actor_count":0,"move_queue_pressure":0,"tick_stage_micros":0,"relay_fanout":0,"handover_failures":0,"low_pressure_windows":5,"cell_age_secs":120,"owner_worker_id":"worker-a"}]}"#;
    let mut orch_envs = vec![
        ("RUST_LOG", rust_log.as_str()),
        ("TESSERA_ORCH_ADDR", orch_addr),
        ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
        ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
        ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "manual"),
    ];
    if matches!(mode, ActivationSmokeMode::RestartRecovery) || is_merge_restart_mode(mode) {
        orch_envs.push((
            "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
            assignment_state_path_raw.as_str(),
        ));
    }
    if mode == ActivationSmokeMode::Plan {
        orch_envs.push((
            "TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON",
            split_merge_preview_json,
        ));
    } else if is_canonical_merge_mode(mode) {
        orch_envs.push((
            "TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON",
            canonical_merge_preview_json,
        ));
    } else if matches!(
        mode,
        ActivationSmokeMode::MergePlan
            | ActivationSmokeMode::PlannerMutation
            | ActivationSmokeMode::MergeActivation
            | ActivationSmokeMode::MergeCrossWorker
            | ActivationSmokeMode::MergeRestart
            | ActivationSmokeMode::MergePostPublishFailure
            | ActivationSmokeMode::MergeSoak { .. }
    ) {
        orch_envs.push(("TESSERA_ORCH_SPLIT_MERGE_PREVIEW_JSON", merge_preview_json));
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
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
                ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
    let expected_initial_routes = if matches!(
        mode,
        ActivationSmokeMode::MergePlan
            | ActivationSmokeMode::PlannerMutation
            | ActivationSmokeMode::MergeActivation
            | ActivationSmokeMode::CanonicalMergeActivation
            | ActivationSmokeMode::CanonicalMergeRestart
            | ActivationSmokeMode::CanonicalMergePostPublishFailure
            | ActivationSmokeMode::CanonicalMergeSoak { .. }
            | ActivationSmokeMode::MergeCrossWorker
            | ActivationSmokeMode::MergeRestart
            | ActivationSmokeMode::MergePostPublishFailure
            | ActivationSmokeMode::MergeSoak { .. }
    ) {
        4
    } else {
        1
    };
    assert_gateway_ready_routes(gateway_metrics_addr, expected_initial_routes)?;

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
    if mode == ActivationSmokeMode::MergePlan {
        let plan_path = default_merge_activation_plan_path();
        let plan = build_merge_activation_plan(
            &orch_endpoint,
            orch_metrics_addr,
            Some(format!("merge-plan-smoke-{}", unix_timestamp_secs())),
        )?;
        if plan.status != "ready" {
            bail!(
                "merge plan smoke expected ready plan, got status={} reason={}",
                plan.status,
                plan.reason
            );
        }
        if plan.parent != Some(CellId::grid(0, 0, 0))
            || plan.owner_worker_id.as_deref() != Some("worker-a")
            || plan.siblings.len() != 4
        {
            bail!(
                "merge plan smoke produced unexpected plan: parent={:?} owner={:?} siblings={:?}",
                plan.parent,
                plan.owner_worker_id,
                plan.siblings
            );
        }
        let report_path = write_merge_activation_plan_report(&plan, Some(&plan_path))?;
        let report = read_json_report(&report_path)?;
        validate_merge_activation_plan_report(&report)?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[
                (activation_child_cell(0), "worker-a"),
                (activation_child_cell(1), "worker-a"),
                (activation_child_cell(2), "worker-a"),
                (activation_child_cell(3), "worker-a"),
            ],
        ))?;
        println!(
            "merge plan smoke: preview merge candidate became a ready operator plan without assignment mutation, report={}",
            report_path.display()
        );
        return Ok(());
    }
    if mode == ActivationSmokeMode::PlannerMutation {
        let blocked_path = root
            .join(".dev/reports")
            .join("planner-activation-blocked-latest.json");
        let blocked_report_path = execute_planner_activation(PlannerActivationOptions {
            kind: "merge",
            orch_addr,
            plan_source: PlannerActivationPlanSource {
                preview_addr: orch_metrics_addr,
                live_worker_metrics: &[],
                live_policy: LiveMetricsPlanPolicy::default(),
            },
            operation_id: Some(format!(
                "planner-mutation-blocked-smoke-{}",
                unix_timestamp_secs()
            )),
            mutation: PlannerActivationMutation {
                allow_mutation: false,
                policy_id: None,
                out: Some(&blocked_path),
            },
        })?;
        let blocked_report = read_json_report(&blocked_report_path)?;
        validate_planner_activation_report(&blocked_report, false)?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[
                (activation_child_cell(0), "worker-a"),
                (activation_child_cell(1), "worker-a"),
                (activation_child_cell(2), "worker-a"),
                (activation_child_cell(3), "worker-a"),
            ],
        ))?;
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        assert_gateway_ping_until(gateway_addr, activation_child_cell(0), 7_510)?;

        let published_report_path = execute_planner_activation(PlannerActivationOptions {
            kind: "merge",
            orch_addr,
            plan_source: PlannerActivationPlanSource {
                preview_addr: orch_metrics_addr,
                live_worker_metrics: &[],
                live_policy: LiveMetricsPlanPolicy::default(),
            },
            operation_id: Some(format!("planner-mutation-smoke-{}", unix_timestamp_secs())),
            mutation: PlannerActivationMutation {
                allow_mutation: true,
                policy_id: Some(PLANNER_MUTATION_POLICY_ID),
                out: Some(&default_planner_activation_path()),
            },
        })?;
        let published_report = read_json_report(&published_report_path)?;
        validate_planner_activation_report(&published_report, true)?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(CellId::grid(0, 0, 0), "worker-a")],
        ))?;
        assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
        assert_gateway_ping_until(gateway_addr, CellId::grid(0, 0, 0), 7_511)?;
        println!(
            "planner mutation smoke: default-off planner merge was blocked without policy and published only with policy id {}, blocked_report={}, published_report={}",
            PLANNER_MUTATION_POLICY_ID,
            blocked_report_path.display(),
            published_report_path.display()
        );
        return Ok(());
    }
    if matches!(
        mode,
        ActivationSmokeMode::MergeActivation
            | ActivationSmokeMode::CanonicalMergeActivation
            | ActivationSmokeMode::CanonicalMergeRestart
            | ActivationSmokeMode::CanonicalMergePostPublishFailure
            | ActivationSmokeMode::CanonicalMergeSoak { .. }
            | ActivationSmokeMode::MergeCrossWorker
            | ActivationSmokeMode::MergeRestart
            | ActivationSmokeMode::MergePostPublishFailure
            | ActivationSmokeMode::MergeSoak { .. }
    ) {
        let canonical_merge = is_canonical_merge_mode(mode);
        let parent = if canonical_merge {
            multi_depth_activation_parent()
        } else {
            CellId::grid(0, 0, 0)
        };
        let merged_children = if canonical_merge {
            parent
                .canonical_children()
                .expect("canonical merge parent")
                .to_vec()
        } else {
            (0..4).map(activation_child_cell).collect::<Vec<_>>()
        };
        let child0 = merged_children[0];
        let child3 = merged_children[3];
        let parent_actor_metric = worker_cell_actor_count_metric(parent);
        let actor_a = EntityId(7_401);
        let actor_b = EntityId(7_402);
        let mut child0_session = open_gateway_join_until_snapshot(
            gateway_addr,
            child0,
            actor_a,
            Position { x: 4.0, y: 4.0 },
        )?;
        let mut child3_session = open_gateway_join_until_snapshot(
            gateway_addr,
            child3,
            actor_b,
            Position { x: 24.0, y: 24.0 },
        )?;
        let operation_prefix = match mode {
            ActivationSmokeMode::MergeRestart => "merge-activation-restart-smoke",
            ActivationSmokeMode::MergeCrossWorker => "merge-activation-cross-worker-smoke",
            ActivationSmokeMode::CanonicalMergeActivation => "canonical-merge-activation-smoke",
            ActivationSmokeMode::CanonicalMergeRestart => {
                "canonical-merge-activation-restart-smoke"
            }
            ActivationSmokeMode::CanonicalMergePostPublishFailure => {
                "canonical-merge-activation-failure-smoke"
            }
            ActivationSmokeMode::CanonicalMergeSoak { .. } => "canonical-merge-activation-soak",
            ActivationSmokeMode::MergePostPublishFailure => "merge-activation-failure-smoke",
            ActivationSmokeMode::MergeSoak { .. } => "merge-activation-soak",
            _ => "merge-activation-smoke",
        };
        let operation_id = format!("{operation_prefix}-{}", unix_timestamp_secs());
        if mode == ActivationSmokeMode::MergeCrossWorker {
            let response = runtime.block_on(submit_merge_activation(
                &orch_endpoint,
                operation_id.clone(),
                parent,
                "worker-a".to_string(),
            ))?;
            assert_merge_activation_published(&response)?;
            runtime.block_on(wait_for_split_listing(
                &orch_endpoint,
                &[(parent, "worker-a")],
            ))?;
            assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
            assert_gateway_ping_until(gateway_addr, parent, 7_430)?;
            let local_move_observed = request_move_until_delta(
                &mut child0_session,
                parent,
                actor_a,
                1.0,
                1.0,
                "cross-worker merge local parent move",
            )?;
            let remote_move_observed = request_move_until_delta(
                &mut child3_session,
                parent,
                actor_b,
                1.0,
                1.0,
                "cross-worker merge remote parent move",
            )?;
            let worker_a_metrics = assert_metrics_endpoint_body_until(
                "cross-worker merge worker-a",
                worker_a_metrics_addr,
                &[
                    "tessera_worker_cell_actor_count",
                    "tessera_worker_relay_connections_total",
                ],
            )?;
            assert_prometheus_sample_at_least(
                "cross-worker merge worker-a",
                &worker_a_metrics,
                parent_actor_metric.as_str(),
                2.0,
            )?;
            assert_prometheus_sample_at_least(
                "cross-worker merge worker-a",
                &worker_a_metrics,
                "tessera_worker_relay_connections_total",
                1.0,
            )?;
            let worker_b_metrics = assert_metrics_endpoint_body_until(
                "cross-worker merge worker-b",
                worker_b_metrics_addr,
                &["tessera_worker_remote_relay_frames_sent_total"],
            )?;
            assert_prometheus_sample_at_least(
                "cross-worker merge worker-b",
                &worker_b_metrics,
                "tessera_worker_remote_relay_frames_sent_total",
                1.0,
            )?;
            let gateway_metrics = assert_metrics_endpoint_body_until(
                "cross-worker merge gateway",
                gateway_metrics_addr,
                &["tessera_gateway_routes"],
            )?;
            let report_path = write_merge_activation_cross_worker_smoke_report(
                MergeActivationCrossWorkerSmokeReport {
                    operation_id: &operation_id,
                    gateway_addr,
                    gateway_metrics_addr,
                    orch_addr,
                    worker_a_addr,
                    worker_b_addr,
                    worker_a_metrics_addr,
                    worker_b_metrics_addr,
                    parent,
                    remote_child: child3,
                    owner_worker_id: "worker-a",
                    remote_source_worker_id: "worker-b",
                    gateway_routes: prometheus_sample_value(
                        &gateway_metrics,
                        "tessera_gateway_routes",
                    )?,
                    worker_a_parent_actor_count: prometheus_sample_value(
                        &worker_a_metrics,
                        parent_actor_metric.as_str(),
                    )?,
                    worker_a_relay_connections: prometheus_sample_value(
                        &worker_a_metrics,
                        "tessera_worker_relay_connections_total",
                    )?,
                    worker_b_remote_relay_frames_sent: prometheus_sample_value(
                        &worker_b_metrics,
                        "tessera_worker_remote_relay_frames_sent_total",
                    )?,
                    local_ignored_frames_before_parent_delta: local_move_observed.ignored_frames,
                    remote_ignored_frames_before_parent_delta: remote_move_observed.ignored_frames,
                    remote_delta_frames_before_parent_delta: remote_move_observed
                        .remote_delta_frames,
                    remote_snapshot_frames_before_parent_delta: remote_move_observed
                        .remote_snapshot_frames,
                },
            )?;
            let report = read_json_report(&report_path)?;
            validate_merge_activation_cross_worker_smoke_report(&report)?;
            println!(
                "merge activation cross-worker smoke: manual mixed-owner merge replayed worker-b child state into worker-a parent and stable-session parent moves succeeded, report={}",
                report_path.display()
            );
            return Ok(());
        }
        let plan = build_merge_activation_plan(
            &orch_endpoint,
            orch_metrics_addr,
            Some(operation_id.clone()),
        )?;
        if plan.status != "ready" {
            bail!(
                "merge activation smoke expected ready plan, got status={} reason={}",
                plan.status,
                plan.reason
            );
        }
        if plan.parent != Some(parent) || plan.owner_worker_id.as_deref() != Some("worker-a") {
            bail!(
                "merge activation smoke produced unexpected plan: parent={:?} owner={:?}",
                plan.parent,
                plan.owner_worker_id
            );
        }
        let plan_report_path =
            write_merge_activation_plan_report(&plan, Some(&default_merge_activation_plan_path()))?;
        let response = runtime.block_on(submit_merge_activation(
            &orch_endpoint,
            operation_id.clone(),
            parent,
            "worker-a".to_string(),
        ))?;
        assert_merge_activation_published(&response)?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(parent, "worker-a")],
        ))?;
        assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
        assert_gateway_ping_until(gateway_addr, parent, 7_430)?;
        let mut gateway_routes_after_restart = None;
        if is_merge_restart_mode(mode) {
            if !assignment_state_path.exists() {
                bail!(
                    "merge activation restart smoke expected assignment state file at {}",
                    assignment_state_path.display()
                );
            }
            stack.terminate_named("activation-orch")?;
            let restart_orch_envs = vec![
                ("RUST_LOG", rust_log.as_str()),
                ("TESSERA_ORCH_ADDR", orch_addr),
                ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
                ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
                (
                    "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
                    assignment_state_path_raw.as_str(),
                ),
            ];
            stack.spawn(
                &root,
                &logs,
                &pids,
                DevProcessSpec {
                    name: "activation-orch",
                    bin: &orchestrator_bin,
                    ready_addr: orch_addr,
                    envs: &restart_orch_envs,
                },
            )?;
            runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
            runtime.block_on(wait_for_split_listing(
                &orch_endpoint,
                &[(parent, "worker-a")],
            ))?;
            assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
            assert_gateway_ping_until(gateway_addr, parent, 7_431)?;
            let gateway_metrics_after_restart = assert_metrics_endpoint_body_until(
                "merge activation restart gateway",
                gateway_metrics_addr,
                &["tessera_gateway_routes"],
            )?;
            gateway_routes_after_restart = Some(prometheus_sample_value(
                &gateway_metrics_after_restart,
                "tessera_gateway_routes",
            )?);
        }
        let move_observed = request_move_until_delta(
            &mut child0_session,
            parent,
            actor_a,
            1.0,
            1.0,
            "merge activation parent move",
        )?;
        let worker_metrics = assert_metrics_endpoint_body_until(
            "merge activation worker-a",
            worker_a_metrics_addr,
            &["tessera_worker_cell_actor_count"],
        )?;
        assert_prometheus_sample_at_least(
            "merge activation worker-a",
            &worker_metrics,
            parent_actor_metric.as_str(),
            2.0,
        )?;
        let gateway_metrics = assert_metrics_endpoint_body_until(
            "merge activation gateway",
            gateway_metrics_addr,
            &["tessera_gateway_routes"],
        )?;
        let gateway_routes = prometheus_sample_value(&gateway_metrics, "tessera_gateway_routes")?;
        if let Some((iterations, sleep_ms)) = match mode {
            ActivationSmokeMode::MergeSoak {
                iterations,
                sleep_ms,
            }
            | ActivationSmokeMode::CanonicalMergeSoak {
                iterations,
                sleep_ms,
            } => Some((iterations, sleep_ms)),
            _ => None,
        } {
            assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
            let stats = run_parent_activation_soak_loop(
                gateway_addr,
                parent,
                iterations,
                Duration::from_millis(sleep_ms),
            )?;
            runtime.block_on(wait_for_split_listing(
                &orch_endpoint,
                &[(parent, "worker-a")],
            ))?;
            assert_gateway_ready_routes(gateway_metrics_addr, 1)?;

            let gateway_metrics = assert_metrics_endpoint_body_until(
                "merge activation soak gateway",
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
                "merge activation soak gateway",
                &gateway_metrics,
                "tessera_gateway_routes",
                1.0,
            )?;
            let expected_actor_requests = f64::from(iterations) * 4.0;
            assert_prometheus_sample_at_least(
                "merge activation soak gateway",
                &gateway_metrics,
                "tessera_gateway_ping_roundtrip_seconds_count",
                expected_actor_requests,
            )?;
            assert_prometheus_sample_at_least(
                "merge activation soak gateway",
                &gateway_metrics,
                "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
                expected_actor_requests,
            )?;
            assert_prometheus_sample_at_least(
                "merge activation soak gateway",
                &gateway_metrics,
                "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
                6.0,
            )?;
            assert_prometheus_sample_eq(
                "merge activation soak gateway",
                &gateway_metrics,
                "tessera_gateway_client_closes_no_route_total",
                0.0,
            )?;
            assert_prometheus_sample_eq(
                "merge activation soak gateway",
                &gateway_metrics,
                "tessera_gateway_client_closes_upstream_retry_exhausted_total",
                0.0,
            )?;
            assert_prometheus_sample_eq(
                "merge activation soak gateway",
                &gateway_metrics,
                "tessera_gateway_client_closes_ambiguous_upstream_total",
                0.0,
            )?;
            let worker_a_metrics = assert_metrics_endpoint_body_until(
                "merge activation soak worker-a",
                worker_a_metrics_addr,
                &[
                    "tessera_worker_accepted_connections_total",
                    "tessera_worker_relay_connections_total",
                ],
            )?;
            let report_path = write_merge_activation_soak_report(MergeActivationSoakReport {
                operation_id: &operation_id,
                plan_report_path: &plan_report_path,
                gateway_addr,
                gateway_metrics_addr,
                orch_addr,
                worker_a_addr,
                worker_a_metrics_addr,
                parent,
                merged_children: &merged_children,
                owner_worker_id: "worker-a",
                iterations,
                sleep_ms,
                stats,
                gateway_routes: prometheus_sample_value(
                    &gateway_metrics,
                    "tessera_gateway_routes",
                )?,
                gateway_ping_roundtrips: prometheus_sample_value(
                    &gateway_metrics,
                    "tessera_gateway_ping_roundtrip_seconds_count",
                )?,
                gateway_join_roundtrips: prometheus_sample_value(
                    &gateway_metrics,
                    "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
                )?,
                gateway_move_roundtrips: prometheus_sample_value(
                    &gateway_metrics,
                    "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
                )?,
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
                worker_a_relay_connections: prometheus_sample_value(
                    &worker_a_metrics,
                    "tessera_worker_relay_connections_total",
                )?,
                ignored_frames_before_parent_delta: move_observed.ignored_frames,
                remote_delta_frames_before_parent_delta: move_observed.remote_delta_frames,
                remote_snapshot_frames_before_parent_delta: move_observed.remote_snapshot_frames,
            })?;
            let report = read_json_report(&report_path)?;
            validate_merge_activation_soak_report(&report, iterations)?;
            if canonical_merge {
                validate_canonical_merge_activation_smoke_report(&report)?;
            }
            if canonical_merge {
                println!(
                    "canonical merge activation soak: manual canonical same-worker merge stayed converged across {iterations} per-actor parent ping/move iterations, report={}",
                    report_path.display()
                );
            } else {
                println!(
                    "merge activation soak: manual same-worker merge stayed converged across {iterations} per-actor parent ping/move iterations, report={}",
                    report_path.display()
                );
            }
            return Ok(());
        }
        if is_merge_failure_mode(mode) {
            drop(child0_session);
            stack.terminate_named("activation-worker-a")?;
            let failure_error = match assert_gateway_ping_until(gateway_addr, parent, 7_432) {
                Ok(()) => bail!(
                    "merge activation failure smoke expected parent Ping to fail while owner Worker was down"
                ),
                Err(err) => err.to_string(),
            };
            runtime.block_on(wait_for_split_listing(
                &orch_endpoint,
                &[(parent, "worker-a")],
            ))?;
            let gateway_metrics_after_failure = assert_metrics_endpoint_body_until(
                "merge activation failure gateway",
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
                    name: "activation-worker-a",
                    bin: &worker_bin,
                    ready_addr: worker_a_addr,
                    envs: &[
                        ("RUST_LOG", rust_log.as_str()),
                        ("TESSERA_WORKER_ID", "worker-a"),
                        ("TESSERA_WORKER_ADDR", worker_a_addr),
                        ("TESSERA_WORKER_ADVERTISE_ADDR", worker_a_addr),
                        ("TESSERA_WORKER_METRICS_ADDR", worker_a_metrics_addr),
                        ("TESSERA_WORKER_REFRESH_SECS", "1"),
                        ("TESSERA_ORCH_ADDR", orch_addr),
                    ],
                },
            )?;
            runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
            runtime.block_on(wait_for_split_listing(
                &orch_endpoint,
                &[(parent, "worker-a")],
            ))?;
            assert_gateway_ready_routes(gateway_metrics_addr, 1)?;
            assert_gateway_ping_until(gateway_addr, parent, 7_433)?;
            let gateway_metrics_after_recovery = assert_metrics_endpoint_body_until(
                "merge activation recovery gateway",
                gateway_metrics_addr,
                &["tessera_gateway_routes"],
            )?;
            let gateway_routes_after_recovery =
                prometheus_sample_value(&gateway_metrics_after_recovery, "tessera_gateway_routes")?;
            let report_path =
                write_merge_activation_failure_smoke_report(MergeActivationFailureSmokeReport {
                    operation_id: &operation_id,
                    plan_report_path: &plan_report_path,
                    gateway_addr,
                    gateway_metrics_addr,
                    orch_addr,
                    worker_a_addr,
                    parent,
                    merged_children: &merged_children,
                    owner_worker_id: "worker-a",
                    gateway_routes_before_failure: gateway_routes,
                    gateway_routes_after_failure,
                    gateway_routes_after_recovery,
                    failure_error: &failure_error,
                    ignored_frames_before_parent_delta: move_observed.ignored_frames,
                    remote_delta_frames_before_parent_delta: move_observed.remote_delta_frames,
                    remote_snapshot_frames_before_parent_delta: move_observed
                        .remote_snapshot_frames,
                })?;
            let report = read_json_report(&report_path)?;
            validate_merge_activation_failure_smoke_report(&report)?;
            if canonical_merge {
                validate_canonical_merge_activation_smoke_report(&report)?;
                println!(
                    "canonical merge activation failure smoke: post-publish owner outage was detected, canonical parent assignment stayed published, owner Worker restart recovered parent route and fresh Ping, report={}",
                    report_path.display()
                );
            } else {
                println!(
                    "merge activation failure smoke: post-publish owner outage was detected, parent assignment stayed published, owner Worker restart recovered parent route and fresh Ping, report={}",
                    report_path.display()
                );
            }
            return Ok(());
        }
        let report_path = write_merge_activation_smoke_report(MergeActivationSmokeReport {
            operation_id: &operation_id,
            plan_report_path: &plan_report_path,
            gateway_addr,
            gateway_metrics_addr,
            orch_addr,
            worker_a_addr,
            worker_a_metrics_addr,
            parent,
            merged_children: &merged_children,
            owner_worker_id: "worker-a",
            gateway_routes,
            assignment_state_path: is_merge_restart_mode(mode)
                .then_some(assignment_state_path.as_path()),
            orchestrator_restarted: is_merge_restart_mode(mode),
            gateway_routes_after_restart,
            ignored_frames_before_parent_delta: move_observed.ignored_frames,
            remote_delta_frames_before_parent_delta: move_observed.remote_delta_frames,
            remote_snapshot_frames_before_parent_delta: move_observed.remote_snapshot_frames,
        })?;
        if mode == ActivationSmokeMode::MergeRestart {
            println!(
                "merge activation restart smoke: manual same-worker merge survived Orchestrator restart from persisted assignment state, Gateway parent route stayed converged, stable-session parent move succeeded, report={}",
                report_path.display()
            );
        } else if mode == ActivationSmokeMode::CanonicalMergeRestart {
            println!(
                "canonical merge activation restart smoke: manual canonical same-worker merge survived Orchestrator restart from persisted assignment state, Gateway parent route stayed converged, stable-session parent move succeeded, report={}",
                report_path.display()
            );
        } else if mode == ActivationSmokeMode::CanonicalMergeActivation {
            println!(
                "canonical merge activation smoke: manual canonical same-worker merge published parent {:?}, Worker coalesced canonical children, Gateway converged to one parent route, stable-session parent move succeeded, report={}",
                parent,
                report_path.display()
            );
        } else {
            println!(
                "merge activation smoke: manual same-worker merge published parent assignment, Worker coalesced child actors, Gateway converged to one parent route, stable-session parent move succeeded, report={}",
                report_path.display()
            );
        }
        return Ok(());
    }
    if mode == ActivationSmokeMode::LivePlannerMutation {
        let parent = CellId::grid(0, 0, 0);
        let actor = EntityId(7_221);
        let mut session = GatewaySession::connect(gateway_addr)?;
        let joined = session.request(
            parent,
            ClientMsg::Join {
                actor,
                pos: Position { x: 16.0, y: 16.0 },
            },
        )?;
        assert_snapshot_contains(
            "activation live planner mutation parent join",
            joined,
            parent,
            actor,
        )?;
        let worker_metrics = assert_metrics_endpoint_body_until(
            "activation live planner mutation worker-a",
            worker_a_metrics_addr,
            &["tessera_worker_cell_actor_count"],
        )?;
        assert_prometheus_sample_at_least(
            "activation live planner mutation worker-a",
            &worker_metrics,
            "tessera_worker_cell_actor_count{world=\"0\",cx=\"0\",cy=\"0\",depth=\"0\",sub=\"0\"}",
            1.0,
        )?;
        assert_metrics_endpoint_body_until(
            "activation live planner mutation worker-b",
            worker_b_metrics_addr,
            &["tessera_worker_cell_actor_count"],
        )?;
        let live_metrics = vec![
            format!("worker-a={worker_a_metrics_addr}"),
            format!("worker-b={worker_b_metrics_addr}"),
        ];
        let live_policy = LiveMetricsPlanPolicy {
            actor_threshold: 1,
            move_threshold: 1,
            min_pressure_signals: 1,
            cell_age_secs: 60,
        };
        let blocked_path = root
            .join(".dev/reports")
            .join("planner-activation-live-blocked-latest.json");
        let blocked_report_path = execute_planner_activation(PlannerActivationOptions {
            kind: "split",
            orch_addr,
            plan_source: PlannerActivationPlanSource {
                preview_addr: orch_metrics_addr,
                live_worker_metrics: &live_metrics,
                live_policy,
            },
            operation_id: Some(format!(
                "activation-live-planner-blocked-smoke-{}",
                unix_timestamp_secs()
            )),
            mutation: PlannerActivationMutation {
                allow_mutation: false,
                policy_id: None,
                out: Some(&blocked_path),
            },
        })?;
        let blocked_report = read_json_report(&blocked_report_path)?;
        validate_planner_activation_report(&blocked_report, false)?;
        validate_planner_activation_live_metrics_report(&blocked_report)?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(parent, "worker-a")],
        ))?;
        assert_gateway_ready_routes(gateway_metrics_addr, 1)?;

        let published_report_path = execute_planner_activation(PlannerActivationOptions {
            kind: "split",
            orch_addr,
            plan_source: PlannerActivationPlanSource {
                preview_addr: orch_metrics_addr,
                live_worker_metrics: &live_metrics,
                live_policy,
            },
            operation_id: Some(format!(
                "activation-live-planner-mutation-smoke-{}",
                unix_timestamp_secs()
            )),
            mutation: PlannerActivationMutation {
                allow_mutation: true,
                policy_id: Some(PLANNER_MUTATION_POLICY_ID),
                out: Some(&default_planner_activation_path()),
            },
        })?;
        let published_report = read_json_report(&published_report_path)?;
        validate_planner_activation_report(&published_report, true)?;
        validate_planner_activation_live_metrics_report(&published_report)?;
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
        let probe = probe_split_convergence(gateway_addr, 7_620);
        probe.assert_success()?;
        println!(
            "activation live planner mutation smoke: live Worker metrics selected a split plan, default-off policy blocked mutation first, policy id {} then published child routes, blocked_report={}, published_report={}",
            PLANNER_MUTATION_POLICY_ID,
            blocked_report_path.display(),
            published_report_path.display()
        );
        return Ok(());
    }
    if mode == ActivationSmokeMode::LiveMetricsPlan {
        let parent = CellId::grid(0, 0, 0);
        let actor = EntityId(7101);
        let mut session = GatewaySession::connect(gateway_addr)?;
        let joined = session.request(
            parent,
            ClientMsg::Join {
                actor,
                pos: Position { x: 16.0, y: 16.0 },
            },
        )?;
        assert_snapshot_contains("activation live plan parent join", joined, parent, actor)?;
        let worker_metrics = assert_metrics_endpoint_body_until(
            "activation live plan worker-a",
            worker_a_metrics_addr,
            &["tessera_worker_cell_actor_count"],
        )?;
        assert_prometheus_sample_at_least(
            "activation live plan worker-a",
            &worker_metrics,
            "tessera_worker_cell_actor_count{world=\"0\",cx=\"0\",cy=\"0\",depth=\"0\",sub=\"0\"}",
            1.0,
        )?;
        let plan_path = default_split_activation_plan_path();
        let live_metrics = vec![
            format!("worker-a={worker_a_metrics_addr}"),
            format!("worker-b={worker_b_metrics_addr}"),
        ];
        let plan = build_split_activation_plan_from_live_metrics(
            &orch_endpoint,
            &live_metrics,
            LiveMetricsPlanPolicy {
                actor_threshold: 1,
                move_threshold: 1,
                min_pressure_signals: 1,
                cell_age_secs: 60,
            },
            Some(format!(
                "activation-live-plan-smoke-{}",
                unix_timestamp_secs()
            )),
            &[],
        )?;
        if plan.status != "ready" {
            bail!(
                "activation live plan smoke expected ready plan, got status={} reason={}",
                plan.status,
                plan.reason
            );
        }
        if !plan.preview_source.starts_with("live_worker_metrics:") {
            bail!(
                "activation live plan smoke expected live_worker_metrics source, got {}",
                plan.preview_source
            );
        }
        let report_path = write_split_activation_plan_report(&plan, Some(&plan_path))?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(CellId::grid(0, 0, 0), "worker-a")],
        ))?;
        println!(
            "activation live plan smoke: live Worker metrics became a ready operator plan without assignment mutation, report={}",
            report_path.display()
        );
        return Ok(());
    }

    let parent = CellId::grid(0, 0, 0);
    let actor = EntityId(if mode == ActivationSmokeMode::LiveMetricsActivation {
        7201
    } else {
        7001
    });
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
        ActivationSmokeMode::LiveMetricsPlan => "activation-live-plan-smoke",
        ActivationSmokeMode::LiveMetricsActivation => "activation-live-metrics-smoke",
        ActivationSmokeMode::LivePlannerMutation => "activation-live-planner-mutation-smoke",
        ActivationSmokeMode::MergePlan => "merge-plan-smoke",
        ActivationSmokeMode::PlannerMutation => "planner-mutation-smoke",
        ActivationSmokeMode::MergeActivation => "merge-activation-smoke",
        ActivationSmokeMode::CanonicalMergeActivation => "canonical-merge-activation-smoke",
        ActivationSmokeMode::CanonicalMergeRestart => "canonical-merge-activation-restart-smoke",
        ActivationSmokeMode::CanonicalMergePostPublishFailure => {
            "canonical-merge-activation-failure-smoke"
        }
        ActivationSmokeMode::CanonicalMergeSoak { .. } => "canonical-merge-activation-soak",
        ActivationSmokeMode::MergeCrossWorker => "merge-activation-cross-worker-smoke",
        ActivationSmokeMode::MergeRestart => "merge-activation-restart-smoke",
        ActivationSmokeMode::MergePostPublishFailure => "merge-activation-failure-smoke",
        ActivationSmokeMode::MergeSoak { .. } => "merge-activation-soak",
        ActivationSmokeMode::Success => "activation-smoke",
        ActivationSmokeMode::PostPublishFailure => "activation-failure-smoke",
        ActivationSmokeMode::RestartRecovery => "activation-restart-smoke",
        ActivationSmokeMode::Soak { .. } => "activation-soak",
    };
    let mut operation_id = format!("{operation_prefix}-{}", unix_timestamp_secs());
    let mut smoke_targets = vec![
        (0, "worker-a".to_string()),
        (1, "worker-b".to_string()),
        (2, "worker-a".to_string()),
        (3, "worker-b".to_string()),
    ];
    let mut operator_plan_source = None;
    let mut operator_plan_report_path = None;
    if mode == ActivationSmokeMode::LiveMetricsActivation {
        let worker_metrics = assert_metrics_endpoint_body_until(
            "activation live metrics worker-a",
            worker_a_metrics_addr,
            &["tessera_worker_cell_actor_count"],
        )?;
        assert_prometheus_sample_at_least(
            "activation live metrics worker-a",
            &worker_metrics,
            "tessera_worker_cell_actor_count{world=\"0\",cx=\"0\",cy=\"0\",depth=\"0\",sub=\"0\"}",
            1.0,
        )?;
        let live_metrics = vec![
            format!("worker-a={worker_a_metrics_addr}"),
            format!("worker-b={worker_b_metrics_addr}"),
        ];
        let plan = build_split_activation_plan_from_live_metrics(
            &orch_endpoint,
            &live_metrics,
            LiveMetricsPlanPolicy {
                actor_threshold: 1,
                move_threshold: 1,
                min_pressure_signals: 1,
                cell_age_secs: 60,
            },
            Some(operation_id.clone()),
            &[],
        )?;
        if plan.status != "ready" {
            bail!(
                "activation live metrics smoke expected ready plan, got status={} reason={}",
                plan.status,
                plan.reason
            );
        }
        if plan.parent != Some(parent) {
            bail!(
                "activation live metrics smoke expected parent {:?}, got {:?}",
                parent,
                plan.parent
            );
        }
        if !plan.preview_source.starts_with("live_worker_metrics:") {
            bail!(
                "activation live metrics smoke expected live_worker_metrics source, got {}",
                plan.preview_source
            );
        }
        if plan.recommended_targets.len() != 4 {
            bail!(
                "activation live metrics smoke expected four targets, got {:?}",
                plan.recommended_targets
            );
        }
        smoke_targets = plan
            .recommended_targets
            .iter()
            .map(|target| (target.sub, target.worker_id.clone()))
            .collect();
        operation_id = plan.operation_id.clone();
        operator_plan_source = Some(plan.preview_source.clone());
        operator_plan_report_path = Some(write_split_activation_plan_report(
            &plan,
            Some(&default_split_activation_plan_path()),
        )?);
    }
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

    if mode == ActivationSmokeMode::RestartRecovery {
        if !assignment_state_path.exists() {
            bail!(
                "activation restart smoke expected assignment state file at {}",
                assignment_state_path.display()
            );
        }
        drop(session);
        stack.terminate_named("activation-orch")?;
        let restart_orch_envs = vec![
            ("RUST_LOG", rust_log.as_str()),
            ("TESSERA_ORCH_ADDR", orch_addr),
            ("TESSERA_ORCH_METRICS_ADDR", orch_metrics_addr),
            ("TESSERA_ORCH_CONFIG_JSON", orch_config_json.as_str()),
            (
                "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
                assignment_state_path_raw.as_str(),
            ),
        ];
        stack.spawn(
            &root,
            &logs,
            &pids,
            DevProcessSpec {
                name: "activation-orch",
                bin: &orchestrator_bin,
                ready_addr: orch_addr,
                envs: &restart_orch_envs,
            },
        )?;
        runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[
                (activation_child_cell(0), "worker-a"),
                (activation_child_cell(1), "worker-b"),
                (activation_child_cell(2), "worker-a"),
                (activation_child_cell(3), "worker-b"),
            ],
        ))?;

        stack.terminate_named("activation-gateway")?;
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
        assert_gateway_ready_routes(gateway_metrics_addr, 4)?;
        let restart_probe = probe_split_convergence(gateway_addr, 8_300);
        restart_probe.assert_success()?;

        let post_restart_stats =
            run_activation_soak_loop(gateway_addr, 4, Duration::from_millis(10))?;

        let gateway_metrics = assert_metrics_endpoint_body_until(
            "activation restart gateway",
            gateway_metrics_addr,
            &["tessera_gateway_routes"],
        )?;
        let gateway_routes = prometheus_sample_value(&gateway_metrics, "tessera_gateway_routes")?;
        let report_path = write_activation_restart_smoke_report(ActivationRestartSmokeReport {
            operation_id: &operation_id,
            assignment_state_path: &assignment_state_path,
            gateway_addr,
            gateway_metrics_addr,
            orch_addr,
            worker_a_addr,
            worker_b_addr,
            gateway_routes,
            restart_probe: &restart_probe,
            post_restart_stats,
        })?;
        println!(
            "activation restart smoke: published split assignments survived Orchestrator restart with manual activation disabled, Gateway routes reconverged, child traffic and AOI resync succeeded, report={}",
            report_path.display()
        );
        return Ok(());
    }

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
                    ("TESSERA_WORKER_REFRESH_SECS", "1"),
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
        operator_plan_source: operator_plan_source.as_deref(),
        operator_plan_report_path: operator_plan_report_path.as_deref(),
    })?;

    if mode == ActivationSmokeMode::LiveMetricsActivation {
        println!(
            "activation live metrics smoke: live Worker metrics plan was submitted manually, gateway routes converged to 4 children, stable-session post-split move and remote AOI resync succeeded, report={}",
            report_path.display()
        );
    } else {
        println!(
            "activation smoke: manual split published, gateway routes converged to 4 children, source/target workers accepted child traffic, stable-session post-split move and remote AOI resync succeeded, report={}",
            report_path.display()
        );
    }
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

fn multi_depth_activation_parent() -> CellId {
    CellId::leaf(0, -2, 3, 2)
}

#[cfg(test)]
fn merge_child_cell(parent: CellId, sub: u8) -> CellId {
    parent.legacy_shallow_child(sub).unwrap_or(CellId {
        world: parent.world,
        cx: parent.cx,
        cy: parent.cy,
        depth: parent.depth.saturating_add(1),
        sub,
    })
}

fn merge_child_family_candidates(parent: CellId) -> Vec<[CellId; 4]> {
    let mut families = Vec::new();
    if let Some(children) = parent.legacy_shallow_children() {
        families.push(children);
    }
    if parent.depth > 0
        && let Some(children) = parent.canonical_children()
        && !families.iter().any(|existing| existing == &children)
    {
        families.push(children);
    }
    families
}

fn merge_child_cells_from_listing(
    parent: CellId,
    listing: &AssignmentListing,
) -> Result<Vec<CellId>> {
    for children in merge_child_family_candidates(parent) {
        let mut has_assigned_child = false;
        for child in &children {
            if !owners_for_listing_cell(listing, *child)?.is_empty() {
                has_assigned_child = true;
                break;
            }
        }
        if has_assigned_child {
            return Ok(children.to_vec());
        }
    }
    bail!(
        "merge candidate parent depth={},sub={} has no assigned legacy shallow or canonical child family",
        parent.depth,
        parent.sub
    )
}

fn worker_cell_actor_count_metric(cell: CellId) -> String {
    format!(
        "tessera_worker_cell_actor_count{{world=\"{}\",cx=\"{}\",cy=\"{}\",depth=\"{}\",sub=\"{}\"}}",
        cell.world, cell.cx, cell.cy, cell.depth, cell.sub
    )
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
    let deadline = Instant::now() + Duration::from_secs(10);
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
            Err(err) if is_temporary_io_error(&err) => {
                if Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                bail!(
                    "activation child move: timed out waiting for correlated move and AOI snapshot after temporary read error: {err}"
                );
            }
            Err(err) => return Err(err),
        };
        if reply_is_correlated_delta_for_actor(&reply, actor)
            || reply_is_delta_for_actor_on_cell(&reply, move_cell, actor)
        {
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

fn reply_is_delta_for_actor_on_cell(
    envelope: &ServerEnvelope,
    expected_cell: CellId,
    actor: EntityId,
) -> bool {
    if envelope.cell != expected_cell {
        return false;
    }
    matches!(
        &envelope.payload,
        ServerMsg::Delta { cell, moved }
            if *cell == expected_cell && moved.iter().any(|moved| moved.id == actor)
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
    let cells = [
        activation_child_cell(0),
        activation_child_cell(1),
        activation_child_cell(2),
        activation_child_cell(3),
    ];
    run_cell_activation_soak_loop(gateway_addr, &cells, iterations, sleep)
}

fn run_cell_activation_soak_loop(
    gateway_addr: &str,
    cells: &[CellId],
    iterations: u32,
    sleep: Duration,
) -> Result<ActivationSoakStats> {
    run_activation_soak_loop_inner(gateway_addr, cells, iterations, sleep, true)
}

fn run_parent_activation_soak_loop(
    gateway_addr: &str,
    cell: CellId,
    iterations: u32,
    sleep: Duration,
) -> Result<ActivationSoakStats> {
    let cells = [cell, cell, cell, cell];
    run_activation_soak_loop_inner(gateway_addr, &cells, iterations, sleep, false)
}

fn run_activation_soak_loop_inner(
    gateway_addr: &str,
    cells: &[CellId],
    iterations: u32,
    sleep: Duration,
    require_remote_aoi: bool,
) -> Result<ActivationSoakStats> {
    if cells.is_empty() {
        bail!("activation soak requires at least one child cell");
    }
    let mut sessions = Vec::new();
    for (idx, cell) in cells.iter().enumerate() {
        let position_idx =
            u8::try_from(idx).with_context(|| format!("activation soak cell index {idx}"))?;
        let actor = EntityId(7_300 + idx as u64);
        let session = open_gateway_join_until_snapshot(
            gateway_addr,
            *cell,
            actor,
            activation_soak_position(position_idx),
        )?;
        sessions.push(ActivationSoakSession {
            cell: *cell,
            actor,
            session,
        });
    }

    let mut stats = ActivationSoakStats::default();
    for iteration in 0..iterations {
        for (idx, session) in sessions.iter_mut().enumerate() {
            let ts = 9_000 + u64::from(iteration) * 10 + idx as u64;
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

    if require_remote_aoi {
        stats.assert_remote_aoi_observed()?;
    }
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
        if reply_is_correlated_delta_for_actor(&reply, actor)
            || reply_is_delta_for_actor_on_cell(&reply, cell, actor)
        {
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
    source_worker_id: String,
    source_worker_service: String,
    target_worker_deploy: String,
    target_worker_service: String,
    target_worker_id: String,
    require_target_worker: bool,
    require_assignment_state_storage: bool,
    argocd_namespace: String,
    argocd_app: String,
    skip_argocd_check: bool,
    expected_image: Option<String>,
    local_orch_port: u16,
    local_orch_metrics_port: u16,
    local_gateway_port: u16,
    local_gateway_metrics_port: u16,
    local_source_worker_metrics_port: u16,
    local_target_worker_metrics_port: u16,
    use_live_worker_metrics: bool,
    live_actor_threshold: u64,
    live_move_threshold: u64,
    live_min_pressure_signals: u8,
    live_cell_age_secs: u64,
    operation_id: Option<String>,
    targets: Vec<String>,
    allow_activation: bool,
    with_failure: bool,
    allow_scale: bool,
    with_restart: bool,
    allow_rollout_restart: bool,
    expected_assignment_state_path: String,
    out: Option<PathBuf>,
}

#[derive(Debug)]
struct K8sMultiDepthActivationSmokeOptions {
    namespace: String,
    context: Option<String>,
    orch_service: String,
    orch_deploy: String,
    gateway_service: String,
    gateway_deploy: String,
    source_worker_deploy: String,
    source_worker_id: String,
    target_worker_deploy: String,
    target_worker_id: String,
    parent: CellId,
    target_cells: Vec<String>,
    argocd_namespace: String,
    argocd_app: String,
    skip_argocd_check: bool,
    expected_image: Option<String>,
    local_orch_port: u16,
    local_gateway_port: u16,
    local_gateway_metrics_port: u16,
    operation_id: Option<String>,
    allow_activation: bool,
    with_failure: bool,
    allow_scale: bool,
    with_restart: bool,
    allow_rollout_restart: bool,
    with_soak: bool,
    soak_iterations: u32,
    soak_sleep_ms: u64,
    out: Option<PathBuf>,
}

fn run_k8s_activation_smoke(options: K8sActivationSmokeOptions) -> Result<()> {
    if options.with_failure && !options.allow_scale {
        bail!(
            "internal k8s failure smoke scales a deployment; pass --allow-scale with --with-failure"
        );
    }
    if options.with_restart && !options.allow_rollout_restart {
        bail!(
            "internal k8s restart smoke restarts a deployment; pass --allow-rollout-restart with --with-restart"
        );
    }
    if options.with_restart && !options.allow_activation {
        bail!(
            "internal k8s restart smoke needs a published split; pass --allow-activation with --with-restart"
        );
    }
    if options.use_live_worker_metrics && options.live_min_pressure_signals == 0 {
        bail!("internal k8s live metrics plan requires --live-min-pressure-signals > 0");
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
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                restart_storage_required: options.require_assignment_state_storage
                    || options.with_restart,
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
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                restart_storage_required: options.require_assignment_state_storage
                    || options.with_restart,
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
    let local_source_worker_metrics_addr =
        format!("127.0.0.1:{}", options.local_source_worker_metrics_port);
    let local_target_worker_metrics_addr =
        format!("127.0.0.1:{}", options.local_target_worker_metrics_port);
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
    if options.use_live_worker_metrics {
        forwards.spawn(
            &context,
            &options.namespace,
            &options.source_worker_service,
            "source-worker-metrics",
            &[(options.local_source_worker_metrics_port, 5100)],
        )?;
        forwards.spawn(
            &context,
            &options.namespace,
            &options.target_worker_service,
            "target-worker-metrics",
            &[(options.local_target_worker_metrics_port, 5100)],
        )?;
    }

    let orch_endpoint = grpc_endpoint(&local_orch_addr);
    let plan = if options.use_live_worker_metrics {
        let live_worker_metrics = vec![
            format!(
                "{}={}",
                options.source_worker_id, local_source_worker_metrics_addr
            ),
            format!(
                "{}={}",
                options.target_worker_id, local_target_worker_metrics_addr
            ),
        ];
        build_split_activation_plan_from_live_metrics(
            &orch_endpoint,
            &live_worker_metrics,
            LiveMetricsPlanPolicy {
                actor_threshold: options.live_actor_threshold,
                move_threshold: options.live_move_threshold,
                min_pressure_signals: options.live_min_pressure_signals,
                cell_age_secs: options.live_cell_age_secs,
            },
            Some(operation_id.clone()),
            &options.targets,
        )?
    } else {
        build_split_activation_plan(
            &orch_endpoint,
            &local_orch_metrics_addr,
            Some(operation_id.clone()),
            &options.targets,
        )?
    };
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
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                restart_storage_required: options.require_assignment_state_storage
                    || options.with_restart,
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
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                restart_storage_required: options.require_assignment_state_storage
                    || options.with_restart,
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
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.target_worker_service,
            0,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_pods(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            0,
            Duration::from_secs(120),
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
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.target_worker_service,
            original_replicas,
            Duration::from_secs(120),
        )?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
        let observed_recovery = probe_split_convergence(&local_gateway_addr, 12_200);
        observed_recovery.assert_success()?;

        failure_assert?;
        failure_probe = Some(observed_failure);
        recovery_probe = Some(observed_recovery);
    }

    let mut restart_probe = None;
    let mut post_restart_stats = None;
    let mut gateway_routes_after_restart = None;
    if options.with_restart {
        forwards.stop_all();
        kubectl_rollout_restart_deploy(&context, &options.namespace, &options.orch_deploy)?;
        kubectl_rollout_status(
            &context,
            &options.namespace,
            &options.orch_deploy,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.orch_deploy,
            1,
        )?;

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

        runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
        runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
        let observed_restart = probe_split_convergence(&local_gateway_addr, 12_300);
        observed_restart.assert_success()?;
        let stats = run_activation_soak_loop(&local_gateway_addr, 4, Duration::from_millis(10))?;
        gateway_routes_after_restart = Some(4);
        restart_probe = Some(observed_restart);
        post_restart_stats = Some(stats);
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
            assignment_state_storage: preflight.assignment_state_storage.as_ref(),
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
            with_restart: options.with_restart,
            restart_probe: restart_probe.as_ref(),
            post_restart_stats,
            gateway_routes_after_restart,
        },
        options.out.as_deref(),
    )?;
    println!(
        "internal k8s activation smoke: split published and converged{}; report={}",
        match (options.with_failure, options.with_restart) {
            (true, true) => ", failure/recovery and orchestrator restart recovery verified",
            (true, false) => ", failure/recovery path verified",
            (false, true) => ", orchestrator restart recovery verified",
            (false, false) => "",
        },
        report_path.display()
    );
    Ok(())
}

fn run_k8s_multi_depth_activation_smoke(
    options: K8sMultiDepthActivationSmokeOptions,
) -> Result<()> {
    if options.with_failure && !options.allow_scale {
        bail!(
            "internal k8s multi-depth failure smoke scales a deployment; pass --allow-scale with --with-failure"
        );
    }
    if options.with_restart && !options.allow_rollout_restart {
        bail!(
            "internal k8s multi-depth restart smoke restarts a deployment; pass --allow-rollout-restart with --with-restart"
        );
    }
    if (options.with_failure || options.with_restart || options.with_soak)
        && !options.allow_activation
    {
        bail!(
            "internal k8s multi-depth completion smoke needs a published split; pass --allow-activation"
        );
    }
    if options.with_soak && options.soak_iterations == 0 {
        bail!("internal k8s multi-depth soak requires --soak-iterations > 0");
    }

    let context = resolve_kube_context(options.context.as_deref())?;
    let operation_id = options
        .operation_id
        .clone()
        .unwrap_or_else(|| format!("internal-multi-depth-smoke-{}", unix_timestamp_secs()));
    let preflight_result = k8s_multi_depth_activation_preflight(&context, &options);
    let preflight = preflight_result.preflight;
    if !preflight_result.errors.is_empty() {
        let reason = format!(
            "internal k8s multi-depth activation preflight failed: {}",
            preflight_result.errors.join("; ")
        );
        let report_path = write_internal_k8s_multi_depth_activation_report(
            InternalK8sMultiDepthActivationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                source_worker_deploy: &options.source_worker_deploy,
                source_worker_id: &options.source_worker_id,
                target_worker_deploy: &options.target_worker_deploy,
                target_worker_id: &options.target_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan: None,
                stage: "blocked_before_plan",
                reason: &reason,
                preflight_errors: &preflight_result.errors,
                completion: InternalK8sMultiDepthCompletion::default(),
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
        let report_path = write_internal_k8s_multi_depth_activation_report(
            InternalK8sMultiDepthActivationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                source_worker_deploy: &options.source_worker_deploy,
                source_worker_id: &options.source_worker_id,
                target_worker_deploy: &options.target_worker_deploy,
                target_worker_id: &options.target_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan: None,
                stage: "blocked_before_plan",
                reason: &reason,
                preflight_errors: &[],
                completion: InternalK8sMultiDepthCompletion::default(),
            },
            options.out.as_deref(),
        )?;
        bail!("{reason}; internal report={}", report_path.display());
    }

    let local_orch_addr = format!("127.0.0.1:{}", options.local_orch_port);
    let local_gateway_addr = format!("127.0.0.1:{}", options.local_gateway_port);
    let local_gateway_metrics_addr = format!("127.0.0.1:{}", options.local_gateway_metrics_port);
    let mut forwards = ManagedK8sPortForwards::default();
    forwards.spawn(
        &context,
        &options.namespace,
        &options.orch_service,
        "multi-depth-orchestrator",
        &[(options.local_orch_port, 6000)],
    )?;
    if options.allow_activation {
        forwards.spawn(
            &context,
            &options.namespace,
            &options.gateway_service,
            "multi-depth-gateway",
            &[
                (options.local_gateway_port, 4000),
                (options.local_gateway_metrics_port, 4100),
            ],
        )?;
    }

    let orch_endpoint = grpc_endpoint(&local_orch_addr);
    let runtime = tokio::runtime::Runtime::new()?;
    let (health, listing) = runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;
    let plan = build_internal_multi_depth_activation_plan(
        &orch_endpoint,
        health,
        listing,
        operation_id.clone(),
        &options,
    )?;
    let stage = if plan.status == "ready" {
        "planned_without_activation"
    } else {
        "blocked_before_activation"
    };
    let report_path = write_internal_k8s_multi_depth_activation_report(
        InternalK8sMultiDepthActivationReport {
            context: &context,
            namespace: &options.namespace,
            orch_service: &options.orch_service,
            gateway_service: &options.gateway_service,
            source_worker_deploy: &options.source_worker_deploy,
            source_worker_id: &options.source_worker_id,
            target_worker_deploy: &options.target_worker_deploy,
            target_worker_id: &options.target_worker_id,
            argocd_namespace: &options.argocd_namespace,
            argocd_app: &options.argocd_app,
            argocd_status: preflight.argocd_status.as_ref(),
            deployment_images: &preflight.deployment_images,
            expected_image: options.expected_image.as_deref(),
            operation_id: &operation_id,
            plan: Some(&plan),
            stage,
            reason: &plan.reason,
            preflight_errors: &[],
            completion: InternalK8sMultiDepthCompletion::default(),
        },
        options.out.as_deref(),
    )?;
    if plan.status != "ready" {
        bail!(
            "internal k8s multi-depth activation smoke blocked before mutation: status={} reason={}; internal report={}",
            plan.status,
            plan.reason,
            report_path.display()
        );
    }

    if !options.allow_activation {
        println!(
            "internal k8s multi-depth activation readiness stopped before mutation; report={}",
            report_path.display()
        );
        return Ok(());
    }

    let targets = plan
        .targets
        .iter()
        .map(|target| (target.cell, target.worker_id.clone()))
        .collect::<Vec<_>>();
    let expected_listing = targets
        .iter()
        .map(|(cell, worker_id)| (*cell, worker_id.as_str()))
        .collect::<Vec<_>>();
    assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
    let response = runtime.block_on(submit_split_activation_with_child_cells(
        &orch_endpoint,
        operation_id.clone(),
        plan.parent,
        &targets,
    ))?;
    assert_split_activation_published(&response)?;
    runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
    assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
    let success_probe =
        probe_multi_depth_convergence(&local_gateway_addr, &expected_listing, 13_100);
    success_probe.assert_success()?;

    let mut completion = InternalK8sMultiDepthCompletion {
        activation_mutated: true,
        activation_allowed: true,
        multi_depth_published: true,
        ..InternalK8sMultiDepthCompletion::default()
    };

    if options.with_failure {
        let expected_failed_cells = plan
            .targets
            .iter()
            .filter(|target| target.worker_id == options.target_worker_id)
            .map(|target| target.cell)
            .collect::<Vec<_>>();
        if expected_failed_cells.is_empty() {
            bail!(
                "internal k8s multi-depth failure smoke expected at least one child owned by --target-worker-id {}",
                options.target_worker_id
            );
        }
        let original_replicas = kubectl_deploy_spec_replicas(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
        )?;
        if original_replicas == 0 {
            bail!(
                "target deployment {} already has 0 replicas; cannot run multi-depth failure/recovery smoke",
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
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            0,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_pods(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            0,
            Duration::from_secs(120),
        )?;
        let observed_failure =
            probe_multi_depth_convergence(&local_gateway_addr, &expected_listing, 13_200);
        let failure_assert = observed_failure.assert_failed_cells_only(&expected_failed_cells);
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
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.target_worker_deploy,
            original_replicas,
            Duration::from_secs(120),
        )?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
        let observed_recovery =
            probe_multi_depth_convergence(&local_gateway_addr, &expected_listing, 13_300);
        observed_recovery.assert_success()?;
        failure_assert?;
        completion.post_publish_failure_smoke_ran = true;
        completion.target_worker_restart_recovered_convergence = true;
    }

    if options.with_restart {
        forwards.stop_all();
        kubectl_rollout_restart_deploy(&context, &options.namespace, &options.orch_deploy)?;
        kubectl_rollout_status(
            &context,
            &options.namespace,
            &options.orch_deploy,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.orch_deploy,
            1,
        )?;
        forwards.spawn(
            &context,
            &options.namespace,
            &options.orch_service,
            "multi-depth-orchestrator",
            &[(options.local_orch_port, 6000)],
        )?;
        forwards.spawn(
            &context,
            &options.namespace,
            &options.gateway_service,
            "multi-depth-gateway",
            &[
                (options.local_gateway_port, 4000),
                (options.local_gateway_metrics_port, 4100),
            ],
        )?;
        runtime.block_on(wait_for_orchestrator_registered(
            &orch_endpoint,
            plan.health.registered_workers.max(1),
        ))?;
        runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
        let restart_probe =
            probe_multi_depth_convergence(&local_gateway_addr, &expected_listing, 13_400);
        restart_probe.assert_success()?;
        completion.orchestrator_restart_smoke_ran = true;
    }

    if options.with_soak {
        let cells = plan
            .targets
            .iter()
            .map(|target| target.cell)
            .collect::<Vec<_>>();
        let stats = run_cell_activation_soak_loop(
            &local_gateway_addr,
            &cells,
            options.soak_iterations,
            Duration::from_millis(options.soak_sleep_ms),
        )?;
        let min_total = u64::from(options.soak_iterations) * cells.len() as u64;
        if stats.pings_ok < min_total || stats.moves_ok < min_total {
            bail!(
                "internal multi-depth soak expected at least {min_total} pings/moves, got pings={} moves={}",
                stats.pings_ok,
                stats.moves_ok
            );
        }
        runtime.block_on(wait_for_split_listing(&orch_endpoint, &expected_listing))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
        completion.load_soak_ran = true;
    }

    let report_path = write_internal_k8s_multi_depth_activation_report(
        InternalK8sMultiDepthActivationReport {
            context: &context,
            namespace: &options.namespace,
            orch_service: &options.orch_service,
            gateway_service: &options.gateway_service,
            source_worker_deploy: &options.source_worker_deploy,
            source_worker_id: &options.source_worker_id,
            target_worker_deploy: &options.target_worker_deploy,
            target_worker_id: &options.target_worker_id,
            argocd_namespace: &options.argocd_namespace,
            argocd_app: &options.argocd_app,
            argocd_status: preflight.argocd_status.as_ref(),
            deployment_images: &preflight.deployment_images,
            expected_image: options.expected_image.as_deref(),
            operation_id: &operation_id,
            plan: Some(&plan),
            stage: "published",
            reason: "canonical multi-depth activation published through guarded internal helper",
            preflight_errors: &[],
            completion,
        },
        options.out.as_deref(),
    )?;
    println!(
        "internal k8s multi-depth activation smoke published and converged{}{}{}; report={}",
        if options.with_failure {
            ", failure/recovery verified"
        } else {
            ""
        },
        if options.with_restart {
            ", orchestrator restart recovery verified"
        } else {
            ""
        },
        if options.with_soak {
            ", load/soak verified"
        } else {
            ""
        },
        report_path.display()
    );
    Ok(())
}

#[derive(Debug)]
struct K8sMergeActivationSmokeOptions {
    namespace: String,
    context: Option<String>,
    orch_service: String,
    orch_deploy: String,
    gateway_service: String,
    gateway_deploy: String,
    owner_worker_deploy: String,
    owner_worker_id: String,
    argocd_namespace: String,
    argocd_app: String,
    skip_argocd_check: bool,
    expected_image: Option<String>,
    local_orch_port: u16,
    local_orch_metrics_port: u16,
    local_gateway_port: u16,
    local_gateway_metrics_port: u16,
    operation_id: Option<String>,
    allow_activation: bool,
    with_failure: bool,
    allow_scale: bool,
    with_restart: bool,
    allow_rollout_restart: bool,
    with_soak: bool,
    soak_iterations: u32,
    soak_sleep_ms: u64,
    out: Option<PathBuf>,
}

fn run_k8s_merge_activation_smoke(options: K8sMergeActivationSmokeOptions) -> Result<()> {
    if options.with_failure && !options.allow_scale {
        bail!(
            "internal k8s merge failure smoke scales a deployment; pass --allow-scale with --with-failure"
        );
    }
    if options.with_restart && !options.allow_rollout_restart {
        bail!(
            "internal k8s merge restart smoke restarts a deployment; pass --allow-rollout-restart with --with-restart"
        );
    }
    if (options.with_failure || options.with_restart || options.with_soak)
        && !options.allow_activation
    {
        bail!(
            "internal k8s merge completion smoke needs a published merge; pass --allow-activation"
        );
    }
    let context = resolve_kube_context(options.context.as_deref())?;
    let operation_id = options
        .operation_id
        .clone()
        .unwrap_or_else(|| format!("internal-merge-smoke-{}", unix_timestamp_secs()));
    let preflight_result = k8s_merge_activation_preflight(&context, &options);
    let preflight = preflight_result.preflight;
    if !preflight_result.errors.is_empty() {
        let reason = format!(
            "internal k8s merge activation preflight failed: {}",
            preflight_result.errors.join("; ")
        );
        let report_path = write_internal_k8s_merge_activation_report(
            InternalK8sMergeActivationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: None,
                plan: None,
                stage: "blocked_before_plan",
                reason: &reason,
                preflight_errors: &preflight_result.errors,
                completion: InternalK8sMergeCompletion::default(),
            },
            options.out.as_deref(),
        )?;
        bail!("{reason}; internal merge report={}", report_path.display());
    }
    let image_validation = options.expected_image.as_ref().map(|expected_image| {
        validate_k8s_deployment_images(&preflight.deployment_images, expected_image)
    });
    if let Some(Err(err)) = image_validation {
        let reason = err.to_string();
        let report_path = write_internal_k8s_merge_activation_report(
            InternalK8sMergeActivationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: None,
                plan: None,
                stage: "blocked_before_plan",
                reason: &reason,
                preflight_errors: &[],
                completion: InternalK8sMergeCompletion::default(),
            },
            options.out.as_deref(),
        )?;
        bail!("{reason}; internal merge report={}", report_path.display());
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
    if options.allow_activation {
        forwards.spawn(
            &context,
            &options.namespace,
            &options.gateway_service,
            "merge-gateway",
            &[
                (options.local_gateway_port, 4000),
                (options.local_gateway_metrics_port, 4100),
            ],
        )?;
    }

    let orch_endpoint = grpc_endpoint(&local_orch_addr);
    let plan = build_merge_activation_plan(
        &orch_endpoint,
        &local_orch_metrics_addr,
        Some(operation_id.clone()),
    )?;
    let plan_report_path =
        write_merge_activation_plan_report(&plan, Some(&default_merge_activation_plan_path()))?;
    if plan.status != "ready" {
        let report_path = write_internal_k8s_merge_activation_report(
            InternalK8sMergeActivationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: Some(&plan_report_path),
                plan: Some(&plan),
                stage: "blocked_before_activation",
                reason: plan.reason.as_str(),
                preflight_errors: &[],
                completion: InternalK8sMergeCompletion::default(),
            },
            options.out.as_deref(),
        )?;
        bail!(
            "internal k8s merge activation smoke blocked before mutation: status={} reason={}; plan report={}; internal report={}",
            plan.status,
            plan.reason,
            plan_report_path.display(),
            report_path.display()
        );
    }

    if !options.allow_activation {
        let report_path = write_internal_k8s_merge_activation_report(
            InternalK8sMergeActivationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                expected_image: options.expected_image.as_deref(),
                operation_id: &operation_id,
                plan_report_path: Some(&plan_report_path),
                plan: Some(&plan),
                stage: "planned_without_activation",
                reason: "merge plan is ready; internal helper stopped before mutation because --allow-activation was not provided",
                preflight_errors: &[],
                completion: InternalK8sMergeCompletion::default(),
            },
            options.out.as_deref(),
        )?;
        println!(
            "internal k8s merge activation smoke stopped before mutation; plan is ready, plan report={}, internal report={}",
            plan_report_path.display(),
            report_path.display()
        );
        return Ok(());
    }

    let parent = plan
        .parent
        .ok_or_else(|| anyhow::anyhow!("ready merge plan had no parent cell"))?;
    let owner_worker_id = plan
        .owner_worker_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("ready merge plan had no owner worker id"))?;
    if owner_worker_id != options.owner_worker_id {
        bail!(
            "ready merge plan owner {owner_worker_id} did not match --owner-worker-id {}",
            options.owner_worker_id
        );
    }
    let runtime = tokio::runtime::Runtime::new()?;
    let response = runtime.block_on(submit_merge_activation(
        &orch_endpoint,
        operation_id.clone(),
        parent,
        owner_worker_id.to_string(),
    ))?;
    assert_merge_activation_published(&response)?;
    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(parent, owner_worker_id)],
    ))?;
    assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(&local_gateway_addr, parent, 13_000)?;

    let mut completion = InternalK8sMergeCompletion {
        activation_mutated: true,
        activation_allowed: true,
        merge_published: true,
        ..InternalK8sMergeCompletion::default()
    };

    if options.with_failure {
        let original_replicas = kubectl_deploy_spec_replicas(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
        )?;
        if original_replicas == 0 {
            bail!(
                "owner deployment {} already has 0 replicas; cannot run merge failure/recovery smoke",
                options.owner_worker_deploy
            );
        }
        kubectl_scale_deploy(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            0,
        )?;
        let mut restore = K8sScaleRestore::new(
            context.clone(),
            options.namespace.clone(),
            options.owner_worker_deploy.clone(),
            original_replicas,
        );
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            0,
        )?;
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            0,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_pods(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            0,
            Duration::from_secs(120),
        )?;
        if assert_gateway_ping_until(&local_gateway_addr, parent, 13_010).is_ok() {
            bail!(
                "internal merge failure smoke expected parent Ping to fail while owner Worker was scaled down"
            );
        }
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(parent, owner_worker_id)],
        ))?;
        kubectl_scale_deploy(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            original_replicas,
        )?;
        restore.disarm();
        kubectl_rollout_status(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            original_replicas,
        )?;
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            original_replicas,
            Duration::from_secs(120),
        )?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
        assert_gateway_ping_until(&local_gateway_addr, parent, 13_020)?;
        completion.post_publish_failure_smoke_ran = true;
        completion.owner_worker_restart_recovered_convergence = true;
    }

    if options.with_restart {
        forwards.stop_all();
        kubectl_rollout_restart_deploy(&context, &options.namespace, &options.orch_deploy)?;
        kubectl_rollout_status(
            &context,
            &options.namespace,
            &options.orch_deploy,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.orch_deploy,
            1,
        )?;
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
            "merge-gateway",
            &[
                (options.local_gateway_port, 4000),
                (options.local_gateway_metrics_port, 4100),
            ],
        )?;
        runtime.block_on(wait_for_orchestrator_registered(
            &orch_endpoint,
            plan.health.registered_workers.max(1),
        ))?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(parent, owner_worker_id)],
        ))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
        assert_gateway_ping_until(&local_gateway_addr, parent, 13_030)?;
        completion.orchestrator_restart_smoke_ran = true;
    }

    if options.with_soak {
        let stats = run_parent_activation_soak_loop(
            &local_gateway_addr,
            parent,
            options.soak_iterations,
            Duration::from_millis(options.soak_sleep_ms),
        )?;
        let min_total = u64::from(options.soak_iterations) * 4;
        if stats.pings_ok < min_total || stats.moves_ok < min_total {
            bail!(
                "internal merge soak expected at least {min_total} pings/moves, got pings={} moves={}",
                stats.pings_ok,
                stats.moves_ok
            );
        }
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(parent, owner_worker_id)],
        ))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
        completion.load_soak_ran = true;
    }

    let report_path = write_internal_k8s_merge_activation_report(
        InternalK8sMergeActivationReport {
            context: &context,
            namespace: &options.namespace,
            orch_service: &options.orch_service,
            gateway_service: &options.gateway_service,
            owner_worker_deploy: &options.owner_worker_deploy,
            owner_worker_id: &options.owner_worker_id,
            argocd_namespace: &options.argocd_namespace,
            argocd_app: &options.argocd_app,
            argocd_status: preflight.argocd_status.as_ref(),
            deployment_images: &preflight.deployment_images,
            expected_image: options.expected_image.as_deref(),
            operation_id: &operation_id,
            plan_report_path: Some(&plan_report_path),
            plan: Some(&plan),
            stage: "published",
            reason: "merge activation published through guarded internal helper",
            preflight_errors: &[],
            completion,
        },
        options.out.as_deref(),
    )?;
    println!(
        "internal k8s merge activation smoke published and converged{}{}{}; plan report={}, internal report={}",
        if options.with_failure {
            ", failure/recovery verified"
        } else {
            ""
        },
        if options.with_restart {
            ", orchestrator restart recovery verified"
        } else {
            ""
        },
        if options.with_soak {
            ", load/soak verified"
        } else {
            ""
        },
        plan_report_path.display(),
        report_path.display()
    );
    Ok(())
}

#[derive(Debug)]
struct K8sOperationSmokeOptions {
    namespace: String,
    context: Option<String>,
    orch_service: String,
    orch_deploy: String,
    gateway_service: String,
    gateway_deploy: String,
    owner_worker_deploy: String,
    owner_worker_service: String,
    owner_worker_id: String,
    argocd_namespace: String,
    argocd_app: String,
    skip_argocd_check: bool,
    expected_image: Option<String>,
    local_orch_port: u16,
    local_orch_metrics_port: u16,
    local_gateway_port: u16,
    local_gateway_metrics_port: u16,
    local_owner_worker_metrics_port: u16,
    allow_execution: bool,
    with_soak: bool,
    with_failure: bool,
    allow_scale: bool,
    with_restart: bool,
    allow_rollout_restart: bool,
    expected_assignment_state_path: String,
    soak_iterations: u32,
    soak_sleep_ms: u64,
    out: Option<PathBuf>,
}

fn run_k8s_operation_smoke(options: K8sOperationSmokeOptions) -> Result<()> {
    if options.with_soak && !options.allow_execution {
        bail!("internal k8s P7 operation soak needs a published execution; pass --allow-execution");
    }
    if options.with_failure && !options.allow_execution {
        bail!(
            "internal k8s P7 operation failure/recovery needs a published execution; pass --allow-execution"
        );
    }
    if options.with_failure && !options.allow_scale {
        bail!(
            "internal k8s P7 operation failure/recovery scales the owner Worker; pass --allow-scale"
        );
    }
    if options.with_failure && options.with_soak {
        bail!(
            "internal k8s P7 operation --with-failure and --with-soak are separate evidence gates; run them separately"
        );
    }
    if options.with_restart && !options.allow_execution {
        bail!(
            "internal k8s P7 operation restart needs a published execution; pass --allow-execution"
        );
    }
    if options.with_restart && !options.allow_rollout_restart {
        bail!(
            "internal k8s P7 operation restart rolls the Orchestrator deployment; pass --allow-rollout-restart"
        );
    }
    if options.with_restart && (options.with_failure || options.with_soak) {
        bail!(
            "internal k8s P7 operation restart, failure, and soak are separate evidence gates; run them separately"
        );
    }
    let context = resolve_kube_context(options.context.as_deref())?;
    let preflight_result = k8s_operation_preflight(&context, &options);
    let preflight = preflight_result.preflight;
    if !preflight_result.errors.is_empty() {
        let reason = format!(
            "internal k8s P7 operation preflight failed: {}",
            preflight_result.errors.join("; ")
        );
        let report_path = write_internal_k8s_operation_report(
            InternalK8sOperationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_service: &options.owner_worker_service,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                expected_image: options.expected_image.as_deref(),
                stage: "blocked_before_proposal",
                reason: &reason,
                preflight_errors: &preflight_result.errors,
                operation: None,
                proposal_response: None,
                approval_response: None,
                execution_response: None,
                observation_response: None,
                ledger_snapshot: None,
                ledger_summary: None,
                orchestrator: None,
                gateway: None,
                worker: None,
                soak: None,
                failure: None,
                completion: InternalK8sOperationCompletion::default(),
            },
            options.out.as_deref(),
        )?;
        bail!(
            "{reason}; internal P7 operation report={}",
            report_path.display()
        );
    }
    if let Some(expected_image) = options.expected_image.as_deref()
        && let Err(err) =
            validate_k8s_deployment_images(&preflight.deployment_images, expected_image)
    {
        let reason = err.to_string();
        let report_path = write_internal_k8s_operation_report(
            InternalK8sOperationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_service: &options.owner_worker_service,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                expected_image: options.expected_image.as_deref(),
                stage: "blocked_before_proposal",
                reason: &reason,
                preflight_errors: &[],
                operation: None,
                proposal_response: None,
                approval_response: None,
                execution_response: None,
                observation_response: None,
                ledger_snapshot: None,
                ledger_summary: None,
                orchestrator: None,
                gateway: None,
                worker: None,
                soak: None,
                failure: None,
                completion: InternalK8sOperationCompletion::default(),
            },
            options.out.as_deref(),
        )?;
        bail!(
            "{reason}; internal P7 operation report={}",
            report_path.display()
        );
    }

    let local_orch_addr = format!("127.0.0.1:{}", options.local_orch_port);
    let local_orch_metrics_addr = format!("127.0.0.1:{}", options.local_orch_metrics_port);
    let local_gateway_addr = format!("127.0.0.1:{}", options.local_gateway_port);
    let local_gateway_metrics_addr = format!("127.0.0.1:{}", options.local_gateway_metrics_port);
    let local_owner_worker_metrics_addr =
        format!("127.0.0.1:{}", options.local_owner_worker_metrics_port);
    let mut forwards = ManagedK8sPortForwards::default();
    forwards.spawn(
        &context,
        &options.namespace,
        &options.orch_service,
        "p7-operation-orchestrator",
        &[
            (options.local_orch_port, 6000),
            (options.local_orch_metrics_port, 6100),
        ],
    )?;
    if options.allow_execution {
        forwards.spawn(
            &context,
            &options.namespace,
            &options.gateway_service,
            "p7-operation-gateway",
            &[
                (options.local_gateway_port, 4000),
                (options.local_gateway_metrics_port, 4100),
            ],
        )?;
        forwards.spawn(
            &context,
            &options.namespace,
            &options.owner_worker_service,
            "p7-operation-owner-worker",
            &[(options.local_owner_worker_metrics_port, 5100)],
        )?;
    }

    let runtime = tokio::runtime::Runtime::new()?;
    let orch_endpoint = grpc_endpoint(&local_orch_addr);
    let (before_health, before_listing) = match runtime
        .block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))
        .and_then(|_| runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint)))
    {
        Ok(snapshot) => snapshot,
        Err(err) => {
            let reason = format!("internal P7 operation Orchestrator readiness failed: {err:#}");
            let report_path = write_internal_k8s_operation_report(
                InternalK8sOperationReport {
                    context: &context,
                    namespace: &options.namespace,
                    orch_service: &options.orch_service,
                    gateway_service: &options.gateway_service,
                    owner_worker_deploy: &options.owner_worker_deploy,
                    owner_worker_service: &options.owner_worker_service,
                    owner_worker_id: &options.owner_worker_id,
                    argocd_namespace: &options.argocd_namespace,
                    argocd_app: &options.argocd_app,
                    argocd_status: preflight.argocd_status.as_ref(),
                    deployment_images: &preflight.deployment_images,
                    assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                    expected_image: options.expected_image.as_deref(),
                    stage: "blocked_before_proposal",
                    reason: &reason,
                    preflight_errors: std::slice::from_ref(&reason),
                    operation: None,
                    proposal_response: None,
                    approval_response: None,
                    execution_response: None,
                    observation_response: None,
                    ledger_snapshot: None,
                    ledger_summary: None,
                    orchestrator: None,
                    gateway: None,
                    worker: None,
                    soak: None,
                    failure: None,
                    completion: InternalK8sOperationCompletion::default(),
                },
                options.out.as_deref(),
            )?;
            bail!(
                "{reason}; internal P7 operation report={}",
                report_path.display()
            );
        }
    };
    let mut orchestrator_evidence = InternalK8sOperationOrchestratorEvidence {
        grpc_addr: local_orch_addr.clone(),
        metrics_addr: local_orch_metrics_addr.clone(),
        registered_workers_before: before_health.registered_workers,
        registered_workers_after: None,
        assignment_listing_before: assignment_listing_summary_json(&before_listing)?,
        assignment_listing_after: None,
    };

    let mut gateway_evidence = None;
    let mut child0_session = None;
    if options.allow_execution {
        assert_http_status_endpoint(
            "internal P7 operation gateway readiness",
            &local_gateway_metrics_addr,
            "/ready",
            "200 OK",
        )?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 4)?;
        let gateway_metrics_before = assert_metrics_endpoint_body_until(
            "internal P7 operation gateway before",
            &local_gateway_metrics_addr,
            &[
                "tessera_gateway_routes",
                "tessera_gateway_client_closes_no_route_total",
                "tessera_gateway_client_closes_upstream_retry_exhausted_total",
                "tessera_gateway_client_closes_ambiguous_upstream_total",
            ],
        )?;
        let gateway_close_before = gateway_close_counters_from_metrics(&gateway_metrics_before)?;
        let gateway_routes_before =
            prometheus_sample_value(&gateway_metrics_before, "tessera_gateway_routes")?;
        let actor_a = EntityId(8_701);
        let actor_b = EntityId(8_702);
        child0_session = Some(open_gateway_join_until_snapshot(
            &local_gateway_addr,
            activation_child_cell(0),
            actor_a,
            Position { x: 4.0, y: 4.0 },
        )?);
        let _child3_session = open_gateway_join_until_snapshot(
            &local_gateway_addr,
            activation_child_cell(3),
            actor_b,
            Position { x: 24.0, y: 24.0 },
        )?;
        gateway_evidence = Some(InternalK8sOperationGatewayEvidence {
            addr: local_gateway_addr.clone(),
            metrics_addr: local_gateway_metrics_addr.clone(),
            routes_before: Some(gateway_routes_before),
            routes_after: None,
            ping_roundtrips: None,
            join_roundtrips: None,
            move_roundtrips: None,
            close_before: Some(gateway_close_before),
            close_after: None,
        });
    }

    let proposal_response = http_json_post(
        "internal P7 operation proposal",
        &local_orch_metrics_addr,
        "/operations/proposals",
    )?;
    assert_json_bool_eq(&proposal_response, &["assignments_changed"], false)?;
    let proposal_snapshot = http_json_get(
        "internal P7 operation ledger",
        &local_orch_metrics_addr,
        "/operations",
    )?;
    let selection = select_internal_p7_merge_operation(&proposal_response, &proposal_snapshot);
    let (operation, proposal_record) = match selection {
        Ok(selection) => selection,
        Err(err) => {
            let reason = err.to_string();
            let summary =
                validate_p7_operation_ledger(&proposal_snapshot, false, false, false, false, false)
                    .ok();
            let report_path = write_internal_k8s_operation_report(
                InternalK8sOperationReport {
                    context: &context,
                    namespace: &options.namespace,
                    orch_service: &options.orch_service,
                    gateway_service: &options.gateway_service,
                    owner_worker_deploy: &options.owner_worker_deploy,
                    owner_worker_service: &options.owner_worker_service,
                    owner_worker_id: &options.owner_worker_id,
                    argocd_namespace: &options.argocd_namespace,
                    argocd_app: &options.argocd_app,
                    argocd_status: preflight.argocd_status.as_ref(),
                    deployment_images: &preflight.deployment_images,
                    assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                    expected_image: options.expected_image.as_deref(),
                    stage: "blocked_before_execution",
                    reason: &reason,
                    preflight_errors: &[],
                    operation: None,
                    proposal_response: Some(&proposal_response),
                    approval_response: None,
                    execution_response: None,
                    observation_response: None,
                    ledger_snapshot: Some(&proposal_snapshot),
                    ledger_summary: summary,
                    orchestrator: Some(&orchestrator_evidence),
                    gateway: gateway_evidence.as_ref(),
                    worker: None,
                    soak: None,
                    failure: None,
                    completion: InternalK8sOperationCompletion::default(),
                },
                options.out.as_deref(),
            )?;
            bail!(
                "{reason}; internal P7 operation report={}",
                report_path.display()
            );
        }
    };
    if operation.owner_worker_id != options.owner_worker_id {
        bail!(
            "internal P7 operation owner {} did not match --owner-worker-id {}",
            operation.owner_worker_id,
            options.owner_worker_id
        );
    }
    validate_p7_operation_record(proposal_record)?;
    let proposal_summary =
        validate_p7_operation_ledger(&proposal_snapshot, false, false, false, false, false)?;
    if !options.allow_execution {
        let report_path = write_internal_k8s_operation_report(
            InternalK8sOperationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_service: &options.owner_worker_service,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                expected_image: options.expected_image.as_deref(),
                stage: "planned_without_execution",
                reason: "P7 merge operation proposal is recorded; helper stopped before approval/execution because --allow-execution was not provided",
                preflight_errors: &[],
                operation: Some(&operation),
                proposal_response: Some(&proposal_response),
                approval_response: None,
                execution_response: None,
                observation_response: None,
                ledger_snapshot: Some(&proposal_snapshot),
                ledger_summary: Some(proposal_summary),
                orchestrator: Some(&orchestrator_evidence),
                gateway: gateway_evidence.as_ref(),
                worker: None,
                soak: None,
                failure: None,
                completion: InternalK8sOperationCompletion {
                    operation_recorded: true,
                    ..InternalK8sOperationCompletion::default()
                },
            },
            options.out.as_deref(),
        )?;
        println!(
            "internal P7 operation smoke stopped before mutation; operation={} report={}",
            operation.operation_id,
            report_path.display()
        );
        return Ok(());
    }

    let policy_id = "operator_approved_dynamic_operation_v1";
    let approval_response = http_json_post(
        "internal P7 operation approval",
        &local_orch_metrics_addr,
        &format!(
            "/operations/approvals?operation_id={}&policy_id={policy_id}&approver=internal-p7-operation-smoke&expected_proposal_hash={}&ttl_secs=600&cooldown_key=internal-p7-operation-smoke&budget_key=internal-p7-operation-smoke",
            operation.operation_id, operation.proposal_hash
        ),
    )?;
    assert_json_str_eq(&approval_response, &["status"], "approved")?;
    assert_json_bool_eq(&approval_response, &["assignments_changed"], false)?;

    let execution_response = http_json_post(
        "internal P7 operation execution",
        &local_orch_metrics_addr,
        &format!(
            "/operations/executions?operation_id={}&expected_proposal_hash={}&policy_id={policy_id}",
            operation.operation_id, operation.proposal_hash
        ),
    )?;
    assert_json_str_eq(&execution_response, &["status"], "published")?;
    assert_json_bool_eq(&execution_response, &["assignments_changed"], true)?;
    assert_json_bool_eq(&execution_response, &["mutation_attempted"], true)?;
    assert_json_bool_eq(&execution_response, &["mutation_allowed"], true)?;

    runtime.block_on(wait_for_split_listing(
        &orch_endpoint,
        &[(operation.parent, operation.owner_worker_id.as_str())],
    ))?;
    assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
    assert_gateway_ping_until(&local_gateway_addr, operation.parent, 17_001)?;
    let mut child0_session = child0_session
        .ok_or_else(|| anyhow::anyhow!("internal P7 operation child0 session was not opened"))?;
    let _move_observed = request_move_until_delta(
        &mut child0_session,
        operation.parent,
        EntityId(8_701),
        1.0,
        1.0,
        "internal P7 operation parent move",
    )?;
    drop(child0_session);

    let mut soak_evidence = None;
    if options.with_soak {
        let stats = run_parent_activation_soak_loop(
            &local_gateway_addr,
            operation.parent,
            options.soak_iterations,
            Duration::from_millis(options.soak_sleep_ms),
        )?;
        let min_total = u64::from(options.soak_iterations) * 4;
        if stats.pings_ok < min_total || stats.moves_ok < min_total {
            bail!(
                "internal P7 operation soak expected at least {min_total} pings/moves, got pings={} moves={}",
                stats.pings_ok,
                stats.moves_ok
            );
        }
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(operation.parent, operation.owner_worker_id.as_str())],
        ))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
        soak_evidence = Some(InternalK8sOperationSoakEvidence {
            iterations: options.soak_iterations,
            sleep_ms: options.soak_sleep_ms,
            pings_ok: stats.pings_ok,
            moves_ok: stats.moves_ok,
            expected_actor_requests: min_total,
        });
    }

    let _parent_probe_session = open_gateway_join_until_snapshot(
        &local_gateway_addr,
        operation.parent,
        EntityId(8_703),
        Position { x: 16.0, y: 16.0 },
    )?;
    let worker_parent_metric = worker_cell_actor_count_metric(operation.parent);
    let worker_metrics = assert_metrics_endpoint_body_until(
        "internal P7 operation owner worker",
        &local_owner_worker_metrics_addr,
        &[
            "tessera_worker_cell_actor_count",
            "tessera_worker_accepted_connections_total",
        ],
    )?;
    assert_prometheus_sample_at_least(
        "internal P7 operation owner worker",
        &worker_metrics,
        worker_parent_metric.as_str(),
        1.0,
    )?;
    let worker_parent_actor_count =
        prometheus_sample_value(&worker_metrics, worker_parent_metric.as_str())?;
    let worker_evidence = InternalK8sOperationWorkerEvidence {
        worker_id: options.owner_worker_id.clone(),
        metrics_addr: local_owner_worker_metrics_addr.clone(),
        parent_actor_count: worker_parent_actor_count,
    };

    let gateway_metrics_after = assert_metrics_endpoint_body_until(
        "internal P7 operation gateway after",
        &local_gateway_metrics_addr,
        &[
            "tessera_gateway_routes",
            "tessera_gateway_ping_roundtrip_seconds_count",
            "tessera_gateway_request_roundtrip_seconds_count",
            "tessera_gateway_client_closes_no_route_total",
            "tessera_gateway_client_closes_upstream_retry_exhausted_total",
            "tessera_gateway_client_closes_ambiguous_upstream_total",
        ],
    )?;
    let gateway_routes_after =
        prometheus_sample_value(&gateway_metrics_after, "tessera_gateway_routes")?;
    let expected_ping_roundtrips = if let Some(soak) = soak_evidence.as_ref() {
        soak.expected_actor_requests as f64
    } else {
        1.0
    };
    let expected_move_roundtrips = if let Some(soak) = soak_evidence.as_ref() {
        soak.expected_actor_requests as f64
    } else {
        1.0
    };
    assert_prometheus_sample_at_least(
        "internal P7 operation gateway",
        &gateway_metrics_after,
        "tessera_gateway_ping_roundtrip_seconds_count",
        expected_ping_roundtrips,
    )?;
    assert_prometheus_sample_at_least(
        "internal P7 operation gateway",
        &gateway_metrics_after,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
        3.0,
    )?;
    assert_prometheus_sample_at_least(
        "internal P7 operation gateway",
        &gateway_metrics_after,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        expected_move_roundtrips,
    )?;
    let gateway_close_after = gateway_close_counters_from_metrics(&gateway_metrics_after)?;
    let gateway_close_before = gateway_evidence
        .as_ref()
        .and_then(|gateway| gateway.close_before)
        .ok_or_else(|| anyhow::anyhow!("internal P7 operation missing gateway close baseline"))?;
    assert_gateway_close_counters_not_increased(
        "internal P7 operation gateway",
        gateway_close_before,
        gateway_close_after,
    )?;
    let ping_roundtrips = prometheus_sample_value(
        &gateway_metrics_after,
        "tessera_gateway_ping_roundtrip_seconds_count",
    )?;
    let join_roundtrips = prometheus_sample_value(
        &gateway_metrics_after,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
    )?;
    let move_roundtrips = prometheus_sample_value(
        &gateway_metrics_after,
        "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
    )?;
    if let Some(gateway) = gateway_evidence.as_mut() {
        gateway.routes_after = Some(gateway_routes_after);
        gateway.ping_roundtrips = Some(ping_roundtrips);
        gateway.join_roundtrips = Some(join_roundtrips);
        gateway.move_roundtrips = Some(move_roundtrips);
        gateway.close_after = Some(gateway_close_after);
    }
    let (_after_health, after_listing) =
        runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;
    orchestrator_evidence.registered_workers_after = Some(after_listing.workers.len() as u64);
    orchestrator_evidence.assignment_listing_after =
        Some(assignment_listing_summary_json(&after_listing)?);

    let route_converged = (gateway_routes_after - 1.0).abs() < f64::EPSILON;
    let worker_refreshed = worker_parent_actor_count >= 1.0;
    let traffic_confirmed =
        ping_roundtrips >= expected_ping_roundtrips && move_roundtrips >= expected_move_roundtrips;
    let counters_clean = gateway_close_before == gateway_close_after;
    if !(route_converged && worker_refreshed && traffic_confirmed && counters_clean) {
        bail!(
            "internal P7 operation evidence incomplete: route_converged={route_converged} worker_refreshed={worker_refreshed} traffic_confirmed={traffic_confirmed} counters_clean={counters_clean}"
        );
    }

    if options.with_failure {
        let original_replicas = kubectl_deploy_spec_replicas(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
        )?;
        if original_replicas == 0 {
            bail!(
                "owner Worker deployment {} already has 0 replicas; cannot run P7 operation failure/recovery smoke",
                options.owner_worker_deploy
            );
        }
        kubectl_scale_deploy(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            0,
        )?;
        let mut restore = K8sScaleRestore::new(
            context.clone(),
            options.namespace.clone(),
            options.owner_worker_deploy.clone(),
            original_replicas,
        );
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            0,
        )?;
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.owner_worker_service,
            0,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_pods(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            0,
            Duration::from_secs(120),
        )?;
        let failure_error = match assert_gateway_ping_until(
            &local_gateway_addr,
            operation.parent,
            17_002,
        ) {
            Ok(()) => bail!(
                "internal P7 operation failure/recovery smoke expected parent Ping to fail while owner Worker was down"
            ),
            Err(err) => err.to_string(),
        };
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(operation.parent, operation.owner_worker_id.as_str())],
        ))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
        let gateway_metrics_after_failure = assert_metrics_endpoint_body_until(
            "internal P7 operation gateway after owner failure",
            &local_gateway_metrics_addr,
            &[
                "tessera_gateway_routes",
                "tessera_gateway_client_closes_no_route_total",
                "tessera_gateway_client_closes_upstream_retry_exhausted_total",
                "tessera_gateway_client_closes_ambiguous_upstream_total",
            ],
        )?;
        let gateway_routes_after_failure =
            prometheus_sample_value(&gateway_metrics_after_failure, "tessera_gateway_routes")?;
        let gateway_close_after_failure =
            gateway_close_counters_from_metrics(&gateway_metrics_after_failure)?;

        let observation_response = http_json_post(
            "internal P7 operation failed observation",
            &local_orch_metrics_addr,
            &format!(
                "/operations/observations?operation_id={}&expected_proposal_hash={}&observer=internal-p7-operation-failure-smoke&route_converged=true&worker_refreshed=true&traffic_confirmed=false&counters_clean=false",
                operation.operation_id, operation.proposal_hash
            ),
        )?;
        assert_json_str_eq(&observation_response, &["status"], "recovery_required")?;
        assert_json_bool_eq(&observation_response, &["observation_accepted"], false)?;

        kubectl_scale_deploy(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            original_replicas,
        )?;
        restore.disarm();
        kubectl_rollout_status(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.owner_worker_deploy,
            original_replicas,
        )?;
        wait_for_kubectl_service_ready_endpoints(
            &context,
            &options.namespace,
            &options.owner_worker_service,
            original_replicas,
            Duration::from_secs(120),
        )?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
        assert_gateway_ping_until(&local_gateway_addr, operation.parent, 17_003)?;
        let gateway_metrics_after_recovery = assert_metrics_endpoint_body_until(
            "internal P7 operation gateway after owner recovery",
            &local_gateway_metrics_addr,
            &["tessera_gateway_routes"],
        )?;
        let gateway_routes_after_recovery =
            prometheus_sample_value(&gateway_metrics_after_recovery, "tessera_gateway_routes")?;
        let (_after_health, after_listing) =
            runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;
        orchestrator_evidence.registered_workers_after = Some(after_listing.workers.len() as u64);
        orchestrator_evidence.assignment_listing_after =
            Some(assignment_listing_summary_json(&after_listing)?);

        let recovery_snapshot = http_json_get(
            "internal P7 operation ledger",
            &local_orch_metrics_addr,
            "/operations",
        )?;
        let recovery_summary =
            validate_p7_operation_ledger(&recovery_snapshot, true, false, true, false, true)?;
        let recovery_record =
            find_p7_operation_record(&recovery_snapshot, &operation.operation_id)?;
        validate_p7_recovery_required(recovery_record)?;
        let failure_evidence = InternalK8sOperationFailureEvidence {
            owner_worker_deploy: options.owner_worker_deploy.clone(),
            original_replicas,
            failure_error,
            routes_after_failure: Some(gateway_routes_after_failure),
            routes_after_recovery: Some(gateway_routes_after_recovery),
            close_after_failure: Some(gateway_close_after_failure),
        };
        let completion = InternalK8sOperationCompletion {
            operation_recorded: true,
            approval_recorded: true,
            execution_published: true,
            route_converged,
            worker_refreshed,
            traffic_confirmed: false,
            gateway_close_counters_clean: false,
            observation_completed: false,
            owner_outage_detected: true,
            observation_recovery_required: true,
            operator_recovery_confirmed: true,
            ledger_recovery_required: true,
            orchestrator_restart_smoke_ran: false,
            load_soak_ran: false,
        };
        let report_path = write_internal_k8s_operation_report(
            InternalK8sOperationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_service: &options.owner_worker_service,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                expected_image: options.expected_image.as_deref(),
                stage: "recovery_required",
                reason: "P7 merge operation owner outage was observed as recovery_required and operator-scaled Worker recovery restored parent traffic",
                preflight_errors: &[],
                operation: Some(&operation),
                proposal_response: Some(&proposal_response),
                approval_response: Some(&approval_response),
                execution_response: Some(&execution_response),
                observation_response: Some(&observation_response),
                ledger_snapshot: Some(&recovery_snapshot),
                ledger_summary: Some(recovery_summary),
                orchestrator: Some(&orchestrator_evidence),
                gateway: gateway_evidence.as_ref(),
                worker: Some(&worker_evidence),
                soak: None,
                failure: Some(&failure_evidence),
                completion,
            },
            options.out.as_deref(),
        )?;
        println!(
            "internal P7 operation failure/recovery smoke completed: operation={} report={}",
            operation.operation_id,
            report_path.display()
        );
        return Ok(());
    }

    if options.with_restart {
        forwards.stop_all();
        kubectl_rollout_restart_deploy(&context, &options.namespace, &options.orch_deploy)?;
        kubectl_rollout_status(
            &context,
            &options.namespace,
            &options.orch_deploy,
            Duration::from_secs(120),
        )?;
        wait_for_kubectl_deploy_available_replicas(
            &context,
            &options.namespace,
            &options.orch_deploy,
            1,
        )?;
        forwards.spawn(
            &context,
            &options.namespace,
            &options.orch_service,
            "p7-operation-restart-orchestrator",
            &[
                (options.local_orch_port, 6000),
                (options.local_orch_metrics_port, 6100),
            ],
        )?;
        forwards.spawn(
            &context,
            &options.namespace,
            &options.gateway_service,
            "p7-operation-restart-gateway",
            &[
                (options.local_gateway_port, 4000),
                (options.local_gateway_metrics_port, 4100),
            ],
        )?;
        forwards.spawn(
            &context,
            &options.namespace,
            &options.owner_worker_service,
            "p7-operation-restart-owner-worker",
            &[(options.local_owner_worker_metrics_port, 5100)],
        )?;

        runtime.block_on(wait_for_orchestrator_registered(&orch_endpoint, 2))?;
        runtime.block_on(wait_for_split_listing(
            &orch_endpoint,
            &[(operation.parent, operation.owner_worker_id.as_str())],
        ))?;
        assert_gateway_ready_routes(&local_gateway_metrics_addr, 1)?;
        assert_gateway_ping_until(&local_gateway_addr, operation.parent, 17_004)?;
        let restart_ledger_snapshot = http_json_get(
            "internal P7 operation ledger after restart",
            &local_orch_metrics_addr,
            "/operations",
        )?;
        let restart_record =
            find_p7_operation_record(&restart_ledger_snapshot, &operation.operation_id)?;
        validate_p7_published_execution(restart_record)?;

        let restart_actor = EntityId(8_704);
        let mut restart_session = open_gateway_join_until_snapshot(
            &local_gateway_addr,
            operation.parent,
            restart_actor,
            Position { x: 16.0, y: 16.0 },
        )?;
        let _move_observed = request_move_until_delta(
            &mut restart_session,
            operation.parent,
            restart_actor,
            1.0,
            1.0,
            "internal P7 operation restart parent move",
        )?;
        let worker_parent_metric = worker_cell_actor_count_metric(operation.parent);
        let worker_metrics_after_restart = assert_metrics_endpoint_body_until(
            "internal P7 operation owner worker after restart",
            &local_owner_worker_metrics_addr,
            &[
                "tessera_worker_cell_actor_count",
                "tessera_worker_accepted_connections_total",
            ],
        )?;
        assert_prometheus_sample_at_least(
            "internal P7 operation owner worker after restart",
            &worker_metrics_after_restart,
            worker_parent_metric.as_str(),
            1.0,
        )?;
        let worker_parent_actor_count =
            prometheus_sample_value(&worker_metrics_after_restart, worker_parent_metric.as_str())?;
        let gateway_metrics_after_restart = assert_metrics_endpoint_body_until(
            "internal P7 operation gateway after restart",
            &local_gateway_metrics_addr,
            &[
                "tessera_gateway_routes",
                "tessera_gateway_ping_roundtrip_seconds_count",
                "tessera_gateway_request_roundtrip_seconds_count",
                "tessera_gateway_client_closes_no_route_total",
                "tessera_gateway_client_closes_upstream_retry_exhausted_total",
                "tessera_gateway_client_closes_ambiguous_upstream_total",
            ],
        )?;
        let gateway_routes_after_restart =
            prometheus_sample_value(&gateway_metrics_after_restart, "tessera_gateway_routes")?;
        let ping_roundtrips_after_restart = prometheus_sample_value(
            &gateway_metrics_after_restart,
            "tessera_gateway_ping_roundtrip_seconds_count",
        )?;
        let join_roundtrips_after_restart = prometheus_sample_value(
            &gateway_metrics_after_restart,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"join\"}",
        )?;
        let move_roundtrips_after_restart = prometheus_sample_value(
            &gateway_metrics_after_restart,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
        )?;
        assert_prometheus_sample_at_least(
            "internal P7 operation gateway after restart",
            &gateway_metrics_after_restart,
            "tessera_gateway_ping_roundtrip_seconds_count",
            1.0,
        )?;
        assert_prometheus_sample_at_least(
            "internal P7 operation gateway after restart",
            &gateway_metrics_after_restart,
            "tessera_gateway_request_roundtrip_seconds_count{kind=\"move\"}",
            1.0,
        )?;
        let gateway_close_after_restart =
            gateway_close_counters_from_metrics(&gateway_metrics_after_restart)?;
        assert_gateway_close_counters_not_increased(
            "internal P7 operation gateway after restart",
            gateway_close_before,
            gateway_close_after_restart,
        )?;
        if let Some(gateway) = gateway_evidence.as_mut() {
            gateway.routes_after = Some(gateway_routes_after_restart);
            gateway.ping_roundtrips = Some(ping_roundtrips_after_restart);
            gateway.join_roundtrips = Some(join_roundtrips_after_restart);
            gateway.move_roundtrips = Some(move_roundtrips_after_restart);
            gateway.close_after = Some(gateway_close_after_restart);
        }
        let (_after_health, after_listing) =
            runtime.block_on(fetch_orch_health_and_listing(&orch_endpoint))?;
        orchestrator_evidence.registered_workers_after = Some(after_listing.workers.len() as u64);
        orchestrator_evidence.assignment_listing_after =
            Some(assignment_listing_summary_json(&after_listing)?);

        let route_converged_after_restart =
            (gateway_routes_after_restart - 1.0).abs() < f64::EPSILON;
        let worker_refreshed_after_restart = worker_parent_actor_count >= 1.0;
        let traffic_confirmed_after_restart =
            ping_roundtrips_after_restart >= 1.0 && move_roundtrips_after_restart >= 1.0;
        let counters_clean_after_restart = gateway_close_before == gateway_close_after_restart;
        if !(route_converged_after_restart
            && worker_refreshed_after_restart
            && traffic_confirmed_after_restart
            && counters_clean_after_restart)
        {
            bail!(
                "internal P7 operation restart evidence incomplete: route_converged={route_converged_after_restart} worker_refreshed={worker_refreshed_after_restart} traffic_confirmed={traffic_confirmed_after_restart} counters_clean={counters_clean_after_restart}"
            );
        }

        let observation_response = http_json_post(
            "internal P7 operation restart observation",
            &local_orch_metrics_addr,
            &format!(
                "/operations/observations?operation_id={}&expected_proposal_hash={}&observer=internal-p7-operation-restart-smoke&route_converged=true&worker_refreshed=true&traffic_confirmed=true&counters_clean=true",
                operation.operation_id, operation.proposal_hash
            ),
        )?;
        assert_json_str_eq(&observation_response, &["status"], "completed")?;
        assert_json_bool_eq(&observation_response, &["observation_accepted"], true)?;
        assert_json_bool_eq(&observation_response, &["assignments_changed"], false)?;

        let completed_snapshot = http_json_get(
            "internal P7 operation ledger",
            &local_orch_metrics_addr,
            "/operations",
        )?;
        let completed_summary =
            validate_p7_operation_ledger(&completed_snapshot, true, false, true, true, false)?;
        let completed_record =
            find_p7_operation_record(&completed_snapshot, &operation.operation_id)?;
        validate_p7_completed_observation(completed_record)?;
        let worker_evidence = InternalK8sOperationWorkerEvidence {
            worker_id: options.owner_worker_id.clone(),
            metrics_addr: local_owner_worker_metrics_addr.clone(),
            parent_actor_count: worker_parent_actor_count,
        };
        let completion = InternalK8sOperationCompletion {
            operation_recorded: true,
            approval_recorded: true,
            execution_published: true,
            route_converged: route_converged_after_restart,
            worker_refreshed: worker_refreshed_after_restart,
            traffic_confirmed: traffic_confirmed_after_restart,
            gateway_close_counters_clean: counters_clean_after_restart,
            observation_completed: true,
            owner_outage_detected: false,
            observation_recovery_required: false,
            operator_recovery_confirmed: false,
            ledger_recovery_required: false,
            orchestrator_restart_smoke_ran: true,
            load_soak_ran: false,
        };
        let report_path = write_internal_k8s_operation_report(
            InternalK8sOperationReport {
                context: &context,
                namespace: &options.namespace,
                orch_service: &options.orch_service,
                gateway_service: &options.gateway_service,
                owner_worker_deploy: &options.owner_worker_deploy,
                owner_worker_service: &options.owner_worker_service,
                owner_worker_id: &options.owner_worker_id,
                argocd_namespace: &options.argocd_namespace,
                argocd_app: &options.argocd_app,
                argocd_status: preflight.argocd_status.as_ref(),
                deployment_images: &preflight.deployment_images,
                assignment_state_storage: preflight.assignment_state_storage.as_ref(),
                expected_image: options.expected_image.as_deref(),
                stage: "completed",
                reason: "P7 merge operation survived Orchestrator rollout restart and observation completed",
                preflight_errors: &[],
                operation: Some(&operation),
                proposal_response: Some(&proposal_response),
                approval_response: Some(&approval_response),
                execution_response: Some(&execution_response),
                observation_response: Some(&observation_response),
                ledger_snapshot: Some(&completed_snapshot),
                ledger_summary: Some(completed_summary),
                orchestrator: Some(&orchestrator_evidence),
                gateway: gateway_evidence.as_ref(),
                worker: Some(&worker_evidence),
                soak: None,
                failure: None,
                completion,
            },
            options.out.as_deref(),
        )?;
        println!(
            "internal P7 operation restart smoke completed: operation={} report={}",
            operation.operation_id,
            report_path.display()
        );
        return Ok(());
    }

    let observation_response = http_json_post(
        "internal P7 operation observation",
        &local_orch_metrics_addr,
        &format!(
            "/operations/observations?operation_id={}&expected_proposal_hash={}&observer=internal-p7-operation-smoke&route_converged=true&worker_refreshed=true&traffic_confirmed=true&counters_clean=true",
            operation.operation_id, operation.proposal_hash
        ),
    )?;
    assert_json_str_eq(&observation_response, &["status"], "completed")?;
    assert_json_bool_eq(&observation_response, &["observation_accepted"], true)?;
    assert_json_bool_eq(&observation_response, &["assignments_changed"], false)?;

    let completed_snapshot = http_json_get(
        "internal P7 operation ledger",
        &local_orch_metrics_addr,
        "/operations",
    )?;
    let completed_summary =
        validate_p7_operation_ledger(&completed_snapshot, true, false, true, true, false)?;
    let completed_record = find_p7_operation_record(&completed_snapshot, &operation.operation_id)?;
    validate_p7_completed_observation(completed_record)?;
    let completion = InternalK8sOperationCompletion {
        operation_recorded: true,
        approval_recorded: true,
        execution_published: true,
        route_converged,
        worker_refreshed,
        traffic_confirmed,
        gateway_close_counters_clean: counters_clean,
        observation_completed: true,
        owner_outage_detected: false,
        observation_recovery_required: false,
        operator_recovery_confirmed: false,
        ledger_recovery_required: false,
        orchestrator_restart_smoke_ran: false,
        load_soak_ran: options.with_soak,
    };
    let report_path = write_internal_k8s_operation_report(
        InternalK8sOperationReport {
            context: &context,
            namespace: &options.namespace,
            orch_service: &options.orch_service,
            gateway_service: &options.gateway_service,
            owner_worker_deploy: &options.owner_worker_deploy,
            owner_worker_service: &options.owner_worker_service,
            owner_worker_id: &options.owner_worker_id,
            argocd_namespace: &options.argocd_namespace,
            argocd_app: &options.argocd_app,
            argocd_status: preflight.argocd_status.as_ref(),
            deployment_images: &preflight.deployment_images,
            assignment_state_storage: preflight.assignment_state_storage.as_ref(),
            expected_image: options.expected_image.as_deref(),
            stage: "completed",
            reason: "P7 merge operation executed through guarded internal helper and observation completed",
            preflight_errors: &[],
            operation: Some(&operation),
            proposal_response: Some(&proposal_response),
            approval_response: Some(&approval_response),
            execution_response: Some(&execution_response),
            observation_response: Some(&observation_response),
            ledger_snapshot: Some(&completed_snapshot),
            ledger_summary: Some(completed_summary),
            orchestrator: Some(&orchestrator_evidence),
            gateway: gateway_evidence.as_ref(),
            worker: Some(&worker_evidence),
            soak: soak_evidence.as_ref(),
            failure: None,
            completion,
        },
        options.out.as_deref(),
    )?;
    println!(
        "internal P7 operation smoke completed: operation={}{} report={}",
        operation.operation_id,
        if options.with_soak {
            ", soak verified"
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
    let assignment_state_storage =
        if options.with_restart || options.require_assignment_state_storage {
            match kubectl_assignment_state_storage(
                context,
                &options.namespace,
                &options.orch_deploy,
                &options.expected_assignment_state_path,
            ) {
                Ok(storage) => Some(storage),
                Err(err) => {
                    errors.push(format!(
                        "orchestrator assignment state storage preflight failed: {err:#}"
                    ));
                    None
                }
            }
        } else {
            None
        };
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
    if options.use_live_worker_metrics {
        for resource in [
            format!("svc/{}", options.source_worker_service),
            format!("svc/{}", options.target_worker_service),
        ] {
            if let Err(err) = kubectl_resource_name(context, &options.namespace, &resource) {
                errors.push(format!(
                    "required resource {resource} is not ready: {err:#}"
                ));
            }
        }
    }
    if options.with_failure || options.require_target_worker || options.use_live_worker_metrics {
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
            assignment_state_storage,
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
    assignment_state_storage: Option<K8sAssignmentStateStorage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sActivationPreflightResult {
    preflight: K8sActivationPreflight,
    errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sMergeActivationPreflight {
    argocd_status: Option<ArgoCdAppStatus>,
    deployment_images: Vec<K8sDeploymentImage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sMergeActivationPreflightResult {
    preflight: K8sMergeActivationPreflight,
    errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sMultiDepthActivationPreflight {
    argocd_status: Option<ArgoCdAppStatus>,
    deployment_images: Vec<K8sDeploymentImage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sMultiDepthActivationPreflightResult {
    preflight: K8sMultiDepthActivationPreflight,
    errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sOperationPreflight {
    argocd_status: Option<ArgoCdAppStatus>,
    deployment_images: Vec<K8sDeploymentImage>,
    assignment_state_storage: Option<K8sAssignmentStateStorage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sOperationPreflightResult {
    preflight: K8sOperationPreflight,
    errors: Vec<String>,
}

fn k8s_operation_preflight(
    context: &str,
    options: &K8sOperationSmokeOptions,
) -> K8sOperationPreflightResult {
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
        collect_k8s_operation_deployment_images(context, options);
    errors.extend(deployment_errors);
    let assignment_state_storage = if options.with_restart {
        match kubectl_assignment_state_storage(
            context,
            &options.namespace,
            &options.orch_deploy,
            &options.expected_assignment_state_path,
        ) {
            Ok(storage) => Some(storage),
            Err(err) => {
                errors.push(format!(
                    "orchestrator assignment state storage preflight failed: {err:#}"
                ));
                None
            }
        }
    } else {
        None
    };
    for resource in [
        format!("svc/{}", options.orch_service),
        format!("svc/{}", options.gateway_service),
        format!("deploy/{}", options.owner_worker_deploy),
    ] {
        if let Err(err) = kubectl_resource_name(context, &options.namespace, &resource) {
            errors.push(format!(
                "required resource {resource} is not ready: {err:#}"
            ));
        }
    }
    if options.allow_execution {
        let resource = format!("svc/{}", options.owner_worker_service);
        if let Err(err) = kubectl_resource_name(context, &options.namespace, &resource) {
            errors.push(format!(
                "required resource {resource} is not ready: {err:#}"
            ));
        }
    }
    K8sOperationPreflightResult {
        preflight: K8sOperationPreflight {
            argocd_status,
            deployment_images,
            assignment_state_storage,
        },
        errors,
    }
}

fn collect_k8s_operation_deployment_images(
    context: &str,
    options: &K8sOperationSmokeOptions,
) -> (Vec<K8sDeploymentImage>, Vec<String>) {
    let deployments = [
        ("orchestrator", options.orch_deploy.as_str()),
        ("gateway", options.gateway_deploy.as_str()),
        ("owner_worker", options.owner_worker_deploy.as_str()),
    ];

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

fn k8s_multi_depth_activation_preflight(
    context: &str,
    options: &K8sMultiDepthActivationSmokeOptions,
) -> K8sMultiDepthActivationPreflightResult {
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
        collect_k8s_multi_depth_runtime_deployment_images(context, options);
    errors.extend(deployment_errors);
    for resource in [
        format!("svc/{}", options.orch_service),
        format!("svc/{}", options.gateway_service),
        format!("deploy/{}", options.source_worker_deploy),
        format!("deploy/{}", options.target_worker_deploy),
    ] {
        if let Err(err) = kubectl_resource_name(context, &options.namespace, &resource) {
            errors.push(format!(
                "required resource {resource} is not ready: {err:#}"
            ));
        }
    }
    K8sMultiDepthActivationPreflightResult {
        preflight: K8sMultiDepthActivationPreflight {
            argocd_status,
            deployment_images,
        },
        errors,
    }
}

fn collect_k8s_multi_depth_runtime_deployment_images(
    context: &str,
    options: &K8sMultiDepthActivationSmokeOptions,
) -> (Vec<K8sDeploymentImage>, Vec<String>) {
    let deployments = [
        ("orchestrator", options.orch_deploy.as_str()),
        ("gateway", options.gateway_deploy.as_str()),
        ("source_worker", options.source_worker_deploy.as_str()),
        ("target_worker", options.target_worker_deploy.as_str()),
    ];

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

fn k8s_merge_activation_preflight(
    context: &str,
    options: &K8sMergeActivationSmokeOptions,
) -> K8sMergeActivationPreflightResult {
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
        collect_k8s_merge_runtime_deployment_images(context, options);
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
    let owner_resource = format!("deploy/{}", options.owner_worker_deploy);
    if let Err(err) = kubectl_resource_name(context, &options.namespace, &owner_resource) {
        errors.push(format!(
            "required resource {owner_resource} is not ready: {err:#}"
        ));
    }
    K8sMergeActivationPreflightResult {
        preflight: K8sMergeActivationPreflight {
            argocd_status,
            deployment_images,
        },
        errors,
    }
}

fn collect_k8s_merge_runtime_deployment_images(
    context: &str,
    options: &K8sMergeActivationSmokeOptions,
) -> (Vec<K8sDeploymentImage>, Vec<String>) {
    let deployments = [
        ("orchestrator", options.orch_deploy.as_str()),
        ("gateway", options.gateway_deploy.as_str()),
        ("owner_worker", options.owner_worker_deploy.as_str()),
    ];

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sDeploymentImage {
    role: &'static str,
    deployment: String,
    image: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct K8sAssignmentStateStorage {
    env_name: String,
    path: String,
    mount_path: String,
    volume_name: String,
    claim_name: String,
    policy_id: &'static str,
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
    if options.allow_activation
        || options.with_failure
        || options.require_target_worker
        || options.use_live_worker_metrics
    {
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

const ORCH_ASSIGNMENT_STATE_ENV: &str = "TESSERA_ORCH_ASSIGNMENT_STATE_PATH";
const ORCH_ASSIGNMENT_STATE_STORAGE_POLICY: &str = "orchestrator_assignment_state_pvc_rwo_v1";

fn kubectl_assignment_state_storage(
    context: &str,
    namespace: &str,
    deploy: &str,
    expected_path: &str,
) -> Result<K8sAssignmentStateStorage> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args(["get", "deploy", deploy, "-o", "json"]);
    let raw = command_stdout(&mut cmd)?;
    let deploy_json: serde_json::Value =
        serde_json::from_str(&raw).context("parse Kubernetes Deployment JSON")?;
    assignment_state_storage_from_deploy_json(&deploy_json, expected_path)
}

fn assignment_state_storage_from_deploy_json(
    deploy: &serde_json::Value,
    expected_path: &str,
) -> Result<K8sAssignmentStateStorage> {
    if expected_path.trim().is_empty() {
        bail!("expected assignment state path must not be empty");
    }
    let container = deploy
        .pointer("/spec/template/spec/containers/0")
        .ok_or_else(|| anyhow::anyhow!("orchestrator deployment has no first container"))?;
    let env = container
        .get("env")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("orchestrator deployment has no env array"))?;
    let actual_path = env
        .iter()
        .find(|entry| {
            entry.get("name").and_then(serde_json::Value::as_str) == Some(ORCH_ASSIGNMENT_STATE_ENV)
        })
        .and_then(|entry| entry.get("value"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            anyhow::anyhow!("orchestrator deployment is missing env {ORCH_ASSIGNMENT_STATE_ENV}")
        })?;
    if actual_path != expected_path {
        bail!(
            "orchestrator assignment state path mismatch: expected {expected_path}, got {actual_path}"
        );
    }
    let mount_path = Path::new(expected_path)
        .parent()
        .and_then(Path::to_str)
        .ok_or_else(|| {
            anyhow::anyhow!("expected assignment state path {expected_path} has no parent")
        })?;
    if mount_path == "/" || mount_path.is_empty() {
        bail!("assignment state path must live under a dedicated mounted directory");
    }
    let volume_mounts = container
        .get("volumeMounts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("orchestrator deployment has no volumeMounts array"))?;
    let mount = volume_mounts
        .iter()
        .find(|entry| {
            entry.get("mountPath").and_then(serde_json::Value::as_str) == Some(mount_path)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "orchestrator deployment has no volumeMount for assignment state dir {mount_path}"
            )
        })?;
    if mount
        .get("readOnly")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        bail!("assignment state volumeMount {mount_path} must be writable");
    }
    let volume_name = mount
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("assignment state volumeMount is missing name"))?;
    let volumes = deploy
        .pointer("/spec/template/spec/volumes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("orchestrator deployment has no volumes array"))?;
    let volume = volumes
        .iter()
        .find(|entry| entry.get("name").and_then(serde_json::Value::as_str) == Some(volume_name))
        .ok_or_else(|| anyhow::anyhow!("assignment state volume {volume_name} is not declared"))?;
    let claim_name = volume
        .get("persistentVolumeClaim")
        .and_then(|pvc| pvc.get("claimName"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            anyhow::anyhow!("assignment state volume {volume_name} must use persistentVolumeClaim")
        })?;

    Ok(K8sAssignmentStateStorage {
        env_name: ORCH_ASSIGNMENT_STATE_ENV.to_string(),
        path: actual_path.to_string(),
        mount_path: mount_path.to_string(),
        volume_name: volume_name.to_string(),
        claim_name: claim_name.to_string(),
        policy_id: ORCH_ASSIGNMENT_STATE_STORAGE_POLICY,
    })
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

    fn stop_all(&mut self) {
        while let Some(mut forward) = self.forwards.pop() {
            forward.terminate();
        }
    }
}

impl Drop for ManagedK8sPortForwards {
    fn drop(&mut self) {
        self.stop_all();
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

fn kubectl_service_ready_endpoints(context: &str, namespace: &str, service: &str) -> Result<u32> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args([
        "get",
        "endpointslice",
        "-l",
        &format!("kubernetes.io/service-name={service}"),
        "-o",
        "json",
    ]);
    let output = command_stdout(&mut cmd)?;
    let document: serde_json::Value =
        serde_json::from_str(&output).context("parse Kubernetes EndpointSlice JSON")?;
    let items = document
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("EndpointSlice list JSON is missing items[]"))?;
    let mut ready = 0_u32;
    for item in items {
        let Some(endpoints) = item.get("endpoints").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for endpoint in endpoints {
            let conditions = endpoint
                .get("conditions")
                .and_then(serde_json::Value::as_object);
            let is_ready = conditions
                .and_then(|conditions| conditions.get("ready"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let terminating = conditions
                .and_then(|conditions| conditions.get("terminating"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if is_ready && !terminating {
                ready += 1;
            }
        }
    }
    Ok(ready)
}

fn wait_for_kubectl_service_ready_endpoints(
    context: &str,
    namespace: &str,
    service: &str,
    expected: u32,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let ready = kubectl_service_ready_endpoints(context, namespace, service)?;
        if ready == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "service {service} did not reach ready EndpointSlice endpoints={expected} before timeout; last ready endpoints={ready}"
            );
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn kubectl_deploy_selector(context: &str, namespace: &str, deploy: &str) -> Result<String> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args(["get", "deploy", deploy, "-o", "json"]);
    let output = command_stdout(&mut cmd)?;
    let document: serde_json::Value =
        serde_json::from_str(&output).context("parse Kubernetes Deployment JSON")?;
    let labels = document
        .pointer("/spec/selector/matchLabels")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            anyhow::anyhow!("deployment {deploy} is missing spec.selector.matchLabels")
        })?;
    if labels.is_empty() {
        bail!("deployment {deploy} has an empty selector");
    }
    let mut selector_parts = labels
        .iter()
        .map(|(key, value)| {
            let value = value.as_str().ok_or_else(|| {
                anyhow::anyhow!("deployment {deploy} selector label {key} is not a string")
            })?;
            Ok(format!("{key}={value}"))
        })
        .collect::<Result<Vec<_>>>()?;
    selector_parts.sort();
    Ok(selector_parts.join(","))
}

fn kubectl_deploy_pods(context: &str, namespace: &str, deploy: &str) -> Result<u32> {
    let selector = kubectl_deploy_selector(context, namespace, deploy)?;
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args(["get", "pods", "-l", &selector, "-o", "json"]);
    let output = command_stdout(&mut cmd)?;
    let document: serde_json::Value =
        serde_json::from_str(&output).context("parse Kubernetes PodList JSON")?;
    active_kubernetes_pod_count(&document)
}

fn active_kubernetes_pod_count(document: &serde_json::Value) -> Result<u32> {
    let items = document
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("PodList JSON is missing items[]"))?;
    let active = items
        .iter()
        .filter(|pod| kubernetes_pod_counts_as_active(pod))
        .count();
    u32::try_from(active).map_err(|err| anyhow::anyhow!("active pod count overflowed u32: {err}"))
}

fn kubernetes_pod_counts_as_active(pod: &serde_json::Value) -> bool {
    let terminating = pod
        .pointer("/metadata/deletionTimestamp")
        .is_some_and(|value| !value.is_null());
    if terminating {
        return false;
    }
    !matches!(
        pod.pointer("/status/phase")
            .and_then(serde_json::Value::as_str),
        Some("Failed" | "Succeeded")
    )
}

fn wait_for_kubectl_deploy_pods(
    context: &str,
    namespace: &str,
    deploy: &str,
    expected: u32,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let pods = kubectl_deploy_pods(context, namespace, deploy)?;
        if pods == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "deployment {deploy} did not reach pod count {expected} before timeout; last pod count={pods}"
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

fn kubectl_rollout_restart_deploy(context: &str, namespace: &str, deploy: &str) -> Result<()> {
    let mut cmd = kubectl_cmd(context, Some(namespace));
    cmd.args(["rollout", "restart", &format!("deploy/{deploy}")]);
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
    assignment_state_storage: Option<&'a K8sAssignmentStateStorage>,
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
    with_restart: bool,
    restart_probe: Option<&'a SplitConvergenceProbe>,
    post_restart_stats: Option<ActivationSoakStats>,
    gateway_routes_after_restart: Option<u64>,
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
    assignment_state_storage: Option<&'a K8sAssignmentStateStorage>,
    restart_storage_required: bool,
    expected_image: Option<&'a str>,
    operation_id: &'a str,
    plan_report_path: Option<&'a Path>,
    plan: Option<&'a SplitActivationOperatorPlan>,
    stage: &'a str,
    activation_allowed: bool,
    reason: &'a str,
    preflight_errors: &'a [String],
}

struct InternalK8sMergeActivationReport<'a> {
    context: &'a str,
    namespace: &'a str,
    orch_service: &'a str,
    gateway_service: &'a str,
    owner_worker_deploy: &'a str,
    owner_worker_id: &'a str,
    argocd_namespace: &'a str,
    argocd_app: &'a str,
    argocd_status: Option<&'a ArgoCdAppStatus>,
    deployment_images: &'a [K8sDeploymentImage],
    expected_image: Option<&'a str>,
    operation_id: &'a str,
    plan_report_path: Option<&'a Path>,
    plan: Option<&'a MergeActivationOperatorPlan>,
    stage: &'a str,
    reason: &'a str,
    preflight_errors: &'a [String],
    completion: InternalK8sMergeCompletion,
}

struct InternalK8sMultiDepthActivationReport<'a> {
    context: &'a str,
    namespace: &'a str,
    orch_service: &'a str,
    gateway_service: &'a str,
    source_worker_deploy: &'a str,
    source_worker_id: &'a str,
    target_worker_deploy: &'a str,
    target_worker_id: &'a str,
    argocd_namespace: &'a str,
    argocd_app: &'a str,
    argocd_status: Option<&'a ArgoCdAppStatus>,
    deployment_images: &'a [K8sDeploymentImage],
    expected_image: Option<&'a str>,
    operation_id: &'a str,
    plan: Option<&'a InternalMultiDepthActivationPlan>,
    stage: &'a str,
    reason: &'a str,
    preflight_errors: &'a [String],
    completion: InternalK8sMultiDepthCompletion,
}

struct InternalK8sOperationReport<'a> {
    context: &'a str,
    namespace: &'a str,
    orch_service: &'a str,
    gateway_service: &'a str,
    owner_worker_deploy: &'a str,
    owner_worker_service: &'a str,
    owner_worker_id: &'a str,
    argocd_namespace: &'a str,
    argocd_app: &'a str,
    argocd_status: Option<&'a ArgoCdAppStatus>,
    deployment_images: &'a [K8sDeploymentImage],
    assignment_state_storage: Option<&'a K8sAssignmentStateStorage>,
    expected_image: Option<&'a str>,
    stage: &'a str,
    reason: &'a str,
    preflight_errors: &'a [String],
    operation: Option<&'a InternalK8sOperationSelection>,
    proposal_response: Option<&'a serde_json::Value>,
    approval_response: Option<&'a serde_json::Value>,
    execution_response: Option<&'a serde_json::Value>,
    observation_response: Option<&'a serde_json::Value>,
    ledger_snapshot: Option<&'a serde_json::Value>,
    ledger_summary: Option<P7OperationLedgerSummary>,
    orchestrator: Option<&'a InternalK8sOperationOrchestratorEvidence>,
    gateway: Option<&'a InternalK8sOperationGatewayEvidence>,
    worker: Option<&'a InternalK8sOperationWorkerEvidence>,
    soak: Option<&'a InternalK8sOperationSoakEvidence>,
    failure: Option<&'a InternalK8sOperationFailureEvidence>,
    completion: InternalK8sOperationCompletion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InternalK8sOperationSelection {
    operation_id: String,
    kind: String,
    proposal_hash: String,
    parent: CellId,
    owner_worker_id: String,
    policy_id: &'static str,
}

#[derive(Debug)]
struct InternalK8sOperationOrchestratorEvidence {
    grpc_addr: String,
    metrics_addr: String,
    registered_workers_before: u64,
    registered_workers_after: Option<u64>,
    assignment_listing_before: serde_json::Value,
    assignment_listing_after: Option<serde_json::Value>,
}

#[derive(Debug)]
struct InternalK8sOperationGatewayEvidence {
    addr: String,
    metrics_addr: String,
    routes_before: Option<f64>,
    routes_after: Option<f64>,
    ping_roundtrips: Option<f64>,
    join_roundtrips: Option<f64>,
    move_roundtrips: Option<f64>,
    close_before: Option<GatewayCloseCounters>,
    close_after: Option<GatewayCloseCounters>,
}

#[derive(Debug)]
struct InternalK8sOperationWorkerEvidence {
    worker_id: String,
    metrics_addr: String,
    parent_actor_count: f64,
}

#[derive(Debug)]
struct InternalK8sOperationSoakEvidence {
    iterations: u32,
    sleep_ms: u64,
    pings_ok: u64,
    moves_ok: u64,
    expected_actor_requests: u64,
}

#[derive(Debug)]
struct InternalK8sOperationFailureEvidence {
    owner_worker_deploy: String,
    original_replicas: u32,
    failure_error: String,
    routes_after_failure: Option<f64>,
    routes_after_recovery: Option<f64>,
    close_after_failure: Option<GatewayCloseCounters>,
}

#[derive(Debug, Clone, Copy, Default)]
struct InternalK8sMergeCompletion {
    activation_mutated: bool,
    activation_allowed: bool,
    merge_published: bool,
    post_publish_failure_smoke_ran: bool,
    owner_worker_restart_recovered_convergence: bool,
    orchestrator_restart_smoke_ran: bool,
    load_soak_ran: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct InternalK8sOperationCompletion {
    operation_recorded: bool,
    approval_recorded: bool,
    execution_published: bool,
    route_converged: bool,
    worker_refreshed: bool,
    traffic_confirmed: bool,
    gateway_close_counters_clean: bool,
    observation_completed: bool,
    owner_outage_detected: bool,
    observation_recovery_required: bool,
    operator_recovery_confirmed: bool,
    ledger_recovery_required: bool,
    orchestrator_restart_smoke_ran: bool,
    load_soak_ran: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct InternalK8sMultiDepthCompletion {
    activation_mutated: bool,
    activation_allowed: bool,
    multi_depth_published: bool,
    post_publish_failure_smoke_ran: bool,
    target_worker_restart_recovered_convergence: bool,
    orchestrator_restart_smoke_ran: bool,
    load_soak_ran: bool,
}

fn p5_rollback_policy_json() -> serde_json::Value {
    serde_json::json!({
        "policy_id": "operator_recovery_no_automatic_merge_rollback_v1",
        "automatic_rollback": "disabled",
        "failure_recovery": "operator restarts or restores the failed target Worker, then reruns convergence checks",
        "gitops_backout": "revert the controlled smoke GitOps slice, including image tag, second Worker topology, preview fixture, and manual activation flag, then wait for ArgoCD Synced/Healthy",
        "merge_activation": P5_ROLLBACK_MERGE_ACTIVATION
    })
}

fn merge_actor_state_recovery_policy_json() -> serde_json::Value {
    serde_json::json!({
        "policy_id": "volatile_worker_actor_state_rejoin_required_v1",
        "durable_actor_state": false,
        "owner_worker_restart_actor_recovery": "excluded_until_durable_worker_state",
        "scope": "same-Worker merge activation persists assignment state, but Worker actor runtime state remains in-memory in this slice",
        "operator_recovery": "restore or restart the owner Worker, verify parent route convergence, then require affected clients to rejoin or be reseeded by a future durable Worker state slice"
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
    let mut remaining_uncovered = vec![
        "guarded_kubernetes_activation_publish",
        "guarded_kubernetes_failure_recovery_smoke",
    ];
    if input.restart_storage_required {
        remaining_uncovered.push("guarded_kubernetes_restart_recovery_smoke");
    }
    let report = serde_json::json!({
        "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
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
            "automatic_rollback_observed": false,
            "assignment_state_storage_configured": input.assignment_state_storage.is_some(),
            "orchestrator_restart_smoke_ran": false
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

fn write_internal_k8s_merge_activation_report(
    input: InternalK8sMergeActivationReport<'_>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_merge_activation_smoke_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let plan_ready = input.plan.is_some_and(|plan| plan.status == "ready");
    let completion = input.completion;
    let mut remaining_uncovered = Vec::new();
    if !completion.merge_published {
        remaining_uncovered.push("guarded_kubernetes_merge_activation_publish");
    }
    if !completion.post_publish_failure_smoke_ran {
        remaining_uncovered.push("guarded_kubernetes_merge_failure_recovery_smoke");
    }
    if !completion.orchestrator_restart_smoke_ran {
        remaining_uncovered.push("guarded_kubernetes_merge_restart_recovery_smoke");
    }
    if !completion.load_soak_ran {
        remaining_uncovered.push("guarded_kubernetes_merge_load_soak");
    }
    if !plan_ready {
        remaining_uncovered.push("guarded_kubernetes_merge_ready_plan");
    }
    let report = serde_json::json!({
        "schema": "tessera.guarded_kubernetes_merge_activation_smoke.v1",
        "unix_ts": unix_timestamp_secs(),
        "operation_id": input.operation_id,
        "stage": input.stage,
        "activation_mode": "manual",
        "activation_mutated": completion.activation_mutated,
        "activation_allowed": completion.activation_allowed,
        "reason": input.reason,
        "preflight_errors": input.preflight_errors,
        "cluster": {
            "context": input.context,
            "namespace": input.namespace,
            "orchestrator_service": input.orch_service,
            "gateway_service": input.gateway_service,
            "owner_worker_deployment": input.owner_worker_deploy,
            "owner_worker_id": input.owner_worker_id,
            "argocd": argocd_status_json(
                input.argocd_namespace,
                input.argocd_app,
                input.argocd_status
            ),
            "expected_image": input.expected_image,
            "deployment_images": k8s_deployment_images_json(input.deployment_images)
        },
        "plan": internal_k8s_merge_plan_json(input.plan_report_path, input.plan, input.reason),
        "checks": {
            "merge_plan_ready": plan_ready,
            "merge_published": completion.merge_published,
            "post_publish_failure_smoke_ran": completion.post_publish_failure_smoke_ran,
            "owner_worker_restart_recovered_convergence": completion.owner_worker_restart_recovered_convergence,
            "orchestrator_restart_smoke_ran": completion.orchestrator_restart_smoke_ran,
            "load_soak_ran": completion.load_soak_ran,
            "automatic_rollback_observed": false
        },
        "rollback_policy": {
            "policy_id": "operator_controlled_manual_merge_v1",
            "automatic_rollback": false,
            "backout": "re-run manual split activation from the parent only after operator review"
        },
        "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
        "remaining_uncovered": remaining_uncovered
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_path.with_file_name(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn write_internal_k8s_multi_depth_activation_report(
    input: InternalK8sMultiDepthActivationReport<'_>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_multi_depth_activation_smoke_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let plan_ready = input.plan.is_some_and(|plan| plan.status == "ready");
    let completion = input.completion;
    let mut remaining_uncovered = Vec::new();
    if !completion.multi_depth_published {
        remaining_uncovered.push("guarded_kubernetes_multi_depth_activation_publish");
    }
    if !completion.post_publish_failure_smoke_ran {
        remaining_uncovered.push("guarded_kubernetes_multi_depth_failure_recovery_smoke");
    }
    if !completion.orchestrator_restart_smoke_ran {
        remaining_uncovered.push("guarded_kubernetes_multi_depth_restart_recovery_smoke");
    }
    if !completion.load_soak_ran {
        remaining_uncovered.push("guarded_kubernetes_multi_depth_load_soak");
    }
    if !plan_ready {
        remaining_uncovered.push("guarded_kubernetes_multi_depth_ready_plan");
    }
    let report = serde_json::json!({
        "schema": "tessera.guarded_kubernetes_multi_depth_activation_smoke.v1",
        "unix_ts": unix_timestamp_secs(),
        "operation_id": input.operation_id,
        "stage": input.stage,
        "activation_mode": "manual",
        "activation_mutated": completion.activation_mutated,
        "activation_allowed": completion.activation_allowed,
        "reason": input.reason,
        "preflight_errors": input.preflight_errors,
        "cluster": {
            "context": input.context,
            "namespace": input.namespace,
            "orchestrator_service": input.orch_service,
            "gateway_service": input.gateway_service,
            "source_worker_deployment": input.source_worker_deploy,
            "source_worker_id": input.source_worker_id,
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
        "plan": internal_k8s_multi_depth_plan_json(input.plan, input.reason),
        "checks": {
            "multi_depth_plan_ready": plan_ready,
            "multi_depth_published": completion.multi_depth_published,
            "post_publish_failure_smoke_ran": completion.post_publish_failure_smoke_ran,
            "target_worker_restart_recovered_convergence": completion.target_worker_restart_recovered_convergence,
            "orchestrator_restart_smoke_ran": completion.orchestrator_restart_smoke_ran,
            "load_soak_ran": completion.load_soak_ran,
            "automatic_rollback_observed": false
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

fn internal_k8s_multi_depth_plan_json(
    plan: Option<&InternalMultiDepthActivationPlan>,
    reason: &str,
) -> serde_json::Value {
    let Some(plan) = plan else {
        return serde_json::json!({
            "status": "not_run",
            "reason": reason,
            "activation_mutated": false
        });
    };
    serde_json::json!({
        "status": plan.status,
        "reason": plan.reason.as_str(),
        "operation_id": plan.operation_id.as_str(),
        "activation_mutated": false,
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
        "parent": plan.parent,
        "source_worker_id": plan.source_worker_id.as_deref(),
        "targets": plan.targets.iter().map(|target| {
            serde_json::json!({
                "cell": target.cell,
                "worker_id": target.worker_id.as_str()
            })
        }).collect::<Vec<_>>(),
        "submission_command": plan.submission_command.as_deref()
    })
}

fn internal_k8s_merge_plan_json(
    report_path: Option<&Path>,
    plan: Option<&MergeActivationOperatorPlan>,
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
            "owner_worker_id": plan.owner_worker_id.as_deref(),
            "siblings": plan.siblings,
            "submission_command": plan.submission_command.as_deref()
        }
    })
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
    let mut remaining_uncovered = Vec::<&str>::new();
    if !input.with_failure {
        remaining_uncovered.push("guarded_kubernetes_failure_recovery_smoke");
    }
    let restart_stats = input.post_restart_stats.unwrap_or_default();
    let report = serde_json::json!({
        "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
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
            "deployment_images": k8s_deployment_images_json(input.deployment_images),
            "assignment_state_storage": k8s_assignment_state_storage_json(input.assignment_state_storage)
        },
        "plan": {
            "report_path": input.plan_report_path.display().to_string(),
            "status": input.plan.status,
            "reason": input.plan.reason.as_str(),
            "activation_mutated": false,
            "preview": {
                "addr": input.plan.preview_addr.as_str(),
                "mode": input.plan.preview_mode.as_str(),
                "source": input.plan.preview_source.as_str(),
                "assignments_changed": false,
                "plan_count": input.plan.preview_plan_count
            },
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
            "automatic_rollback_observed": false,
            "orchestrator_restart_smoke_ran": input.with_restart,
            "assignment_state_storage_configured": input.assignment_state_storage.is_some(),
            "orchestrator_rollout_restarted": input.with_restart,
            "restarted_orchestrator_loaded_child_routes": input.with_restart,
            "worker_assignment_refresh_after_restart": input.with_restart,
            "gateway_ready_routes_after_restart": input.gateway_routes_after_restart.unwrap_or(0),
            "child_ping_all_routes_after_restart": input.restart_probe.is_some_and(|probe| probe.failures.is_empty() && probe.succeeded.len() == 4),
            "child_move_iterations_after_restart": restart_stats.moves_ok,
            "remote_aoi_frames_observed_after_restart": restart_stats.remote_delta_frames + restart_stats.remote_snapshot_frames,
            "live_remote_aoi_resync_snapshot_after_restart": input.with_restart
        },
        "success_probe": split_convergence_probe_json(input.success_probe),
        "failure_probe": input.failure_probe.map(split_convergence_probe_json),
        "recovery_probe": input.recovery_probe.map(split_convergence_probe_json),
        "restart_probe": input.restart_probe.map(split_convergence_probe_json),
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
        .join("guarded-kubernetes-activation-smoke-latest.json")
}

fn default_internal_k8s_merge_activation_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("guarded-kubernetes-merge-activation-smoke-latest.json")
}

fn default_internal_k8s_planner_activation_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("guarded-kubernetes-planner-activation-latest.json")
}

fn default_planner_activation_blocked_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("planner-activation-blocked-latest.json")
}

fn default_internal_k8s_multi_depth_activation_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("guarded-kubernetes-multi-depth-activation-smoke-latest.json")
}

fn default_internal_k8s_operation_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("guarded-kubernetes-p7-operation-smoke-latest.json")
}

fn select_internal_p7_merge_operation<'a>(
    proposal_response: &serde_json::Value,
    ledger_snapshot: &'a serde_json::Value,
) -> Result<(InternalK8sOperationSelection, &'a serde_json::Value)> {
    let operation_ids = json_array(proposal_response, &["operation_ids"])?;
    if operation_ids.is_empty() {
        bail!("internal P7 operation proposal response has no operation_ids");
    }
    for operation_id in operation_ids {
        let Some(operation_id) = operation_id.as_str() else {
            continue;
        };
        let record = find_p7_operation_record(ledger_snapshot, operation_id)?;
        let kind = json_str(record, &["kind"])?;
        if kind != "merge" {
            continue;
        }
        let proposal_hash = json_str(record, &["proposal", "proposal_hash"])?.to_string();
        let parent = json_cell_id(record, &["proposal", "parent"])?;
        let targets = json_array(record, &["proposal", "targets"])?;
        let owner_worker_id = targets
            .first()
            .ok_or_else(|| anyhow::anyhow!("internal P7 merge operation has no proposal targets"))
            .and_then(|target| json_str(target, &["worker_id"]).map(str::to_string))?;
        return Ok((
            InternalK8sOperationSelection {
                operation_id: operation_id.to_string(),
                kind: kind.to_string(),
                proposal_hash,
                parent,
                owner_worker_id,
                policy_id: "operator_approved_dynamic_operation_v1",
            },
            record,
        ));
    }
    bail!("internal P7 operation proposal response did not include an executable merge operation")
}

fn write_internal_k8s_operation_report(
    input: InternalK8sOperationReport<'_>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_operation_smoke_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let completion = input.completion;
    let mut remaining_uncovered = Vec::new();
    if !completion.operation_recorded {
        remaining_uncovered.push("guarded_kubernetes_operation_proposal_smoke");
    }
    if !completion.execution_published {
        remaining_uncovered.push("guarded_kubernetes_operation_execution_smoke");
    }
    if !completion.observation_completed {
        remaining_uncovered.push("guarded_kubernetes_operation_observation_smoke");
    }
    if !completion.load_soak_ran {
        remaining_uncovered.push("guarded_kubernetes_operation_soak_smoke");
    }
    if !completion.observation_recovery_required {
        remaining_uncovered.push("guarded_kubernetes_operation_failure_recovery_smoke");
    }
    if !completion.orchestrator_restart_smoke_ran {
        remaining_uncovered.push("guarded_kubernetes_operation_restart_smoke");
    }
    remaining_uncovered.push("p7_completion_audit");

    let report = serde_json::json!({
        "schema": "tessera.guarded_kubernetes_p7_operation_smoke.v1",
        "unix_ts": unix_timestamp_secs(),
        "stage": input.stage,
        "operation_mode": "policy_gated_manual",
        "execution_mutated": completion.execution_published,
        "execution_allowed": completion.execution_published,
        "reason": input.reason,
        "preflight_errors": input.preflight_errors,
        "cluster": {
            "context": input.context,
            "namespace": input.namespace,
            "orchestrator_service": input.orch_service,
            "gateway_service": input.gateway_service,
            "owner_worker_deployment": input.owner_worker_deploy,
            "owner_worker_service": input.owner_worker_service,
            "owner_worker_id": input.owner_worker_id,
            "argocd": argocd_status_json(
                input.argocd_namespace,
                input.argocd_app,
                input.argocd_status
            ),
            "expected_image": input.expected_image,
            "deployment_images": k8s_deployment_images_json(input.deployment_images),
            "assignment_state_storage": k8s_assignment_state_storage_json(input.assignment_state_storage)
        },
        "operation": internal_k8s_operation_selection_json(input.operation),
        "orchestrator": internal_k8s_operation_orchestrator_json(input.orchestrator),
        "gateway": internal_k8s_operation_gateway_json(input.gateway),
        "worker": internal_k8s_operation_worker_json(input.worker),
        "soak": internal_k8s_operation_soak_json(input.soak),
        "failure": internal_k8s_operation_failure_json(input.failure),
        "ledger": internal_k8s_operation_ledger_json(input.ledger_snapshot, input.ledger_summary),
        "responses": {
            "proposal": input.proposal_response,
            "approval": input.approval_response,
            "execution": input.execution_response,
            "observation": input.observation_response
        },
        "checks": {
            "operation_recorded": completion.operation_recorded,
            "approval_recorded": completion.approval_recorded,
            "execution_published": completion.execution_published,
            "route_converged": completion.route_converged,
            "worker_refreshed": completion.worker_refreshed,
            "traffic_confirmed": completion.traffic_confirmed,
            "gateway_close_counters_clean": completion.gateway_close_counters_clean,
            "observation_completed": completion.observation_completed,
            "owner_outage_detected": completion.owner_outage_detected,
            "observation_recovery_required": completion.observation_recovery_required,
            "operator_recovery_confirmed": completion.operator_recovery_confirmed,
            "ledger_recovery_required": completion.ledger_recovery_required,
            "orchestrator_restart_smoke_ran": completion.orchestrator_restart_smoke_ran,
            "load_soak_ran": completion.load_soak_ran,
            "automatic_rollback_observed": false
        },
        "remaining_uncovered": remaining_uncovered
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped_name = input
        .operation
        .map(|operation| operation.operation_id.as_str())
        .unwrap_or_else(|| {
            if input.stage == "blocked_before_proposal" {
                "internal-p7-operation-blocked-before-proposal"
            } else {
                "internal-p7-operation-blocked"
            }
        });
    let stamped = report_path.with_file_name(format!("{stamped_name}.json"));
    fs::write(&stamped, &body)?;
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn internal_k8s_operation_selection_json(
    operation: Option<&InternalK8sOperationSelection>,
) -> serde_json::Value {
    let Some(operation) = operation else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "operation_id": operation.operation_id.as_str(),
        "kind": operation.kind.as_str(),
        "proposal_hash": operation.proposal_hash.as_str(),
        "parent": operation.parent,
        "owner_worker_id": operation.owner_worker_id.as_str(),
        "policy_id": operation.policy_id
    })
}

fn internal_k8s_operation_orchestrator_json(
    evidence: Option<&InternalK8sOperationOrchestratorEvidence>,
) -> serde_json::Value {
    let Some(evidence) = evidence else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "grpc_addr": evidence.grpc_addr.as_str(),
        "metrics_addr": evidence.metrics_addr.as_str(),
        "registered_workers_before": evidence.registered_workers_before,
        "registered_workers_after": evidence.registered_workers_after,
        "assignment_listing_before": evidence.assignment_listing_before,
        "assignment_listing_after": evidence.assignment_listing_after
    })
}

fn internal_k8s_operation_gateway_json(
    evidence: Option<&InternalK8sOperationGatewayEvidence>,
) -> serde_json::Value {
    let Some(evidence) = evidence else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "addr": evidence.addr.as_str(),
        "metrics_addr": evidence.metrics_addr.as_str(),
        "routes_before": evidence.routes_before,
        "routes_after": evidence.routes_after,
        "ping_roundtrips": evidence.ping_roundtrips,
        "join_roundtrips": evidence.join_roundtrips,
        "move_roundtrips": evidence.move_roundtrips,
        "close_counters": {
            "before": evidence.close_before.map(gateway_close_counters_json),
            "after": evidence.close_after.map(gateway_close_counters_json)
        }
    })
}

fn internal_k8s_operation_worker_json(
    evidence: Option<&InternalK8sOperationWorkerEvidence>,
) -> serde_json::Value {
    let Some(evidence) = evidence else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "worker_id": evidence.worker_id.as_str(),
        "metrics_addr": evidence.metrics_addr.as_str(),
        "parent_actor_count": evidence.parent_actor_count
    })
}

fn internal_k8s_operation_soak_json(
    evidence: Option<&InternalK8sOperationSoakEvidence>,
) -> serde_json::Value {
    let Some(evidence) = evidence else {
        return serde_json::json!({
            "ran": false
        });
    };
    serde_json::json!({
        "ran": true,
        "iterations": evidence.iterations,
        "sleep_ms": evidence.sleep_ms,
        "pings_ok": evidence.pings_ok,
        "moves_ok": evidence.moves_ok,
        "expected_actor_requests": evidence.expected_actor_requests
    })
}

fn internal_k8s_operation_failure_json(
    evidence: Option<&InternalK8sOperationFailureEvidence>,
) -> serde_json::Value {
    let Some(evidence) = evidence else {
        return serde_json::json!({
            "ran": false
        });
    };
    serde_json::json!({
        "ran": true,
        "owner_worker_deployment": evidence.owner_worker_deploy.as_str(),
        "original_replicas": evidence.original_replicas,
        "failure_error": evidence.failure_error.as_str(),
        "routes_after_failure": evidence.routes_after_failure,
        "routes_after_recovery": evidence.routes_after_recovery,
        "close_counters": {
            "after_failure": evidence.close_after_failure.map(gateway_close_counters_json)
        }
    })
}

fn internal_k8s_operation_ledger_json(
    snapshot: Option<&serde_json::Value>,
    summary: Option<P7OperationLedgerSummary>,
) -> serde_json::Value {
    let Some(snapshot) = snapshot else {
        return serde_json::Value::Null;
    };
    let summary = summary.map(|summary| {
        serde_json::json!({
            "records": summary.records,
            "proposal_records": summary.proposal_records,
            "approval_records": summary.approval_records,
            "blocked_execution_records": summary.blocked_execution_records,
            "published_execution_records": summary.published_execution_records,
            "completed_observation_records": summary.completed_observation_records,
            "recovery_required_records": summary.recovery_required_records
        })
    });
    serde_json::json!({
        "summary": summary,
        "snapshot": snapshot
    })
}

#[derive(Debug)]
struct K8sPlannerActivationReportOptions {
    namespace: String,
    context: Option<String>,
    orch_deploy: String,
    gateway_deploy: String,
    source_worker_deploy: String,
    target_worker_deploy: String,
    argocd_namespace: String,
    argocd_app: String,
    skip_argocd_check: bool,
    expected_image: Option<String>,
    blocked_report: Option<PathBuf>,
    published_report: Option<PathBuf>,
    out: Option<PathBuf>,
}

struct InternalK8sPlannerActivationReportInput<'a> {
    context: &'a str,
    namespace: &'a str,
    argocd_namespace: &'a str,
    argocd_app: &'a str,
    argocd_status: Option<&'a ArgoCdAppStatus>,
    deployment_images: &'a [K8sDeploymentImage],
    expected_image: Option<&'a str>,
    preflight_errors: &'a [String],
    blocked_report: &'a serde_json::Value,
    published_report: &'a serde_json::Value,
}

fn run_k8s_planner_activation_report(options: K8sPlannerActivationReportOptions) -> Result<()> {
    let blocked_path = options
        .blocked_report
        .clone()
        .unwrap_or_else(default_planner_activation_blocked_path);
    let published_path = options
        .published_report
        .clone()
        .unwrap_or_else(default_planner_activation_path);
    let blocked_report = read_json_report(&blocked_path)?;
    validate_planner_activation_report(&blocked_report, false)
        .with_context(|| format!("validate blocked planner report {}", blocked_path.display()))?;
    let published_report = read_json_report(&published_path)?;
    validate_planner_activation_report(&published_report, true).with_context(|| {
        format!(
            "validate published planner report {}",
            published_path.display()
        )
    })?;

    let context = resolve_kube_context(options.context.as_deref())?;
    let mut preflight_errors = Vec::new();
    let argocd_status = if options.skip_argocd_check {
        None
    } else {
        match kubectl_argocd_app_status(&context, &options.argocd_namespace, &options.argocd_app) {
            Ok(status) => Some(status),
            Err(err) => {
                preflight_errors.push(format!(
                    "ArgoCD application status preflight failed: {err:#}"
                ));
                None
            }
        }
    };
    if let Some(status) = argocd_status.as_ref()
        && let Err(err) = validate_argocd_app_ready(status)
    {
        preflight_errors.push(err.to_string());
    }
    let (deployment_images, deployment_errors) = collect_p6_rollout_deployment_images(
        &context,
        &options.namespace,
        &options.orch_deploy,
        &options.gateway_deploy,
        &options.source_worker_deploy,
        &options.target_worker_deploy,
    );
    preflight_errors.extend(deployment_errors);
    if let Some(expected_image) = options.expected_image.as_deref()
        && let Err(err) = validate_k8s_deployment_images(&deployment_images, expected_image)
    {
        preflight_errors.push(err.to_string());
    }

    let report_path = write_internal_k8s_planner_activation_report(
        InternalK8sPlannerActivationReportInput {
            context: &context,
            namespace: &options.namespace,
            argocd_namespace: &options.argocd_namespace,
            argocd_app: &options.argocd_app,
            argocd_status: argocd_status.as_ref(),
            deployment_images: &deployment_images,
            expected_image: options.expected_image.as_deref(),
            preflight_errors: &preflight_errors,
            blocked_report: &blocked_report,
            published_report: &published_report,
        },
        options.out.as_deref(),
    )?;

    println!(
        "internal k8s planner activation report written: {}",
        report_path.display()
    );
    if !preflight_errors.is_empty() {
        println!(
            "internal k8s planner activation report is incomplete: {} preflight error(s)",
            preflight_errors.len()
        );
    }
    Ok(())
}

fn write_internal_k8s_planner_activation_report(
    input: InternalK8sPlannerActivationReportInput<'_>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_planner_activation_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let blocked_mutated = json_path(input.blocked_report, &["activation_mutated"])
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let published_mutated = json_path(input.published_report, &["activation_mutated"])
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let published_policy_accepted = json_path(input.published_report, &["policy", "accepted"])
        .and_then(serde_json::Value::as_bool)
        == Some(true);
    let default_off_blocked = json_str(input.blocked_report, &["status"]).unwrap_or_default()
        == "blocked_by_policy"
        && !blocked_mutated;
    let policy_approved_published =
        json_str(input.published_report, &["status"]).unwrap_or_default() == "published"
            && published_mutated
            && published_policy_accepted;
    let activation_mutated_only_after_policy =
        default_off_blocked && policy_approved_published && !blocked_mutated && published_mutated;
    let mut remaining_uncovered = Vec::new();
    if !default_off_blocked {
        remaining_uncovered.push("guarded_kubernetes_planner_default_off_block");
    }
    if !policy_approved_published {
        remaining_uncovered.push("guarded_kubernetes_planner_policy_approved_publish");
    }
    if !activation_mutated_only_after_policy {
        remaining_uncovered.push("guarded_kubernetes_planner_mutation_policy_order");
    }
    if !input.preflight_errors.is_empty() {
        remaining_uncovered.push("guarded_kubernetes_planner_cluster_preflight_clean");
    }

    let report = serde_json::json!({
        "schema": "tessera.guarded_kubernetes_planner_activation.v1",
        "unix_ts": unix_timestamp_secs(),
        "activation_mode": "policy_gated",
        "activation_mutated": policy_approved_published,
        "cluster": {
            "context": input.context,
            "namespace": input.namespace,
            "expected_image": input.expected_image,
            "argocd": argocd_status_json(
                input.argocd_namespace,
                input.argocd_app,
                input.argocd_status
            ),
            "deployment_images": k8s_deployment_images_json(input.deployment_images)
        },
        "blocked_report": input.blocked_report,
        "published_report": input.published_report,
        "checks": {
            "default_off_blocked": default_off_blocked,
            "policy_approved_published": policy_approved_published,
            "activation_mutated_only_after_policy": activation_mutated_only_after_policy,
            "automatic_mutation_observed": false
        },
        "preflight_errors": input.preflight_errors,
        "remaining_uncovered": remaining_uncovered
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn run_k8s_activation_report_check(
    report: Option<&Path>,
    require_published: bool,
    require_failure: bool,
    require_restart: bool,
    require_live_metrics_plan: bool,
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
        require_restart,
        require_live_metrics_plan,
        expected_image,
        expect_preflight_errors,
    )?;
    println!(
        "internal k8s activation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct InternalCompletionRequirements {
    require_published: bool,
    require_failure: bool,
    require_restart: bool,
    require_soak: bool,
}

#[derive(Debug, Clone, Copy)]
struct InternalP7OperationRequirements {
    require_published_execution: bool,
    require_completed_observation: bool,
    require_soak: bool,
    require_recovery_required: bool,
    require_restart: bool,
}

fn run_k8s_merge_activation_report_check(
    report: Option<&Path>,
    require_ready_plan: bool,
    requirements: InternalCompletionRequirements,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_merge_activation_smoke_path);
    let body = fs::read_to_string(&report_path)
        .with_context(|| format!("read merge activation report {}", report_path.display()))?;
    let report: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parse merge activation report {}", report_path.display()))?;
    validate_internal_k8s_merge_activation_report(
        &report,
        require_ready_plan,
        expected_image,
        expect_preflight_errors,
    )?;
    validate_internal_k8s_merge_completion_gates(
        &report,
        requirements.require_published,
        requirements.require_failure,
        requirements.require_restart,
        requirements.require_soak,
    )?;
    println!(
        "internal k8s merge activation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn run_k8s_operation_report_check(
    report: Option<&Path>,
    requirements: InternalP7OperationRequirements,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_operation_smoke_path);
    let body = fs::read_to_string(&report_path)
        .with_context(|| format!("read P7 operation report {}", report_path.display()))?;
    let report: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parse P7 operation report {}", report_path.display()))?;
    validate_internal_k8s_operation_report(
        &report,
        requirements,
        expected_image,
        expect_preflight_errors,
    )?;
    println!(
        "internal k8s P7 operation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn run_k8s_multi_depth_activation_report_check(
    report: Option<&Path>,
    require_ready_plan: bool,
    requirements: InternalCompletionRequirements,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_multi_depth_activation_smoke_path);
    let body = fs::read_to_string(&report_path).with_context(|| {
        format!(
            "read multi-depth activation report {}",
            report_path.display()
        )
    })?;
    let report: serde_json::Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "parse multi-depth activation report {}",
            report_path.display()
        )
    })?;
    validate_internal_k8s_multi_depth_activation_report(
        &report,
        require_ready_plan,
        expected_image,
        expect_preflight_errors,
    )?;
    validate_internal_k8s_multi_depth_completion_gates(
        &report,
        requirements.require_published,
        requirements.require_failure,
        requirements.require_restart,
        requirements.require_soak,
    )?;
    println!(
        "internal k8s multi-depth activation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

fn run_k8s_planner_activation_report_check(
    report: Option<&Path>,
    expected_image: Option<&str>,
    require_live_metrics_plan: bool,
) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_internal_k8s_planner_activation_path);
    let planner = read_json_report(&report_path)?;
    validate_internal_k8s_planner_activation_report(
        &planner,
        expected_image,
        require_live_metrics_plan,
    )?;
    println!(
        "internal k8s planner activation report is valid: {}",
        report_path.display()
    );
    Ok(())
}

struct DevActivationReportCheckOptions<'a> {
    plan_report: Option<&'a Path>,
    activation_report: Option<&'a Path>,
    failure_report: Option<&'a Path>,
    soak_report: Option<&'a Path>,
    restart_report: Option<&'a Path>,
    merge_plan_report: Option<&'a Path>,
    merge_activation_report: Option<&'a Path>,
    merge_cross_worker_report: Option<&'a Path>,
    merge_failure_report: Option<&'a Path>,
    merge_restart_report: Option<&'a Path>,
    merge_soak_report: Option<&'a Path>,
    planner_mutation_report: Option<&'a Path>,
    require_live_metrics_plan: bool,
    require_planner_live_metrics: bool,
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
    let restart_path = options.restart_report.map(Path::to_path_buf);
    let merge_plan_path = options.merge_plan_report.map(Path::to_path_buf);
    let merge_activation_path = options.merge_activation_report.map(Path::to_path_buf);
    let merge_cross_worker_path = options.merge_cross_worker_report.map(Path::to_path_buf);
    let merge_failure_path = options.merge_failure_report.map(Path::to_path_buf);
    let merge_restart_path = options.merge_restart_report.map(Path::to_path_buf);
    let merge_soak_path = options.merge_soak_report.map(Path::to_path_buf);
    let planner_mutation_path = options.planner_mutation_report.map(Path::to_path_buf);

    let plan = read_json_report(&plan_path)?;
    validate_split_activation_plan_report(&plan)?;
    if options.require_live_metrics_plan {
        validate_split_activation_live_metrics_plan_report(&plan)?;
    }
    let activation = read_json_report(&activation_path)?;
    validate_activation_smoke_report(&activation)?;
    if options.require_live_metrics_plan {
        validate_activation_smoke_live_metrics_plan_link(&activation)?;
    }
    let failure = read_json_report(&failure_path)?;
    validate_activation_failure_smoke_report(&failure)?;
    let soak = read_json_report(&soak_path)?;
    validate_activation_soak_report(&soak, options.min_soak_iterations)?;
    if let Some(restart_path) = restart_path.as_ref() {
        let restart = read_json_report(restart_path)?;
        validate_activation_restart_smoke_report(&restart)?;
    }
    if let Some(merge_plan_path) = merge_plan_path.as_ref() {
        let merge_plan = read_json_report(merge_plan_path)?;
        validate_merge_activation_plan_report(&merge_plan)?;
    }
    if let Some(merge_activation_path) = merge_activation_path.as_ref() {
        let merge_activation = read_json_report(merge_activation_path)?;
        validate_merge_activation_smoke_report(&merge_activation, false)?;
    }
    if let Some(merge_cross_worker_path) = merge_cross_worker_path.as_ref() {
        let merge_cross_worker = read_json_report(merge_cross_worker_path)?;
        validate_merge_activation_cross_worker_smoke_report(&merge_cross_worker)?;
    }
    if let Some(merge_failure_path) = merge_failure_path.as_ref() {
        let merge_failure = read_json_report(merge_failure_path)?;
        validate_merge_activation_failure_smoke_report(&merge_failure)?;
    }
    if let Some(merge_restart_path) = merge_restart_path.as_ref() {
        let merge_restart = read_json_report(merge_restart_path)?;
        validate_merge_activation_smoke_report(&merge_restart, true)?;
    }
    if let Some(merge_soak_path) = merge_soak_path.as_ref() {
        let merge_soak = read_json_report(merge_soak_path)?;
        validate_merge_activation_soak_report(&merge_soak, options.min_soak_iterations)?;
    }
    if let Some(planner_mutation_path) = planner_mutation_path.as_ref() {
        let planner_mutation = read_json_report(planner_mutation_path)?;
        validate_planner_activation_report(&planner_mutation, true)?;
        if options.require_planner_live_metrics {
            validate_planner_activation_live_metrics_report(&planner_mutation)?;
        }
    }

    println!(
        "local activation reports are valid: {}, {}, {}, {}{}{}{}{}{}{}{}{}",
        plan_path.display(),
        activation_path.display(),
        failure_path.display(),
        soak_path.display(),
        restart_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default(),
        merge_plan_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default(),
        merge_activation_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default(),
        merge_cross_worker_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default(),
        merge_failure_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default(),
        merge_restart_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default(),
        merge_soak_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default(),
        planner_mutation_path
            .as_ref()
            .map(|path| format!(", {}", path.display()))
            .unwrap_or_default()
    );
    Ok(())
}

fn read_json_report(path: &Path) -> Result<serde_json::Value> {
    let body =
        fs::read_to_string(path).with_context(|| format!("read report {}", path.display()))?;
    serde_json::from_str(&body).with_context(|| format!("parse report {}", path.display()))
}

fn default_p7_operation_ledger_path() -> PathBuf {
    workspace_root().join(".dev/operation-ledger.json")
}

fn default_p7_operation_loop_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("p7-operation-loop-smoke-latest.json")
}

fn default_p7_operation_execution_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("p7-operation-execution-smoke-latest.json")
}

fn default_p7_operation_observation_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("p7-operation-observation-smoke-latest.json")
}

fn default_p7_operation_recovery_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("p7-operation-recovery-smoke-latest.json")
}

fn default_p7_operation_restart_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("p7-operation-restart-smoke-latest.json")
}

fn default_p7_operation_soak_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("p7-operation-soak-smoke-latest.json")
}

fn run_p7_operation_ledger_check(
    ledger: Option<&Path>,
    require_approval: bool,
    require_blocked_execution: bool,
    require_published_execution: bool,
    require_completed_observation: bool,
    require_recovery_required: bool,
    json: bool,
) -> Result<()> {
    let ledger_path = ledger
        .map(Path::to_path_buf)
        .unwrap_or_else(default_p7_operation_ledger_path);
    let ledger = read_json_report(&ledger_path)?;
    let summary = validate_p7_operation_ledger(
        &ledger,
        require_approval,
        require_blocked_execution,
        require_published_execution,
        require_completed_observation,
        require_recovery_required,
    )
    .with_context(|| format!("validate P7 operation ledger {}", ledger_path.display()))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "complete": true,
                "schema": "tessera.p7_operation_ledger_check.v1",
                "ledger": ledger_path,
                "records": summary.records,
                "proposal_records": summary.proposal_records,
                "approval_records": summary.approval_records,
                "blocked_execution_records": summary.blocked_execution_records,
                "published_execution_records": summary.published_execution_records,
                "completed_observation_records": summary.completed_observation_records,
                "recovery_required_records": summary.recovery_required_records
            }))?
        );
    } else {
        println!(
            "P7 operation ledger ok: records={}, proposals={}, approvals={}, blocked_executions={}, published_executions={}, completed_observations={}, recovery_required={} ({})",
            summary.records,
            summary.proposal_records,
            summary.approval_records,
            summary.blocked_execution_records,
            summary.published_execution_records,
            summary.completed_observation_records,
            summary.recovery_required_records,
            ledger_path.display()
        );
    }
    Ok(())
}

fn write_p7_operation_loop_smoke_report(report: &serde_json::Value) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let body = format!("{}\n", serde_json::to_string_pretty(report)?);
    let stamped = report_dir.join(format!(
        "p7-operation-loop-smoke-{}.json",
        unix_timestamp_secs()
    ));
    fs::write(&stamped, &body)?;
    let latest = default_p7_operation_loop_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_p7_operation_execution_smoke_report(report: &serde_json::Value) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let body = format!("{}\n", serde_json::to_string_pretty(report)?);
    let stamped = report_dir.join(format!(
        "p7-operation-execution-smoke-{}.json",
        unix_timestamp_secs()
    ));
    fs::write(&stamped, &body)?;
    let latest = default_p7_operation_execution_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_p7_operation_observation_smoke_report(report: &serde_json::Value) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let body = format!("{}\n", serde_json::to_string_pretty(report)?);
    let stamped = report_dir.join(format!(
        "p7-operation-observation-smoke-{}.json",
        unix_timestamp_secs()
    ));
    fs::write(&stamped, &body)?;
    let latest = default_p7_operation_observation_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_p7_operation_recovery_smoke_report(report: &serde_json::Value) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let body = format!("{}\n", serde_json::to_string_pretty(report)?);
    let stamped = report_dir.join(format!(
        "p7-operation-recovery-smoke-{}.json",
        unix_timestamp_secs()
    ));
    fs::write(&stamped, &body)?;
    let latest = default_p7_operation_recovery_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_p7_operation_restart_smoke_report(report: &serde_json::Value) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let body = format!("{}\n", serde_json::to_string_pretty(report)?);
    let stamped = report_dir.join(format!(
        "p7-operation-restart-smoke-{}.json",
        unix_timestamp_secs()
    ));
    fs::write(&stamped, &body)?;
    let latest = default_p7_operation_restart_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_p7_operation_soak_smoke_report(report: &serde_json::Value) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let body = format!("{}\n", serde_json::to_string_pretty(report)?);
    let stamped = report_dir.join(format!(
        "p7-operation-soak-smoke-{}.json",
        unix_timestamp_secs()
    ));
    fs::write(&stamped, &body)?;
    let latest = default_p7_operation_soak_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct P7OperationLedgerSummary {
    records: usize,
    proposal_records: usize,
    approval_records: usize,
    blocked_execution_records: usize,
    published_execution_records: usize,
    completed_observation_records: usize,
    recovery_required_records: usize,
}

fn validate_p7_operation_ledger(
    ledger: &serde_json::Value,
    require_approval: bool,
    require_blocked_execution: bool,
    require_published_execution: bool,
    require_completed_observation: bool,
    require_recovery_required: bool,
) -> Result<P7OperationLedgerSummary> {
    let schema = json_str(ledger, &["schema"])?;
    if schema != "tessera.orch.operation_ledger.v1" {
        bail!("operation ledger schema must be tessera.orch.operation_ledger.v1; got {schema}");
    }
    let records = json_array(ledger, &["records"])?;
    if records.is_empty() {
        bail!("operation ledger must contain at least one record");
    }

    let mut proposal_records = 0_usize;
    let mut approval_records = 0_usize;
    let mut blocked_execution_records = 0_usize;
    let mut published_execution_records = 0_usize;
    let mut completed_observation_records = 0_usize;
    let mut recovery_required_records = 0_usize;
    for record in records {
        validate_p7_operation_record(record)?;
        proposal_records += 1;
        if validate_p7_operation_approval(record).is_ok() {
            approval_records += 1;
        }
        if validate_p7_blocked_execution(record).is_ok() {
            blocked_execution_records += 1;
        }
        if validate_p7_published_execution(record).is_ok() {
            published_execution_records += 1;
        }
        if validate_p7_completed_observation(record).is_ok() {
            completed_observation_records += 1;
        }
        if validate_p7_recovery_required(record).is_ok() {
            recovery_required_records += 1;
        }
    }
    if require_approval && approval_records == 0 {
        bail!("operation ledger is missing required approval evidence");
    }
    if require_blocked_execution && blocked_execution_records == 0 {
        bail!("operation ledger is missing required blocked execution evidence");
    }
    if require_published_execution && published_execution_records == 0 {
        bail!("operation ledger is missing required published execution evidence");
    }
    if require_completed_observation && completed_observation_records == 0 {
        bail!("operation ledger is missing required completed observation evidence");
    }
    if require_recovery_required && recovery_required_records == 0 {
        bail!("operation ledger is missing required recovery-required evidence");
    }

    Ok(P7OperationLedgerSummary {
        records: records.len(),
        proposal_records,
        approval_records,
        blocked_execution_records,
        published_execution_records,
        completed_observation_records,
        recovery_required_records,
    })
}

fn validate_p7_operation_record(record: &serde_json::Value) -> Result<()> {
    let operation_id = json_str(record, &["operation_id"])?;
    if operation_id.trim().is_empty() {
        bail!("operation record has empty operation_id");
    }
    let kind = json_str(record, &["kind"])?;
    if !matches!(kind, "split" | "merge" | "multi_depth_split") {
        bail!("operation_id={operation_id} has unknown kind={kind}");
    }
    let status = json_str(record, &["status"])?;
    if !matches!(
        status,
        "proposed"
            | "blocked_by_policy"
            | "approved"
            | "executing"
            | "observing"
            | "recovery_required"
            | "completed"
            | "failed"
    ) {
        bail!("operation_id={operation_id} has unknown status={status}");
    }
    let created = json_u64(record, &["created_unix_secs"])?;
    let updated = json_u64(record, &["updated_unix_secs"])?;
    if updated < created {
        bail!("operation_id={operation_id} has updated_unix_secs before created_unix_secs");
    }
    let source = json_str(record, &["proposal", "source"])?;
    if source.trim().is_empty() {
        bail!("operation_id={operation_id} proposal.source is empty");
    }
    let proposal_hash = json_str(record, &["proposal", "proposal_hash"])?;
    if proposal_hash.trim().is_empty() {
        bail!("operation_id={operation_id} proposal.proposal_hash is empty");
    }
    let targets = json_array(record, &["proposal", "targets"])?;
    if targets.is_empty() {
        bail!("operation_id={operation_id} proposal.targets is empty");
    }
    let command = json_str(record, &["proposal", "submission_command"])?;
    if command.trim().is_empty() {
        bail!("operation_id={operation_id} proposal.submission_command is empty");
    }
    let phases = json_array(record, &["phases"])?;
    if !phases.iter().any(|phase| {
        json_str(phase, &["name"]).is_ok_and(|name| name == "proposal_recorded")
            && json_str(phase, &["state"]).is_ok_and(|state| state == "succeeded")
    }) {
        bail!("operation_id={operation_id} is missing succeeded proposal_recorded phase");
    }
    Ok(())
}

fn validate_p7_operation_approval(record: &serde_json::Value) -> Result<()> {
    let operation_id = json_str(record, &["operation_id"])?;
    let kind = json_str(record, &["kind"])?;
    let approval = json_field(record, &["approval"])?;
    if approval.is_null() {
        bail!("operation_id={operation_id} approval is null");
    }
    let policy_id = json_str(record, &["approval", "policy_id"])?;
    let approver = json_str(record, &["approval", "approver"])?;
    let allowed_kind = json_str(record, &["approval", "allowed_kind"])?;
    let expected_hash = json_str(record, &["approval", "expected_proposal_hash"])?;
    let proposal_hash = json_str(record, &["proposal", "proposal_hash"])?;
    let approved = json_u64(record, &["approval", "approved_unix_secs"])?;
    let expires = json_u64(record, &["approval", "expires_unix_secs"])?;
    let cooldown_key = json_str(record, &["approval", "cooldown_key"])?;
    let budget_key = json_str(record, &["approval", "budget_key"])?;
    if policy_id.trim().is_empty() || approver.trim().is_empty() {
        bail!("operation_id={operation_id} approval policy_id/approver must not be empty");
    }
    if allowed_kind != kind {
        bail!("operation_id={operation_id} approval.allowed_kind does not match kind");
    }
    if expected_hash != proposal_hash {
        bail!(
            "operation_id={operation_id} approval expected_proposal_hash does not match proposal_hash"
        );
    }
    if expires <= approved {
        bail!("operation_id={operation_id} approval expires before approved time");
    }
    if cooldown_key.trim().is_empty() || budget_key.trim().is_empty() {
        bail!("operation_id={operation_id} approval cooldown_key/budget_key must not be empty");
    }
    Ok(())
}

fn validate_p7_operation_loop_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.p7_operation_loop_smoke.v1")?;
    let ledger_path = json_str(report, &["ledger", "path"])?;
    if ledger_path.trim().is_empty() {
        bail!("P7 operation loop smoke report has empty ledger.path");
    }
    assert_json_number_at_least(report, &["ledger", "records"], 3.0)?;
    assert_json_number_at_least(report, &["ledger", "proposal_records"], 3.0)?;
    assert_json_number_at_least(report, &["ledger", "approval_records"], 3.0)?;
    assert_json_number_at_least(report, &["ledger", "blocked_execution_records"], 3.0)?;
    assert_json_bool_eq(report, &["checks", "split_operation_loop_smoke"], true)?;
    assert_json_bool_eq(report, &["checks", "merge_operation_loop_smoke"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "multi_depth_operation_loop_smoke"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "proposal_approval_default_off_execution"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "ledger_check_passed"], true)?;

    let cases = json_array(report, &["cases"])?;
    for expected_kind in ["split", "merge", "multi_depth_split"] {
        let Some(case) = cases.iter().find(|case| {
            json_str(case, &["operation", "kind"]).is_ok_and(|kind| kind == expected_kind)
        }) else {
            bail!("P7 operation loop smoke report missing case kind `{expected_kind}`");
        };
        let operation_id = json_str(case, &["operation", "operation_id"])?;
        if operation_id.trim().is_empty() {
            bail!("P7 operation loop smoke report has empty operation_id for {expected_kind}");
        }
        let proposal_hash = json_str(case, &["operation", "proposal_hash"])?;
        if proposal_hash.trim().is_empty() {
            bail!("P7 operation loop smoke report has empty proposal_hash for {expected_kind}");
        }
        assert_json_str_eq(
            case,
            &["operation", "policy_id"],
            "operator_approved_dynamic_operation_v1",
        )?;
        assert_json_bool_eq(case, &["checks", "proposal_recorded"], true)?;
        assert_json_bool_eq(case, &["checks", "approval_recorded"], true)?;
        assert_json_bool_eq(case, &["checks", "execution_blocked_by_policy"], true)?;
        assert_json_bool_eq(case, &["checks", "assignments_unchanged"], true)?;
        assert_json_bool_eq(case, &["checks", "mutation_attempted"], false)?;
        assert_json_bool_eq(case, &["checks", "mutation_allowed"], false)?;
        assert_json_bool_eq(case, &["checks", "ledger_check_passed"], true)?;
        assert_json_bool_eq(
            case,
            &["responses", "proposal", "assignments_changed"],
            false,
        )?;
        assert_json_str_eq(case, &["responses", "approval", "status"], "approved")?;
        assert_json_bool_eq(
            case,
            &["responses", "approval", "assignments_changed"],
            false,
        )?;
        assert_json_bool_eq(
            case,
            &["responses", "execution", "assignments_changed"],
            false,
        )?;
        assert_json_bool_eq(
            case,
            &["responses", "execution", "mutation_attempted"],
            false,
        )?;
        assert_json_bool_eq(case, &["responses", "execution", "mutation_allowed"], false)?;
        let status = json_str(case, &["responses", "execution", "status"])?;
        if !matches!(status, "blocked_by_policy" | "already_blocked_by_policy") {
            bail!(
                "P7 operation loop smoke report case {expected_kind} has unexpected execution status `{status}`"
            );
        }
    }
    assert_remaining_uncovered_contains(report, "guarded_kubernetes_operation_loop_smoke")?;
    Ok(())
}

fn validate_p7_operation_execution_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.p7_operation_execution_smoke.v1",
    )?;
    assert_json_str_eq(report, &["operation", "kind"], "merge")?;
    assert_json_str_eq(
        report,
        &["operation", "policy_id"],
        "operator_approved_dynamic_operation_v1",
    )?;
    let ledger_path = json_str(report, &["ledger", "path"])?;
    if ledger_path.trim().is_empty() {
        bail!("P7 operation execution smoke report has empty ledger.path");
    }
    assert_json_number_at_least(report, &["ledger", "records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "proposal_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "approval_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "published_execution_records"], 1.0)?;
    assert_json_str_eq(report, &["responses", "approval", "status"], "approved")?;
    assert_json_str_eq(report, &["responses", "execution", "status"], "published")?;
    assert_json_bool_eq(
        report,
        &["responses", "execution", "assignments_changed"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["responses", "execution", "mutation_attempted"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["responses", "execution", "mutation_allowed"],
        true,
    )?;
    assert_json_str_eq(
        report,
        &["responses", "repeat_execution", "status"],
        "already_published",
    )?;
    assert_json_bool_eq(
        report,
        &["responses", "repeat_execution", "assignments_changed"],
        false,
    )?;
    assert_json_bool_eq(
        report,
        &["responses", "repeat_execution", "mutation_attempted"],
        false,
    )?;
    assert_json_bool_eq(report, &["checks", "merge_execution_published"], true)?;
    assert_json_bool_eq(report, &["checks", "parent_route_published"], true)?;
    assert_json_bool_eq(report, &["checks", "repeat_execution_idempotent"], true)?;
    assert_json_bool_eq(report, &["checks", "ledger_execution_published"], true)?;
    assert_remaining_uncovered_contains(report, "guarded_kubernetes_operation_execution_smoke")?;
    Ok(())
}

fn validate_p7_operation_observation_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.p7_operation_observation_smoke.v1",
    )?;
    assert_json_str_eq(report, &["operation", "kind"], "merge")?;
    assert_json_str_eq(
        report,
        &["operation", "policy_id"],
        "operator_approved_dynamic_operation_v1",
    )?;
    let ledger_path = json_str(report, &["ledger", "path"])?;
    if ledger_path.trim().is_empty() {
        bail!("P7 operation observation smoke report has empty ledger.path");
    }
    assert_json_number_at_least(report, &["ledger", "records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "proposal_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "approval_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "published_execution_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "completed_observation_records"], 1.0)?;
    assert_json_str_eq(report, &["responses", "approval", "status"], "approved")?;
    assert_json_str_eq(report, &["responses", "execution", "status"], "published")?;
    assert_json_str_eq(report, &["responses", "observation", "status"], "completed")?;
    assert_json_bool_eq(
        report,
        &["responses", "observation", "observation_accepted"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["responses", "observation", "assignments_changed"],
        false,
    )?;
    assert_json_bool_eq(report, &["checks", "merge_execution_published"], true)?;
    assert_json_bool_eq(report, &["checks", "route_converged"], true)?;
    assert_json_bool_eq(report, &["checks", "worker_refreshed"], true)?;
    assert_json_bool_eq(report, &["checks", "traffic_confirmed"], true)?;
    assert_json_bool_eq(report, &["checks", "gateway_close_counters_clean"], true)?;
    assert_json_bool_eq(report, &["checks", "observation_completed"], true)?;
    assert_remaining_uncovered_contains(report, "guarded_kubernetes_operation_observation_smoke")?;
    Ok(())
}

fn validate_p7_operation_recovery_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.p7_operation_recovery_smoke.v1",
    )?;
    assert_json_str_eq(report, &["operation", "kind"], "merge")?;
    assert_json_str_eq(
        report,
        &["operation", "policy_id"],
        "operator_approved_dynamic_operation_v1",
    )?;
    let ledger_path = json_str(report, &["ledger", "path"])?;
    if ledger_path.trim().is_empty() {
        bail!("P7 operation recovery smoke report has empty ledger.path");
    }
    assert_json_number_at_least(report, &["ledger", "records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "published_execution_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "recovery_required_records"], 1.0)?;
    assert_json_str_eq(report, &["responses", "execution", "status"], "published")?;
    assert_json_str_eq(
        report,
        &["responses", "observation", "status"],
        "recovery_required",
    )?;
    assert_json_bool_eq(
        report,
        &["responses", "observation", "observation_accepted"],
        false,
    )?;
    assert_json_bool_eq(report, &["checks", "merge_execution_published"], true)?;
    assert_json_bool_eq(report, &["checks", "owner_outage_detected"], true)?;
    assert_json_bool_eq(report, &["checks", "observation_recovery_required"], true)?;
    assert_json_bool_eq(report, &["checks", "no_automatic_rollback"], true)?;
    assert_json_bool_eq(report, &["checks", "operator_recovery_confirmed"], true)?;
    assert_remaining_uncovered_contains(report, "guarded_kubernetes_operation_recovery_smoke")?;
    Ok(())
}

fn validate_p7_operation_restart_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.p7_operation_restart_smoke.v1")?;
    assert_json_str_eq(report, &["operation", "kind"], "merge")?;
    assert_json_str_eq(
        report,
        &["operation", "policy_id"],
        "operator_approved_dynamic_operation_v1",
    )?;
    let ledger_path = json_str(report, &["ledger", "path"])?;
    if ledger_path.trim().is_empty() {
        bail!("P7 operation restart smoke report has empty ledger.path");
    }
    let assignment_state_path = json_str(report, &["orchestrator", "assignment_state_path"])?;
    if assignment_state_path.trim().is_empty() {
        bail!("P7 operation restart smoke report has empty assignment_state_path");
    }
    assert_json_number_at_least(report, &["ledger", "records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "proposal_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "approval_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "published_execution_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "completed_observation_records"], 1.0)?;
    assert_json_number_at_least(
        report,
        &["orchestrator", "registered_workers_after_restart"],
        2.0,
    )?;
    assert_json_number_at_least(report, &["gateway", "routes_after_restart"], 1.0)?;
    assert_json_number_at_least(report, &["worker", "parent_actor_count_after_restart"], 2.0)?;
    assert_json_str_eq(report, &["responses", "approval", "status"], "approved")?;
    assert_json_str_eq(report, &["responses", "execution", "status"], "published")?;
    assert_json_str_eq(report, &["responses", "observation", "status"], "completed")?;
    assert_json_bool_eq(
        report,
        &["responses", "observation", "observation_accepted"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "merge_execution_published"], true)?;
    assert_json_bool_eq(report, &["checks", "assignment_state_persisted"], true)?;
    assert_json_bool_eq(report, &["checks", "orchestrator_restarted"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "ledger_execution_survived_restart"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "route_converged_after_restart"], true)?;
    assert_json_bool_eq(report, &["checks", "worker_refreshed_after_restart"], true)?;
    assert_json_bool_eq(report, &["checks", "traffic_confirmed_after_restart"], true)?;
    assert_json_bool_eq(report, &["checks", "gateway_close_counters_clean"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "observation_completed_after_restart"],
        true,
    )?;
    assert_remaining_uncovered_contains(report, "guarded_kubernetes_operation_restart_smoke")?;
    Ok(())
}

fn validate_p7_operation_soak_smoke_report(
    report: &serde_json::Value,
    min_iterations: u32,
) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.p7_operation_soak_smoke.v1")?;
    assert_json_str_eq(report, &["operation", "kind"], "merge")?;
    assert_json_str_eq(
        report,
        &["operation", "policy_id"],
        "operator_approved_dynamic_operation_v1",
    )?;
    let ledger_path = json_str(report, &["ledger", "path"])?;
    if ledger_path.trim().is_empty() {
        bail!("P7 operation soak smoke report has empty ledger.path");
    }
    assert_json_number_at_least(report, &["soak", "iterations"], f64::from(min_iterations))?;
    let iterations = json_u64(report, &["soak", "iterations"])?;
    let expected_actor_requests = iterations as f64 * 4.0;
    assert_json_number_at_least(report, &["soak", "pings_ok"], expected_actor_requests)?;
    assert_json_number_at_least(report, &["soak", "moves_ok"], expected_actor_requests)?;
    assert_json_number_at_least(report, &["ledger", "records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "proposal_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "approval_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "published_execution_records"], 1.0)?;
    assert_json_number_at_least(report, &["ledger", "completed_observation_records"], 1.0)?;
    assert_json_number_at_least(report, &["gateway", "routes_after_soak"], 1.0)?;
    assert_json_number_at_least(
        report,
        &["gateway", "ping_roundtrips"],
        expected_actor_requests,
    )?;
    assert_json_number_at_least(
        report,
        &["gateway", "move_roundtrips"],
        expected_actor_requests,
    )?;
    assert_json_number_at_least(report, &["gateway", "join_roundtrips"], 4.0)?;
    assert_json_number_at_least(report, &["worker", "parent_actor_count_after_soak"], 1.0)?;
    assert_json_str_eq(report, &["responses", "approval", "status"], "approved")?;
    assert_json_str_eq(report, &["responses", "execution", "status"], "published")?;
    assert_json_str_eq(report, &["responses", "observation", "status"], "completed")?;
    assert_json_bool_eq(
        report,
        &["responses", "observation", "observation_accepted"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "merge_execution_published"], true)?;
    assert_json_bool_eq(report, &["checks", "route_converged_after_soak"], true)?;
    assert_json_bool_eq(report, &["checks", "worker_refreshed_after_soak"], true)?;
    assert_json_bool_eq(report, &["checks", "traffic_confirmed_after_soak"], true)?;
    assert_json_bool_eq(report, &["checks", "gateway_close_counters_clean"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "observation_completed_after_soak"],
        true,
    )?;
    assert_remaining_uncovered_contains(report, "guarded_kubernetes_operation_soak_smoke")?;
    Ok(())
}

fn validate_p7_blocked_execution(record: &serde_json::Value) -> Result<()> {
    let operation_id = json_str(record, &["operation_id"])?;
    let status = json_str(record, &["status"])?;
    if status != "blocked_by_policy" {
        bail!("operation_id={operation_id} status is not blocked_by_policy");
    }
    let phases = json_array(record, &["phases"])?;
    if !phases.iter().any(|phase| {
        json_str(phase, &["name"]).is_ok_and(|name| name == "execution_blocked_by_policy")
            && json_str(phase, &["state"]).is_ok_and(|state| state == "failed")
            && json_str(phase, &["reason"]).is_ok_and(|reason| !reason.trim().is_empty())
    }) {
        bail!("operation_id={operation_id} is missing failed execution_blocked_by_policy phase");
    }
    Ok(())
}

fn validate_p7_published_execution(record: &serde_json::Value) -> Result<()> {
    let operation_id = json_str(record, &["operation_id"])?;
    let status = json_str(record, &["status"])?;
    if !matches!(status, "observing" | "completed" | "recovery_required") {
        bail!(
            "operation_id={operation_id} status is not observing/completed/recovery_required after published execution"
        );
    }
    let phases = json_array(record, &["phases"])?;
    if !phases.iter().any(|phase| {
        json_str(phase, &["name"]).is_ok_and(|name| name == "execution_started")
            && json_str(phase, &["state"]).is_ok_and(|state| state == "succeeded")
    }) {
        bail!("operation_id={operation_id} is missing succeeded execution_started phase");
    }
    if !phases.iter().any(|phase| {
        json_str(phase, &["name"]).is_ok_and(|name| name == "execution_published")
            && json_str(phase, &["state"]).is_ok_and(|state| state == "succeeded")
    }) {
        bail!("operation_id={operation_id} is missing succeeded execution_published phase");
    }
    Ok(())
}

fn validate_p7_completed_observation(record: &serde_json::Value) -> Result<()> {
    let operation_id = json_str(record, &["operation_id"])?;
    let status = json_str(record, &["status"])?;
    if status != "completed" {
        bail!("operation_id={operation_id} status is not completed after observation");
    }
    validate_p7_published_execution(record)?;
    let phases = json_array(record, &["phases"])?;
    if !phases.iter().any(|phase| {
        json_str(phase, &["name"]).is_ok_and(|name| name == "observation_completed")
            && json_str(phase, &["state"]).is_ok_and(|state| state == "succeeded")
            && json_str(phase, &["reason"]).is_ok_and(|reason| {
                reason.contains("route_converged")
                    && reason.contains("worker_refreshed")
                    && reason.contains("traffic_confirmed")
                    && reason.contains("counters_clean")
            })
    }) {
        bail!("operation_id={operation_id} is missing succeeded observation_completed phase");
    }
    Ok(())
}

fn validate_p7_recovery_required(record: &serde_json::Value) -> Result<()> {
    let operation_id = json_str(record, &["operation_id"])?;
    let status = json_str(record, &["status"])?;
    if status != "recovery_required" {
        bail!("operation_id={operation_id} status is not recovery_required");
    }
    validate_p7_published_execution(record)?;
    let phases = json_array(record, &["phases"])?;
    if !phases.iter().any(|phase| {
        json_str(phase, &["name"]).is_ok_and(|name| name == "observation_failed")
            && json_str(phase, &["state"]).is_ok_and(|state| state == "failed")
            && json_str(phase, &["reason"]).is_ok_and(|reason| {
                reason.contains("traffic_confirmed") || reason.contains("counters_clean")
            })
    }) {
        bail!("operation_id={operation_id} is missing failed observation_failed phase");
    }
    Ok(())
}

fn default_p6_gitops_rollout_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("p6-gitops-rollout-latest.json")
}

#[derive(Debug)]
struct P6RolloutReportOptions {
    namespace: String,
    context: Option<String>,
    orch_deploy: String,
    gateway_deploy: String,
    source_worker_deploy: String,
    target_worker_deploy: String,
    argocd_namespace: String,
    argocd_app: String,
    skip_argocd_check: bool,
    image: String,
    rollout_revision: Option<String>,
    cleanup_revision: Option<String>,
    image_published: bool,
    gitops_rollout_approved: bool,
    post_smoke_default_off_cleanup: bool,
    manual_activation_default_off: bool,
    preview_fixture_removed: bool,
    out: Option<PathBuf>,
}

struct P6RolloutReportInput<'a> {
    context: &'a str,
    namespace: &'a str,
    image: &'a str,
    argocd_namespace: &'a str,
    argocd_app: &'a str,
    argocd_status: Option<&'a ArgoCdAppStatus>,
    deployment_images: &'a [K8sDeploymentImage],
    preflight_errors: &'a [String],
    rollout_revision: Option<&'a str>,
    cleanup_revision: Option<&'a str>,
    image_published: bool,
    gitops_rollout_approved: bool,
    post_smoke_default_off_cleanup: bool,
    manual_activation_default_off: bool,
    preview_fixture_removed: bool,
}

fn run_p6_rollout_report(options: P6RolloutReportOptions) -> Result<()> {
    if options.image.trim().is_empty() {
        bail!("--image must not be empty");
    }
    let context = resolve_kube_context(options.context.as_deref())?;
    let mut preflight_errors = Vec::new();
    let argocd_status = if options.skip_argocd_check {
        None
    } else {
        match kubectl_argocd_app_status(&context, &options.argocd_namespace, &options.argocd_app) {
            Ok(status) => Some(status),
            Err(err) => {
                preflight_errors.push(format!(
                    "ArgoCD application status preflight failed: {err:#}"
                ));
                None
            }
        }
    };
    if let Some(status) = argocd_status.as_ref()
        && let Err(err) = validate_argocd_app_ready(status)
    {
        preflight_errors.push(err.to_string());
    }
    let (deployment_images, deployment_errors) = collect_p6_rollout_deployment_images(
        &context,
        &options.namespace,
        &options.orch_deploy,
        &options.gateway_deploy,
        &options.source_worker_deploy,
        &options.target_worker_deploy,
    );
    preflight_errors.extend(deployment_errors);

    let report_path = write_p6_gitops_rollout_report(
        P6RolloutReportInput {
            context: &context,
            namespace: &options.namespace,
            image: &options.image,
            argocd_namespace: &options.argocd_namespace,
            argocd_app: &options.argocd_app,
            argocd_status: argocd_status.as_ref(),
            deployment_images: &deployment_images,
            preflight_errors: &preflight_errors,
            rollout_revision: options.rollout_revision.as_deref(),
            cleanup_revision: options.cleanup_revision.as_deref(),
            image_published: options.image_published,
            gitops_rollout_approved: options.gitops_rollout_approved,
            post_smoke_default_off_cleanup: options.post_smoke_default_off_cleanup,
            manual_activation_default_off: options.manual_activation_default_off,
            preview_fixture_removed: options.preview_fixture_removed,
        },
        options.out.as_deref(),
    )?;

    println!(
        "P6 GitOps rollout report written: {}",
        report_path.display()
    );
    if !preflight_errors.is_empty() {
        println!(
            "P6 GitOps rollout report is incomplete: {} preflight error(s); run p6-rollout-report-check after the rollout evidence is complete",
            preflight_errors.len()
        );
    }
    Ok(())
}

fn collect_p6_rollout_deployment_images(
    context: &str,
    namespace: &str,
    orch_deploy: &str,
    gateway_deploy: &str,
    source_worker_deploy: &str,
    target_worker_deploy: &str,
) -> (Vec<K8sDeploymentImage>, Vec<String>) {
    let deployments = [
        ("orchestrator", orch_deploy),
        ("gateway", gateway_deploy),
        ("source_worker", source_worker_deploy),
        ("target_worker", target_worker_deploy),
    ];
    let mut images = Vec::new();
    let mut errors = Vec::new();
    for (role, deployment) in deployments {
        match kubectl_deploy_first_container_image(context, namespace, deployment)
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

fn write_p6_gitops_rollout_report(
    input: P6RolloutReportInput<'_>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_p6_gitops_rollout_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let argocd_synced_healthy = input
        .argocd_status
        .is_some_and(|status| status.sync == "Synced" && status.health == "Healthy");
    let deployment_images_match = !input.deployment_images.is_empty()
        && input
            .deployment_images
            .iter()
            .all(|deployment| deployment.image == input.image);
    let mut remaining_uncovered = Vec::new();
    if !input.image_published {
        remaining_uncovered.push("p6_image_published");
    }
    if !input.gitops_rollout_approved {
        remaining_uncovered.push("p6_gitops_rollout_approved");
    }
    if !argocd_synced_healthy {
        remaining_uncovered.push("p6_argocd_synced_healthy");
    }
    if !deployment_images_match {
        remaining_uncovered.push("p6_runtime_deployments_match_image");
    }
    if !input.post_smoke_default_off_cleanup {
        remaining_uncovered.push("p6_post_smoke_default_off_cleanup");
    }
    if !input.manual_activation_default_off {
        remaining_uncovered.push("p6_manual_activation_default_off");
    }
    if !input.preview_fixture_removed {
        remaining_uncovered.push("p6_preview_fixture_removed");
    }
    if input.rollout_revision.unwrap_or_default().trim().is_empty() {
        remaining_uncovered.push("p6_gitops_rollout_revision");
    }
    if input.cleanup_revision.unwrap_or_default().trim().is_empty() {
        remaining_uncovered.push("p6_gitops_cleanup_revision");
    }
    if !input.preflight_errors.is_empty() {
        remaining_uncovered.push("p6_rollout_preflight_clean");
    }

    let report = serde_json::json!({
        "schema": "tessera.p6_gitops_rollout.v1",
        "unix_ts": unix_timestamp_secs(),
        "image": {
            "name": input.image,
            "deployment_images_match": deployment_images_match
        },
        "gitops": {
            "rollout_revision": input.rollout_revision.unwrap_or_default(),
            "cleanup_revision": input.cleanup_revision.unwrap_or_default()
        },
        "argocd": argocd_status_json(
            input.argocd_namespace,
            input.argocd_app,
            input.argocd_status
        ),
        "cluster": {
            "context": input.context,
            "namespace": input.namespace,
            "deployment_images": k8s_deployment_images_json(input.deployment_images)
        },
        "cleanup": {
            "manual_activation_default_off": input.manual_activation_default_off,
            "preview_fixture_removed": input.preview_fixture_removed
        },
        "checks": {
            "image_published": input.image_published,
            "gitops_rollout_approved": input.gitops_rollout_approved,
            "argocd_synced_healthy": argocd_synced_healthy,
            "deployment_images_match": deployment_images_match,
            "post_smoke_default_off_cleanup": input.post_smoke_default_off_cleanup
        },
        "preflight_errors": input.preflight_errors,
        "remaining_uncovered": remaining_uncovered
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn run_p6_rollout_report_check(report: Option<&Path>, expected_image: Option<&str>) -> Result<()> {
    let report_path = report
        .map(Path::to_path_buf)
        .unwrap_or_else(default_p6_gitops_rollout_path);
    let report = read_json_report(&report_path)?;
    validate_p6_gitops_rollout_report(&report, expected_image)?;
    println!(
        "P6 GitOps rollout report is valid: {}",
        report_path.display()
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct P6CompletionFinding {
    gate: &'static str,
    evidence: String,
    missing: String,
}

fn run_p6_completion_audit(report_dir: &Path, json: bool) -> Result<()> {
    let findings = p6_completion_findings(report_dir);
    if json {
        let body = serde_json::json!({
            "schema": "tessera.p6_completion_audit.v1",
            "complete": findings.is_empty(),
            "report_dir": report_dir.display().to_string(),
            "findings": findings.iter().map(|finding| {
                serde_json::json!({
                    "gate": finding.gate,
                    "evidence": finding.evidence,
                    "missing": finding.missing
                })
            }).collect::<Vec<_>>()
        });
        println!("{}", serde_json::to_string_pretty(&body)?);
    } else if findings.is_empty() {
        println!(
            "P6+ completion audit passed: all required report gates are covered in {}",
            report_dir.display()
        );
    } else {
        println!(
            "P6+ completion audit incomplete: {} missing gate(s) in {}",
            findings.len(),
            report_dir.display()
        );
        for finding in &findings {
            println!(
                "- {}: evidence={} missing={}",
                finding.gate, finding.evidence, finding.missing
            );
        }
    }
    if !findings.is_empty() {
        bail!(
            "P6+ completion audit is incomplete: {} missing gate(s)",
            findings.len()
        );
    }
    Ok(())
}

fn p6_completion_findings(report_dir: &Path) -> Vec<P6CompletionFinding> {
    let mut findings = Vec::new();
    let internal_split_path = report_dir.join("guarded-kubernetes-activation-smoke-latest.json");
    match read_json_report(&internal_split_path) {
        Ok(report) => {
            let p5_split_baseline = normalize_historical_p5_split_report_for_audit(&report);
            if let Err(err) = validate_internal_k8s_activation_report(
                &p5_split_baseline,
                true,
                true,
                false,
                false,
                None,
                &[],
            ) {
                findings.push(P6CompletionFinding {
                    gate: "p5_internal_split_publish_failure_recovery",
                    evidence: format!("{} failed verifier: {err}", internal_split_path.display()),
                    missing: "published split success/failure/recovery report with empty P5 uncovered set".to_string(),
                });
            }
            if let Err(err) = validate_internal_k8s_activation_report(
                &report,
                true,
                false,
                true,
                false,
                None,
                &[],
            ) {
                let evidence = p6_restart_preflight_evidence(report_dir).unwrap_or_else(|| {
                    format!(
                        "{} failed --require-restart: {err}",
                        internal_split_path.display()
                    )
                });
                findings.push(P6CompletionFinding {
                    gate: "p6_internal_restart_recovery",
                    evidence,
                    missing:
                        "approved internal restart recovery report from PVC-backed assignment state"
                            .to_string(),
                });
            }
            if let Err(err) = validate_internal_k8s_activation_report(
                &report,
                false,
                false,
                false,
                true,
                None,
                &[],
            ) {
                let evidence =
                    p6_live_metrics_readiness_evidence(report_dir).unwrap_or_else(|| {
                        format!(
                            "{} failed --require-live-metrics-plan: {err}",
                            internal_split_path.display()
                        )
                    });
                findings.push(P6CompletionFinding {
                    gate: "p6_internal_live_metrics_plan",
                    evidence,
                    missing: "internal ready split plan sourced from live Worker metrics"
                        .to_string(),
                });
            }
            if json_str(&report, &["cluster", "expected_image"])
                .unwrap_or_default()
                .ends_with(":v2026.05.2")
            {
                findings.push(P6CompletionFinding {
                    gate: "p6_gitops_rollout_image",
                    evidence: "latest internal split report still references v2026.05.2".to_string(),
                    missing: "new P6 image tag, approved GitOps rollout, ArgoCD evidence, and post-smoke cleanup".to_string(),
                });
            }
            let rollout_path = report_dir.join("p6-gitops-rollout-latest.json");
            match read_json_report(&rollout_path) {
                Ok(rollout) => {
                    if let Err(err) = validate_p6_gitops_rollout_report(&rollout, None) {
                        findings.push(P6CompletionFinding {
                            gate: "p6_gitops_rollout_evidence",
                            evidence: format!("{} failed verifier: {err}", rollout_path.display()),
                            missing: "P6 image publish, approved GitOps rollout, ArgoCD Synced/Healthy, and default-off cleanup report".to_string(),
                        });
                    }
                }
                Err(err) => findings.push(P6CompletionFinding {
                    gate: "p6_gitops_rollout_evidence",
                    evidence: format!("{} unavailable: {err}", rollout_path.display()),
                    missing: "P6 image publish, approved GitOps rollout, ArgoCD Synced/Healthy, and default-off cleanup report".to_string(),
                }),
            }
        }
        Err(err) => {
            findings.push(P6CompletionFinding {
                gate: "p5_internal_split_publish_failure_recovery",
                evidence: format!("{} unavailable: {err}", internal_split_path.display()),
                missing: "internal split activation report".to_string(),
            });
            findings.push(P6CompletionFinding {
                gate: "p6_internal_restart_recovery",
                evidence: format!("{} unavailable: {err}", internal_split_path.display()),
                missing: "internal restart recovery report".to_string(),
            });
            findings.push(P6CompletionFinding {
                gate: "p6_internal_live_metrics_plan",
                evidence: format!("{} unavailable: {err}", internal_split_path.display()),
                missing: "internal live-metrics plan report".to_string(),
            });
        }
    }

    let internal_merge_path =
        report_dir.join("guarded-kubernetes-merge-activation-smoke-latest.json");
    match read_json_report(&internal_merge_path) {
        Ok(report) => {
            if let Err(err) =
                validate_internal_k8s_merge_activation_report(&report, true, None, &[])
            {
                findings.push(P6CompletionFinding {
                    gate: "p6_internal_merge_ready_plan",
                    evidence: format!(
                        "{} failed --require-ready-plan: {err}",
                        internal_merge_path.display()
                    ),
                    missing: "internal no-mutation ready merge plan".to_string(),
                });
            }
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_merge_publish",
                &internal_merge_path,
                &["checks", "merge_published"],
                "approved internal merge publish evidence",
            );
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_merge_failure_recovery",
                &internal_merge_path,
                &["checks", "post_publish_failure_smoke_ran"],
                "internal merge owner outage detection and recovery smoke",
            );
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_merge_restart_recovery",
                &internal_merge_path,
                &["checks", "orchestrator_restart_smoke_ran"],
                "internal merge restart recovery smoke",
            );
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_merge_load_soak",
                &internal_merge_path,
                &["checks", "load_soak_ran"],
                "internal merge load/soak smoke",
            );
            if bool_gate_set_complete(
                &report,
                &[
                    &["checks", "merge_published"],
                    &["checks", "post_publish_failure_smoke_ran"],
                    &["checks", "orchestrator_restart_smoke_ran"],
                    &["checks", "load_soak_ran"],
                ],
            ) && let Err(err) =
                validate_internal_k8s_merge_completion_gates(&report, true, true, true, true)
            {
                findings.push(P6CompletionFinding {
                    gate: "p6_internal_merge_completion_schema",
                    evidence: format!(
                        "{} failed full completion verifier: {err}",
                        internal_merge_path.display()
                    ),
                    missing: "internal merge published report with clean completion schema"
                        .to_string(),
                });
            }
        }
        Err(err) => findings.push(P6CompletionFinding {
            gate: "p6_internal_merge_ready_plan",
            evidence: format!("{} unavailable: {err}", internal_merge_path.display()),
            missing: "internal merge readiness/publish report".to_string(),
        }),
    }

    let internal_multi_depth_path =
        report_dir.join("guarded-kubernetes-multi-depth-activation-smoke-latest.json");
    match read_json_report(&internal_multi_depth_path) {
        Ok(report) => {
            if let Err(err) =
                validate_internal_k8s_multi_depth_activation_report(&report, true, None, &[])
            {
                findings.push(P6CompletionFinding {
                    gate: "p6_internal_multi_depth_ready_plan",
                    evidence: format!(
                        "{} failed --require-ready-plan: {err}",
                        internal_multi_depth_path.display()
                    ),
                    missing: "internal no-mutation ready canonical multi-depth plan".to_string(),
                });
            }
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_multi_depth_publish",
                &internal_multi_depth_path,
                &["checks", "multi_depth_published"],
                "approved internal canonical multi-depth publish evidence",
            );
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_multi_depth_failure_recovery",
                &internal_multi_depth_path,
                &["checks", "post_publish_failure_smoke_ran"],
                "internal canonical multi-depth target outage detection and recovery smoke",
            );
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_multi_depth_restart_recovery",
                &internal_multi_depth_path,
                &["checks", "orchestrator_restart_smoke_ran"],
                "internal canonical multi-depth restart recovery smoke",
            );
            push_bool_gate_finding(
                &mut findings,
                &report,
                "p6_internal_multi_depth_load_soak",
                &internal_multi_depth_path,
                &["checks", "load_soak_ran"],
                "internal canonical multi-depth load/soak smoke",
            );
            if bool_gate_set_complete(
                &report,
                &[
                    &["checks", "multi_depth_published"],
                    &["checks", "post_publish_failure_smoke_ran"],
                    &["checks", "orchestrator_restart_smoke_ran"],
                    &["checks", "load_soak_ran"],
                ],
            ) && let Err(err) =
                validate_internal_k8s_multi_depth_completion_gates(&report, true, true, true, true)
            {
                findings.push(P6CompletionFinding {
                    gate: "p6_internal_multi_depth_completion_schema",
                    evidence: format!(
                        "{} failed full completion verifier: {err}",
                        internal_multi_depth_path.display()
                    ),
                    missing:
                        "internal canonical multi-depth published report with clean completion schema"
                            .to_string(),
                });
            }
        }
        Err(err) => findings.push(P6CompletionFinding {
            gate: "p6_internal_multi_depth_ready_plan",
            evidence: format!("{} unavailable: {err}", internal_multi_depth_path.display()),
            missing: "internal canonical multi-depth readiness/publish report".to_string(),
        }),
    }

    let internal_planner_path = report_dir.join("guarded-kubernetes-planner-activation-latest.json");
    match read_json_report(&internal_planner_path) {
        Ok(report) => {
            if let Err(err) = validate_internal_k8s_planner_activation_report(&report, None, false)
            {
                findings.push(P6CompletionFinding {
                    gate: "p6_internal_planner_mutation_policy",
                    evidence: format!(
                        "{} failed verifier: {err}",
                        internal_planner_path.display()
                    ),
                    missing: "internal planner mutation evidence with default-off block and approved policy-gated mutation path".to_string(),
                });
            }
        }
        Err(err) => findings.push(P6CompletionFinding {
            gate: "p6_internal_planner_mutation_policy",
            evidence: format!("{} unavailable: {err}", internal_planner_path.display()),
            missing: "internal planner mutation evidence with default-off block and approved policy-gated mutation path".to_string(),
        }),
    }

    findings
}

fn p6_restart_preflight_evidence(report_dir: &Path) -> Option<String> {
    let expected_errors = [ORCH_ASSIGNMENT_STATE_ENV.to_string()];
    for report_name in [
        "guarded-kubernetes-restart-readiness-preflight.json",
        "guarded-kubernetes-restart-readiness-negative.json",
    ] {
        let path = report_dir.join(report_name);
        let Ok(report) = read_json_report(&path) else {
            continue;
        };
        if validate_internal_k8s_activation_report(
            &report,
            false,
            false,
            false,
            false,
            None,
            &expected_errors,
        )
        .is_err()
        {
            continue;
        }
        let stage = json_str(&report, &["stage"]).unwrap_or("<unknown>");
        let storage_configured =
            json_bool(&report, &["checks", "assignment_state_storage_configured"])
                .map(|value| value.to_string())
                .unwrap_or_else(|_| "unknown".to_string());
        return Some(format!(
            "{} accepted as negative restart preflight: stage={stage}, activation_mutated=false, assignment_state_storage_configured={storage_configured}, expected preflight error contains {}",
            path.display(),
            ORCH_ASSIGNMENT_STATE_ENV
        ));
    }
    None
}

fn p6_live_metrics_readiness_evidence(report_dir: &Path) -> Option<String> {
    let path = report_dir.join("guarded-kubernetes-live-metrics-readiness-negative.json");
    let report = read_json_report(&path).ok()?;
    if validate_internal_k8s_activation_report(&report, false, false, false, false, None, &[])
        .is_err()
    {
        return None;
    }
    let stage = json_str(&report, &["stage"]).ok()?;
    let status = json_str(&report, &["plan", "status"]).ok()?;
    if status == "ready" {
        return None;
    }
    let source = json_str(&report, &["plan", "preview", "source"]).ok()?;
    if !source.starts_with("live_worker_metrics:") {
        return None;
    }
    Some(format!(
        "{} recognized as negative live-metrics readiness: stage={stage}, plan.status={status}, activation_mutated=false, plan.preview.source starts with live_worker_metrics",
        path.display()
    ))
}

fn bool_gate_set_complete(report: &serde_json::Value, paths: &[&[&str]]) -> bool {
    paths
        .iter()
        .all(|path| json_path(report, path).is_some_and(|value| value.as_bool() == Some(true)))
}

fn normalize_historical_p5_split_report_for_audit(report: &serde_json::Value) -> serde_json::Value {
    if json_str(report, &["rollback_policy", "merge_activation"])
        .is_ok_and(|value| value == P5_HISTORICAL_ROLLBACK_MERGE_ACTIVATION)
    {
        let mut normalized = report.clone();
        if let Some(policy) = normalized
            .get_mut("rollback_policy")
            .and_then(|value| value.as_object_mut())
        {
            policy.insert(
                "merge_activation".to_string(),
                serde_json::json!(P5_ROLLBACK_MERGE_ACTIVATION),
            );
        }
        normalized
    } else {
        report.clone()
    }
}

fn push_bool_gate_finding(
    findings: &mut Vec<P6CompletionFinding>,
    report: &serde_json::Value,
    gate: &'static str,
    path: &Path,
    json_path_parts: &[&str],
    missing: &str,
) {
    if json_path(report, json_path_parts).is_none_or(|value| value.as_bool() != Some(true)) {
        findings.push(P6CompletionFinding {
            gate,
            evidence: format!(
                "{} has {}={:?}",
                path.display(),
                json_path_parts.join("."),
                json_path(report, json_path_parts)
            ),
            missing: missing.to_string(),
        });
    }
}

fn validate_split_activation_plan_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.split_activation_plan.v1")?;
    assert_json_str_eq(report, &["status"], "ready")?;
    assert_json_bool_eq(report, &["activation_mutated"], false)?;
    assert_json_bool_eq(report, &["preview", "assignments_changed"], false)?;
    assert_json_array_len(report, &["recommendation", "targets"], 4)?;
    assert_remaining_uncovered_only(report, "guarded_kubernetes_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_split_activation_live_metrics_plan_report(report: &serde_json::Value) -> Result<()> {
    let source = json_str(report, &["preview", "source"])?;
    if !source.starts_with("live_worker_metrics:") {
        bail!("split activation plan report source is not live_worker_metrics: {source}");
    }
    assert_json_number_at_least(report, &["preview", "plan_count"], 1.0)?;
    assert_json_array_len(report, &["recommendation", "targets"], 4)?;
    Ok(())
}

fn validate_merge_activation_plan_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.merge_activation_plan.v1")?;
    assert_json_str_eq(report, &["status"], "ready")?;
    assert_json_bool_eq(report, &["activation_mutated"], false)?;
    assert_json_bool_eq(report, &["preview", "assignments_changed"], false)?;
    assert_json_str_eq(report, &["preview", "selected_plan", "kind"], "merge")?;
    assert_json_array_len(report, &["recommendation", "siblings"], 4)?;
    assert_json_str_eq(report, &["runtime_activation", "state"], "manual_available")?;
    assert_remaining_uncovered_absent(report, "guarded_kubernetes_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_planner_activation_report(
    report: &serde_json::Value,
    require_published: bool,
) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.planner_activation.v1")?;
    assert_json_str_eq(report, &["activation_mode"], "policy_gated")?;
    let planner_kind = json_str(report, &["planner_kind"])?;
    match planner_kind {
        "split" => {
            assert_json_str_eq(
                report,
                &["plan", "schema"],
                "tessera.split_activation_plan.v1",
            )?;
            assert_json_array_len(report, &["plan", "recommendation", "targets"], 4)?;
        }
        "merge" => {
            assert_json_str_eq(
                report,
                &["plan", "schema"],
                "tessera.merge_activation_plan.v1",
            )?;
            assert_json_array_len(report, &["plan", "recommendation", "siblings"], 4)?;
        }
        other => bail!("planner activation report has unknown planner_kind `{other}`"),
    }
    assert_json_str_eq(
        report,
        &["policy", "required_policy_id"],
        PLANNER_MUTATION_POLICY_ID,
    )?;
    assert_json_bool_eq(report, &["checks", "policy_gate_default_off"], true)?;
    assert_json_bool_eq(report, &["checks", "policy_id_required"], true)?;

    if require_published {
        assert_json_str_eq(report, &["status"], "published")?;
        assert_json_bool_eq(report, &["activation_mutated"], true)?;
        assert_json_bool_eq(report, &["policy", "accepted"], true)?;
        assert_json_bool_eq(report, &["checks", "planner_selected_ready_plan"], true)?;
        assert_json_bool_eq(report, &["checks", "policy_accepted"], true)?;
        assert_json_bool_eq(report, &["checks", "activation_mutated"], true)?;
        assert_json_bool_eq(report, &["checks", "submit_activation_published"], true)?;
        assert_json_bool_eq(report, &["checks", "assignments_changed"], true)?;
        assert_json_str_eq(report, &["response", "state"], "published")?;
        assert_remaining_uncovered_only(report, "guarded_kubernetes_planner_mutation")?;
    } else {
        assert_json_str_eq(report, &["status"], "blocked_by_policy")?;
        assert_json_bool_eq(report, &["activation_mutated"], false)?;
        assert_json_bool_eq(report, &["policy", "accepted"], false)?;
        assert_json_bool_eq(report, &["checks", "policy_accepted"], false)?;
        assert_json_bool_eq(report, &["checks", "activation_mutated"], false)?;
        assert_json_bool_eq(report, &["checks", "submit_activation_published"], false)?;
        assert_remaining_uncovered_contains(report, "policy_approved_planner_mutation")?;
    }
    assert_remaining_uncovered_absent(report, "automatic_planner_merge_mutation")?;
    Ok(())
}

fn validate_planner_activation_live_metrics_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["planner_kind"], "split")?;
    assert_json_str_eq(
        report,
        &["plan", "schema"],
        "tessera.split_activation_plan.v1",
    )?;
    let source = json_str(report, &["plan", "preview", "source"])?;
    if !source.starts_with("live_worker_metrics:") {
        bail!("planner activation report source is not live_worker_metrics: {source}");
    }
    assert_json_number_at_least(report, &["plan", "preview", "plan_count"], 1.0)?;
    assert_json_array_len(report, &["plan", "recommendation", "targets"], 4)?;
    Ok(())
}

fn validate_merge_activation_smoke_report(
    report: &serde_json::Value,
    require_restart: bool,
) -> Result<()> {
    let expected_schema = if require_restart {
        "tessera.merge_activation_restart_smoke.v1"
    } else {
        "tessera.merge_activation_smoke.v1"
    };
    assert_json_str_eq(report, &["schema"], expected_schema)?;
    assert_json_str_eq(report, &["activation_mode"], "manual")?;
    assert_json_str_eq(
        report,
        &["operator_plan", "schema"],
        "tessera.merge_activation_plan.v1",
    )?;
    let plan_report_path = json_str(report, &["operator_plan", "report_path"])?;
    if plan_report_path.trim().is_empty() {
        bail!("merge activation smoke report has empty operator_plan.report_path");
    }
    let owner_worker_id = json_str(report, &["owner_worker_id"])?;
    if owner_worker_id.trim().is_empty() {
        bail!("merge activation smoke report has empty owner_worker_id");
    }
    assert_json_array_len(report, &["merged_children"], 4)?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_merge_activation_published"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "orchestrator_listing_parent_route"],
        true,
    )?;
    assert_json_number_at_least(report, &["checks", "gateway_ready_routes"], 1.0)?;
    assert_json_bool_eq(report, &["checks", "parent_ping_route"], true)?;
    assert_json_bool_eq(report, &["checks", "worker_coalesced_child_actors"], true)?;
    assert_json_bool_eq(report, &["checks", "stable_session_parent_move"], true)?;
    assert_json_number_at_least(
        report,
        &["checks", "remote_delta_frames_before_parent_delta"],
        1.0,
    )?;
    validate_manual_merge_rollback_policy(report)?;
    validate_merge_actor_state_recovery_policy(report)?;
    assert_remaining_uncovered_absent(report, "runtime_merge_activation")?;

    if require_restart {
        let assignment_state_path = json_str(report, &["assignment_state_path"])?;
        if assignment_state_path.trim().is_empty() {
            bail!("merge activation restart smoke report has empty assignment_state_path");
        }
        assert_json_bool_eq(report, &["checks", "orchestrator_restarted"], true)?;
        assert_json_bool_eq(
            report,
            &["checks", "restarted_orchestrator_loaded_parent_route"],
            true,
        )?;
        assert_json_number_at_least(
            report,
            &["checks", "gateway_ready_routes_after_restart"],
            1.0,
        )?;
    } else if let Some(restarted) = json_path(report, &["checks", "orchestrator_restarted"]) {
        match restarted.as_bool() {
            Some(false) | None => {}
            Some(true) => bail!(
                "merge activation smoke report is restart evidence but was validated as non-restart"
            ),
        }
    }
    Ok(())
}

fn validate_canonical_merge_activation_smoke_report(report: &serde_json::Value) -> Result<()> {
    let parent = json_cell_id(report, &["parent"])?;
    if parent.depth == 0 || parent.sub != 0 || !parent.is_canonical_leaf() {
        bail!(
            "canonical merge activation report parent must be a non-root canonical leaf, got {:?}",
            parent
        );
    }
    let child_values = json_array(report, &["merged_children"])?;
    let mut children = Vec::with_capacity(child_values.len());
    for (index, child) in child_values.iter().enumerate() {
        let cell = serde_json::from_value::<CellId>(child.clone())
            .with_context(|| format!("canonical merge child index {index} is not a CellId"))?;
        children.push(cell);
    }
    match parent.child_family_kind(&children) {
        Some(CellChildFamilyKind::CanonicalLeaf) => {}
        other => bail!(
            "canonical merge activation report expected canonical child family for {:?}, got kind={:?} children={:?}",
            parent,
            other,
            children
        ),
    }
    assert_remaining_uncovered_contains(report, "guarded_kubernetes_merge_activation_smoke")?;
    Ok(())
}

fn validate_merge_activation_cross_worker_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.merge_activation_cross_worker_smoke.v1",
    )?;
    assert_json_str_eq(report, &["activation_mode"], "manual")?;
    assert_json_str_eq(
        report,
        &["operator_plan", "schema"],
        "manual_cross_worker_merge_replay.v1",
    )?;
    assert_json_str_eq(report, &["owner_worker_id"], "worker-a")?;
    assert_json_str_eq(report, &["remote_source_worker_id"], "worker-b")?;
    assert_json_array_len(report, &["merged_children"], 4)?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_merge_activation_published"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "orchestrator_listing_parent_route"],
        true,
    )?;
    assert_json_number_at_least(report, &["checks", "gateway_ready_routes"], 1.0)?;
    assert_json_bool_eq(report, &["checks", "parent_ping_route"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "target_worker_coalesced_local_children"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "remote_child_replayed_to_parent"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "stable_session_parent_move_local_child"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "stable_session_parent_move_remote_child"],
        true,
    )?;
    assert_json_number_at_least(report, &["checks", "worker_a_parent_actor_count"], 2.0)?;
    assert_json_number_at_least(report, &["checks", "worker_a_relay_connections_total"], 1.0)?;
    assert_json_number_at_least(
        report,
        &["checks", "worker_b_remote_relay_frames_sent_total"],
        1.0,
    )?;
    validate_manual_merge_rollback_policy(report)?;
    validate_merge_actor_state_recovery_policy(report)?;
    assert_remaining_uncovered_absent(report, "cross_worker_merge_replay")?;
    assert_remaining_uncovered_absent(report, "runtime_merge_activation")?;
    Ok(())
}

fn validate_merge_activation_failure_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.merge_activation_failure_smoke.v1",
    )?;
    assert_json_str_eq(report, &["activation_mode"], "manual")?;
    assert_json_str_eq(
        report,
        &["operator_plan", "schema"],
        "tessera.merge_activation_plan.v1",
    )?;
    let plan_report_path = json_str(report, &["operator_plan", "report_path"])?;
    if plan_report_path.trim().is_empty() {
        bail!("merge activation failure report has empty operator_plan.report_path");
    }
    let owner_worker_id = json_str(report, &["owner_worker_id"])?;
    if owner_worker_id.trim().is_empty() {
        bail!("merge activation failure report has empty owner_worker_id");
    }
    assert_json_array_len(report, &["merged_children"], 4)?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_merge_activation_published"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "orchestrator_listing_parent_route"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ready_routes_before_failure"],
        1.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "parent_ping_route_before_failure"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "worker_coalesced_child_actors"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "stable_session_parent_move_before_failure"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "owner_worker_outage_detected"], true)?;
    assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
    assert_json_bool_eq(
        report,
        &["checks", "parent_assignment_stayed_published"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ready_routes_after_failure"],
        1.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "owner_worker_restart_recovered_parent_route"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "parent_ping_route_after_recovery"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ready_routes_after_recovery"],
        1.0,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "remote_delta_frames_before_parent_delta"],
        1.0,
    )?;
    let failure_owner = json_str(report, &["failure", "owner_worker_id"])?;
    if failure_owner != owner_worker_id {
        bail!(
            "merge activation failure report owner mismatch: failure.owner_worker_id={failure_owner} owner_worker_id={owner_worker_id}"
        );
    }
    let failure_error = json_str(report, &["failure", "error"])?;
    if failure_error.trim().is_empty() {
        bail!("merge activation failure report has empty failure.error");
    }
    validate_manual_merge_rollback_policy(report)?;
    let failure_recovery = json_str(report, &["rollback_policy", "failure_recovery"])?;
    if !failure_recovery.contains("convergence checks") {
        bail!("merge activation failure report does not describe recovery convergence checks");
    }
    validate_merge_actor_state_recovery_policy(report)?;
    assert_remaining_uncovered_absent(report, "runtime_merge_activation")?;
    assert_remaining_uncovered_absent(report, "merge_failure_recovery_smoke")?;
    assert_remaining_uncovered_absent(report, "merge_actor_state_recovery_after_owner_restart")?;
    Ok(())
}

fn validate_merge_activation_soak_report(
    report: &serde_json::Value,
    min_iterations: u32,
) -> Result<()> {
    let min_total = f64::from(min_iterations) * 4.0;
    assert_json_str_eq(report, &["schema"], "tessera.merge_activation_soak.v1")?;
    assert_json_str_eq(report, &["activation_mode"], "manual")?;
    assert_json_str_eq(
        report,
        &["operator_plan", "schema"],
        "tessera.merge_activation_plan.v1",
    )?;
    let plan_report_path = json_str(report, &["operator_plan", "report_path"])?;
    if plan_report_path.trim().is_empty() {
        bail!("merge activation soak report has empty operator_plan.report_path");
    }
    let owner_worker_id = json_str(report, &["owner_worker_id"])?;
    if owner_worker_id.trim().is_empty() {
        bail!("merge activation soak report has empty owner_worker_id");
    }
    assert_json_array_len(report, &["merged_children"], 4)?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_merge_activation_published"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "orchestrator_listing_parent_route"],
        true,
    )?;
    assert_json_number_at_least(report, &["checks", "gateway_ready_routes_after_soak"], 1.0)?;
    assert_json_number_at_least(
        report,
        &["traffic", "iterations_per_actor"],
        f64::from(min_iterations),
    )?;
    assert_json_number_at_least(report, &["checks", "parent_ping_iterations"], min_total)?;
    assert_json_number_at_least(report, &["checks", "parent_move_iterations"], min_total)?;
    assert_json_bool_eq(report, &["checks", "worker_coalesced_child_actors"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "stable_session_parent_move_before_soak"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "remote_delta_frames_before_parent_delta"],
        1.0,
    )?;
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
    validate_manual_merge_rollback_policy(report)?;
    validate_merge_actor_state_recovery_policy(report)?;
    assert_remaining_uncovered_absent(report, "runtime_merge_activation")?;
    assert_remaining_uncovered_absent(report, "merge_load_soak")?;
    assert_remaining_uncovered_absent(report, "merge_actor_state_recovery_after_owner_restart")?;
    Ok(())
}

fn validate_manual_merge_rollback_policy(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["rollback_policy", "policy_id"],
        "operator_controlled_manual_merge_v1",
    )?;
    assert_json_bool_eq(report, &["rollback_policy", "automatic_rollback"], false)?;
    let backout = json_str(report, &["rollback_policy", "backout"])?;
    if !backout.contains("manual split activation") {
        bail!("merge activation rollback policy does not describe manual split backout");
    }
    Ok(())
}

fn validate_merge_actor_state_recovery_policy(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["actor_state_recovery_policy", "policy_id"],
        "volatile_worker_actor_state_rejoin_required_v1",
    )?;
    assert_json_bool_eq(
        report,
        &["actor_state_recovery_policy", "durable_actor_state"],
        false,
    )?;
    assert_json_str_eq(
        report,
        &[
            "actor_state_recovery_policy",
            "owner_worker_restart_actor_recovery",
        ],
        "excluded_until_durable_worker_state",
    )?;
    let recovery = json_str(
        report,
        &["actor_state_recovery_policy", "operator_recovery"],
    )?;
    if !recovery.contains("rejoin") || !recovery.contains("parent route convergence") {
        bail!(
            "merge actor-state recovery policy must require parent route convergence and client rejoin/reseed"
        );
    }
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
    assert_remaining_uncovered_only(report, "guarded_kubernetes_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_multi_depth_activation_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.multi_depth_activation_smoke.v1",
    )?;
    assert_json_str_eq(report, &["activation_mode"], "manual")?;
    validate_multi_depth_activation_report_topology(report)?;
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
    assert_json_number_at_least(report, &["checks", "gateway_route_change_reconnects"], 1.0)?;
    assert_json_number_at_least(report, &["checks", "target_worker_relay_connections"], 1.0)?;
    assert_remaining_uncovered_only(report, "guarded_kubernetes_multi_depth_activation_smoke")?;
    assert_remaining_uncovered_absent(report, "multi_depth_failure_recovery_smoke")?;
    assert_remaining_uncovered_absent(report, "multi_depth_restart_recovery_smoke")?;
    Ok(())
}

fn validate_multi_depth_activation_failure_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.multi_depth_activation_failure_smoke.v1",
    )?;
    assert_json_str_eq(report, &["activation_mode"], "manual")?;
    validate_multi_depth_activation_report_topology(report)?;
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
    assert_json_array_nonempty(report, &["checks", "failed_child_cells"])?;
    assert_json_array_nonempty(report, &["checks", "succeeded_child_cells_during_failure"])?;
    assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
    assert_json_bool_eq(report, &["checks", "operator_recovery_required"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "target_worker_restart_recovered_convergence"],
        true,
    )?;
    assert_json_array_len(report, &["checks", "recovered_child_cells"], 4)?;
    assert_json_array_nonempty(report, &["failure_probe", "failures"])?;
    assert_json_array_empty_or_missing(report, &["recovery_probe", "failures"])?;
    validate_p5_rollback_policy(report)?;
    assert_remaining_uncovered_only(report, "guarded_kubernetes_multi_depth_activation_smoke")?;
    assert_remaining_uncovered_absent(report, "multi_depth_failure_recovery_smoke")?;
    assert_remaining_uncovered_absent(report, "multi_depth_restart_recovery_smoke")?;
    Ok(())
}

fn validate_multi_depth_activation_restart_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.multi_depth_activation_restart_smoke.v1",
    )?;
    assert_json_str_eq(
        report,
        &["activation_mode"],
        "manual_publish_then_default_off_restart",
    )?;
    validate_multi_depth_activation_report_topology(report)?;
    let assignment_state_path = json_str(report, &["assignment_state_path"])?;
    if assignment_state_path.trim().is_empty() {
        bail!("multi-depth activation restart report has empty assignment_state_path");
    }
    assert_json_bool_eq(
        report,
        &["checks", "submit_split_activation_published"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "assignment_state_file_written"], true)?;
    assert_json_bool_eq(report, &["checks", "orchestrator_restarted"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "restarted_with_manual_activation_disabled"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "restarted_orchestrator_loaded_child_routes"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "worker_assignment_refresh_after_restart"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ready_routes_after_restart"],
        4.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "child_ping_all_routes_after_restart"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "remote_aoi_interest_resync_after_restart"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "worker_b_remote_interest_clients_after_restart"],
        1.0,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "worker_b_remote_interest_cells_after_restart"],
        1.0,
    )?;
    assert_json_array_len(
        report,
        &["checks", "recovered_child_cells_after_restart"],
        4,
    )?;
    assert_json_array_empty_or_missing(report, &["restart_probe", "failures"])?;
    assert_remaining_uncovered_only(report, "guarded_kubernetes_multi_depth_activation_smoke")?;
    assert_remaining_uncovered_absent(report, "multi_depth_restart_recovery_smoke")?;
    Ok(())
}

fn validate_multi_depth_activation_soak_report(
    report: &serde_json::Value,
    min_iterations: u32,
) -> Result<()> {
    let min_total = f64::from(min_iterations) * 4.0;
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.multi_depth_activation_soak.v1",
    )?;
    assert_json_str_eq(report, &["activation_mode"], "manual")?;
    validate_multi_depth_activation_report_topology(report)?;
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
        f64::from(min_iterations),
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
    assert_remaining_uncovered_only(report, "guarded_kubernetes_multi_depth_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_multi_depth_activation_report_topology(report: &serde_json::Value) -> Result<()> {
    let parent = json_cell_id(report, &["parent"])?;
    if parent.depth == 0 || parent.sub != 0 || !parent.is_canonical_leaf() {
        bail!(
            "multi-depth activation report parent must be a non-root canonical leaf, got {:?}",
            parent
        );
    }
    let expected_children = parent
        .canonical_children()
        .ok_or_else(|| anyhow::anyhow!("multi-depth activation parent cannot produce children"))?;
    let children = json_array(report, &["children"])?;
    if children.len() != 4 {
        bail!(
            "multi-depth activation report expected four children, got {}",
            children.len()
        );
    }
    let mut actual_children = Vec::with_capacity(children.len());
    for (index, child) in children.iter().enumerate() {
        let cell = json_cell_id(child, &["cell"])
            .with_context(|| format!("multi-depth activation child index {index}"))?;
        let worker_id = json_str(child, &["worker_id"])?;
        if worker_id.trim().is_empty() {
            bail!("multi-depth activation child index {index} has empty worker_id");
        }
        actual_children.push(cell);
    }
    for expected in expected_children {
        let count = actual_children
            .iter()
            .filter(|actual| **actual == expected)
            .count();
        if count != 1 {
            bail!(
                "multi-depth activation report expected canonical child {:?} exactly once, got count={count}",
                expected
            );
        }
    }
    Ok(())
}

fn validate_activation_smoke_live_metrics_plan_link(report: &serde_json::Value) -> Result<()> {
    let source = json_str(report, &["operator_plan", "source"])?;
    if !source.starts_with("live_worker_metrics:") {
        bail!("activation smoke report operator_plan.source is not live_worker_metrics: {source}");
    }
    let report_path = json_str(report, &["operator_plan", "report_path"])?;
    if report_path.trim().is_empty() {
        bail!("activation smoke report operator_plan.report_path is empty");
    }
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
    assert_remaining_uncovered_only(report, "guarded_kubernetes_activation_smoke")?;
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
    assert_remaining_uncovered_only(report, "guarded_kubernetes_activation_smoke")?;
    validate_p5_rollback_policy(report)?;
    Ok(())
}

fn validate_activation_restart_smoke_report(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.activation_restart_smoke.v1")?;
    assert_json_bool_eq(
        report,
        &["checks", "submit_split_activation_published"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "assignment_state_file_written"], true)?;
    assert_json_bool_eq(report, &["checks", "orchestrator_restarted"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "restarted_with_manual_activation_disabled"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "restarted_orchestrator_loaded_child_routes"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "worker_assignment_refresh_after_restart"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ready_routes_after_restart"],
        4.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "child_ping_all_routes_after_restart"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "child_move_iterations_after_restart"],
        1.0,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "remote_aoi_frames_observed_after_restart"],
        1.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "live_remote_aoi_resync_snapshot_after_restart"],
        true,
    )?;
    assert_probe_succeeded_all(report, &["restart_probe"])?;
    assert_remaining_uncovered_only(report, "guarded_kubernetes_restart_recovery_smoke")?;
    Ok(())
}

fn validate_internal_k8s_activation_report(
    report: &serde_json::Value,
    require_published: bool,
    require_failure: bool,
    require_restart: bool,
    require_live_metrics_plan: bool,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.guarded_kubernetes_activation_smoke.v1",
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
    if require_live_metrics_plan {
        validate_internal_k8s_live_metrics_plan(report)?;
    }
    if require_failure {
        validate_internal_k8s_published_report(report, true)?;
        if require_restart {
            validate_internal_k8s_restart_report(report)?;
        }
        return Ok(());
    }
    if require_published {
        validate_internal_k8s_published_report(report, false)?;
    }
    if require_restart {
        validate_internal_k8s_published_report(report, false)?;
        validate_internal_k8s_restart_report(report)?;
    }
    Ok(())
}

fn validate_internal_k8s_merge_activation_report(
    report: &serde_json::Value,
    require_ready_plan: bool,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.guarded_kubernetes_merge_activation_smoke.v1",
    )?;
    let stage = json_str(report, &["stage"])?;
    match stage {
        "blocked_before_plan"
        | "blocked_before_activation"
        | "planned_without_activation"
        | "published" => {}
        other => bail!("internal merge activation report has unknown stage `{other}`"),
    }
    if stage == "published" {
        assert_json_bool_eq(report, &["activation_mutated"], true)?;
        assert_json_bool_eq(report, &["activation_allowed"], true)?;
    } else {
        assert_json_bool_eq(report, &["activation_mutated"], false)?;
        assert_json_bool_eq(report, &["activation_allowed"], false)?;
        assert_json_bool_eq(report, &["checks", "merge_published"], false)?;
        assert_json_bool_eq(report, &["checks", "post_publish_failure_smoke_ran"], false)?;
        assert_json_bool_eq(report, &["checks", "orchestrator_restart_smoke_ran"], false)?;
        assert_json_bool_eq(report, &["checks", "load_soak_ran"], false)?;
    }
    assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
    validate_manual_merge_rollback_policy(report)?;
    validate_merge_actor_state_recovery_policy(report)?;
    assert_report_deployment_roles(report, &["orchestrator", "gateway", "owner_worker"])?;
    if !expect_preflight_errors.is_empty() {
        assert_preflight_errors_contain(report, expect_preflight_errors)?;
    }
    if let Some(expected_image) = expected_image {
        assert_report_images_match(report, expected_image)?;
    }
    if require_ready_plan {
        if stage != "planned_without_activation" && stage != "published" {
            bail!("internal merge activation report is not ready or published: stage={stage}");
        }
        assert_json_bool_eq(report, &["checks", "merge_plan_ready"], true)?;
        assert_json_str_eq(report, &["plan", "status"], "ready")?;
        assert_json_bool_eq(report, &["plan", "activation_mutated"], false)?;
        assert_json_array_len(report, &["plan", "recommendation", "siblings"], 4)?;
        let owner = json_str(report, &["plan", "recommendation", "owner_worker_id"])?;
        if owner.trim().is_empty() {
            bail!("internal merge activation report has empty owner_worker_id");
        }
        let command = json_str(report, &["plan", "recommendation", "submission_command"])?;
        if !command.contains("merge-activation") {
            bail!("internal merge activation report missing merge submission command");
        }
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_merge_ready_plan")?;
    }
    Ok(())
}

fn validate_internal_k8s_multi_depth_activation_report(
    report: &serde_json::Value,
    require_ready_plan: bool,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.guarded_kubernetes_multi_depth_activation_smoke.v1",
    )?;
    let stage = json_str(report, &["stage"])?;
    match stage {
        "blocked_before_plan"
        | "blocked_before_activation"
        | "planned_without_activation"
        | "published" => {}
        other => bail!("internal multi-depth activation report has unknown stage `{other}`"),
    }
    if stage == "published" {
        assert_json_bool_eq(report, &["activation_mutated"], true)?;
        assert_json_bool_eq(report, &["activation_allowed"], true)?;
    } else {
        assert_json_bool_eq(report, &["activation_mutated"], false)?;
        assert_json_bool_eq(report, &["activation_allowed"], false)?;
        assert_json_bool_eq(report, &["checks", "multi_depth_published"], false)?;
        assert_json_bool_eq(report, &["checks", "post_publish_failure_smoke_ran"], false)?;
        assert_json_bool_eq(report, &["checks", "orchestrator_restart_smoke_ran"], false)?;
        assert_json_bool_eq(report, &["checks", "load_soak_ran"], false)?;
    }
    assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
    validate_p5_rollback_policy(report)?;
    assert_report_deployment_roles(
        report,
        &["orchestrator", "gateway", "source_worker", "target_worker"],
    )?;
    if !expect_preflight_errors.is_empty() {
        assert_preflight_errors_contain(report, expect_preflight_errors)?;
    }
    if let Some(expected_image) = expected_image {
        assert_report_images_match(report, expected_image)?;
    }
    if json_path(report, &["plan", "parent"]).is_some() {
        validate_internal_k8s_multi_depth_plan_topology(report)?;
    }
    if require_ready_plan {
        if stage != "planned_without_activation" && stage != "published" {
            bail!(
                "internal multi-depth activation report is not ready or published: stage={stage}"
            );
        }
        assert_json_bool_eq(report, &["checks", "multi_depth_plan_ready"], true)?;
        assert_json_str_eq(report, &["plan", "status"], "ready")?;
        assert_json_bool_eq(report, &["plan", "activation_mutated"], false)?;
        validate_internal_k8s_multi_depth_plan_topology(report)?;
        let source = json_str(report, &["plan", "source_worker_id"])?;
        if source.trim().is_empty() {
            bail!("internal multi-depth activation report has empty source_worker_id");
        }
        let command = json_str(report, &["plan", "submission_command"])?;
        if !command.contains("split-activation") || !command.contains("--target-cell") {
            bail!(
                "internal multi-depth activation report missing explicit split submission command"
            );
        }
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_multi_depth_ready_plan")?;
    }
    Ok(())
}

fn validate_internal_k8s_operation_report(
    report: &serde_json::Value,
    requirements: InternalP7OperationRequirements,
    expected_image: Option<&str>,
    expect_preflight_errors: &[String],
) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.guarded_kubernetes_p7_operation_smoke.v1",
    )?;
    let stage = json_str(report, &["stage"])?;
    match stage {
        "blocked_before_proposal"
        | "blocked_before_execution"
        | "planned_without_execution"
        | "recovery_required"
        | "completed" => {}
        other => bail!("internal P7 operation report has unknown stage `{other}`"),
    }
    assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
    assert_report_deployment_roles(report, &["orchestrator", "gateway", "owner_worker"])?;
    if !expect_preflight_errors.is_empty() {
        assert_preflight_errors_contain(report, expect_preflight_errors)?;
    }
    if let Some(expected_image) = expected_image {
        assert_report_images_match(report, expected_image)?;
    }
    if stage == "completed" || stage == "recovery_required" {
        assert_json_bool_eq(report, &["execution_mutated"], true)?;
        assert_json_bool_eq(report, &["execution_allowed"], true)?;
    } else {
        assert_json_bool_eq(report, &["execution_mutated"], false)?;
        assert_json_bool_eq(report, &["execution_allowed"], false)?;
        assert_json_bool_eq(report, &["checks", "execution_published"], false)?;
        assert_json_bool_eq(report, &["checks", "observation_completed"], false)?;
        assert_json_bool_eq(report, &["checks", "load_soak_ran"], false)?;
    }

    if stage == "planned_without_execution" || stage == "completed" || stage == "recovery_required"
    {
        assert_json_str_eq(report, &["operation", "kind"], "merge")?;
        assert_json_str_eq(
            report,
            &["operation", "policy_id"],
            "operator_approved_dynamic_operation_v1",
        )?;
        let operation_id = json_str(report, &["operation", "operation_id"])?;
        if operation_id.trim().is_empty() {
            bail!("internal P7 operation report has empty operation_id");
        }
        let proposal_hash = json_str(report, &["operation", "proposal_hash"])?;
        if proposal_hash.trim().is_empty() {
            bail!("internal P7 operation report has empty proposal_hash");
        }
        let owner = json_str(report, &["operation", "owner_worker_id"])?;
        if owner.trim().is_empty() {
            bail!("internal P7 operation report has empty owner_worker_id");
        }
        json_cell_id(report, &["operation", "parent"])?;
        assert_json_bool_eq(report, &["checks", "operation_recorded"], true)?;
        assert_json_bool_eq(
            report,
            &["responses", "proposal", "assignments_changed"],
            false,
        )?;
        let ledger = json_field(report, &["ledger", "snapshot"])?;
        validate_p7_operation_ledger(ledger, false, false, false, false, false)?;
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_operation_proposal_smoke")?;
    }

    if requirements.require_published_execution
        || requirements.require_completed_observation
        || requirements.require_soak
        || requirements.require_recovery_required
        || requirements.require_restart
    {
        if requirements.require_completed_observation
            || requirements.require_soak
            || requirements.require_restart
        {
            assert_json_str_eq(report, &["stage"], "completed")?;
        } else if requirements.require_recovery_required {
            assert_json_str_eq(report, &["stage"], "recovery_required")?;
        } else if stage != "completed" && stage != "recovery_required" {
            bail!(
                "internal P7 operation report must be completed or recovery_required for published execution evidence, got `{stage}`"
            );
        }
        assert_json_bool_eq(report, &["checks", "approval_recorded"], true)?;
        assert_json_bool_eq(report, &["checks", "execution_published"], true)?;
        assert_json_str_eq(report, &["responses", "approval", "status"], "approved")?;
        assert_json_str_eq(report, &["responses", "execution", "status"], "published")?;
        assert_json_bool_eq(
            report,
            &["responses", "execution", "assignments_changed"],
            true,
        )?;
        assert_json_bool_eq(
            report,
            &["responses", "execution", "mutation_attempted"],
            true,
        )?;
        assert_json_bool_eq(
            report,
            &["responses", "execution", "mutation_allowed"],
            true,
        )?;
        let ledger = json_field(report, &["ledger", "snapshot"])?;
        validate_p7_operation_ledger(ledger, true, false, true, false, false)?;
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_operation_execution_smoke")?;
    }

    if requirements.require_completed_observation {
        assert_json_bool_eq(report, &["checks", "route_converged"], true)?;
        assert_json_bool_eq(report, &["checks", "worker_refreshed"], true)?;
        assert_json_bool_eq(report, &["checks", "traffic_confirmed"], true)?;
        assert_json_bool_eq(report, &["checks", "gateway_close_counters_clean"], true)?;
        assert_json_bool_eq(report, &["checks", "observation_completed"], true)?;
        assert_json_str_eq(report, &["responses", "observation", "status"], "completed")?;
        assert_json_bool_eq(
            report,
            &["responses", "observation", "observation_accepted"],
            true,
        )?;
        assert_json_number_at_least(report, &["gateway", "routes_after"], 1.0)?;
        assert_json_number_at_least(report, &["gateway", "ping_roundtrips"], 1.0)?;
        assert_json_number_at_least(report, &["gateway", "join_roundtrips"], 3.0)?;
        assert_json_number_at_least(report, &["gateway", "move_roundtrips"], 1.0)?;
        assert_json_number_at_least(report, &["worker", "parent_actor_count"], 1.0)?;
        let ledger = json_field(report, &["ledger", "snapshot"])?;
        validate_p7_operation_ledger(ledger, true, false, true, true, false)?;
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_operation_observation_smoke")?;
    }

    if requirements.require_soak {
        assert_json_bool_eq(report, &["checks", "load_soak_ran"], true)?;
        assert_json_bool_eq(report, &["soak", "ran"], true)?;
        assert_json_number_at_least(report, &["soak", "iterations"], 1.0)?;
        let expected_actor_requests = json_f64(report, &["soak", "expected_actor_requests"])?;
        assert_json_number_at_least(report, &["soak", "pings_ok"], expected_actor_requests)?;
        assert_json_number_at_least(report, &["soak", "moves_ok"], expected_actor_requests)?;
        assert_json_number_at_least(
            report,
            &["gateway", "ping_roundtrips"],
            expected_actor_requests,
        )?;
        assert_json_number_at_least(
            report,
            &["gateway", "move_roundtrips"],
            expected_actor_requests,
        )?;
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_operation_soak_smoke")?;
    }
    if requirements.require_restart {
        assert_json_bool_eq(report, &["checks", "orchestrator_restart_smoke_ran"], true)?;
        assert_json_bool_eq(
            report,
            &["cluster", "assignment_state_storage", "checked"],
            true,
        )?;
        assert_json_str_eq(
            report,
            &["cluster", "assignment_state_storage", "policy_id"],
            ORCH_ASSIGNMENT_STATE_STORAGE_POLICY,
        )?;
        assert_json_str_eq(
            report,
            &["cluster", "assignment_state_storage", "env"],
            ORCH_ASSIGNMENT_STATE_ENV,
        )?;
        let path = json_str(report, &["cluster", "assignment_state_storage", "path"])?;
        if path.trim().is_empty() {
            bail!("internal P7 operation restart report has empty assignment state path");
        }
        assert_json_bool_eq(report, &["checks", "route_converged"], true)?;
        assert_json_bool_eq(report, &["checks", "worker_refreshed"], true)?;
        assert_json_bool_eq(report, &["checks", "traffic_confirmed"], true)?;
        assert_json_bool_eq(report, &["checks", "gateway_close_counters_clean"], true)?;
        assert_json_bool_eq(report, &["checks", "observation_completed"], true)?;
        let ledger = json_field(report, &["ledger", "snapshot"])?;
        validate_p7_operation_ledger(ledger, true, false, true, true, false)?;
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_operation_restart_smoke")?;
    }
    if requirements.require_recovery_required {
        assert_json_bool_eq(report, &["checks", "route_converged"], true)?;
        assert_json_bool_eq(report, &["checks", "worker_refreshed"], true)?;
        assert_json_bool_eq(report, &["checks", "traffic_confirmed"], false)?;
        assert_json_bool_eq(report, &["checks", "gateway_close_counters_clean"], false)?;
        assert_json_bool_eq(report, &["checks", "observation_completed"], false)?;
        assert_json_bool_eq(report, &["checks", "owner_outage_detected"], true)?;
        assert_json_bool_eq(report, &["checks", "observation_recovery_required"], true)?;
        assert_json_bool_eq(report, &["checks", "operator_recovery_confirmed"], true)?;
        assert_json_bool_eq(report, &["checks", "ledger_recovery_required"], true)?;
        assert_json_bool_eq(report, &["failure", "ran"], true)?;
        let failure_error = json_str(report, &["failure", "failure_error"])?;
        if failure_error.trim().is_empty() {
            bail!("internal P7 operation recovery report has empty failure_error");
        }
        assert_json_number_at_least(report, &["failure", "original_replicas"], 1.0)?;
        assert_json_number_at_least(report, &["failure", "routes_after_failure"], 1.0)?;
        assert_json_number_at_least(report, &["failure", "routes_after_recovery"], 1.0)?;
        assert_json_str_eq(
            report,
            &["responses", "observation", "status"],
            "recovery_required",
        )?;
        assert_json_bool_eq(
            report,
            &["responses", "observation", "observation_accepted"],
            false,
        )?;
        let ledger = json_field(report, &["ledger", "snapshot"])?;
        validate_p7_operation_ledger(ledger, true, false, true, false, true)?;
        assert_remaining_uncovered_absent(
            report,
            "guarded_kubernetes_operation_failure_recovery_smoke",
        )?;
    }
    Ok(())
}

fn validate_internal_k8s_merge_completion_gates(
    report: &serde_json::Value,
    require_published: bool,
    require_failure: bool,
    require_restart: bool,
    require_soak: bool,
) -> Result<()> {
    if !(require_published || require_failure || require_restart || require_soak) {
        return Ok(());
    }
    assert_json_str_eq(report, &["stage"], "published")?;
    assert_json_bool_eq(report, &["activation_mutated"], true)?;
    assert_json_bool_eq(report, &["activation_allowed"], true)?;
    assert_json_bool_eq(report, &["checks", "merge_plan_ready"], true)?;
    assert_json_str_eq(report, &["plan", "status"], "ready")?;
    assert_json_bool_eq(report, &["plan", "activation_mutated"], false)?;
    assert_json_bool_eq(report, &["checks", "merge_published"], true)?;
    assert_remaining_uncovered_absent(report, "guarded_kubernetes_merge_activation_publish")?;

    if require_failure {
        assert_json_bool_eq(report, &["checks", "post_publish_failure_smoke_ran"], true)?;
        assert_json_bool_eq(
            report,
            &["checks", "owner_worker_restart_recovered_convergence"],
            true,
        )?;
        assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
        assert_remaining_uncovered_absent(
            report,
            "guarded_kubernetes_merge_failure_recovery_smoke",
        )?;
    }
    if require_restart {
        assert_json_bool_eq(report, &["checks", "orchestrator_restart_smoke_ran"], true)?;
        assert_remaining_uncovered_absent(
            report,
            "guarded_kubernetes_merge_restart_recovery_smoke",
        )?;
    }
    if require_soak {
        assert_json_bool_eq(report, &["checks", "load_soak_ran"], true)?;
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_merge_load_soak")?;
    }
    if require_published && require_failure && require_restart && require_soak {
        assert_remaining_uncovered_empty(report)?;
    }
    Ok(())
}

fn validate_internal_k8s_multi_depth_completion_gates(
    report: &serde_json::Value,
    require_published: bool,
    require_failure: bool,
    require_restart: bool,
    require_soak: bool,
) -> Result<()> {
    if !(require_published || require_failure || require_restart || require_soak) {
        return Ok(());
    }
    assert_json_str_eq(report, &["stage"], "published")?;
    assert_json_bool_eq(report, &["activation_mutated"], true)?;
    assert_json_bool_eq(report, &["activation_allowed"], true)?;
    assert_json_bool_eq(report, &["checks", "multi_depth_plan_ready"], true)?;
    assert_json_str_eq(report, &["plan", "status"], "ready")?;
    assert_json_bool_eq(report, &["plan", "activation_mutated"], false)?;
    assert_json_bool_eq(report, &["checks", "multi_depth_published"], true)?;
    validate_internal_k8s_multi_depth_plan_topology(report)?;
    assert_remaining_uncovered_absent(report, "guarded_kubernetes_multi_depth_activation_publish")?;

    if require_failure {
        assert_json_bool_eq(report, &["checks", "post_publish_failure_smoke_ran"], true)?;
        assert_json_bool_eq(
            report,
            &["checks", "target_worker_restart_recovered_convergence"],
            true,
        )?;
        assert_json_bool_eq(report, &["checks", "automatic_rollback_observed"], false)?;
        assert_remaining_uncovered_absent(
            report,
            "guarded_kubernetes_multi_depth_failure_recovery_smoke",
        )?;
    }
    if require_restart {
        assert_json_bool_eq(report, &["checks", "orchestrator_restart_smoke_ran"], true)?;
        assert_remaining_uncovered_absent(
            report,
            "guarded_kubernetes_multi_depth_restart_recovery_smoke",
        )?;
    }
    if require_soak {
        assert_json_bool_eq(report, &["checks", "load_soak_ran"], true)?;
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_multi_depth_load_soak")?;
    }
    if require_published && require_failure && require_restart && require_soak {
        assert_remaining_uncovered_empty(report)?;
    }
    Ok(())
}

fn validate_internal_k8s_planner_activation_report(
    report: &serde_json::Value,
    expected_image: Option<&str>,
    require_live_metrics_plan: bool,
) -> Result<()> {
    assert_json_str_eq(
        report,
        &["schema"],
        "tessera.guarded_kubernetes_planner_activation.v1",
    )?;
    assert_json_str_eq(report, &["activation_mode"], "policy_gated")?;
    assert_json_bool_eq(report, &["activation_mutated"], true)?;
    assert_json_bool_eq(report, &["cluster", "argocd", "checked"], true)?;
    assert_json_str_eq(report, &["cluster", "argocd", "sync"], "Synced")?;
    assert_json_str_eq(report, &["cluster", "argocd", "health"], "Healthy")?;
    assert_report_deployment_roles(
        report,
        &["orchestrator", "gateway", "source_worker", "target_worker"],
    )?;
    assert_json_array_len(report, &["preflight_errors"], 0)?;
    if let Some(expected_image) = expected_image {
        assert_report_images_match(report, expected_image)?;
    }

    let blocked = json_field(report, &["blocked_report"])?;
    validate_planner_activation_report(blocked, false)?;
    let published = json_field(report, &["published_report"])?;
    validate_planner_activation_report(published, true)?;
    if require_live_metrics_plan {
        validate_planner_activation_live_metrics_report(published)?;
    }

    assert_json_bool_eq(report, &["checks", "default_off_blocked"], true)?;
    assert_json_bool_eq(report, &["checks", "policy_approved_published"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "activation_mutated_only_after_policy"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "automatic_mutation_observed"], false)?;
    assert_remaining_uncovered_empty(report)?;
    Ok(())
}

fn validate_p6_gitops_rollout_report(
    report: &serde_json::Value,
    expected_image: Option<&str>,
) -> Result<()> {
    assert_json_str_eq(report, &["schema"], "tessera.p6_gitops_rollout.v1")?;
    let image = json_str(report, &["image", "name"])?;
    if image.trim().is_empty() {
        bail!("P6 rollout report has empty image.name");
    }
    if image.ends_with(":v2026.05.2") {
        bail!("P6 rollout report still references the P5 image tag v2026.05.2");
    }
    if let Some(expected_image) = expected_image {
        assert_json_str_eq(report, &["image", "name"], expected_image)?;
    }
    assert_json_bool_eq(report, &["image", "deployment_images_match"], true)?;
    assert_json_bool_eq(report, &["argocd", "checked"], true)?;
    assert_json_array_nonempty(report, &["cluster", "deployment_images"])?;
    for (index, deployment) in json_array(report, &["cluster", "deployment_images"])?
        .iter()
        .enumerate()
    {
        let deployment_image = json_str(deployment, &["image"])
            .with_context(|| format!("P6 rollout deployment_images[{index}]"))?;
        if deployment_image != image {
            bail!(
                "P6 rollout deployment_images[{index}] does not match image.name: expected {image}, got {deployment_image}"
            );
        }
    }
    assert_json_array_len(report, &["preflight_errors"], 0)?;
    assert_json_bool_eq(report, &["checks", "image_published"], true)?;
    assert_json_bool_eq(report, &["checks", "gitops_rollout_approved"], true)?;
    assert_json_bool_eq(report, &["checks", "argocd_synced_healthy"], true)?;
    assert_json_bool_eq(report, &["checks", "deployment_images_match"], true)?;
    assert_json_bool_eq(report, &["checks", "post_smoke_default_off_cleanup"], true)?;
    assert_json_str_eq(report, &["argocd", "sync"], "Synced")?;
    assert_json_str_eq(report, &["argocd", "health"], "Healthy")?;

    let rollout_revision = json_str(report, &["gitops", "rollout_revision"])?;
    if rollout_revision.trim().is_empty() {
        bail!("P6 rollout report has empty gitops.rollout_revision");
    }
    let cleanup_revision = json_str(report, &["gitops", "cleanup_revision"])?;
    if cleanup_revision.trim().is_empty() {
        bail!("P6 rollout report has empty gitops.cleanup_revision");
    }
    assert_json_bool_eq(report, &["cleanup", "manual_activation_default_off"], true)?;
    assert_json_bool_eq(report, &["cleanup", "preview_fixture_removed"], true)?;
    assert_remaining_uncovered_empty(report)?;
    Ok(())
}

fn validate_internal_k8s_multi_depth_plan_topology(report: &serde_json::Value) -> Result<()> {
    let parent = json_cell_id(report, &["plan", "parent"])?;
    if parent.depth == 0 || parent.sub != 0 || !parent.is_canonical_leaf() {
        bail!(
            "internal multi-depth activation plan parent must be a non-root canonical leaf, got {:?}",
            parent
        );
    }
    let expected_children = parent
        .canonical_children()
        .ok_or_else(|| anyhow::anyhow!("internal multi-depth parent cannot produce children"))?;
    let targets = json_array(report, &["plan", "targets"])?;
    if targets.len() != 4 {
        bail!(
            "internal multi-depth activation plan expected four targets, got {}",
            targets.len()
        );
    }
    let mut actual_children = Vec::with_capacity(targets.len());
    let mut has_non_source = false;
    let source = json_str(report, &["plan", "source_worker_id"]).unwrap_or("");
    for (index, target) in targets.iter().enumerate() {
        let cell = json_cell_id(target, &["cell"])
            .with_context(|| format!("internal multi-depth target index {index}"))?;
        let worker_id = json_str(target, &["worker_id"])?;
        if worker_id.trim().is_empty() {
            bail!("internal multi-depth target index {index} has empty worker_id");
        }
        if !source.is_empty() && worker_id != source {
            has_non_source = true;
        }
        actual_children.push(cell);
    }
    for expected in expected_children {
        let count = actual_children
            .iter()
            .filter(|actual| **actual == expected)
            .count();
        if count != 1 {
            bail!(
                "internal multi-depth activation plan expected canonical child {:?} exactly once, got count={count}",
                expected
            );
        }
    }
    if !source.is_empty() && !has_non_source {
        bail!("internal multi-depth activation target map has no non-source worker");
    }
    Ok(())
}

fn validate_internal_k8s_live_metrics_plan(report: &serde_json::Value) -> Result<()> {
    assert_json_str_eq(report, &["plan", "status"], "ready")?;
    assert_json_bool_eq(report, &["plan", "activation_mutated"], false)?;
    let source = json_str(report, &["plan", "preview", "source"])?;
    if !source.starts_with("live_worker_metrics:") {
        bail!(
            "internal activation report plan preview source is not live_worker_metrics: {source}"
        );
    }
    assert_json_number_at_least(report, &["plan", "preview", "plan_count"], 1.0)?;
    if json_path(report, &["plan", "recommendation", "targets"]).is_some() {
        assert_json_array_len(report, &["plan", "recommendation", "targets"], 4)?;
    } else {
        assert_json_array_len(report, &["plan", "targets"], 4)?;
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
    assert_remaining_uncovered_absent(report, "guarded_kubernetes_activation_publish")?;

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
        assert_remaining_uncovered_absent(report, "guarded_kubernetes_failure_recovery_smoke")?;
        assert_remaining_uncovered_absent(report, "merge_rollback_policy_gate")?;
        assert_remaining_uncovered_empty(report)?;
        validate_p5_rollback_policy(report)?;
    }
    Ok(())
}

fn validate_internal_k8s_restart_report(report: &serde_json::Value) -> Result<()> {
    assert_json_bool_eq(report, &["checks", "orchestrator_restart_smoke_ran"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "assignment_state_storage_configured"],
        true,
    )?;
    assert_json_bool_eq(report, &["checks", "orchestrator_rollout_restarted"], true)?;
    assert_json_bool_eq(
        report,
        &["checks", "restarted_orchestrator_loaded_child_routes"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "worker_assignment_refresh_after_restart"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "gateway_ready_routes_after_restart"],
        4.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "child_ping_all_routes_after_restart"],
        true,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "child_move_iterations_after_restart"],
        1.0,
    )?;
    assert_json_number_at_least(
        report,
        &["checks", "remote_aoi_frames_observed_after_restart"],
        1.0,
    )?;
    assert_json_bool_eq(
        report,
        &["checks", "live_remote_aoi_resync_snapshot_after_restart"],
        true,
    )?;
    assert_json_bool_eq(
        report,
        &["cluster", "assignment_state_storage", "checked"],
        true,
    )?;
    assert_json_str_eq(
        report,
        &["cluster", "assignment_state_storage", "policy_id"],
        ORCH_ASSIGNMENT_STATE_STORAGE_POLICY,
    )?;
    assert_json_str_eq(
        report,
        &["cluster", "assignment_state_storage", "env"],
        ORCH_ASSIGNMENT_STATE_ENV,
    )?;
    let claim = json_str(
        report,
        &[
            "cluster",
            "assignment_state_storage",
            "persistent_volume_claim",
        ],
    )?;
    if claim.trim().is_empty() {
        bail!("internal restart report has empty persistent_volume_claim");
    }
    assert_probe_succeeded_all(report, &["restart_probe"])?;
    assert_remaining_uncovered_absent(report, "guarded_kubernetes_restart_recovery_smoke")?;
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
        P5_ROLLBACK_MERGE_ACTIVATION,
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

fn assert_remaining_uncovered_contains(report: &serde_json::Value, blocker: &str) -> Result<()> {
    let values = json_array(report, &["remaining_uncovered"])?;
    if values.iter().any(|value| value.as_str() == Some(blocker)) {
        return Ok(());
    }
    bail!("report remaining_uncovered does not list `{blocker}`: {values:?}")
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

fn json_u64(value: &serde_json::Value, path: &[&str]) -> Result<u64> {
    json_field(value, path)?.as_u64().ok_or_else(|| {
        anyhow::anyhow!("report field {} is not an unsigned integer", path.join("."))
    })
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

fn find_p7_operation_record<'a>(
    ledger: &'a serde_json::Value,
    operation_id: &str,
) -> Result<&'a serde_json::Value> {
    json_array(ledger, &["records"])?
        .iter()
        .find(|record| {
            json_str(record, &["operation_id"]).is_ok_and(|candidate| candidate == operation_id)
        })
        .ok_or_else(|| anyhow::anyhow!("operation ledger missing operation_id={operation_id}"))
}

fn json_cell_id(value: &serde_json::Value, path: &[&str]) -> Result<CellId> {
    serde_json::from_value(json_field(value, path)?.clone())
        .with_context(|| format!("report field {} is not a CellId", path.join(".")))
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

fn k8s_assignment_state_storage_json(
    storage: Option<&K8sAssignmentStateStorage>,
) -> serde_json::Value {
    let Some(storage) = storage else {
        return serde_json::json!({
            "checked": false,
            "policy_id": ORCH_ASSIGNMENT_STATE_STORAGE_POLICY,
            "env": ORCH_ASSIGNMENT_STATE_ENV
        });
    };
    serde_json::json!({
        "checked": true,
        "policy_id": storage.policy_id,
        "env": storage.env_name.as_str(),
        "path": storage.path.as_str(),
        "mount_path": storage.mount_path.as_str(),
        "volume": storage.volume_name.as_str(),
        "persistent_volume_claim": storage.claim_name.as_str()
    })
}

fn run_split_activation_operator(
    orch_addr: &str,
    operation_id: String,
    parent: CellId,
    raw_targets: &[String],
    raw_cell_targets: &[String],
) -> Result<()> {
    validate_split_activation_target_mode(raw_targets, raw_cell_targets)?;
    let endpoint = grpc_endpoint(orch_addr);
    let runtime = tokio::runtime::Runtime::new()?;
    let response = if raw_cell_targets.is_empty() {
        let targets = parse_split_activation_targets(raw_targets)?;
        runtime.block_on(submit_split_activation(
            &endpoint,
            operation_id,
            parent,
            &targets,
        ))?
    } else {
        let targets = parse_split_activation_cell_targets(raw_cell_targets)?;
        runtime.block_on(submit_split_activation_with_child_cells(
            &endpoint,
            operation_id,
            parent,
            &targets,
        ))?
    };
    print_split_activation_response(&response)?;
    assert_split_activation_published(&response)
}

fn run_merge_activation_operator(
    orch_addr: &str,
    operation_id: String,
    parent: CellId,
    owner_worker_id: String,
) -> Result<()> {
    let endpoint = grpc_endpoint(orch_addr);
    let runtime = tokio::runtime::Runtime::new()?;
    let response = runtime.block_on(submit_merge_activation(
        &endpoint,
        operation_id,
        parent,
        owner_worker_id,
    ))?;
    print_merge_activation_response(&response)?;
    assert_merge_activation_published(&response)
}

fn run_split_activation_plan_operator(
    orch_addr: &str,
    preview_addr: &str,
    live_worker_metrics: &[String],
    live_policy: LiveMetricsPlanPolicy,
    operation_id: Option<String>,
    raw_targets: &[String],
    out: Option<&Path>,
) -> Result<()> {
    let endpoint = grpc_endpoint(orch_addr);
    let plan = if live_worker_metrics.is_empty() {
        build_split_activation_plan(&endpoint, preview_addr, operation_id, raw_targets)?
    } else {
        build_split_activation_plan_from_live_metrics(
            &endpoint,
            live_worker_metrics,
            live_policy,
            operation_id,
            raw_targets,
        )?
    };
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

fn run_merge_activation_plan_operator(
    orch_addr: &str,
    preview_addr: &str,
    operation_id: Option<String>,
    out: Option<&Path>,
) -> Result<()> {
    let endpoint = grpc_endpoint(orch_addr);
    let plan = build_merge_activation_plan(&endpoint, preview_addr, operation_id)?;
    let report_path = write_merge_activation_plan_report(&plan, out)?;
    println!(
        "merge activation plan: status={} reason={} report={}",
        plan.status,
        plan.reason,
        report_path.display()
    );
    if let Some(command) = plan.submission_command.as_ref() {
        println!("submission command: {command}");
    }
    Ok(())
}

struct PlannerActivationPlanSource<'a> {
    preview_addr: &'a str,
    live_worker_metrics: &'a [String],
    live_policy: LiveMetricsPlanPolicy,
}

struct PlannerActivationMutation<'a> {
    allow_mutation: bool,
    policy_id: Option<&'a str>,
    out: Option<&'a Path>,
}

struct PlannerActivationOptions<'a> {
    kind: &'a str,
    orch_addr: &'a str,
    plan_source: PlannerActivationPlanSource<'a>,
    operation_id: Option<String>,
    mutation: PlannerActivationMutation<'a>,
}

fn run_planner_activation_operator(options: PlannerActivationOptions<'_>) -> Result<()> {
    if !options.plan_source.live_worker_metrics.is_empty()
        && options.plan_source.live_policy.min_pressure_signals == 0
    {
        bail!("planner activation live metrics mode requires --live-min-pressure-signals > 0");
    }
    let should_validate_mutated = options.mutation.allow_mutation
        && options.mutation.policy_id == Some(PLANNER_MUTATION_POLICY_ID);
    let report_path = execute_planner_activation(options)?;
    let report = read_json_report(&report_path)?;
    println!(
        "planner activation: kind={} status={} activation_mutated={} policy_accepted={} report={}",
        json_str(&report, &["planner_kind"])?,
        json_str(&report, &["status"])?,
        json_bool(&report, &["activation_mutated"])?,
        json_bool(&report, &["policy", "accepted"])?,
        report_path.display()
    );
    if should_validate_mutated {
        validate_planner_activation_report(&report, true)?;
    }
    Ok(())
}

fn execute_planner_activation(options: PlannerActivationOptions<'_>) -> Result<PathBuf> {
    let endpoint = grpc_endpoint(options.orch_addr);
    match options.kind {
        "split" => execute_split_planner_activation(
            &endpoint,
            options.plan_source,
            options.operation_id,
            options.mutation,
        ),
        "merge" => {
            if !options.plan_source.live_worker_metrics.is_empty() {
                bail!("--live-worker-metrics is only supported with --kind split");
            }
            execute_merge_planner_activation(
                &endpoint,
                options.plan_source.preview_addr,
                options.operation_id,
                options.mutation.allow_mutation,
                options.mutation.policy_id,
                options.mutation.out,
            )
        }
        other => bail!("unknown planner activation kind `{other}`; expected split or merge"),
    }
}

fn execute_split_planner_activation(
    endpoint: &str,
    plan_source: PlannerActivationPlanSource<'_>,
    operation_id: Option<String>,
    mutation: PlannerActivationMutation<'_>,
) -> Result<PathBuf> {
    let plan = if plan_source.live_worker_metrics.is_empty() {
        build_split_activation_plan(endpoint, plan_source.preview_addr, operation_id, &[])?
    } else {
        build_split_activation_plan_from_live_metrics(
            endpoint,
            plan_source.live_worker_metrics,
            plan_source.live_policy,
            operation_id,
            &[],
        )?
    };
    if plan.status != "ready" {
        return write_split_planner_activation_report(
            &plan,
            "plan_not_ready",
            "planner selected no submit-ready split activation",
            mutation.allow_mutation,
            mutation.policy_id,
            None,
            mutation.out,
        );
    }
    if !planner_mutation_policy_accepted(mutation.allow_mutation, mutation.policy_id) {
        return write_split_planner_activation_report(
            &plan,
            "blocked_by_policy",
            "planner mutation is default-off and requires explicit policy approval",
            mutation.allow_mutation,
            mutation.policy_id,
            None,
            mutation.out,
        );
    }

    let parent = plan
        .parent
        .ok_or_else(|| anyhow::anyhow!("ready split planner activation is missing parent"))?;
    let targets = plan
        .recommended_targets
        .iter()
        .map(|target| (target.sub, target.worker_id.clone()))
        .collect::<Vec<_>>();
    let runtime = tokio::runtime::Runtime::new()?;
    let response = runtime.block_on(submit_split_activation(
        endpoint,
        plan.operation_id.clone(),
        parent,
        &targets,
    ))?;
    let published = response.accepted
        && response.state == SplitActivationState::Published as i32
        && response.assignments_changed
        && response.staged_children.len() == 4;
    let report_path = write_split_planner_activation_report(
        &plan,
        if published {
            "published"
        } else {
            "submit_rejected"
        },
        if published {
            "policy-approved planner split activation was published"
        } else {
            "policy-approved planner split activation submit did not publish"
        },
        mutation.allow_mutation,
        mutation.policy_id,
        Some(&response),
        mutation.out,
    )?;
    if published {
        Ok(report_path)
    } else {
        bail!(
            "planner split activation was not published: accepted={} state={} assignments_changed={} reason={}",
            response.accepted,
            split_activation_state_name(response.state),
            response.assignments_changed,
            response.reason
        );
    }
}

fn execute_merge_planner_activation(
    endpoint: &str,
    preview_addr: &str,
    operation_id: Option<String>,
    allow_mutation: bool,
    policy_id: Option<&str>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let plan = build_merge_activation_plan(endpoint, preview_addr, operation_id)?;
    if plan.status != "ready" {
        return write_merge_planner_activation_report(
            &plan,
            "plan_not_ready",
            "planner selected no submit-ready merge activation",
            allow_mutation,
            policy_id,
            None,
            out,
        );
    }
    if !planner_mutation_policy_accepted(allow_mutation, policy_id) {
        return write_merge_planner_activation_report(
            &plan,
            "blocked_by_policy",
            "planner mutation is default-off and requires explicit policy approval",
            allow_mutation,
            policy_id,
            None,
            out,
        );
    }

    let parent = plan
        .parent
        .ok_or_else(|| anyhow::anyhow!("ready merge planner activation is missing parent"))?;
    let owner_worker_id = plan
        .owner_worker_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("ready merge planner activation is missing owner"))?;
    let runtime = tokio::runtime::Runtime::new()?;
    let response = runtime.block_on(submit_merge_activation(
        endpoint,
        plan.operation_id.clone(),
        parent,
        owner_worker_id,
    ))?;
    let published = response.accepted
        && response.state == MergeActivationState::Published as i32
        && response.assignments_changed
        && response.merged_children.len() == 4;
    let report_path = write_merge_planner_activation_report(
        &plan,
        if published {
            "published"
        } else {
            "submit_rejected"
        },
        if published {
            "policy-approved planner merge activation was published"
        } else {
            "policy-approved planner merge activation submit did not publish"
        },
        allow_mutation,
        policy_id,
        Some(&response),
        out,
    )?;
    if published {
        Ok(report_path)
    } else {
        bail!(
            "planner merge activation was not published: accepted={} state={} assignments_changed={} reason={}",
            response.accepted,
            merge_activation_state_name(response.state),
            response.assignments_changed,
            response.reason
        );
    }
}

fn planner_mutation_policy_accepted(allow_mutation: bool, policy_id: Option<&str>) -> bool {
    allow_mutation && policy_id == Some(PLANNER_MUTATION_POLICY_ID)
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

#[derive(Debug, Clone, Copy)]
struct LiveMetricsPlanPolicy {
    actor_threshold: u64,
    move_threshold: u64,
    min_pressure_signals: u8,
    cell_age_secs: u64,
}

impl Default for LiveMetricsPlanPolicy {
    fn default() -> Self {
        Self {
            actor_threshold: 100,
            move_threshold: 64,
            min_pressure_signals: 2,
            cell_age_secs: 60,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveWorkerMetricsEndpoint {
    worker_id: String,
    addr: String,
}

#[derive(Debug, Clone)]
struct LiveWorkerMetricsSnapshot {
    worker_id: String,
    addr: String,
    actor_counts: std::collections::HashMap<CellId, u64>,
    pending_moves: std::collections::HashMap<CellId, u64>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct InternalMultiDepthActivationTarget {
    cell: CellId,
    worker_id: String,
}

#[derive(Debug)]
struct InternalMultiDepthActivationPlan {
    operation_id: String,
    status: &'static str,
    reason: String,
    orch_addr: String,
    health: OrchestratorHealth,
    workers: Vec<SplitActivationPlanWorker>,
    parent: CellId,
    source_worker_id: Option<String>,
    targets: Vec<InternalMultiDepthActivationTarget>,
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

#[derive(Debug)]
struct MergeActivationOperatorPlan {
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
    owner_worker_id: Option<String>,
    siblings: Vec<CellId>,
    submission_command: Option<String>,
}

fn build_split_activation_plan(
    orch_endpoint: &str,
    preview_addr: &str,
    operation_id: Option<String>,
    raw_targets: &[String],
) -> Result<SplitActivationOperatorPlan> {
    let preview = fetch_split_merge_preview(preview_addr)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let (health, listing) = runtime.block_on(fetch_orch_health_and_listing(orch_endpoint))?;
    build_split_activation_plan_from_parts(
        orch_endpoint,
        preview_addr,
        preview,
        health,
        listing,
        operation_id,
        raw_targets,
    )
}

fn build_merge_activation_plan(
    orch_endpoint: &str,
    preview_addr: &str,
    operation_id: Option<String>,
) -> Result<MergeActivationOperatorPlan> {
    let preview = fetch_split_merge_preview(preview_addr)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let (health, listing) = runtime.block_on(fetch_orch_health_and_listing(orch_endpoint))?;
    build_merge_activation_plan_from_parts(
        orch_endpoint,
        preview_addr,
        preview,
        health,
        listing,
        operation_id,
    )
}

fn build_split_activation_plan_from_live_metrics(
    orch_endpoint: &str,
    live_worker_metrics: &[String],
    policy: LiveMetricsPlanPolicy,
    operation_id: Option<String>,
    raw_targets: &[String],
) -> Result<SplitActivationOperatorPlan> {
    let endpoints = parse_live_worker_metrics_endpoints(live_worker_metrics)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let (health, listing) = runtime.block_on(fetch_orch_health_and_listing(orch_endpoint))?;
    let snapshots = endpoints
        .iter()
        .map(fetch_live_worker_metrics_snapshot)
        .collect::<Result<Vec<_>>>()?;
    let preview = build_live_metrics_split_preview(&listing, &snapshots, policy)?;
    let preview_source = preview.source.clone();
    build_split_activation_plan_from_parts(
        orch_endpoint,
        &preview_source,
        preview,
        health,
        listing,
        operation_id,
        raw_targets,
    )
}

fn build_split_activation_plan_from_parts(
    orch_endpoint: &str,
    preview_addr: &str,
    preview: SplitMergePreviewJson,
    health: OrchestratorHealth,
    listing: AssignmentListing,
    operation_id: Option<String>,
    raw_targets: &[String],
) -> Result<SplitActivationOperatorPlan> {
    if preview.assignments_changed {
        bail!("split activation plan requires dry-run preview with assignments_changed=false");
    }

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

fn parse_live_worker_metrics_endpoints(
    raw_endpoints: &[String],
) -> Result<Vec<LiveWorkerMetricsEndpoint>> {
    let mut endpoints = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for raw in raw_endpoints {
        let Some((worker_raw, addr_raw)) = raw.split_once('=') else {
            bail!("invalid --live-worker-metrics `{raw}`; expected worker-id=addr");
        };
        let worker_id = worker_raw.trim();
        let addr = addr_raw.trim();
        if worker_id.is_empty() || addr.is_empty() {
            bail!("invalid --live-worker-metrics `{raw}`; worker-id and addr must be non-empty");
        }
        if !seen.insert(worker_id.to_string()) {
            bail!("duplicate --live-worker-metrics worker id `{worker_id}`");
        }
        endpoints.push(LiveWorkerMetricsEndpoint {
            worker_id: worker_id.to_string(),
            addr: addr.to_string(),
        });
    }
    Ok(endpoints)
}

fn fetch_live_worker_metrics_snapshot(
    endpoint: &LiveWorkerMetricsEndpoint,
) -> Result<LiveWorkerMetricsSnapshot> {
    let addr = readiness_addr(&endpoint.addr)?;
    let response = http_get(addr, "/metrics")?;
    let body = http_response_body("worker live metrics", &response)?;
    Ok(LiveWorkerMetricsSnapshot {
        worker_id: endpoint.worker_id.clone(),
        addr: endpoint.addr.clone(),
        actor_counts: parse_worker_cell_metric(body, "tessera_worker_cell_actor_count")?,
        pending_moves: parse_worker_cell_metric(body, "tessera_worker_cell_pending_move_count")?,
    })
}

fn build_merge_activation_plan_from_parts(
    orch_endpoint: &str,
    preview_addr: &str,
    preview: SplitMergePreviewJson,
    health: OrchestratorHealth,
    listing: AssignmentListing,
    operation_id: Option<String>,
) -> Result<MergeActivationOperatorPlan> {
    if preview.assignments_changed {
        bail!("merge activation plan requires dry-run preview with assignments_changed=false");
    }

    let workers = split_activation_plan_workers(&health, &listing);
    let operation_id =
        operation_id.unwrap_or_else(|| format!("planned-merge-{}", unix_timestamp_secs()));
    let selected_plan = select_merge_preview_candidate(&preview);
    let Some(selected_plan) = selected_plan else {
        return Ok(MergeActivationOperatorPlan {
            operation_id,
            status: "no_merge_candidate",
            reason: "preview returned no merge candidate".to_string(),
            preview_addr: preview_addr.to_string(),
            preview_mode: preview.mode,
            preview_source: preview.source,
            preview_plan_count: preview.plans.len(),
            selected_plan: None,
            orch_addr: orch_endpoint.to_string(),
            health,
            workers,
            parent: None,
            owner_worker_id: None,
            siblings: Vec::new(),
            submission_command: None,
        });
    };

    let parent = selected_plan.cell;
    let parent_owners = owners_for_listing_cell(&listing, parent)?;
    if !parent_owners.is_empty() {
        return blocked_merge_activation_plan(
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
            "selected merge parent is already assigned",
        );
    }

    let siblings = match merge_child_cells_from_listing(parent, &listing) {
        Ok(siblings) => siblings,
        Err(err) => {
            let reason = err.to_string();
            return blocked_merge_activation_plan(
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
                reason.as_str(),
            );
        }
    };
    let mut owner_worker_id = None::<String>;
    for sibling in &siblings {
        let owners = owners_for_listing_cell(&listing, *sibling)?;
        match owners.as_slice() {
            [owner] => {
                if let Some(existing) = owner_worker_id.as_deref() {
                    if existing != owner {
                        return blocked_merge_activation_plan(
                            operation_id,
                            preview_addr,
                            preview,
                            selected_plan,
                            orch_endpoint,
                            health,
                            workers,
                            Some(parent),
                            owner_worker_id,
                            siblings,
                            "merge siblings must share one owner in the first runtime slice",
                        );
                    }
                } else {
                    owner_worker_id = Some(owner.clone());
                }
            }
            [] => {
                return blocked_merge_activation_plan(
                    operation_id,
                    preview_addr,
                    preview,
                    selected_plan,
                    orch_endpoint,
                    health,
                    workers,
                    Some(parent),
                    owner_worker_id,
                    siblings,
                    "merge sibling is not currently assigned",
                );
            }
            _ => {
                return blocked_merge_activation_plan(
                    operation_id,
                    preview_addr,
                    preview,
                    selected_plan,
                    orch_endpoint,
                    health,
                    workers,
                    Some(parent),
                    owner_worker_id,
                    siblings,
                    "merge sibling has multiple owners",
                );
            }
        }
    }

    let Some(owner_worker_id) = owner_worker_id else {
        return blocked_merge_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            None,
            siblings,
            "merge candidate has no owned siblings",
        );
    };
    let Some(owner) = workers
        .iter()
        .find(|worker| worker.worker_id == owner_worker_id)
    else {
        return blocked_merge_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            Some(owner_worker_id),
            siblings,
            "merge owner is not in listing",
        );
    };
    if !owner.registered {
        return blocked_merge_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            Some(owner_worker_id),
            siblings,
            "merge owner worker is not registered",
        );
    }
    if owner.active_handover
        || listing.handovers.iter().any(|handover| {
            handover.cell.as_ref().is_some_and(|cell| {
                proto_assignment_to_cell(cell)
                    .is_ok_and(|cell| cell == parent || siblings.contains(&cell))
            })
        })
    {
        return blocked_merge_activation_plan(
            operation_id,
            preview_addr,
            preview,
            selected_plan,
            orch_endpoint,
            health,
            workers,
            Some(parent),
            Some(owner_worker_id),
            siblings,
            "active handover touches merge owner or cell family",
        );
    }

    let submission_command =
        merge_activation_submission_command(orch_endpoint, &operation_id, parent, &owner_worker_id);
    Ok(MergeActivationOperatorPlan {
        operation_id,
        status: "ready",
        reason: "preview merge candidate has a single registered sibling owner and can be submitted through the manual merge activation gate".to_string(),
        preview_addr: preview_addr.to_string(),
        preview_mode: preview.mode,
        preview_source: preview.source,
        preview_plan_count: preview.plans.len(),
        selected_plan: Some(selected_plan),
        orch_addr: orch_endpoint.to_string(),
        health,
        workers,
        parent: Some(parent),
        owner_worker_id: Some(owner_worker_id),
        siblings,
        submission_command: Some(submission_command),
    })
}

#[allow(clippy::too_many_arguments)]
fn blocked_merge_activation_plan(
    operation_id: String,
    preview_addr: &str,
    preview: SplitMergePreviewJson,
    selected_plan: SplitMergePreviewPlanJson,
    orch_endpoint: &str,
    health: OrchestratorHealth,
    workers: Vec<SplitActivationPlanWorker>,
    parent: Option<CellId>,
    owner_worker_id: Option<String>,
    siblings: Vec<CellId>,
    reason: &str,
) -> Result<MergeActivationOperatorPlan> {
    Ok(MergeActivationOperatorPlan {
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
        owner_worker_id,
        siblings,
        submission_command: None,
    })
}

fn build_internal_multi_depth_activation_plan(
    orch_endpoint: &str,
    health: OrchestratorHealth,
    listing: AssignmentListing,
    operation_id: String,
    options: &K8sMultiDepthActivationSmokeOptions,
) -> Result<InternalMultiDepthActivationPlan> {
    let workers = split_activation_plan_workers(&health, &listing);
    let parent = options.parent;
    let targets = if options.target_cells.is_empty() {
        default_internal_multi_depth_targets(
            parent,
            &options.source_worker_id,
            &options.target_worker_id,
        )?
    } else {
        parse_split_activation_cell_targets(&options.target_cells)?
            .into_iter()
            .map(|(cell, worker_id)| InternalMultiDepthActivationTarget { cell, worker_id })
            .collect()
    };
    if parent.depth == 0 || parent.sub != 0 || !parent.is_canonical_leaf() {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            None,
            "multi-depth parent must be a non-root canonical leaf with sub=0",
        ));
    }
    let expected_children = parent
        .canonical_children()
        .ok_or_else(|| anyhow::anyhow!("multi-depth parent cannot produce canonical children"))?;
    if targets.len() != 4 {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            None,
            "multi-depth target map must contain exactly four child cells",
        ));
    }
    for expected in expected_children {
        let count = targets
            .iter()
            .filter(|target| target.cell == expected)
            .count();
        if count != 1 {
            return Ok(blocked_internal_multi_depth_activation_plan(
                orch_endpoint,
                health,
                workers,
                operation_id,
                parent,
                targets,
                None,
                "multi-depth target map must cover the canonical child family exactly once",
            ));
        }
    }

    let parent_owners = owners_for_listing_cell(&listing, parent)?;
    let Some(source_worker_id) = parent_owners.first().cloned() else {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            None,
            "canonical multi-depth parent is not currently assigned",
        ));
    };
    if parent_owners.len() != 1 {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            Some(source_worker_id),
            "canonical multi-depth parent has multiple owners",
        ));
    }
    if source_worker_id != options.source_worker_id {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            Some(source_worker_id),
            "canonical multi-depth parent owner does not match expected source worker",
        ));
    }
    let Some(source) = workers
        .iter()
        .find(|worker| worker.worker_id == source_worker_id)
    else {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            Some(source_worker_id),
            "source worker is not in assignment listing",
        ));
    };
    if !source.registered {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            Some(source_worker_id),
            "source worker is not registered",
        ));
    }
    for target in &targets {
        let Some(worker) = workers
            .iter()
            .find(|worker| worker.worker_id == target.worker_id)
        else {
            return Ok(blocked_internal_multi_depth_activation_plan(
                orch_endpoint,
                health,
                workers,
                operation_id,
                parent,
                targets,
                Some(source_worker_id),
                "target worker is not in assignment listing",
            ));
        };
        if !worker.registered {
            return Ok(blocked_internal_multi_depth_activation_plan(
                orch_endpoint,
                health,
                workers,
                operation_id,
                parent,
                targets,
                Some(source_worker_id),
                "target worker is not registered",
            ));
        }
        if !owners_for_listing_cell(&listing, target.cell)?.is_empty() {
            return Ok(blocked_internal_multi_depth_activation_plan(
                orch_endpoint,
                health,
                workers,
                operation_id,
                parent,
                targets,
                Some(source_worker_id),
                "canonical child target is already assigned",
            ));
        }
    }
    if !targets
        .iter()
        .any(|target| target.worker_id != source_worker_id)
    {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            Some(source_worker_id),
            "target map assigns all canonical children to the source worker",
        ));
    }
    if workers
        .iter()
        .any(|worker| worker.active_handover && involved_multi_depth_worker(&targets, worker))
        || listing.handovers.iter().any(|handover| {
            handover.cell.as_ref().is_some_and(|cell| {
                proto_assignment_to_cell(cell).is_ok_and(|cell| {
                    cell == parent || targets.iter().any(|target| target.cell == cell)
                })
            })
        })
    {
        return Ok(blocked_internal_multi_depth_activation_plan(
            orch_endpoint,
            health,
            workers,
            operation_id,
            parent,
            targets,
            Some(source_worker_id),
            "active handover touches multi-depth source, target, or cell family",
        ));
    }

    let submission_command =
        multi_depth_activation_submission_command(orch_endpoint, &operation_id, parent, &targets);
    Ok(InternalMultiDepthActivationPlan {
        operation_id,
        status: "ready",
        reason: "canonical multi-depth parent and explicit child target map are ready for the manual split activation gate".to_string(),
        orch_addr: orch_endpoint.to_string(),
        health,
        workers,
        parent,
        source_worker_id: Some(source_worker_id),
        targets,
        submission_command: Some(submission_command),
    })
}

fn default_internal_multi_depth_targets(
    parent: CellId,
    source_worker_id: &str,
    target_worker_id: &str,
) -> Result<Vec<InternalMultiDepthActivationTarget>> {
    let children = parent
        .canonical_children()
        .ok_or_else(|| anyhow::anyhow!("multi-depth parent cannot produce canonical children"))?;
    Ok(children
        .into_iter()
        .enumerate()
        .map(|(idx, cell)| InternalMultiDepthActivationTarget {
            cell,
            worker_id: if idx % 2 == 0 {
                source_worker_id.to_string()
            } else {
                target_worker_id.to_string()
            },
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn blocked_internal_multi_depth_activation_plan(
    orch_endpoint: &str,
    health: OrchestratorHealth,
    workers: Vec<SplitActivationPlanWorker>,
    operation_id: String,
    parent: CellId,
    targets: Vec<InternalMultiDepthActivationTarget>,
    source_worker_id: Option<String>,
    reason: &str,
) -> InternalMultiDepthActivationPlan {
    InternalMultiDepthActivationPlan {
        operation_id,
        status: "blocked",
        reason: reason.to_string(),
        orch_addr: orch_endpoint.to_string(),
        health,
        workers,
        parent,
        source_worker_id,
        targets,
        submission_command: None,
    }
}

fn involved_multi_depth_worker(
    targets: &[InternalMultiDepthActivationTarget],
    worker: &SplitActivationPlanWorker,
) -> bool {
    targets
        .iter()
        .any(|target| target.worker_id == worker.worker_id)
}

fn build_live_metrics_split_preview(
    listing: &AssignmentListing,
    snapshots: &[LiveWorkerMetricsSnapshot],
    policy: LiveMetricsPlanPolicy,
) -> Result<SplitMergePreviewJson> {
    if policy.min_pressure_signals == 0 {
        bail!("live metrics min pressure signals must be greater than zero");
    }
    let metrics_by_worker = snapshots
        .iter()
        .map(|snapshot| (snapshot.worker_id.as_str(), snapshot))
        .collect::<std::collections::HashMap<_, _>>();
    let mut plans = Vec::new();
    for bundle in &listing.workers {
        let Some(metrics) = metrics_by_worker.get(bundle.worker_id.as_str()) else {
            continue;
        };
        for assignment in &bundle.cells {
            let cell = proto_assignment_to_cell(assignment)?;
            let actor_count = *metrics.actor_counts.get(&cell).unwrap_or(&0);
            let pending_moves = *metrics.pending_moves.get(&cell).unwrap_or(&0);
            let pressure_signals = u8::from(actor_count >= policy.actor_threshold)
                + u8::from(pending_moves >= policy.move_threshold);
            if cell.depth == 0 && cell.sub == 0 && pressure_signals >= policy.min_pressure_signals {
                let score = u64::from(pressure_signals) * 1_000_000
                    + actor_count * 10_000
                    + pending_moves * 1_000;
                plans.push(SplitMergePreviewPlanJson {
                    kind: "split".to_string(),
                    cell,
                    pressure_signals,
                    score,
                    required_handover_ops: 1,
                    cells_moved: 1,
                });
            }
        }
    }
    plans.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.cell.world.cmp(&right.cell.world))
            .then_with(|| left.cell.cy.cmp(&right.cell.cy))
            .then_with(|| left.cell.cx.cmp(&right.cell.cx))
            .then_with(|| left.cell.depth.cmp(&right.cell.depth))
            .then_with(|| left.cell.sub.cmp(&right.cell.sub))
    });
    let sources = snapshots
        .iter()
        .map(|snapshot| format!("{}={}", snapshot.worker_id, snapshot.addr))
        .collect::<Vec<_>>()
        .join(",");
    Ok(SplitMergePreviewJson {
        mode: "dry_run".to_string(),
        source: format!(
            "live_worker_metrics:{sources};actor_threshold={};move_threshold={};min_pressure_signals={};cell_age_secs={}",
            policy.actor_threshold,
            policy.move_threshold,
            policy.min_pressure_signals,
            policy.cell_age_secs
        ),
        assignments_changed: false,
        plans,
    })
}

fn parse_worker_cell_metric(
    body: &str,
    metric_name: &str,
) -> Result<std::collections::HashMap<CellId, u64>> {
    let mut values = std::collections::HashMap::new();
    let labeled_prefix = format!("{metric_name}{{");
    for line in body.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') || !line.starts_with(&labeled_prefix) {
            continue;
        }
        let Some((sample, raw_value)) = line.split_once(' ') else {
            bail!("metric sample `{line}` is missing value");
        };
        let labels_raw = sample
            .strip_prefix(&labeled_prefix)
            .and_then(|rest| rest.strip_suffix('}'))
            .ok_or_else(|| anyhow::anyhow!("metric sample `{line}` has invalid labels"))?;
        let labels = parse_prometheus_labels(labels_raw)?;
        let cell = CellId {
            world: parse_label_u32(&labels, "world")?,
            cx: parse_label_i32(&labels, "cx")?,
            cy: parse_label_i32(&labels, "cy")?,
            depth: parse_label_u8(&labels, "depth")?,
            sub: parse_label_u8(&labels, "sub")?,
        };
        let value = raw_value
            .trim()
            .parse::<f64>()
            .with_context(|| format!("parse metric value for `{line}`"))?;
        if !value.is_finite() || value < 0.0 {
            bail!("metric sample `{line}` has invalid non-negative value");
        }
        values.insert(cell, value as u64);
    }
    Ok(values)
}

fn parse_prometheus_labels(raw: &str) -> Result<std::collections::HashMap<String, String>> {
    let mut labels = std::collections::HashMap::new();
    if raw.trim().is_empty() {
        return Ok(labels);
    }
    for part in raw.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            bail!("invalid prometheus label `{part}`");
        };
        let key = key.trim();
        let value = value.trim();
        let Some(value) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
            bail!("invalid prometheus label value `{part}`");
        };
        labels.insert(key.to_string(), value.to_string());
    }
    Ok(labels)
}

fn parse_label_u32(labels: &std::collections::HashMap<String, String>, key: &str) -> Result<u32> {
    labels
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("metric labels missing `{key}`"))?
        .parse::<u32>()
        .with_context(|| format!("parse metric label `{key}`"))
}

fn parse_label_i32(labels: &std::collections::HashMap<String, String>, key: &str) -> Result<i32> {
    labels
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("metric labels missing `{key}`"))?
        .parse::<i32>()
        .with_context(|| format!("parse metric label `{key}`"))
}

fn parse_label_u8(labels: &std::collections::HashMap<String, String>, key: &str) -> Result<u8> {
    labels
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("metric labels missing `{key}`"))?
        .parse::<u8>()
        .with_context(|| format!("parse metric label `{key}`"))
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

async fn register_orchestrator_worker(endpoint: &str, worker_id: &str, addr: &str) -> Result<()> {
    let mut client = OrchestratorClient::connect(endpoint.to_string()).await?;
    client
        .register_worker(tonic::Request::new(WorkerRegistration {
            worker_id: worker_id.to_string(),
            addr: addr.to_string(),
        }))
        .await
        .with_context(|| format!("register worker {worker_id} with Orchestrator at {endpoint}"))?;
    Ok(())
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

fn select_merge_preview_candidate(
    preview: &SplitMergePreviewJson,
) -> Option<SplitMergePreviewPlanJson> {
    preview
        .plans
        .iter()
        .find(|plan| plan.kind == "merge")
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

fn multi_depth_activation_submission_command(
    orch_endpoint: &str,
    operation_id: &str,
    parent: CellId,
    targets: &[InternalMultiDepthActivationTarget],
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
        format!("--depth {}", parent.depth),
        format!("--sub {}", parent.sub),
    ];
    let mut targets = targets.to_vec();
    targets.sort_by_key(|target| {
        (
            target.cell.world,
            target.cell.cy,
            target.cell.cx,
            target.cell.depth,
            target.cell.sub,
        )
    });
    parts.extend(targets.into_iter().map(|target| {
        format!(
            "--target-cell {},{},{},{},{}={}",
            target.cell.world,
            target.cell.cx,
            target.cell.cy,
            target.cell.depth,
            target.cell.sub,
            target.worker_id
        )
    }));
    parts.join(" ")
}

fn merge_activation_submission_command(
    orch_endpoint: &str,
    operation_id: &str,
    parent: CellId,
    owner_worker_id: &str,
) -> String {
    let orch_addr = orch_endpoint
        .strip_prefix("http://")
        .or_else(|| orch_endpoint.strip_prefix("https://"))
        .unwrap_or(orch_endpoint);
    [
        "cargo xt merge-activation".to_string(),
        format!("--orch-addr {orch_addr}"),
        format!("--operation-id {operation_id}"),
        format!("--world {}", parent.world),
        format!("--cx {}", parent.cx),
        format!("--cy {}", parent.cy),
        format!("--depth {}", parent.depth),
        format!("--owner-worker-id {owner_worker_id}"),
    ]
    .join(" ")
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
            "guarded_kubernetes_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn write_merge_activation_plan_report(
    plan: &MergeActivationOperatorPlan,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_merge_activation_plan_path);
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
        "schema": "tessera.merge_activation_plan.v1",
        "unix_ts": unix_timestamp_secs(),
        "status": plan.status,
        "reason": plan.reason,
        "operation_id": plan.operation_id,
        "activation_mutated": false,
        "runtime_activation": {
            "state": "manual_available",
            "reason": "same-Worker merge activation is available through SubmitMergeActivation when TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual is enabled"
        },
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
            "owner_worker_id": plan.owner_worker_id,
            "siblings": plan.siblings,
            "submission_command": plan.submission_command,
            "required_preconditions": [
                "operator reviewed that all siblings are cold and share a registered owner",
                "TESSERA_ORCH_SPLIT_MERGE_ACTIVATION=manual is enabled only for the controlled smoke window",
                "parent route must not already be published",
                "run merge-activation-smoke or equivalent convergence checks after submit"
            ]
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": [
            "guarded_kubernetes_merge_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn write_split_planner_activation_report(
    plan: &SplitActivationOperatorPlan,
    status: &str,
    reason: &str,
    allow_mutation: bool,
    policy_id: Option<&str>,
    response: Option<&SplitActivationResponse>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    write_planner_activation_report(
        "split",
        status,
        reason,
        allow_mutation,
        policy_id,
        split_planner_plan_json(plan),
        response.map(split_planner_response_json).transpose()?,
        out,
    )
}

fn write_merge_planner_activation_report(
    plan: &MergeActivationOperatorPlan,
    status: &str,
    reason: &str,
    allow_mutation: bool,
    policy_id: Option<&str>,
    response: Option<&MergeActivationResponse>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    write_planner_activation_report(
        "merge",
        status,
        reason,
        allow_mutation,
        policy_id,
        merge_planner_plan_json(plan),
        response.map(merge_planner_response_json).transpose()?,
        out,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_planner_activation_report(
    planner_kind: &str,
    status: &str,
    reason: &str,
    allow_mutation: bool,
    policy_id: Option<&str>,
    plan: serde_json::Value,
    response: Option<serde_json::Value>,
    out: Option<&Path>,
) -> Result<PathBuf> {
    let report_path = out
        .map(Path::to_path_buf)
        .unwrap_or_else(default_planner_activation_path);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let activation_mutated = status == "published";
    let policy_accepted = planner_mutation_policy_accepted(allow_mutation, policy_id);
    let planner_ready = plan
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|plan_status| plan_status == "ready");
    let submit_published = response
        .as_ref()
        .and_then(|value| value.get("state"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|state| state == "published");
    let assignments_changed = response
        .as_ref()
        .and_then(|value| value.get("assignments_changed"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let remaining_uncovered = if activation_mutated {
        vec!["guarded_kubernetes_planner_mutation"]
    } else {
        vec![
            "policy_approved_planner_mutation",
            "guarded_kubernetes_planner_mutation",
        ]
    };
    let report = serde_json::json!({
        "schema": "tessera.planner_activation.v1",
        "unix_ts": unix_timestamp_secs(),
        "planner_kind": planner_kind,
        "status": status,
        "reason": reason,
        "activation_mode": "policy_gated",
        "activation_mutated": activation_mutated,
        "policy": {
            "allow_mutation": allow_mutation,
            "supplied_policy_id": policy_id,
            "required_policy_id": PLANNER_MUTATION_POLICY_ID,
            "accepted": policy_accepted
        },
        "plan": plan,
        "response": response,
        "checks": {
            "policy_gate_default_off": true,
            "policy_id_required": true,
            "planner_selected_ready_plan": planner_ready,
            "policy_accepted": policy_accepted,
            "activation_mutated": activation_mutated,
            "submit_activation_published": submit_published,
            "assignments_changed": assignments_changed
        },
        "remaining_uncovered": remaining_uncovered
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    fs::write(&report_path, body)?;
    Ok(report_path)
}

fn split_planner_plan_json(plan: &SplitActivationOperatorPlan) -> serde_json::Value {
    serde_json::json!({
        "schema": "tessera.split_activation_plan.v1",
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
            "selected_plan": preview_plan_json(plan.selected_plan.as_ref())
        },
        "orchestrator": {
            "addr": plan.orch_addr,
            "status": plan.health.status,
            "configured_workers": plan.health.configured_workers,
            "registered_workers": plan.health.registered_workers,
            "assigned_cells": plan.health.assigned_cells
        },
        "workers": planner_workers_json(&plan.workers),
        "recommendation": {
            "parent": plan.parent,
            "source_worker_id": plan.source_worker_id,
            "targets": plan.recommended_targets.iter().map(|target| {
                serde_json::json!({
                    "sub": target.sub,
                    "worker_id": target.worker_id
                })
            }).collect::<Vec<_>>(),
            "submission_command": plan.submission_command
        }
    })
}

fn merge_planner_plan_json(plan: &MergeActivationOperatorPlan) -> serde_json::Value {
    serde_json::json!({
        "schema": "tessera.merge_activation_plan.v1",
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
            "selected_plan": preview_plan_json(plan.selected_plan.as_ref())
        },
        "orchestrator": {
            "addr": plan.orch_addr,
            "status": plan.health.status,
            "configured_workers": plan.health.configured_workers,
            "registered_workers": plan.health.registered_workers,
            "assigned_cells": plan.health.assigned_cells
        },
        "workers": planner_workers_json(&plan.workers),
        "recommendation": {
            "parent": plan.parent,
            "owner_worker_id": plan.owner_worker_id,
            "siblings": plan.siblings,
            "submission_command": plan.submission_command
        }
    })
}

fn preview_plan_json(plan: Option<&SplitMergePreviewPlanJson>) -> Option<serde_json::Value> {
    plan.map(|selected| {
        serde_json::json!({
            "kind": selected.kind,
            "cell": selected.cell,
            "pressure_signals": selected.pressure_signals,
            "score": selected.score,
            "required_handover_ops": selected.required_handover_ops,
            "cells_moved": selected.cells_moved
        })
    })
}

fn planner_workers_json(workers: &[SplitActivationPlanWorker]) -> Vec<serde_json::Value> {
    workers
        .iter()
        .map(|worker| {
            serde_json::json!({
                "worker_id": worker.worker_id,
                "addr": worker.addr,
                "cell_count": worker.cell_count,
                "registered": worker.registered,
                "active_handover": worker.active_handover
            })
        })
        .collect()
}

fn split_planner_response_json(response: &SplitActivationResponse) -> Result<serde_json::Value> {
    let children = response
        .staged_children
        .iter()
        .map(|child| {
            let cell = child
                .cell
                .as_ref()
                .map(proto_assignment_to_cell)
                .transpose()?;
            Ok(serde_json::json!({
                "cell": cell,
                "target_worker_id": child.target_worker_id
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(serde_json::json!({
        "accepted": response.accepted,
        "state": split_activation_state_name(response.state),
        "assignments_changed": response.assignments_changed,
        "source_worker_id": response.source_worker_id,
        "reason": response.reason,
        "children": children
    }))
}

fn merge_planner_response_json(response: &MergeActivationResponse) -> Result<serde_json::Value> {
    let children = response
        .merged_children
        .iter()
        .map(proto_assignment_to_cell)
        .collect::<Result<Vec<_>>>()?;
    Ok(serde_json::json!({
        "accepted": response.accepted,
        "state": merge_activation_state_name(response.state),
        "assignments_changed": response.assignments_changed,
        "owner_worker_id": response.owner_worker_id,
        "reason": response.reason,
        "children": children
    }))
}

fn default_split_activation_plan_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("split-activation-plan-latest.json")
}

fn default_merge_activation_plan_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("merge-activation-plan-latest.json")
}

const PLANNER_MUTATION_POLICY_ID: &str = "operator_approved_planner_mutation_v1";

fn default_planner_activation_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("planner-activation-latest.json")
}

fn default_merge_activation_soak_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("merge-activation-soak-latest.json")
}

fn default_merge_activation_cross_worker_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("merge-activation-cross-worker-smoke-latest.json")
}

fn default_canonical_merge_activation_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("canonical-merge-activation-smoke-latest.json")
}

fn default_canonical_merge_activation_restart_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("canonical-merge-activation-restart-smoke-latest.json")
}

fn default_canonical_merge_activation_failure_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("canonical-merge-activation-failure-smoke-latest.json")
}

fn default_canonical_merge_activation_soak_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("canonical-merge-activation-soak-latest.json")
}

fn default_multi_depth_activation_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("multi-depth-activation-smoke-latest.json")
}

fn default_multi_depth_activation_failure_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("multi-depth-activation-failure-smoke-latest.json")
}

fn default_multi_depth_activation_restart_smoke_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("multi-depth-activation-restart-smoke-latest.json")
}

fn default_multi_depth_activation_soak_path() -> PathBuf {
    workspace_root()
        .join(".dev/reports")
        .join("multi-depth-activation-soak-latest.json")
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

fn validate_split_activation_target_mode(
    raw_targets: &[String],
    raw_cell_targets: &[String],
) -> Result<()> {
    if !raw_targets.is_empty() && !raw_cell_targets.is_empty() {
        bail!("split activation accepts either --target or --target-cell, not both");
    }
    if raw_targets.is_empty() && raw_cell_targets.is_empty() {
        bail!("split activation requires either four --target values or four --target-cell values");
    }
    Ok(())
}

fn parse_split_activation_cell_targets(raw_targets: &[String]) -> Result<Vec<(CellId, String)>> {
    if raw_targets.len() != 4 {
        bail!("split activation requires exactly four --target-cell values");
    }

    let mut targets = Vec::with_capacity(raw_targets.len());
    for raw in raw_targets {
        let Some((cell_raw, worker_raw)) = raw.split_once('=') else {
            bail!("invalid --target-cell `{raw}`; expected world,cx,cy,depth,sub=worker-id");
        };
        let cell = parse_cell_id_tuple(cell_raw.trim())
            .with_context(|| format!("invalid --target-cell `{raw}`"))?;
        let worker_id = worker_raw.trim();
        if worker_id.is_empty() {
            bail!(
                "target worker id must not be empty for child world={},cx={},cy={},depth={},sub={}",
                cell.world,
                cell.cx,
                cell.cy,
                cell.depth,
                cell.sub
            );
        }
        if targets.iter().any(|(existing, _)| existing == &cell) {
            bail!(
                "duplicate target cell world={},cx={},cy={},depth={},sub={}",
                cell.world,
                cell.cx,
                cell.cy,
                cell.depth,
                cell.sub
            );
        }
        targets.push((cell, worker_id.to_string()));
    }
    Ok(targets)
}

fn parse_cell_id_tuple(raw: &str) -> Result<CellId> {
    let parts = raw.split(',').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 5 {
        bail!("expected world,cx,cy,depth,sub");
    }
    Ok(CellId {
        world: parts[0]
            .parse()
            .with_context(|| format!("invalid world `{}`", parts[0]))?,
        cx: parts[1]
            .parse()
            .with_context(|| format!("invalid cx `{}`", parts[1]))?,
        cy: parts[2]
            .parse()
            .with_context(|| format!("invalid cy `{}`", parts[2]))?,
        depth: parts[3]
            .parse()
            .with_context(|| format!("invalid depth `{}`", parts[3]))?,
        sub: parts[4]
            .parse()
            .with_context(|| format!("invalid sub `{}`", parts[4]))?,
    })
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
            parent: Some(cell_to_proto_assignment(parent)),
            targets: targets
                .iter()
                .map(|(sub, target_worker_id)| SplitChildTarget {
                    sub: *sub,
                    target_worker_id: target_worker_id.clone(),
                    cell: None,
                })
                .collect(),
        }))
        .await?
        .into_inner();

    Ok(response)
}

async fn submit_split_activation_with_child_cells(
    endpoint: &str,
    operation_id: String,
    parent: CellId,
    targets: &[(CellId, String)],
) -> Result<SplitActivationResponse> {
    let mut client = OrchestratorClient::connect(endpoint.to_string()).await?;
    let response = client
        .submit_split_activation(tonic::Request::new(SplitActivationRequest {
            operation_id,
            parent: Some(cell_to_proto_assignment(parent)),
            targets: targets
                .iter()
                .map(|(cell, target_worker_id)| SplitChildTarget {
                    sub: cell.sub.into(),
                    target_worker_id: target_worker_id.clone(),
                    cell: Some(cell_to_proto_assignment(*cell)),
                })
                .collect(),
        }))
        .await?
        .into_inner();

    Ok(response)
}

async fn submit_merge_activation(
    endpoint: &str,
    operation_id: String,
    parent: CellId,
    owner_worker_id: String,
) -> Result<MergeActivationResponse> {
    let mut client = OrchestratorClient::connect(endpoint.to_string()).await?;
    let response = client
        .submit_merge_activation(tonic::Request::new(MergeActivationRequest {
            operation_id,
            parent: Some(cell_to_proto_assignment(parent)),
            owner_worker_id,
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

fn assert_merge_activation_published(response: &MergeActivationResponse) -> Result<()> {
    if !response.accepted
        || response.state != MergeActivationState::Published as i32
        || !response.assignments_changed
        || response.merged_children.len() != 4
    {
        bail!(
            "merge activation smoke failed: accepted={} state={} assignments_changed={} children={} reason={}",
            response.accepted,
            response.state,
            response.assignments_changed,
            response.merged_children.len(),
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

fn print_merge_activation_response(response: &MergeActivationResponse) -> Result<()> {
    println!(
        "merge activation: accepted={} state={} assignments_changed={} owner_worker_id={} reason={}",
        response.accepted,
        merge_activation_state_name(response.state),
        response.assignments_changed,
        response.owner_worker_id,
        response.reason
    );
    for child in &response.merged_children {
        println!(
            "merged child world={} cx={} cy={} depth={} sub={}",
            child.world, child.cx, child.cy, child.depth, child.sub
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

fn merge_activation_state_name(value: i32) -> &'static str {
    match MergeActivationState::try_from(value).unwrap_or(MergeActivationState::Unspecified) {
        MergeActivationState::Unspecified => "unspecified",
        MergeActivationState::Disabled => "disabled",
        MergeActivationState::Rejected => "rejected",
        MergeActivationState::Published => "published",
        MergeActivationState::Failed => "failed",
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

fn assignment_listing_summary_json(listing: &AssignmentListing) -> Result<serde_json::Value> {
    let mut workers = Vec::with_capacity(listing.workers.len());
    for bundle in &listing.workers {
        let mut cells = bundle
            .cells
            .iter()
            .map(proto_assignment_to_cell)
            .collect::<Result<Vec<_>>>()?;
        sort_cells(&mut cells);
        workers.push(serde_json::json!({
            "worker_id": bundle.worker_id.as_str(),
            "addr": bundle.addr.as_str(),
            "cells": cells.iter().map(|cell| cell_id_json(*cell)).collect::<Vec<_>>()
        }));
    }
    workers.sort_by(|left, right| {
        left.get("worker_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .cmp(
                right
                    .get("worker_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            )
    });
    Ok(serde_json::json!({
        "workers": workers,
        "handovers": listing.handovers.len()
    }))
}

fn listing_cell_owners(listing: &AssignmentListing, cell: CellId) -> Result<Vec<String>> {
    let mut owners = Vec::new();
    for bundle in &listing.workers {
        for assignment in &bundle.cells {
            if proto_assignment_to_cell(assignment)? == cell {
                owners.push(bundle.worker_id.clone());
            }
        }
    }
    owners.sort();
    Ok(owners)
}

fn cell_id_json(cell: CellId) -> serde_json::Value {
    serde_json::json!({
        "world": cell.world,
        "cx": cell.cx,
        "cy": cell.cy,
        "depth": cell.depth,
        "sub": cell.sub
    })
}

fn cell_to_proto_assignment(cell: CellId) -> Assignment {
    Assignment {
        world: cell.world,
        cx: cell.cx,
        cy: cell.cy,
        depth: u32::from(cell.depth),
        sub: u32::from(cell.sub),
    }
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

#[derive(Debug, Clone)]
struct MultiDepthConvergenceProbe {
    succeeded: Vec<CellId>,
    failures: Vec<MultiDepthConvergenceFailure>,
}

#[derive(Debug, Clone)]
struct MultiDepthConvergenceFailure {
    cell: CellId,
    worker_id: String,
    error: String,
}

impl MultiDepthConvergenceProbe {
    fn assert_success(&self) -> Result<()> {
        if self.failures.is_empty() && self.succeeded.len() == 4 {
            return Ok(());
        }
        bail!(
            "multi-depth convergence probe expected all children to pass, succeeded={:?}, failures={:?}",
            self.succeeded,
            self.failures
        )
    }

    fn assert_failed_cells_only(&self, expected_failed: &[CellId]) -> Result<()> {
        let mut actual = self
            .failures
            .iter()
            .map(|failure| failure.cell)
            .collect::<Vec<_>>();
        sort_cells(&mut actual);
        let mut expected = expected_failed.to_vec();
        sort_cells(&mut expected);
        if actual == expected {
            return Ok(());
        }
        bail!(
            "multi-depth convergence probe expected failed child cells {:?}, got {:?}; succeeded={:?}, failures={:?}",
            expected,
            actual,
            self.succeeded,
            self.failures
        )
    }
}

fn probe_multi_depth_convergence(
    gateway_addr: &str,
    expected: &[(CellId, &str)],
    ts_base: u64,
) -> MultiDepthConvergenceProbe {
    let mut succeeded = Vec::new();
    let mut failures = Vec::new();
    for (idx, (cell, worker_id)) in expected.iter().enumerate() {
        match assert_gateway_ping_until(gateway_addr, *cell, ts_base + idx as u64) {
            Ok(()) => succeeded.push(*cell),
            Err(err) => failures.push(MultiDepthConvergenceFailure {
                cell: *cell,
                worker_id: (*worker_id).to_string(),
                error: err.to_string(),
            }),
        }
    }
    MultiDepthConvergenceProbe {
        succeeded,
        failures,
    }
}

fn sort_cells(cells: &mut [CellId]) {
    cells.sort_by_key(|cell| (cell.world, cell.cx, cell.cy, cell.depth, cell.sub));
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
    operator_plan_source: Option<&'a str>,
    operator_plan_report_path: Option<&'a Path>,
}

struct MultiDepthActivationSmokeReport<'a> {
    operation_id: &'a str,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    worker_a_metrics_addr: &'a str,
    worker_b_metrics_addr: &'a str,
    parent: CellId,
    children: &'a [(CellId, &'a str)],
    gateway_routes: f64,
    gateway_route_change_reconnects: f64,
    worker_b_relay_connections: f64,
}

struct MultiDepthActivationFailureSmokeReport<'a> {
    operation_id: &'a str,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    parent: CellId,
    children: &'a [(CellId, &'a str)],
    gateway_routes_after_failure: f64,
    failure_probe: &'a MultiDepthConvergenceProbe,
    recovery_probe: &'a MultiDepthConvergenceProbe,
}

struct MultiDepthActivationRestartSmokeReport<'a> {
    operation_id: &'a str,
    assignment_state_path: &'a Path,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    parent: CellId,
    children: &'a [(CellId, &'a str)],
    gateway_routes: f64,
    restart_probe: &'a MultiDepthConvergenceProbe,
    worker_b_remote_interest_clients: f64,
    worker_b_remote_interest_cells: f64,
}

struct MultiDepthActivationSoakReport<'a> {
    operation_id: &'a str,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    parent: CellId,
    children: &'a [(CellId, &'a str)],
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

struct MergeActivationSmokeReport<'a> {
    operation_id: &'a str,
    plan_report_path: &'a Path,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_a_metrics_addr: &'a str,
    parent: CellId,
    merged_children: &'a [CellId],
    owner_worker_id: &'a str,
    gateway_routes: f64,
    assignment_state_path: Option<&'a Path>,
    orchestrator_restarted: bool,
    gateway_routes_after_restart: Option<f64>,
    ignored_frames_before_parent_delta: u64,
    remote_delta_frames_before_parent_delta: u64,
    remote_snapshot_frames_before_parent_delta: u64,
}

struct MergeActivationFailureSmokeReport<'a> {
    operation_id: &'a str,
    plan_report_path: &'a Path,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    parent: CellId,
    merged_children: &'a [CellId],
    owner_worker_id: &'a str,
    gateway_routes_before_failure: f64,
    gateway_routes_after_failure: f64,
    gateway_routes_after_recovery: f64,
    failure_error: &'a str,
    ignored_frames_before_parent_delta: u64,
    remote_delta_frames_before_parent_delta: u64,
    remote_snapshot_frames_before_parent_delta: u64,
}

struct MergeActivationCrossWorkerSmokeReport<'a> {
    operation_id: &'a str,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    worker_a_metrics_addr: &'a str,
    worker_b_metrics_addr: &'a str,
    parent: CellId,
    remote_child: CellId,
    owner_worker_id: &'a str,
    remote_source_worker_id: &'a str,
    gateway_routes: f64,
    worker_a_parent_actor_count: f64,
    worker_a_relay_connections: f64,
    worker_b_remote_relay_frames_sent: f64,
    local_ignored_frames_before_parent_delta: u64,
    remote_ignored_frames_before_parent_delta: u64,
    remote_delta_frames_before_parent_delta: u64,
    remote_snapshot_frames_before_parent_delta: u64,
}

struct MergeActivationSoakReport<'a> {
    operation_id: &'a str,
    plan_report_path: &'a Path,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_a_metrics_addr: &'a str,
    parent: CellId,
    merged_children: &'a [CellId],
    owner_worker_id: &'a str,
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
    worker_a_relay_connections: f64,
    ignored_frames_before_parent_delta: u64,
    remote_delta_frames_before_parent_delta: u64,
    remote_snapshot_frames_before_parent_delta: u64,
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

struct ActivationRestartSmokeReport<'a> {
    operation_id: &'a str,
    assignment_state_path: &'a Path,
    gateway_addr: &'a str,
    gateway_metrics_addr: &'a str,
    orch_addr: &'a str,
    worker_a_addr: &'a str,
    worker_b_addr: &'a str,
    gateway_routes: f64,
    restart_probe: &'a SplitConvergenceProbe,
    post_restart_stats: ActivationSoakStats,
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
        "operator_plan": {
            "source": input.operator_plan_source,
            "report_path": input.operator_plan_report_path.map(|path| path.display().to_string())
        },
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
            "guarded_kubernetes_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = report_dir.join("activation-smoke-latest.json");
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_multi_depth_activation_smoke_report(
    input: MultiDepthActivationSmokeReport<'_>,
) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let report = serde_json::json!({
        "schema": "tessera.multi_depth_activation_smoke.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_b": input.worker_b_addr,
            "worker_a_metrics": input.worker_a_metrics_addr,
            "worker_b_metrics": input.worker_b_metrics_addr
        },
        "parent": input.parent,
        "children": input.children.iter().map(|(cell, worker_id)| {
            serde_json::json!({
                "cell": cell,
                "worker_id": worker_id
            })
        }).collect::<Vec<_>>(),
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
        "remaining_uncovered": [
            "guarded_kubernetes_multi_depth_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = default_multi_depth_activation_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_multi_depth_activation_failure_smoke_report(
    input: MultiDepthActivationFailureSmokeReport<'_>,
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
                "cell": failure.cell,
                "worker_id": failure.worker_id,
                "error": failure.error
            })
        })
        .collect::<Vec<_>>();
    let recovery_details = input
        .recovery_probe
        .failures
        .iter()
        .map(|failure| {
            serde_json::json!({
                "cell": failure.cell,
                "worker_id": failure.worker_id,
                "error": failure.error
            })
        })
        .collect::<Vec<_>>();
    let failed_child_cells = input
        .failure_probe
        .failures
        .iter()
        .map(|failure| failure.cell)
        .collect::<Vec<_>>();
    let report = serde_json::json!({
        "schema": "tessera.multi_depth_activation_failure_smoke.v1",
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
        "parent": input.parent,
        "children": input.children.iter().map(|(cell, worker_id)| {
            serde_json::json!({
                "cell": cell,
                "worker_id": worker_id
            })
        }).collect::<Vec<_>>(),
        "checks": {
            "submit_split_activation_published": true,
            "orchestrator_listing_child_routes_after_failure": 4,
            "gateway_ready_routes_after_failure": input.gateway_routes_after_failure,
            "post_publish_target_outage_detected": true,
            "failed_child_cells": failed_child_cells,
            "succeeded_child_cells_during_failure": &input.failure_probe.succeeded,
            "automatic_rollback_observed": false,
            "operator_recovery_required": true,
            "target_worker_restart_recovered_convergence": true,
            "recovered_child_cells": &input.recovery_probe.succeeded
        },
        "failure_probe": {
            "failures": failure_details
        },
        "recovery_probe": {
            "failures": recovery_details
        },
        "rollback_policy": p5_rollback_policy_json(),
        "remaining_uncovered": [
            "guarded_kubernetes_multi_depth_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = default_multi_depth_activation_failure_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_multi_depth_activation_restart_smoke_report(
    input: MultiDepthActivationRestartSmokeReport<'_>,
) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let restart_failures = input
        .restart_probe
        .failures
        .iter()
        .map(|failure| {
            serde_json::json!({
                "cell": failure.cell,
                "worker_id": failure.worker_id,
                "error": failure.error
            })
        })
        .collect::<Vec<_>>();
    let report = serde_json::json!({
        "schema": "tessera.multi_depth_activation_restart_smoke.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual_publish_then_default_off_restart",
        "assignment_state_path": input.assignment_state_path.display().to_string(),
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_b": input.worker_b_addr
        },
        "parent": input.parent,
        "children": input.children.iter().map(|(cell, worker_id)| {
            serde_json::json!({
                "cell": cell,
                "worker_id": worker_id
            })
        }).collect::<Vec<_>>(),
        "checks": {
            "submit_split_activation_published": true,
            "assignment_state_file_written": true,
            "orchestrator_restarted": true,
            "restarted_with_manual_activation_disabled": true,
            "restarted_orchestrator_loaded_child_routes": true,
            "worker_assignment_refresh_after_restart": true,
            "gateway_ready_routes_after_restart": input.gateway_routes,
            "child_ping_all_routes_after_restart": true,
            "remote_aoi_interest_resync_after_restart": true,
            "worker_b_remote_interest_clients_after_restart": input.worker_b_remote_interest_clients,
            "worker_b_remote_interest_cells_after_restart": input.worker_b_remote_interest_cells,
            "recovered_child_cells_after_restart": &input.restart_probe.succeeded
        },
        "restart_probe": {
            "failures": restart_failures
        },
        "remaining_uncovered": [
            "guarded_kubernetes_multi_depth_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = default_multi_depth_activation_restart_smoke_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_multi_depth_activation_soak_report(
    input: MultiDepthActivationSoakReport<'_>,
) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let report = serde_json::json!({
        "schema": "tessera.multi_depth_activation_soak.v1",
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
        "parent": input.parent,
        "children": input.children.iter().map(|(cell, worker_id)| {
            serde_json::json!({
                "cell": cell,
                "worker_id": worker_id
            })
        }).collect::<Vec<_>>(),
        "traffic": {
            "iterations_per_child": input.iterations,
            "sleep_ms": input.sleep_ms,
            "actors": input.children.len(),
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
            "guarded_kubernetes_multi_depth_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = default_multi_depth_activation_soak_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_merge_activation_smoke_report(input: MergeActivationSmokeReport<'_>) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let schema = if input.orchestrator_restarted {
        "tessera.merge_activation_restart_smoke.v1"
    } else {
        "tessera.merge_activation_smoke.v1"
    };
    let report = serde_json::json!({
        "schema": schema,
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "assignment_state_path": input.assignment_state_path.map(|path| path.display().to_string()),
        "operator_plan": {
            "schema": "tessera.merge_activation_plan.v1",
            "report_path": input.plan_report_path.display().to_string()
        },
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_a_metrics": input.worker_a_metrics_addr
        },
        "parent": input.parent,
        "owner_worker_id": input.owner_worker_id,
        "merged_children": input.merged_children,
        "checks": {
            "submit_merge_activation_published": true,
            "orchestrator_listing_parent_route": true,
            "gateway_ready_routes": input.gateway_routes,
            "parent_ping_route": true,
            "worker_coalesced_child_actors": true,
            "stable_session_parent_move": true,
            "orchestrator_restarted": input.orchestrator_restarted,
            "restarted_orchestrator_loaded_parent_route": input.orchestrator_restarted,
            "gateway_ready_routes_after_restart": input.gateway_routes_after_restart.unwrap_or(0.0),
            "ignored_frames_before_parent_delta": input.ignored_frames_before_parent_delta,
            "remote_delta_frames_before_parent_delta": input.remote_delta_frames_before_parent_delta,
            "remote_snapshot_frames_before_parent_delta": input.remote_snapshot_frames_before_parent_delta
        },
        "rollback_policy": {
            "policy_id": "operator_controlled_manual_merge_v1",
            "automatic_rollback": false,
            "backout": "re-run manual split activation from the parent only after operator review"
        },
        "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
        "remaining_uncovered": [
            "guarded_kubernetes_merge_activation_smoke",
            "cross_worker_merge_replay",
            "automatic_planner_merge_mutation"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = if input.orchestrator_restarted && input.parent.depth > 0 {
        default_canonical_merge_activation_restart_path()
    } else if input.orchestrator_restarted {
        report_dir.join("merge-activation-restart-smoke-latest.json")
    } else if input.parent.depth > 0 {
        default_canonical_merge_activation_path()
    } else {
        report_dir.join("merge-activation-smoke-latest.json")
    };
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_merge_activation_cross_worker_smoke_report(
    input: MergeActivationCrossWorkerSmokeReport<'_>,
) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let report = serde_json::json!({
        "schema": "tessera.merge_activation_cross_worker_smoke.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "operator_plan": {
            "schema": "manual_cross_worker_merge_replay.v1",
            "reason": "mixed-owner sibling family submitted through default-off SubmitMergeActivation"
        },
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_b": input.worker_b_addr,
            "worker_a_metrics": input.worker_a_metrics_addr,
            "worker_b_metrics": input.worker_b_metrics_addr
        },
        "parent": input.parent,
        "owner_worker_id": input.owner_worker_id,
        "remote_source_worker_id": input.remote_source_worker_id,
        "remote_child": input.remote_child,
        "merged_children": [
            activation_child_cell(0),
            activation_child_cell(1),
            activation_child_cell(2),
            activation_child_cell(3)
        ],
        "checks": {
            "submit_merge_activation_published": true,
            "orchestrator_listing_parent_route": true,
            "gateway_ready_routes": input.gateway_routes,
            "parent_ping_route": true,
            "target_worker_coalesced_local_children": true,
            "remote_child_replayed_to_parent": true,
            "stable_session_parent_move_local_child": true,
            "stable_session_parent_move_remote_child": true,
            "worker_a_parent_actor_count": input.worker_a_parent_actor_count,
            "worker_a_relay_connections_total": input.worker_a_relay_connections,
            "worker_b_remote_relay_frames_sent_total": input.worker_b_remote_relay_frames_sent,
            "local_ignored_frames_before_parent_delta": input.local_ignored_frames_before_parent_delta,
            "remote_ignored_frames_before_parent_delta": input.remote_ignored_frames_before_parent_delta,
            "remote_delta_frames_before_parent_delta": input.remote_delta_frames_before_parent_delta,
            "remote_snapshot_frames_before_parent_delta": input.remote_snapshot_frames_before_parent_delta
        },
        "rollback_policy": {
            "policy_id": "operator_controlled_manual_merge_v1",
            "automatic_rollback": false,
            "backout": "re-run manual split activation from the parent only after operator review"
        },
        "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
        "remaining_uncovered": [
            "guarded_kubernetes_merge_activation_smoke",
            "automatic_planner_merge_mutation"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = default_merge_activation_cross_worker_path();
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_merge_activation_failure_smoke_report(
    input: MergeActivationFailureSmokeReport<'_>,
) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let report = serde_json::json!({
        "schema": "tessera.merge_activation_failure_smoke.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "operator_plan": {
            "schema": "tessera.merge_activation_plan.v1",
            "report_path": input.plan_report_path.display().to_string()
        },
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr
        },
        "parent": input.parent,
        "merged_children": input.merged_children,
        "owner_worker_id": input.owner_worker_id,
        "failure": {
            "owner_worker_id": input.owner_worker_id,
            "error": input.failure_error
        },
        "checks": {
            "submit_merge_activation_published": true,
            "orchestrator_listing_parent_route": true,
            "gateway_ready_routes_before_failure": input.gateway_routes_before_failure,
            "parent_ping_route_before_failure": true,
            "worker_coalesced_child_actors": true,
            "stable_session_parent_move_before_failure": true,
            "owner_worker_outage_detected": true,
            "automatic_rollback_observed": false,
            "parent_assignment_stayed_published": true,
            "gateway_ready_routes_after_failure": input.gateway_routes_after_failure,
            "owner_worker_restart_recovered_parent_route": true,
            "parent_ping_route_after_recovery": true,
            "gateway_ready_routes_after_recovery": input.gateway_routes_after_recovery,
            "ignored_frames_before_parent_delta": input.ignored_frames_before_parent_delta,
            "remote_delta_frames_before_parent_delta": input.remote_delta_frames_before_parent_delta,
            "remote_snapshot_frames_before_parent_delta": input.remote_snapshot_frames_before_parent_delta
        },
        "rollback_policy": {
            "policy_id": "operator_controlled_manual_merge_v1",
            "automatic_rollback": false,
            "backout": "re-run manual split activation from the parent only after operator review",
            "failure_recovery": "operator restores or restarts the owner Worker, then reruns parent route and traffic convergence checks"
        },
        "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
        "remaining_uncovered": [
            "guarded_kubernetes_merge_activation_smoke",
            "cross_worker_merge_replay",
            "automatic_planner_merge_mutation"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = if input.parent.depth > 0 {
        default_canonical_merge_activation_failure_path()
    } else {
        report_dir.join("merge-activation-failure-smoke-latest.json")
    };
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_merge_activation_soak_report(input: MergeActivationSoakReport<'_>) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let report = serde_json::json!({
        "schema": "tessera.merge_activation_soak.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual",
        "operator_plan": {
            "schema": "tessera.merge_activation_plan.v1",
            "report_path": input.plan_report_path.display().to_string()
        },
        "addresses": {
            "gateway": input.gateway_addr,
            "gateway_metrics": input.gateway_metrics_addr,
            "orchestrator": input.orch_addr,
            "worker_a": input.worker_a_addr,
            "worker_a_metrics": input.worker_a_metrics_addr
        },
        "parent": input.parent,
        "owner_worker_id": input.owner_worker_id,
        "merged_children": input.merged_children,
        "traffic": {
            "iterations_per_actor": input.iterations,
            "sleep_ms": input.sleep_ms,
            "actors": 4,
            "pings_ok": input.stats.pings_ok,
            "moves_ok": input.stats.moves_ok,
            "ignored_frames": input.stats.ignored_frames,
            "remote_delta_frames": input.stats.remote_delta_frames,
            "remote_snapshot_frames": input.stats.remote_snapshot_frames
        },
        "checks": {
            "submit_merge_activation_published": true,
            "orchestrator_listing_parent_route": true,
            "gateway_ready_routes_after_soak": input.gateway_routes,
            "parent_ping_iterations": input.stats.pings_ok,
            "parent_move_iterations": input.stats.moves_ok,
            "worker_coalesced_child_actors": true,
            "stable_session_parent_move_before_soak": true,
            "ignored_frames_before_parent_delta": input.ignored_frames_before_parent_delta,
            "remote_delta_frames_before_parent_delta": input.remote_delta_frames_before_parent_delta,
            "remote_snapshot_frames_before_parent_delta": input.remote_snapshot_frames_before_parent_delta,
            "gateway_ping_roundtrip_count": input.gateway_ping_roundtrips,
            "gateway_join_roundtrip_count": input.gateway_join_roundtrips,
            "gateway_move_roundtrip_count": input.gateway_move_roundtrips,
            "gateway_client_closes_no_route_total": input.gateway_no_route_closes,
            "gateway_client_closes_upstream_retry_exhausted_total": input.gateway_retry_exhausted_closes,
            "gateway_client_closes_ambiguous_upstream_total": input.gateway_ambiguous_upstream_closes,
            "worker_a_accepted_connections_total": input.worker_a_accepted_connections,
            "worker_a_relay_connections_total": input.worker_a_relay_connections
        },
        "rollback_policy": {
            "policy_id": "operator_controlled_manual_merge_v1",
            "automatic_rollback": false,
            "backout": "re-run manual split activation from the parent only after operator review"
        },
        "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
        "remaining_uncovered": [
            "guarded_kubernetes_merge_activation_smoke",
            "cross_worker_merge_replay",
            "automatic_planner_merge_mutation"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = if input.parent.depth > 0 {
        default_canonical_merge_activation_soak_path()
    } else {
        default_merge_activation_soak_path()
    };
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
            "guarded_kubernetes_activation_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = report_dir.join("activation-soak-latest.json");
    fs::write(&latest, body)?;
    Ok(latest)
}

fn write_activation_restart_smoke_report(
    input: ActivationRestartSmokeReport<'_>,
) -> Result<PathBuf> {
    let report_dir = workspace_root().join(".dev/reports");
    fs::create_dir_all(&report_dir)?;
    let unix_ts = unix_timestamp_secs();
    let failures = input
        .restart_probe
        .failures
        .iter()
        .map(|failure| {
            serde_json::json!({
                "sub": failure.sub,
                "error": failure.error,
            })
        })
        .collect::<Vec<_>>();
    let report = serde_json::json!({
        "schema": "tessera.activation_restart_smoke.v1",
        "unix_ts": unix_ts,
        "operation_id": input.operation_id,
        "activation_mode": "manual_publish_then_default_off_restart",
        "assignment_state_path": input.assignment_state_path.display().to_string(),
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
            "assignment_state_file_written": true,
            "orchestrator_restarted": true,
            "restarted_with_manual_activation_disabled": true,
            "restarted_orchestrator_loaded_child_routes": true,
            "worker_assignment_refresh_after_restart": true,
            "gateway_ready_routes_after_restart": input.gateway_routes,
            "child_ping_all_routes_after_restart": true,
            "child_move_iterations_after_restart": input.post_restart_stats.moves_ok,
            "remote_aoi_frames_observed_after_restart": input.post_restart_stats.remote_delta_frames + input.post_restart_stats.remote_snapshot_frames,
            "live_remote_aoi_resync_snapshot_after_restart": true
        },
        "restart_probe": {
            "succeeded": input.restart_probe.succeeded.clone(),
            "failures": failures
        },
        "remaining_uncovered": [
            "guarded_kubernetes_restart_recovery_smoke"
        ]
    });
    let body = format!("{}\n", serde_json::to_string_pretty(&report)?);
    let stamped = report_dir.join(format!("{}.json", input.operation_id));
    fs::write(&stamped, &body)?;
    let latest = report_dir.join("activation-restart-smoke-latest.json");
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
            "guarded_kubernetes_activation_smoke"
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

fn assert_prometheus_sample_at_least_until(
    service: &str,
    raw_addr: &str,
    metric: &str,
    min: f64,
) -> Result<f64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match assert_metrics_endpoint_body(service, raw_addr, &[metric]).and_then(|body| {
            assert_prometheus_sample_at_least(service, &body, metric, min)?;
            prometheus_sample_value(&body, metric)
        }) {
            Ok(value) => return Ok(value),
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

fn http_post(addr: SocketAddr, path: &str) -> Result<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
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

fn http_json_get(service: &str, raw_addr: &str, path: &str) -> Result<serde_json::Value> {
    let addr = readiness_addr(raw_addr)?;
    let response = http_get(addr, path)?;
    let body = http_response_body(service, &response)?;
    serde_json::from_str(body).with_context(|| format!("parse {service} JSON response"))
}

fn http_json_post(service: &str, raw_addr: &str, path: &str) -> Result<serde_json::Value> {
    let addr = readiness_addr(raw_addr)?;
    let response = http_post(addr, path)?;
    let body = http_response_body(service, &response)?;
    serde_json::from_str(body).with_context(|| format!("parse {service} JSON response"))
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
    use tessera_proto::orch::v1::AssignmentBundle;

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
    fn p7_operation_ledger_check_accepts_proposal_approval_and_blocked_execution() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-split-1",
                    "kind": "split",
                    "status": "blocked_by_policy",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 120,
                    "proposal": {
                        "source": "live_worker_metrics:test",
                        "proposal_hash": "fnv1a64:abc",
                        "parent": {"world": 0, "cx": 0, "cy": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 0}, "worker_id": "worker-a"},
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 1}, "worker_id": "worker-b"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt split-activation --operation-id p7-split-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "split",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:abc",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"},
                        {"name": "approval_recorded", "state": "succeeded", "unix_secs": 110, "reason": "approval persisted"},
                        {"name": "execution_blocked_by_policy", "state": "failed", "unix_secs": 120, "reason": "executor mutation is default-off"}
                    ]
                }
            ]
        });

        let summary = validate_p7_operation_ledger(&ledger, true, true, false, false, false)
            .expect("valid P7 ledger");

        assert_eq!(
            summary,
            P7OperationLedgerSummary {
                records: 1,
                proposal_records: 1,
                approval_records: 1,
                blocked_execution_records: 1,
                published_execution_records: 0,
                completed_observation_records: 0,
                recovery_required_records: 0,
            }
        );
    }

    #[test]
    fn p7_operation_ledger_check_accepts_published_execution() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-merge-1",
                    "kind": "merge",
                    "status": "observing",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 120,
                    "proposal": {
                        "source": "live_worker_metrics:test",
                        "proposal_hash": "fnv1a64:abc",
                        "parent": {"world": 0, "cx": 0, "cy": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0}, "worker_id": "worker-a"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt merge-activation --operation-id p7-merge-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "merge",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:abc",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"},
                        {"name": "approval_recorded", "state": "succeeded", "unix_secs": 110, "reason": "approval persisted"},
                        {"name": "execution_started", "state": "succeeded", "unix_secs": 120, "reason": "operation execution passed policy gates"},
                        {"name": "execution_published", "state": "succeeded", "unix_secs": 121, "reason": "merge activation published"}
                    ]
                }
            ]
        });

        let summary = validate_p7_operation_ledger(&ledger, true, false, true, false, false)
            .expect("valid P7 ledger");

        assert_eq!(
            summary,
            P7OperationLedgerSummary {
                records: 1,
                proposal_records: 1,
                approval_records: 1,
                blocked_execution_records: 0,
                published_execution_records: 1,
                completed_observation_records: 0,
                recovery_required_records: 0,
            }
        );
    }

    #[test]
    fn p7_operation_ledger_check_accepts_completed_observation() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-merge-1",
                    "kind": "merge",
                    "status": "completed",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 130,
                    "proposal": {
                        "source": "live_worker_metrics:test",
                        "proposal_hash": "fnv1a64:abc",
                        "parent": {"world": 0, "cx": 0, "cy": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0}, "worker_id": "worker-a"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt merge-activation --operation-id p7-merge-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "merge",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:abc",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"},
                        {"name": "approval_recorded", "state": "succeeded", "unix_secs": 110, "reason": "approval persisted"},
                        {"name": "execution_started", "state": "succeeded", "unix_secs": 120, "reason": "operation execution passed policy gates"},
                        {"name": "execution_published", "state": "succeeded", "unix_secs": 121, "reason": "merge activation published"},
                        {"name": "observation_completed", "state": "succeeded", "unix_secs": 130, "reason": "observer=smoke confirmed route_converged, worker_refreshed, traffic_confirmed, counters_clean"}
                    ]
                }
            ]
        });

        let summary = validate_p7_operation_ledger(&ledger, true, false, true, true, false)
            .expect("valid P7 ledger");

        assert_eq!(
            summary,
            P7OperationLedgerSummary {
                records: 1,
                proposal_records: 1,
                approval_records: 1,
                blocked_execution_records: 0,
                published_execution_records: 1,
                completed_observation_records: 1,
                recovery_required_records: 0,
            }
        );
    }

    #[test]
    fn p7_operation_ledger_check_accepts_recovery_required_observation() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-merge-1",
                    "kind": "merge",
                    "status": "recovery_required",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 130,
                    "proposal": {
                        "source": "live_worker_metrics:test",
                        "proposal_hash": "fnv1a64:abc",
                        "parent": {"world": 0, "cx": 0, "cy": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0}, "worker_id": "worker-a"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt merge-activation --operation-id p7-merge-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "merge",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:abc",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"},
                        {"name": "approval_recorded", "state": "succeeded", "unix_secs": 110, "reason": "approval persisted"},
                        {"name": "execution_started", "state": "succeeded", "unix_secs": 120, "reason": "operation execution passed policy gates"},
                        {"name": "execution_published", "state": "succeeded", "unix_secs": 121, "reason": "merge activation published"},
                        {"name": "observation_failed", "state": "failed", "unix_secs": 130, "reason": "observer=smoke reported incomplete observation; missing=traffic_confirmed,counters_clean"}
                    ]
                }
            ]
        });

        let summary = validate_p7_operation_ledger(&ledger, true, false, true, false, true)
            .expect("valid P7 recovery-required ledger");

        assert_eq!(
            summary,
            P7OperationLedgerSummary {
                records: 1,
                proposal_records: 1,
                approval_records: 1,
                blocked_execution_records: 0,
                published_execution_records: 1,
                completed_observation_records: 0,
                recovery_required_records: 1,
            }
        );
    }

    #[test]
    fn p7_operation_loop_smoke_report_accepts_default_off_path_evidence() {
        let case = |kind: &str| {
            serde_json::json!({
                "case": kind,
                "operation": {
                    "operation_id": format!("p7-{kind}-1"),
                    "kind": kind,
                    "proposal_hash": "fnv1a64:abc",
                    "policy_id": "operator_approved_dynamic_operation_v1"
                },
                "orchestrator": {
                    "grpc_addr": "127.0.0.1:6310",
                    "metrics_addr": "127.0.0.1:6311",
                    "registered_workers": 2,
                    "assignment_listing_before": {"workers": [], "handovers": 0},
                    "assignment_listing_after": {"workers": [], "handovers": 0}
                },
                "responses": {
                    "proposal": {
                        "assignments_changed": false,
                        "planned_count": 1,
                        "recorded_count": 1,
                        "already_recorded_count": 0,
                        "operation_ids": [format!("p7-{kind}-1")]
                    },
                    "approval": {
                        "status": "approved",
                        "assignments_changed": false
                    },
                    "execution": {
                        "status": "blocked_by_policy",
                        "assignments_changed": false,
                        "mutation_attempted": false,
                        "mutation_allowed": false
                    }
                },
                "checks": {
                    "proposal_recorded": true,
                    "approval_recorded": true,
                    "execution_blocked_by_policy": true,
                    "assignments_unchanged": true,
                    "mutation_attempted": false,
                    "mutation_allowed": false,
                    "ledger_check_passed": true
                }
            })
        };
        let report = serde_json::json!({
            "schema": "tessera.p7_operation_loop_smoke.v1",
            "unix_secs": 120,
            "ledger": {
                "path": ".dev/reports/p7-operation-loop-ledger-latest.json",
                "records": 3,
                "proposal_records": 3,
                "approval_records": 3,
                "blocked_execution_records": 3
            },
            "cases": [
                case("split"),
                case("merge"),
                case("multi_depth_split")
            ],
            "checks": {
                "split_operation_loop_smoke": true,
                "merge_operation_loop_smoke": true,
                "multi_depth_operation_loop_smoke": true,
                "proposal_approval_default_off_execution": true,
                "ledger_check_passed": true
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_loop_smoke"
            ]
        });

        validate_p7_operation_loop_smoke_report(&report)
            .expect("valid P7 operation loop smoke report");
    }

    #[test]
    fn p7_operation_execution_smoke_report_accepts_published_merge_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.p7_operation_execution_smoke.v1",
            "unix_secs": 120,
            "operation": {
                "operation_id": "p7-merge-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:abc",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "orchestrator": {
                "grpc_addr": "127.0.0.1:6320",
                "metrics_addr": "127.0.0.1:6321",
                "registered_workers": 2,
                "assignment_listing_before": {"workers": [], "handovers": 0},
                "assignment_listing_after": {"workers": [], "handovers": 0}
            },
            "ledger": {
                "path": ".dev/reports/p7-operation-execution-ledger-latest.json",
                "records": 1,
                "proposal_records": 1,
                "approval_records": 1,
                "published_execution_records": 1
            },
            "responses": {
                "proposal": {
                    "assignments_changed": false,
                    "planned_count": 1,
                    "recorded_count": 1,
                    "already_recorded_count": 0,
                    "operation_ids": ["p7-merge-1"]
                },
                "approval": {
                    "status": "approved",
                    "assignments_changed": false
                },
                "execution": {
                    "status": "published",
                    "assignments_changed": true,
                    "mutation_attempted": true,
                    "mutation_allowed": true
                },
                "repeat_execution": {
                    "status": "already_published",
                    "assignments_changed": false,
                    "mutation_attempted": false,
                    "mutation_allowed": true
                }
            },
            "checks": {
                "merge_execution_published": true,
                "parent_route_published": true,
                "repeat_execution_idempotent": true,
                "ledger_execution_published": true
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_execution_smoke"
            ]
        });

        validate_p7_operation_execution_smoke_report(&report)
            .expect("valid P7 operation execution smoke report");
    }

    #[test]
    fn p7_operation_observation_smoke_report_accepts_completed_observation_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.p7_operation_observation_smoke.v1",
            "unix_secs": 130,
            "operation": {
                "operation_id": "p7-merge-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:abc",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "orchestrator": {
                "grpc_addr": "127.0.0.1:6330",
                "metrics_addr": "127.0.0.1:6331",
                "registered_workers": 2,
                "assignment_listing_before": {"workers": [], "handovers": 0},
                "assignment_listing_after": {"workers": [], "handovers": 0}
            },
            "gateway": {
                "addr": "127.0.0.1:4330",
                "metrics_addr": "127.0.0.1:4331",
                "routes_before": 4,
                "routes_after": 1,
                "ping_roundtrips": 1,
                "join_roundtrips": 2,
                "move_roundtrips": 1,
                "close_counters": {
                    "before": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0},
                    "after": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0}
                }
            },
            "worker": {
                "worker_a_addr": "127.0.0.1:5331",
                "worker_a_metrics_addr": "127.0.0.1:5333",
                "worker_b_addr": "127.0.0.1:5332",
                "worker_b_metrics_addr": "127.0.0.1:5334",
                "parent_actor_count": 2
            },
            "ledger": {
                "path": ".dev/reports/p7-operation-observation-ledger-latest.json",
                "records": 1,
                "proposal_records": 1,
                "approval_records": 1,
                "published_execution_records": 1,
                "completed_observation_records": 1
            },
            "responses": {
                "proposal": {
                    "assignments_changed": false,
                    "planned_count": 1,
                    "recorded_count": 1,
                    "already_recorded_count": 0,
                    "operation_ids": ["p7-merge-1"]
                },
                "approval": {
                    "status": "approved",
                    "assignments_changed": false
                },
                "execution": {
                    "status": "published",
                    "assignments_changed": true,
                    "mutation_attempted": true,
                    "mutation_allowed": true
                },
                "observation": {
                    "status": "completed",
                    "assignments_changed": false,
                    "observation_accepted": true
                }
            },
            "checks": {
                "merge_execution_published": true,
                "route_converged": true,
                "worker_refreshed": true,
                "traffic_confirmed": true,
                "gateway_close_counters_clean": true,
                "stable_session_parent_move": true,
                "observation_completed": true,
                "ledger_observation_completed": true
            },
            "frames": {
                "ignored_before_parent_delta": 0,
                "remote_delta_before_parent_delta": 0,
                "remote_snapshot_before_parent_delta": 0
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_observation_smoke"
            ]
        });

        validate_p7_operation_observation_smoke_report(&report)
            .expect("valid P7 operation observation smoke report");
    }

    #[test]
    fn p7_operation_recovery_smoke_report_accepts_recovery_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.p7_operation_recovery_smoke.v1",
            "unix_secs": 140,
            "operation": {
                "operation_id": "p7-merge-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:abc",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "orchestrator": {
                "grpc_addr": "127.0.0.1:6340",
                "metrics_addr": "127.0.0.1:6341",
                "registered_workers": 2,
                "assignment_listing_before": {"workers": [], "handovers": 0},
                "assignment_listing_after": {"workers": [], "handovers": 0}
            },
            "gateway": {
                "addr": "127.0.0.1:4340",
                "metrics_addr": "127.0.0.1:4341",
                "close_counters": {
                    "before": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0},
                    "after_failure": {"no_route": 0, "upstream_retry_exhausted": 1, "ambiguous_upstream": 0}
                },
                "routes_after_recovery": 1
            },
            "ledger": {
                "path": ".dev/reports/p7-operation-recovery-ledger-latest.json",
                "records": 1,
                "proposal_records": 1,
                "approval_records": 1,
                "published_execution_records": 1,
                "recovery_required_records": 1
            },
            "responses": {
                "proposal": {"assignments_changed": false, "operation_ids": ["p7-merge-1"]},
                "approval": {"status": "approved", "assignments_changed": false},
                "execution": {"status": "published", "assignments_changed": true},
                "observation": {
                    "status": "recovery_required",
                    "assignments_changed": false,
                    "observation_accepted": false
                }
            },
            "failure": {
                "owner_worker_id": "worker-a",
                "error": "expected failure"
            },
            "checks": {
                "merge_execution_published": true,
                "owner_outage_detected": true,
                "observation_recovery_required": true,
                "no_automatic_rollback": true,
                "operator_recovery_confirmed": true,
                "ledger_recovery_required": true
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_recovery_smoke"
            ]
        });

        validate_p7_operation_recovery_smoke_report(&report)
            .expect("valid P7 operation recovery smoke report");
    }

    #[test]
    fn p7_operation_restart_smoke_report_accepts_restart_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.p7_operation_restart_smoke.v1",
            "unix_secs": 150,
            "operation": {
                "operation_id": "p7-merge-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:abc",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "orchestrator": {
                "grpc_addr": "127.0.0.1:6350",
                "metrics_addr": "127.0.0.1:6351",
                "registered_workers_before": 2,
                "registered_workers_after_restart": 2,
                "assignment_listing_before": {"workers": [], "handovers": 0},
                "assignment_listing_after_restart": {"workers": [], "handovers": 0},
                "assignment_state_path": ".dev/reports/p7-operation-restart-assignment-state-latest.json"
            },
            "gateway": {
                "addr": "127.0.0.1:4350",
                "metrics_addr": "127.0.0.1:4351",
                "routes_before": 4,
                "routes_after_restart": 1,
                "ping_roundtrips": 2,
                "join_roundtrips": 2,
                "move_roundtrips": 1,
                "close_counters": {
                    "before": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0},
                    "after_restart": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0}
                }
            },
            "worker": {
                "worker_a_addr": "127.0.0.1:5351",
                "worker_a_metrics_addr": "127.0.0.1:5353",
                "worker_b_addr": "127.0.0.1:5352",
                "worker_b_metrics_addr": "127.0.0.1:5354",
                "parent_actor_count_after_restart": 2
            },
            "ledger": {
                "path": ".dev/reports/p7-operation-restart-ledger-latest.json",
                "records": 1,
                "proposal_records": 1,
                "approval_records": 1,
                "published_execution_records": 1,
                "completed_observation_records": 1
            },
            "responses": {
                "proposal": {"assignments_changed": false, "operation_ids": ["p7-merge-1"]},
                "approval": {"status": "approved", "assignments_changed": false},
                "execution": {"status": "published", "assignments_changed": true},
                "observation": {
                    "status": "completed",
                    "assignments_changed": false,
                    "observation_accepted": true
                }
            },
            "checks": {
                "merge_execution_published": true,
                "assignment_state_persisted": true,
                "orchestrator_restarted": true,
                "ledger_execution_survived_restart": true,
                "route_converged_after_restart": true,
                "worker_refreshed_after_restart": true,
                "traffic_confirmed_after_restart": true,
                "gateway_close_counters_clean": true,
                "stable_session_parent_move_after_restart": true,
                "observation_completed_after_restart": true,
                "ledger_observation_completed": true
            },
            "frames": {
                "ignored_before_parent_delta": 0,
                "remote_delta_before_parent_delta": 0,
                "remote_snapshot_before_parent_delta": 0
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_restart_smoke"
            ]
        });

        validate_p7_operation_restart_smoke_report(&report)
            .expect("valid P7 operation restart smoke report");
    }

    #[test]
    fn p7_operation_soak_smoke_report_accepts_soak_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.p7_operation_soak_smoke.v1",
            "unix_secs": 160,
            "operation": {
                "operation_id": "p7-merge-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:abc",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "soak": {
                "iterations": 16,
                "sleep_ms": 10,
                "pings_ok": 64,
                "moves_ok": 64,
                "expected_actor_requests": 64
            },
            "orchestrator": {
                "grpc_addr": "127.0.0.1:6360",
                "metrics_addr": "127.0.0.1:6361",
                "registered_workers": 2,
                "assignment_listing_before": {"workers": [], "handovers": 0},
                "assignment_listing_after": {"workers": [], "handovers": 0}
            },
            "gateway": {
                "addr": "127.0.0.1:4360",
                "metrics_addr": "127.0.0.1:4361",
                "routes_before": 4,
                "routes_after_soak": 1,
                "ping_roundtrips": 64,
                "join_roundtrips": 4,
                "move_roundtrips": 64,
                "close_counters": {
                    "before": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0},
                    "after_soak": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0}
                }
            },
            "worker": {
                "worker_a_addr": "127.0.0.1:5361",
                "worker_a_metrics_addr": "127.0.0.1:5363",
                "worker_b_addr": "127.0.0.1:5362",
                "worker_b_metrics_addr": "127.0.0.1:5364",
                "parent_actor_count_after_soak": 1,
                "worker_a_accepted_connections": 4
            },
            "ledger": {
                "path": ".dev/reports/p7-operation-soak-ledger-latest.json",
                "records": 1,
                "proposal_records": 1,
                "approval_records": 1,
                "published_execution_records": 1,
                "completed_observation_records": 1
            },
            "responses": {
                "proposal": {"assignments_changed": false, "operation_ids": ["p7-merge-1"]},
                "approval": {"status": "approved", "assignments_changed": false},
                "execution": {"status": "published", "assignments_changed": true},
                "observation": {
                    "status": "completed",
                    "assignments_changed": false,
                    "observation_accepted": true
                }
            },
            "checks": {
                "merge_execution_published": true,
                "route_converged_after_soak": true,
                "worker_refreshed_after_soak": true,
                "traffic_confirmed_after_soak": true,
                "gateway_close_counters_clean": true,
                "observation_completed_after_soak": true,
                "ledger_observation_completed": true
            },
            "frames": {
                "ignored_frames": 0,
                "remote_delta_frames": 0,
                "remote_snapshot_frames": 0
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_soak_smoke"
            ]
        });

        validate_p7_operation_soak_smoke_report(&report, 16)
            .expect("valid P7 operation soak smoke report");
    }

    #[test]
    fn internal_k8s_p7_operation_report_check_accepts_completed_soak_evidence() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-merge-1",
                    "kind": "merge",
                    "status": "completed",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 130,
                    "proposal": {
                        "source": "split_merge_preview:test",
                        "proposal_hash": "fnv1a64:abc",
                        "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0}, "worker_id": "worker-a"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt merge-activation --operation-id p7-merge-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "merge",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:abc",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"},
                        {"name": "approval_recorded", "state": "succeeded", "unix_secs": 110, "reason": "approval persisted"},
                        {"name": "execution_started", "state": "succeeded", "unix_secs": 120, "reason": "operation execution passed policy gates"},
                        {"name": "execution_published", "state": "succeeded", "unix_secs": 121, "reason": "merge activation published"},
                        {"name": "observation_completed", "state": "succeeded", "unix_secs": 130, "reason": "observer=internal confirmed route_converged, worker_refreshed, traffic_confirmed, counters_clean"}
                    ]
                }
            ]
        });
        let report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_p7_operation_smoke.v1",
            "unix_ts": 130,
            "stage": "completed",
            "operation_mode": "policy_gated_manual",
            "execution_mutated": true,
            "execution_allowed": true,
            "reason": "complete",
            "preflight_errors": [],
            "cluster": {
                "context": "example-cluster",
                "namespace": "tessera",
                "orchestrator_service": "tessera-orch",
                "gateway_service": "tessera-gateway",
                "owner_worker_deployment": "tessera-worker",
                "owner_worker_service": "tessera-worker",
                "owner_worker_id": "worker-a",
                "argocd": {"checked": true, "sync": "Synced", "health": "Healthy"},
                "expected_image": "repo/tessera:v1",
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "owner_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"}
                ]
            },
            "operation": {
                "operation_id": "p7-merge-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:abc",
                "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
                "owner_worker_id": "worker-a",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "orchestrator": {
                "grpc_addr": "127.0.0.1:6000",
                "metrics_addr": "127.0.0.1:6100",
                "registered_workers_before": 2,
                "registered_workers_after": 2,
                "assignment_listing_before": {"workers": [], "handovers": 0},
                "assignment_listing_after": {"workers": [], "handovers": 0}
            },
            "gateway": {
                "addr": "127.0.0.1:4000",
                "metrics_addr": "127.0.0.1:4100",
                "routes_before": 4,
                "routes_after": 1,
                "ping_roundtrips": 64,
                "join_roundtrips": 4,
                "move_roundtrips": 64,
                "close_counters": {
                    "before": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0},
                    "after": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0}
                }
            },
            "worker": {
                "worker_id": "worker-a",
                "metrics_addr": "127.0.0.1:5100",
                "parent_actor_count": 1
            },
            "soak": {
                "ran": true,
                "iterations": 16,
                "sleep_ms": 10,
                "pings_ok": 64,
                "moves_ok": 64,
                "expected_actor_requests": 64
            },
            "ledger": {
                "summary": {
                    "records": 1,
                    "proposal_records": 1,
                    "approval_records": 1,
                    "blocked_execution_records": 0,
                    "published_execution_records": 1,
                    "completed_observation_records": 1,
                    "recovery_required_records": 0
                },
                "snapshot": ledger
            },
            "responses": {
                "proposal": {"assignments_changed": false, "operation_ids": ["p7-merge-1"]},
                "approval": {"status": "approved", "assignments_changed": false},
                "execution": {
                    "status": "published",
                    "assignments_changed": true,
                    "mutation_attempted": true,
                    "mutation_allowed": true
                },
                "observation": {
                    "status": "completed",
                    "assignments_changed": false,
                    "observation_accepted": true
                }
            },
            "checks": {
                "operation_recorded": true,
                "approval_recorded": true,
                "execution_published": true,
                "route_converged": true,
                "worker_refreshed": true,
                "traffic_confirmed": true,
                "gateway_close_counters_clean": true,
                "observation_completed": true,
                "load_soak_ran": true,
                "automatic_rollback_observed": false
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_failure_recovery_smoke",
                "guarded_kubernetes_operation_restart_smoke",
                "p7_completion_audit"
            ]
        });

        validate_internal_k8s_operation_report(
            &report,
            InternalP7OperationRequirements {
                require_published_execution: true,
                require_completed_observation: true,
                require_soak: true,
                require_recovery_required: false,
                require_restart: false,
            },
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("valid internal P7 operation smoke report");
    }

    #[test]
    fn internal_k8s_p7_operation_report_check_accepts_recovery_required_evidence() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-merge-recovery-1",
                    "kind": "merge",
                    "status": "recovery_required",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 140,
                    "proposal": {
                        "source": "split_merge_preview:test",
                        "proposal_hash": "fnv1a64:def",
                        "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0}, "worker_id": "worker-a"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt merge-activation --operation-id p7-merge-recovery-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "merge",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:def",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"},
                        {"name": "approval_recorded", "state": "succeeded", "unix_secs": 110, "reason": "approval persisted"},
                        {"name": "execution_started", "state": "succeeded", "unix_secs": 120, "reason": "operation execution passed policy gates"},
                        {"name": "execution_published", "state": "succeeded", "unix_secs": 121, "reason": "merge activation published"},
                        {"name": "observation_failed", "state": "failed", "unix_secs": 140, "reason": "observer=internal reported traffic_confirmed=false counters_clean=false"}
                    ]
                }
            ]
        });
        let report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_p7_operation_smoke.v1",
            "unix_ts": 140,
            "stage": "recovery_required",
            "operation_mode": "policy_gated_manual",
            "execution_mutated": true,
            "execution_allowed": true,
            "reason": "owner outage",
            "preflight_errors": [],
            "cluster": {
                "context": "example-cluster",
                "namespace": "tessera",
                "orchestrator_service": "tessera-orch",
                "gateway_service": "tessera-gateway",
                "owner_worker_deployment": "tessera-worker",
                "owner_worker_service": "tessera-worker",
                "owner_worker_id": "worker-a",
                "argocd": {"checked": true, "sync": "Synced", "health": "Healthy"},
                "expected_image": "repo/tessera:v1",
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "owner_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"}
                ]
            },
            "operation": {
                "operation_id": "p7-merge-recovery-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:def",
                "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
                "owner_worker_id": "worker-a",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "orchestrator": {
                "grpc_addr": "127.0.0.1:6000",
                "metrics_addr": "127.0.0.1:6100",
                "registered_workers_before": 2,
                "registered_workers_after": 2,
                "assignment_listing_before": {"workers": [], "handovers": 0},
                "assignment_listing_after": {"workers": [], "handovers": 0}
            },
            "gateway": {
                "addr": "127.0.0.1:4000",
                "metrics_addr": "127.0.0.1:4100",
                "routes_before": 4,
                "routes_after": 1,
                "ping_roundtrips": 1,
                "join_roundtrips": 3,
                "move_roundtrips": 1,
                "close_counters": {
                    "before": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0},
                    "after": {"no_route": 0, "upstream_retry_exhausted": 0, "ambiguous_upstream": 0}
                }
            },
            "worker": {
                "worker_id": "worker-a",
                "metrics_addr": "127.0.0.1:5100",
                "parent_actor_count": 1
            },
            "soak": {"ran": false},
            "failure": {
                "ran": true,
                "owner_worker_deployment": "tessera-worker",
                "original_replicas": 1,
                "failure_error": "connection refused while owner worker was down",
                "routes_after_failure": 1,
                "routes_after_recovery": 1,
                "close_counters": {
                    "after_failure": {"no_route": 0, "upstream_retry_exhausted": 1, "ambiguous_upstream": 0}
                }
            },
            "ledger": {
                "summary": {
                    "records": 1,
                    "proposal_records": 1,
                    "approval_records": 1,
                    "blocked_execution_records": 0,
                    "published_execution_records": 1,
                    "completed_observation_records": 0,
                    "recovery_required_records": 1
                },
                "snapshot": ledger
            },
            "responses": {
                "proposal": {"assignments_changed": false, "operation_ids": ["p7-merge-recovery-1"]},
                "approval": {"status": "approved", "assignments_changed": false},
                "execution": {
                    "status": "published",
                    "assignments_changed": true,
                    "mutation_attempted": true,
                    "mutation_allowed": true
                },
                "observation": {
                    "status": "recovery_required",
                    "assignments_changed": false,
                    "observation_accepted": false
                }
            },
            "checks": {
                "operation_recorded": true,
                "approval_recorded": true,
                "execution_published": true,
                "route_converged": true,
                "worker_refreshed": true,
                "traffic_confirmed": false,
                "gateway_close_counters_clean": false,
                "observation_completed": false,
                "owner_outage_detected": true,
                "observation_recovery_required": true,
                "operator_recovery_confirmed": true,
                "ledger_recovery_required": true,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_observation_smoke",
                "guarded_kubernetes_operation_soak_smoke",
                "guarded_kubernetes_operation_restart_smoke",
                "p7_completion_audit"
            ]
        });

        validate_internal_k8s_operation_report(
            &report,
            InternalP7OperationRequirements {
                require_published_execution: true,
                require_completed_observation: false,
                require_soak: false,
                require_recovery_required: true,
                require_restart: false,
            },
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("valid internal P7 operation recovery report");
    }

    #[test]
    fn internal_k8s_p7_operation_report_check_accepts_restart_evidence() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-merge-restart-1",
                    "kind": "merge",
                    "status": "completed",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 150,
                    "proposal": {
                        "source": "split_merge_preview:test",
                        "proposal_hash": "fnv1a64:restart",
                        "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0}, "worker_id": "worker-a"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt merge-activation --operation-id p7-merge-restart-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "merge",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:restart",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"},
                        {"name": "approval_recorded", "state": "succeeded", "unix_secs": 110, "reason": "approval persisted"},
                        {"name": "execution_started", "state": "succeeded", "unix_secs": 120, "reason": "operation execution passed policy gates"},
                        {"name": "execution_published", "state": "succeeded", "unix_secs": 121, "reason": "merge activation published"},
                        {"name": "observation_completed", "state": "succeeded", "unix_secs": 150, "reason": "observer=internal confirmed route_converged, worker_refreshed, traffic_confirmed, counters_clean"}
                    ]
                }
            ]
        });
        let report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_p7_operation_smoke.v1",
            "unix_ts": 150,
            "stage": "completed",
            "operation_mode": "policy_gated_manual",
            "execution_mutated": true,
            "execution_allowed": true,
            "reason": "restart recovered",
            "preflight_errors": [],
            "cluster": {
                "context": "example-cluster",
                "namespace": "tessera",
                "orchestrator_service": "tessera-orch",
                "gateway_service": "tessera-gateway",
                "owner_worker_deployment": "tessera-worker",
                "owner_worker_service": "tessera-worker",
                "owner_worker_id": "worker-a",
                "argocd": {"checked": true, "sync": "Synced", "health": "Healthy"},
                "expected_image": "repo/tessera:v1",
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "owner_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"}
                ],
                "assignment_state_storage": {
                    "checked": true,
                    "policy_id": "orchestrator_assignment_state_pvc_rwo_v1",
                    "env": "TESSERA_ORCH_ASSIGNMENT_STATE_PATH",
                    "path": "/var/lib/tessera/assignment-state-p7-operation-restart-20260504.json",
                    "mount_path": "/var/lib/tessera",
                    "volume": "orch-state",
                    "persistent_volume_claim": "tessera-orch-state"
                }
            },
            "operation": {
                "operation_id": "p7-merge-restart-1",
                "kind": "merge",
                "proposal_hash": "fnv1a64:restart",
                "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
                "owner_worker_id": "worker-a",
                "policy_id": "operator_approved_dynamic_operation_v1"
            },
            "ledger": {
                "summary": {
                    "records": 1,
                    "proposal_records": 1,
                    "approval_records": 1,
                    "blocked_execution_records": 0,
                    "published_execution_records": 1,
                    "completed_observation_records": 1,
                    "recovery_required_records": 0
                },
                "snapshot": ledger
            },
            "responses": {
                "proposal": {"assignments_changed": false, "operation_ids": ["p7-merge-restart-1"]},
                "approval": {"status": "approved", "assignments_changed": false},
                "execution": {"status": "published", "assignments_changed": true, "mutation_attempted": true, "mutation_allowed": true},
                "observation": {"status": "completed", "assignments_changed": false, "observation_accepted": true}
            },
            "checks": {
                "operation_recorded": true,
                "approval_recorded": true,
                "execution_published": true,
                "route_converged": true,
                "worker_refreshed": true,
                "traffic_confirmed": true,
                "gateway_close_counters_clean": true,
                "observation_completed": true,
                "owner_outage_detected": false,
                "observation_recovery_required": false,
                "operator_recovery_confirmed": false,
                "ledger_recovery_required": false,
                "orchestrator_restart_smoke_ran": true,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "remaining_uncovered": [
                "guarded_kubernetes_operation_soak_smoke",
                "guarded_kubernetes_operation_failure_recovery_smoke",
                "p7_completion_audit"
            ]
        });

        validate_internal_k8s_operation_report(
            &report,
            InternalP7OperationRequirements {
                require_published_execution: true,
                require_completed_observation: false,
                require_soak: false,
                require_recovery_required: false,
                require_restart: true,
            },
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("valid internal P7 operation restart report");
    }

    #[test]
    fn p7_operation_ledger_check_rejects_missing_blocked_execution() {
        let ledger = serde_json::json!({
            "schema": "tessera.orch.operation_ledger.v1",
            "records": [
                {
                    "operation_id": "p7-split-1",
                    "kind": "split",
                    "status": "approved",
                    "created_unix_secs": 100,
                    "updated_unix_secs": 110,
                    "proposal": {
                        "source": "live_worker_metrics:test",
                        "proposal_hash": "fnv1a64:abc",
                        "parent": {"world": 0, "cx": 0, "cy": 0},
                        "targets": [
                            {"cell": {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 0}, "worker_id": "worker-a"}
                        ],
                        "preconditions": ["operator approval required"],
                        "submission_command": "cargo xt split-activation --operation-id p7-split-1"
                    },
                    "approval": {
                        "policy_id": "operator_approved_dynamic_operation_v1",
                        "approver": "operator",
                        "allowed_kind": "split",
                        "approved_unix_secs": 110,
                        "expires_unix_secs": 710,
                        "expected_proposal_hash": "fnv1a64:abc",
                        "cooldown_key": "world-0",
                        "budget_key": "daily"
                    },
                    "phases": [
                        {"name": "proposal_recorded", "state": "succeeded", "unix_secs": 100, "reason": "proposal persisted"}
                    ]
                }
            ]
        });

        let err = validate_p7_operation_ledger(&ledger, true, true, false, false, false)
            .expect_err("missing blocked execution should fail");

        assert!(err.to_string().contains("blocked execution"));
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
    fn active_kubernetes_pod_count_ignores_finished_and_terminating_pods() {
        let pods = serde_json::json!({
            "items": [
                {"metadata": {"name": "running"}, "status": {"phase": "Running"}},
                {"metadata": {"name": "pending"}, "status": {"phase": "Pending"}},
                {"metadata": {"name": "failed"}, "status": {"phase": "Failed"}},
                {"metadata": {"name": "succeeded"}, "status": {"phase": "Succeeded"}},
                {
                    "metadata": {
                        "name": "terminating",
                        "deletionTimestamp": "2026-05-03T13:40:00Z"
                    },
                    "status": {"phase": "Running"}
                },
                {"metadata": {"name": "unknown"}, "status": {}}
            ]
        });

        assert_eq!(
            active_kubernetes_pod_count(&pods).expect("active pod count"),
            3
        );
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

    #[test]
    fn p6_gitops_rollout_report_check_accepts_completion_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.p6_gitops_rollout.v1",
            "image": {
                "name": "repo/tessera:v2026.05.3",
                "source": "github_actions",
                "digest": "sha256:abc",
                "deployment_images_match": true
            },
            "gitops": {
                "rollout_revision": "abc123",
                "cleanup_revision": "def456",
                "approved": true
            },
            "argocd": {
                "checked": true,
                "sync": "Synced",
                "health": "Healthy"
            },
            "cluster": {
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v2026.05.3"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v2026.05.3"},
                    {"role": "source_worker", "deployment": "tessera-worker", "image": "repo/tessera:v2026.05.3"},
                    {"role": "target_worker", "deployment": "tessera-worker-b", "image": "repo/tessera:v2026.05.3"}
                ]
            },
            "cleanup": {
                "manual_activation_default_off": true,
                "preview_fixture_removed": true
            },
            "checks": {
                "image_published": true,
                "gitops_rollout_approved": true,
                "argocd_synced_healthy": true,
                "deployment_images_match": true,
                "post_smoke_default_off_cleanup": true
            },
            "preflight_errors": [],
            "remaining_uncovered": []
        });

        validate_p6_gitops_rollout_report(&report, Some("repo/tessera:v2026.05.3"))
            .expect("valid P6 rollout evidence should pass");

        let mut stale = report;
        stale["image"]["name"] = serde_json::json!("repo/tessera:v2026.05.2");
        let err = validate_p6_gitops_rollout_report(&stale, None)
            .expect_err("P5 image must not satisfy P6 rollout evidence");
        assert!(err.to_string().contains("P5 image tag"));
    }

    #[test]
    fn p6_gitops_rollout_report_writer_records_cluster_image_evidence() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "tessera-p6-rollout-report-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        let status = ArgoCdAppStatus {
            sync: "Synced".to_string(),
            health: "Healthy".to_string(),
        };
        let images = vec![
            K8sDeploymentImage {
                role: "orchestrator",
                deployment: "tessera-orch".to_string(),
                image: "repo/tessera:v2026.05.3".to_string(),
            },
            K8sDeploymentImage {
                role: "gateway",
                deployment: "tessera-gateway".to_string(),
                image: "repo/tessera:v2026.05.3".to_string(),
            },
            K8sDeploymentImage {
                role: "source_worker",
                deployment: "tessera-worker".to_string(),
                image: "repo/tessera:v2026.05.3".to_string(),
            },
            K8sDeploymentImage {
                role: "target_worker",
                deployment: "tessera-worker-b".to_string(),
                image: "repo/tessera:v2026.05.3".to_string(),
            },
        ];

        write_p6_gitops_rollout_report(
            P6RolloutReportInput {
                context: "example-cluster",
                namespace: "tessera",
                image: "repo/tessera:v2026.05.3",
                argocd_namespace: "argocd",
                argocd_app: "tessera",
                argocd_status: Some(&status),
                deployment_images: &images,
                preflight_errors: &[],
                rollout_revision: Some("abc123"),
                cleanup_revision: Some("def456"),
                image_published: true,
                gitops_rollout_approved: true,
                post_smoke_default_off_cleanup: true,
                manual_activation_default_off: true,
                preview_fixture_removed: true,
            },
            Some(&path),
        )
        .expect("write rollout report");

        let report = read_json_report(&path).expect("read rollout report");
        validate_p6_gitops_rollout_report(&report, Some("repo/tessera:v2026.05.3"))
            .expect("writer output should satisfy rollout verifier");
    }

    fn complete_internal_k8s_failure_report() -> serde_json::Value {
        serde_json::json!({
            "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
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
                "merge_activation": P5_ROLLBACK_MERGE_ACTIVATION
            },
            "remaining_uncovered": []
        })
    }

    #[test]
    fn activation_report_check_accepts_complete_failure_evidence() {
        let report = complete_internal_k8s_failure_report();
        validate_internal_k8s_activation_report(
            &report,
            true,
            true,
            false,
            false,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("complete failure evidence should pass");
    }

    #[test]
    fn internal_k8s_merge_activation_report_check_accepts_ready_plan_evidence() {
        let mut report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_merge_activation_smoke.v1",
            "stage": "planned_without_activation",
            "activation_mutated": false,
            "activation_allowed": false,
            "preflight_errors": [],
            "cluster": {
                "argocd": {
                    "checked": true,
                    "sync": "Synced",
                    "health": "Healthy"
                },
                "expected_image": "repo/tessera:v1",
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "owner_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"}
                ]
            },
            "plan": {
                "status": "ready",
                "activation_mutated": false,
                "recommendation": {
                    "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
                    "owner_worker_id": "worker-a",
                    "siblings": [
                        activation_child_cell(0),
                        activation_child_cell(1),
                        activation_child_cell(2),
                        activation_child_cell(3)
                    ],
                    "submission_command": "cargo xt merge-activation --orch-addr http://127.0.0.1:6000 --operation-id internal-merge-test --owner-worker-id worker-a"
                }
            },
            "checks": {
                "merge_plan_ready": true,
                "merge_published": false,
                "post_publish_failure_smoke_ran": false,
                "owner_worker_restart_recovered_convergence": false,
                "orchestrator_restart_smoke_ran": false,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_publish",
                "guarded_kubernetes_merge_failure_recovery_smoke",
                "guarded_kubernetes_merge_restart_recovery_smoke",
                "guarded_kubernetes_merge_load_soak"
            ]
        });

        validate_internal_k8s_merge_activation_report(&report, true, Some("repo/tessera:v1"), &[])
            .expect("ready internal merge plan evidence should pass");

        report["stage"] = serde_json::json!("published");
        report["activation_mutated"] = serde_json::json!(true);
        report["activation_allowed"] = serde_json::json!(true);
        report["checks"]["merge_published"] = serde_json::json!(true);
        report["checks"]["post_publish_failure_smoke_ran"] = serde_json::json!(true);
        report["checks"]["owner_worker_restart_recovered_convergence"] = serde_json::json!(true);
        report["checks"]["orchestrator_restart_smoke_ran"] = serde_json::json!(true);
        report["checks"]["load_soak_ran"] = serde_json::json!(true);
        report["remaining_uncovered"] = serde_json::json!([]);
        validate_internal_k8s_merge_activation_report(&report, true, Some("repo/tessera:v1"), &[])
            .expect("published internal merge evidence should pass base report check");
        validate_internal_k8s_merge_completion_gates(&report, true, true, true, true)
            .expect("published internal merge completion evidence should pass");
    }

    #[test]
    fn internal_k8s_merge_activation_report_check_matches_preflight_errors() {
        let report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_merge_activation_smoke.v1",
            "stage": "blocked_before_plan",
            "activation_mutated": false,
            "activation_allowed": false,
            "preflight_errors": ["ArgoCD application status preflight failed: not found"],
            "cluster": {
                "argocd": {
                    "checked": false,
                    "sync": null,
                    "health": null
                },
                "expected_image": null,
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "owner_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"}
                ]
            },
            "plan": {
                "status": "not_run",
                "activation_mutated": false
            },
            "checks": {
                "merge_plan_ready": false,
                "merge_published": false,
                "post_publish_failure_smoke_ran": false,
                "owner_worker_restart_recovered_convergence": false,
                "orchestrator_restart_smoke_ran": false,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_publish",
                "guarded_kubernetes_merge_ready_plan"
            ]
        });

        validate_internal_k8s_merge_activation_report(
            &report,
            false,
            None,
            &["ArgoCD application".to_string()],
        )
        .expect("preflight error matching should pass");
    }

    #[test]
    fn internal_k8s_multi_depth_activation_report_check_accepts_ready_plan_evidence() {
        let parent = multi_depth_activation_parent();
        let children = parent
            .canonical_children()
            .expect("canonical parent children");
        let mut report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_multi_depth_activation_smoke.v1",
            "stage": "planned_without_activation",
            "activation_mutated": false,
            "activation_allowed": false,
            "preflight_errors": [],
            "cluster": {
                "argocd": {
                    "checked": true,
                    "sync": "Synced",
                    "health": "Healthy"
                },
                "expected_image": "repo/tessera:v1",
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
                "parent": parent,
                "source_worker_id": "worker-a",
                "targets": [
                    {"cell": children[0], "worker_id": "worker-a"},
                    {"cell": children[1], "worker_id": "worker-b"},
                    {"cell": children[2], "worker_id": "worker-a"},
                    {"cell": children[3], "worker_id": "worker-b"}
                ],
                "submission_command": "cargo xt split-activation --orch-addr 127.0.0.1:6000 --operation-id internal-multi-depth-test --depth 2 --sub 0 --target-cell 0,-4,6,3,0=worker-a"
            },
            "checks": {
                "multi_depth_plan_ready": true,
                "multi_depth_published": false,
                "post_publish_failure_smoke_ran": false,
                "target_worker_restart_recovered_convergence": false,
                "orchestrator_restart_smoke_ran": false,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "rollback_policy": p5_rollback_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_multi_depth_activation_publish",
                "guarded_kubernetes_multi_depth_failure_recovery_smoke",
                "guarded_kubernetes_multi_depth_restart_recovery_smoke",
                "guarded_kubernetes_multi_depth_load_soak"
            ]
        });

        validate_internal_k8s_multi_depth_activation_report(
            &report,
            true,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("ready internal multi-depth plan evidence should pass");

        report["stage"] = serde_json::json!("published");
        report["activation_mutated"] = serde_json::json!(true);
        report["activation_allowed"] = serde_json::json!(true);
        report["checks"]["multi_depth_published"] = serde_json::json!(true);
        report["checks"]["post_publish_failure_smoke_ran"] = serde_json::json!(true);
        report["checks"]["target_worker_restart_recovered_convergence"] = serde_json::json!(true);
        report["checks"]["orchestrator_restart_smoke_ran"] = serde_json::json!(true);
        report["checks"]["load_soak_ran"] = serde_json::json!(true);
        report["remaining_uncovered"] = serde_json::json!([]);
        validate_internal_k8s_multi_depth_activation_report(
            &report,
            true,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("published internal multi-depth evidence should pass base report check");
        validate_internal_k8s_multi_depth_completion_gates(&report, true, true, true, true)
            .expect("published internal multi-depth completion evidence should pass");
    }

    #[test]
    fn internal_k8s_multi_depth_activation_report_check_matches_preflight_errors() {
        let report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_multi_depth_activation_smoke.v1",
            "stage": "blocked_before_plan",
            "activation_mutated": false,
            "activation_allowed": false,
            "preflight_errors": ["target_worker=tessera-worker-b: not found"],
            "cluster": {
                "argocd": {
                    "checked": true,
                    "sync": "Synced",
                    "health": "Healthy"
                },
                "expected_image": null,
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "source_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"},
                    {"role": "target_worker", "deployment": "tessera-worker-b", "image": "repo/tessera:v1"}
                ]
            },
            "plan": {
                "status": "not_run",
                "activation_mutated": false
            },
            "checks": {
                "multi_depth_plan_ready": false,
                "multi_depth_published": false,
                "post_publish_failure_smoke_ran": false,
                "target_worker_restart_recovered_convergence": false,
                "orchestrator_restart_smoke_ran": false,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "rollback_policy": p5_rollback_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_multi_depth_activation_publish",
                "guarded_kubernetes_multi_depth_ready_plan"
            ]
        });

        validate_internal_k8s_multi_depth_activation_report(
            &report,
            false,
            None,
            &["tessera-worker-b".to_string()],
        )
        .expect("preflight error matching should pass");
    }

    #[test]
    fn p6_completion_audit_normalizes_historical_p5_split_baseline() {
        let mut report = complete_internal_k8s_failure_report();
        report["rollback_policy"]["merge_activation"] =
            serde_json::json!(P5_HISTORICAL_ROLLBACK_MERGE_ACTIVATION);

        let normalized = normalize_historical_p5_split_report_for_audit(&report);

        assert_eq!(
            json_str(&normalized, &["rollback_policy", "merge_activation"])
                .expect("normalized merge activation policy"),
            P5_ROLLBACK_MERGE_ACTIVATION
        );
        validate_internal_k8s_activation_report(&normalized, true, true, false, false, None, &[])
            .expect("historical P5 split baseline remains valid for P6 audit");
    }

    #[test]
    fn p6_completion_audit_reports_missing_internal_gates() {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "tessera-p6-audit-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("create temp report dir");

        let split = complete_internal_k8s_failure_report();
        fs::write(
            dir.join("guarded-kubernetes-activation-smoke-latest.json"),
            serde_json::to_string_pretty(&split).expect("serialize split report"),
        )
        .expect("write split report");

        let restart_preflight = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
            "stage": "blocked_before_plan",
            "activation_mutated": false,
            "preflight_errors": [
                "orchestrator deployment is missing env TESSERA_ORCH_ASSIGNMENT_STATE_PATH"
            ],
            "checks": {
                "assignment_state_storage_configured": false
            }
        });
        fs::write(
            dir.join("guarded-kubernetes-restart-readiness-preflight.json"),
            serde_json::to_string_pretty(&restart_preflight)
                .expect("serialize restart preflight report"),
        )
        .expect("write restart preflight report");

        let live_metrics_readiness = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
            "stage": "blocked_before_activation",
            "activation_mutated": false,
            "preflight_errors": [],
            "plan": {
                "activation_mutated": false,
                "preview": {
                    "source": "live_worker_metrics:worker-a=127.0.0.1:5603",
                    "plan_count": 0
                },
                "status": "no_split_candidate"
            }
        });
        fs::write(
            dir.join("guarded-kubernetes-live-metrics-readiness-negative.json"),
            serde_json::to_string_pretty(&live_metrics_readiness)
                .expect("serialize live metrics readiness report"),
        )
        .expect("write live metrics readiness report");

        let merge = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_merge_activation_smoke.v1",
            "stage": "blocked_before_activation",
            "activation_mutated": false,
            "activation_allowed": false,
            "preflight_errors": [],
            "cluster": {
                "argocd": {"checked": true, "sync": "Synced", "health": "Healthy"},
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "owner_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"}
                ]
            },
            "plan": {"status": "no_merge_candidate", "activation_mutated": false},
            "checks": {
                "merge_plan_ready": false,
                "merge_published": false,
                "post_publish_failure_smoke_ran": false,
                "owner_worker_restart_recovered_convergence": false,
                "orchestrator_restart_smoke_ran": false,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": ["guarded_kubernetes_merge_ready_plan"]
        });
        fs::write(
            dir.join("guarded-kubernetes-merge-activation-smoke-latest.json"),
            serde_json::to_string_pretty(&merge).expect("serialize merge report"),
        )
        .expect("write merge report");

        let parent = multi_depth_activation_parent();
        let multi_depth = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_multi_depth_activation_smoke.v1",
            "stage": "blocked_before_activation",
            "activation_mutated": false,
            "activation_allowed": false,
            "preflight_errors": [],
            "cluster": {
                "argocd": {"checked": true, "sync": "Synced", "health": "Healthy"},
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "source_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"},
                    {"role": "target_worker", "deployment": "tessera-worker-b", "image": "repo/tessera:v1"}
                ]
            },
            "plan": {"status": "blocked", "activation_mutated": false, "parent": parent},
            "checks": {
                "multi_depth_plan_ready": false,
                "multi_depth_published": false,
                "post_publish_failure_smoke_ran": false,
                "target_worker_restart_recovered_convergence": false,
                "orchestrator_restart_smoke_ran": false,
                "load_soak_ran": false,
                "automatic_rollback_observed": false
            },
            "rollback_policy": p5_rollback_policy_json(),
            "remaining_uncovered": ["guarded_kubernetes_multi_depth_ready_plan"]
        });
        fs::write(
            dir.join("guarded-kubernetes-multi-depth-activation-smoke-latest.json"),
            serde_json::to_string_pretty(&multi_depth).expect("serialize multi-depth report"),
        )
        .expect("write multi-depth report");

        let findings = p6_completion_findings(&dir);
        assert!(findings.iter().any(|finding| {
            finding.gate == "p6_internal_restart_recovery"
                && finding
                    .evidence
                    .contains("guarded-kubernetes-restart-readiness-preflight.json")
                && finding
                    .evidence
                    .contains("TESSERA_ORCH_ASSIGNMENT_STATE_PATH")
        }));
        assert!(findings.iter().any(|finding| {
            finding.gate == "p6_internal_live_metrics_plan"
                && finding
                    .evidence
                    .contains("guarded-kubernetes-live-metrics-readiness-negative.json")
                && finding.evidence.contains("no_split_candidate")
        }));
        assert!(
            findings
                .iter()
                .any(|finding| finding.gate == "p6_internal_merge_ready_plan")
        );
        assert!(
            findings
                .iter()
                .any(|finding| { finding.gate == "p6_internal_multi_depth_ready_plan" })
        );
        assert!(
            findings
                .iter()
                .any(|finding| finding.gate == "p6_internal_planner_mutation_policy")
        );
        assert!(
            findings
                .iter()
                .any(|finding| finding.gate == "p6_gitops_rollout_evidence")
        );

        let mut merge_bool_only = merge.clone();
        merge_bool_only["checks"]["merge_published"] = serde_json::json!(true);
        merge_bool_only["checks"]["post_publish_failure_smoke_ran"] = serde_json::json!(true);
        merge_bool_only["checks"]["orchestrator_restart_smoke_ran"] = serde_json::json!(true);
        merge_bool_only["checks"]["load_soak_ran"] = serde_json::json!(true);
        fs::write(
            dir.join("guarded-kubernetes-merge-activation-smoke-latest.json"),
            serde_json::to_string_pretty(&merge_bool_only)
                .expect("serialize bool-only merge report"),
        )
        .expect("write bool-only merge report");
        let mut multi_depth_bool_only = multi_depth.clone();
        multi_depth_bool_only["checks"]["multi_depth_published"] = serde_json::json!(true);
        multi_depth_bool_only["checks"]["post_publish_failure_smoke_ran"] = serde_json::json!(true);
        multi_depth_bool_only["checks"]["orchestrator_restart_smoke_ran"] = serde_json::json!(true);
        multi_depth_bool_only["checks"]["load_soak_ran"] = serde_json::json!(true);
        fs::write(
            dir.join("guarded-kubernetes-multi-depth-activation-smoke-latest.json"),
            serde_json::to_string_pretty(&multi_depth_bool_only)
                .expect("serialize bool-only multi-depth report"),
        )
        .expect("write bool-only multi-depth report");
        let findings = p6_completion_findings(&dir);
        assert!(
            findings
                .iter()
                .any(|finding| finding.gate == "p6_internal_merge_completion_schema")
        );
        assert!(
            findings
                .iter()
                .any(|finding| finding.gate == "p6_internal_multi_depth_completion_schema")
        );

        fs::remove_dir_all(&dir).expect("remove temp report dir");
    }

    #[test]
    fn assignment_state_storage_requires_pvc_backed_mount() {
        let deploy = serde_json::json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "env": [
                                {"name": "TESSERA_ORCH_ASSIGNMENT_STATE_PATH", "value": "/var/lib/tessera/assignment-state.json"}
                            ],
                            "volumeMounts": [
                                {"name": "orch-state", "mountPath": "/var/lib/tessera"}
                            ]
                        }],
                        "volumes": [
                            {"name": "orch-state", "persistentVolumeClaim": {"claimName": "tessera-orch-state"}}
                        ]
                    }
                }
            }
        });

        let storage = assignment_state_storage_from_deploy_json(
            &deploy,
            "/var/lib/tessera/assignment-state.json",
        )
        .expect("pvc-backed assignment state storage passes");
        assert_eq!(storage.claim_name, "tessera-orch-state");
        assert_eq!(storage.policy_id, ORCH_ASSIGNMENT_STATE_STORAGE_POLICY);

        let mut bad = deploy.clone();
        bad["spec"]["template"]["spec"]["volumes"] =
            serde_json::json!([{"name": "orch-state", "emptyDir": {}}]);
        let err = assignment_state_storage_from_deploy_json(
            &bad,
            "/var/lib/tessera/assignment-state.json",
        )
        .expect_err("emptyDir is not durable enough for the internal restart gate");
        assert!(err.to_string().contains("persistentVolumeClaim"));
    }

    #[test]
    fn worker_cell_metric_parser_reads_labeled_samples() {
        let body = r#"
# TYPE tessera_worker_cell_actor_count gauge
tessera_worker_cell_actor_count{world="0",cx="-1",cy="2",depth="1",sub="3"} 12
"#;
        let values = parse_worker_cell_metric(body, "tessera_worker_cell_actor_count")
            .expect("parse worker cell metric");
        assert_eq!(
            values.get(&CellId {
                world: 0,
                cx: -1,
                cy: 2,
                depth: 1,
                sub: 3
            }),
            Some(&12)
        );
    }

    #[test]
    fn live_worker_metrics_preview_proposes_split_without_mutation() {
        let parent = CellId::grid(0, 0, 0);
        let listing = AssignmentListing {
            workers: vec![
                AssignmentBundle {
                    worker_id: "worker-a".to_string(),
                    addr: "127.0.0.1:5301".to_string(),
                    cells: vec![Assignment {
                        world: parent.world,
                        cx: parent.cx,
                        cy: parent.cy,
                        depth: parent.depth as u32,
                        sub: parent.sub as u32,
                    }],
                },
                AssignmentBundle {
                    worker_id: "worker-b".to_string(),
                    addr: "127.0.0.1:5302".to_string(),
                    cells: vec![],
                },
            ],
            handovers: vec![],
        };
        let snapshots = vec![LiveWorkerMetricsSnapshot {
            worker_id: "worker-a".to_string(),
            addr: "127.0.0.1:5303".to_string(),
            actor_counts: std::collections::HashMap::from([(parent, 150)]),
            pending_moves: std::collections::HashMap::new(),
        }];

        let preview = build_live_metrics_split_preview(
            &listing,
            &snapshots,
            LiveMetricsPlanPolicy {
                actor_threshold: 100,
                move_threshold: 64,
                min_pressure_signals: 1,
                cell_age_secs: 60,
            },
        )
        .expect("build live metrics preview");

        assert_eq!(preview.mode, "dry_run");
        assert!(!preview.assignments_changed);
        assert!(preview.source.starts_with("live_worker_metrics:"));
        assert_eq!(preview.plans.len(), 1);
        assert_eq!(preview.plans[0].kind, "split");
        assert_eq!(preview.plans[0].cell, parent);
        assert_eq!(preview.plans[0].pressure_signals, 1);
    }

    #[test]
    fn split_activation_report_check_accepts_live_metrics_plan_report() {
        let report = serde_json::json!({
            "schema": "tessera.split_activation_plan.v1",
            "status": "ready",
            "activation_mutated": false,
            "preview": {
                "source": "live_worker_metrics:worker-a=127.0.0.1:5100,worker-b=127.0.0.1:5101",
                "assignments_changed": false,
                "plan_count": 1
            },
            "recommendation": {
                "targets": [{}, {}, {}, {}]
            },
            "rollback_policy": p5_rollback_policy_json(),
            "remaining_uncovered": ["guarded_kubernetes_activation_smoke"]
        });

        validate_split_activation_plan_report(&report).expect("valid base plan report");
        validate_split_activation_live_metrics_plan_report(&report)
            .expect("valid live metrics plan report");
    }

    #[test]
    fn merge_activation_plan_accepts_cold_sibling_family_without_mutation() {
        let parent = CellId::grid(0, 0, 0);
        let preview = SplitMergePreviewJson {
            mode: "dry_run".to_string(),
            source: "test-merge-fixture".to_string(),
            assignments_changed: false,
            plans: vec![SplitMergePreviewPlanJson {
                kind: "merge".to_string(),
                cell: parent,
                pressure_signals: 5,
                score: 5_000_000,
                required_handover_ops: 1,
                cells_moved: 4,
            }],
        };
        let health = OrchestratorHealth {
            status: "SERVING".to_string(),
            configured_workers: 2,
            registered_workers: 2,
            assigned_cells: 4,
            workers: vec![
                tessera_proto::orch::v1::WorkerHealth {
                    worker_id: "worker-a".to_string(),
                    configured_addr: "127.0.0.1:5301".to_string(),
                    runtime_addr: "127.0.0.1:5301".to_string(),
                    registered: true,
                    assigned_cells: 4,
                    last_seen_unix_secs: 1,
                    addr_matches_config: true,
                },
                tessera_proto::orch::v1::WorkerHealth {
                    worker_id: "worker-b".to_string(),
                    configured_addr: "127.0.0.1:5302".to_string(),
                    runtime_addr: "127.0.0.1:5302".to_string(),
                    registered: true,
                    assigned_cells: 0,
                    last_seen_unix_secs: 1,
                    addr_matches_config: true,
                },
            ],
        };
        let listing = AssignmentListing {
            workers: vec![
                AssignmentBundle {
                    worker_id: "worker-a".to_string(),
                    addr: "127.0.0.1:5301".to_string(),
                    cells: (0..4)
                        .map(|sub| cell_to_proto_assignment(merge_child_cell(parent, sub)))
                        .collect(),
                },
                AssignmentBundle {
                    worker_id: "worker-b".to_string(),
                    addr: "127.0.0.1:5302".to_string(),
                    cells: Vec::new(),
                },
            ],
            handovers: Vec::new(),
        };

        let plan = build_merge_activation_plan_from_parts(
            "http://127.0.0.1:6300",
            "127.0.0.1:6301",
            preview,
            health,
            listing,
            Some("merge-test".to_string()),
        )
        .expect("build merge plan");

        assert_eq!(plan.status, "ready");
        assert_eq!(plan.parent, Some(parent));
        assert_eq!(plan.owner_worker_id.as_deref(), Some("worker-a"));
        assert_eq!(plan.siblings.len(), 4);
    }

    #[test]
    fn merge_activation_plan_accepts_canonical_cold_sibling_family_without_mutation() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let preview = SplitMergePreviewJson {
            mode: "dry_run".to_string(),
            source: "test-canonical-merge-fixture".to_string(),
            assignments_changed: false,
            plans: vec![SplitMergePreviewPlanJson {
                kind: "merge".to_string(),
                cell: parent,
                pressure_signals: 5,
                score: 5_000_000,
                required_handover_ops: 1,
                cells_moved: 4,
            }],
        };
        let health = OrchestratorHealth {
            status: "SERVING".to_string(),
            configured_workers: 2,
            registered_workers: 2,
            assigned_cells: 4,
            workers: vec![
                tessera_proto::orch::v1::WorkerHealth {
                    worker_id: "worker-a".to_string(),
                    configured_addr: "127.0.0.1:5301".to_string(),
                    runtime_addr: "127.0.0.1:5301".to_string(),
                    registered: true,
                    assigned_cells: 4,
                    last_seen_unix_secs: 1,
                    addr_matches_config: true,
                },
                tessera_proto::orch::v1::WorkerHealth {
                    worker_id: "worker-b".to_string(),
                    configured_addr: "127.0.0.1:5302".to_string(),
                    runtime_addr: "127.0.0.1:5302".to_string(),
                    registered: true,
                    assigned_cells: 0,
                    last_seen_unix_secs: 1,
                    addr_matches_config: true,
                },
            ],
        };
        let listing = AssignmentListing {
            workers: vec![
                AssignmentBundle {
                    worker_id: "worker-a".to_string(),
                    addr: "127.0.0.1:5301".to_string(),
                    cells: children
                        .iter()
                        .map(|cell| cell_to_proto_assignment(*cell))
                        .collect(),
                },
                AssignmentBundle {
                    worker_id: "worker-b".to_string(),
                    addr: "127.0.0.1:5302".to_string(),
                    cells: Vec::new(),
                },
            ],
            handovers: Vec::new(),
        };

        let plan = build_merge_activation_plan_from_parts(
            "http://127.0.0.1:6300",
            "127.0.0.1:6301",
            preview,
            health,
            listing,
            Some("canonical-merge-test".to_string()),
        )
        .expect("build canonical merge plan");

        assert_eq!(plan.status, "ready");
        assert_eq!(plan.parent, Some(parent));
        assert_eq!(plan.owner_worker_id.as_deref(), Some("worker-a"));
        assert_eq!(plan.siblings, children.to_vec());
        assert!(
            plan.submission_command
                .as_deref()
                .expect("submission command")
                .contains("--depth 2")
        );
    }

    #[test]
    fn planner_activation_report_check_accepts_policy_gate_evidence() {
        let base_plan = serde_json::json!({
            "schema": "tessera.merge_activation_plan.v1",
            "status": "ready",
            "recommendation": {
                "siblings": [
                    activation_child_cell(0),
                    activation_child_cell(1),
                    activation_child_cell(2),
                    activation_child_cell(3)
                ]
            }
        });
        let blocked = serde_json::json!({
            "schema": "tessera.planner_activation.v1",
            "planner_kind": "merge",
            "status": "blocked_by_policy",
            "activation_mode": "policy_gated",
            "activation_mutated": false,
            "policy": {
                "allow_mutation": false,
                "supplied_policy_id": null,
                "required_policy_id": PLANNER_MUTATION_POLICY_ID,
                "accepted": false
            },
            "plan": base_plan,
            "response": null,
            "checks": {
                "policy_gate_default_off": true,
                "policy_id_required": true,
                "planner_selected_ready_plan": true,
                "policy_accepted": false,
                "activation_mutated": false,
                "submit_activation_published": false,
                "assignments_changed": false
            },
            "remaining_uncovered": [
                "policy_approved_planner_mutation",
                "guarded_kubernetes_planner_mutation"
            ]
        });
        validate_planner_activation_report(&blocked, false)
            .expect("valid default-off planner mutation block report");

        let published = serde_json::json!({
            "schema": "tessera.planner_activation.v1",
            "planner_kind": "merge",
            "status": "published",
            "activation_mode": "policy_gated",
            "activation_mutated": true,
            "policy": {
                "allow_mutation": true,
                "supplied_policy_id": PLANNER_MUTATION_POLICY_ID,
                "required_policy_id": PLANNER_MUTATION_POLICY_ID,
                "accepted": true
            },
            "plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "status": "ready",
                "recommendation": {
                    "siblings": [
                        activation_child_cell(0),
                        activation_child_cell(1),
                        activation_child_cell(2),
                        activation_child_cell(3)
                    ]
                }
            },
            "response": {
                "state": "published",
                "assignments_changed": true
            },
            "checks": {
                "policy_gate_default_off": true,
                "policy_id_required": true,
                "planner_selected_ready_plan": true,
                "policy_accepted": true,
                "activation_mutated": true,
                "submit_activation_published": true,
                "assignments_changed": true
            },
            "remaining_uncovered": ["guarded_kubernetes_planner_mutation"]
        });
        validate_planner_activation_report(&published, true)
            .expect("valid policy-approved planner mutation publish report");
    }

    #[test]
    fn planner_activation_report_check_accepts_live_metrics_split_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.planner_activation.v1",
            "planner_kind": "split",
            "status": "published",
            "activation_mode": "policy_gated",
            "activation_mutated": true,
            "policy": {
                "allow_mutation": true,
                "supplied_policy_id": PLANNER_MUTATION_POLICY_ID,
                "required_policy_id": PLANNER_MUTATION_POLICY_ID,
                "accepted": true
            },
            "plan": {
                "schema": "tessera.split_activation_plan.v1",
                "status": "ready",
                "preview": {
                    "source": "live_worker_metrics:worker-a=127.0.0.1:5303,worker-b=127.0.0.1:5304",
                    "plan_count": 1
                },
                "recommendation": {
                    "targets": [
                        {"sub": 0, "worker_id": "worker-a"},
                        {"sub": 1, "worker_id": "worker-b"},
                        {"sub": 2, "worker_id": "worker-a"},
                        {"sub": 3, "worker_id": "worker-b"}
                    ]
                }
            },
            "response": {
                "state": "published",
                "assignments_changed": true
            },
            "checks": {
                "policy_gate_default_off": true,
                "policy_id_required": true,
                "planner_selected_ready_plan": true,
                "policy_accepted": true,
                "activation_mutated": true,
                "submit_activation_published": true,
                "assignments_changed": true
            },
            "remaining_uncovered": ["guarded_kubernetes_planner_mutation"]
        });

        validate_planner_activation_report(&report, true)
            .expect("valid live metrics planner activation report");
        validate_planner_activation_live_metrics_report(&report)
            .expect("valid live metrics planner source");
    }

    #[test]
    fn internal_planner_activation_report_check_accepts_policy_gate_evidence() {
        let base_plan = serde_json::json!({
            "schema": "tessera.merge_activation_plan.v1",
            "status": "ready",
            "recommendation": {
                "siblings": [
                    activation_child_cell(0),
                    activation_child_cell(1),
                    activation_child_cell(2),
                    activation_child_cell(3)
                ]
            }
        });
        let blocked = serde_json::json!({
            "schema": "tessera.planner_activation.v1",
            "planner_kind": "merge",
            "status": "blocked_by_policy",
            "activation_mode": "policy_gated",
            "activation_mutated": false,
            "policy": {
                "allow_mutation": false,
                "supplied_policy_id": null,
                "required_policy_id": PLANNER_MUTATION_POLICY_ID,
                "accepted": false
            },
            "plan": base_plan,
            "response": null,
            "checks": {
                "policy_gate_default_off": true,
                "policy_id_required": true,
                "planner_selected_ready_plan": true,
                "policy_accepted": false,
                "activation_mutated": false,
                "submit_activation_published": false,
                "assignments_changed": false
            },
            "remaining_uncovered": [
                "policy_approved_planner_mutation",
                "guarded_kubernetes_planner_mutation"
            ]
        });
        let published = serde_json::json!({
            "schema": "tessera.planner_activation.v1",
            "planner_kind": "merge",
            "status": "published",
            "activation_mode": "policy_gated",
            "activation_mutated": true,
            "policy": {
                "allow_mutation": true,
                "supplied_policy_id": PLANNER_MUTATION_POLICY_ID,
                "required_policy_id": PLANNER_MUTATION_POLICY_ID,
                "accepted": true
            },
            "plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "status": "ready",
                "recommendation": {
                    "siblings": [
                        activation_child_cell(0),
                        activation_child_cell(1),
                        activation_child_cell(2),
                        activation_child_cell(3)
                    ]
                }
            },
            "response": {
                "state": "published",
                "assignments_changed": true
            },
            "checks": {
                "policy_gate_default_off": true,
                "policy_id_required": true,
                "planner_selected_ready_plan": true,
                "policy_accepted": true,
                "activation_mutated": true,
                "submit_activation_published": true,
                "assignments_changed": true
            },
            "remaining_uncovered": ["guarded_kubernetes_planner_mutation"]
        });
        let report = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_planner_activation.v1",
            "activation_mode": "policy_gated",
            "activation_mutated": true,
            "cluster": {
                "expected_image": "repo/tessera:v1",
                "argocd": {"checked": true, "sync": "Synced", "health": "Healthy"},
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"},
                    {"role": "gateway", "deployment": "tessera-gateway", "image": "repo/tessera:v1"},
                    {"role": "source_worker", "deployment": "tessera-worker", "image": "repo/tessera:v1"},
                    {"role": "target_worker", "deployment": "tessera-worker-b", "image": "repo/tessera:v1"}
                ]
            },
            "blocked_report": blocked,
            "published_report": published,
            "checks": {
                "default_off_blocked": true,
                "policy_approved_published": true,
                "activation_mutated_only_after_policy": true,
                "automatic_mutation_observed": false
            },
            "preflight_errors": [],
            "remaining_uncovered": []
        });

        validate_internal_k8s_planner_activation_report(&report, Some("repo/tessera:v1"), false)
            .expect("valid internal planner mutation report");

        let mut path = std::env::temp_dir();
        path.push(format!(
            "tessera-internal-planner-report-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        let status = ArgoCdAppStatus {
            sync: "Synced".to_string(),
            health: "Healthy".to_string(),
        };
        let images = vec![
            K8sDeploymentImage {
                role: "orchestrator",
                deployment: "tessera-orch".to_string(),
                image: "repo/tessera:v1".to_string(),
            },
            K8sDeploymentImage {
                role: "gateway",
                deployment: "tessera-gateway".to_string(),
                image: "repo/tessera:v1".to_string(),
            },
            K8sDeploymentImage {
                role: "source_worker",
                deployment: "tessera-worker".to_string(),
                image: "repo/tessera:v1".to_string(),
            },
            K8sDeploymentImage {
                role: "target_worker",
                deployment: "tessera-worker-b".to_string(),
                image: "repo/tessera:v1".to_string(),
            },
        ];
        write_internal_k8s_planner_activation_report(
            InternalK8sPlannerActivationReportInput {
                context: "example-cluster",
                namespace: "tessera",
                argocd_namespace: "argocd",
                argocd_app: "tessera",
                argocd_status: Some(&status),
                deployment_images: &images,
                expected_image: Some("repo/tessera:v1"),
                preflight_errors: &[],
                blocked_report: &report["blocked_report"],
                published_report: &report["published_report"],
            },
            Some(&path),
        )
        .expect("write internal planner report");
        let written = read_json_report(&path).expect("read internal planner report");
        validate_internal_k8s_planner_activation_report(&written, Some("repo/tessera:v1"), false)
            .expect("writer output should satisfy internal planner verifier");
    }

    #[test]
    fn activation_smoke_report_check_accepts_live_metrics_plan_link() {
        let report = serde_json::json!({
            "schema": "tessera.activation_smoke.v1",
            "operator_plan": {
                "source": "live_worker_metrics:worker-a=127.0.0.1:5100,worker-b=127.0.0.1:5101",
                "report_path": ".dev/reports/split-activation-plan-latest.json"
            },
            "checks": {
                "submit_split_activation_published": true,
                "orchestrator_listing_child_routes": 4,
                "gateway_ready_routes": 4,
                "child_ping_all_routes": true,
                "stable_session_post_split_move": true,
                "live_remote_aoi_resync_snapshot": true,
                "target_worker_relay_connections": 1
            },
            "rollback_policy": p5_rollback_policy_json(),
            "remaining_uncovered": ["guarded_kubernetes_activation_smoke"]
        });

        validate_activation_smoke_report(&report).expect("valid activation smoke report");
        validate_activation_smoke_live_metrics_plan_link(&report)
            .expect("valid live metrics activation link");
    }

    #[test]
    fn merge_activation_report_check_accepts_local_smoke_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_smoke.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
            "owner_worker_id": "worker-a",
            "merged_children": [
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 0},
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 1},
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 2},
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 3}
            ],
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes": 1,
                "parent_ping_route": true,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move": true,
                "remote_delta_frames_before_parent_delta": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_smoke_report(&report, false)
            .expect("valid merge activation smoke report");
    }

    #[test]
    fn canonical_merge_activation_report_check_accepts_local_smoke_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_smoke.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": parent,
            "owner_worker_id": "worker-a",
            "merged_children": children,
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes": 1,
                "parent_ping_route": true,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move": true,
                "remote_delta_frames_before_parent_delta": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_smoke_report(&report, false)
            .expect("valid canonical merge activation smoke report");
        validate_canonical_merge_activation_smoke_report(&report)
            .expect("valid canonical merge topology");
    }

    #[test]
    fn merge_activation_report_check_accepts_cross_worker_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_cross_worker_smoke.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "manual_cross_worker_merge_replay.v1"
            },
            "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
            "owner_worker_id": "worker-a",
            "remote_source_worker_id": "worker-b",
            "remote_child": {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 3},
            "merged_children": [
                activation_child_cell(0),
                activation_child_cell(1),
                activation_child_cell(2),
                activation_child_cell(3)
            ],
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes": 1,
                "parent_ping_route": true,
                "target_worker_coalesced_local_children": true,
                "remote_child_replayed_to_parent": true,
                "stable_session_parent_move_local_child": true,
                "stable_session_parent_move_remote_child": true,
                "worker_a_parent_actor_count": 2,
                "worker_a_relay_connections_total": 1,
                "worker_b_remote_relay_frames_sent_total": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_cross_worker_smoke_report(&report)
            .expect("valid cross-worker merge activation smoke report");
    }

    #[test]
    fn merge_activation_report_check_accepts_restart_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_restart_smoke.v1",
            "activation_mode": "manual",
            "assignment_state_path": ".dev/reports/activation-restart-assignment-state.json",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
            "owner_worker_id": "worker-a",
            "merged_children": [
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 0},
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 1},
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 2},
                {"world": 0, "cx": 0, "cy": 0, "depth": 1, "sub": 3}
            ],
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes": 1,
                "parent_ping_route": true,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move": true,
                "orchestrator_restarted": true,
                "restarted_orchestrator_loaded_parent_route": true,
                "gateway_ready_routes_after_restart": 1,
                "remote_delta_frames_before_parent_delta": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_smoke_report(&report, true)
            .expect("valid merge activation restart smoke report");
    }

    #[test]
    fn merge_activation_report_check_accepts_failure_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_failure_smoke.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
            "owner_worker_id": "worker-a",
            "merged_children": [
                activation_child_cell(0),
                activation_child_cell(1),
                activation_child_cell(2),
                activation_child_cell(3)
            ],
            "failure": {
                "owner_worker_id": "worker-a",
                "error": "upstream retry exhausted"
            },
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes_before_failure": 1,
                "parent_ping_route_before_failure": true,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move_before_failure": true,
                "owner_worker_outage_detected": true,
                "automatic_rollback_observed": false,
                "parent_assignment_stayed_published": true,
                "gateway_ready_routes_after_failure": 1,
                "owner_worker_restart_recovered_parent_route": true,
                "parent_ping_route_after_recovery": true,
                "gateway_ready_routes_after_recovery": 1,
                "remote_delta_frames_before_parent_delta": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review",
                "failure_recovery": "operator restores the owner Worker, then reruns parent route and traffic convergence checks"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_failure_smoke_report(&report)
            .expect("valid merge activation failure smoke report");
    }

    #[test]
    fn canonical_merge_activation_restart_report_check_accepts_local_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_restart_smoke.v1",
            "activation_mode": "manual",
            "assignment_state_path": ".dev/reports/activation-restart-assignment-state.json",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": parent,
            "owner_worker_id": "worker-a",
            "merged_children": children,
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes": 1,
                "parent_ping_route": true,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move": true,
                "orchestrator_restarted": true,
                "restarted_orchestrator_loaded_parent_route": true,
                "gateway_ready_routes_after_restart": 1,
                "remote_delta_frames_before_parent_delta": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_smoke_report(&report, true)
            .expect("valid canonical merge restart smoke report");
        validate_canonical_merge_activation_smoke_report(&report)
            .expect("valid canonical merge restart topology");
    }

    #[test]
    fn canonical_merge_activation_failure_report_check_accepts_local_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_failure_smoke.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": parent,
            "owner_worker_id": "worker-a",
            "merged_children": children,
            "failure": {
                "owner_worker_id": "worker-a",
                "error": "upstream retry exhausted"
            },
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes_before_failure": 1,
                "parent_ping_route_before_failure": true,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move_before_failure": true,
                "owner_worker_outage_detected": true,
                "automatic_rollback_observed": false,
                "parent_assignment_stayed_published": true,
                "gateway_ready_routes_after_failure": 1,
                "owner_worker_restart_recovered_parent_route": true,
                "parent_ping_route_after_recovery": true,
                "gateway_ready_routes_after_recovery": 1,
                "remote_delta_frames_before_parent_delta": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review",
                "failure_recovery": "operator restores the owner Worker, then reruns parent route and traffic convergence checks"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_failure_smoke_report(&report)
            .expect("valid canonical merge failure smoke report");
        validate_canonical_merge_activation_smoke_report(&report)
            .expect("valid canonical merge failure topology");
    }

    #[test]
    fn canonical_merge_activation_soak_report_check_accepts_local_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_soak.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": parent,
            "owner_worker_id": "worker-a",
            "merged_children": children,
            "traffic": {
                "iterations_per_actor": 8,
                "sleep_ms": 5,
                "actors": 4,
                "pings_ok": 32,
                "moves_ok": 32,
                "ignored_frames": 4,
                "remote_delta_frames": 0,
                "remote_snapshot_frames": 0
            },
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes_after_soak": 1,
                "parent_ping_iterations": 32,
                "parent_move_iterations": 32,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move_before_soak": true,
                "remote_delta_frames_before_parent_delta": 1,
                "gateway_ping_roundtrip_count": 32,
                "gateway_move_roundtrip_count": 32,
                "gateway_client_closes_no_route_total": 0,
                "gateway_client_closes_upstream_retry_exhausted_total": 0,
                "gateway_client_closes_ambiguous_upstream_total": 0
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_soak_report(&report, 8)
            .expect("valid canonical merge soak report");
        validate_canonical_merge_activation_smoke_report(&report)
            .expect("valid canonical merge soak topology");
    }

    #[test]
    fn merge_activation_soak_report_check_accepts_local_evidence() {
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_soak.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "parent": {"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},
            "owner_worker_id": "worker-a",
            "merged_children": [
                activation_child_cell(0),
                activation_child_cell(1),
                activation_child_cell(2),
                activation_child_cell(3)
            ],
            "traffic": {
                "iterations_per_actor": 8,
                "sleep_ms": 5,
                "actors": 4,
                "pings_ok": 32,
                "moves_ok": 32,
                "ignored_frames": 4,
                "remote_delta_frames": 0,
                "remote_snapshot_frames": 0
            },
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes_after_soak": 1,
                "parent_ping_iterations": 32,
                "parent_move_iterations": 32,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move_before_soak": true,
                "remote_delta_frames_before_parent_delta": 1,
                "gateway_ping_roundtrip_count": 32,
                "gateway_move_roundtrip_count": 32,
                "gateway_client_closes_no_route_total": 0,
                "gateway_client_closes_upstream_retry_exhausted_total": 0,
                "gateway_client_closes_ambiguous_upstream_total": 0
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": [
                "guarded_kubernetes_merge_activation_smoke",
                "cross_worker_merge_replay",
                "automatic_planner_merge_mutation"
            ]
        });

        validate_merge_activation_soak_report(&report, 8)
            .expect("valid merge activation soak report");
    }

    #[test]
    fn merge_activation_report_check_rejects_unresolved_runtime_merge_gate() {
        let report = serde_json::json!({
            "schema": "tessera.merge_activation_smoke.v1",
            "activation_mode": "manual",
            "operator_plan": {
                "schema": "tessera.merge_activation_plan.v1",
                "report_path": ".dev/reports/merge-activation-plan-latest.json"
            },
            "owner_worker_id": "worker-a",
            "merged_children": [{}, {}, {}, {}],
            "checks": {
                "submit_merge_activation_published": true,
                "orchestrator_listing_parent_route": true,
                "gateway_ready_routes": 1,
                "parent_ping_route": true,
                "worker_coalesced_child_actors": true,
                "stable_session_parent_move": true,
                "remote_delta_frames_before_parent_delta": 1
            },
            "rollback_policy": {
                "policy_id": "operator_controlled_manual_merge_v1",
                "automatic_rollback": false,
                "backout": "re-run manual split activation from the parent only after operator review"
            },
            "actor_state_recovery_policy": merge_actor_state_recovery_policy_json(),
            "remaining_uncovered": ["runtime_merge_activation"]
        });

        let err = validate_merge_activation_smoke_report(&report, false)
            .expect_err("runtime merge gate must be resolved");
        assert!(err.to_string().contains("runtime_merge_activation"));
    }

    #[test]
    fn activation_report_check_accepts_internal_restart_evidence() {
        let mut report = complete_internal_k8s_failure_report();
        report["cluster"]["assignment_state_storage"] = serde_json::json!({
            "checked": true,
            "policy_id": ORCH_ASSIGNMENT_STATE_STORAGE_POLICY,
            "env": ORCH_ASSIGNMENT_STATE_ENV,
            "path": "/var/lib/tessera/assignment-state.json",
            "mount_path": "/var/lib/tessera",
            "volume": "orch-state",
            "persistent_volume_claim": "tessera-orch-state"
        });
        report["checks"]["orchestrator_restart_smoke_ran"] = serde_json::json!(true);
        report["checks"]["assignment_state_storage_configured"] = serde_json::json!(true);
        report["checks"]["orchestrator_rollout_restarted"] = serde_json::json!(true);
        report["checks"]["restarted_orchestrator_loaded_child_routes"] = serde_json::json!(true);
        report["checks"]["worker_assignment_refresh_after_restart"] = serde_json::json!(true);
        report["checks"]["gateway_ready_routes_after_restart"] = serde_json::json!(4);
        report["checks"]["child_ping_all_routes_after_restart"] = serde_json::json!(true);
        report["checks"]["child_move_iterations_after_restart"] = serde_json::json!(16);
        report["checks"]["remote_aoi_frames_observed_after_restart"] = serde_json::json!(1);
        report["checks"]["live_remote_aoi_resync_snapshot_after_restart"] = serde_json::json!(true);
        report["restart_probe"] = serde_json::json!({
            "succeeded": [0, 1, 2, 3],
            "failures": []
        });

        validate_internal_k8s_activation_report(
            &report,
            true,
            true,
            true,
            false,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("complete restart evidence should pass");
    }

    #[test]
    fn activation_report_check_accepts_live_metrics_plan_evidence() {
        let mut report = complete_internal_k8s_failure_report();
        report["plan"]["preview"] = serde_json::json!({
            "addr": "127.0.0.1:6100",
            "mode": "dry_run",
            "source": "live_worker_metrics:worker-a=127.0.0.1:5100,worker-b=127.0.0.1:5101;actor_threshold=100;move_threshold=64;min_pressure_signals=1;cell_age_secs=60",
            "assignments_changed": false,
            "plan_count": 1
        });

        validate_internal_k8s_activation_report(
            &report,
            true,
            false,
            false,
            true,
            Some("repo/tessera:v1"),
            &[],
        )
        .expect("live metrics plan evidence should pass for published reports");

        let planned = serde_json::json!({
            "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
            "stage": "planned_without_activation",
            "activation_mutated": false,
            "plan": {
                "status": "ready",
                "activation_mutated": false,
                "preview": {
                    "source": "live_worker_metrics:worker-a=127.0.0.1:5100,worker-b=127.0.0.1:5101",
                    "plan_count": 1
                },
                "recommendation": {
                    "targets": [{}, {}, {}, {}]
                }
            }
        });
        validate_internal_k8s_activation_report(&planned, false, false, false, true, None, &[])
            .expect("live metrics plan evidence should pass for read-only reports");
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
            false,
            false,
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
            false,
            false,
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
            false,
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
            false,
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
            "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
            "stage": "blocked_before_plan",
            "activation_mutated": false,
            "cluster": {
                "expected_image": "repo/tessera:v2",
                "deployment_images": [
                    {"role": "orchestrator", "deployment": "tessera-orch", "image": "repo/tessera:v1"}
                ]
            },
            "remaining_uncovered": [
                "guarded_kubernetes_activation_publish",
                "guarded_kubernetes_failure_recovery_smoke"
            ]
        });

        let err = validate_internal_k8s_activation_report(
            &report,
            true,
            false,
            false,
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
            "schema": "tessera.guarded_kubernetes_activation_smoke.v1",
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
                "guarded_kubernetes_activation_publish",
                "guarded_kubernetes_failure_recovery_smoke"
            ]
        });

        validate_internal_k8s_activation_report(
            &report,
            false,
            false,
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
        let remaining_uncovered = ["guarded_kubernetes_activation_smoke"];
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

        let restart = serde_json::json!({
            "schema": "tessera.activation_restart_smoke.v1",
            "checks": {
                "submit_split_activation_published": true,
                "assignment_state_file_written": true,
                "orchestrator_restarted": true,
                "restarted_with_manual_activation_disabled": true,
                "restarted_orchestrator_loaded_child_routes": true,
                "worker_assignment_refresh_after_restart": true,
                "gateway_ready_routes_after_restart": 4,
                "child_ping_all_routes_after_restart": true,
                "child_move_iterations_after_restart": 16,
                "remote_aoi_frames_observed_after_restart": 1,
                "live_remote_aoi_resync_snapshot_after_restart": true
            },
            "restart_probe": {
                "succeeded": [0, 1, 2, 3],
                "failures": []
            },
            "remaining_uncovered": ["guarded_kubernetes_restart_recovery_smoke"]
        });
        validate_activation_restart_smoke_report(&restart).expect("valid restart report");
    }

    #[test]
    fn multi_depth_activation_report_check_accepts_canonical_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.multi_depth_activation_smoke.v1",
            "activation_mode": "manual",
            "parent": parent,
            "children": [
                {"cell": children[0], "worker_id": "worker-a"},
                {"cell": children[1], "worker_id": "worker-b"},
                {"cell": children[2], "worker_id": "worker-a"},
                {"cell": children[3], "worker_id": "worker-b"}
            ],
            "checks": {
                "submit_split_activation_published": true,
                "orchestrator_listing_child_routes": 4,
                "gateway_ready_routes": 4,
                "child_ping_all_routes": true,
                "stable_session_post_split_move": true,
                "live_remote_aoi_resync_snapshot": true,
                "gateway_route_change_reconnects": 1,
                "target_worker_relay_connections": 1
            },
            "remaining_uncovered": ["guarded_kubernetes_multi_depth_activation_smoke"]
        });
        validate_multi_depth_activation_smoke_report(&report)
            .expect("valid multi-depth activation report");
    }

    #[test]
    fn multi_depth_activation_failure_report_check_accepts_recovery_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.multi_depth_activation_failure_smoke.v1",
            "activation_mode": "manual",
            "parent": parent,
            "children": [
                {"cell": children[0], "worker_id": "worker-a"},
                {"cell": children[1], "worker_id": "worker-b"},
                {"cell": children[2], "worker_id": "worker-a"},
                {"cell": children[3], "worker_id": "worker-b"}
            ],
            "checks": {
                "submit_split_activation_published": true,
                "orchestrator_listing_child_routes_after_failure": 4,
                "gateway_ready_routes_after_failure": 4,
                "post_publish_target_outage_detected": true,
                "failed_child_cells": [children[1], children[3]],
                "succeeded_child_cells_during_failure": [children[0], children[2]],
                "automatic_rollback_observed": false,
                "operator_recovery_required": true,
                "target_worker_restart_recovered_convergence": true,
                "recovered_child_cells": children
            },
            "failure_probe": {
                "failures": [
                    {"cell": children[1], "worker_id": "worker-b", "error": "connection refused"},
                    {"cell": children[3], "worker_id": "worker-b", "error": "connection refused"}
                ]
            },
            "recovery_probe": {
                "failures": []
            },
            "rollback_policy": p5_rollback_policy_json(),
            "remaining_uncovered": ["guarded_kubernetes_multi_depth_activation_smoke"]
        });
        validate_multi_depth_activation_failure_smoke_report(&report)
            .expect("valid multi-depth activation failure report");
    }

    #[test]
    fn multi_depth_activation_restart_report_check_accepts_recovery_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.multi_depth_activation_restart_smoke.v1",
            "activation_mode": "manual_publish_then_default_off_restart",
            "assignment_state_path": ".dev/reports/multi-depth-activation-restart-assignment-state.json",
            "parent": parent,
            "children": [
                {"cell": children[0], "worker_id": "worker-a"},
                {"cell": children[1], "worker_id": "worker-b"},
                {"cell": children[2], "worker_id": "worker-a"},
                {"cell": children[3], "worker_id": "worker-b"}
            ],
            "checks": {
                "submit_split_activation_published": true,
                "assignment_state_file_written": true,
                "orchestrator_restarted": true,
                "restarted_with_manual_activation_disabled": true,
                "restarted_orchestrator_loaded_child_routes": true,
                "worker_assignment_refresh_after_restart": true,
                "gateway_ready_routes_after_restart": 4,
                "child_ping_all_routes_after_restart": true,
                "remote_aoi_interest_resync_after_restart": true,
                "worker_b_remote_interest_clients_after_restart": 1,
                "worker_b_remote_interest_cells_after_restart": 1,
                "recovered_child_cells_after_restart": children
            },
            "restart_probe": {
                "failures": []
            },
            "remaining_uncovered": ["guarded_kubernetes_multi_depth_activation_smoke"]
        });
        validate_multi_depth_activation_restart_smoke_report(&report)
            .expect("valid multi-depth activation restart report");
    }

    #[test]
    fn multi_depth_activation_soak_report_check_accepts_canonical_evidence() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = parent.canonical_children().expect("canonical children");
        let report = serde_json::json!({
            "schema": "tessera.multi_depth_activation_soak.v1",
            "activation_mode": "manual",
            "parent": parent,
            "children": [
                {"cell": children[0], "worker_id": "worker-a"},
                {"cell": children[1], "worker_id": "worker-b"},
                {"cell": children[2], "worker_id": "worker-a"},
                {"cell": children[3], "worker_id": "worker-b"}
            ],
            "traffic": {
                "iterations_per_child": 8,
                "sleep_ms": 5,
                "actors": 4,
                "pings_ok": 32,
                "moves_ok": 32,
                "ignored_frames": 2,
                "remote_delta_frames": 1,
                "remote_snapshot_frames": 0
            },
            "checks": {
                "submit_split_activation_published": true,
                "orchestrator_listing_child_routes": 4,
                "gateway_ready_routes_after_soak": 4,
                "child_ping_iterations": 32,
                "child_move_iterations": 32,
                "remote_aoi_frames_observed": 1,
                "gateway_ping_roundtrip_count": 32,
                "gateway_move_roundtrip_count": 32,
                "gateway_client_closes_no_route_total": 0,
                "gateway_client_closes_upstream_retry_exhausted_total": 0,
                "gateway_client_closes_ambiguous_upstream_total": 0
            },
            "rollback_policy": p5_rollback_policy_json(),
            "remaining_uncovered": ["guarded_kubernetes_multi_depth_activation_smoke"]
        });
        validate_multi_depth_activation_soak_report(&report, 8)
            .expect("valid multi-depth activation soak report");
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
    fn split_activation_cell_targets_parse_explicit_child_cells() {
        let targets = parse_split_activation_cell_targets(&[
            "0,-4,6,3,0=worker-b".to_string(),
            "0,-3,6,3,0=worker-b".to_string(),
            "0,-4,7,3,0=worker-c".to_string(),
            "0,-3,7,3,0=worker-c".to_string(),
        ])
        .expect("valid cell targets");

        assert_eq!(
            targets,
            vec![
                (CellId::leaf(0, -4, 6, 3), "worker-b".to_string()),
                (CellId::leaf(0, -3, 6, 3), "worker-b".to_string()),
                (CellId::leaf(0, -4, 7, 3), "worker-c".to_string()),
                (CellId::leaf(0, -3, 7, 3), "worker-c".to_string()),
            ]
        );

        let duplicate = parse_split_activation_cell_targets(&[
            "0,-4,6,3,0=worker-b".to_string(),
            "0,-4,6,3,0=worker-c".to_string(),
            "0,-4,7,3,0=worker-c".to_string(),
            "0,-3,7,3,0=worker-c".to_string(),
        ])
        .expect_err("duplicate cell should fail");
        assert!(duplicate.to_string().contains("duplicate target cell"));
    }

    #[test]
    fn split_activation_target_mode_rejects_mixed_or_missing_modes() {
        let legacy = vec!["0=worker-a".to_string()];
        let cells = vec!["0,-4,6,3,0=worker-b".to_string()];

        let mixed = validate_split_activation_target_mode(&legacy, &cells)
            .expect_err("mixed target modes should fail");
        assert!(
            mixed
                .to_string()
                .contains("either --target or --target-cell")
        );

        let missing = validate_split_activation_target_mode(&[], &[])
            .expect_err("missing target mode should fail");
        assert!(
            missing
                .to_string()
                .contains("requires either four --target")
        );
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
