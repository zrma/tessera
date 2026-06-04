# P12 Operator Readiness And Alert Handoff

Last reviewed: 2026-06-04

## Objective

P12 turns the completed P11 endurance and failure-recovery evidence into an
operator-ready packet. The milestone should make it clear how an operator checks
Tessera health, what SLO and alert candidates are justified by current metrics,
which reports are replayable source-of-truth evidence, and which choices need a
human decision before external observability or production manifests are wired.

P12 remains read-only by default. It should not create alerting resources,
change Kubernetes manifests, restart pods, scale workloads, or enable runtime
mutation unless a later controlled slice is explicitly approved.

## Source Evidence

- Completed P11 audit: `cargo xt p11-completion-audit --json`
- Completed P12 read-only audit: `cargo xt p12-readiness-audit --json`
- Runtime image: `registry.example.com/example/tessera:v2026.05.10`
- deployment evidence: `.dev/reports/p11-gitops-rollout-latest.json`
- Internal smoke evidence:
  `.dev/reports/guarded-kubernetes-p11-endurance-recovery-latest.json`
- Local endurance evidence:
  `.dev/reports/p11-endurance-soak-latest.json`
- Local restart evidence:
  `.dev/reports/p11-restart-recovery-latest.json`
- Local transient failure evidence:
  `.dev/reports/p11-transient-failure-recovery-latest.json`
- P12 operator readiness evidence:
  `.dev/reports/p12-operator-readiness-latest.json`
- P12 source replay evidence:
  `.dev/reports/p12-source-replay-latest.json`
- P12 SLO/alert candidate evidence:
  `.dev/reports/p12-slo-alert-candidates-latest.json`
- P12 runbook drill evidence:
  `.dev/reports/p12-runbook-drill-latest.json`
- P12 decision packet evidence:
  `.dev/reports/p12-decision-packet-latest.json`

## Candidate Slices

1. **Contract and audit skeleton**: done. `cargo xt
   p12-readiness-audit --json` requires P12 report artifacts and fails closed
   until readiness evidence exists.
2. **Local evidence replay**: done. `cargo xt dev p12-local-report-replay`
   loads the P11 reports, validates them, and writes compact operator readiness
   plus source replay reports with image, deployment, deployment controller, route, assignment,
   latency, close-counter, ledger, and default-off evidence.
3. **SLO and alert candidates**: done. `cargo xt dev
   p12-slo-alert-candidates` writes a machine-readable recommendation report
   from existing Gateway latency/close-counter, Worker relay, Orchestrator
   assignment, and default-off evidence without provisioning alert resources.
4. **Runbook drill**: done. `cargo xt dev p12-runbook-drill` maps operator
   symptoms to read-only checks for Gateway readiness/routes, Gateway close
   counters, Worker relay state, Orchestrator assignment state, operation
   ledger durability, and default-off runtime flags.
5. **Internal read-only snapshot**: optional follow-up. Add a guarded `cargo xt k8s` helper that
   records deployment controller health, deployment image match, readiness, metrics,
   assignment listing, operation ledger availability, and default-off state
   without pod restarts, scale changes, or activation.
6. **Decision packet**: done. `cargo xt dev p12-decision-packet` records
   unresolved choices for alert backend, notification target, SLO thresholds,
   retention period, production manifest ownership, and whether P12 should stop
   at recommendations or wire live alerts.

## Escalation Gates

Escalate before doing any of these:

- choosing or provisioning an external alert backend
- setting production SLO thresholds that would page a human
- adding credentials, notification targets, or webhook URLs
- changing production or deployment manifests
- running pod restarts, scale changes, or controlled failure windows
- deciding report retention policy beyond the current local `.dev/reports`
  evidence convention

## Done Criteria

P12 can close only when:

1. The P12 audit maps each requirement to a concrete report and fails closed
   when any artifact is missing or stale.
2. A local operator readiness report replays P11 evidence without runtime
   mutation.
3. SLO and alert candidates are documented with source metrics and known gaps.
4. A read-only runbook drill exists and is validated by a report checker.
5. Optional internal Kubernetes snapshot evidence may extend P12 without
   changing cluster state, but is not required for the read-only audit gate.
6. Any external observability or production-manifest choices are either
   explicitly approved and implemented in a separate controlled slice, or listed
   as unresolved decisions.
