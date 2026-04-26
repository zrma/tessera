# Tessera Quality Harness

Last verified: 2026-04-26

This document is the repo-local quality map for agents. It keeps the expected autonomy, feedback loops, and crate boundaries visible without requiring external chat history.

## Autonomy contract

- Default mode is autonomous execution: read `README.md` and `AGENTS.md`, inspect the local pattern, implement, verify, document, and report assumptions.
- Escalate to the user only for judgment-heavy requirement gaps, destructive operations, published history rewrite, unspecified remote bookmark movement, external cost or operations risk, and secrets or credentials.
- Small uncertainty should not stop the task. Capture the assumption in code, docs, tests, or the final report.

## Feedback loops

- `cargo xt` is the default local gate and runs fmt, clippy with `-D warnings`, workspace check, and `cargo xt harness`.
- `cargo test` is the default behavioral gate.
- Runtime or networking changes also need the local smoke loop: `cargo xt dev up --with-orch`, `cargo run -p tessera-client -- ping --ts 123`, and `cargo xt dev down --with-orch`.
- GitHub Actions runs the same verification and smoke loop on push and pull requests.

## Crate boundary policy

- `tessera-core` and `tessera-proto` must stay free of internal Tessera crate dependencies.
- Runtime crates (`tessera-gateway`, `tessera-worker`, `tessera-orch`) may depend on `tessera-core` and `tessera-proto`, but not on each other.
- `tessera-client` may depend on `tessera-core`, but not on runtime crates.
- `tessera-sim` may depend on `tessera-core` or `tessera-client`, but not on runtime crates unless the design overview and harness rule are updated together.

## Mechanical guardrails

- `cargo xt harness` verifies this document, README/AGENTS discoverability, CI smoke coverage, and crate dependency boundaries.
- If an intentional architecture edge fails the harness, update `README.md` Design Overview, this document, and the `xtask` rule in the same change.
- Keep implemented and planned work separate: README's implemented/planned sections are treated as the user-facing status source.

## Known gaps

- Orchestrator/Gateway/Worker have opt-in Prometheus text endpoints, Gateway has a `/ready` endpoint, and Ping/Pong round-trip latency is covered by a histogram. Long-running scrape/tracing assertions and non-Ping request latency correlation are not covered yet.
- Docker/Compose/Kubernetes sample packaging exists, but production manifests are intentionally deferred until target cluster conventions are known.
- Orchestrator has an inactive split/merge planner skeleton and dry-run preview endpoint, but runtime split/merge activation remains deferred.
- `docs/todo-next.md` is the current execution-plan index; keep README's implemented/planned sections and detailed `docs/` notes in sync when a task spans multiple changes.
