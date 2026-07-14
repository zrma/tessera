#!/usr/bin/env python3
"""Render and validate the repository-owned Tessera Helm packaging contract."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "deploy" / "helm" / "tessera"
SCALE_VALUES = CHART / "ci" / "scale-out-values.yaml"
COMPOSE = ROOT / "deploy" / "docker-compose.yml"
KUBERNETES_SAMPLE = ROOT / "deploy" / "kubernetes" / "tessera-sample.yaml"


class PackagingCheckError(RuntimeError):
    pass


@dataclass(frozen=True)
class RenderCase:
    name: str
    release: str
    namespace: str
    worker_names: tuple[str, ...]
    worker_ids: tuple[str, ...]
    worker_cells: tuple[tuple[dict[str, int], ...], ...]
    gateway_replicas: int
    persistence: bool
    log_format: str
    values_file: Path | None = None


CASES = (
    RenderCase(
        name="default",
        release="tessera-default",
        namespace="tessera-default",
        worker_names=("worker-a",),
        worker_ids=("worker-a",),
        worker_cells=(({"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},),),
        gateway_replicas=1,
        persistence=False,
        log_format="compact",
    ),
    RenderCase(
        name="scale-out",
        release="tessera-scale",
        namespace="tessera-scale",
        worker_names=("worker-a", "worker-b", "worker-c"),
        worker_ids=("worker-west", "worker-center", "worker-east"),
        worker_cells=(
            ({"world": 0, "cx": 0, "cy": 0, "depth": 0, "sub": 0},),
            ({"world": 0, "cx": 1, "cy": 0, "depth": 0, "sub": 0},),
            ({"world": 0, "cx": 2, "cy": 0, "depth": 0, "sub": 0},),
        ),
        gateway_replicas=2,
        persistence=True,
        log_format="json",
        values_file=SCALE_VALUES,
    ),
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise PackagingCheckError(message)


def run(command: list[str], *, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if expect_success and result.returncode != 0:
        detail = result.stderr.strip().splitlines()
        summary = detail[-1] if detail else "no diagnostic"
        raise PackagingCheckError(f"command failed ({command[0]}): {summary}")
    if not expect_success and result.returncode == 0:
        raise PackagingCheckError(f"negative check unexpectedly passed ({command[0]})")
    return result


def helm_template_command(case: RenderCase) -> list[str]:
    command = [
        "helm",
        "template",
        case.release,
        str(CHART),
        "--namespace",
        case.namespace,
    ]
    if case.values_file is not None:
        command.extend(["--values", str(case.values_file)])
    return command


def split_documents(rendered: str) -> list[str]:
    documents = []
    for candidate in re.split(r"(?m)^---\s*$", rendered):
        if re.search(r"(?m)^kind:\s*\S+\s*$", candidate):
            documents.append(candidate.strip() + "\n")
    return documents


def scalar(document: str, pattern: str, label: str) -> str:
    match = re.search(pattern, document, flags=re.MULTILINE)
    require(match is not None, f"missing {label}")
    assert match is not None
    value = match.group(1).strip()
    if value.startswith('"'):
        return json.loads(value)
    return value


def document_identity(document: str) -> tuple[str, str, str]:
    kind = scalar(document, r"^kind:\s*(\S+)\s*$", "kind")
    name = scalar(document, r"^  name:\s*(\S+)\s*$", f"{kind} metadata.name")
    namespace = scalar(
        document,
        r"^  namespace:\s*(\S+)\s*$",
        f"{kind}/{name} metadata.namespace",
    )
    return kind, name, namespace


def expected_objects(case: RenderCase) -> set[tuple[str, str]]:
    objects = {
        ("ConfigMap", f"{case.release}-orchestrator"),
        ("Service", f"{case.release}-gateway"),
        ("Service", f"{case.release}-orchestrator"),
        ("Deployment", f"{case.release}-gateway"),
        ("Deployment", f"{case.release}-orchestrator"),
    }
    for worker_name in case.worker_names:
        objects.add(("Service", f"{case.release}-{worker_name}"))
        objects.add(("Deployment", f"{case.release}-{worker_name}"))
    if case.persistence:
        objects.add(("PersistentVolumeClaim", f"{case.release}-orchestrator-state"))
    return objects


def parse_orchestrator_config(document: str) -> dict[str, object]:
    encoded = scalar(
        document,
        r"^  orch-config\.json:\s*(.+)$",
        "ConfigMap orch-config.json",
    )
    return json.loads(encoded)


def validate_case(case: RenderCase, rendered: str) -> None:
    documents = split_documents(rendered)
    indexed: dict[tuple[str, str], str] = {}
    for document in documents:
        kind, name, namespace = document_identity(document)
        require(namespace == case.namespace, f"{case.name}: unexpected namespace for {kind}/{name}")
        key = (kind, name)
        require(key not in indexed, f"{case.name}: duplicate rendered object {kind}/{name}")
        indexed[key] = document

    require(set(indexed) == expected_objects(case), f"{case.name}: rendered object set drifted")
    forbidden_kinds = {"Namespace", "Secret", "Ingress", "ClusterRole", "ClusterRoleBinding"}
    require(not any(kind in forbidden_kinds for kind, _ in indexed), f"{case.name}: forbidden object kind")
    forbidden_fields = ("hostNetwork:", "hostPath:", "nodeName:", "externalIPs:")
    require(not any(field in rendered for field in forbidden_fields), f"{case.name}: site-specific pod or service field")

    services = [document for (kind, _), document in indexed.items() if kind == "Service"]
    deployments = [document for (kind, _), document in indexed.items() if kind == "Deployment"]
    require(all("  type: ClusterIP\n" in document for document in services), f"{case.name}: non-ClusterIP Service")
    for document in deployments:
        require("automountServiceAccountToken: false" in document, f"{case.name}: service account token mount enabled")
        require("readinessProbe:" in document, f"{case.name}: missing readiness probe")
        require("livenessProbe:" in document, f"{case.name}: missing liveness probe")
        require(
            f'- name: TESSERA_LOG_FORMAT\n              value: "{case.log_format}"' in document,
            f"{case.name}: runtime log format drifted",
        )

    expected_secret_refs = 1 + len(case.worker_names)
    require(rendered.count("secretKeyRef:") == expected_secret_refs, f"{case.name}: runtime Secret reference count drifted")
    for variable in ("TESSERA_ORCH_SPLIT_MERGE_ACTIVATION", "TESSERA_ORCH_OPERATION_EXECUTION"):
        pattern = rf"- name: {variable}\n\s+value: \"disabled\""
        require(len(re.findall(pattern, rendered)) == 1, f"{case.name}: {variable} is not default-off")

    gateway = indexed[("Deployment", f"{case.release}-gateway")]
    require(
        re.search(rf"(?m)^  replicas: {case.gateway_replicas}$", gateway) is not None,
        f"{case.name}: Gateway replica count drifted",
    )
    require(
        f'value: "{case.release}-{case.worker_names[0]}:5001"' in gateway,
        f"{case.name}: Gateway fallback Worker address drifted",
    )

    config_document = indexed[("ConfigMap", f"{case.release}-orchestrator")]
    config = parse_orchestrator_config(config_document)
    expected_workers = []
    for worker_name, worker_id, cells in zip(
        case.worker_names,
        case.worker_ids,
        case.worker_cells,
        strict=True,
    ):
        expected_workers.append(
            {
                "id": worker_id,
                "addr": f"{case.release}-{worker_name}:5001",
                "cells": list(cells),
            }
        )
        worker_document = indexed[("Deployment", f"{case.release}-{worker_name}")]
        require(f'value: "{worker_id}"' in worker_document, f"{case.name}: Worker identity drifted")
        require(
            f'value: "{case.release}-{worker_name}:5001"' in worker_document,
            f"{case.name}: Worker advertised address drifted",
        )
    require(config == {"workers": expected_workers}, f"{case.name}: Orchestrator assignment seed drifted")

    orchestrator = indexed[("Deployment", f"{case.release}-orchestrator")]
    state_variables = ("TESSERA_ORCH_ASSIGNMENT_STATE_PATH", "TESSERA_ORCH_OPERATION_LEDGER_PATH")
    if case.persistence:
        require("type: Recreate" in orchestrator, f"{case.name}: persistent Orchestrator is not Recreate")
        require(all(variable in orchestrator for variable in state_variables), f"{case.name}: state paths missing")
        pvc = indexed[("PersistentVolumeClaim", f"{case.release}-orchestrator-state")]
        require("helm.sh/resource-policy: keep" in pvc, f"{case.name}: state claim retention missing")
    else:
        require(all(variable not in orchestrator for variable in state_variables), f"{case.name}: persistence enabled by default")


def validate_negative_cases() -> None:
    base = ["helm", "template", "tessera-negative", str(CHART)]
    run([*base, "--set", "unexpected=true"], expect_success=False)
    run([*base, "--set", "logFormat=JSON"], expect_success=False)
    run(
        [
            *base,
            "--set",
            "persistence.enabled=true",
            "--set",
            "persistence.type=existingClaim",
        ],
        expect_success=False,
    )
    run(
        [
            *base,
            "--set-json",
            'workers=[{"name":"worker-a","id":"worker-a","cells":[]},{"name":"worker-a","id":"worker-b","cells":[]}]',
        ],
        expect_success=False,
    )
    run(
        [
            *base,
            "--set-json",
            'workers=[{"name":"worker-a","id":"worker-a","cells":[{"world":0,"cx":0,"cy":0}]},{"name":"worker-b","id":"worker-b","cells":[{"world":0,"cx":0,"cy":0,"depth":0,"sub":0}]}]',
        ],
        expect_success=False,
    )


def validate_example_surfaces() -> None:
    compose = COMPOSE.read_text()
    compose_contract = "TESSERA_LOG_FORMAT: ${TESSERA_LOG_FORMAT:-compact}"
    require(
        compose.count(compose_contract) == 3,
        "Docker Compose runtime log format contract drifted",
    )

    sample = KUBERNETES_SAMPLE.read_text()
    sample_contract = "- name: TESSERA_LOG_FORMAT\n              value: compact"
    require(
        sample.count(sample_contract) == 3,
        "static Kubernetes runtime log format contract drifted",
    )


def main() -> int:
    if shutil.which("helm") is None:
        raise PackagingCheckError("helm is required; install Helm v3 before running the packaging gate")
    require(CHART.is_dir(), "Helm chart directory is missing")
    require(SCALE_VALUES.is_file(), "scale-out values fixture is missing")
    run(["helm", "lint", "--strict", str(CHART)])
    for case in CASES:
        command = helm_template_command(case)
        first = run(command).stdout
        second = run(command).stdout
        require(first == second, f"{case.name}: Helm render is not deterministic")
        validate_case(case, first)
    validate_negative_cases()
    validate_example_surfaces()
    print("k8s packaging check passed: deterministic default and scale-out renders plus example parity")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PackagingCheckError as error:
        print(f"k8s packaging check failed: {error}", file=sys.stderr)
        raise SystemExit(1)
