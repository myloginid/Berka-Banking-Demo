#!/usr/bin/env python3
"""Automate CDSW model deployment for the Berka loan default model."""
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import mlflow
from cmlapi.models.create_model_build_request import CreateModelBuildRequest
from cmlapi.models.create_model_deployment_request import CreateModelDeploymentRequest
from cmlapi.models.create_model_request import CreateModelRequest
from cmlapi.utils.default_client import default_client


def select_best_run(experiment_name: str) -> tuple[str, str, float]:
    """Return (experiment_id, run_id, best_val_acc)."""
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "cml://localhost"))
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise RuntimeError(f"Experiment '{experiment_name}' was not found.")

    runs = mlflow.search_runs(
        [experiment.experiment_id],
        order_by=["metrics.val_accuracy DESC"],
        max_results=1,
    )
    if runs.empty:
        raise RuntimeError(f"Experiment '{experiment_name}' does not contain any runs.")

    best = runs.iloc[0]
    return experiment.experiment_id, best.run_id, float(best["metrics.val_accuracy"])


def promote_checkpoint(api, project_id: str, experiment_id: str, run_id: str) -> Path:
    """Copy the tracked .pt artifact from the best run into cml/models."""
    run = api.get_experiment_run(project_id, experiment_id, run_id)
    artifact_root = Path(run.artifact_uri)
    model_files = []
    for file_info in run.data.files:
        file_path = getattr(file_info, "path", None)
        if file_path is None and isinstance(file_info, dict):
            file_path = file_info.get("path")
        is_dir = getattr(file_info, "is_dir", None)
        if is_dir is None and isinstance(file_info, dict):
            is_dir = file_info.get("is_dir")
        if file_path and not is_dir and file_path.endswith(".pt"):
            model_files.append(Path(file_path).name)
    if not model_files:
        raise RuntimeError(f"Run {run_id} has no saved .pt artifacts.")
    source = artifact_root / model_files[0]
    destination = Path("cml/models/loan_default_best.pt")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return destination


def wait_for_build(api, project_id: str, model_id: str, build_id: str, timeout: int = 900):
    start = time.time()
    final_states = {"BUILT", "BUILD_COMPLETED", "COMPLETED", "READY"}
    failed_states = {"FAILED", "BUILD_FAILED", "ERROR"}
    while True:
        build = api.get_model_build(project_id, model_id, build_id)
        status = (build.status or "").upper()
        print(f"Model build {build_id} status: {status}")
        if status in failed_states:
            raise RuntimeError(f"Model build {build_id} failed with status {status}")
        if status in final_states:
            return build
        if time.time() - start > timeout:
            raise TimeoutError(f"Timed out waiting for model build {build_id}")
        time.sleep(10)


def wait_for_deployment(api, project_id: str, model_id: str, build_id: str, deployment_id: str, timeout: int = 900):
    start = time.time()
    ready_states = {"RUNNING", "READY"}
    failed_states = {"FAILED", "STOPPED", "ERROR"}
    while True:
        deployment = api.get_model_deployment(project_id, model_id, build_id, deployment_id)
        status = (deployment.status or "").upper()
        print(f"Deployment {deployment_id} status: {status}")
        if status in failed_states:
            raise RuntimeError(f"Deployment {deployment_id} failed with status {status}")
        if status in ready_states:
            return deployment
        if time.time() - start > timeout:
            raise TimeoutError(f"Timed out waiting for deployment {deployment_id}")
        time.sleep(10)


def main() -> None:
    required_env = ["CDSW_PROJECT_ID", "CDSW_APIV2_KEY"]
    missing = [var for var in required_env if not os.environ.get(var)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    project_id = os.environ["CDSW_PROJECT_ID"]
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "berka_loan_default")

    experiment_id, run_id, best_acc = select_best_run(experiment_name)
    print(f"Best run: {run_id} (val_accuracy={best_acc:.4f})")

    api = default_client()
    checkpoint_path = promote_checkpoint(api, project_id, experiment_id, run_id)
    print(f"Promoted checkpoint to {checkpoint_path}")

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    model_name = f"loan-default-pytorch-{timestamp}"

    model_request = CreateModelRequest(
        project_id=project_id,
        name=model_name,
        description=f"PyTorch Berka loan default model from run {run_id} (val_acc={best_acc:.4f}).",
        disable_authentication=False,
        visibility="private",
    )
    model = api.create_model(model_request, project_id)
    model_id = model.id
    print(f"Created model {model_name} (id={model_id})")

    runtime_identifier = os.environ.get(
        "MODEL_RUNTIME_IDENTIFIER",
        "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.10-standard:2025.09.1-b5",
    )
    build_request = CreateModelBuildRequest(
        project_id=project_id,
        model_id=model_id,
        comment=f"Build for run {run_id}",
        file_path="cml/realtime_scoring_service.py",
        function_name="predict",
        kernel="python3",
        runtime_identifier=runtime_identifier,
        build_script_path="cml/install_model_requirements.sh",
    )
    build = api.create_model_build(build_request, project_id, model_id)
    build_id = build.id
    print(f"Triggered model build {build_id}")

    build = wait_for_build(api, project_id, model_id, build_id)
    print(f"Model build {build_id} finished with status {build.status}")

    env_vars = {
        "MODEL_PATH": str(checkpoint_path),
    }
    deployment_request = CreateModelDeploymentRequest(
        project_id=project_id,
        model_id=model_id,
        build_id=build_id,
        cpu=float(os.environ.get("MODEL_DEPLOY_CPU", "1")),
        memory=float(os.environ.get("MODEL_DEPLOY_MEMORY", "2")),
        replicas=int(os.environ.get("MODEL_DEPLOY_REPLICAS", "1")),
        environment=env_vars,
    )
    deployment = api.create_model_deployment(deployment_request, project_id, model_id, build_id)
    deployment_id = deployment.id
    print(f"Triggered deployment {deployment_id}")

    deployment = wait_for_deployment(api, project_id, model_id, build_id, deployment_id)
    print(f"Deployment {deployment_id} is {deployment.status}")

    domain = os.environ.get("CDSW_DOMAIN")
    if domain:
        invoke_url = f"https://{domain}/model-deployments/{deployment_id}/invocations"
        print(f"Invoke URL: {invoke_url}")

    print("Model access key:", model.access_key)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - CLI helper
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
