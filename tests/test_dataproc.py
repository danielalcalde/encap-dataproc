import argparse
import importlib
import importlib.machinery
import importlib.util
import io
import json
import os
import re
import stat
import subprocess
import sys
import time
import types
import zipfile
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import MagicMock, Mock
from urllib.error import HTTPError, URLError

import pytest

from encap_lib import autopull, backends, encap_settings as settings
from encap_lib.backends import BackendError, ConfigurationError, MonitoringError, PluginConfig, RecordWriter, RunHandle, RunRequest
from encap_lib.run_preparation import stage_source, stage_workspace, write_run_configuration
from encap_dataproc import registration
from encap_dataproc.backend import DataprocBackend, archive_layout, build_workflow, create_archive, validate_config
from encap_dataproc.backend import timeout_seconds
from encap_dataproc.instances import instance_ids, parse_instances
from encap_dataproc.pulling import PullExclusions


@pytest.fixture
def config():
    return {
        "bucket": "test-bucket", "project": "test-project", "region": "europe-west1",
        "zone": "europe-west1-b",
        "subnet": "projects/network-project/regions/europe-west1/subnetworks/private",
        "python": "/opt/python/bin/python",
        "image_uri": "projects/image-project/global/images/family/custom",
        "service_account": "worker@test-project.iam.gserviceaccount.com",
        "network_tag": "test-network", "machine_type": "n4-standard-4",
    }


def cloud_job(job_id, state="RUNNING"):
    return {
        "reference": {"jobId": job_id}, "status": {"state": state},
        "placement": {"clusterName": "remote-cluster"},
        "labels": {"encap-request-id": "123"},
        "pysparkJob": {"args": ["--request-uri", "gs://test-bucket/encap/trial/source/request.json"]},
    }


@pytest.fixture
def cloud_metadata(monkeypatch):
    read = Mock(return_value=({
        "source": "experiments/training", "run_id": "trial", "instance": 0, "multiple_instances": False,
    }, "1"))
    monkeypatch.setattr("encap_dataproc.backend.GCSStore.read", read)
    return read


def test_cloud_listing_only_includes_encap_jobs_and_paginates(monkeypatch, cloud_metadata):
    untracked = cloud_job("untracked-job")
    untracked.pop("labels")
    execute = Mock(side_effect=[
        {"jobs": [untracked, cloud_job("queued-job", "PENDING")], "nextPageToken": "next"},
        {"jobs": [cloud_job("encap-job")]},
    ])
    monkeypatch.setattr(DataprocBackend, "_api", execute)
    config = PluginConfig({"project": "test-project", "region": "europe-west1", "console_authuser": "1"})
    statuses = registration.list_runs(config)
    assert [status.state for status in statuses] == ["submitted", "running"]
    assert [status.details["job_id"] for status in statuses] == ["queued-job", "encap-job"]
    assert statuses[0].details == {
        "source": "experiments/training", "name": "trial", "instance": 0, "multiple_instances": False,
        "project": "test-project", "region": "europe-west1", "job_id": "queued-job",
        "job": {"state": "PENDING"}, "placement": {"clusterName": "remote-cluster"},
        "labels": {"encap-request-id": "123"},
        "link": (
            "https://console.cloud.google.com/dataproc/jobs/queued-job/summary"
            "?region=europe-west1&authuser=1&project=test-project"
        ),
    }
    assert statuses[0].outputs_finalized is None
    assert execute.call_count == 2
    for call in execute.call_args_list:
        assert call.args == ("GET", "projects/test-project/regions/europe-west1/jobs")
        assert call.kwargs["parameters"]["filter"] == "status.state = ACTIVE AND labels.encap-request-id = *"
        assert "jobStateMatcher" not in call.kwargs["parameters"]
        assert call.kwargs["parameters"]["pageSize"] == 100
    assert "pageToken" not in execute.call_args_list[0].kwargs["parameters"]
    assert execute.call_args_list[1].kwargs["parameters"]["pageToken"] == "next"
    assert cloud_metadata.call_count == 2
    cloud_metadata.assert_called_with("encap/trial/source/request.json")


@pytest.mark.parametrize("labels", [{}, {"encap-request-id": ""}, {"other": "value"}])
def test_cloud_listing_ignores_non_encap_jobs_without_reading_storage(monkeypatch, cloud_metadata, labels):
    job = cloud_job("not-encap")
    job["labels"] = labels
    job.pop("pysparkJob")
    monkeypatch.setattr(DataprocBackend, "_api", Mock(return_value={"jobs": [job]}))
    assert DataprocBackend.list_runs({"project": "test-project", "region": "europe-west1"}) == []
    cloud_metadata.assert_not_called()


@pytest.mark.parametrize("metadata,source", [
    ({"source": "experiments/objectives_and_eligibility/quantity_ranking_original_framework"},
     "experiments/objectives_and_eligibility/quantity_ranking_original_framework"),
    ({"display_folder": "experiments/objectives/0encap_folder/training/trial"}, "experiments/objectives/training"),
    ({"display_folder": "experiments/train/trial", "entrypoint": "train.py"}, "experiments/train.py"),
    ({"display_folder": "/workspace/experiments/0encap_folder/training/trial"}, "/workspace/experiments/training"),
    ({"storage_layout": "mirrored", "root_path": "workspace",
      "experiment_path": "workspace/experiments/0encap_folder/training/trial",
      "display_folder": "../0encap_folder/training/trial"}, "experiments/training"),
    ({"storage_layout": "mirrored", "root_path": ".",
      "experiment_path": "experiments/train/trial", "entrypoint": "train.py"}, "experiments/train.py"),
])
def test_cloud_identity_recovers_source_for_new_and_existing_runs(cloud_metadata, metadata, source):
    cloud_metadata.return_value = (dict(metadata, run_id="trial"), "1")
    identity = DataprocBackend({})._job_identity(cloud_job("encap-job"))
    assert identity == {"source": source, "name": "trial", "instance": 0, "multiple_instances": False}


def test_cloud_identity_preserves_instance_information(cloud_metadata):
    cloud_metadata.return_value = ({
        "source": "experiments/training", "run_id": "trial", "instance": 7, "multiple_instances": True,
    }, "1")
    assert DataprocBackend({})._job_identity(cloud_job("encap-job")) == {
        "source": "experiments/training", "name": "trial", "instance": 7, "multiple_instances": True,
    }


@pytest.mark.parametrize("metadata", [
    [], {}, {"run_id": ""}, {"run_id": "../bad"}, {"run_id": "trial", "source": ""},
    {"run_id": "trial", "source": "experiments/\x1b[31m"},
    {"run_id": "trial", "display_folder": "experiments/0encap_folder/training/other"},
    {"run_id": "trial", "display_folder": "experiments/train/trial"},
    {"run_id": "trial", "display_folder": "experiments/train/trial", "entrypoint": "../train.py"},
    {"run_id": "trial", "source": "experiments/training", "instance": True},
    {"run_id": "trial", "source": "experiments/training", "instance": -1},
    {"run_id": "trial", "source": "experiments/training", "multiple_instances": "yes"},
    {"run_id": "trial", "storage_layout": "mirrored", "root_path": "workspace", "experiment_path": "outside/trial"},
])
def test_cloud_identity_reports_invalid_metadata(cloud_metadata, metadata):
    cloud_metadata.return_value = (metadata, "1")
    with pytest.raises(MonitoringError, match="Encap"):
        DataprocBackend({})._job_identity(cloud_job("encap-job"))


@pytest.mark.parametrize("arguments", [
    [], ["--request-uri"], ["--request-uri", "gs://test-bucket/a", "--request-uri", "gs://test-bucket/b"],
    ["--request-uri", "https://example.com/request.json"], ["--request-uri", "gs://test-bucket"], None,
])
def test_cloud_identity_reports_missing_or_invalid_request_uri(cloud_metadata, arguments):
    job = cloud_job("encap-job")
    job["pysparkJob"]["args"] = arguments
    with pytest.raises(MonitoringError):
        DataprocBackend({})._job_identity(job)
    cloud_metadata.assert_not_called()


def test_cloud_identity_reports_missing_or_inaccessible_metadata(cloud_metadata):
    cloud_metadata.return_value = None
    with pytest.raises(MonitoringError, match="metadata is missing"):
        DataprocBackend({})._job_identity(cloud_job("encap-job"))
    cloud_metadata.side_effect = BackendError("permission denied")
    with pytest.raises(MonitoringError, match="Cannot read Encap request metadata .*permission denied"):
        DataprocBackend({})._job_identity(cloud_job("encap-job"))


@pytest.mark.parametrize("state,expected", [
    ("RUNNING", "running"), ("PENDING", "submitted"), ("SETUP_DONE", "submitted"),
    ("CANCEL_PENDING", "running"), ("CANCEL_STARTED", "running"), ("ATTEMPT_FAILURE", "running"),
    ("STATE_UNSPECIFIED", "unknown"), ("FUTURE_STATE", "unknown"),
    ("DONE", None), ("ERROR", None), ("CANCELLED", None),
])
def test_cloud_listing_maps_states_without_claiming_completion(monkeypatch, cloud_metadata, state, expected):
    monkeypatch.setattr(DataprocBackend, "_api", Mock(return_value={"jobs": [cloud_job("job", state)]}))
    statuses = DataprocBackend.list_runs({"project": "test-project", "region": "europe-west1"})
    assert [status.state for status in statuses] == ([] if expected is None else [expected])


@pytest.mark.parametrize("page", [{}, {"jobs": []}])
def test_cloud_listing_empty(monkeypatch, page):
    monkeypatch.setattr(DataprocBackend, "_api", Mock(return_value=page))
    assert DataprocBackend.list_runs({"project": "test-project", "region": "europe-west1"}) == []


@pytest.mark.parametrize("config", [
    {}, {"project": "test-project"}, {"region": "europe-west1"},
    {"project": "bad/project", "region": "europe-west1"},
    {"project": "test-project", "region": None},
])
def test_cloud_listing_requires_only_valid_project_and_region(monkeypatch, config):
    execute = Mock()
    monkeypatch.setattr(DataprocBackend, "_api", execute)
    with pytest.raises(ConfigurationError, match="Cloud listing requires"):
        DataprocBackend.list_runs(config)
    execute.assert_not_called()


@pytest.mark.parametrize("page", [
    {"unreachable": ["encrypted-job"]}, {"jobs": {}}, {"jobs": [None]},
    {"jobs": [{"labels": {"encap-request-id": "123"}}]},
    {"jobs": [{"reference": None, "labels": {"encap-request-id": "123"}}]},
    {"jobs": [{"reference": {"jobId": "job"}, "status": {}, "labels": {"encap-request-id": "123"}}]},
    {"jobs": [{"labels": []}]},
    {"nextPageToken": 123}, {"nextPageToken": 0}, {"nextPageToken": []},
])
def test_cloud_listing_rejects_incomplete_or_invalid_results(monkeypatch, page):
    monkeypatch.setattr(DataprocBackend, "_api", Mock(return_value=page))
    with pytest.raises(MonitoringError):
        DataprocBackend.list_runs({"project": "test-project", "region": "europe-west1"})


def test_cloud_listing_rejects_repeated_pages(monkeypatch):
    execute = Mock(return_value={"nextPageToken": "repeated"})
    monkeypatch.setattr(DataprocBackend, "_api", execute)
    with pytest.raises(MonitoringError, match="repeated"):
        DataprocBackend.list_runs({"project": "test-project", "region": "europe-west1"})
    assert execute.call_count == 2


def test_cloud_listing_does_not_hide_later_page_errors(monkeypatch, cloud_metadata):
    execute = Mock(side_effect=[
        {"jobs": [cloud_job("job")], "nextPageToken": "next"},
        MonitoringError("permission denied"),
    ])
    monkeypatch.setattr(DataprocBackend, "_api", execute)
    with pytest.raises(MonitoringError, match="permission denied"):
        DataprocBackend.list_runs({"project": "test-project", "region": "europe-west1"})


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("json_output", [False, True])
def test_cloud_status_cli_needs_no_local_runs(instance_cli, cloud_metadata, monkeypatch, capsys, explicit, json_output):
    _, submit, source = instance_cli
    config = {"plugins": {"dataproc": {"project": "test-project", "region": "europe-west1"}}}
    config["dependencies"] = ["../missing-source"]
    settings.write_config_file(source / ".encap.conf", config)
    execute = Mock(return_value={"jobs": [cloud_job("remote-only")]})
    monkeypatch.setattr(DataprocBackend, "_api", execute)
    monkeypatch.setattr(sys, "argv", [
        "encap", "status", str(source), *(["-gcp"] if explicit else []), *(["--json"] if json_output else []),
    ])
    loader = importlib.machinery.SourceFileLoader(
        "encap_dataproc_cloud_status_cli", str(Path(backends.__file__).resolve().parents[1] / "encap"),
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    module.main()
    output = capsys.readouterr().out
    if json_output:
        result = json.loads(output)
        assert result["backend"] == "dataproc"
        assert result["runs"][0]["details"]["job_id"] == "remote-only"
        assert result["runs"][0]["details"]["source"] == "experiments/training"
        assert result["runs"][0]["details"]["name"] == "trial"
    else:
        assert "Backend: dataproc" in output
        assert "| Experiment" in output
        assert "experiments/training" in output
        assert "trial" in output
        assert "remote-only" not in output
        assert "Running" in output
    submit.assert_not_called()
    assert not (source.parent / "0encap_folder").exists()


@pytest.mark.parametrize("text,expected,ids", [
    ("4", 4, [0, 1, 2, 3]),
    ("[0, 2, 7]", [0, 2, 7], [0, 2, 7]),
    ("range(2, 5)", [2, 3, 4], [2, 3, 4]),
    ("range(4)", [0, 1, 2, 3], [0, 1, 2, 3]),
    ("range(1, 6, 2)", [1, 3, 5], [1, 3, 5]),
    ("[7]", [7], [7]),
])
def test_dataproc_instance_parser(text, expected, ids):
    assert parse_instances(text) == expected
    assert instance_ids(expected) == ids


@pytest.mark.parametrize("text", [
    "0", "-2", "[]", "[1, 1]", "[True]", "[1.2]", "range(0)", "range(1, 3, 0)",
    "__import__('os').getcwd()",
])
def test_dataproc_instance_parser_rejects_invalid_values(text):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_instances(text)


@pytest.fixture
def instance_cli(config, tmp_path, monkeypatch):
    loader = importlib.machinery.SourceFileLoader(
        "encap_dataproc_instance_test_cli", str(Path(backends.__file__).resolve().parents[1] / "encap"),
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "config", {"file_extension": {"py": "python -u"}})
    monkeypatch.setattr(settings, "args_config", {})
    monkeypatch.setattr(settings, "using_slurm", False)
    monkeypatch.setattr(settings, "using_pueue", False)
    monkeypatch.setattr(autopull, "watch", Mock(return_value={
        "enabled": True, "interval_seconds": 60, "pid": 123,
    }))
    monkeypatch.setattr(backends, "discover", lambda: {"dataproc": registration})
    submit = Mock(return_value=RunHandle({"operation": "submitted"}))
    monkeypatch.setattr(DataprocBackend, "submit", submit)
    monkeypatch.setattr(DataprocBackend, "tail", Mock())
    monkeypatch.setattr(DataprocBackend, "pull", Mock())
    source = tmp_path / "experiment"
    source.mkdir()
    (source / "run.py").write_text("print('hello')")
    settings.write_config_file(source / ".encap.conf", {"plugins": {"dataproc": dict(config, i=[2, 5])}})

    def invoke(*arguments):
        monkeypatch.setattr(sys, "argv", ["encap", "run", "experiment", "-n", "trial", *arguments])
        return module.main()

    return invoke, submit, source


@pytest.mark.parametrize("arguments,expected", [
    (["-gcp"], [2, 5]),
    (["-gcp_i", "2"], 2),
    (["-gcp_i", "[7]"], [7]),
    (["-gcp_i", "range(2, 5)"], [2, 3, 4]),
])
def test_dataproc_cli_options_override_config_and_persist(instance_cli, arguments, expected):
    invoke, submit, _ = instance_cli
    invoke(*arguments)
    request = submit.call_args.args[0]
    assert not hasattr(request, "instances")
    for filename in (".encap.conf", ".encap_history.conf"):
        saved = settings.read_config_file(request.run_folder / filename)
        assert saved["plugins"]["dataproc"]["i"] == expected
        assert "backend_instances" not in saved


@pytest.mark.parametrize("text", ["0", "[]", "[1, 1]", "range(0)"])
def test_invalid_dataproc_cli_selection_never_prepares_run(instance_cli, text):
    invoke, submit, source = instance_cli
    with pytest.raises(SystemExit) as error:
        invoke("-gcp_i", text)
    assert error.value.code == 2
    submit.assert_not_called()
    assert not (source.parent / "0encap_folder").exists()


def test_invalid_dataproc_config_selection_never_prepares_run(instance_cli):
    invoke, submit, source = instance_cli
    config = settings.read_config_file(source / ".encap.conf")
    config["plugins"]["dataproc"]["i"] = [1, 1]
    settings.write_config_file(source / ".encap.conf", config)
    with pytest.raises(SystemExit) as error:
        invoke("-gcp")
    assert error.value.code == 2
    submit.assert_not_called()
    assert not (source.parent / "0encap_folder").exists()


@pytest.mark.parametrize("legacy_root", [False, True])
def test_dataproc_uses_shared_workspace_root(config, tmp_path, legacy_root):
    namespace = PluginConfig(dict(config, inputs={"{root}/data": "gs://input-bucket/data/"}))
    namespace.workspace_root = tmp_path.resolve()
    if legacy_root:
        namespace["workspace_root"] = str(tmp_path)
    resolved = validate_config(namespace)
    assert resolved["workspace_root"] == str(tmp_path.resolve())


def test_dataproc_rejects_conflicting_workspace_roots(config, tmp_path):
    other = tmp_path / "other"
    other.mkdir()
    namespace = PluginConfig(dict(config, workspace_root=str(other)))
    namespace.workspace_root = tmp_path.resolve()
    with pytest.raises(ConfigurationError, match="workspace_root.*match"):
        validate_config(namespace)


def test_dataproc_cli_uses_core_root_for_dependencies_inputs_and_cloud_path(instance_cli, monkeypatch):
    invoke, submit, source = instance_cli
    root = source.parent
    (root / "src").mkdir()
    (root / "src/value.txt").write_text("core root")
    config = settings.read_config_file(source / ".encap.conf")
    config.update(workspace_root="..", dependencies=["{root}/src"])
    config["plugins"]["dataproc"]["inputs"] = {"{root}/data": "gs://input-bucket/data/"}
    settings.write_config_file(source / ".encap.conf", config)
    validated = []
    original = DataprocBackend.__init__

    def initialize(backend, namespace):
        original(backend, namespace)
        validated.append(backend)

    monkeypatch.setattr(DataprocBackend, "__init__", initialize)
    invoke("-gcp_i", "1")
    request = submit.call_args.args[0]
    backend = validated[0]
    assert backend.config["workspace_root"] == str(root.resolve())
    assert (request.workspace_folder / "src/value.txt").read_text() == "core root"
    handle = backend._new_handle(request)
    assert handle.data["run_uri"] == "gs://test-bucket/encap/0encap_folder/experiment/trial"
    archive = root / "payload.zip"
    experiment = create_archive(request, archive, backend.config["inputs"], backend.config["workspace_root"])
    assert experiment == "0encap_folder/experiment/trial"
    assert archive_layout(request, backend.config["inputs"], backend.config["workspace_root"])[2] == {
        "data": "gs://input-bucket/data/",
    }


@pytest.fixture
def handle():
    return RunHandle({
        "project": "saved-project", "region": "saved-region",
        "run_uri": "gs://saved-bucket/trial", "output_uri": "gs://saved-bucket/trial/outputs",
        "result_uri": "gs://saved-bucket/trial/result.json",
        "request_id": "test-uuid", "cluster_base": "encap-unique",
        "operation": "projects/saved-project/regions/saved-region/operations/123",
    })


@pytest.fixture
def run_request(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "train.py").write_text("print('training')")
    (source / ".encap.conf").write_text("script_name: train.py\n")
    (source / ".encap_history.conf").write_text("script_name: train.py\n")
    return RunRequest(source, "train.py", ["--value", "two words"], {"MY_VALUE": "yes"},
                      "trial", tmp_path / "record")


def test_submission_publishes_original_source_identity(config, run_request, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    request = replace(run_request, source_path=tmp_path / "experiments/train.py")
    backend = DataprocBackend(config)
    backend._check_buckets = Mock()
    captured = []
    def command(arguments, stream=False):
        for argument in arguments:
            if argument.endswith("/request.json") and not argument.startswith("gs://"):
                captured.append(json.loads(Path(argument).read_text()))
        return Mock(stdout="")
    backend._command = Mock(side_effect=command)
    backend._api = Mock(return_value={"name": "projects/test-project/regions/europe-west1/operations/123"})
    writer = RecordWriter.reserve(request.record_dir, registration, request.run_id)
    backend.submit(request, writer)
    assert len(captured) == 1
    assert captured[0]["source"] == "experiments/train.py"
    assert captured[0]["run_id"] == "trial"


@pytest.mark.parametrize("key,value", [
    ("bucket", "gs://bucket"), ("project", "../project"), ("region", ""),
    ("image_uri", "local-image"), ("service_account", "user@example.com"),
    ("subnet", "short-name"), ("python", "python"), ("output_prefix", "../unsafe"),
    ("output_prefix", ""), ("output_prefix", "/absolute"), ("output_prefix", "name/../other"),
    ("output_prefix", "name//other"), ("output_prefix", "gs://bucket/folder"),
    ("disk_size_gb", True), ("cluster_properties", {"memory": 2}),
    ("cluster_properties", {"spark.secret": "must-not-be-persisted"}),
    ("access_token", "forbidden"),
    ("cluster_name", ""), ("cluster_name", None), ("cluster_name", 123),
    ("cluster_name", "a"), ("cluster_name", "a" * 36),
    ("cluster_name", "ML-cluster"), ("cluster_name", "1-cluster"),
    ("cluster_name", "ml_cluster"), ("cluster_name", "ml-cluster-"),
    ("cluster_name", "-ml-cluster"), ("cluster_name", "ml cluster"),
    ("inputs", []), ("inputs", {"../data": "https://bucket/path"}),
    ("inputs", {".": "gs://input-bucket/path/"}),
    ("inputs", {"data": "gs://input-bucket/path/", "data/nested": "gs://input-bucket/other/"}),
    ("i", 0), ("i", -1), ("i", True), ("i", []), ("i", [1, 1]),
    ("i", [-1]), ("i", [True]), ("i", "2"),
    ("inputs", {"{root}/data": "gs://input-bucket/data/"}),
])
def test_invalid_config(config, key, value):
    config[key] = value
    with pytest.raises(ConfigurationError):
        validate_config(config)


def test_required_config_and_alternate_workflow(config, handle):
    with pytest.raises(ConfigurationError, match="missing"):
        validate_config({})
    config.update(project="alternate", region="us-central1", zone="us-central1-a",
                  machine_type="n4-standard-8", python="/another/python",
                  image_uri="projects/other/global/images/another-image",
                  service_account="other@another-project.iam.gserviceaccount.com",
                  output_prefix="alternate/prefix", cluster_properties={"spark:spark.driver.memory": "4g"})
    validate_config(config)
    handle.data["job_step_id"] = "experiment-trial"
    workflow = build_workflow(config, handle.data)
    cluster = workflow["placement"]["managedCluster"]["config"]
    assert cluster["masterConfig"]["machineTypeUri"] == "n4-standard-8"
    assert cluster["masterConfig"]["imageUri"] == config["image_uri"]
    assert cluster["gceClusterConfig"]["serviceAccount"] == config["service_account"]
    assert cluster["softwareConfig"]["properties"]["spark:spark.driver.memory"] == "4g"
    assert cluster["softwareConfig"]["properties"]["yarn:yarn.nodemanager.resource.memory-mb"] == "8192"
    job = workflow["jobs"][0]["pysparkJob"]
    assert job["properties"]["spark.pyspark.python"] == "/another/python"
    assert job["mainPythonFileUri"] == "gs://saved-bucket/trial/source/runner.py"


@pytest.mark.parametrize("settings,seconds", [
    ({}, 86400),
    ({"timeout": "1s"}, 1),
    ({"timeout": "30s"}, 30),
    ({"timeout": "599s"}, 599),
    ({"timeout": "10m"}, 600),
    ({"timeout": "2h"}, 7200),
    ({"timeout": "24h"}, 86400),
    ({"timeout": "1d"}, 86400),
    ({"timeout": "86401s"}, 86401),
    ({"timeout": "25h"}, 90000),
    ({"timeout": "2d"}, 172800),
    ({"timeout": "1w"}, 604800),
    ({"timeout": 7200}, 7200),
    ({"timeout": "unlimited"}, None),
    ({"timeout": -1}, None),
    ({"timeout": "-1"}, None),
    ({"timeout_seconds": 30}, 30),
    ({"timeout_seconds": 3600}, 3600),
    ({"timeout_seconds": 172800}, 172800),
])
def test_timeout_parsing_and_workflow_enforcement(config, handle, settings, seconds):
    config.update(settings)
    validated = validate_config(config)
    assert timeout_seconds(validated) == seconds
    data = dict(handle.data, job_step_id="experiment-trial")
    workflow = build_workflow(validated, data)
    if seconds is not None and 600 <= seconds <= 86400:
        assert workflow["dagTimeout"] == f"{seconds}s"
    else:
        assert "dagTimeout" not in workflow


@pytest.mark.parametrize("value", [
    None, True, False, 0, -2, 1.5, "", "0s", "-1h", "1.5h", "1hour", "24H",
    "forever", "600", " 24h", "24h ", "1h30m", [], {}, 10**30, "999999999999999w",
])
def test_invalid_timeout_is_rejected(config, value):
    with pytest.raises(ConfigurationError, match="plugins.dataproc.timeout"):
        validate_config(dict(config, timeout=value))


@pytest.mark.parametrize("value", [None, True, 0, -1, "24h", 1.5, 10**30])
def test_invalid_legacy_timeout_is_rejected(config, value):
    with pytest.raises(ConfigurationError, match="timeout_seconds"):
        validate_config(dict(config, timeout_seconds=value))


def test_timeout_aliases_cannot_be_combined(config):
    with pytest.raises(ConfigurationError, match="Specify only one"):
        validate_config(dict(config, timeout="24h", timeout_seconds=86400))


def test_invalid_timeout_never_prepares_or_submits(instance_cli):
    invoke, submit, source = instance_cli
    config = settings.read_config_file(source / ".encap.conf")
    config["plugins"]["dataproc"]["timeout"] = "0h"
    settings.write_config_file(source / ".encap.conf", config)
    with pytest.raises(SystemExit) as result:
        invoke("-gcp")
    assert result.value.code == 2
    submit.assert_not_called()
    assert not (source.parent / "0encap_folder").exists()


@pytest.mark.parametrize("settings,worker_seconds", [
    ({}, None), ({"timeout": "10m"}, None), ({"timeout": "24h"}, None),
    ({"timeout": "unlimited"}, None), ({"timeout": -1}, None),
    ({"timeout": "30s"}, 30), ({"timeout": "2d"}, 172800), ({"timeout": "1w"}, 604800),
    ({"timeout_seconds": 172800}, 172800),
])
def test_submit_wires_worker_timeout_only_outside_dataproc_range(
    config, run_request, monkeypatch, settings, worker_seconds,
):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    backend = DataprocBackend(dict(config, **settings))
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    uploaded = []

    def command(arguments, stream=False):
        if arguments[1:3] == ["storage", "cp"]:
            for argument in arguments:
                if argument.endswith("/request.json") and not argument.startswith("gs://"):
                    uploaded.append(json.loads(Path(argument).read_text()))
        return Mock(stdout="")

    backend._command = Mock(side_effect=command)
    backend._api = Mock(return_value={"name": "projects/test-project/regions/europe-west1/operations/123"})
    backend.submit(run_request, writer)
    assert len(uploaded) == 1
    if worker_seconds is None:
        assert "timeout_seconds" not in uploaded[0]
    else:
        assert uploaded[0]["timeout_seconds"] == worker_seconds
        assert "dagTimeout" not in backend._api.call_args.args[2]


@pytest.mark.parametrize("prefix", [None, "encap", "ml-experiments", "team/experiments"])
def test_output_folder_default_and_custom_name(config, run_request, prefix):
    if prefix is not None:
        config["output_prefix"] = prefix
    handle = DataprocBackend(config)._new_handle(run_request)
    expected = f"gs://test-bucket/{prefix or 'encap'}/trial-{handle.data['request_id']}"
    assert handle.data["run_uri"] == expected
    assert handle.data["output_uri"] == expected + "/outputs"
    assert handle.data["result_uri"] == expected + "/result.json"


def test_new_output_folder_default_does_not_change_saved_run_locations(config, handle):
    backend = DataprocBackend(config)
    saved = dict(handle.data)
    assert backend._data(handle) == saved
    assert handle.data["run_uri"] == "gs://saved-bucket/trial"
    assert handle.data["output_uri"] == "gs://saved-bucket/trial/outputs"


@pytest.mark.parametrize("cluster_name", [None, "ml-cluster", "ab", "a" * 35])
def test_workflow_names(config, run_request, cluster_name):
    if cluster_name is not None:
        config["cluster_name"] = cluster_name
    backend = DataprocBackend(config)
    handle = backend._new_handle(run_request)
    workflow = build_workflow(backend.config, handle.data)
    assert handle.data["cluster_base"] == (cluster_name or "encap-cluster")
    assert workflow["placement"]["managedCluster"]["clusterName"] == handle.data["cluster_base"]
    assert workflow["jobs"][0]["stepId"] == "experiment-trial"
    assert workflow["jobs"][0]["labels"] == {"encap-request-id": handle.data["request_id"]}
    assert handle.data["request_id"] in handle.data["run_uri"]


@pytest.mark.parametrize("name,expected", [
    ("trial", "experiment-trial"),
    ("Trial_2.0", "experiment-Trial_2-0"),
    ("trial._-", "experiment-trial"),
    ("x" * 100, "experiment-" + "x" * 39),
    ("x" * 38 + "_" + "tail", "experiment-" + "x" * 38),
])
def test_job_name_normalization(config, run_request, name, expected):
    backend = DataprocBackend(config)
    handle = backend._new_handle(replace(run_request, run_id=name))
    job = build_workflow(config, handle.data)["jobs"][0]
    assert job["stepId"] == expected
    assert 3 <= len(job["stepId"]) <= 50
    assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*[A-Za-z0-9]", job["stepId"])


def test_source_archive_exclusions_and_symlinks(run_request, tmp_path):
    source = run_request.run_folder
    (source / ".env").write_text("private")
    (source / ".gcp").mkdir()
    (source / ".gcp" / "config.yaml").write_text("old config")
    (source / "nested").mkdir()
    (source / "nested" / "empty").touch()
    archive = tmp_path / "source.zip"
    create_archive(run_request, archive)
    with zipfile.ZipFile(archive) as result:
        assert set(result.namelist()) == {"train.py", "nested/empty", ".encap.conf", ".encap_history.conf"}
        assert result.read("nested/empty") == b""
    (source / "symlink").symlink_to(source / "train.py")
    with pytest.raises(ConfigurationError, match="symlinks"):
        create_archive(run_request, archive)


def test_archive_requires_prepared_run_and_packages_its_snapshot(run_request, tmp_path):
    original = tmp_path / "original"
    original.mkdir()
    (original / "train.py").write_text("this is not the submitted snapshot")
    staged = run_request.run_folder
    (staged / ".encap-run").mkdir()
    (staged / ".encap-run" / "run.json").write_text("internal handle")
    archive = tmp_path / "payload.zip"
    create_archive(run_request, archive)
    with zipfile.ZipFile(archive) as payload:
        assert payload.read("train.py") == b"print('training')"
        assert ".encap.conf" in payload.namelist()
        assert ".encap_history.conf" in payload.namelist()
        assert not any(".encap-run" in name for name in payload.namelist())
    (staged / ".encap_history.conf").unlink()
    with pytest.raises(ConfigurationError, match="prepared run folder"):
        create_archive(run_request, archive)


def test_dependency_workspace_archive_and_execution(run_request, tmp_path, runner):
    from encap_lib import encap_settings as settings
    from encap_lib.run_preparation import resolve_dependencies, stage_source, stage_workspace, write_run_configuration

    project = tmp_path / "project"
    source = project / "experiments/training"
    shared = project / "experiments/shared"
    library = project / "src"
    source.mkdir(parents=True)
    shared.mkdir()
    library.mkdir()
    (library / "empty").mkdir()
    (library / "value.txt").write_text("library")
    (library / ".env").write_text("excluded")
    (shared / "value.txt").write_text("other experiment")
    (source / ".encap.conf").write_text("dependencies: [../../src, ../shared]\n")
    (source / "train.py").write_text(
        "from pathlib import Path\n"
        "assert Path('../../src/value.txt').read_text() == 'library'\n"
        "assert (Path(__file__).parent / '../shared/value.txt').read_text() == 'other experiment'\n"
        "assert Path('../../src/empty').is_dir()\n"
        "assert not Path('../../src/.env').exists()\n"
        "Path('result.txt').write_text('dependencies passed')\n"
    )
    config = settings.read_config_file(source / ".encap.conf")
    dependencies = resolve_dependencies(config["dependencies"], source, [])
    stage_source(source, run_request.run_folder, [])
    write_run_configuration(run_request.run_folder, config, {})
    (run_request.run_folder / ".encap-run").mkdir()
    workspace, experiment_path = stage_workspace(source, run_request.run_folder, dependencies, [])
    request = replace(run_request, workspace_folder=workspace, experiment_path=experiment_path)
    archive = tmp_path / "dependencies.zip"
    create_archive(request, archive)
    with zipfile.ZipFile(archive) as payload:
        assert "src/value.txt" in payload.namelist()
        assert "src/empty/" in payload.namelist()
        assert "src/.env" not in payload.namelist()
        assert "experiments/training/train.py" in payload.namelist()
        assert not any(".encap-run" in Path(name).parts for name in payload.namelist())
    (library / "value.txt").write_text("changed")
    worker_request = {
        "entrypoint": "train.py", "experiment_path": experiment_path,
        "arguments": [], "run_id": "trial", "project": "test", "zone": "test-zone",
        "data_url": "https://example.invalid",
    }
    def download(uri, destination, project):
        if destination.name == "request.json":
            destination.write_text(json.dumps(worker_request))
        else:
            destination.write_bytes(archive.read_bytes())
    runner.checkpoint.download.side_effect = download
    def upload(folder, uri, project, log_checkpoint=None):
        assert folder.parts[-2:] == ("experiments", "training")
        assert (folder / "result.txt").read_text() == "dependencies passed"
        assert not (folder / "src").exists()
        assert (folder / "log").is_file()
    runner.checkpoint.upload_checkpoint.side_effect = upload
    assert runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output",
                      "gs://bucket/result", "test") == 0
    assert runner.checkpoint.save_result.call_args.args[0]["outputs_finalized"] is True


@pytest.mark.parametrize("experiment_path", ["../outside", "/absolute", "a/../../b", "a\\b", "", None])
def test_workspace_rejects_unsafe_experiment_path(run_request, tmp_path, experiment_path):
    with pytest.raises(ConfigurationError, match="safe relative"):
        create_archive(replace(run_request, experiment_path=experiment_path), tmp_path / "payload.zip")


def test_submit_saves_recovery_before_upload_and_request(config, run_request, monkeypatch, capsys):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    backend = DataprocBackend(config)
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    def command(arguments, stream=False):
        if arguments[1:4] == ["storage", "buckets", "describe"]:
            assert not stream
            assert writer.read()["handle"] is None
            return Mock(stdout="")
        assert stream is True
        saved = writer.read()
        assert saved["state"] == "submission-unconfirmed"
        assert saved["handle"]["data"]["request_id"]
        assert "operation" not in saved["handle"]["data"]
        return Mock(stdout="")
    backend._command = Mock(side_effect=command)
    def api(method, path, body, parameters):
        saved = writer.read()
        assert saved["submission_phase"] == "request-started"
        assert saved["handle"]["data"]["request_id"] == parameters["requestId"]
        assert method == "POST"
        assert body["placement"]["managedCluster"]["clusterName"] == "encap-cluster"
        assert body["jobs"][0]["stepId"] == "experiment-trial"
        assert body["jobs"][0]["labels"]["encap-request-id"] == parameters["requestId"]
        assert saved["handle"]["data"]["job_step_id"] == "experiment-trial"
        assert "hello_world" not in json.dumps(body)
        return {"name": "projects/test-project/regions/europe-west1/operations/123"}
    backend._api = Mock(side_effect=api)
    returned = backend.submit(run_request, writer)
    assert returned.data["operation"].endswith("/123")
    assert returned.data["settings"] == config
    assert (writer.path.parent / "bucket_location").read_text() == returned.data["run_uri"] + "\n"
    assert writer.read()["state"] == "submitted"
    assert "access_token" not in json.dumps(writer.read())
    output = capsys.readouterr().out
    assert f"Run: {returned.data['run_uri']}" in output
    assert "(n4-standard-4)" in output
    assert "Submission record:" in output
    assert "workflows: https://console.cloud.google.com/dataproc/workflows?project=test-project" in output
    assert "Submitting workflow:" in output
    assert "Workflow submitted: projects/test-project/regions/europe-west1/operations/123" in output


@pytest.mark.parametrize("instances,ids", [
    (4, [0, 1, 2, 3]), ([0, 2, 7], [0, 2, 7]), ([7, 2, 0], [7, 2, 0]), ([5], [5]),
])
def test_submit_separate_workflow_per_instance(config, run_request, monkeypatch, instances, ids):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    config["i"] = instances
    backend = DataprocBackend(config)
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    requests = []
    def upload(command, stream=False):
        if command[1:4] == ["storage", "buckets", "describe"]:
            assert not stream
            return Mock(stdout="")
        assert stream
        request_file = next(Path(arg) for arg in command if arg.endswith("/request.json"))
        requests.append(json.loads(request_file.read_text()))
        saved = writer.read()
        assert saved["state"] == "submission-unconfirmed"
        assert len(saved["handle"]["data"]["instances"]) == len(ids)
        assert saved["handle"]["data"]["log_instance"] == ids[0]
        return Mock(stdout="")
    backend._command = Mock(side_effect=upload)
    backend._api = Mock(side_effect=[
        {"name": f"projects/test-project/regions/europe-west1/operations/{instance}"} for instance in ids
    ])
    request = replace(run_request, arguments=["--id={i}", "two words"])
    group = backend.submit(request, writer)
    assert group.version == 2
    assert group.data["log_instance"] == ids[0]
    children = group.data["instances"]
    assert [child["instance"] for child in children] == ids
    assert {child["handle"]["data"]["cluster_base"] for child in children} == {"encap-cluster"}
    assert len({child["handle"]["data"]["request_id"] for child in children}) == len(ids)
    assert len({child["handle"]["data"]["output_uri"] for child in children}) == len(ids)
    assert [item["instance"] for item in requests] == ids
    assert [item["arguments"] for item in requests] == [[f"--id={i}", "two words"] for i in ids]
    for item, child in zip(requests, children):
        assert item["project"] == config["project"]
        assert item["zone"] == config["zone"]
        assert item["data_url"] == backend._links(child["handle"]["data"])["results"]
        assert item["jobs_url"] == backend._links(child["handle"]["data"])["jobs"]
    assert all(child["submission_phase"] == "submitted" for child in children)
    assert writer.read()["state"] == "submitted"
    assert backend._api.call_count == len(ids)
    assert (writer.path.parent / "bucket_location").read_text().splitlines() == [
        child["handle"]["data"]["run_uri"] for child in children
    ]


def test_partial_group_submission_keeps_recovery_for_every_instance(config, run_request, monkeypatch):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    config["i"] = 3
    backend = DataprocBackend(config)
    backend._command = Mock()
    backend._api = Mock(side_effect=[
        {"name": "projects/test-project/regions/europe-west1/operations/first"},
        MonitoringError("connection lost"),
    ])
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    with pytest.raises(MonitoringError, match="connection lost"):
        backend.submit(run_request, writer)
    record = writer.read()
    assert record["state"] == "submission-unconfirmed"
    children = record["handle"]["data"]["instances"]
    assert [child["submission_phase"] for child in children] == ["submitted", "request-started", "prepared"]
    assert children[0]["handle"]["data"]["operation"].endswith("/first")
    backend._api.assert_called()
    assert backend._api.call_count == 2


def test_bucket_preflight_checks_all_unique_buckets(config):
    config["inputs"] = {
        "../data": "gs://input-bucket/folder/",
        "../more": "gs://input-bucket/file",
        "local": "gs://test-bucket/other/",
    }
    backend = DataprocBackend(config)
    backend._command = Mock()
    backend._check_buckets()
    commands = [call.args[0] for call in backend._command.call_args_list]
    assert commands == [
        ["gcloud", "storage", "buckets", "describe", f"gs://{bucket}", "--project", config["project"]]
        for bucket in ("input-bucket", "test-bucket")
    ]


@pytest.mark.parametrize("failure", ["bucket not found", "permission denied"])
def test_bucket_failure_prevents_upload_and_submission(config, run_request, monkeypatch, failure):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    backend = DataprocBackend(config)
    backend._command = Mock(side_effect=BackendError(failure))
    backend._api = Mock()
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    with pytest.raises(ConfigurationError, match="Cannot access bucket"):
        backend.submit(run_request, writer)
    backend._api.assert_not_called()
    assert backend._command.call_count == 1
    assert writer.read()["state"] == "not-submitted"
    assert writer.read()["handle"] is None
    assert not (writer.path.parent / "bucket_location").exists()


@pytest.mark.parametrize("failure_stage", [None, "bucket", "upload", "submission"])
@pytest.mark.parametrize("instances", [1, 3])
def test_dependency_workspace_is_removed_on_all_submission_exits(
    config, run_request, monkeypatch, failure_stage, instances,
):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    run_request = replace(run_request, record_dir=run_request.run_folder / ".encap-run")
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    workspace = run_request.record_dir / "workspace"
    workspace.mkdir()
    for name in ("train.py", ".encap.conf", ".encap_history.conf"):
        (workspace / name).write_bytes((run_request.run_folder / name).read_bytes())
    request = replace(run_request, workspace_folder=workspace)
    config["i"] = instances
    backend = DataprocBackend(config)
    backend._check_buckets = Mock(side_effect=BackendError("bucket failure") if failure_stage == "bucket" else None)
    backend._command = Mock(side_effect=BackendError("upload failure") if failure_stage == "upload" else None)
    backend._api = Mock(side_effect=MonitoringError("response lost") if failure_stage == "submission" else None,
                        return_value={"name": backend._parent(backend._new_handle(request).data) + "/operations/123"})
    if failure_stage:
        with pytest.raises(BackendError):
            backend.submit(request, writer)
    else:
        backend.submit(request, writer)
    assert not workspace.exists()
    assert (run_request.run_folder / "train.py").is_file()
    assert writer.path.exists()
    assert (writer.path.parent / "bucket_location").exists() == (failure_stage in (None, "submission"))
    if failure_stage is None:
        assert len((writer.path.parent / "bucket_location").read_text().splitlines()) == instances


def test_bucket_location_write_failure_prevents_workflow(config, run_request, monkeypatch):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    backend = DataprocBackend(config)
    backend._command = Mock()
    backend._api = Mock()
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    (writer.path.parent / "bucket_location").mkdir()
    with pytest.raises(BackendError, match="Cannot save bucket location"):
        backend.submit(run_request, writer)
    backend._api.assert_not_called()


def test_workspace_cleanup_rejects_unrelated_directory(config, run_request, tmp_path):
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    (unrelated / "keep").touch()
    with pytest.raises(ConfigurationError, match="run's own"):
        DataprocBackend(config).submit(replace(run_request, workspace_folder=unrelated), Mock())
    assert (unrelated / "keep").exists()


def test_input_only_archive_adds_virtual_parent_without_changing_core(run_request, tmp_path):
    path = tmp_path / "payload.zip"
    experiment = create_archive(run_request, path, {"../data": "gs://input-bucket/files/"})
    assert experiment == "workspace"
    with zipfile.ZipFile(path) as archive:
        assert "workspace/train.py" in archive.namelist()
        assert "workspace/.encap.conf" in archive.namelist()


def test_gcs_input_collision_fails_before_upload(config, run_request, monkeypatch):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    config["inputs"] = {"train.py": "gs://input-bucket/file.py"}
    backend = DataprocBackend(config)
    backend._check_buckets = Mock()
    backend._command = Mock()
    backend._api = Mock()
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    with pytest.raises(ConfigurationError, match="collides"):
        backend.submit(run_request, writer)
    backend._command.assert_not_called()
    backend._api.assert_not_called()


def test_input_mapping_is_sent_to_worker_with_rebased_layout(config, run_request, monkeypatch):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    config["inputs"] = {"../data": "gs://input-bucket/tables/"}
    backend = DataprocBackend(config)
    def command(arguments, stream=False):
        if stream:
            worker_file = next(Path(arg) for arg in arguments if arg.endswith("/request.json"))
            worker = json.loads(worker_file.read_text())
            assert worker["inputs"] == config["inputs"]
            assert worker["experiment_path"] == "workspace"
            assert any(arg.endswith("/inputs.py") for arg in arguments)
            archive = next(Path(arg) for arg in arguments if arg.endswith("/payload.zip"))
            with zipfile.ZipFile(archive) as payload:
                assert "workspace/train.py" in payload.namelist()
        return Mock(stdout="")
    backend._command = Mock(side_effect=command)
    backend._api = Mock(return_value={
        "name": "projects/test-project/regions/europe-west1/operations/123",
    })
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    backend.submit(run_request, writer)
    assert backend._command.call_count == 3
    assert backend._api.call_args.args[2]["jobs"][0]["pysparkJob"]["pythonFileUris"][-1].endswith("/inputs.py")


def grouped(handle):
    return RunHandle({"instances": [
        {"instance": instance, "handle": asdict(handle), "submission_phase": "submitted"}
        for instance in (0, 2)
    ]}, version=2)


@pytest.mark.parametrize("states,expected", [
    (["DONE", "DONE"], "succeeded"), (["DONE", "RUNNING"], "running"),
    (["ERROR", "RUNNING"], "running"), (["DONE", "ERROR"], "failed"),
    (["DONE", "CANCELLED"], "cancelled"), (["DONE", "UNRECOGNIZED"], "unknown"),
])
def test_group_status_waits_for_all_instances(handle, states, expected):
    backend = DataprocBackend({})
    backend._operation = Mock(return_value={
        "done": True, "metadata": {"graph": {"nodes": [{"jobId": "job"}]}},
    })
    backend._api = Mock(side_effect=[{"status": {"state": state}} for state in states])
    backend._result = Mock(return_value={"state": "succeeded", "exit_code": 0, "outputs_finalized": True})
    status = backend.status(grouped(handle))
    assert status.state == expected
    assert set(status.details["instances"]) == {"0", "2"}
    assert status.exit_code == (0 if expected == "succeeded" else 130 if expected == "cancelled" else None)


@pytest.mark.parametrize("iap", [False, True])
def test_ssh_discovers_actual_master_and_inherits_terminal(handle, monkeypatch, iap):
    backend = DataprocBackend({})
    backend._operation = Mock(return_value={"metadata": {"clusterName": "actual-generated-cluster"}})
    backend._api = Mock(return_value={"config": {
        "masterConfig": {"instanceNames": ["actual-master"]},
        "gceClusterConfig": {
            "zoneUri": "https://www.googleapis.com/compute/v1/projects/saved-project/zones/europe-west1-b",
        },
    }})
    command = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", command)
    assert registration.supports_ssh
    backend.ssh(handle, tunnel_through_iap=iap)
    backend._operation.assert_called_once_with(handle)
    backend._api.assert_called_once_with(
        "GET", "projects/saved-project/regions/saved-region/clusters/actual-generated-cluster",
    )
    command.assert_called_once_with([
        "gcloud", "compute", "ssh", "actual-master", "--project", "saved-project",
        "--zone", "europe-west1-b", *(["--tunnel-through-iap"] if iap else []),
    ], check=True)


@pytest.mark.parametrize("instance", [None, 0, 2, 7])
def test_ssh_group_requires_an_explicit_existing_instance(handle, monkeypatch, instance):
    backend = DataprocBackend({})
    group = grouped(handle)
    for child in group.data["instances"]:
        child["handle"]["data"]["instance"] = child["instance"]
    backend._operation = Mock(return_value={"metadata": {"clusterName": "actual-cluster"}})
    backend._api = Mock(return_value={"config": {
        "masterConfig": {"instanceNames": ["actual-master"]},
        "gceClusterConfig": {"zoneUri": "projects/saved-project/zones/europe-west1-b"},
    }})
    command = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", command)
    if instance in (0, 2):
        backend.ssh(group, instance=instance, tunnel_through_iap=True)
        backend._operation.assert_called_once_with(RunHandle(**group.data["instances"][instance // 2]["handle"]))
        assert command.call_args.args[0][-1] == "--tunnel-through-iap"
    else:
        with pytest.raises(ConfigurationError, match="available IDs"):
            backend.ssh(group, instance=instance)
        backend._operation.assert_not_called()
        command.assert_not_called()


@pytest.mark.parametrize("instance", [-1, True, "0", 2])
def test_ssh_rejects_invalid_single_instance(handle, instance):
    backend = DataprocBackend({})
    backend._operation = Mock()
    with pytest.raises(ConfigurationError):
        backend.ssh(handle, instance=instance)
    backend._operation.assert_not_called()


def test_ssh_never_submits_a_prepared_group_instance(handle):
    backend = DataprocBackend({})
    group = grouped(handle)
    group.data["instances"][0]["submission_phase"] = "prepared"
    backend._operation = Mock()
    with pytest.raises(MonitoringError, match="not submitted"):
        backend.ssh(group, instance=0)
    backend._operation.assert_not_called()


@pytest.mark.parametrize("operation", [
    {"done": True, "metadata": {"clusterName": "deleted-cluster"}},
    {"metadata": {}},
])
def test_ssh_reports_finished_or_unassigned_cluster(handle, monkeypatch, operation):
    backend = DataprocBackend({})
    backend._operation = Mock(return_value=operation)
    backend._api = Mock()
    command = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", command)
    with pytest.raises(MonitoringError):
        backend.ssh(handle)
    backend._api.assert_not_called()
    command.assert_not_called()


@pytest.mark.parametrize("description", [
    {},
    {"config": {"masterConfig": {"instanceNames": []}}},
    {"config": {"masterConfig": {"instanceNames": ["master"]}, "gceClusterConfig": {"zoneUri": ""}}},
    {"config": {"masterConfig": {"instanceNames": ["--bad"]},
                "gceClusterConfig": {"zoneUri": "projects/p/zones/europe-west1-b"}}},
])
def test_ssh_reports_missing_or_invalid_master_metadata(handle, monkeypatch, description):
    backend = DataprocBackend({})
    backend._operation = Mock(return_value={"metadata": {"clusterName": "cluster"}})
    backend._api = Mock(return_value=description)
    command = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", command)
    with pytest.raises(MonitoringError, match="master instance or zone"):
        backend.ssh(handle)
    command.assert_not_called()


@pytest.mark.parametrize("error", [
    OSError("gcloud not found"), subprocess.CalledProcessError(255, ["gcloud"]), KeyboardInterrupt(),
])
def test_ssh_errors_and_interrupts_never_cancel(handle, monkeypatch, error):
    backend = DataprocBackend({})
    backend._operation = Mock(return_value={"metadata": {"clusterName": "cluster"}})
    backend._api = Mock(return_value={"config": {
        "masterConfig": {"instanceNames": ["master"]},
        "gceClusterConfig": {"zoneUri": "projects/p/zones/europe-west1-b"},
    }})
    backend.cancel = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", Mock(side_effect=error))
    with pytest.raises(KeyboardInterrupt if isinstance(error, KeyboardInterrupt) else BackendError):
        backend.ssh(handle)
    backend.cancel.assert_not_called()


def test_group_management_visits_every_instance(handle, tmp_path, monkeypatch):
    backend = DataprocBackend({})
    group = grouped(handle)
    backend._operation = Mock(return_value={"metadata": {"graph": {"nodes": [{"jobId": "job"}]}}})
    backend._api = Mock()
    backend.status = Mock(return_value=backends.RunStatus("succeeded", 0, outputs_finalized=True))
    execute = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    backend.tail(group)
    execute.assert_called_once()
    backend._command = Mock(return_value=Mock(stdout="gs://saved-bucket/trial/outputs/log\n"))
    backend.pull(group, tmp_path)
    syncs = [call.args[0] for call in backend._command.call_args_list if call.args[0][2] == "rsync"]
    assert len(syncs) == 2
    assert all(sync[4] == str(tmp_path) for sync in syncs)
    backend.cancel(group)
    assert backend._api.call_count == 2


@pytest.fixture
def group_logs(handle):
    backend = DataprocBackend({})
    group = grouped(handle)
    for child in group.data["instances"]:
        child["handle"]["data"]["instance"] = child["instance"]
    backend._operation = Mock(side_effect=lambda saved: {
        "metadata": {"graph": {"nodes": [{"jobId": f"job-{saved.data['instance']}"}]}},
    })
    backend.status = Mock(return_value=backends.RunStatus("succeeded", 0, outputs_finalized=True))
    backend.cancel = Mock()
    return backend, group


@pytest.mark.parametrize("selection", [None, 0, 2])
def test_group_tail_follows_only_first_instance_of_current_launch(group_logs, monkeypatch, capsys, selection):
    backend, group = group_logs
    if selection is not None:
        group.data["log_instance"] = selection
    instance = 0 if selection is None else selection
    execute = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr("encap_dataproc.backend.subprocess.Popen", Mock(
        side_effect=AssertionError("Must not multiplex instance logs"),
    ))
    backend.tail(group)
    execute.assert_called_once_with([
        "gcloud", "dataproc", "jobs", "wait", f"job-{instance}",
        "--project", "saved-project", "--region", "saved-region",
    ], check=True)
    assert backend._operation.call_args.args[0].data["instance"] == instance
    output = capsys.readouterr().out
    assert f"Instance {instance}" in output
    assert f"Instance {2 if instance == 0 else 0}" not in output
    assert "[Instance" not in output
    backend.status.assert_called_once_with(group)
    backend.cancel.assert_not_called()


def test_group_tail_waits_for_other_instances_without_attaching_logs(group_logs, monkeypatch, capsys):
    backend, group = group_logs
    backend.status.side_effect = [
        backends.RunStatus("running"),
        backends.RunStatus("succeeded", 0, outputs_finalized=True),
    ]
    execute = Mock()
    sleep = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr("encap_lib.backends.time.sleep", sleep)
    backend.tail(group)
    execute.assert_called_once()
    assert backend.status.call_count == 2
    sleep.assert_called_once_with(backends.POLL_INTERVAL)
    output = capsys.readouterr().out
    assert "no other instance logs will be shown" in output
    assert "Instance 2" not in output


@pytest.mark.parametrize("state,code", [("failed", 9), ("cancelled", 130), ("unknown", None)])
def test_group_tail_reports_other_instance_failure_without_streaming_it(group_logs, monkeypatch, state, code):
    backend, group = group_logs
    backend.status.return_value = backends.RunStatus(
        state, code, details={"instances": {"2": {"state": state, "exit_code": code}}},
    )
    execute = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    with pytest.raises(BackendError, match=state):
        backend.tail(group)
    execute.assert_called_once()
    assert backend._operation.call_count == 1
    backend.cancel.assert_not_called()


@pytest.mark.parametrize("error", [
    OSError("gcloud unavailable"), subprocess.CalledProcessError(1, ["gcloud"]), KeyboardInterrupt(),
])
def test_group_tail_attachment_error_never_switches_instances(group_logs, monkeypatch, error):
    backend, group = group_logs
    execute = Mock(side_effect=error)
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    with pytest.raises(KeyboardInterrupt if isinstance(error, KeyboardInterrupt) else BackendError):
        backend.tail(group)
    execute.assert_called_once()
    assert backend._operation.call_count == 1
    backend.status.assert_not_called()
    backend.cancel.assert_not_called()


def test_group_tail_interrupt_during_quiet_wait_does_not_cancel(group_logs, monkeypatch):
    backend, group = group_logs
    backend.status.side_effect = KeyboardInterrupt
    execute = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    with pytest.raises(KeyboardInterrupt):
        backend.tail(group)
    execute.assert_called_once()
    backend.cancel.assert_not_called()


@pytest.mark.parametrize("selection", [None, True, -1, 9, "2"])
def test_group_tail_rejects_invalid_saved_selection(group_logs, monkeypatch, selection):
    backend, group = group_logs
    group.data["log_instance"] = selection
    execute = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    with pytest.raises(MonitoringError, match=r"[Ll]og instance"):
        backend.tail(group)
    execute.assert_not_called()
    backend._operation.assert_not_called()


def test_group_tail_validates_all_submissions_before_attaching(group_logs, monkeypatch):
    backend, group = group_logs
    group.data["instances"][1]["submission_phase"] = "prepared"
    launch = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", launch)
    with pytest.raises(MonitoringError, match="Instance 2 was not submitted"):
        backend.tail(group)
    launch.assert_not_called()
    backend._operation.assert_not_called()


def test_single_child_group_retains_unprefixed_attachment(group_logs, monkeypatch, capsys):
    backend, group = group_logs
    group.data["instances"] = group.data["instances"][:1]
    execute = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    backend.tail(group)
    execute.assert_called_once_with([
        "gcloud", "dataproc", "jobs", "wait", "job-0",
        "--project", "saved-project", "--region", "saved-region",
    ], check=True)
    output = capsys.readouterr().out
    assert "Instance 0" in output
    assert "[Instance" not in output
    backend.status.assert_not_called()


def test_group_status_links_to_each_instances_actual_job(handle):
    backend = DataprocBackend({})
    group = grouped(handle)
    backend._operation = Mock(side_effect=[
        {"metadata": {"graph": {"nodes": [{"jobId": f"experiment-trial-{instance}"}]}}}
        for instance in (0, 2)
    ])
    backend._api = Mock(return_value={"status": {"state": "RUNNING"}})
    status = backend.status(group)
    for child in group.data["instances"]:
        instance = child["instance"]
        assert child["handle"]["data"]["job_id"] == f"experiment-trial-{instance}"
        assert status.details["instances"][str(instance)]["details"]["links"]["jobs"] == (
            f"https://console.cloud.google.com/dataproc/jobs/experiment-trial-{instance}/summary"
            "?region=saved-region&project=saved-project"
        )


def test_group_cancel_attempts_remaining_instances_after_failure(handle):
    backend = DataprocBackend({})
    backend._operation = Mock()
    backend._api = Mock(side_effect=[MonitoringError("offline"), {}])
    with pytest.raises(BackendError, match="Instance 0: offline"):
        backend.cancel(grouped(handle))
    assert backend._api.call_count == 2


def test_unsubmitted_group_instance_is_unknown_and_never_retried(handle):
    backend = DataprocBackend({})
    group = grouped(handle)
    for child in group.data["instances"]:
        child["submission_phase"] = "prepared"
    backend._operation = Mock()
    backend._api = Mock()
    assert backend.status(group).state == "unknown"
    with pytest.raises(MonitoringError, match="was not submitted"):
        backend.tail(group)
    backend.cancel(group)
    backend._operation.assert_not_called()
    backend._api.assert_not_called()


@pytest.mark.parametrize("saved", [{}, {"data": {}, "version": 1, "extra": True}, {"data": [], "version": 1}])
def test_group_rejects_invalid_nested_handle(handle, saved):
    backend = DataprocBackend({})
    group = grouped(handle)
    group.data["instances"][0]["handle"] = saved
    with pytest.raises(BackendError, match="recovery record"):
        backend.status(group)


def streamed_process(output="", code=0):
    process = Mock(stdout=io.StringIO(output))
    process.wait.return_value = code
    process.__enter__ = Mock(return_value=process)
    process.__exit__ = Mock(return_value=False)
    return process


def test_upload_progress_streams_but_authentication_is_captured(monkeypatch, capsys):
    execute = Mock(return_value=Mock(stdout="private-token"))
    launch = Mock(return_value=streamed_process("upload progress\n"))
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr("encap_dataproc.backend.subprocess.Popen", launch)
    backend = DataprocBackend({})
    command = ["gcloud", "storage", "cp", "two words.zip", "gs://bucket/source/"]
    backend._command(command, stream=True)
    launch.assert_called_once_with(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    execute.assert_not_called()
    backend._command(["gcloud", "auth", "print-access-token"])
    execute.assert_called_with(["gcloud", "auth", "print-access-token"], check=True, capture_output=True, text=True)
    output = capsys.readouterr().out
    assert "gcloud storage cp 'two words.zip'" in output
    assert "upload progress" in output
    assert "private-token" not in output
    assert "print-access-token" not in output


def test_captured_reauthentication_runs_login_and_retries_once(monkeypatch, capsys):
    command = ["gcloud", "auth", "print-access-token"]
    execute = Mock(side_effect=[
        subprocess.CalledProcessError(1, command, stderr="Reauthentication required.\n"),
        subprocess.CompletedProcess(["gcloud", "auth", "login"], 0),
        subprocess.CompletedProcess(command, 0, stdout="private-token"),
    ])
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    assert DataprocBackend._command(command).stdout == "private-token"
    assert [call.args[0] for call in execute.call_args_list] == [
        command, ["gcloud", "auth", "login"], command,
    ]
    assert execute.call_args_list[1].kwargs == {"check": True}
    output = capsys.readouterr()
    assert "gcloud auth login" in output.err
    assert "private-token" not in output.out + output.err


def test_streamed_reauthentication_preserves_progress_and_retries(monkeypatch, capsys):
    command = ["gcloud", "storage", "cp", "payload.zip", "gs://bucket/source/"]
    launch = Mock(side_effect=[
        streamed_process("Reauthentication required.\n" + "progress\n" * 150, 1),
        streamed_process("upload complete\n"),
    ])
    login = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.Popen", launch)
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", login)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    result = DataprocBackend._command(command, stream=True)
    assert result.returncode == 0
    assert launch.call_count == 2
    login.assert_called_once_with(["gcloud", "auth", "login"], check=True)
    output = capsys.readouterr()
    assert "progress" in output.out
    assert "upload complete" in output.out


def test_streamed_reauthentication_with_real_subprocess(tmp_path, monkeypatch, capsys):
    marker = tmp_path / "first-attempt"
    command = [
        sys.executable, "-u", "-c",
        "import sys\nfrom pathlib import Path\n"
        "marker = Path(sys.argv[1])\n"
        "if not marker.exists():\n"
        "    marker.touch()\n"
        "    print('Reauthentication required.', file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "print('upload complete')\n",
        str(marker),
    ]
    login = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", login)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    assert DataprocBackend._command(command, stream=True).returncode == 0
    login.assert_called_once_with(["gcloud", "auth", "login"], check=True)
    assert "upload complete" in capsys.readouterr().out


@pytest.mark.parametrize("stream", [False, True])
def test_repeated_reauthentication_does_not_loop(monkeypatch, stream):
    command = ["gcloud", "storage", "ls", "gs://bucket/"]
    error = subprocess.CalledProcessError(1, command, stderr="Reauthentication required.")
    execute = Mock(side_effect=[error, subprocess.CompletedProcess(command, 0), error] if not stream else None)
    launch = Mock(side_effect=[
        streamed_process("Reauthentication required.\n", 1),
        streamed_process("Reauthentication required.\n", 1),
    ])
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr("encap_dataproc.backend.subprocess.Popen", launch)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    with pytest.raises(BackendError, match="Reauthentication required"):
        DataprocBackend._command(command, stream=stream)
    assert sum(call.args[0] == ["gcloud", "auth", "login"] for call in execute.call_args_list) == 1


@pytest.mark.parametrize("login_error", [
    subprocess.CalledProcessError(1, ["gcloud", "auth", "login"]),
    OSError("cannot start login"),
])
def test_reauthentication_login_failure_is_explicit(monkeypatch, login_error):
    command = ["gcloud", "auth", "print-access-token"]
    execute = Mock(side_effect=[
        subprocess.CalledProcessError(1, command, stderr="Reauthentication required."),
        login_error,
    ])
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    with pytest.raises(BackendError, match="reauthentication failed"):
        DataprocBackend._command(command)
    assert execute.call_count == 2


def test_noninteractive_reauthentication_gives_manual_login_instruction(monkeypatch):
    command = ["gcloud", "auth", "print-access-token"]
    execute = Mock(side_effect=subprocess.CalledProcessError(1, command, stderr="Reauthentication required."))
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    with pytest.raises(BackendError, match="gcloud auth login.*interactive terminal"):
        DataprocBackend._command(command)
    assert execute.call_count == 1


@pytest.mark.parametrize("stream", [False, True])
def test_unrelated_gcloud_errors_never_launch_login(monkeypatch, stream):
    command = ["gcloud", "storage", "ls", "gs://bucket/"]
    execute = Mock(side_effect=subprocess.CalledProcessError(1, command, stderr="Permission denied"))
    launch = Mock(return_value=streamed_process("Permission denied\n", 1))
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr("encap_dataproc.backend.subprocess.Popen", launch)
    with pytest.raises(BackendError, match="Permission denied"):
        DataprocBackend._command(command, stream=stream)
    assert execute.call_count == (0 if stream else 1)
    assert launch.call_count == (1 if stream else 0)


def test_submit_reauthentication_retries_upload_not_workflow(config, run_request, monkeypatch):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    launch = Mock(side_effect=[
        streamed_process("Reauthentication required.\n", 1),
        streamed_process("upload complete\n"),
    ])
    monkeypatch.setattr("encap_dataproc.backend.subprocess.Popen", launch)
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    def login(command, check):
        assert command == ["gcloud", "auth", "login"]
        assert check
        assert writer.read().get("submission_phase") != "request-started"
    execute = Mock(side_effect=login)
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    backend = DataprocBackend(config)
    backend._check_buckets = Mock()
    backend._api = Mock(return_value={
        "name": "projects/test-project/regions/europe-west1/operations/123",
    })
    backend.submit(run_request, writer)
    assert writer.read()["state"] == "submitted"
    assert launch.call_count == 2
    assert launch.call_args_list[0].args == launch.call_args_list[1].args
    assert "--if-generation-match=0" in launch.call_args.args[0]
    assert execute.call_count == 1
    backend._api.assert_called_once()


def test_tail_shows_saved_context_wait_and_completion(handle, monkeypatch, capsys):
    handle.data["settings"] = {"console_authuser": "user+test@example.com", "machine_type": "n4-standard-4"}
    backend = DataprocBackend({})
    backend._operation = Mock(side_effect=[
        {"metadata": {"state": "RUNNING"}},
        {"metadata": {"graph": {"nodes": [{"jobId": "job"}]}}},
    ])
    monkeypatch.setattr("encap_dataproc.backend.time.sleep", Mock())
    execute = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    backend.tail(handle)
    execute.assert_called_once_with([
        "gcloud", "dataproc", "jobs", "wait", "job", "--project", "saved-project",
        "--region", "saved-region",
    ], check=True)
    output = capsys.readouterr().out
    assert "Run: gs://saved-bucket/trial" in output
    assert "authuser=user%2Btest%40example.com" in output
    assert (
        f"Workflow operation: {handle.data['operation']}\n\n"
        "Waiting for the workflow's job; Ctrl-C detaches without cancelling it."
    ) in output
    assert "gcloud dataproc jobs wait job" in output
    assert (
        "jobs: https://console.cloud.google.com/dataproc/jobs/job/summary"
        "?region=saved-region&authuser=user%2Btest%40example.com&project=saved-project"
    ) in output
    assert handle.data["job_id"] == "job"
    assert "Job finished. GCP manages the remaining workflow cleanup." in output


def test_startup_failure_exposes_capacity_error_and_cleanup_in_tail_and_status(handle):
    backend = DataprocBackend({})
    operation = {
        "done": True, "error": {"code": 10, "message": "Failed to create cluster"},
        "metadata": {
            "createCluster": {"done": True, "error": "UNAVAILABLE: europe-west1-b does not have enough resources\n"},
            "deleteCluster": {"done": True},
            "graph": {"nodes": [{"stepId": "computation", "state": "RUNNABLE"}]},
        },
    }
    backend._operation = Mock(return_value=operation)
    with pytest.raises(MonitoringError) as result:
        backend.tail(handle)
    message = str(result.value)
    assert "Failed to create cluster" in message
    assert "europe-west1-b does not have enough resources" in message
    assert "Managed cluster cleanup completed." in message
    assert "https://console.cloud.google.com/dataproc/workflows?project=saved-project" in message
    status = backend.status(handle)
    assert status.state == "failed"
    assert status.details["cluster_creation"] == operation["metadata"]["createCluster"]
    assert "workflows" in status.details["links"]


def test_startup_failure_does_not_hide_cleanup_error(handle):
    backend = DataprocBackend({})
    backend._operation = Mock(return_value={
        "done": True, "error": {"message": "Failed to create cluster"},
        "metadata": {"deleteCluster": {"error": "permission denied"}},
    })
    with pytest.raises(MonitoringError, match="Cluster cleanup failed: permission denied"):
        backend.tail(handle)


def test_disconnect_during_submission_never_retries(config, run_request, monkeypatch):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    backend = DataprocBackend(config)
    backend._command = Mock()
    backend._api = Mock(side_effect=MonitoringError("network response lost"))
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    with pytest.raises(MonitoringError):
        backend.submit(run_request, writer)
    saved = writer.read()
    assert saved["state"] == "submission-unconfirmed"
    assert saved["handle"]["data"]["request_id"]
    assert backend._api.call_count == 1


def test_uncertain_submission_reconciliation_uses_saved_prefix(handle):
    del handle.data["operation"]
    backend = DataprocBackend({})
    backend._api = Mock(side_effect=[
        {"operations": [{"name": "unrelated", "metadata": {"clusterName": "other"}}], "nextPageToken": "next"},
        {"operations": [{"name": "projects/saved-project/regions/saved-region/operations/123",
                         "metadata": {"clusterName": "encap-unique-generated"}}]},
    ])
    operation = backend._operation(handle)
    assert operation["name"] == handle.data["operation"]
    assert all(call.args[0] == "GET" for call in backend._api.call_args_list)
    assert backend._api.call_args.kwargs["parameters"]["pageToken"] == "next"


def test_shared_cluster_prefix_reconciliation_uses_request_label(config, run_request):
    backend = DataprocBackend(config)
    handle = backend._new_handle(run_request)
    parent = backend._parent(handle.data)
    other = {
        "name": parent + "/operations/other",
        "metadata": {
            "clusterName": "encap-cluster-other",
            "graph": {"nodes": [{"stepId": "experiment-trial", "jobId": "other-job"}]},
        },
    }
    own = {
        "name": parent + "/operations/own",
        "metadata": {
            "clusterName": "encap-cluster-own",
            "graph": {"nodes": [{"stepId": "experiment-trial", "jobId": "own-job"}]},
        },
    }
    backend._api = Mock(side_effect=[
        {"operations": [other], "nextPageToken": "next"},
        {"labels": {"encap-request-id": "other-request"}},
        {"operations": [own]},
        {"labels": {"encap-request-id": handle.data["request_id"]}},
    ])
    assert backend._operation(handle) == own
    assert handle.data["operation"] == own["name"]
    calls = backend._api.call_args_list
    assert all(call.args[0] == "GET" for call in calls)
    assert calls[1].args[1] == parent + "/jobs/other-job"
    assert calls[2].kwargs["parameters"]["pageToken"] == "next"
    assert calls[3].args[1] == parent + "/jobs/own-job"


@pytest.mark.parametrize("node", [
    {},
    {"stepId": "experiment-trial"},
    {"stepId": "experiment-other", "jobId": "other-job"},
    {"stepId": "experiment-trial", "jobId": "unlabelled-job"},
])
def test_shared_prefix_without_request_label_never_confirms(config, run_request, node):
    backend = DataprocBackend(config)
    handle = backend._new_handle(run_request)
    backend._api = Mock(side_effect=[
        {"operations": [{
            "name": backend._parent(handle.data) + "/operations/other",
            "metadata": {"clusterName": "encap-cluster-other", "graph": {"nodes": [node]}},
        }]},
        {},
    ])
    with pytest.raises(MonitoringError, match="0 matching workflows"):
        backend._operation(handle)
    assert "operation" not in handle.data
    assert all(call.args[0] == "GET" for call in backend._api.call_args_list)


def test_reconciliation_job_lookup_error_is_explicit(config, run_request):
    backend = DataprocBackend(config)
    handle = backend._new_handle(run_request)
    backend._api = Mock(side_effect=[
        {"operations": [{
            "metadata": {
                "clusterName": "encap-cluster-other",
                "graph": {"nodes": [{"stepId": "experiment-trial", "jobId": "job"}]},
            },
        }]},
        MonitoringError("job lookup failed"),
    ])
    with pytest.raises(MonitoringError, match="job lookup failed"):
        backend._operation(handle)
    assert "operation" not in handle.data


@pytest.mark.parametrize("step_id", [None, 42, "", "a", "-invalid", "a" * 51, "invalid.name"])
def test_invalid_saved_job_step_id(handle, step_id):
    handle.data["job_step_id"] = step_id
    with pytest.raises(BackendError, match="job step ID"):
        DataprocBackend({})._data(handle)


def test_missing_reconciliation_match_is_not_rejection(handle):
    del handle.data["operation"]
    backend = DataprocBackend({})
    backend._api = Mock(return_value={"operations": []})
    with pytest.raises(MonitoringError, match="Do not resubmit"):
        backend._operation(handle)
    assert backend._api.call_count == 1


@pytest.mark.parametrize("job_state,result,expected,code,finalized", [
    ("DONE", {"state": "succeeded", "exit_code": 0, "outputs_finalized": True}, "succeeded", 0, True),
    ("ERROR", {"state": "failed", "exit_code": 7, "outputs_finalized": True}, "failed", 7, True),
    ("ERROR", {"state": "failed", "exit_code": 0, "outputs_finalized": False,
               "upload_error": "not saved"}, "failed", 0, False),
    ("ERROR", None, "failed", None, False),
    ("CANCELLED", None, "cancelled", 130, False),
    ("RUNNING", None, "running", None, None),
    ("STATE_UNSPECIFIED", None, "unknown", None, None),
])
def test_status_distinguishes_computation_finalization_cleanup(handle, job_state, result, expected, code, finalized):
    backend = DataprocBackend({})
    backend._api = Mock(side_effect=[
        {"metadata": {"graph": {"nodes": [{"jobId": "job"}]}, "deleteCluster": {"done": False}}},
        {"status": {"state": job_state}},
    ])
    backend._result = Mock(return_value=result)
    status = backend.status(handle)
    assert status.state == expected
    assert status.exit_code == code
    assert status.outputs_finalized == finalized
    assert status.cleanup_pending is True
    assert handle.data["job_id"] == "job"
    assert status.details["links"]["jobs"] == (
        "https://console.cloud.google.com/dataproc/jobs/job/summary"
        "?region=saved-region&project=saved-project"
    )
    assert backend._api.call_args.args[1].startswith("projects/saved-project/regions/saved-region/")


def test_job_done_without_output_marker_cannot_succeed(handle):
    backend = DataprocBackend({})
    backend._api = Mock(side_effect=[
        {"done": True, "metadata": {"graph": {"nodes": [{"jobId": "job"}]}}},
        {"status": {"state": "DONE"}},
    ])
    backend._result = Mock(return_value=None)
    with pytest.raises(MonitoringError, match="finalization"):
        backend.status(handle)


@pytest.fixture
def remote_pull(handle, monkeypatch):
    files = {
        "model.pkl": b"model", "models/other.pkl": b"other model",
        "data.csv": b"required data", "nested/empty": b"",
        "manifest.json": b'{"files":["model.pkl","models/other.pkl","data.csv","nested/empty"]}',
        ".encap-run/remote.json": b"remote recovery",
    }

    def inventory(store, prefix):
        return [
            {"name": prefix + name, "generation": str(abs(hash(content)) + 1), "size": str(len(content))}
            for name, content in sorted(files.items())
        ]

    monkeypatch.setattr("encap_dataproc.backend.GCSStore.objects", inventory)
    backend = DataprocBackend({})

    def command(arguments):
        assert arguments[:2] == ["gcloud", "storage"]
        assert "--delete-unmatched-destination-objects" not in arguments
        if arguments[2] == "ls":
            return Mock(stdout="".join(arguments[3] + name + "\n" for name in files))
        if arguments[2] == "cp":
            name = arguments[3].removeprefix(handle.data["output_uri"] + "/")
            Path(arguments[4]).write_bytes(files[name])
            return Mock(stdout="")
        assert arguments[2] == "rsync"
        regex = arguments[arguments.index("--exclude") + 1]
        for name, content in files.items():
            if not re.match(regex, name):
                path = Path(arguments[4]) / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
        return Mock(stdout="")

    backend._command = Mock(side_effect=command)
    handle.data["rsync_exclude_pull"] = ["*.pkl"]
    return backend, files


def test_dataproc_records_shared_pull_policy_without_polluting_provider_config(config, run_request):
    namespace = PluginConfig(config)
    namespace.rsync_exclude_pull = ["models/", "*.pkl"]
    backend = DataprocBackend(namespace)
    handle = backend._new_handle(run_request)
    assert handle.data["rsync_exclude_pull"] == ["models/", "*.pkl"]
    assert "rsync_exclude_pull" not in handle.data["settings"]
    backend.pull_exclusions.patterns.append("scratch/")
    assert handle.data["rsync_exclude_pull"] == ["models/", "*.pkl"]


def test_selective_pull_preserves_remote_files_manifests_and_local_recovery(
    remote_pull, handle, tmp_path, capsys,
):
    backend, files = remote_pull
    remote = dict(files)
    destination = tmp_path / "local"
    writer = RecordWriter.reserve(destination, registration, "trial")
    writer.save_handle(handle, confirmed=True)
    recovery = writer.path.read_bytes()
    (destination / "model.pkl").write_bytes(b"existing local model")
    backend.pull(handle, destination)
    assert files == remote
    assert writer.path.read_bytes() == recovery
    assert (destination / "model.pkl").read_bytes() == b"existing local model"
    assert not (destination / "models/other.pkl").exists()
    assert (destination / "data.csv").read_bytes() == files["data.csv"]
    assert (destination / "nested/empty").read_bytes() == b""
    assert (destination / "manifest.json").read_bytes() == files["manifest.json"]
    assert not (destination / ".encap-run/remote.json").exists()
    details = writer.read_retrieval()["sources"][handle.data["output_uri"] + "/"]
    assert details["state"] == "succeeded" and details["scope"] == "partial"
    assert {item["path"] for item in details["omitted"]} == {"model.pkl", "models/other.pkl"}
    assert {item["path"]: item["local_file_present"] for item in details["omitted"]} == {
        "model.pkl": True, "models/other.pkl": False,
    }
    assert all(item["generation"].isdigit() for item in details["omitted"])
    assert details["integrity_verified"] is False
    assert "Partial local copy" in capsys.readouterr().out


@pytest.mark.parametrize("override,omitted", [
    (None, {"model.pkl", "models/other.pkl"}),
    ([], set()),
    (["/models/"], {"models/other.pkl"}),
])
def test_pull_override_replaces_saved_policy_without_mutating_it(
    remote_pull, handle, tmp_path, override, omitted,
):
    original, _ = remote_pull
    namespace = PluginConfig()
    namespace.rsync_exclude_pull = override
    backend = DataprocBackend(namespace)
    backend._command = original._command
    backend.pull(handle, tmp_path)
    details = RecordWriter(tmp_path / ".encap-run").read_retrieval()["sources"][handle.data["output_uri"] + "/"]
    assert {item["path"] for item in details["omitted"]} == omitted
    assert handle.data["rsync_exclude_pull"] == ["*.pkl"]


def test_full_pull_after_failure_preserves_then_clears_previous_omissions(remote_pull, handle, tmp_path):
    backend, files = remote_pull
    backend.pull(handle, tmp_path)
    writer = RecordWriter(tmp_path / ".encap-run")
    source = handle.data["output_uri"] + "/"
    backend.pull_exclusions = PullExclusions([])
    command = backend._command.side_effect

    def fail_sync(arguments):
        if arguments[2] == "rsync":
            raise BackendError("download failed")
        return command(arguments)

    backend._command.side_effect = fail_sync
    with pytest.raises(BackendError, match="download failed"):
        backend.pull(handle, tmp_path)
    details = writer.read_retrieval()["sources"][source]
    assert details["state"] == "failed" and details["scope"] == "partial"
    assert {item["path"] for item in details["previous_omissions"]} == {"model.pkl", "models/other.pkl"}
    backend._command.side_effect = command
    backend.pull(handle, tmp_path)
    details = writer.read_retrieval()["sources"][source]
    assert details["state"] == "succeeded" and details["scope"] == "full"
    assert details["omitted"] == details["previous_omissions"] == []
    assert details["integrity_verified"] is False
    assert (tmp_path / "model.pkl").read_bytes() == files["model.pkl"]
    assert handle.data["rsync_exclude_pull"] == ["*.pkl"]


def test_all_excluded_outputs_are_reported_as_zero_transfer_partial_copy(remote_pull, handle, tmp_path):
    backend, files = remote_pull
    files.clear()
    files["model.pkl"] = b"keep remotely"
    backend.pull(handle, tmp_path)
    assert [call.args[0][2] for call in backend._command.call_args_list] == ["ls"]
    details = RecordWriter(tmp_path / ".encap-run").read_retrieval()["sources"][handle.data["output_uri"] + "/"]
    assert details["state"] == "succeeded" and details["scope"] == "partial"
    assert details["selected_count"] == 0
    assert details["omitted"][0]["path"] == "model.pkl"
    assert not (tmp_path / "model.pkl").exists()


@pytest.mark.parametrize("failure", ["missing", "truncated", "changed"])
def test_selected_output_failures_never_look_like_success(remote_pull, handle, tmp_path, failure):
    backend, files = remote_pull
    command = backend._command.side_effect

    def incomplete_sync(arguments):
        result = command(arguments)
        if arguments[2] == "rsync":
            if failure == "missing":
                (tmp_path / "data.csv").unlink()
            elif failure == "truncated":
                (tmp_path / "data.csv").write_bytes(b"")
            else:
                files["new.csv"] = b"written during the pull"
        return result

    backend._command.side_effect = incomplete_sync
    with pytest.raises(BackendError, match="not retrieved completely|changed during retrieval"):
        backend.pull(handle, tmp_path)
    details = RecordWriter(tmp_path / ".encap-run").read_retrieval()["sources"][handle.data["output_uri"] + "/"]
    assert details["state"] == "failed"
    assert details["integrity_verified"] is False


def test_live_sync_accepts_growing_logs_but_final_sync_remains_strict(remote_pull, handle, tmp_path):
    backend, files = remote_pull
    files["log"] = b"starting\n"
    command = backend._command.side_effect
    def growing(arguments):
        result = command(arguments)
        if arguments[2] == "rsync":
            files["log"] += b"more output\n"
            (tmp_path / "log").write_bytes(files["log"])
        return result
    backend._command.side_effect = growing
    backend.sync(handle, tmp_path)
    writer = RecordWriter(tmp_path / ".encap-run")
    details = writer.read_retrieval()["sources"][handle.data["output_uri"] + "/"]
    assert details["state"] == "succeeded"
    assert details["live"] and details["remote_stable"] is False
    assert details["integrity_verified"] is False
    assert not (tmp_path / "model.pkl").exists()
    with pytest.raises(BackendError, match="changed during retrieval|not retrieved completely"):
        backend.sync(handle, tmp_path, final=True)


def test_automatic_sync_preserves_saved_sources_and_local_edits(remote_pull, handle, tmp_path):
    backend, files = remote_pull
    destination = tmp_path / "local"
    files.update({
        "run.py": b"old source", ".encap.conf": b"old config",
        ".encap_history.conf": b"old history", "log": b"job log",
    })
    writer = RecordWriter.reserve(destination, registration, "trial")
    writer.update(preparation={"source_files": ["run.py"]})
    (destination / "run.py").write_bytes(b"edited source")
    (destination / ".encap.conf").write_bytes(b"edited config")
    (destination / ".encap_history.conf").write_bytes(b"new history")
    backend.sync(handle, destination)
    assert (destination / "run.py").read_bytes() == b"edited source"
    assert (destination / ".encap.conf").read_bytes() == b"edited config"
    assert (destination / ".encap_history.conf").read_bytes() == b"new history"
    assert (destination / "log").read_bytes() == b"job log"
    backend.sync(handle, destination, final=True)
    assert (destination / "run.py").read_bytes() == b"edited source"
    details = writer.read_retrieval()["sources"][handle.data["output_uri"] + "/"]
    protected = {item["path"] for item in details["omitted"] if item.get("reason") == "saved_source"}
    assert protected == {"run.py", ".encap.conf", ".encap_history.conf"}


def test_live_group_sync_deduplicates_shared_outputs_and_skips_unsubmitted_instances(
    remote_pull, handle, tmp_path,
):
    backend, _ = remote_pull
    group = grouped(handle)
    group.data["instances"].append({
        "instance": 3, "handle": asdict(handle), "submission_phase": "prepared",
    })
    backend.sync(group, tmp_path)
    commands = [call.args[0] for call in backend._command.call_args_list]
    assert sum(arguments[2] == "rsync" for arguments in commands) == 1
    assert sum(arguments[2] == "ls" for arguments in commands) == 1


@pytest.mark.parametrize("failure", ["missing", "truncated"])
def test_live_sync_still_reports_incomplete_downloads(remote_pull, handle, tmp_path, failure):
    backend, _ = remote_pull
    command = backend._command.side_effect
    def incomplete(arguments):
        result = command(arguments)
        if arguments[2] == "rsync":
            if failure == "missing":
                (tmp_path / "data.csv").unlink()
            else:
                (tmp_path / "data.csv").write_bytes(b"")
        return result
    backend._command.side_effect = incomplete
    with pytest.raises(BackendError, match="not retrieved completely"):
        backend.sync(handle, tmp_path)


def test_live_log_generation_race_retries_without_removing_local_log(remote_pull, handle, tmp_path):
    backend, files = remote_pull
    files["log_3"] = b"latest log\n"
    local = tmp_path / "log_3"
    local.write_bytes(b"previous log\n")
    command = backend._command.side_effect
    copies = []
    def race(arguments):
        if arguments[2] == "cp":
            copies.append(arguments)
            assert local.read_bytes() == b"previous log\n"
            if len(copies) == 1:
                raise BackendError("log_3#previous-generation not found: 404")
        return command(arguments)
    backend._command.side_effect = race
    backend.sync(handle, tmp_path)
    assert len(copies) == 2
    assert local.read_bytes() == b"latest log\n"
    assert not list((tmp_path / ".encap-run").glob("log-pull-*"))


@pytest.mark.parametrize("error", ["not found: 404", "permission denied"])
def test_failed_live_log_download_preserves_previous_snapshot(remote_pull, handle, tmp_path, error):
    backend, files = remote_pull
    files["log"] = b"remote log\n"
    local = tmp_path / "log"
    local.write_bytes(b"keep local snapshot\n")
    command = backend._command.side_effect
    def fail(arguments):
        if arguments[2] == "cp":
            Path(arguments[4]).write_bytes(b"partial")
            raise BackendError(error)
        return command(arguments)
    backend._command.side_effect = fail
    with pytest.raises(BackendError, match=error):
        backend.sync(handle, tmp_path)
    assert local.read_bytes() == b"keep local snapshot\n"
    assert not list((tmp_path / ".encap-run").glob("log-pull-*"))


def test_unchanged_live_logs_are_not_downloaded_again(remote_pull, handle, tmp_path):
    backend, files = remote_pull
    files["log"] = b"same log\n"
    backend.sync(handle, tmp_path)
    backend._command.reset_mock()
    backend.sync(handle, tmp_path)
    assert not any(call.args[0][2] == "cp" for call in backend._command.call_args_list)


def test_receipt_write_failure_stops_transfer_explicitly(remote_pull, handle, tmp_path, monkeypatch):
    backend, _ = remote_pull
    monkeypatch.setattr(RecordWriter, "_write", Mock(side_effect=OSError("disk unavailable")))
    with pytest.raises(BackendError, match="Cannot save retrieval"):
        backend.pull(handle, tmp_path)
    assert [call.args[0][2] for call in backend._command.call_args_list] == ["ls"]


def test_unmatched_pull_exclusions_are_visible(remote_pull, handle, tmp_path, capsys):
    backend, _ = remote_pull
    backend.pull_exclusions = PullExclusions(["never-there/"])
    backend.pull(handle, tmp_path)
    assert "matched no remote outputs" in capsys.readouterr().out


def test_filtered_pull_rejects_metadata_only_outputs(remote_pull, handle, tmp_path):
    backend, files = remote_pull
    files.clear()
    files[".encap-run/remote.json"] = b"recovery only"
    with pytest.raises(BackendError, match="No remote output files"):
        backend.pull(handle, tmp_path)
    assert not (tmp_path / ".encap-run/retrieval.json").exists()


@pytest.mark.parametrize("mirrored", [False, True])
def test_group_pulls_apply_the_same_policy_to_every_instance(remote_pull, handle, tmp_path, mirrored):
    backend, _ = remote_pull
    if mirrored:
        handle.data.update(output_uri=handle.data["run_uri"], layout="mirrored")
    backend.pull(grouped(handle), tmp_path)
    syncs = [call.args[0] for call in backend._command.call_args_list if call.args[0][2] == "rsync"]
    assert len(syncs) == 2
    for command in syncs:
        regex = command[command.index("--exclude") + 1]
        assert re.match(regex, "model.pkl")
        assert re.match(regex, "nested/model.pkl")
        assert re.match(regex, ".encap-run/control.json")
        assert not re.match(regex, "data.csv")


def test_cli_automatic_reattached_and_explicit_pulls_use_the_shared_policy(
    remote_pull, config, tmp_path, monkeypatch,
):
    backend, files = remote_pull
    loader = importlib.machinery.SourceFileLoader(
        "encap_selective_pull_cli", str(Path(backends.__file__).resolve().parents[1] / "encap"),
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "config", {"file_extension": {"py": "python -u"}})
    monkeypatch.setattr(settings, "args_config", {})
    monkeypatch.setattr(settings, "using_slurm", False)
    monkeypatch.setattr(settings, "using_pueue", False)
    monkeypatch.setattr(backends, "discover", lambda: {"dataproc": registration})
    monkeypatch.setattr(autopull, "watch", Mock(return_value={
        "enabled": True, "interval_seconds": 60, "pid": 123,
    }))
    monkeypatch.setattr(DataprocBackend, "_command", staticmethod(backend._command))
    monkeypatch.setattr(DataprocBackend, "tail", Mock())
    monkeypatch.setattr(DataprocBackend, "submit", lambda selected, request, writer: selected._new_handle(request))
    source = tmp_path / "experiment"
    source.mkdir()
    (source / "run.py").write_text("print('experiment')")
    settings.write_config_file(source / ".encap.conf", {
        "rsync_exclude_pull": ["*.pkl"],
        "rsync_exclude_push": ["*.csv"], "rsync_exclude": ["manifest.json"],
        "plugins": {"dataproc": dict(config)},
    })

    def invoke(mode, *options):
        monkeypatch.setattr(sys, "argv", ["encap", mode, "experiment", "-n", "trial", *options])
        module.main()

    invoke("run", "-gcp")
    destination = tmp_path / "0encap_folder/experiment/trial"
    writer = RecordWriter(destination / ".encap-run")
    saved = writer.read()["handle"]
    assert saved["data"]["rsync_exclude_pull"] == ["*.pkl"]
    assert not (destination / "model.pkl").exists()
    assert (destination / "data.csv").read_bytes() == files["data.csv"]
    assert (destination / "manifest.json").read_bytes() == files["manifest.json"]
    (source / ".encap.conf").write_text("broken: [configuration")
    invoke("tail")
    assert not (destination / "model.pkl").exists()
    invoke("pull", "--rsync-exclude-pull", "/models/")
    assert (destination / "model.pkl").read_bytes() == files["model.pkl"]
    assert not (destination / "models/other.pkl").exists()
    invoke("pull", "--rsync-exclude-pull")
    assert (destination / "models/other.pkl").read_bytes() == files["models/other.pkl"]
    files["model.pkl"] = b"a newer remote model"
    invoke("pull")
    assert (destination / "model.pkl").read_bytes() == b"model"
    assert writer.read()["handle"] == saved


@pytest.mark.parametrize("values", [["../model.pkl"], ["[z-a]"], ["***"]])
def test_invalid_saved_pull_patterns_fail_before_cloud_access(handle, tmp_path, values):
    handle.data["rsync_exclude_pull"] = values
    backend = DataprocBackend({})
    backend._command = Mock()
    with pytest.raises(ConfigurationError, match="rsync_exclude_pull"):
        backend.pull(handle, tmp_path)
    backend._command.assert_not_called()


@pytest.mark.parametrize("relative", ["../outside", "nested/../outside", "a//b", "a\\b", "line\nbreak"])
def test_filtered_inventory_rejects_unsafe_remote_paths(remote_pull, handle, tmp_path, relative):
    backend, files = remote_pull
    files[relative] = b"invalid"
    with pytest.raises(BackendError, match="Unsafe GCS output path"):
        backend.pull(handle, tmp_path)
    assert [call.args[0][2] for call in backend._command.call_args_list] == ["ls"]


def test_pull_saved_location_preserves_nested_zero_byte_files(handle, tmp_path):
    backend = DataprocBackend({})
    destination = tmp_path / "outputs"
    destination.mkdir()
    (destination / "unrelated").write_text("keep me")
    def command(arguments):
        assert "saved-project" in arguments
        assert "gs://saved-bucket/trial/outputs/" in arguments
        assert "--delete-unmatched-destination-objects" not in arguments
        if arguments[2] == "rsync":
            assert arguments[4] == str(destination)
            (destination / "nested").mkdir()
            (destination / "nested" / "empty").touch()
            (destination / "log").write_text("partial output")
        return Mock(stdout="gs://saved-bucket/trial/outputs/log\n")
    backend._command = Mock(side_effect=command)
    backend.pull(handle, destination)
    assert (destination / "nested" / "empty").read_bytes() == b""
    assert (destination / "unrelated").read_text() == "keep me"
    assert (destination / "log").read_text() == "partial output"


def test_pull_reuses_destination_and_excludes_run_metadata(handle, tmp_path):
    backend = DataprocBackend({})
    metadata = tmp_path / ".encap-run"
    metadata.mkdir()
    (metadata / "run.json").write_text("saved run")
    (tmp_path / "log").write_text("existing output")
    backend._command = Mock(return_value=Mock(stdout="gs://saved-bucket/trial/outputs/log\n"))
    for _ in range(2):
        backend.pull(handle, tmp_path)
    syncs = [call.args[0] for call in backend._command.call_args_list if call.args[0][2] == "rsync"]
    assert len(syncs) == 2
    for command in syncs:
        assert command[4] == str(tmp_path)
        assert "--recursive" in command
        assert "--delete-unmatched-destination-objects" not in command
        exclude = command[command.index("--exclude") + 1]
        for name in (".encap-run", ".encap-run/run.json", "nested/.encap-run/run.json",
                     "a/b/.encap-run/run.json"):
            assert re.match(exclude, name)
        for name in ("log", "nested/empty", ".encap-run-other/run.json"):
            assert not re.match(exclude, name)
    assert (metadata / "run.json").read_text() == "saved run"
    assert (tmp_path / "log").read_text() == "existing output"


@pytest.mark.parametrize("location", ["destination", "ancestor", "file", "directory", "dangling"])
def test_pull_rejects_symlinks_before_sync(handle, tmp_path, location):
    backend = DataprocBackend({})
    backend._command = Mock(return_value=Mock(stdout="gs://saved-bucket/trial/outputs/log\n"))
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "log").write_text("untouched")
    destination = tmp_path / "outputs"
    if location in ("destination", "ancestor"):
        destination.symlink_to(outside, target_is_directory=True)
        if location == "ancestor":
            destination = destination / "nested"
    else:
        destination.mkdir()
        target = outside / ("missing" if location == "dangling" else "log")
        if location == "directory":
            target = outside
        (destination / "link").symlink_to(target)
    with pytest.raises(BackendError, match="symlink"):
        backend.pull(handle, destination)
    assert backend._command.call_count == (0 if location in ("destination", "ancestor") else 1)
    assert (outside / "log").read_text() == "untouched"


def test_empty_outputs_fail_before_sync(handle, tmp_path):
    backend = DataprocBackend({})
    backend._command = Mock(return_value=Mock(stdout=""))
    with pytest.raises(BackendError, match="No remote output files"):
        backend.pull(handle, tmp_path)
    assert backend._command.call_count == 1


def test_pull_sync_failure_is_explicit(handle, tmp_path):
    backend = DataprocBackend({})
    backend._command = Mock(side_effect=[
        Mock(stdout="gs://saved-bucket/trial/outputs/log\n"),
        BackendError("download failed"),
    ])
    with pytest.raises(BackendError, match="download failed"):
        backend.pull(handle, tmp_path)


def test_missing_outputs_fail_explicitly(handle, tmp_path):
    backend = DataprocBackend({})
    backend._command = Mock(side_effect=MonitoringError("missing prefix"))
    with pytest.raises(MonitoringError, match="missing prefix"):
        backend.pull(handle, tmp_path)


def test_cancel_and_tail_interrupt_are_explicit(handle, monkeypatch):
    backend = DataprocBackend({})
    backend._operation = Mock(return_value={"metadata": {"graph": {"nodes": [{"jobId": "job"}]}}})
    backend._api = Mock()
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", Mock(side_effect=KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        backend.tail(handle)
    backend._api.assert_not_called()
    backend.cancel(handle)
    backend._api.assert_called_once_with("POST", handle.data["operation"] + ":cancel", {})


@pytest.fixture
def runner(monkeypatch):
    import encap_dataproc
    checkpoint = types.ModuleType("encap_dataproc.checkpoint")
    checkpoint.parse_gcs_uri = Mock(return_value=("bucket", "prefix"))
    checkpoint.download = Mock()
    checkpoint.upload_checkpoint = Mock()
    checkpoint.save_result = Mock()
    checkpoint.create_client = Mock()
    checkpoint.LogCheckpoint = MagicMock()
    monkeypatch.setitem(sys.modules, "encap_dataproc.checkpoint", checkpoint)
    monkeypatch.setattr(encap_dataproc, "checkpoint", checkpoint, raising=False)
    module = importlib.import_module("encap_dataproc.runner")
    monkeypatch.setattr(module, "checkpoint", checkpoint)
    return module


@pytest.mark.parametrize("fail_download", [False, True])
def test_worker_materializes_gcs_inputs_before_running_and_excludes_them_from_outputs(
    runner, run_request, tmp_path, fail_download,
):
    inputs = {"../other": "gs://input-bucket/other/", "data/value.txt": "gs://input-bucket/value"}
    (run_request.run_folder / "train.py").write_text(
        "from pathlib import Path\n"
        "assert Path('../other/reference.txt').read_text() == 'other experiment'\n"
        "assert (Path(__file__).parent / 'data/value.txt').read_text() == 'input data'\n"
        "Path('result.txt').write_text('passed')\n"
    )
    archive = tmp_path / "inputs.zip"
    experiment = create_archive(run_request, archive, inputs)
    def download(uri, destination, project):
        if destination.name == "request.json":
            destination.write_text(json.dumps({
                "experiment_path": experiment, "inputs": inputs,
                "entrypoint": "train.py", "arguments": [], "run_id": "trial",
                "project": "test", "zone": "test-zone", "data_url": "https://example.invalid",
            }))
        else:
            destination.write_bytes(archive.read_bytes())
    runner.checkpoint.download.side_effect = download
    def blob(name, content):
        return types.SimpleNamespace(
            name=name, generation=123,
            download_to_file=lambda output, if_generation_match: output.write(content),
        )
    client = runner.checkpoint.create_client.return_value
    client.list_blobs.return_value = [blob("other/reference.txt", b"other experiment")]
    if fail_download:
        client.bucket.return_value.get_blob.side_effect = PermissionError("denied")
    else:
        client.bucket.return_value.get_blob.return_value = blob("value", b"input data")
    def upload(folder, uri, project, excluded, log_checkpoint=None):
        assert folder.name == experiment
        assert (folder.parent / "other/reference.txt").read_text() == "other experiment"
        assert excluded == [folder.parent / "other", folder / "data/value.txt"]
        assert (folder / "result.txt").exists() is (not fail_download)
    runner.checkpoint.upload_checkpoint.side_effect = upload
    args = ("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output", "gs://bucket/result", "test")
    if fail_download:
        with pytest.raises(PermissionError, match="denied"):
            runner.run(*args)
    else:
        assert runner.run(*args) == 0
    result = runner.checkpoint.save_result.call_args.args[0]
    assert result["state"] == ("failed" if fail_download else "succeeded")
    assert result["outputs_finalized"] is True


@pytest.mark.parametrize("origin_position", ["inside", "equal", "above"])
def test_root_archive_preserves_source_and_dependency_layout(
    run_request, tmp_path, origin_position,
):
    root = tmp_path / "projects"
    origins = {
        "inside": (root / "experiments", "train", ".", "experiments/0encap_folder/train/trial"),
        "equal": (root, "experiments/train", ".", "experiments/0encap_folder/train/trial"),
        "above": (
            root.parent, "projects/experiments/train", "projects", "projects/experiments/0encap_folder/train/trial",
        ),
    }
    origin, experiment, root_path, expected_experiment = origins[origin_position]
    workspace = tmp_path / "workspace"
    folder = workspace / experiment
    folder.mkdir(parents=True)
    for filename in ("train.py", ".encap.conf", ".encap_history.conf"):
        (folder / filename).write_bytes((run_request.run_folder / filename).read_bytes())
    saved = root / "experiments/0encap_folder/train/trial"
    saved.mkdir(parents=True)
    for filename in ("train.py", ".encap.conf", ".encap_history.conf"):
        (saved / filename).write_bytes((run_request.run_folder / filename).read_bytes())
    library = folder.parent / "src"
    library.mkdir()
    (library / "helper.py").write_text("VALUE = 42")
    request = replace(
        run_request, run_folder=saved, workspace_folder=workspace, experiment_path=experiment, workspace_origin=origin,
    )
    inputs = {"{root}/data": "gs://input-bucket/data/", "local": "gs://input-bucket/local"}
    prefix, actual_experiment, targets, actual_root = archive_layout(request, inputs, root)
    assert actual_experiment == expected_experiment
    assert actual_root == root_path
    archive = tmp_path / "root.zip"
    assert create_archive(request, archive, inputs, root) == expected_experiment
    with zipfile.ZipFile(archive) as payload:
        assert f"{expected_experiment}/train.py" in payload.namelist()
        assert str(Path(root_path) / "experiments/src/helper.py") in payload.namelist()
        assert str(prefix / experiment / "train.py") not in payload.namelist()
    assert targets[str(Path(root_path) / "data")] == "gs://input-bucket/data/"
    assert targets[f"{expected_experiment}/local"] == "gs://input-bucket/local"


@pytest.mark.parametrize("file_target", [False, True])
@pytest.mark.parametrize("with_dependencies", [False, True])
@pytest.mark.parametrize("with_root", [False, True])
def test_worker_starts_in_saved_run_directory(
    runner, tmp_path, file_target, with_dependencies, with_root,
):
    root = tmp_path / "projects"
    source = root / "experiments/training"
    source.mkdir(parents=True)
    library = root / "experiments/src"
    library.mkdir()
    (library / "helper.txt").write_text("dependency")
    saved = (
        source / "run/trial" if file_target
        else root / "experiments/0encap_folder/training/trial"
    )
    origin = source.parent if with_dependencies else source
    anchor = root if with_root else origin
    relative = os.path.relpath(saved, anchor)
    script = (
        "import os\nfrom pathlib import Path\n"
        "root = Path(os.environ['ENCAP_WORKSPACE_ROOT'])\n"
        f"assert Path.cwd() == (root / {relative!r}).resolve()\n"
        "assert Path.cwd().name == 'trial'\n"
        "assert Path(__file__).resolve().parent == Path.cwd()\n"
        "assert root.is_dir()\n"
    )
    if with_dependencies:
        dependency = os.path.relpath(library / "helper.txt", anchor)
        script += f"assert (root / {dependency!r}).read_text() == 'dependency'\n"
    script += "Path('cwd.txt').write_text(os.path.relpath(Path.cwd(), root))\n"
    (source / "run.py").write_text(script)
    saved.mkdir(parents=True)
    stage_source(source / "run.py" if file_target else source, saved, [])
    write_run_configuration(saved, {"backend": "dataproc", "script_name": "run.py"}, {})
    (saved / ".encap-run").mkdir()
    dependencies = [library] if with_dependencies else []
    workspace, original_experiment = stage_workspace(source, saved, dependencies, [])
    request = RunRequest(
        saved, "run.py", [], {}, "trial", saved / ".encap-run",
        workspace_folder=workspace, experiment_path=original_experiment, workspace_origin=origin,
        workspace_dependencies=("src",) if with_dependencies else (),
    )
    archive = tmp_path / "saved-run.zip"
    configured_root = root if with_root else None
    experiment = create_archive(request, archive, workspace_root=configured_root)
    _, _, _, root_path = archive_layout(request, {}, configured_root)
    if with_root:
        assert experiment == saved.relative_to(root).as_posix()
    assert Path(experiment).name == "trial"
    def download(uri, destination, project):
        if destination.name == "request.json":
            destination.write_text(json.dumps({
                "experiment_path": experiment, "root_path": root_path,
                "entrypoint": "run.py", "arguments": [], "run_id": "trial",
                "project": "test", "zone": "test-zone", "data_url": "https://example.invalid",
            }))
        else:
            destination.write_bytes(archive.read_bytes())
    runner.checkpoint.download.side_effect = download
    def upload(folder, uri, project, log_checkpoint=None):
        assert folder.name == "trial"
        assert (folder / "cwd.txt").read_text() == relative
        assert not (folder / "src").exists()
    runner.checkpoint.upload_checkpoint.side_effect = upload
    assert runner.run(
        "gs://bucket/archive", "gs://bucket/request", "gs://bucket/output", "gs://bucket/result", "test",
    ) == 0


def test_dependency_inside_experiment_keeps_original_root_location(run_request, tmp_path):
    root = tmp_path / "projects"
    saved = root / "0encap_folder/train/trial"
    saved.mkdir(parents=True)
    workspace = tmp_path / "workspace"
    folder = workspace / "train"
    folder.mkdir(parents=True)
    for filename in ("train.py", ".encap.conf", ".encap_history.conf"):
        content = (run_request.run_folder / filename).read_bytes()
        (folder / filename).write_bytes(content)
        (saved / filename).write_bytes(content)
    (folder / "helper.py").write_text("VALUE = 42")
    (saved / "helper.py").write_text("VALUE = 42")
    request = replace(
        run_request, run_folder=saved, workspace_folder=workspace, experiment_path="train",
        workspace_origin=root, workspace_dependencies=("train/helper.py",),
    )
    archive = tmp_path / "inside.zip"
    experiment = create_archive(request, archive, workspace_root=root)
    with zipfile.ZipFile(archive) as payload:
        assert payload.read("train/helper.py") == b"VALUE = 42"
        assert payload.read(experiment + "/helper.py") == b"VALUE = 42"
        assert "train/train.py" not in payload.namelist()


def test_dependency_inside_file_source_is_not_moved_into_run(run_request, tmp_path):
    root = tmp_path / "projects"
    saved = root / "train/trial"
    saved.mkdir(parents=True)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for filename in ("train.py", ".encap.conf", ".encap_history.conf"):
        content = (run_request.run_folder / filename).read_bytes()
        (workspace / filename).write_bytes(content)
        (saved / filename).write_bytes(content)
    (workspace / "helper.py").write_text("VALUE = 42")
    request = replace(
        run_request, run_folder=saved, workspace_folder=workspace, workspace_origin=root,
        workspace_dependencies=("helper.py",),
    )
    archive = tmp_path / "file-source.zip"
    experiment = create_archive(request, archive, workspace_root=root)
    with zipfile.ZipFile(archive) as payload:
        assert payload.read("helper.py") == b"VALUE = 42"
        assert experiment + "/helper.py" not in payload.namelist()
        assert experiment + "/train.py" in payload.namelist()


def test_root_inputs_without_snapshot_origin_fail_before_cloud_access(config, run_request, monkeypatch, tmp_path):
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda name: "/bin/gcloud")
    backend = DataprocBackend(dict(
        config, workspace_root=str(tmp_path), inputs={"{root}/data": "gs://input-bucket/data/"},
    ))
    backend._check_buckets = Mock()
    backend._api = Mock()
    writer = RecordWriter.reserve(run_request.record_dir, registration, run_request.run_id)
    with pytest.raises(ConfigurationError, match="origin metadata"):
        backend.submit(run_request, writer)
    backend._check_buckets.assert_not_called()
    backend._api.assert_not_called()


def test_root_archive_normalizes_origin_metadata(run_request, tmp_path):
    root = tmp_path / "projects"
    saved = root / "train/trial"
    saved.mkdir(parents=True)
    for filename in ("train.py", ".encap.conf", ".encap_history.conf"):
        (saved / filename).write_bytes((run_request.run_folder / filename).read_bytes())
    request = replace(run_request, run_folder=saved, workspace_origin=root / "experiments/../train")
    archive = tmp_path / "normalized.zip"
    assert create_archive(request, archive, {"{root}/data": "gs://input-bucket/data/"}, root) == "train/trial"
    with zipfile.ZipFile(archive) as payload:
        assert "train/trial/train.py" in payload.namelist()
        assert all(".." not in Path(name).parts for name in payload.namelist())


@pytest.mark.parametrize("fail_download", [False, True])
def test_worker_downloads_root_inputs_and_exposes_worker_root(runner, run_request, tmp_path, fail_download):
    root = tmp_path / "projects"
    inputs = {
        "{root}/experiments/0encap_folder/seed": "gs://input-bucket/seed/",
        "local.txt": "gs://input-bucket/value",
    }
    saved = root / "experiments/0encap_folder/train/trial"
    saved.mkdir(parents=True)
    for filename in ("train.py", ".encap.conf", ".encap_history.conf"):
        (saved / filename).write_bytes((run_request.run_folder / filename).read_bytes())
    request = replace(run_request, run_folder=saved, workspace_origin=root / "experiments/train")
    (request.run_folder / "train.py").write_text(
        "import os\nfrom pathlib import Path\n"
        "root = Path(os.environ['ENCAP_WORKSPACE_ROOT'])\n"
        "assert Path.cwd() == root / 'experiments/0encap_folder/train/trial'\n"
        "assert (root / 'experiments/0encap_folder/seed/results/shared.json').read_text() == 'seed'\n"
        "assert Path('local.txt').read_text() == 'local'\n"
        "assert not Path('experiments').exists()\n"
        "Path('result.txt').write_text('passed')\n"
    )
    archive = tmp_path / "root-worker.zip"
    experiment = create_archive(request, archive, inputs, root)
    _, _, _, root_path = archive_layout(request, inputs, root)
    def download(uri, destination, project):
        if destination.name == "request.json":
            destination.write_text(json.dumps({
                "experiment_path": experiment, "root_path": root_path, "inputs": inputs,
                "entrypoint": "train.py", "arguments": [], "run_id": "trial",
                "project": "test", "zone": "test-zone", "data_url": "https://example.invalid",
                "storage_layout": "mirrored", "instance": 0,
            }))
        else:
            destination.write_bytes(archive.read_bytes())
    runner.checkpoint.download.side_effect = download
    def blob(name, content):
        return types.SimpleNamespace(
            name=name, generation=123,
            download_to_file=lambda output, if_generation_match: output.write(content),
        )
    client = runner.checkpoint.create_client.return_value
    client.list_blobs.return_value = [
        blob("seed/results/shared.json", b"seed"), blob("seed/.encap-run/control.json", b"internal"),
    ]
    if fail_download:
        client.bucket.return_value.get_blob.side_effect = PermissionError("denied")
    else:
        client.bucket.return_value.get_blob.return_value = blob("value", b"local")
    def upload(folder, uri, project, excluded, merge_instance, log_checkpoint=None):
        worker_root = folder.parents[3]
        assert (folder / "result.txt").exists() is (not fail_download)
        if not fail_download:
            assert (folder / "result.txt").read_text() == "passed"
        assert excluded == [worker_root / "experiments/0encap_folder/seed", folder / "local.txt"]
        assert not (excluded[0] / ".encap-run").exists()
        assert merge_instance == 0
    runner.checkpoint.upload_checkpoint.side_effect = upload
    arguments = ("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output", "gs://bucket/result", "test")
    if fail_download:
        with pytest.raises(PermissionError, match="denied"):
            runner.run(*arguments)
    else:
        assert runner.run(*arguments) == 0
    assert runner.checkpoint.save_result.call_args.args[0]["state"] == ("failed" if fail_download else "succeeded")


@pytest.mark.parametrize("name", ["../outside", "/absolute", "dir\\outside"])
def test_runner_rejects_unsafe_archives(runner, tmp_path, name):
    archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive, "w") as payload:
        payload.writestr(name, "unsafe")
    with pytest.raises(ValueError, match="Unsafe"):
        runner.extract_archive(archive, tmp_path / "extracted")


def test_worker_root_preserves_environment_validation(runner, tmp_path):
    (tmp_path / "train.py").write_text("print('test')")
    with pytest.raises(ValueError, match="Environment must map"):
        runner.run_computation(tmp_path, {
            "entrypoint": "train.py", "arguments": [], "environment": [["NAME", "value"]],
            "run_id": "trial", "project": "test", "zone": "test-zone", "data_url": "https://example.invalid",
        }, workspace_root=tmp_path)


def test_runner_rejects_archive_symlinks(runner, tmp_path):
    archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive, "w") as payload:
        entry = zipfile.ZipInfo("link")
        entry.external_attr = (stat.S_IFLNK | 0o777) << 16
        payload.writestr(entry, "../outside")
    with pytest.raises(ValueError, match="Unsafe"):
        runner.extract_archive(archive, tmp_path / "extracted")


@pytest.mark.parametrize("code", [0, 7])
def test_worker_runs_arbitrary_entrypoint_arguments_environment(runner, tmp_path, code):
    (tmp_path / "different.py").write_text(
        "import os,sys\n"
        "print(sys.argv[1], os.environ['MY_VALUE'], os.environ['ENCAP_PROCID'])\n"
        "open('empty', 'w').close()\n"
        f"sys.exit({code})\n"
    )
    result = runner.run_computation(tmp_path, {
        "entrypoint": "different.py", "arguments": ["two words"], "environment": {"MY_VALUE": "yes"},
        "run_id": "my-run",
        "project": "test-project", "zone": "europe-west1-b",
        "data_url": "https://console.cloud.google.com/storage/browser/test-bucket/trial?project=test-project",
    })
    assert result == code
    assert (tmp_path / "empty").read_bytes() == b""
    assert "two words yes 0" in (tmp_path / "log").read_text()
    log = (tmp_path / "log").read_text()
    assert re.match(r"\w{3} \w{3} +\d+ .* \d{4}\nhost: .+\n", log)
    assert "my-run/different.py 'two words'  \n\n" in log
    assert re.search(r"\nreal\s+\d+m[\d.]+s\nuser\s+\d+m[\d.]+s\nsys\s+\d+m[\d.]+s\n\x04\n$", log)


@pytest.mark.parametrize("script", [
    "import time\ntime.sleep(15)\n",
    "import sys,time\nsys.stdout.write('partial output');sys.stdout.flush()\ntime.sleep(15)\n",
    (
        "import subprocess,sys\n"
        "from pathlib import Path\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(15)'])\n"
        "Path('child.pid').write_text(str(child.pid))\n"
        "child.wait()\n"
    ),
])
def test_worker_timeout_kills_silent_partial_output_and_descendant_processes(runner, tmp_path, script):
    (tmp_path / "train.py").write_text(script)
    started = time.monotonic()
    with pytest.raises(runner.ComputationTimeout, match="timed out after 1 seconds"):
        runner.run_computation(tmp_path, {
            "entrypoint": "train.py", "arguments": [], "run_id": "trial",
            "project": "test", "zone": "test-zone", "data_url": "https://example.invalid",
            "timeout_seconds": 1,
        })
    assert time.monotonic() - started < 5
    assert "Experiment timed out after 1 seconds." in (tmp_path / "log").read_text()
    assert (tmp_path / "log").read_text().endswith("\x04\n")
    if (tmp_path / "child.pid").exists():
        pid = (tmp_path / "child.pid").read_text()
        state = subprocess.run(["ps", "-o", "stat=", "-p", pid], capture_output=True, text=True)
        assert state.returncode in (0, 1)
        assert not state.stdout.strip() or state.stdout.strip().startswith("Z")


def test_worker_completes_before_timeout_and_cancels_watchdog(runner, tmp_path):
    (tmp_path / "train.py").write_text("print('completed')\n")
    timer = Mock(wraps=runner.threading.Timer)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runner.threading, "Timer", timer)
        assert runner.run_computation(tmp_path, {
            "entrypoint": "train.py", "arguments": [], "run_id": "trial",
            "project": "test", "zone": "test-zone", "data_url": "https://example.invalid",
            "timeout_seconds": 172800,
        }) == 0
    assert timer.call_args.args[0] == 172800
    assert "timed out" not in (tmp_path / "log").read_text()


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "1s", "unlimited", 10**30])
def test_worker_rejects_invalid_deadline_before_launch(runner, tmp_path, monkeypatch, value):
    launch = Mock()
    monkeypatch.setattr(runner.subprocess, "Popen", launch)
    with pytest.raises(ValueError, match="timeout_seconds"):
        runner.run_computation(tmp_path, {"timeout_seconds": value})
    launch.assert_not_called()


@pytest.mark.parametrize("upload_failure", [False, True])
def test_worker_timeout_saves_partial_outputs_and_failure_marker(runner, monkeypatch, upload_failure):
    runner.checkpoint.download.side_effect = lambda uri, destination, project: destination.write_text("{}")
    monkeypatch.setattr(runner, "extract_archive", Mock())
    monkeypatch.setattr(runner, "run_computation", Mock(
        side_effect=runner.ComputationTimeout("Experiment timed out after 172800 seconds."),
    ))
    if upload_failure:
        runner.checkpoint.upload_checkpoint.side_effect = RuntimeError("upload failed")
        with pytest.raises(RuntimeError, match="upload failed"):
            runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output",
                       "gs://bucket/result", "project")
    else:
        assert runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output",
                          "gs://bucket/result", "project") == 124
    runner.checkpoint.upload_checkpoint.assert_called_once()
    result = runner.checkpoint.save_result.call_args.args[0]
    assert result["state"] == "failed"
    assert result["exit_code"] == 124
    assert result["timed_out"] is True
    assert result["outputs_finalized"] is (not upload_failure)
    assert "timed out after 172800 seconds" in result["computation_error"]


def test_worker_real_timeout_finalizes_partial_outputs(runner, monkeypatch):
    def download(uri, destination, project):
        if destination.name == "request.json":
            destination.write_text(json.dumps({
                "entrypoint": "train.py", "arguments": [], "run_id": "trial",
                "project": "test", "zone": "test-zone", "data_url": "https://example.invalid",
                "timeout_seconds": 1,
            }))

    def extract(archive, destination):
        (destination / "train.py").write_text(
            "import time\nfrom pathlib import Path\n"
            "Path('partial.txt').write_text('saved before timeout')\n"
            "time.sleep(15)\n"
        )

    def upload(folder, output_uri, project, log_checkpoint=None):
        assert (folder / "partial.txt").read_text() == "saved before timeout"
        assert "Experiment timed out" in (folder / "log").read_text()

    runner.checkpoint.download.side_effect = download
    runner.checkpoint.upload_checkpoint.side_effect = upload
    monkeypatch.setattr(runner, "extract_archive", extract)
    assert runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output",
                      "gs://bucket/result", "project") == 124
    result = runner.checkpoint.save_result.call_args.args[0]
    assert result["state"] == "failed"
    assert result["timed_out"] is True
    assert result["exit_code"] == 124
    assert result["outputs_finalized"] is True


@pytest.mark.parametrize("saved_jobs_url", [
    None, "https://console.cloud.google.com/dataproc/jobs?project=saved-project&authuser=0",
])
def test_worker_sparse_instance_log_and_environment(runner, tmp_path, capsys, monkeypatch, saved_jobs_url):
    monkeypatch.setattr(runner.socket, "gethostname", lambda: "encap-instance-7.internal")
    (tmp_path / "train.py").write_text(
        "import os\nprint(os.environ['ENCAP_PROCID'], os.environ['ENCAP_GCP_INSTANCE'])\n"
        "print(os.environ['ENCAP_LOG'])\n"
    )
    assert runner.run_computation(tmp_path, {
        "entrypoint": "train.py", "arguments": [], "environment": {}, "run_id": "trial",
        "instance": 7, "multiple_instances": True, "display_folder": "test/trial",
        "project": "test-project", "zone": "europe-west1-b",
        "console_authuser": "user+test@example.com",
        "data_url": "https://console.cloud.google.com/storage/browser/test-bucket/trial-7?project=test-project",
        **({"jobs_url": saved_jobs_url} if saved_jobs_url is not None else {}),
    }) == 0
    log = (tmp_path / "log_7").read_text()
    assert (
        "\nhost: https://console.cloud.google.com/compute/instancesDetail/zones/"
        "europe-west1-b/instances/encap-instance-7?project=test-project&authuser=user%2Btest%40example.com"
        " (temporary VM; deleted after execution)\n"
    ) in log
    expected_jobs_url = saved_jobs_url or (
        "https://console.cloud.google.com/dataproc/jobs?project=test-project&authuser=user%2Btest%40example.com"
    )
    assert f"\njobs: {expected_jobs_url}\n" in log
    assert "\ndata: https://console.cloud.google.com/storage/browser/test-bucket/trial-7?project=test-project\n" in log
    assert "\nInstance 7\ntest/trial/train.py   \n\n" in log
    assert "\n7 7\n" in log
    assert str(tmp_path / "log_7") in log
    assert log.endswith("\x04\n")
    assert capsys.readouterr().out == log
    assert not (tmp_path / "log").exists()


@pytest.mark.parametrize("code,upload_error", [(0, None), (7, None), (0, "upload failed"), (7, "upload failed")])
def test_worker_finalization_preserves_failures(runner, monkeypatch, code, upload_error):
    def download(uri, destination, project):
        if destination.name == "request.json":
            destination.write_text("{}")
    runner.checkpoint.download.side_effect = download
    monkeypatch.setattr(runner, "extract_archive", Mock())
    monkeypatch.setattr(runner, "run_computation", Mock(return_value=code))
    if upload_error:
        runner.checkpoint.upload_checkpoint.side_effect = RuntimeError(upload_error)
        with pytest.raises(RuntimeError, match=upload_error):
            runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output",
                       "gs://bucket/result", "project")
    else:
        assert runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output",
                          "gs://bucket/result", "project") == code
    result = runner.checkpoint.save_result.call_args.args[0]
    assert result["exit_code"] == code
    assert result["outputs_finalized"] is (upload_error is None)
    assert result["state"] == ("succeeded" if code == 0 and upload_error is None else "failed")


def test_worker_reports_both_computation_and_upload_exceptions(runner, monkeypatch):
    runner.checkpoint.download.side_effect = lambda uri, destination, project: destination.write_text("{}")
    monkeypatch.setattr(runner, "extract_archive", Mock())
    monkeypatch.setattr(runner, "run_computation", Mock(side_effect=ValueError("computation broke")))
    runner.checkpoint.upload_checkpoint.side_effect = RuntimeError("upload broke")
    with pytest.raises(RuntimeError, match="computation broke.*upload broke"):
        runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output", "gs://bucket/result", "project")
    result = runner.checkpoint.save_result.call_args.args[0]
    assert "computation broke" in result["computation_error"]
    assert "upload broke" in result["upload_error"]


@pytest.mark.parametrize("experiment_path", ["../outside", "/absolute", "a\\b", "", None])
def test_worker_rejects_unsafe_workspace_path_before_extraction(runner, monkeypatch, experiment_path):
    def download(uri, destination, project):
        destination.write_text(json.dumps({"experiment_path": experiment_path}))
    runner.checkpoint.download.side_effect = download
    extract = Mock()
    monkeypatch.setattr(runner, "extract_archive", extract)
    with pytest.raises(ValueError, match="safe relative workspace"):
        runner.run("gs://bucket/archive", "gs://bucket/request", "gs://bucket/output",
                   "gs://bucket/result", "project")
    extract.assert_not_called()
    result = runner.checkpoint.save_result.call_args.args[0]
    assert result["state"] == "failed"


def test_api_uses_explicit_request_id_and_surfaces_network_errors(monkeypatch):
    import io
    backend = DataprocBackend({})
    backend._command = Mock(return_value=Mock(stdout="transient-token"))
    response = Mock()
    response.__enter__ = Mock(return_value=io.StringIO('{"name": "operation"}'))
    response.__exit__ = Mock(return_value=False)
    opener = Mock(return_value=response)
    monkeypatch.setattr("encap_dataproc.backend.urlopen", opener)
    result = backend._api("POST", "projects/p/regions/r/workflowTemplates:instantiateInline",
                          {"jobs": []}, {"requestId": "uuid"})
    request = opener.call_args.args[0]
    assert request.full_url.endswith("?requestId=uuid")
    assert json.loads(request.data) == {"jobs": []}
    assert result["name"] == "operation"
    opener.side_effect = URLError("offline")
    with pytest.raises(MonitoringError, match="Cannot contact"):
        backend._api("GET", "projects/p/regions/r/operations/op")
    assert opener.call_count == 2  # No automatic retry.


def test_api_reauthentication_happens_before_single_post(monkeypatch):
    command = ["gcloud", "auth", "print-access-token"]
    execute = Mock(side_effect=[
        subprocess.CalledProcessError(1, command, stderr="Reauthentication required."),
        subprocess.CompletedProcess(["gcloud", "auth", "login"], 0),
        subprocess.CompletedProcess(command, 0, stdout="private-token"),
    ])
    monkeypatch.setattr("encap_dataproc.backend.subprocess.run", execute)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    response = Mock()
    response.__enter__ = Mock(return_value=io.StringIO('{"name": "operation"}'))
    response.__exit__ = Mock(return_value=False)
    opener = Mock(return_value=response)
    monkeypatch.setattr("encap_dataproc.backend.urlopen", opener)
    result = DataprocBackend({})._api(
        "POST", "projects/p/regions/r/workflowTemplates:instantiateInline",
        {"jobs": []}, {"requestId": "uuid"},
    )
    assert result["name"] == "operation"
    opener.assert_called_once()
    assert execute.call_count == 3


def test_finalization_marker_404_is_pending_but_permission_failure_is_error(handle, monkeypatch):
    backend = DataprocBackend({})
    backend._command = Mock(return_value=Mock(stdout="transient-token"))
    opener = Mock(side_effect=HTTPError("uri", 404, "missing", {}, None))
    monkeypatch.setattr("encap_dataproc.backend.urlopen", opener)
    assert backend._result(handle.data) is None
    opener.side_effect = HTTPError("uri", 403, "forbidden", {}, None)
    with pytest.raises(MonitoringError, match="403"):
        backend._result(handle.data)


def test_checkpoint_preserves_empty_files_and_attempts_remaining_uploads(tmp_path, monkeypatch):
    class CloudError(Exception):
        pass
    fake_storage = types.ModuleType("google.cloud.storage")
    fake_storage.Client = Mock()
    modules = {
        "google": types.ModuleType("google"), "google.auth": types.ModuleType("google.auth"),
        "google.auth.compute_engine": types.SimpleNamespace(Credentials=object),
        "google.auth.exceptions": types.SimpleNamespace(GoogleAuthError=CloudError),
        "google.auth.transport": types.ModuleType("google.auth.transport"),
        "google.auth.transport.requests": types.SimpleNamespace(Request=Mock()),
        "google.cloud": types.ModuleType("google.cloud"), "google.cloud.storage": fake_storage,
        "google.api_core": types.ModuleType("google.api_core"),
        "google.api_core.exceptions": types.SimpleNamespace(GoogleAPIError=CloudError, PreconditionFailed=CloudError),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    path = Path(__file__).resolve().parents[1] / "encap_dataproc" / "checkpoint.py"
    spec = importlib.util.spec_from_file_location("checkpoint_under_test", path)
    checkpoint = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checkpoint)
    bucket = Mock()
    monkeypatch.setattr(checkpoint, "create_client", Mock(return_value=Mock(bucket=Mock(return_value=bucket))))
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "empty").touch()
    (tmp_path / "data").write_text("data")
    (tmp_path / ".encap-run").mkdir()
    (tmp_path / ".encap-run" / "run.json").write_text("reserved")
    bucket.blob.side_effect = lambda name: Mock(upload_from_filename=Mock(side_effect=CloudError("denied"))) if name.endswith("/data") else Mock()
    with pytest.raises(RuntimeError, match="data: denied"):
        checkpoint.upload_checkpoint(tmp_path, "gs://bucket/results", "project")
    assert [call.args[0] for call in bucket.blob.call_args_list] == ["results/data", "results/nested/empty"]
    bucket.reset_mock()
    bucket.blob.side_effect = None
    checkpoint.upload_checkpoint(tmp_path, "gs://bucket/results", "project", excluded=[tmp_path / "data"])
    assert [call.args[0] for call in bucket.blob.call_args_list] == ["results/nested/empty"]
