import copy
import importlib.machinery
import importlib.util
import io
import json
import sys
import threading
import types
import zipfile
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pytest

from encap_lib import autopull, backends, encap_settings as settings
from encap_lib.backends import BackendError, ConfigurationError, MonitoringError, PluginConfig, RecordWriter, RunHandle, RunRequest, RunStatus
from encap_dataproc import registration
from encap_dataproc.backend import DataprocBackend, build_workflow, validate_config
from encap_dataproc.storage import GCSStore, RunClaim, StorageConflict


class MemoryStore:
    def __init__(self):
        self.bucket = "test-bucket"
        self.data = {}
        self.sequence = 0
        self.deleted = []

    def read(self, key):
        return copy.deepcopy(self.data.get(key))

    def generation(self, key):
        value = self.data.get(key)
        return int(value[1]) if value is not None else 0

    def write(self, key, record, generation):
        existing = self.data.get(key)
        if (existing is None and generation != 0) or (existing is not None and existing[1] != generation):
            raise StorageConflict("generation mismatch")
        self.sequence += 1
        self.data[key] = (copy.deepcopy(record), str(self.sequence))
        return str(self.sequence)

    def objects(self, prefix):
        return iter([{"name": key, "generation": value[1]}
                     for key, value in sorted(self.data.items()) if key.startswith(prefix)])

    def delete(self, key, generation):
        if self.data[key][1] != generation:
            raise StorageConflict("generation mismatch")
        self.deleted.append(key)
        del self.data[key]


@pytest.fixture
def mirrored(tmp_path, monkeypatch):
    root = tmp_path / "projects"
    folder = root / "experiments/0encap_folder/training/trial"
    folder.parent.mkdir(parents=True)
    RecordWriter.reserve(folder, registration, "trial")
    for name, value in (("run.py", "print('test')"), (".encap.conf", ""), (".encap_history.conf", "")):
        (folder / name).write_text(value)
    config = {
        "bucket": "test-bucket", "project": "test-project", "region": "europe-west1",
        "zone": "europe-west1-b", "subnet": "projects/host/regions/europe-west1/subnetworks/private",
        "python": "/opt/python/bin/python", "image_uri": "projects/image/global/images/custom",
        "service_account": "worker@test-project.iam.gserviceaccount.com",
        "network_tag": "test-network", "machine_type": "n4-standard-4",
        "workspace_root": str(root), "console_authuser": "0",
    }
    request = RunRequest(folder, "run.py", [], {}, "trial", folder / ".encap-run")
    backend = DataprocBackend(config)
    store = MemoryStore()
    monkeypatch.setattr("encap_dataproc.backend.GCSStore", lambda bucket, command: store)
    monkeypatch.setattr("encap_dataproc.backend.shutil.which", lambda command: "/bin/gcloud")
    backend._check_buckets = Mock()
    backend._command = Mock(return_value=Mock(stdout=""))
    backend._api = Mock(side_effect=lambda *args: {
        "name": "projects/test-project/regions/europe-west1/operations/" + args[3]["requestId"],
    })
    return backend, request, store


def test_mirrored_path_and_hidden_metadata(mirrored, monkeypatch, tmp_path):
    backend, request, _ = mirrored
    first = backend._new_handle(request, 7)
    monkeypatch.chdir(tmp_path)
    second = backend._new_handle(request, 7)
    uri = "gs://test-bucket/encap/experiments/0encap_folder/training/trial"
    assert first.data["run_uri"] == second.data["run_uri"] == uri
    assert first.data["output_uri"] == uri
    assert first.data["request_id"] != second.data["request_id"]
    assert first.data["result_uri"] == uri + "/.encap-run/instances/7/result.json"
    workflow = build_workflow(backend.config, first.data)
    job = workflow["jobs"][0]["pysparkJob"]
    assert job["mainPythonFileUri"] == uri + "/.encap-run/source/runner.py"
    assert uri + "/.encap-run/instances/7/request.json" in job["args"]


def test_mirrored_source_identity_is_workspace_relative(mirrored, monkeypatch, tmp_path):
    backend, request, _ = mirrored
    root = Path(backend.config["workspace_root"])
    request = replace(request, source_path=root / "experiments/training")
    monkeypatch.chdir(tmp_path)
    captured = []
    def command(arguments, stream=False):
        for argument in arguments:
            if argument.endswith("/request.json") and not argument.startswith("gs://"):
                captured.append(json.loads(Path(argument).read_text()))
        return Mock(stdout="")
    backend._command.side_effect = command
    backend.submit(request, RecordWriter(request.record_dir))
    assert len(captured) == 1
    assert captured[0]["source"] == "experiments/training"


@pytest.mark.parametrize("initial", [1, [0]])
def test_rerun_adds_instances_without_replacing_zero_or_deleting_outputs(mirrored, initial):
    backend, request, store = mirrored
    writer = RecordWriter(request.record_dir)
    backend.config["i"] = initial
    first = backend.submit(request, writer)
    zero = RunHandle(**first.data["instances"][0]["handle"]) if first.version == 2 else first
    old_control = store.read(next(iter(store.data)))[0]
    output = zero.data["run_uri"][5:].split("/", 1)[1] + "/results_0.txt"
    store.write(output, {"keep": True}, 0)
    (request.run_folder / "results_0.txt").write_text("keep")
    backend.config["i"] = [1, 2, 3]
    backend.status = Mock(return_value=RunStatus("running"))
    backend._api.reset_mock()
    handle = backend.rerun(request, first, writer)
    assert handle.data["log_instance"] == 1
    children = handle.data["instances"]
    assert [child["instance"] for child in children] == [0, 1, 2, 3]
    assert children[0]["handle"] == asdict(zero)
    assert backend._api.call_count == 3
    backend.status.assert_not_called()
    assert store.read(output)[0] == {"keep": True}
    assert not store.deleted
    assert (request.run_folder / "results_0.txt").read_text() == "keep"
    control = store.read(next(iter(store.data)))[0]
    assert control["instances"][0] == old_control["instances"][0]
    assert [child["instance"] for child in control["instances"]] == [0, 1, 2, 3]
    assert writer.read()["handle"] == asdict(handle)
    assert writer.read()["state"] == "submitted"
    for child in children[1:]:
        data = child["handle"]["data"]
        assert data["source_uri"] != zero.data["source_uri"]
        assert data["request_id"] in data["request_uri"]
        assert child["submission_phase"] == "submitted"


def test_rerun_restarts_selected_completed_instance_with_fresh_control_paths(mirrored):
    backend, request, store = mirrored
    writer = RecordWriter(request.record_dir)
    first = backend.submit(request, writer)
    prefix = first.data["run_uri"][5:].split("/", 1)[1]
    log_generation = store.write(prefix + "/log", {"old": "log"}, 0)
    backend.status = Mock(return_value=RunStatus("succeeded", 0, outputs_finalized=True, cleanup_pending=False))
    backend.config["i"] = [0]
    handle = backend.rerun(request, first, writer)
    assert handle.data["log_instance"] == 0
    restarted = handle.data["instances"][0]["handle"]["data"]
    assert restarted["run_uri"] == first.data["run_uri"]
    for key in ("request_id", "request_uri", "result_uri", "source_uri"):
        assert restarted[key] != first.data[key]
    assert handle.data["history"][0]["handle"] == asdict(first)
    assert restarted["log_generation"] == int(log_generation)
    assert not store.deleted
    assert backend.status.call_count == 1


@pytest.mark.parametrize("state,cleanup", [("running", False), ("unknown", False), ("succeeded", True)])
def test_rerun_rejects_selected_active_or_uncertain_instance(mirrored, state, cleanup):
    backend, request, store = mirrored
    writer = RecordWriter(request.record_dir)
    first = backend.submit(request, writer)
    saved = writer.read()
    cloud = copy.deepcopy(store.data)
    backend.status = Mock(return_value=RunStatus(state, cleanup_pending=cleanup))
    backend._api.reset_mock()
    with pytest.raises(MonitoringError, match="active or uncertain"):
        backend.rerun(request, first, writer)
    assert writer.read() == saved
    assert store.data == cloud
    backend._api.assert_not_called()


def test_original_rerun_command_uses_saved_snapshot_and_keeps_all_instances(mirrored, monkeypatch):
    backend, request, store = mirrored
    writer = RecordWriter(request.record_dir)
    first = backend.submit(request, writer)
    source = request.run_folder.parents[2] / "training"
    source.mkdir()
    (source / "run.py").write_text("print('not the saved experiment')")
    (request.run_folder / "run.py").write_text("print('saved experiment')")
    (request.run_folder / "results_0.txt").write_text("keep results")
    (request.run_folder / "skip.large").write_text("excluded on rerun")
    settings.write_config_file(request.run_folder / ".encap_history.conf", {
        "backend": "dataproc", "script_name": "run.py", "rsync_exclude_push": ["*.large"],
        "plugins": {"dataproc": backend.config},
    })
    loader = importlib.machinery.SourceFileLoader(
        "encap_rerun_test_cli", str(Path(backends.__file__).resolve().parents[1] / "encap"),
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    monkeypatch.chdir(source.parent)
    monkeypatch.setattr(settings, "config", {"file_extension": {"py": "python -u"}})
    monkeypatch.setattr(settings, "args_config", {})
    monkeypatch.setattr(settings, "using_slurm", False)
    monkeypatch.setattr(settings, "using_pueue", False)
    monkeypatch.setattr(backends, "discover", lambda: {"dataproc": registration})
    monkeypatch.setattr(autopull, "watch", Mock(return_value={
        "enabled": True, "interval_seconds": 60, "pid": 123,
    }))
    monkeypatch.setattr(DataprocBackend, "_check_buckets", backend._check_buckets)
    monkeypatch.setattr(DataprocBackend, "_api", backend._api)
    archives = []
    def command(arguments, stream=False):
        for argument in arguments:
            if argument.endswith("/payload.zip") and not argument.startswith("gs://"):
                with zipfile.ZipFile(argument) as archive:
                    archives.append({name: archive.read(name) for name in archive.namelist()})
        return Mock(stdout="")
    monkeypatch.setattr(DataprocBackend, "_command", staticmethod(command))
    tail, pull = Mock(), Mock()
    monkeypatch.setattr(DataprocBackend, "tail", tail)
    monkeypatch.setattr(DataprocBackend, "pull", pull)
    monkeypatch.setattr(sys, "argv", [
        "encap", "rerun", "training", "-sn", "run.py", "-n", "trial", "-gcp_i", "[1, 2, 3]",
    ])
    backend._api.reset_mock()
    module.main()
    group = RunHandle(**writer.read()["handle"])
    assert [child["instance"] for child in group.data["instances"]] == [0, 1, 2, 3]
    assert group.data["instances"][0]["handle"] == asdict(first)
    assert backend._api.call_count == 3
    assert len(archives) == 1
    prefix = request.run_folder.relative_to(Path(backend.config["workspace_root"])).as_posix()
    assert archives[0][prefix + "/run.py"] == b"print('saved experiment')"
    assert prefix + "/skip.large" not in archives[0]
    assert (request.run_folder / "results_0.txt").read_text() == "keep results"
    assert not (request.record_dir / "workspace").exists()
    tail.assert_called_once()
    pull.assert_called_once()
    assert not store.deleted


@pytest.mark.parametrize("layout", ["legacy", "mirrored"])
def test_rerun_partial_submission_keeps_previous_and_all_recovery_handles(mirrored, layout):
    backend, request, _ = mirrored
    if layout == "legacy":
        backend.config.pop("workspace_root")
    writer = RecordWriter(request.record_dir)
    first = backend.submit(request, writer)
    backend.config["i"] = [1, 2, 3]
    backend._api.side_effect = [
        {"name": "projects/test-project/regions/europe-west1/operations/one"},
        MonitoringError("acceptance is uncertain"),
    ]
    with pytest.raises(MonitoringError, match="acceptance is uncertain"):
        backend.rerun(request, first, writer)
    saved = writer.read()
    assert saved["state"] == "submission-unconfirmed"
    children = saved["handle"]["data"]["instances"]
    assert [child["instance"] for child in children] == [0, 1, 2, 3]
    assert children[0]["handle"] == asdict(first)
    assert [child["submission_phase"] for child in children] == [
        "submitted", "submitted", "request-started", "prepared",
    ]
    assert all(child["handle"]["data"]["request_id"] for child in children)


@pytest.mark.parametrize("failure", ["missing", "foreign", "concurrent"])
def test_rerun_refuses_changed_cloud_ownership_without_submitting(mirrored, failure):
    backend, request, store = mirrored
    writer = RecordWriter(request.record_dir)
    first = backend.submit(request, writer)
    before = writer.read()
    key = next(iter(store.data))
    if failure == "missing":
        store.data.pop(key)
    elif failure == "foreign":
        record, generation = store.read(key)
        record["instances"][0]["handle"]["data"]["request_id"] = "foreign-owner"
        store.write(key, record, generation)
    else:
        store.write = Mock(side_effect=StorageConflict("concurrent change"))
    backend.config["i"] = [1, 2, 3]
    backend._api.reset_mock()
    with pytest.raises(BackendError, match="ownership|concurrent change"):
        backend.rerun(request, first, writer)
    backend._api.assert_not_called()
    assert writer.read() == before
    assert not store.deleted


def test_legacy_rerun_preserves_zero_and_restarts_selected_completed_ids(mirrored):
    backend, request, _ = mirrored
    backend.config.pop("workspace_root")
    writer = RecordWriter(request.record_dir)
    first = backend.submit(request, writer)
    backend.config["i"] = [1, 2, 3]
    handle = backend.rerun(request, first, writer)
    assert handle.data["log_instance"] == 1
    assert [child["instance"] for child in handle.data["instances"]] == [0, 1, 2, 3]
    assert handle.data["instances"][0]["handle"] == asdict(first)
    previous_one = copy.deepcopy(handle.data["instances"][1])
    backend.config["i"] = [1]
    backend.status = Mock(return_value=RunStatus("failed", 1, cleanup_pending=False))
    restarted = backend.rerun(request, handle, writer)
    assert restarted.data["log_instance"] == 1
    assert restarted.data["instances"][0] == handle.data["instances"][0]
    assert restarted.data["instances"][1]["handle"] != previous_one["handle"]
    assert restarted.data["instances"][2:] == handle.data["instances"][2:]
    assert restarted.data["history"] == [previous_one]


@pytest.mark.parametrize("layout", ["mirrored", "legacy"])
def test_rerun_saves_first_selected_instance_not_first_retained_instance(mirrored, layout):
    backend, request, _ = mirrored
    if layout == "legacy":
        backend.config.pop("workspace_root")
    backend.config["i"] = [0, 1, 2]
    writer = RecordWriter(request.record_dir)
    first = backend.submit(request, writer)
    assert first.data["log_instance"] == 0
    backend.config["i"] = [2, 1]
    backend.status = Mock(return_value=RunStatus("succeeded", 0, outputs_finalized=True, cleanup_pending=False))
    restarted = backend.rerun(request, first, writer)
    assert [child["instance"] for child in restarted.data["instances"]] == [0, 1, 2]
    assert restarted.data["log_instance"] == 2
    assert writer.read()["handle"]["data"]["log_instance"] == 2


def test_root_input_metadata_is_sent_without_laptop_paths(mirrored):
    backend, request, _ = mirrored
    root = Path(backend.config["workspace_root"])
    backend.config["inputs"] = {"{root}/experiments/0encap_folder/seed": "gs://input-bucket/seed/"}
    request = replace(request, workspace_origin=root / "experiments/training")
    worker_requests = []
    def command(arguments, stream=False):
        for argument in arguments:
            if argument.endswith("/request.json") and not argument.startswith("gs://"):
                worker_requests.append(json.loads(Path(argument).read_text()))
        return Mock(stdout="")
    backend._command.side_effect = command
    backend.submit(request, RecordWriter(request.record_dir))
    assert len(worker_requests) == 1
    worker = worker_requests[0]
    assert worker["root_path"] == "."
    assert worker["experiment_path"] == "experiments/0encap_folder/training/trial"
    assert worker["inputs"] == backend.config["inputs"]
    assert "workspace_origin" not in worker


def test_custom_namespace_and_file_target_layout(mirrored):
    backend, request, _ = mirrored
    backend.config["output_prefix"] = "team/ml"
    file_folder = Path(backend.config["workspace_root"]) / "scripts/train/v1"
    handle = backend._new_handle(replace(request, run_folder=file_folder, run_id="v1"))
    assert handle.data["run_uri"] == "gs://test-bucket/team/ml/scripts/train/v1"


def test_same_run_name_in_other_experiment_has_distinct_path(mirrored):
    backend, request, _ = mirrored
    other = request.run_folder.parent.parent / "other/trial"
    assert backend._new_handle(request).data["run_uri"] != backend._new_handle(
        replace(request, run_folder=other),
    ).data["run_uri"]


@pytest.mark.parametrize("kind", ["root", "outside"])
def test_run_must_be_beneath_workspace_root(mirrored, kind, tmp_path):
    backend, request, _ = mirrored
    folder = Path(backend.config["workspace_root"]) if kind == "root" else tmp_path / "outside"
    writer = RecordWriter(request.record_dir)
    with pytest.raises(ConfigurationError, match="strictly beneath"):
        backend.submit(replace(request, run_folder=folder), writer)
    backend._check_buckets.assert_not_called()
    backend._command.assert_not_called()


def test_workspace_root_origin_and_validation(mirrored, tmp_path):
    backend, _, _ = mirrored
    config = dict(backend.config, workspace_root="projects")
    assert validate_config(PluginConfig(config, str(tmp_path)))["workspace_root"] == str(tmp_path / "projects")
    with pytest.raises(ConfigurationError, match="declaring"):
        validate_config(config)
    for root in ("", "/", str(tmp_path / "missing"), 123):
        with pytest.raises(ConfigurationError):
            validate_config(dict(backend.config, workspace_root=root))


@pytest.mark.parametrize("instances,ids", [(1, [0]), ([0, 2, 7], [0, 2, 7])])
def test_submission_shares_source_and_outputs_with_distinct_requests(mirrored, instances, ids):
    backend, request, store = mirrored
    requests = []
    def command(arguments, stream=False):
        assert stream
        files = [Path(arg) for arg in arguments if arg.endswith("/request.json") and not arg.startswith("gs://")]
        if files:
            requests.append(json.loads(files[0].read_text()))
        return Mock(stdout="")
    backend._command.side_effect = command
    writer = RecordWriter(request.record_dir)
    backend.config["i"] = instances
    handle = backend.submit(request, writer)
    children = [RunHandle(**child["handle"]) for child in handle.data["instances"]] if handle.version == 2 else [handle]
    assert len({child.data["run_uri"] for child in children}) == 1
    assert len({child.data["request_id"] for child in children}) == len(ids)
    assert len({child.data["result_uri"] for child in children}) == len(ids)
    assert [record["instance"] for record in requests] == ids
    commands = [call.args[0] for call in backend._command.call_args_list]
    assert sum(any(arg.endswith("/payload.zip") for arg in command) for command in commands) == 1
    assert len(commands) == len(ids) + 1
    uri = children[0].data["run_uri"]
    assert (request.record_dir / "bucket_location").read_text() == uri + "\n"
    assert all(record["storage_layout"] == "mirrored" for record in requests)
    assert all("/outputs" not in record["data_url"] for record in requests)
    control = store.read(uri[5:].split("/", 1)[1] + "/.encap-run/control.json")[0]
    assert all(child["phase"] == "submitted" and child["handle"]["data"]["operation"]
               for child in control["instances"])
    assert writer.read()["state"] == "submitted"


@pytest.fixture
def occupied(mirrored):
    backend, request, store = mirrored
    handle = backend._new_handle(request)
    handle.data["operation"] = "projects/test-project/regions/europe-west1/operations/previous"
    uri = handle.data["run_uri"]
    key = uri[5:].split("/", 1)[1] + "/.encap-run/control.json"
    record = {"version": 1, "run_uri": uri,
              "instances": [{"instance": 0, "handle": asdict(handle), "phase": "submitted"}]}
    store.write(key, record, 0)
    prefix = key.removesuffix(".encap-run/control.json")
    store.write(prefix + "result.txt", {"old": True}, 0)
    store.write(prefix.rstrip("/") + "0/result.txt", {"sibling": True}, 0)
    backend.status = Mock(return_value=RunStatus("succeeded", 0, outputs_finalized=True))
    return backend, request, store, handle, key


@pytest.mark.parametrize("answer", ["y", "Y", " y ", "yes", "YES"])
def test_confirmed_replacement_deletes_only_exact_run(occupied, monkeypatch, answer):
    backend, _, store, handle, key = occupied
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    prompt = Mock(return_value=answer)
    monkeypatch.setattr("builtins.input", prompt)
    claim = RunClaim.acquire(store, handle.data["run_uri"], [], backend)
    assert store.deleted == [key.removesuffix(".encap-run/control.json") + "result.txt"]
    assert store.read(key)[0] == claim.record
    assert any(name.endswith("trial0/result.txt") for name in store.data)
    assert handle.data["run_uri"] in prompt.call_args.args[0]
    assert prompt.call_args.args[0].endswith(" y/n: ")
    assert backend.status.call_count == 2


@pytest.mark.parametrize("answer", ["n", "N", "no", "", "maybe"])
def test_deletion_decline_leaves_data_unchanged(occupied, monkeypatch, answer):
    backend, _, store, handle, _ = occupied
    before = copy.deepcopy(store.data)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda text: answer)
    with pytest.raises(BackendError, match="not approved") as error:
        RunClaim.acquire(store, handle.data["run_uri"], [], backend)
    assert "Rerun the same encap run command and answer y" in str(error.value)
    assert "different run name with -n to keep old results" in str(error.value)
    assert store.data == before
    assert not store.deleted


@pytest.mark.parametrize("instances", [1, [0]])
def test_declined_cloud_replacement_can_be_retried(occupied, monkeypatch, instances):
    backend, request, store, _, _ = occupied
    backend.config["i"] = instances
    before = copy.deepcopy(store.data)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    prompt = Mock(side_effect=["n", "y"])
    monkeypatch.setattr("builtins.input", prompt)
    writer = RecordWriter(request.record_dir)
    with pytest.raises(BackendError, match="not approved"):
        backend.submit(request, writer)
    assert writer.read()["state"] == "not-submitted"
    assert writer.read()["handle"] is None
    assert store.data == before
    backend._api.assert_not_called()
    backend._command.assert_not_called()
    handle = backend.submit(request, writer)
    assert writer.read()["state"] == "submitted"
    assert writer.read()["handle"] == asdict(handle)
    backend._api.assert_called_once()
    assert prompt.call_count == 2


@pytest.mark.parametrize("state,cleanup", [("running", True), ("unknown", False), ("succeeded", True)])
def test_active_uncertain_or_cleanup_pending_runs_cannot_be_deleted(occupied, monkeypatch, state, cleanup):
    backend, _, store, handle, _ = occupied
    backend.status.return_value = RunStatus(state, cleanup_pending=cleanup)
    prompt = Mock()
    monkeypatch.setattr("builtins.input", prompt)
    with pytest.raises(BackendError, match="active or uncertain"):
        RunClaim.acquire(store, handle.data["run_uri"], [], backend)
    prompt.assert_not_called()
    assert not store.deleted


def test_noninteractive_cloud_deletion_is_blocked(occupied, monkeypatch):
    backend, _, store, handle, _ = occupied
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    with pytest.raises(BackendError, match="interactive") as error:
        RunClaim.acquire(store, handle.data["run_uri"], [], backend)
    assert "encap run command in an interactive terminal and answer y" in str(error.value)
    assert "different run name with -n to keep old results" in str(error.value)
    assert not store.deleted


def test_confirmation_rechecks_generation(occupied, monkeypatch):
    backend, _, store, handle, key = occupied
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    def confirm(text):
        record, generation = store.read(key)
        store.write(key, record, generation)
        return "yes"
    monkeypatch.setattr("builtins.input", confirm)
    with pytest.raises(StorageConflict):
        RunClaim.acquire(store, handle.data["run_uri"], [], backend)
    assert not store.deleted


def test_untracked_cloud_objects_cannot_be_deleted(mirrored):
    backend, request, store = mirrored
    uri = backend._new_handle(request).data["run_uri"]
    store.write(uri[5:].split("/", 1)[1] + "/foreign.txt", {}, 0)
    with pytest.raises(BackendError, match="untracked"):
        RunClaim.acquire(store, uri, [], backend)
    assert not store.deleted


def test_claim_generation_prevents_second_owner(mirrored):
    backend, request, store = mirrored
    uri = backend._new_handle(request).data["run_uri"]
    claim = RunClaim.acquire(store, uri, [], backend)
    with pytest.raises(StorageConflict):
        store.write(claim.key, {}, 0)
    with pytest.raises(BackendError, match="valid ownership"):
        RunClaim.acquire(store, uri, [], backend)


def test_delete_generation_failure_prevents_workflow(occupied, monkeypatch):
    backend, request, store, _, _ = occupied
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda text: "yes")
    store.delete = Mock(side_effect=StorageConflict("changed output"))
    writer = RecordWriter(request.record_dir)
    with pytest.raises(StorageConflict):
        backend.submit(request, writer)
    backend._api.assert_not_called()
    backend._command.assert_not_called()


def test_manifest_write_failure_never_submits_workflow(mirrored):
    backend, request, store = mirrored
    store.write = Mock(side_effect=BackendError("cannot reserve cloud prefix"))
    with pytest.raises(BackendError, match="cannot reserve"):
        backend.submit(request, RecordWriter(request.record_dir))
    backend._api.assert_not_called()
    backend._command.assert_not_called()


def test_invalid_new_source_never_deletes_previous_run(occupied, monkeypatch):
    backend, request, store, _, _ = occupied
    backend.config["inputs"] = {"run.py": "gs://input-bucket/run.py"}
    prompt = Mock()
    monkeypatch.setattr("builtins.input", prompt)
    with pytest.raises(ConfigurationError, match="collides"):
        backend.submit(request, RecordWriter(request.record_dir))
    assert not store.deleted
    prompt.assert_not_called()
    backend._check_buckets.assert_not_called()


def test_root_collision_never_deletes_previous_run(occupied, monkeypatch):
    backend, request, store, _, _ = occupied
    root = Path(backend.config["workspace_root"])
    request = replace(request, workspace_origin=root / "experiments/training")
    backend.config["inputs"] = {
        "{root}/experiments/0encap_folder/training/trial/run.py": "gs://input-bucket/run.py",
    }
    prompt = Mock()
    monkeypatch.setattr("builtins.input", prompt)
    with pytest.raises(ConfigurationError, match="collides"):
        backend.submit(request, RecordWriter(request.record_dir))
    assert not store.deleted
    prompt.assert_not_called()
    backend._check_buckets.assert_not_called()


def test_mirrored_workspace_cleanup_preserves_record_and_location(mirrored):
    backend, request, _ = mirrored
    workspace = request.record_dir / "workspace"
    workspace.mkdir()
    for name in ("run.py", ".encap.conf", ".encap_history.conf"):
        (workspace / name).write_bytes((request.run_folder / name).read_bytes())
    handle = backend.submit(replace(request, workspace_folder=workspace), RecordWriter(request.record_dir))
    assert not workspace.exists()
    assert (request.record_dir / "run.json").is_file()
    assert (request.record_dir / "bucket_location").read_text() == handle.data["run_uri"] + "\n"


def test_prepared_previous_run_is_not_assumed_safe_to_replace(occupied):
    backend, _, store, handle, key = occupied
    record, generation = store.read(key)
    record["instances"][0]["phase"] = "prepared"
    store.write(key, record, generation)
    with pytest.raises(BackendError, match="active or uncertain"):
        RunClaim.acquire(store, handle.data["run_uri"], [], backend)
    assert not store.deleted


def test_cloud_reservation_rejects_bucket_root(mirrored):
    backend, _, store = mirrored
    for uri in ("gs://test-bucket", "gs://test-bucket/", "gs://other-bucket/run"):
        with pytest.raises(BackendError, match="exact run prefix"):
            RunClaim.acquire(store, uri, [], backend)
    assert not store.data


def test_confirmation_eof_leaves_cloud_unchanged(occupied, monkeypatch):
    backend, _, store, handle, _ = occupied
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", Mock(side_effect=EOFError()))
    with pytest.raises(BackendError, match="cancelled") as error:
        RunClaim.acquire(store, handle.data["run_uri"], [], backend)
    assert "encap run command in an interactive terminal and answer y" in str(error.value)
    assert "different run name with -n to keep old results" in str(error.value)
    assert not store.deleted


@pytest.mark.parametrize("authuser", [None, "0", "1", "user+test@example.com"])
def test_storage_url_targets_saved_outputs_with_console_state(mirrored, authuser):
    backend, request, _ = mirrored
    handle = backend._new_handle(request)
    handle.data["output_uri"] = "gs://saved-bucket/experiment with spaces/outputs"
    handle.data["settings"] = {} if authuser is None else {"console_authuser": authuser}
    url = urlsplit(backend._links(handle.data)["results"])
    assert url.path == "/storage/browser/saved-bucket/experiment%20with%20spaces/outputs"
    parameters = parse_qs(url.query)
    assert parameters["project"] == ["test-project"]
    assert parameters["pageState"] == ['("StorageObjectListTable":("f":"%5B%5D"))']
    assert parameters.get("authuser") == (None if authuser is None else [authuser])


def test_storage_url_matches_requested_legacy_link(mirrored):
    backend, request, _ = mirrored
    handle = backend._new_handle(request)
    handle.data.update(project="example-project", output_uri="gs://example-output-bucket/old/outputs")
    assert backend._links(handle.data)["results"] == (
        "https://console.cloud.google.com/storage/browser/example-output-bucket/old/outputs"
        "?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))"
        "&authuser=0&project=example-project"
    )


def test_job_url_matches_requested_summary_link(mirrored):
    backend, request, _ = mirrored
    handle = backend._new_handle(request)
    handle.data.update(
        project="example-project", region="example-region",
        job_id="example-job-123",
    )
    assert backend._links(handle.data)["jobs"] == (
        "https://console.cloud.google.com/dataproc/jobs/example-job-123/summary"
        "?region=example-region&authuser=0&project=example-project"
    )


@pytest.mark.parametrize("authuser", [None, "0", "user+test@example.com"])
def test_job_url_uses_saved_id_region_and_account(mirrored, authuser):
    backend, request, _ = mirrored
    handle = backend._new_handle(request)
    handle.data.update(job_id="job/with spaces", region="saved-region")
    handle.data["settings"] = {} if authuser is None else {"console_authuser": authuser}
    url = urlsplit(backend._links(handle.data)["jobs"])
    assert url.path == "/dataproc/jobs/job%2Fwith%20spaces/summary"
    parameters = parse_qs(url.query)
    assert parameters["region"] == ["saved-region"]
    assert parameters["project"] == ["test-project"]
    assert parameters.get("authuser") == (None if authuser is None else [authuser])


def test_gcs_store_pagination_and_generation_conditions(monkeypatch):
    store = GCSStore("bucket", Mock())
    store._request = Mock(side_effect=[
        {"items": [{"name": "run/a", "generation": "1"}], "nextPageToken": "next"},
        {"items": [{"name": "run/b", "generation": "2"}]},
        {"generation": "3"}, {},
    ])
    assert len(list(store.objects("run/"))) == 2
    assert store.write("run/.encap-run/control.json", {"ok": True}, "2") == "3"
    store.delete("run/b", "2")
    calls = store._request.call_args_list
    assert parse_qs(urlsplit(calls[1].args[1]).query)["pageToken"] == ["next"]
    assert parse_qs(urlsplit(calls[2].args[1]).query)["ifGenerationMatch"] == ["2"]
    assert parse_qs(urlsplit(calls[3].args[1]).query)["ifGenerationMatch"] == ["2"]


@pytest.mark.parametrize("code,error_type", [(403, BackendError), (412, StorageConflict)])
def test_gcs_http_errors_are_explicit(monkeypatch, code, error_type):
    command = Mock(return_value=Mock(stdout="token"))
    store = GCSStore("bucket", command)
    monkeypatch.setattr("encap_dataproc.storage.urlopen",
                        Mock(side_effect=HTTPError("url", code, "error", {}, io.BytesIO(b"denied"))))
    with pytest.raises(error_type):
        store.read("key")


def test_gcs_missing_object_and_transport_error_are_distinct(monkeypatch):
    store = GCSStore("bucket", Mock(return_value=Mock(stdout="token")))
    opener = Mock(side_effect=[
        HTTPError("url", 404, "missing", {}, io.BytesIO(b"missing")), URLError("offline"),
    ])
    monkeypatch.setattr("encap_dataproc.storage.urlopen", opener)
    assert store.read("key") is None
    with pytest.raises(BackendError, match="offline"):
        store.read("key")


@pytest.fixture
def checkpoint(monkeypatch):
    class CloudError(Exception):
        pass
    class PreconditionFailed(CloudError):
        pass
    modules = {
        "google": types.ModuleType("google"), "google.auth": types.ModuleType("google.auth"),
        "google.auth.compute_engine": types.SimpleNamespace(Credentials=object),
        "google.auth.exceptions": types.SimpleNamespace(GoogleAuthError=CloudError),
        "google.auth.transport": types.ModuleType("google.auth.transport"),
        "google.auth.transport.requests": types.SimpleNamespace(Request=Mock()),
        "google.cloud": types.ModuleType("google.cloud"),
        "google.cloud.storage": types.SimpleNamespace(Client=Mock()),
        "google.api_core": types.ModuleType("google.api_core"),
        "google.api_core.exceptions": types.SimpleNamespace(GoogleAPIError=CloudError, PreconditionFailed=PreconditionFailed),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    path = Path(__file__).resolve().parents[1] / "encap_dataproc/checkpoint.py"
    spec = importlib.util.spec_from_file_location("mirrored_checkpoint_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, PreconditionFailed


@pytest.mark.parametrize("filename,duplicate", [
    ("metrics.csv", "7_metrics_dup.csv"), ("result", "7_result_dup"),
    ("archive.tar.gz", "7_archive.tar_dup.gz"), ("nested/result.json", "nested/7_result_dup.json"),
])
def test_conflicting_outputs_get_instance_duplicate_name(checkpoint, tmp_path, filename, duplicate):
    module, conflict = checkpoint
    path = tmp_path / filename
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(b"new result")
    bucket = Mock(name="test-bucket")
    bucket.name = "test-bucket"
    objects = {f"run/{filename}": b"previous result"}
    def blob(key):
        def upload(source, if_generation_match):
            assert if_generation_match == 0
            if key in objects:
                raise conflict()
            objects[key] = Path(source).read_bytes()
        return Mock(upload_from_filename=upload)
    def get(key):
        return types.SimpleNamespace(
            generation="1", download_to_file=lambda stream, if_generation_match: stream.write(objects[key]),
        )
    bucket.blob.side_effect = blob
    bucket.get_blob.side_effect = get
    module.create_client = Mock(return_value=Mock(bucket=Mock(return_value=bucket)))
    module.upload_checkpoint(tmp_path, "gs://test-bucket/run", "project", merge_instance=7)
    assert objects[f"run/{filename}"] == b"previous result"
    assert objects[f"run/{duplicate}"] == b"new result"
    module.upload_checkpoint(tmp_path, "gs://test-bucket/run", "project", merge_instance=7)
    assert len(objects) == 2
    objects[f"run/{filename}"] = b"new result"
    del objects[f"run/{duplicate}"]
    module.upload_checkpoint(tmp_path, "gs://test-bucket/run", "project", merge_instance=7)
    assert len(objects) == 1


def test_different_content_at_duplicate_path_is_an_explicit_failure(checkpoint, tmp_path):
    module, conflict = checkpoint
    (tmp_path / "result.txt").write_text("new result")
    bucket = Mock()
    bucket.blob.return_value.upload_from_filename.side_effect = conflict()
    bucket.get_blob.return_value.generation = "1"
    bucket.get_blob.return_value.download_to_file.side_effect = (
        lambda stream, if_generation_match: stream.write(b"other content")
    )
    module.create_client = Mock(return_value=Mock(bucket=Mock(return_value=bucket)))
    with pytest.raises(RuntimeError, match="Duplicate output also"):
        module.upload_checkpoint(tmp_path, "gs://test-bucket/run", "project", merge_instance=7)


def test_two_real_worker_runs_merge_outputs_and_finalize_separately(
    mirrored, checkpoint, tmp_path, monkeypatch,
):
    from encap_dataproc.backend import create_archive
    from encap_dataproc import inputs

    backend, request, _ = mirrored
    module, conflict = checkpoint
    (request.run_folder / "run.py").write_text(
        "import os\nfrom pathlib import Path\n"
        "Path('metrics.csv').write_text(os.environ['ENCAP_PROCID'])\n"
    )
    archive = tmp_path / "payload.zip"
    create_archive(request, archive)
    objects = {}
    bucket = Mock()
    bucket.name = "test-bucket"
    def blob(key):
        result = types.SimpleNamespace(generation=None)
        def put(content, generation):
            assert generation == 0
            if key in objects:
                raise conflict()
            objects[key] = content
            result.generation = 1
        result.upload_from_filename = (
            lambda path, if_generation_match: put(Path(path).read_bytes(), if_generation_match)
        )
        result.upload_from_string = (
            lambda data, content_type, if_generation_match: put(data.encode(), if_generation_match)
        )
        def upload_log(stream, rewind, size, content_type, if_generation_match, timeout):
            if rewind:
                stream.seek(0)
            put(stream.read(size), if_generation_match)
        result.upload_from_file = upload_log
        return result
    def get(key):
        return types.SimpleNamespace(
            generation=1, download_to_file=lambda stream, if_generation_match: stream.write(objects[key]),
        ) if key in objects else None
    bucket.blob.side_effect = blob
    bucket.get_blob.side_effect = get
    module.create_client = Mock(return_value=Mock(bucket=Mock(return_value=bucket)))
    monkeypatch.setitem(sys.modules, "checkpoint", module)
    monkeypatch.setitem(sys.modules, "inputs", inputs)
    spec = importlib.util.spec_from_file_location(
        "standalone_mirrored_runner_test",
        Path(__file__).resolve().parents[1] / "encap_dataproc/runner.py",
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    for instance in (0, 7):
        handle = backend._new_handle(request, instance)
        worker_request = {
            "experiment_path": ".", "entrypoint": "run.py", "arguments": [],
            "project": "test-project", "zone": "test-zone", "run_id": "trial",
            "data_url": backend._links(handle.data)["results"],
            "storage_layout": "mirrored", "instance": instance,
        }
        def download(uri, destination, project):
            if destination.name == "request.json":
                destination.write_text(json.dumps(worker_request))
            else:
                destination.write_bytes(archive.read_bytes())
        module.download = download
        assert runner.run(handle.data["source_uri"] + "/payload.zip", handle.data["request_uri"],
                          handle.data["output_uri"], handle.data["result_uri"], "test-project") == 0
    prefix = handle.data["run_uri"][5:].split("/", 1)[1]
    assert objects[prefix + "/metrics.csv"] == b"0"
    assert objects[prefix + "/7_metrics_dup.csv"] == b"7"
    assert objects[prefix + "/run.py"] == (request.run_folder / "run.py").read_bytes()
    assert prefix + "/7_run_dup.py" not in objects
    assert b"/outputs?" not in objects[prefix + "/log"]
    assert b"authuser=0" in objects[prefix + "/log"]
    for instance in (0, 7):
        marker = json.loads(objects[f"{prefix}/.encap-run/instances/{instance}/result.json"])
        assert marker["state"] == "succeeded" and marker["outputs_finalized"]


@pytest.fixture
def log_cloud(checkpoint):
    module, conflict = checkpoint
    objects = {}
    uploads = []
    bucket = Mock()
    bucket.name = "test-bucket"

    def blob(key):
        result = types.SimpleNamespace(generation=None)

        def put(content, expected):
            current = objects.get(key, (None, 0))[1]
            if expected != current:
                raise conflict("generation mismatch")
            result.generation = len(uploads) + 1
            objects[key] = (content, result.generation)
            uploads.append((key, content, expected))

        def upload_log(stream, rewind, size, content_type, if_generation_match, timeout):
            assert rewind and timeout == 30
            assert content_type == "text/plain; charset=utf-8"
            stream.seek(0)
            data = stream.read()
            assert len(data) == size
            put(data, if_generation_match)

        result.upload_from_file = upload_log
        result.upload_from_filename = (
            lambda path, if_generation_match: put(Path(path).read_bytes(), if_generation_match)
        )
        result.upload_from_string = (
            lambda data, content_type, if_generation_match: put(data.encode(), if_generation_match)
        )
        return result

    def get(key):
        if key not in objects:
            return None
        data, generation = objects[key]

        def download(stream, if_generation_match):
            assert if_generation_match == generation
            stream.write(data)

        return types.SimpleNamespace(generation=generation, download_to_file=download)

    bucket.blob.side_effect = blob
    bucket.get_blob.side_effect = get
    module.create_client = Mock(return_value=Mock(bucket=Mock(return_value=bucket)))
    return module, objects, uploads


@pytest.mark.parametrize("instance", [0, 7])
@pytest.mark.parametrize("merge_instance", [None, 7])
def test_log_checkpoint_updates_own_generation_and_finalizes_without_duplicates(
    log_cloud, tmp_path, instance, merge_instance,
):
    module, objects, uploads = log_cloud
    log = module.LogCheckpoint(tmp_path, "gs://test-bucket/run", "project", instance)
    log.path.write_bytes(b"first\n")
    log.upload()
    log.path.write_bytes(b"first\nsecond\n")
    log.upload()
    log.path.write_bytes(b"first\nsecond\nfinal\n")
    (tmp_path / "log_99").write_bytes(b"other instance\n")
    (tmp_path / "result.txt").write_bytes(b"result")
    module.upload_checkpoint(
        tmp_path, "gs://test-bucket/run", "project", merge_instance=merge_instance, log_checkpoint=log,
    )
    key = f"run/{log.path.name}"
    assert objects[key][0] == b"first\nsecond\nfinal\n"
    assert [expected for name, _, expected in uploads if name == key] == [0, 1, 2]
    assert objects["run/log_99"][0] == b"other instance\n"
    assert objects["run/result.txt"][0] == b"result"
    assert set(objects) == {key, "run/log_99", "run/result.txt"}
    assert all(expected == 0 for name, _, expected in uploads if name != key)


@pytest.mark.parametrize("replace_after_upload", [False, True])
def test_log_checkpoint_preserves_foreign_objects_and_still_attempts_other_outputs(
    log_cloud, tmp_path, replace_after_upload,
):
    module, objects, _ = log_cloud
    log = module.LogCheckpoint(tmp_path, "gs://test-bucket/run", "project")
    log.path.write_bytes(b"local log\n")
    if replace_after_upload:
        log.upload()
    objects["run/log"] = (b"foreign log\n", 99)
    (tmp_path / "result.txt").write_bytes(b"result")
    with pytest.raises(RuntimeError, match="log: generation mismatch"):
        module.upload_checkpoint(
            tmp_path, "gs://test-bucket/run", "project", merge_instance=0, log_checkpoint=log,
        )
    assert objects["run/log"] == (b"foreign log\n", 99)
    assert objects["run/result.txt"][0] == b"result"
    assert set(objects) == {"run/log", "run/result.txt"}


@pytest.mark.parametrize("changed", [False, True])
def test_rerun_log_generation_allows_only_the_confirmed_previous_log(log_cloud, tmp_path, changed):
    module, objects, _ = log_cloud
    objects["run/log"] = (b"previous log\n", 99)
    log = module.LogCheckpoint(tmp_path, "gs://test-bucket/run", "project", generation=99)
    log.path.write_bytes(b"restarted log\n")
    if changed:
        objects["run/log"] = (b"concurrent log\n", 100)
        with pytest.raises(module.PreconditionFailed, match="generation mismatch"):
            log.upload()
        assert objects["run/log"] == (b"concurrent log\n", 100)
    else:
        log.upload()
        assert objects["run/log"][0] == b"restarted log\n"


def test_real_worker_rerun_finalizes_with_a_new_result_record_and_owned_log(
    mirrored, log_cloud, tmp_path, monkeypatch,
):
    from encap_dataproc.backend import create_archive
    from encap_dataproc import inputs

    backend, request, _ = mirrored
    module, objects, _ = log_cloud
    monkeypatch.setitem(sys.modules, "checkpoint", module)
    monkeypatch.setitem(sys.modules, "inputs", inputs)
    spec = importlib.util.spec_from_file_location(
        "rerun_worker_test", Path(__file__).resolve().parents[1] / "encap_dataproc/runner.py",
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    archive = tmp_path / "rerun.zip"
    create_archive(request, archive)
    handle = backend._new_handle(request)
    prefix = handle.data["run_uri"][5:].split("/", 1)[1]
    for attempt in ("first", "rerun"):
        worker_request = {
            "experiment_path": ".", "entrypoint": "run.py", "arguments": [],
            "project": "test-project", "zone": "test-zone", "run_id": "trial",
            "data_url": backend._links(handle.data)["results"], "storage_layout": "mirrored",
            "log_generation": objects[prefix + "/log"][1] if attempt == "rerun" else 0,
        }
        def download(uri, destination, project):
            if destination.name == "request.json":
                destination.write_text(json.dumps(worker_request))
            else:
                destination.write_bytes(archive.read_bytes())
        module.download = download
        result_uri = handle.data["run_uri"] + f"/.encap-run/attempts/{attempt}/result.json"
        assert runner.run(
            handle.data["source_uri"] + "/payload.zip", handle.data["request_uri"],
            handle.data["output_uri"], result_uri, "test-project",
        ) == 0
        marker = json.loads(objects[prefix + f"/.encap-run/attempts/{attempt}/result.json"][0])
        assert marker["state"] == "succeeded" and marker["outputs_finalized"]
    assert b"test" in objects[prefix + "/log"][0]


def test_log_checkpoint_bounds_snapshot_while_local_log_grows(log_cloud, tmp_path, monkeypatch):
    module, objects, _ = log_cloud
    log = module.LogCheckpoint(tmp_path, "gs://test-bucket/run", "project")
    log.path.write_bytes(b"first\n")
    fstat = module.os.fstat

    def append_after_stat(fd):
        result = fstat(fd)
        with log.path.open("ab") as stream:
            stream.write(b"second\n")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(module.os, "fstat", append_after_stat)
        log.upload()
    assert objects["run/log"][0] == b"first\n"
    log.upload()
    assert objects["run/log"][0] == b"first\nsecond\n"


def test_log_checkpoint_rejects_symlinks(log_cloud, tmp_path):
    module, objects, _ = log_cloud
    target = tmp_path / "private.txt"
    target.write_bytes(b"not a log")
    log = module.LogCheckpoint(tmp_path, "gs://test-bucket/run", "project")
    log.path.symlink_to(target)
    with pytest.raises(RuntimeError, match="Log symlinks are unsupported"):
        log.upload()
    assert not objects


@pytest.mark.parametrize("error_name", ["OSError", "GoogleAPIError", "GoogleAuthError", "RuntimeError"])
def test_periodic_log_checkpoint_waits_30_seconds_and_retries_with_warning(
    checkpoint, tmp_path, capsys, error_name,
):
    module, _ = checkpoint
    log = module.LogCheckpoint(tmp_path, "gs://test-bucket/run", "project", 7)
    log._stopped = Mock()
    log._stopped.wait.side_effect = [False, False, True]
    error_type = {"OSError": OSError, "RuntimeError": RuntimeError}.get(error_name)
    if error_type is None:
        error_type = getattr(module, error_name)
    log.upload = Mock(side_effect=[error_type("offline"), None])
    log._run()
    assert [call.args for call in log._stopped.wait.call_args_list] == [(30,), (30,), (30,)]
    assert log.upload.call_count == 2
    warning = capsys.readouterr().err
    assert "periodic log upload to gs://test-bucket/run/log_7 failed" in warning
    assert "offline" in warning and "Retrying in 30 seconds" in warning


def test_log_checkpoint_stop_waits_for_in_flight_upload(checkpoint, tmp_path, monkeypatch):
    module, _ = checkpoint
    monkeypatch.setattr(module, "LOG_UPLOAD_INTERVAL_SECONDS", 0.01)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    log = module.LogCheckpoint(tmp_path, "gs://test-bucket/run", "project")

    def upload():
        started.set()
        if not release.wait(3):
            raise RuntimeError("Test did not release the upload")
        finished.set()

    log.upload = Mock(side_effect=upload)
    with log:
        assert started.wait(3)
        timer = threading.Timer(0.05, release.set)
        timer.start()
    timer.join()
    assert finished.is_set()
    assert not log._thread.is_alive()
    log.upload.assert_called_once()


@pytest.mark.parametrize("layout,instance,code,final_failure", [
    ("mirrored", 0, 0, False),
    ("mirrored", 7, 7, False),
    ("legacy", 0, 7, True),
    ("legacy", 7, 0, True),
])
def test_worker_periodic_logs_are_saved_before_exit_and_finalized(
    log_cloud, tmp_path, monkeypatch, layout, instance, code, final_failure,
):
    from encap_dataproc import inputs

    module, objects, uploads = log_cloud
    monkeypatch.setattr(module, "LOG_UPLOAD_INTERVAL_SECONDS", 0.01)
    monkeypatch.setitem(sys.modules, "checkpoint", module)
    monkeypatch.setitem(sys.modules, "inputs", inputs)
    spec = importlib.util.spec_from_file_location(
        "periodic_log_runner_test", Path(__file__).resolve().parents[1] / "encap_dataproc/runner.py",
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    archive = tmp_path / "payload.zip"
    with zipfile.ZipFile(archive, "w") as payload:
        payload.writestr("run.py", (
            "import sys,time\nfrom pathlib import Path\n"
            "print('waiting for snapshot', flush=True)\n"
            "deadline = time.monotonic() + 3\n"
            "while not Path('snapshot-ready').exists():\n"
            "    if time.monotonic() > deadline:\n"
            "        raise RuntimeError('No periodic snapshot arrived')\n"
            "    time.sleep(0.01)\n"
            "Path('result.txt').write_text('result')\n"
            "print('snapshot received', flush=True)\n"
            f"sys.exit({code})\n"
        ))
    worker_request = {
        "entrypoint": "run.py", "arguments": [], "run_id": "trial",
        "project": "test-project", "zone": "test-zone", "data_url": "https://example.invalid",
        "storage_layout": layout, "instance": instance,
    }

    def download(uri, destination, project):
        if destination.name == "request.json":
            destination.write_text(json.dumps(worker_request))
        else:
            destination.write_bytes(archive.read_bytes())

    module.download = download
    upload = module.LogCheckpoint.upload

    def signal_snapshot(log):
        if final_failure and b"\x04\n" in log.path.read_bytes():
            raise OSError("Final log upload failed")
        upload(log)
        if b"waiting for snapshot\n" in objects[f"run/{log.path.name}"][0]:
            (log.path.parent / "snapshot-ready").touch()

    monkeypatch.setattr(module.LogCheckpoint, "upload", signal_snapshot)
    args = ("gs://test-bucket/archive", "gs://test-bucket/request",
            "gs://test-bucket/run", "gs://test-bucket/result", "test-project")
    if final_failure:
        with pytest.raises(RuntimeError, match="Final log upload failed"):
            runner.run(*args)
    else:
        assert runner.run(*args) == code
    name = "log" if instance == 0 else f"log_{instance}"
    snapshots = [data for key, data, _ in uploads if key == f"run/{name}"]
    assert snapshots and any(b"waiting for snapshot\n" in data and b"\x04\n" not in data for data in snapshots)
    saved_log = objects[f"run/{name}"][0]
    assert saved_log.endswith(b"\x04\n") is (not final_failure)
    assert objects["run/result.txt"][0] == b"result"
    result = json.loads(objects["result"][0])
    assert result["exit_code"] == code
    assert result["outputs_finalized"] is (not final_failure)
    assert result["state"] == ("succeeded" if code == 0 and not final_failure else "failed")
    assert uploads[-1][0] == "result"
    assert not any("_dup" in key for key in objects)
