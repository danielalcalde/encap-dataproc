"""Dataproc workflows and GCS; authentication uses the selected gcloud identity."""

import copy
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import zipfile
from collections import deque
from dataclasses import asdict
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from filelock import FileLock

from encap_lib.backends import (
    BackendError, ConfigurationError, MonitoringError, RecordWriter, RunHandle, RunStatus,
    wait as wait_for_completion,
)
from encap_dataproc.instances import instance_ids
from encap_dataproc.inputs import check_input_collisions, input_layout, parse_input_uri, validate_inputs
from encap_dataproc.pulling import PullExclusions
from encap_dataproc.storage import CloudWriter, GCSStore, RunClaim

REQUIRED = frozenset((
    "bucket", "project", "region", "zone", "subnet", "python", "image_uri",
    "service_account", "network_tag", "machine_type",
))
OPTIONAL = frozenset((
    "console_authuser", "output_prefix", "cluster_name", "cluster_properties", "disk_size_gb", "timeout_seconds", "inputs",
    "workspace_root", "i", "timeout",
))
EXCLUDED = frozenset((
    ".git", ".env", ".venv", "venv", ".output", "runs", "0encap_folder", ".encap-run",
    ".gcp", "__pycache__", ".pytest_cache",
    ".ruff_cache", ".DS_Store",
))
CLUSTER_PROPERTIES = {
    "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
    "yarn:yarn.nodemanager.resource.cpu-vcores": "2",
    "yarn:yarn.nodemanager.resource.memory-mb": "8192",
    "yarn:yarn.scheduler.maximum-allocation-vcores": "2",
    "yarn:yarn.scheduler.maximum-allocation-mb": "8192",
    "spark:spark.driver.memory": "2g",
    "spark:spark.driver.cores": "2",
    "spark:spark.executor.instances": "0",
    "dataproc:dataproc.allow.zero.workers": "true",
}


def timeout_seconds(config):
    if "timeout" in config and "timeout_seconds" in config:
        raise ConfigurationError("Specify only one of plugins.dataproc.timeout and plugins.dataproc.timeout_seconds")
    if "timeout" not in config:
        value = config.get("timeout_seconds", 86400)
        if type(value) is not int or value <= 0:
            raise ConfigurationError("plugins.dataproc.timeout_seconds must be a positive integer")
        return value
    value = config["timeout"]
    if type(value) is int:
        if value == -1:
            return None
        if value > 0:
            return value
    elif isinstance(value, str):
        if value in ("unlimited", "-1"):
            return None
        match = re.fullmatch(r"([0-9]+)([smhdw])", value)
        if match and int(match[1]) > 0:
            return int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match[2]]
    raise ConfigurationError(
        "plugins.dataproc.timeout must be a positive duration such as 30s, 10m, 24h, 2d or 1w, "
        "a positive integer of seconds, unlimited, or -1"
    )


def validate_config(config):
    shared_root = getattr(config, "workspace_root", None)
    missing = REQUIRED - config.keys()
    unknown = config.keys() - REQUIRED - OPTIONAL
    if missing or unknown:
        raise ConfigurationError(f"plugins.dataproc: missing {sorted(missing)}; unknown {sorted(unknown)}")
    instance_ids(config.get("i", 1))
    for key in REQUIRED | {"console_authuser", "output_prefix", "cluster_name", "workspace_root"}:
        if key in config and (not isinstance(config[key], str) or not config[key].strip()):
            raise ConfigurationError(f"plugins.dataproc.{key} must be a nonempty string")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,221}[a-z0-9]", config["bucket"]):
        raise ConfigurationError("plugins.dataproc.bucket must be a bucket name without gs://")
    for key in ("project", "region", "zone", "machine_type", "network_tag"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9:._-]*", config[key]):
            raise ConfigurationError(f"Invalid plugins.dataproc.{key}")
    if not re.fullmatch(r"projects/[^/]+/regions/[^/]+/subnetworks/[^/]+", config["subnet"]):
        raise ConfigurationError("plugins.dataproc.subnet must be a full subnetwork resource name")
    if not re.fullmatch(r"projects/[^/]+/global/images/(?:family/)?[^/]+", config["image_uri"]):
        raise ConfigurationError("plugins.dataproc.image_uri must be a full image or image-family resource name")
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.iam\.gserviceaccount\.com", config["service_account"]):
        raise ConfigurationError("plugins.dataproc.service_account must be a service-account email")
    if not config["python"].startswith("/") or "\n" in config["python"]:
        raise ConfigurationError("plugins.dataproc.python must be an absolute remote executable path")
    prefix = config.get("output_prefix", "encap")
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", prefix) or any(part in ("", ".", "..") for part in prefix.split("/")):
        raise ConfigurationError("plugins.dataproc.output_prefix must be a safe relative GCS prefix")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,33}[a-z0-9]", config.get("cluster_name", "encap-cluster")):
        raise ConfigurationError(
            "plugins.dataproc.cluster_name must be 2-35 lowercase letters, digits or hyphens, "
            "starting with a letter and ending with a letter or digit"
        )
    value = config.get("disk_size_gb", 200)
    if type(value) is not int or value <= 0:
        raise ConfigurationError("plugins.dataproc.disk_size_gb must be a positive integer")
    seconds = timeout_seconds(config)
    if seconds is not None and seconds > threading.TIMEOUT_MAX:
        raise ConfigurationError(
            "plugins.dataproc.timeout/timeout_seconds is too large for worker enforcement; use unlimited or -1"
        )
    properties = config.get("cluster_properties", {})
    if not isinstance(properties, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in properties.items()):
        raise ConfigurationError("plugins.dataproc.cluster_properties must map strings to strings")
    if any(re.search(r"credential|token|secret|password|private.?key", key, re.IGNORECASE) for key in properties):
        raise ConfigurationError("Do not put credentials in cluster_properties; use the configured service account")
    try:
        inputs = validate_inputs(config.get("inputs", {}))
        if (any(destination.startswith("{root}/") for destination in inputs)
                and "workspace_root" not in config and shared_root is None):
            raise ValueError("{root} inputs require top-level workspace_root or plugins.dataproc.workspace_root")
        input_layout(".", {
            destination: uri for destination, uri in inputs.items() if not destination.startswith("{root}/")
        })
    except ValueError as error:
        raise ConfigurationError(str(error)) from error
    resolved = dict(config)
    if "workspace_root" in config or shared_root is not None:
        root = Path(config["workspace_root"] if "workspace_root" in config else shared_root).expanduser()
        if not root.is_absolute():
            if not hasattr(config, "resolve_path"):
                raise ConfigurationError("Relative workspace_root requires a declaring configuration file")
            root = config.resolve_path("workspace_root")
        root = root.resolve()
        if not root.is_dir() or root == Path(root.anchor):
            raise ConfigurationError("workspace_root must be an existing non-filesystem-root directory")
        if shared_root is not None and root != Path(shared_root).resolve():
            raise ConfigurationError("plugins.dataproc.workspace_root must match top-level workspace_root")
        resolved["workspace_root"] = str(root)
    return resolved


def archive_layout(request, inputs, workspace_root=None):
    if (not isinstance(request.experiment_path, str) or not request.experiment_path
            or PurePosixPath(request.experiment_path).is_absolute()
            or ".." in PurePosixPath(request.experiment_path).parts or "\\" in request.experiment_path):
        raise ConfigurationError("Experiment path must be a safe relative workspace directory")
    source_prefix = PurePosixPath(".")
    experiment = PurePosixPath(request.experiment_path)
    root_path = None
    try:
        validate_inputs(inputs)
        origin = getattr(request, "workspace_origin", None)
        if any(destination.startswith("{root}/") for destination in inputs) and (
            workspace_root is None or origin is None
        ):
            raise ValueError(
                "{root} inputs require workspace_root and prepared workspace origin metadata; "
                "update Encap if the origin metadata is missing"
            )
        if origin is not None:
            origin = Path(origin)
            root = Path(workspace_root) if workspace_root is not None else origin
            if not origin.is_absolute() or not root.is_absolute():
                raise ValueError("Workspace origin and workspace_root must be absolute paths")
            origin, root = origin.resolve(), root.resolve()
            run_folder = request.run_folder.resolve()
            common = Path(os.path.commonpath([origin, root, run_folder]))
            source_prefix = PurePosixPath(origin.relative_to(common).as_posix())
            root_path = root.relative_to(common).as_posix()
            experiment = PurePosixPath(run_folder.relative_to(common).as_posix())
            if experiment == PurePosixPath("."):
                raise ValueError("Saved run directory cannot replace the workspace root")
        prefix, experiment_path, targets = input_layout(
            experiment.as_posix(), inputs, root_path,
        )
    except ValueError as error:
        raise ConfigurationError(str(error)) from error
    return prefix / source_prefix, experiment_path, targets, (
        (prefix / root_path).as_posix() if root_path is not None else None
    )


def create_archive(request, destination, inputs=None, workspace_root=None):
    source = (request.workspace_folder or request.run_folder).resolve()
    prefix, experiment_path, targets, _ = archive_layout(request, inputs or {}, workspace_root)
    experiment = source / request.experiment_path
    if (experiment.is_symlink() or not experiment.resolve().is_relative_to(source)
            or not experiment.is_dir()
            or not all((experiment / name).is_file() for name in (".encap.conf", ".encap_history.conf"))):
        raise ConfigurationError("Dataproc requires a prepared run folder with Encap configuration and history")
    paths = sorted(source.rglob("*"))
    original_experiment = Path(request.experiment_path)
    dependencies = tuple(Path(path) for path in getattr(request, "workspace_dependencies", ()))
    if any(path.is_absolute() or ".." in path.parts or "\\" in str(path) for path in dependencies):
        raise ConfigurationError("Dependency metadata must use safe relative workspace paths")
    members, files, parents = set(), set(), set()
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in paths:
            relative = path.relative_to(source)
            if any(part in EXCLUDED for part in relative.parts):
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(source):
                raise ConfigurationError(f"Source symlinks are unsupported: {path}")
            if path.is_file() or (request.workspace_folder is not None and path.is_dir()):
                original = prefix / relative.as_posix()
                destinations = [original]
                if relative.is_relative_to(original_experiment):
                    local = relative.relative_to(original_experiment)
                    if request.workspace_folder is None or (request.run_folder / local).exists():
                        destinations = [PurePosixPath(experiment_path) / local.as_posix()]
                        if any(
                            relative.is_relative_to(dependency)
                            or (path.is_dir() and dependency.is_relative_to(relative))
                            for dependency in dependencies
                        ):
                            destinations.append(original)
                for archived in dict.fromkeys(destinations):
                    if (archived in members or any(parent in files for parent in archived.parents)
                            or (path.is_file() and archived in parents)):
                        raise ConfigurationError(f"Packaged dependency collides with saved run directory: {archived}")
                    members.add(archived)
                    if path.is_file():
                        files.add(archived)
                    parents.update(archived.parents)
                    archive.write(path, archived.as_posix())
        try:
            check_input_collisions(targets, [(entry.filename, entry.is_dir()) for entry in archive.infolist()])
        except ValueError as error:
            raise ConfigurationError(str(error)) from error
        if (PurePosixPath(experiment_path) / request.entrypoint).as_posix() not in archive.namelist():
            raise ConfigurationError("Entrypoint is excluded from source packaging")
    return experiment_path


def build_workflow(config, handle):
    uri = handle["run_uri"]
    source_uri = handle.get("source_uri", f"{uri}/source")
    properties = dict(CLUSTER_PROPERTIES)
    properties.update(config.get("cluster_properties", {}))
    workflow = {
        "placement": {"managedCluster": {
            "clusterName": handle["cluster_base"],
            "config": {
                "configBucket": config["bucket"],
                "gceClusterConfig": {
                    "zoneUri": config["zone"], "subnetworkUri": config["subnet"],
                    "internalIpOnly": True, "tags": [config["network_tag"]],
                    "serviceAccount": config["service_account"],
                    "serviceAccountScopes": ["https://www.googleapis.com/auth/cloud-platform"],
                },
                "masterConfig": {
                    "numInstances": 1, "machineTypeUri": config["machine_type"],
                    "imageUri": config["image_uri"],
                    "diskConfig": {"bootDiskSizeGb": config.get("disk_size_gb", 200)},
                },
                "softwareConfig": {"properties": properties},
            },
        }},
        "jobs": [{
            "stepId": handle["job_step_id"],
            "labels": {"encap-request-id": handle["request_id"]},
            "pysparkJob": {
                "mainPythonFileUri": f"{source_uri}/runner.py",
                "pythonFileUris": [f"{source_uri}/checkpoint.py", f"{source_uri}/inputs.py"],
                "args": ["--archive-uri", f"{source_uri}/payload.zip",
                         "--request-uri", handle.get("request_uri", f"{source_uri}/request.json"),
                         "--output-uri", handle["output_uri"], "--result-uri", handle["result_uri"],
                         "--project", config["project"]],
                "properties": {
                    "spark.pyspark.python": config["python"],
                    "spark.pyspark.driver.python": config["python"],
                },
            },
        }],
    }
    seconds = timeout_seconds(config)
    if seconds is not None and 600 <= seconds <= 86400:
        workflow["dagTimeout"] = f"{seconds}s"
    return workflow


class _InstanceWriter:
    def __init__(self, writer, group, child):
        self.writer, self.group, self.child = writer, group, child
        self.path = writer.path

    def save_handle(self, handle, confirmed=False):
        self.child["handle"] = asdict(handle)
        self.child["submission_phase"] = "submitted" if confirmed else "submission-unconfirmed"
        self.writer.save_handle(self.group)

    def update(self, **values):
        self.child.update(values)
        self.writer.save_handle(self.group)


class DataprocBackend:
    def __init__(self, config):
        # Empty configuration is intentional on reconnect; only the saved handle is authoritative.
        self.config = validate_config(config) if config else None
        overrides = getattr(config, "rsync_exclude_pull", None)
        self.pull_exclusions = PullExclusions(overrides) if overrides is not None else None

    @staticmethod
    def _command(command, stream=False):
        for attempt in range(2):
            reauthentication_required = False
            try:
                if stream:
                    print(shlex.join(command), flush=True)
                    output = deque(maxlen=100)
                    with subprocess.Popen(
                        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                    ) as process:
                        if process.stdout is None:
                            raise BackendError("Cannot read GCP command output")
                        for line in process.stdout:
                            print(line, end="", flush=True)
                            output.append(line)
                            reauthentication_required |= "reauthentication required" in line.lower()
                        code = process.wait()
                    result = subprocess.CompletedProcess(command, code, stderr="".join(output))
                    result.check_returncode()
                    return result
                return subprocess.run(command, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as error:
                detail = error.stderr or str(error)
                reauthentication_required |= "reauthentication required" in (
                    (error.stderr or "") + (error.stdout or "")
                ).lower()
                if attempt == 0 and reauthentication_required:
                    if not sys.stdin.isatty():
                        raise BackendError(
                            "GCP reauthentication required. Run gcloud auth login in an interactive terminal, "
                            "then retry."
                        ) from error
                    print(
                        "GCP reauthentication required; running gcloud auth login, then retrying the command once.",
                        file=sys.stderr, flush=True,
                    )
                    try:
                        subprocess.run(["gcloud", "auth", "login"], check=True)
                    except (OSError, subprocess.CalledProcessError) as login_error:
                        raise BackendError(f"GCP reauthentication failed: {login_error}") from login_error
                    continue
                raise BackendError(f"GCP command failed: {detail}") from error
            except OSError as error:
                raise BackendError(f"GCP command failed: {error}") from error

    @staticmethod
    def _job_link(data):
        parameters = {"region": data["region"]}
        authuser = data.get("settings", {}).get("console_authuser")
        if authuser is not None:
            parameters["authuser"] = authuser
        parameters["project"] = data["project"]
        return (
            f"https://console.cloud.google.com/dataproc/jobs/{quote(data['job_id'], safe='')}/summary"
            f"?{urlencode(parameters)}"
        )

    @classmethod
    def _links(cls, data):
        parameters = {"project": data["project"]}
        authuser = data.get("settings", {}).get("console_authuser")
        if authuser is not None:
            parameters["authuser"] = authuser
        query = urlencode(parameters)
        jobs_url = f"https://console.cloud.google.com/dataproc/jobs?{query}"
        if data.get("job_id"):
            jobs_url = cls._job_link(data)
        storage_parameters = {
            "pageState": '("StorageObjectListTable":("f":"%5B%5D"))',
        }
        if authuser is not None:
            storage_parameters["authuser"] = authuser
        storage_parameters["project"] = data["project"]
        storage_query = urlencode(storage_parameters, quote_via=quote, safe="():")
        return {
            "workflows": f"https://console.cloud.google.com/dataproc/workflows?{query}",
            "jobs": jobs_url,
            "results": (
                f"https://console.cloud.google.com/storage/browser/{quote(data['output_uri'][5:], safe='/')}"
                f"?{storage_query}"
            ),
        }

    def _print_context(self, data):
        print(f"Run: {data['run_uri']}", flush=True)
        machine = data.get("settings", {}).get("machine_type")
        suffix = f" ({machine})" if machine else ""
        print(f"Managed cluster base: {data['cluster_base']}{suffix}", flush=True)
        for label, link in self._links(data).items():
            print(f"{label}: {link}", flush=True)
        if data.get("operation"):
            print(f"Workflow operation: {data['operation']}", flush=True)

    def _startup_error(self, data, operation):
        metadata = operation.get("metadata", {})
        messages = [f"Workflow ended without a computation job: {json.dumps(operation.get('error'))}"]
        creation = metadata.get("createCluster", {})
        if creation.get("error"):
            messages.append(f"Cluster creation failed: {creation['error'].strip()}")
        cleanup = metadata.get("deleteCluster", {})
        if cleanup.get("error"):
            messages.append(f"Cluster cleanup failed: {cleanup['error']}")
        elif cleanup.get("done"):
            messages.append("Managed cluster cleanup completed.")
        else:
            messages.append("Managed cluster cleanup is not confirmed complete.")
        messages.append(f"workflows: {self._links(data)['workflows']}")
        return "\n".join(messages)

    def _api(self, method, resource, body=None, parameters=None):
        token = self._command(["gcloud", "auth", "print-access-token"]).stdout.strip()
        if not token:
            raise BackendError("gcloud returned no access token; authenticate with gcloud auth login")
        url = "https://dataproc.googleapis.com/v1/" + resource
        if parameters:
            url += "?" + urlencode(parameters)
        request = Request(url, method=method,
                          data=None if body is None else json.dumps(body).encode(),
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=60) as response:
                data = json.load(response)
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise MonitoringError(f"Dataproc HTTP {error.code}: {detail}") from error
        except (URLError, TimeoutError, OSError, ValueError) as error:
            raise MonitoringError(f"Cannot contact Dataproc: {error}") from error
        if not isinstance(data, dict):
            raise MonitoringError("Dataproc returned a non-object response")
        return data

    def _job_identity(self, job):
        pyspark = job.get("pysparkJob", {})
        arguments = pyspark.get("args", []) if isinstance(pyspark, dict) else []
        if not isinstance(arguments, list) or any(not isinstance(value, str) for value in arguments):
            raise MonitoringError("Encap job has invalid request arguments")
        positions = [index for index, value in enumerate(arguments) if value == "--request-uri"]
        if len(positions) != 1 or positions[0] + 1 >= len(arguments):
            raise MonitoringError("Encap job has no unambiguous request URI; cannot determine its experiment name")
        uri = arguments[positions[0] + 1]
        try:
            bucket, key = parse_input_uri(uri)
        except ValueError as error:
            raise MonitoringError(f"Invalid Encap request URI: {uri}") from error
        if not key:
            raise MonitoringError(f"Encap request URI has no object path: {uri}")
        try:
            saved = GCSStore(bucket, self._command).read(key)
        except BackendError as error:
            raise MonitoringError(f"Cannot read Encap request metadata {uri}: {error}") from error
        if saved is None:
            raise MonitoringError(f"Encap request metadata is missing: {uri}")
        request, _ = saved
        if not isinstance(request, dict):
            raise MonitoringError(f"Invalid Encap request metadata: {uri}")
        name = request.get("run_id")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name):
            raise MonitoringError(f"Encap request metadata has no valid run name: {uri}")
        source = request.get("source")
        if source is None:
            # Older requests only stored the prepared run folder, not the original source.
            folder = request.get("display_folder")
            if request.get("storage_layout") == "mirrored" and "root_path" in request:
                root = request["root_path"]
                experiment = request.get("experiment_path")
                if not isinstance(root, str) or not root or not isinstance(experiment, str) or not experiment:
                    raise MonitoringError(f"Encap request metadata has invalid workspace paths: {uri}")
                try:
                    folder = PurePosixPath(experiment).relative_to(PurePosixPath(root)).as_posix()
                except ValueError as error:
                    raise MonitoringError(f"Encap experiment is outside its archived workspace root: {uri}") from error
            if not isinstance(folder, str) or not folder or PurePosixPath(folder).name != name:
                raise MonitoringError(f"Encap request metadata has no source or matching run folder: {uri}")
            path = PurePosixPath(folder).parent
            if path.parent.name == "0encap_folder":
                source = (path.parent.parent / path.name).as_posix()
            else:
                entrypoint = request.get("entrypoint")
                if (not isinstance(entrypoint, str) or not entrypoint
                        or PurePosixPath(entrypoint).name != entrypoint or entrypoint in (".", "..")):
                    raise MonitoringError(f"Encap file request metadata has no valid entrypoint: {uri}")
                source = (path.parent / entrypoint).as_posix()
        if (not isinstance(source, str) or not source
                or any(ord(char) < 32 or ord(char) == 127 for char in source)):
            raise MonitoringError(f"Encap request metadata has an invalid source: {uri}")
        instance = request.get("instance", 0)
        multiple = request.get("multiple_instances", False)
        if type(instance) is not int or instance < 0 or type(multiple) is not bool:
            raise MonitoringError(f"Encap request metadata has invalid instance information: {uri}")
        return {"source": source, "name": name, "instance": instance, "multiple_instances": multiple}

    @classmethod
    def list_runs(cls, config):
        for key in ("project", "region"):
            value = config.get(key)
            if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9][a-z0-9:._-]*", value):
                raise ConfigurationError(f"Cloud listing requires a valid plugins.dataproc.{key}")
        backend = cls({})
        statuses = []
        parameters = {"pageSize": 100, "filter": "status.state = ACTIVE AND labels.encap-request-id = *"}
        seen_tokens = set()
        while True:
            page = backend._api("GET", cls._parent(config) + "/jobs", parameters=dict(parameters))
            if page.get("unreachable"):
                raise MonitoringError(f"Dataproc could not list all jobs: {page['unreachable']}")
            jobs = page.get("jobs", [])
            if not isinstance(jobs, list):
                raise MonitoringError("Dataproc returned an invalid jobs list")
            for job in jobs:
                if not isinstance(job, dict):
                    raise MonitoringError("Dataproc returned an invalid job")
                labels = job.get("labels", {})
                if not isinstance(labels, dict):
                    raise MonitoringError("Dataproc returned invalid job labels")
                if not labels.get("encap-request-id"):
                    continue
                reference = job.get("reference", {})
                status = job.get("status", {})
                if (not isinstance(reference, dict) or not isinstance(reference.get("jobId"), str)
                        or not reference["jobId"] or not isinstance(status, dict)
                        or not isinstance(status.get("state"), str)):
                    raise MonitoringError("Dataproc returned a job without an ID or state")
                state = status["state"]
                if state in ("DONE", "ERROR", "CANCELLED"):
                    continue
                data = {
                    "project": config["project"], "region": config["region"],
                    "job_id": reference["jobId"], "settings": config,
                }
                states = {
                    "PENDING": "submitted", "SETUP_DONE": "submitted", "RUNNING": "running",
                    "CANCEL_PENDING": "running", "CANCEL_STARTED": "running", "ATTEMPT_FAILURE": "running",
                }
                statuses.append(RunStatus(states.get(state, "unknown"), details={
                    **backend._job_identity(job),
                    "project": config["project"], "region": config["region"],
                    "job_id": reference["jobId"], "job": status,
                    "placement": job.get("placement", {}), "labels": labels,
                    "link": cls._job_link(data),
                }))
            token = page.get("nextPageToken")
            if token is None or token == "":
                break
            if not isinstance(token, str) or token in seen_tokens:
                raise MonitoringError("Dataproc returned an invalid or repeated jobs page token")
            seen_tokens.add(token)
            parameters["pageToken"] = token
        return statuses

    def submit(self, request, writer):
        return self._submit(request, writer)

    def rerun(self, request, handle, writer):
        return self._submit(request, writer, previous=handle)

    def _submit(self, request, writer, previous=None):
        workspace = request.workspace_folder
        if workspace is not None and (
            workspace != request.record_dir / "workspace"
            or request.record_dir != request.run_folder / ".encap-run"
            or any(path.is_symlink() for path in (workspace, *workspace.parents))
        ):
            raise ConfigurationError("Dependency workspace must be the run's own .encap-run/workspace directory")
        try:
            return self._submit_prepared(request, writer, previous)
        finally:
            if workspace is not None and workspace.exists():
                try:
                    shutil.rmtree(workspace)
                except OSError as error:
                    raise BackendError(f"Cannot remove temporary dependency workspace {workspace}: {error}") from error

    def _check_buckets(self):
        buckets = {self.config["bucket"]}
        buckets.update(parse_input_uri(uri)[0] for uri in self.config.get("inputs", {}).values())
        for bucket in sorted(buckets):
            try:
                self._command([
                    "gcloud", "storage", "buckets", "describe", f"gs://{bucket}",
                    "--project", self.config["project"],
                ])
            except BackendError as error:
                raise ConfigurationError(
                    f"Cannot access bucket gs://{bucket}; verify that it exists and your gcloud identity "
                    f"can read its metadata: {error}"
                ) from error

    def _submit_prepared(self, request, writer, previous=None):
        if previous is None:
            writer.update(state="not-submitted")
        if self.config is None:
            raise ConfigurationError("Configure plugins.dataproc before submission")
        if shutil.which("gcloud") is None:
            raise ConfigurationError("Install gcloud and authenticate before using Dataproc")
        if Path(request.entrypoint).suffix != ".py":
            raise ConfigurationError("The initial Dataproc backend supports Python entrypoints only")
        if request.interpreter is not None:
            raise ConfigurationError("Use plugins.dataproc.python for the remote interpreter, not --interpreter")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", request.run_id):
            raise ConfigurationError("Invalid Dataproc run ID")
        ids = instance_ids(self.config.get("i", 1))
        if previous is not None:
            return self._rerun_prepared(request, writer, previous, ids)
        if "workspace_root" in self.config:
            return self._submit_mirrored(request, writer, ids)
        self._check_buckets()
        if self.config.get("i", 1) == 1:
            return self._submit_instance(request, writer, self._new_handle(request), 0, False)
        children = [
            {"instance": instance, "handle": asdict(self._new_handle(request, instance)), "submission_phase": "prepared"}
            for instance in ids
        ]
        group = RunHandle({"instances": children, "log_instance": ids[0]}, version=2)
        writer.save_handle(group)
        for child in children:
            self._submit_instance(
                request, _InstanceWriter(writer, group, child),
                RunHandle(**child["handle"]), child["instance"], len(ids) > 1,
            )
        writer.save_handle(group, confirmed=True)
        return group

    def _rerun_prepared(self, request, writer, previous, ids):
        previous = copy.deepcopy(previous)
        children = self._children(previous)
        if children is None:
            data = self._data(previous)
            children = [{
                "instance": data.get("instance", 0), "handle": asdict(previous),
                "submission_phase": "submitted",
            }]
        by_id = {child["instance"]: child for child in children}
        completed = set()
        for instance in ids:
            if instance not in by_id:
                continue
            child = by_id[instance]
            if child["submission_phase"] == "prepared":
                continue
            saved = RunHandle(**child["handle"])
            status = self.status(saved)
            child["handle"] = asdict(saved)
            if status.state not in ("succeeded", "failed", "cancelled") or status.cleanup_pending:
                raise MonitoringError(
                    f"Instance {instance} is active or uncertain; wait for confirmed workflow completion before rerun"
                )
            completed.add(instance)
        handles = [self._new_handle(request, instance) for instance in ids]
        mirrored = handles[0].data.get("layout") == "mirrored"
        for child in children:
            data = child["handle"]["data"]
            if (data.get("layout") == "mirrored") != mirrored or (
                mirrored and data["run_uri"] != handles[0].data["run_uri"]
            ):
                raise ConfigurationError("Rerun must preserve the saved run's storage layout, bucket and location")
        if mirrored:
            source_uri = handles[0].data["run_uri"] + "/.encap-run/attempts/" + handles[0].data["request_id"] + "/source"
            for handle in handles:
                data = handle.data
                attempt = f"{data['run_uri']}/.encap-run/attempts/{data['request_id']}"
                data.update(source_uri=source_uri, request_uri=attempt + "/request.json",
                            result_uri=attempt + "/result.json")
        replacements = [
            {"instance": instance, "handle": asdict(handle), "submission_phase": "prepared"}
            for instance, handle in zip(ids, handles)
        ]
        history = copy.deepcopy(previous.data.get("history", [])) if previous.version == 2 else []
        history.extend(copy.deepcopy(by_id[instance]) for instance in ids if instance in by_id)
        by_id.update({child["instance"]: child for child in replacements})
        group = RunHandle({
            "instances": list(by_id.values()), "history": history, "log_instance": ids[0],
        }, version=2)
        with tempfile.TemporaryDirectory(prefix="encap-dataproc-source-") as temporary:
            archive = Path(temporary) / "payload.zip"
            experiment_path = create_archive(
                request, archive, self.config.get("inputs", {}), self.config.get("workspace_root"),
            )
            self._check_buckets()
            claim = None
            if mirrored:
                store = GCSStore(self.config["bucket"], self._command)
                for instance, handle in zip(ids, handles):
                    if instance in completed:
                        prefix = handle.data["run_uri"][5:].split("/", 1)[1]
                        name = "log" if instance == 0 else f"log_{instance}"
                        handle.data["log_generation"] = store.generation(f"{prefix}/{name}")
                for child, handle in zip(replacements, handles):
                    child["handle"] = asdict(handle)
                claim = RunClaim.extend(store, handles[0].data["run_uri"], children, replacements)
            writer.save_handle(group)
            writer.update(computation=None, monitoring_error=None, submission_error=None)
            for index, (child, handle) in enumerate(zip(replacements, handles)):
                child_writer = _InstanceWriter(writer, group, child)
                if claim is not None:
                    cloud_index = next(
                        number for number, item in enumerate(claim.record["instances"])
                        if item["instance"] == child["instance"]
                    )
                    child_writer = CloudWriter(child_writer, claim, cloud_index)
                self._submit_instance(
                    request, child_writer, handle, child["instance"], len(by_id) > 1,
                    upload_source=not mirrored or index == 0, prepared_archive=(archive, experiment_path),
                )
        writer.save_handle(group, confirmed=True)
        return group

    def _submit_mirrored(self, request, writer, ids):
        handles = [self._new_handle(request, instance) for instance in ids]
        with tempfile.TemporaryDirectory(prefix="encap-dataproc-source-") as temporary:
            archive = Path(temporary) / "payload.zip"
            experiment_path = create_archive(
                request, archive, self.config.get("inputs", {}), self.config.get("workspace_root"),
            )
            prepared = (archive, experiment_path)
            self._check_buckets()
            children = [
                {"instance": instance, "handle": asdict(handle), "phase": "prepared"}
                for instance, handle in zip(ids, handles)
            ]
            store = GCSStore(self.config["bucket"], self._command)
            claim = RunClaim.acquire(store, handles[0].data["run_uri"], children, self)
            if self.config.get("i", 1) == 1:
                return self._submit_instance(
                    request, CloudWriter(writer, claim, 0), handles[0], ids[0], False, prepared_archive=prepared,
                )
            local_children = [
                {"instance": child["instance"], "handle": child["handle"], "submission_phase": "prepared"}
                for child in children
            ]
            group = RunHandle({"instances": local_children, "log_instance": ids[0]}, version=2)
            writer.save_handle(group)
            for index, (child, handle) in enumerate(zip(local_children, handles)):
                child_writer = CloudWriter(_InstanceWriter(writer, group, child), claim, index)
                self._submit_instance(
                    request, child_writer, handle, child["instance"], len(ids) > 1,
                    upload_source=index == 0, prepared_archive=prepared,
                )
            writer.save_handle(group, confirmed=True)
            return group

    def _new_handle(self, request, instance=0):
        config = self.config
        request_id = str(uuid.uuid4())
        run_uri = f"gs://{config['bucket']}/{config.get('output_prefix', 'encap')}/{request.run_id}-{request_id}"
        data = {
            "project": config["project"], "region": config["region"], "run_uri": run_uri,
            "output_uri": f"{run_uri}/outputs", "result_uri": f"{run_uri}/result.json",
            "request_id": request_id, "cluster_base": config.get("cluster_name", "encap-cluster"),
            "job_step_id": ("experiment-" + request.run_id.replace(".", "-"))[:50].rstrip("_-"),
            "settings": copy.deepcopy(config),
            "instance": instance,
            "rsync_exclude_pull": list(self.pull_exclusions.patterns) if self.pull_exclusions is not None else [],
        }
        if "workspace_root" in config:
            root = Path(config["workspace_root"])
            folder = request.run_folder.resolve()
            if folder == root or not folder.is_relative_to(root):
                raise ConfigurationError(f"Run directory must be strictly beneath workspace_root: {folder}")
            relative = folder.relative_to(root)
            if any(any(char in part for char in "\x00\\?#") or any(ord(char) < 32 for char in part)
                   for part in relative.parts):
                raise ConfigurationError("Run directory contains unsafe GCS path characters")
            run_uri = f"gs://{config['bucket']}/{config.get('output_prefix', 'encap')}/{relative.as_posix()}"
            data.update(
                layout="mirrored", run_uri=run_uri, output_uri=run_uri,
                source_uri=f"{run_uri}/.encap-run/source",
                request_uri=f"{run_uri}/.encap-run/instances/{instance}/request.json",
                result_uri=f"{run_uri}/.encap-run/instances/{instance}/result.json",
            )
        return RunHandle(data)

    def _submit_instance(self, request, writer, handle, instance, multiple, upload_source=True, prepared_archive=None):
        config = self.config
        run_uri = handle.data["run_uri"]
        request_id = handle.data["request_id"]
        with tempfile.TemporaryDirectory(prefix="encap-dataproc-") as temporary:
            folder = Path(temporary)
            if prepared_archive is None:
                archive = folder / "payload.zip"
                experiment_path = create_archive(
                    request, archive, config.get("inputs", {}), config.get("workspace_root"),
                )
            else:
                archive, experiment_path = prepared_archive
            worker_request = {
                "entrypoint": request.entrypoint,
                "experiment_path": experiment_path,
                "inputs": config.get("inputs", {}),
                "arguments": [argument.replace("{i}", str(instance)) for argument in request.arguments],
                "environment": request.environment, "run_id": request.run_id,
                "instance": instance, "multiple_instances": multiple,
                "display_folder": os.path.relpath(request.run_folder),
                "project": config["project"], "zone": config["zone"],
                "data_url": self._links(handle.data)["results"],
                "jobs_url": self._links(handle.data)["jobs"],
                "console_authuser": config.get("console_authuser"),
                "storage_layout": handle.data.get("layout", "legacy"),
                "log_generation": handle.data.get("log_generation", 0),
            }
            if request.source_path is not None:
                origin = Path(config["workspace_root"]) if "workspace_root" in config else Path.cwd()
                worker_request["source"] = Path(os.path.relpath(request.source_path, origin)).as_posix()
            seconds = timeout_seconds(config)
            if seconds is not None and not 600 <= seconds <= 86400:
                worker_request["timeout_seconds"] = seconds
            _, _, _, root_path = archive_layout(
                request, config.get("inputs", {}), config.get("workspace_root"),
            )
            if root_path is not None:
                worker_request["root_path"] = root_path
            request_file = folder / "request.json"
            request_file.write_text(json.dumps(worker_request), encoding="utf-8")
            workflow = build_workflow(config, handle.data)
            # The UUID, output locations and resolved non-secret infrastructure settings
            # are durable before any uploads or potentially accepted submission.
            writer.save_handle(handle)
            self._print_context(handle.data)
            print(f"Submission record: {writer.path}", flush=True)
            helpers = Path(__file__).parent
            if handle.data.get("layout") == "mirrored":
                if upload_source:
                    self._command([
                        "gcloud", "storage", "cp", str(archive), str(helpers / "runner.py"),
                        str(helpers / "checkpoint.py"), str(helpers / "inputs.py"),
                        handle.data["source_uri"] + "/", "--if-generation-match=0", "--project", config["project"],
                    ], stream=True)
                self._command([
                    "gcloud", "storage", "cp", str(request_file), handle.data["request_uri"],
                    "--if-generation-match=0", "--project", config["project"],
                ], stream=True)
            else:
                self._command([
                    "gcloud", "storage", "cp", str(archive), str(request_file),
                    str(helpers / "runner.py"), str(helpers / "checkpoint.py"), str(helpers / "inputs.py"),
                    f"{run_uri}/source/", "--if-generation-match=0", "--project", config["project"],
                ], stream=True)
            location = writer.path.parent / "bucket_location"
            try:
                if location.is_symlink():
                    raise BackendError(f"Bucket location must not be a symlink: {location}")
                locations = location.read_text().splitlines() if location.exists() else []
                if run_uri not in locations:
                    with location.open("a", encoding="utf-8") as output:
                        output.write(run_uri + "\n")
            except OSError as error:
                raise BackendError(f"Cannot save bucket location {location}: {error}") from error
            writer.update(submission_phase="request-started")
            print(
                "Submitting workflow: if interrupted before confirmation, check workflows before retrying. "
                "The request may have reached GCP.",
                flush=True,
            )
            operation = self._api(
                "POST", self._parent(handle.data) + "/workflowTemplates:instantiateInline",
                workflow, {"requestId": request_id},
            )
            name = operation.get("name")
            if not isinstance(name, str) or not name.startswith(self._parent(handle.data) + "/operations/"):
                raise MonitoringError("Submission response contains no operation name; acceptance is unconfirmed. Do not resubmit.")
            handle.data["operation"] = name
            writer.save_handle(handle, confirmed=True)
            print(f"Workflow submitted: {name}", flush=True)
            print("Submission is not job completion. GCP owns execution and cluster cleanup.", flush=True)
            return handle

    def _children(self, handle):
        if type(handle.version) is not int or handle.version != 2:
            return None
        children = handle.data.get("instances")
        if not isinstance(children, list) or not children or any(not isinstance(child, dict) for child in children):
            raise BackendError("Dataproc group handle is missing instances")
        instance_ids([child.get("instance") for child in children])
        for child in children:
            saved = child.get("handle")
            if (not isinstance(saved, dict) or set(saved) != {"data", "version"}
                    or not isinstance(saved["data"], dict)
                    or child.get("submission_phase") not in (
                        "prepared", "submission-unconfirmed", "request-started", "submitted")):
                raise BackendError("Invalid Dataproc instance recovery record")
            self._data(RunHandle(**saved))
        return children

    @staticmethod
    def _parent(data):
        return f"projects/{quote(data['project'], safe='')}/regions/{quote(data['region'], safe='')}"

    def _data(self, handle):
        if type(handle.version) is not int or handle.version != 1:
            raise BackendError(f"Unsupported Dataproc handle version {handle.version}; install a compatible plugin")
        data = handle.data
        required = ("project", "region", "run_uri", "output_uri", "result_uri", "request_id", "cluster_base")
        if any(not isinstance(data.get(key), str) or not data[key] for key in required):
            raise BackendError("Dataproc handle is missing recovery information")
        if "job_step_id" in data and (
            not isinstance(data["job_step_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{1,48}[A-Za-z0-9]", data["job_step_id"])
        ):
            raise BackendError("Invalid Dataproc job step ID in saved handle")
        for key in ("run_uri", "output_uri", "result_uri"):
            if not re.fullmatch(r"gs://[a-z0-9][a-z0-9._-]+/[^\x00\r\n]+", data[key]):
                raise BackendError(f"Invalid Dataproc {key} in saved handle")
        return data

    def _operation(self, handle):
        data = self._data(handle)
        name = data.get("operation")
        if name:
            if not isinstance(name, str) or not name.startswith(self._parent(data) + "/operations/"):
                raise MonitoringError("Invalid saved Dataproc operation name")
            return self._api("GET", name)
        # Never instantiate again. Listing can fail or lag; neither proves rejection.
        matches = []
        token = None
        while True:
            parameters = {"pageSize": 100}
            if token:
                parameters["pageToken"] = token
            page = self._api("GET", self._parent(data) + "/operations", parameters=parameters)
            for operation in page.get("operations", []):
                cluster = operation.get("metadata", {}).get("clusterName", "")
                if cluster == data["cluster_base"] or cluster.startswith(data["cluster_base"] + "-"):
                    if "job_step_id" in data:
                        # Shared cluster prefixes cannot identify a submission; job labels can.
                        nodes = operation.get("metadata", {}).get("graph", {}).get("nodes", [])
                        for node in nodes:
                            if node.get("stepId") == data["job_step_id"] and node.get("jobId"):
                                job = self._api(
                                    "GET", self._parent(data) + "/jobs/" + quote(node["jobId"], safe=""),
                                )
                                if job.get("labels", {}).get("encap-request-id") == data["request_id"]:
                                    matches.append(operation)
                                    break
                    else:
                        matches.append(operation)
            token = page.get("nextPageToken")
            if not token:
                break
        if len(matches) != 1:
            raise MonitoringError(
                f"Submission remains unconfirmed ({len(matches)} matching workflows). "
                f"Inspect cluster prefix {data['cluster_base']} in project {data['project']}, region {data['region']}. "
                f"Request ID: {data['request_id']}. "
                "Do not resubmit; no request was retried."
            )
        data["operation"] = matches[0]["name"]
        return matches[0]

    def _result(self, data):
        # GCS JSON API gives an unambiguous 404 for a not-yet-written finalization marker.
        bucket, key = data["result_uri"][5:].split("/", 1)
        token = self._command(["gcloud", "auth", "print-access-token"]).stdout.strip()
        request = Request(
            f"https://storage.googleapis.com/storage/v1/b/{quote(bucket, safe='')}/o/{quote(key, safe='')}?alt=media",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urlopen(request, timeout=60) as response:
                result = json.load(response)
        except HTTPError as error:
            if error.code == 404:
                return None
            raise MonitoringError(f"Cannot read output finalization: GCS HTTP {error.code}") from error
        except (URLError, TimeoutError, OSError, ValueError) as error:
            raise MonitoringError(f"Cannot read output finalization: {error}") from error
        if (not isinstance(result, dict) or result.get("state") not in ("succeeded", "failed")
                or type(result.get("outputs_finalized")) is not bool
                or type(result.get("exit_code")) is not int):
            raise MonitoringError("Invalid remote finalization record")
        return result

    def status(self, handle):
        children = self._children(handle)
        if children is not None:
            statuses = [
                RunStatus("unknown", details={"submission_phase": "prepared"})
                if child["submission_phase"] == "prepared"
                else self.status(RunHandle(**child["handle"]))
                for child in children
            ]
            details = {"instances": {
                str(child["instance"]): asdict(status) for child, status in zip(children, statuses)
            }}
            states = {status.state for status in statuses}
            if "unknown" in states:
                state = "unknown"
            elif states & {"submitted", "running"}:
                state = "running" if "running" in states else "submitted"
            else:
                state = "failed" if "failed" in states else "cancelled" if "cancelled" in states else "succeeded"
            code = next((status.exit_code for status in statuses
                         if status.state == state and status.exit_code), None)
            return RunStatus(
                state, 0 if state == "succeeded" else code, details,
                all(status.outputs_finalized is True for status in statuses),
                any(status.cleanup_pending for status in statuses),
            )
        data = self._data(handle)
        operation = self._operation(handle)
        metadata = operation.get("metadata", {})
        nodes = metadata.get("graph", {}).get("nodes", [])
        job_ids = [node["jobId"] for node in nodes if node.get("jobId")]
        if job_ids:
            data["job_id"] = job_ids[0]
        cleanup = not operation.get("done", False)
        details = {
            "operation": data["operation"], "output_uri": data["output_uri"],
            "cleanup": metadata.get("deleteCluster", {}), "workflow_error": operation.get("error"),
            "cluster_creation": metadata.get("createCluster", {}),
            "links": self._links(data),
        }
        if job_ids:
            job = self._api("GET", self._parent(data) + "/jobs/" + quote(job_ids[0], safe=""))
            state = job.get("status", {}).get("state", "STATE_UNSPECIFIED")
            details["job"] = job.get("status", {})
            if state in ("DONE", "ERROR", "CANCELLED"):
                result = self._result(data)
                if result is not None:
                    details["finalization"] = result
                if state == "CANCELLED":
                    return RunStatus("cancelled", 130, details, bool(result and result["outputs_finalized"]), cleanup)
                if result is not None:
                    successful = result["state"] == "succeeded" and result["outputs_finalized"] and result["exit_code"] == 0
                    return RunStatus("succeeded" if successful and state == "DONE" else "failed",
                                     result["exit_code"], details, result["outputs_finalized"], cleanup)
                if state == "DONE":
                    raise MonitoringError("Job completed without an output-finalization record; success cannot be confirmed")
                return RunStatus("failed", None, details, False, cleanup)
            if state in ("PENDING", "SETUP_DONE", "RUNNING", "CANCEL_PENDING", "CANCEL_STARTED", "ATTEMPT_FAILURE"):
                return RunStatus("running", details=details, cleanup_pending=cleanup)
            return RunStatus("unknown", details=details, cleanup_pending=cleanup)
        if operation.get("done"):
            error = operation.get("error")
            if error:
                return RunStatus("cancelled" if error.get("code") == 1 else "failed", details=details,
                                 outputs_finalized=False, cleanup_pending=False)
            return RunStatus("unknown", details=details, cleanup_pending=False)
        if metadata.get("state") in ("PENDING", "RUNNING"):
            return RunStatus("submitted", details=details, cleanup_pending=True)
        return RunStatus("unknown", details=details, cleanup_pending=cleanup)

    def ssh(self, handle, instance=None, tunnel_through_iap=False):
        if instance is not None and (type(instance) is not int or instance < 0):
            raise ConfigurationError("SSH instance must be a nonnegative integer")
        children = self._children(handle)
        if children is not None:
            ids = [child["instance"] for child in children]
            if instance is None:
                if len(children) != 1:
                    raise ConfigurationError(f"Select an SSH instance with --instance ID; available IDs: {ids}")
                instance = ids[0]
            child = next((child for child in children if child["instance"] == instance), None)
            if child is None:
                raise ConfigurationError(f"Unknown SSH instance {instance}; available IDs: {ids}")
            if child["submission_phase"] == "prepared":
                raise MonitoringError(f"Instance {instance} was not submitted; no SSH target exists")
            return self.ssh(RunHandle(**child["handle"]), tunnel_through_iap=tunnel_through_iap)

        data = self._data(handle)
        if instance is not None and instance != data.get("instance", 0):
            raise ConfigurationError(f"Unknown SSH instance {instance}; available ID: {data.get('instance', 0)}")
        operation = self._operation(handle)
        if operation.get("done"):
            raise MonitoringError("The workflow has finished; its managed cluster may already have been deleted")
        cluster = operation.get("metadata", {}).get("clusterName")
        if not isinstance(cluster, str) or not cluster:
            raise MonitoringError("The workflow has not assigned a cluster yet; retry ssh after provisioning")
        description = self._api("GET", self._parent(data) + "/clusters/" + quote(cluster, safe=""))
        config = description.get("config", {})
        masters = config.get("masterConfig", {}).get("instanceNames", [])
        zone_uri = config.get("gceClusterConfig", {}).get("zoneUri", "")
        if (not isinstance(masters, list) or not masters
                or not isinstance(masters[0], str)
                or not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", masters[0])
                or not isinstance(zone_uri, str)
                or not re.fullmatch(r"(?:https://www.googleapis.com/compute/v1/)?projects/[^/]+/zones/[a-z0-9-]+",
                                    zone_uri)):
            raise MonitoringError("Dataproc cluster has no usable master instance or zone; retry after provisioning")
        command = [
            "gcloud", "compute", "ssh", masters[0],
            "--project", data["project"], "--zone", zone_uri.rsplit("/", 1)[-1],
        ]
        if tunnel_through_iap:
            command.append("--tunnel-through-iap")
        print(shlex.join(command), flush=True)
        try:
            subprocess.run(command, check=True)
        except (OSError, subprocess.CalledProcessError) as error:
            raise BackendError(f"Dataproc SSH connection failed: {error}") from error

    def tail(self, handle):
        children = self._children(handle)
        if children is not None:
            for child in children:
                if child["submission_phase"] == "prepared":
                    raise MonitoringError(f"Instance {child['instance']} was not submitted; no automatic retry is allowed")
            instance = handle.data.get("log_instance", children[0]["instance"])
            if type(instance) is not int:
                raise MonitoringError("Invalid log instance in saved Dataproc handle")
            child = next((child for child in children if child["instance"] == instance), None)
            if child is None:
                raise MonitoringError(f"Log instance {instance} is not in the saved Dataproc group")
            print(f"Instance {instance}", flush=True)
            self.tail(RunHandle(**child["handle"]))
            if len(children) > 1:
                print("Waiting for all instances to finish; no other instance logs will be shown.", flush=True)
                status = wait_for_completion(self, handle)
                if status.state != "succeeded":
                    raise BackendError(f"Dataproc group finished with {status.state}: {status.details}")
            return
        data = self._data(handle)
        self._print_context(data)
        print("\nWaiting for the workflow's job; Ctrl-C detaches without cancelling it.", flush=True)
        while True:
            operation = self._operation(handle)
            nodes = operation.get("metadata", {}).get("graph", {}).get("nodes", [])
            ids = [node["jobId"] for node in nodes if node.get("jobId")]
            if ids:
                data["job_id"] = ids[0]
                print(f"jobs: {self._links(data)['jobs']}", flush=True)
                command = ["gcloud", "dataproc", "jobs", "wait", ids[0],
                           "--project", data["project"], "--region", data["region"]]
                try:
                    print(shlex.join(command), flush=True)
                    subprocess.run(command, check=True)
                except (OSError, subprocess.CalledProcessError) as error:
                    raise BackendError(f"Dataproc log attachment failed: {error}") from error
                print("Job finished. GCP manages the remaining workflow cleanup.", flush=True)
                return
            if operation.get("done"):
                raise MonitoringError(self._startup_error(data, operation))
            time.sleep(5)

    def _pull_inventory(self, data):
        try:
            bucket, key = parse_input_uri(data["output_uri"])
        except ValueError as error:
            raise BackendError(str(error)) from error
        prefix = key.rstrip("/") + "/"
        objects = {}
        for item in GCSStore(bucket, self._command).objects(prefix):
            if not item["name"].startswith(prefix):
                raise BackendError("GCS listing returned an object outside the output prefix")
            relative = item["name"][len(prefix):]
            parts = relative.rstrip("/").split("/")
            if (any(part in ("", ".", "..") for part in parts) or "\\" in relative
                    or any(ord(char) < 32 or ord(char) == 127 for char in relative)):
                raise BackendError(f"Unsafe GCS output path: {relative!r}")
            if ".encap-run" in parts or relative.endswith("/"):
                continue
            if not str(item.get("size", "")).isdigit() or relative in objects:
                raise BackendError(f"Invalid GCS output metadata: {relative!r}")
            objects[relative] = {
                "path": relative, "generation": str(item["generation"]),
                "size": int(item["size"]), "content_encoding": item.get("contentEncoding"),
            }
        return objects

    def pull(self, handle, destination):
        return self._download(handle, destination, live=False, automatic=False)

    def sync(self, handle, destination, final=False):
        return self._download(handle, destination, live=not final, automatic=True)

    def _download(self, handle, destination, live, automatic):
        for path in (destination, *destination.parents):
            if path.is_symlink():
                raise BackendError(f"Output path must not traverse a symlink: {path}")
        metadata = destination / ".encap-run"
        if metadata.is_symlink():
            raise BackendError(f"Output metadata must not be a symlink: {metadata}")
        metadata.mkdir(mode=0o700, parents=True, exist_ok=True)
        with FileLock(str(metadata / "pull.lock")):
            protected = {".encap.conf", ".encap_history.conf"} if automatic else set()
            if automatic and (metadata / "run.json").exists():
                record = RecordWriter(metadata).read()
                sources = record.get("preparation", {}).get("source_files", [])
                if (not isinstance(sources, list) or any(
                    not isinstance(name, str) or not name or Path(name).is_absolute()
                    or ".." in Path(name).parts or "\\" in name
                    or any(ord(char) < 32 or ord(char) == 127 for char in name)
                    for name in sources
                )):
                    raise BackendError("Invalid saved source manifest for automatic pulls")
                protected.update(sources)
            return self._pull(handle, destination, live, protected, automatic)

    def _sync_logs(self, data, destination, logs, previous):
        if not isinstance(previous, dict) or any(not isinstance(value, dict) for value in previous.values()):
            raise BackendError("Invalid previous log snapshots in retrieval record")
        for name, item in logs.items():
            path = destination / name
            if path.is_symlink():
                raise BackendError(f"Log path must not be a symlink: {path}")
            saved = previous.get(name, {})
            if (path.is_file() and saved.get("generation") == item["generation"]
                    and saved.get("size") == path.stat().st_size == item["size"]
                    and saved.get("mtime_ns") == path.stat().st_mtime_ns):
                continue
            with tempfile.TemporaryDirectory(prefix="log-pull-", dir=destination / ".encap-run") as temporary:
                staged = Path(temporary) / name
                for attempt in range(3):
                    try:
                        self._command([
                            "gcloud", "storage", "cp", data["output_uri"] + "/" + name,
                            str(staged), "--project", data["project"],
                        ])
                        break
                    except BackendError as error:
                        if attempt == 2 or not any(marker in str(error) for marker in (
                            "not found: 404", "HTTPError 404", "HTTPError 412",
                        )):
                            raise
                        print(f"Log {name} changed during download; retrying with its latest generation.", flush=True)
                if not staged.is_file():
                    raise BackendError(f"Log snapshot was not downloaded: {name}")
                if staged.stat().st_size != item["size"]:
                    current = self._pull_inventory(data).get(name)
                    if current is None or staged.stat().st_size != current["size"]:
                        raise BackendError(f"Log snapshot was not retrieved completely: {name}")
                if path.is_symlink():
                    raise BackendError(f"Log path must not be a symlink: {path}")
                os.replace(staged, path)

    def _pull(self, handle, destination, live=False, protected=(), automatic=False):
        children = self._children(handle)
        if children is not None:
            seen = set()
            errors = []
            for child in children:
                if child["submission_phase"] == "prepared":
                    if live:
                        print(f"Instance {child['instance']} is not submitted; skipping its outputs", flush=True)
                        continue
                    raise BackendError(f"Instance {child['instance']} was not submitted and has no outputs")
                saved_handle = RunHandle(**child["handle"])
                data = self._data(saved_handle)
                patterns = (
                    self.pull_exclusions.patterns if self.pull_exclusions is not None
                    else data.get("rsync_exclude_pull", [])
                )
                key = (data["output_uri"], tuple(patterns))
                if live and key in seen:
                    continue
                seen.add(key)
                try:
                    self._pull(saved_handle, destination, live, protected, automatic)
                except BackendError as error:
                    if not live:
                        raise
                    errors.append(f"Instance {child['instance']}: {error}")
            if errors:
                raise BackendError("Some output snapshots could not be pulled: " + "; ".join(errors))
            if live and not seen:
                raise BackendError("No instances have been submitted; waiting for outputs")
            return
        data = self._data(handle)
        saved = PullExclusions(data.get("rsync_exclude_pull", []))
        exclusions = self.pull_exclusions if self.pull_exclusions is not None else saved
        regex = exclusions.regex
        if protected:
            literals = [("^" + re.escape(name) + "$").replace(",", r"\x2c") for name in sorted(protected)]
            regex = "(?:" + "|".join([regex, *literals]) + ")"
        source = data["output_uri"] + "/"
        listing = self._command(["gcloud", "storage", "ls", source, "--project", data["project"]])
        if not any(line.strip() and not line.strip().rstrip("/").endswith("/.encap-run")
                   for line in listing.stdout.splitlines()):
            raise BackendError(f"No remote output files are available at {source}")
        try:
            for path in (destination, *destination.parents):
                if path.is_symlink():
                    raise BackendError(f"Output path must not traverse a symlink: {path}")
            destination.mkdir(parents=True, exist_ok=True)
            for path in destination.rglob("*"):
                if path.is_symlink():
                    raise BackendError(f"Output path must not traverse a symlink: {path}")
        except OSError as error:
            raise BackendError(f"Cannot prepare output directory: {error}") from error
        writer = RecordWriter(destination / ".encap-run")
        previous = writer.read_retrieval()
        tracked = live or bool(protected or exclusions.patterns or saved.patterns) or previous is not None
        if not tracked:
            self._command(["gcloud", "storage", "rsync", source, str(destination),
                           "--recursive", "--exclude", regex,
                           "--project", data["project"]])
            return
        inventory = self._pull_inventory(data)
        if not inventory:
            raise BackendError(f"No remote output files are available at {source}")
        omitted = [
            dict(item, local_file_present=(destination / name).is_file(),
                 reason="saved_source" if name in protected else "configured")
            for name, item in sorted(inventory.items()) if exclusions.matches(name) or name in protected
        ]
        selected = {
            name: item for name, item in inventory.items() if not exclusions.matches(name) and name not in protected
        }
        logs = {
            name: item for name, item in selected.items()
            if automatic and re.fullmatch(r"log(?:_[0-9]+)?", name)
        }
        if logs:
            regex = "(?:" + regex + r"|^log(?:_[0-9]+)?$)"
        for pattern, rule in zip(exclusions.patterns, exclusions.rules):
            if not any(rule.match(name) for name in inventory):
                print(f"Warning: rsync_exclude_pull pattern matched no remote outputs: {pattern!r}", flush=True)
        prior = previous["sources"].get(source) if previous is not None else None
        pending = list({
            item["path"]: item for item in prior["previous_omissions"] + prior["omitted"]
        }.values()) if prior else []
        request_ids = prior.get("request_ids", []) if prior else []
        if not isinstance(request_ids, list) or any(not isinstance(value, str) for value in request_ids):
            raise BackendError("Invalid request identities in retrieval record")
        details = {
            "project": data["project"], "request_id": data["request_id"],
            "request_ids": sorted(set(request_ids + [data["request_id"]])),
            "patterns": exclusions.patterns, "state": "in_progress",
            "scope": "partial" if omitted or pending else "full",
            "omitted": omitted, "previous_omissions": pending,
            "selected_count": len(selected), "integrity_verified": False,
            "live": live,
        }
        writer.save_retrieval(source, details)
        try:
            if selected.keys() - logs.keys():
                self._command(["gcloud", "storage", "rsync", source, str(destination),
                               "--recursive", "--exclude", regex,
                               "--project", data["project"]])
            if logs:
                self._sync_logs(data, destination, logs, prior.get("logs", {}) if prior else {})
            current_inventory = self._pull_inventory(data) if live else inventory
            for name, item in selected.items():
                path = destination / name
                if any(part.is_symlink() for part in (path, *path.parents)):
                    raise BackendError(f"Output path must not traverse a symlink: {path}")
                current = current_inventory.get(name, item)
                valid_sizes = {item["size"], current["size"]} if live else {item["size"]}
                if not path.is_file() or (
                    item["content_encoding"] != "gzip" and path.stat().st_size not in valid_sizes
                ):
                    raise BackendError(f"Selected output was not retrieved completely: {name}")
            if not live and self._pull_inventory(data) != inventory:
                raise BackendError("Remote outputs changed during retrieval; retry the pull")
        except (BackendError, OSError, KeyboardInterrupt) as error:
            details.update(state="failed", error=f"{type(error).__name__}: {error}")
            writer.save_retrieval(source, details)
            if isinstance(error, OSError):
                raise BackendError(f"Cannot retrieve outputs: {error}") from error
            raise
        details.update(state="succeeded", scope="partial" if omitted else "full", previous_omissions=[])
        if live:
            details["remote_stable"] = current_inventory == inventory
        if logs:
            details["logs"] = {
                name: {
                    "generation": item["generation"] if current_inventory.get(name) == item else None,
                    "size": (destination / name).stat().st_size,
                    "mtime_ns": (destination / name).stat().st_mtime_ns,
                }
                for name, item in logs.items()
            }
        writer.save_retrieval(source, details)
        if live:
            print(
                f"Live output snapshot downloaded ({len(omitted)} excluded). "
                "Active outputs may continue changing; this is not a verified final copy.",
                flush=True,
            )
        elif omitted:
            print(
                f"Partial local copy: {len(omitted)} remote output(s) deliberately skipped; "
                f"omissions recorded in {writer.directory / 'retrieval.json'}. "
                "Remote files and existing local files are retained; this is not a fully verified run.",
                flush=True,
            )
        else:
            print("All user outputs retrieved; full-run integrity verification has not been performed.", flush=True)

    def cancel(self, handle):
        children = self._children(handle)
        if children is not None:
            errors = []
            for child in children:
                if child["submission_phase"] == "prepared":
                    continue
                try:
                    self.cancel(RunHandle(**child["handle"]))
                except BackendError as error:
                    errors.append(f"Instance {child['instance']}: {error}")
            if errors:
                raise BackendError("Some cancellations failed: " + "; ".join(errors))
            return
        data = self._data(handle)
        self._operation(handle)
        self._api("POST", data["operation"] + ":cancel", {})
