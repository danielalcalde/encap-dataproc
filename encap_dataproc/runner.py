"""Stage source, execute an arbitrary Python entrypoint, then finalize outputs."""

import argparse
import json
import os
import re
import shlex
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urlencode

if __package__:
    from . import checkpoint
    from .inputs import download_inputs, input_layout
else:
    # Dataproc ships this file as the main script and checkpoint.py via pythonFileUris.
    import checkpoint
    from inputs import download_inputs, input_layout


def extract_archive(archive_path, destination):
    with zipfile.ZipFile(archive_path) as archive:
        for entry in archive.infolist():
            path = PurePosixPath(entry.filename)
            mode = entry.external_attr >> 16
            if (path.is_absolute() or ".." in path.parts or "\\" in entry.filename
                    or stat.S_ISLNK(mode) or ".encap-run" in path.parts
                    or (stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR))):
                raise ValueError(f"Unsafe archive member: {entry.filename!r}")
        archive.extractall(destination)


class ComputationTimeout(TimeoutError):
    pass


def _kill_computation_group(pid):
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run_computation(folder, request, workspace_root=None):
    entrypoint = request.get("entrypoint")
    arguments = request.get("arguments")
    environment = request.get("environment", {})
    timeout = request.get("timeout_seconds")
    if timeout is not None and (
        type(timeout) is not int or timeout <= 0 or timeout > threading.TIMEOUT_MAX
    ):
        raise ValueError("Worker timeout_seconds must be a positive integer within the timer limit, or null")
    if not isinstance(entrypoint, str):
        raise ValueError("Entrypoint must be a relative source path")
    entry = folder / entrypoint
    if entry.is_symlink() or not entry.resolve().is_relative_to(folder.resolve()) or not entry.is_file():
        raise ValueError(f"Invalid entrypoint: {entrypoint}")
    if not isinstance(arguments, list) or any(not isinstance(arg, str) for arg in arguments):
        raise ValueError("Arguments must be a list of strings")
    if (not isinstance(environment, dict) or
            any(not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)
                or not isinstance(value, str) or "\x00" in value for key, value in environment.items())):
        raise ValueError("Environment must map valid variable names to string values")
    instance = request.get("instance", 0)
    if type(instance) is not int or instance < 0:
        raise ValueError("Instance ID must be a nonnegative integer")
    suffix = "" if instance == 0 else f"_{instance}"
    log_path = folder / f"log{suffix}"
    display_folder = request.get("display_folder", request["run_id"])
    env = dict(os.environ, **environment)
    env.update(ENCAP_PROCID=str(instance), ENCAP_GCP_INSTANCE=str(instance),
               ENCAP_NAME=display_folder, ENCAP_JOB_NAME=request["run_id"] + suffix,
               ENCAP_LOG=str(log_path))
    if workspace_root is not None:
        env["ENCAP_WORKSPACE_ROOT"] = str(workspace_root)
    command = shlex.join([sys.executable, "-u", str(entry), *arguments])
    header = shlex.quote(f"{display_folder}/{entrypoint} {shlex.join(arguments)}  \n")
    instance_line = f"printf '%s\\n' 'Instance {instance}'\n" if request.get("multiple_instances") else ""
    parameters = {"project": request["project"]}
    if request.get("console_authuser") is not None:
        parameters["authuser"] = request["console_authuser"]
    hostname = socket.gethostname().split(".")[0]
    machine_url = (
        "https://console.cloud.google.com/compute/instancesDetail/zones/"
        f"{quote(request['zone'], safe='')}/instances/{quote(hostname, safe='')}?{urlencode(parameters)}"
    )
    jobs_url = request.get("jobs_url", f"https://console.cloud.google.com/dataproc/jobs?{urlencode(parameters)}")
    locations = shlex.quote(
        f"host: {machine_url} (temporary VM; deleted after execution)\n"
        f"jobs: {jobs_url}\ndata: {request['data_url']}"
    )
    script = (
        f"date\nprintf '%s\\n' {locations}\n"
        + instance_line + f"printf '%s\\n' {header}\n"
        + f"time {command}\ncode=$?\nprintf '\\004\\n'\nexit \"$code\"\n"
    )
    with log_path.open("w", encoding="utf-8") as log:
        with subprocess.Popen(
            ["bash", "-c", script], cwd=folder, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            start_new_session=timeout is not None,
        ) as process:
            expired = threading.Event()
            timer = None
            if timeout is not None:
                def expire():
                    expired.set()
                    _kill_computation_group(process.pid)
                timer = threading.Timer(timeout, expire)
                timer.daemon = True
                timer.start()
            try:
                if process.stdout is None:
                    raise RuntimeError("Computation stdout pipe was not created")
                for line in process.stdout:
                    print(line, end="", flush=True)
                    log.write(line)
                    log.flush()
                code = process.wait()
            finally:
                if timer is not None:
                    timer.cancel()
                    timer.join()
                    if expired.is_set() or process.poll() is None:
                        _kill_computation_group(process.pid)
        if expired.is_set():
            message = f"Experiment timed out after {timeout} seconds."
            notice = "\n" + message + "\n\x04\n"
            print(notice, end="", flush=True)
            log.write(notice)
            raise ComputationTimeout(message)
    return code if code >= 0 else min(255, 128 - code)


def run(archive_uri, request_uri, output_uri, result_uri, project):
    for uri in (archive_uri, request_uri, output_uri, result_uri):
        checkpoint.parse_gcs_uri(uri)
    with tempfile.TemporaryDirectory(prefix="encap-dataproc-worker-") as temporary:
        work = Path(temporary)
        folder = work / "source"
        folder.mkdir()
        result = {"state": "failed", "exit_code": 1, "outputs_finalized": False}
        computation_error = None
        upload_error = None
        log_checkpoint = None
        input_paths = []
        request = {}
        try:
            checkpoint.download(archive_uri, work / "payload.zip", project)
            checkpoint.download(request_uri, work / "request.json", project)
            request = json.loads((work / "request.json").read_text(encoding="utf-8"))
            experiment_path = request.get("experiment_path", ".")
            if (not isinstance(experiment_path, str) or not experiment_path
                    or PurePosixPath(experiment_path).is_absolute()
                    or ".." in PurePosixPath(experiment_path).parts or "\\" in experiment_path):
                raise ValueError("Experiment path must be a safe relative workspace directory")
            experiment = folder / experiment_path
            extract_archive(work / "payload.zip", folder)
            if not experiment.is_dir() or not experiment.resolve().is_relative_to(folder.resolve()):
                raise ValueError("Experiment directory is missing or outside the workspace")
            folder = experiment
            inputs = request.get("inputs", {})
            root_path = request.get("root_path")
            prefix, prepared_experiment, targets = input_layout(experiment_path, inputs, root_path)
            if prefix != PurePosixPath(".") or prepared_experiment != experiment_path:
                raise ValueError("GCS input destinations escape the prepared workspace")
            input_paths = [work / "source" / target for target in targets]
            if inputs:
                download_inputs(
                    checkpoint.create_client(project), work / "source", experiment_path, inputs, root_path,
                )
            root_folder = (work / "source" / root_path).resolve() if root_path is not None else None
            if root_folder is not None:
                root_folder.mkdir(parents=True, exist_ok=True)
            log_checkpoint = checkpoint.LogCheckpoint(
                folder, output_uri, project, request.get("instance", 0), request.get("log_generation", 0),
            )
            with log_checkpoint:
                code = run_computation(folder, request, workspace_root=root_folder)
            result.update(state="succeeded" if code == 0 else "failed", exit_code=code)
        except ComputationTimeout as error:
            result.update(exit_code=124, timed_out=True, computation_error=f"ComputationTimeout: {error}")
        except (Exception, KeyboardInterrupt) as error:
            computation_error = error
            result["computation_error"] = f"{type(error).__name__}: {error}"
        try:
            if request.get("storage_layout") == "mirrored":
                checkpoint.upload_checkpoint(
                    folder, output_uri, project, excluded=input_paths, merge_instance=request.get("instance", 0),
                    log_checkpoint=log_checkpoint,
                )
            elif input_paths:
                checkpoint.upload_checkpoint(folder, output_uri, project, excluded=input_paths,
                                             log_checkpoint=log_checkpoint)
            else:
                checkpoint.upload_checkpoint(folder, output_uri, project, log_checkpoint=log_checkpoint)
            result["outputs_finalized"] = True
        except Exception as error:
            upload_error = error
            result.update(state="failed", upload_error=f"{type(error).__name__}: {error}")
        # This is the commit marker: it is created only after output saving was attempted.
        # Failure to save it is deliberately fatal, never a successful workflow.
        checkpoint.save_result(result, result_uri, project)
        if computation_error is not None:
            if upload_error is not None:
                raise RuntimeError(
                    f"Computation failed: {computation_error}; output saving also failed: {upload_error}"
                ) from computation_error
            raise computation_error
        if upload_error is not None:
            raise upload_error
        return result["exit_code"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("archive-uri", "request-uri", "output-uri", "result-uri", "project"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.archive_uri, args.request_uri, args.output_uri, args.result_uri, args.project))


if __name__ == "__main__":
    main()
