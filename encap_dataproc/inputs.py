"""GCS input mappings shared by submission and the standalone worker."""

import posixpath
import re
from pathlib import Path, PurePosixPath


def parse_input_uri(uri):
    if not isinstance(uri, str):
        raise ValueError("GCS inputs must use gs://bucket/path URIs")
    match = re.fullmatch(r"gs://([a-z0-9][a-z0-9._-]{1,221}[a-z0-9])(?:/([^\x00\r\n?#\\]*))?", uri)
    if match is None:
        raise ValueError(f"Invalid GCS input URI: {uri!r}")
    bucket, key = match.groups()
    key = key or ""
    if any(part in (".", "..") for part in key.split("/")):
        raise ValueError(f"Unsafe GCS input path: {uri!r}")
    return bucket, key


def validate_inputs(inputs):
    if not isinstance(inputs, dict):
        raise ValueError("plugins.dataproc.inputs must map relative destinations to GCS URIs")
    for destination, uri in inputs.items():
        if (not isinstance(destination, str) or not destination or "\x00" in destination
                or "\\" in destination or PurePosixPath(destination).is_absolute()
                or ".encap-run" in PurePosixPath(destination).parts):
            raise ValueError(f"Invalid GCS input destination: {destination!r}")
        if destination.startswith("{root}/"):
            relative = destination[len("{root}/"):]
            if (not relative or relative.startswith("/")
                    or any(part in ("", ".", "..") for part in relative.split("/"))
                    or "{" in relative or "}" in relative):
                raise ValueError(f"Invalid root-relative GCS input destination: {destination!r}")
        elif "{" in destination or "}" in destination:
            raise ValueError(f"Unknown GCS input placeholder: {destination!r}; use {{root}}/path")
        parse_input_uri(uri)
    return inputs


def input_layout(experiment_path, inputs, root_path=None):
    """Add virtual parents when destinations reach above the existing archive root."""
    validate_inputs(inputs)
    if root_path is not None and (
        not isinstance(root_path, str) or not root_path
        or PurePosixPath(root_path).is_absolute() or ".." in PurePosixPath(root_path).parts
        or ".encap-run" in PurePosixPath(root_path).parts or "\\" in root_path or "\x00" in root_path
    ):
        raise ValueError("Workspace root must be a safe relative archive directory")
    depth = len(PurePosixPath(experiment_path).parts)
    extra = 0
    for destination in inputs:
        if destination.startswith("{root}/"):
            if root_path is None:
                raise ValueError("{root} inputs require workspace_root and prepared workspace origin metadata")
            continue
        level = depth
        for part in PurePosixPath(destination).parts:
            level += -1 if part == ".." else 1
            extra = max(extra, -level)
    prefix = PurePosixPath(*(["workspace"] * extra))
    experiment = prefix / experiment_path
    targets = {}
    for destination, uri in inputs.items():
        if destination.startswith("{root}/") and root_path is not None:
            target = prefix / root_path / destination[len("{root}/"):]
        else:
            target = PurePosixPath(posixpath.normpath(str(experiment / destination)))
        if (target == experiment or target in experiment.parents or ".." in target.parts
                or any(target == other or target in other.parents or other in target.parents for other in targets)):
            raise ValueError(f"GCS input destinations overlap or replace the experiment: {destination}")
        if target.is_relative_to(experiment):
            first = target.relative_to(experiment).parts[0]
            if first in (".encap.conf", ".encap_history.conf") or re.fullmatch(r"log(?:_\d+)?", first):
                raise ValueError(f"GCS input destination is reserved: {destination}")
        targets[target] = uri
    return prefix, experiment.as_posix(), {path.as_posix(): uri for path, uri in targets.items()}


def check_input_collisions(targets, members):
    paths = [(PurePosixPath(name), is_directory) for name, is_directory in members]
    for target in map(PurePosixPath, targets):
        if any(path == target or target in path.parents or (not directory and path in target.parents)
               for path, directory in paths):
            raise ValueError(f"GCS input collides with packaged source: {target}")


def download_inputs(client, workspace, experiment_path, inputs, root_path=None):
    """Download exact objects or slash-terminated prefixes without overwriting source."""
    prefix, experiment, targets = input_layout(experiment_path, inputs, root_path)
    if prefix != PurePosixPath(".") or experiment != experiment_path:
        raise ValueError("GCS input destinations escape the prepared workspace")
    workspace = Path(workspace).resolve()
    members = []
    for path in workspace.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"Workspace symlinks are unsupported: {path}")
        members.append((path.relative_to(workspace).as_posix(), path.is_dir()))
    check_input_collisions(targets, members)
    downloaded = []
    for target, uri in targets.items():
        bucket_name, key = parse_input_uri(uri)
        bucket = client.bucket(bucket_name)
        destination = workspace / target
        print(f"Downloading input: {uri} -> {target}", flush=True)
        if not key or key.endswith("/"):
            found = False
            for blob in client.list_blobs(bucket_name, prefix=key):
                suffix = blob.name[len(key):]
                parts = suffix.rstrip("/").split("/") if suffix else []
                if ".encap-run" in parts:
                    continue
                found = True
                if any(part in ("", ".", "..") or "\\" in part or "\x00" in part for part in parts):
                    raise ValueError(f"Unsafe GCS object name: {blob.name!r}")
                path = destination.joinpath(*parts)
                if blob.name.endswith("/"):
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with path.open("xb") as output:
                        blob.download_to_file(output, if_generation_match=blob.generation)
            if not found:
                raise FileNotFoundError(f"GCS input prefix is empty or missing: {uri}")
        else:
            blob = bucket.get_blob(key)
            if blob is None:
                raise FileNotFoundError(f"GCS input object does not exist: {uri}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as output:
                blob.download_to_file(output, if_generation_match=blob.generation)
        downloaded.append(destination)
    return downloaded
