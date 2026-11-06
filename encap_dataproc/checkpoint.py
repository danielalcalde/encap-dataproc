"""Worker-side GCS uploads; this module is shipped as a Dataproc Python file."""

import json
import hashlib
import os
import sys
import tempfile
import threading
from pathlib import Path

from google.auth import compute_engine
from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import Request
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError, PreconditionFailed


LOG_UPLOAD_INTERVAL_SECONDS = 30


class DataprocCredentials(compute_engine.Credentials):
    def refresh(self, request):
        # Metadata mTLS needs a session even when AuthorizedSession passes a partial.
        super().refresh(Request())


def parse_gcs_uri(uri):
    if not isinstance(uri, str) or not uri.startswith("gs://"):
        raise ValueError(f"Expected a gs://bucket/path URI, got {uri!r}")
    bucket, separator, key = uri[5:].partition("/")
    if not bucket or not separator or not key.strip("/"):
        raise ValueError(f"Expected a gs://bucket/path URI, got {uri!r}")
    return bucket, key.rstrip("/")


def create_client(project):
    credentials = DataprocCredentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
        universe_domain="googleapis.com",
    )
    return storage.Client(project=project, credentials=credentials)


def download(uri, destination, project):
    bucket, key = parse_gcs_uri(uri)
    create_client(project).bucket(bucket).blob(key).download_to_filename(str(destination))


def save_result(result, uri, project):
    bucket, key = parse_gcs_uri(uri)
    create_client(project).bucket(bucket).blob(key).upload_from_string(
        json.dumps(result), content_type="application/json", if_generation_match=0,
    )


class LogCheckpoint:
    def __init__(self, folder, output_uri, project, instance=0, generation=0):
        if type(instance) is not int or instance < 0:
            raise ValueError("Instance ID must be a nonnegative integer")
        if type(generation) is not int or generation < 0:
            raise ValueError("Log generation must be a nonnegative integer")
        self.path = Path(folder) / ("log" if instance == 0 else f"log_{instance}")
        self._bucket, prefix = parse_gcs_uri(output_uri)
        self._key = f"{prefix}/{self.path.name}"
        self._project = project
        self._blob = None
        self._generation = generation
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, name="encap-log-checkpoint", daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stopped.set()
        self._thread.join()

    def _run(self):
        while not self._stopped.wait(LOG_UPLOAD_INTERVAL_SECONDS):
            try:
                self.upload()
            except (OSError, GoogleAPIError, GoogleAuthError, RuntimeError) as error:
                print(
                    f"Warning: periodic log upload to gs://{self._bucket}/{self._key} failed: "
                    f"{type(error).__name__}: {error}. Retrying in {LOG_UPLOAD_INTERVAL_SECONDS} seconds.",
                    file=sys.stderr, flush=True,
                )

    def upload(self):
        if self.path.is_symlink():
            raise RuntimeError(f"Log symlinks are unsupported: {self.path}")
        with self.path.open("rb") as source, tempfile.TemporaryFile() as snapshot:
            # Bound the snapshot even if the computation keeps appending during the copy.
            size = remaining = os.fstat(source.fileno()).st_size
            while remaining:
                chunk = source.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise RuntimeError(f"Log was truncated while creating a snapshot: {self.path}")
                snapshot.write(chunk)
                remaining -= len(chunk)
            if self._blob is None:
                self._blob = create_client(self._project).bucket(self._bucket).blob(self._key)
            self._blob.upload_from_file(
                snapshot, rewind=True, size=size, content_type="text/plain; charset=utf-8",
                if_generation_match=self._generation, timeout=30,
            )
            if self._blob.generation is None:
                raise RuntimeError(f"Log upload returned no object generation: {self._key}")
            self._generation = self._blob.generation


def _digest(stream):
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.digest()


def _same_content(bucket, key, path):
    existing = bucket.get_blob(key)
    if existing is None:
        raise RuntimeError(f"Conflicting object disappeared while comparing: {key}")
    with tempfile.TemporaryFile() as remote:
        existing.download_to_file(remote, if_generation_match=existing.generation)
        remote.seek(0)
        with path.open("rb") as local:
            return _digest(remote) == _digest(local)


def _upload_merged(bucket, key, path, instance):
    try:
        bucket.blob(key).upload_from_filename(str(path), if_generation_match=0)
        return
    except PreconditionFailed:
        if _same_content(bucket, key, path):
            return
    original = Path(key)
    duplicate = original.with_name(f"{instance}_{original.stem}_dup{original.suffix}").as_posix()
    try:
        bucket.blob(duplicate).upload_from_filename(str(path), if_generation_match=0)
    except PreconditionFailed:
        if not _same_content(bucket, duplicate, path):
            raise RuntimeError(f"Duplicate output also has conflicting content: {duplicate}")
    print(f"Output conflict: gs://{bucket.name}/{key} saved as gs://{bucket.name}/{duplicate}", flush=True)


def upload_checkpoint(folder, output_uri, project, excluded=(), merge_instance=None, log_checkpoint=None):
    folder = Path(folder)
    if not folder.is_dir():
        raise ValueError(f"Output directory does not exist: {folder}")
    if merge_instance is not None and (type(merge_instance) is not int or merge_instance < 0):
        raise ValueError("Merged output instance must be a nonnegative integer")
    bucket_name, prefix = parse_gcs_uri(output_uri)
    bucket = create_client(project).bucket(bucket_name)
    failures = []
    for path in sorted(folder.rglob("*")):
        relative = path.relative_to(folder)
        if ".encap-run" in relative.parts or any(path == item or item in path.parents for item in excluded):
            continue
        if path.is_symlink():
            failures.append(f"Output symlinks are unsupported: {relative}")
            continue
        if path.is_file():
            key = f"{prefix}/{relative.as_posix()}"
            try:
                if log_checkpoint is not None and path == log_checkpoint.path:
                    log_checkpoint.upload()
                elif merge_instance is None:
                    bucket.blob(key).upload_from_filename(str(path), if_generation_match=0)
                else:
                    _upload_merged(bucket, key, path, merge_instance)
            except (OSError, GoogleAPIError, GoogleAuthError, RuntimeError) as error:
                # Try the remaining files to retain as much partial output as possible.
                failures.append(f"{relative}: {error}")
    if failures:
        raise RuntimeError("Output upload failed:\n" + "\n".join(failures))
