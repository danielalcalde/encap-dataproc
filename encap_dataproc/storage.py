"""Generation-conditional GCS control records for readable run locations."""

import copy
import json
import sys
from dataclasses import asdict
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from encap_lib.backends import BackendError, MonitoringError, RunHandle
from encap_dataproc.inputs import parse_input_uri
from encap_dataproc.instances import instance_ids


class StorageConflict(BackendError):
    pass


class GCSStore:
    def __init__(self, bucket, command):
        self.bucket = bucket
        self.command = command
        self.base = "https://storage.googleapis.com/storage/v1/b/" + quote(bucket, safe="")

    def _request(self, method, url, body=None, missing_ok=False):
        token = self.command(["gcloud", "auth", "print-access-token"]).stdout.strip()
        if not token:
            raise BackendError("gcloud returned no access token")
        request = Request(url, method=method, data=body,
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=60) as response:
                data = response.read()
                return json.loads(data) if data else {}
        except HTTPError as error:
            if error.code == 404 and missing_ok:
                return None
            detail = error.read().decode("utf-8", errors="replace")
            if error.code == 412:
                raise StorageConflict(f"GCS object changed concurrently: {detail}") from error
            raise BackendError(f"GCS HTTP {error.code}: {detail}") from error
        except (URLError, TimeoutError, OSError, ValueError) as error:
            raise BackendError(f"Cannot access GCS run metadata: {error}") from error

    def read(self, key):
        url = self.base + "/o/" + quote(key, safe="")
        metadata = self._request("GET", url, missing_ok=True)
        if metadata is None:
            return None
        if not isinstance(metadata, dict) or not str(metadata.get("generation", "")).isdigit():
            raise BackendError("GCS returned invalid control object metadata")
        record = self._request("GET", url + "?" + urlencode({
            "alt": "media", "generation": metadata["generation"],
        }))
        return record, metadata["generation"]

    def generation(self, key):
        metadata = self._request("GET", self.base + "/o/" + quote(key, safe=""), missing_ok=True)
        if metadata is None:
            return 0
        if not isinstance(metadata, dict) or not str(metadata.get("generation", "")).isdigit():
            raise BackendError("GCS returned invalid object generation metadata")
        return int(metadata["generation"])

    def write(self, key, record, generation):
        url = "https://storage.googleapis.com/upload/storage/v1/b/" + quote(self.bucket, safe="") + "/o?"
        result = self._request("POST", url + urlencode({
            "uploadType": "media", "name": key, "ifGenerationMatch": generation,
        }), json.dumps(record).encode())
        if not isinstance(result, dict) or not str(result.get("generation", "")).isdigit():
            raise BackendError("GCS returned no generation for the saved control object")
        return result["generation"]

    def objects(self, prefix):
        token = None
        while True:
            parameters = {"prefix": prefix, "maxResults": 1000}
            if token:
                parameters["pageToken"] = token
            page = self._request("GET", self.base + "/o?" + urlencode(parameters))
            if not isinstance(page, dict) or not isinstance(page.get("items", []), list):
                raise BackendError("GCS returned an invalid object listing")
            for item in page.get("items", []):
                if (not isinstance(item, dict) or not isinstance(item.get("name"), str)
                        or not str(item.get("generation", "")).isdigit()):
                    raise BackendError("GCS listing is missing object names or generations")
            yield from page.get("items", [])
            token = page.get("nextPageToken")
            if not token:
                break

    def delete(self, key, generation):
        self._request("DELETE", self.base + "/o/" + quote(key, safe="") + "?" + urlencode({
            "ifGenerationMatch": generation,
        }))


class RunClaim:
    def __init__(self, store, run_uri, record, generation):
        try:
            bucket, key = parse_input_uri(run_uri)
        except ValueError as error:
            raise BackendError(str(error)) from error
        if not key or key.startswith("/") or bucket != store.bucket:
            raise BackendError("Cloud reservation requires an exact run prefix in the configured bucket")
        self.store = store
        self.run_uri = run_uri
        self.prefix = key.rstrip("/") + "/"
        self.key = self.prefix + ".encap-run/control.json"
        self.record = record
        self.generation = generation

    @staticmethod
    def _terminal(record, backend, run_uri):
        if (not isinstance(record, dict) or record.get("version") != 1
                or record.get("run_uri") != run_uri
                or not isinstance(record.get("instances"), list) or not record["instances"]):
            raise MonitoringError("Occupied cloud run has no valid ownership record; refusing deletion")
        for child in record["instances"]:
            if not isinstance(child, dict) or child.get("phase") not in ("submitted", "request-started"):
                raise MonitoringError("Previous cloud submission is active or uncertain; refusing deletion")
            saved = child.get("handle")
            if (not isinstance(saved, dict) or set(saved) != {"data", "version"}
                    or not isinstance(saved.get("data"), dict)):
                raise MonitoringError("Invalid previous cloud handle; refusing deletion")
            handle = RunHandle(**saved)
            if handle.data.get("run_uri") != run_uri:
                raise MonitoringError("Previous cloud handle refers to a different run; refusing deletion")
            status = backend.status(handle)
            if status.state not in ("succeeded", "failed", "cancelled") or status.cleanup_pending:
                raise MonitoringError("Previous cloud workflow is active or uncertain; refusing deletion")

    @classmethod
    def acquire(cls, store, run_uri, children, backend):
        record = {"version": 1, "run_uri": run_uri, "instances": children}
        claim = cls(store, run_uri, record, None)
        existing = store.read(claim.key)
        if existing is None:
            if next(iter(store.objects(claim.prefix)), None) is not None:
                raise MonitoringError("Cloud run folder contains untracked data; refusing to overwrite it")
            claim.generation = store.write(claim.key, record, 0)
            return claim
        previous, generation = existing
        cls._terminal(previous, backend, run_uri)
        if not sys.stdin.isatty():
            raise MonitoringError(
                f"Cloud deletion requires interactive confirmation: {run_uri}. "
                "Rerun the same encap run command in an interactive terminal and answer y to the cloud "
                "deletion prompt, or choose a different run name with -n to keep old results."
            )
        try:
            answer = input(
                f"Delete ALL previous cloud files under {run_uri}/ and start a new run? "
                "Existing cloud results will be lost. y/n: "
            )
        except EOFError as error:
            raise BackendError(
                "Cloud run unchanged; deletion confirmation was cancelled. "
                "Rerun the same encap run command in an interactive terminal and answer y to the cloud "
                "deletion prompt, or choose a different run name with -n to keep old results."
            ) from error
        if answer.strip().lower() not in ("y", "yes"):
            raise BackendError(
                "Cloud run unchanged; deletion was not approved. "
                "Rerun the same encap run command and answer y to the cloud deletion prompt, "
                "or choose a different run name with -n to keep old results."
            )
        latest = store.read(claim.key)
        if latest is None or latest[1] != generation:
            raise StorageConflict("Cloud run changed during confirmation; nothing was deleted")
        cls._terminal(latest[0], backend, run_uri)
        claim.generation = store.write(claim.key, record, generation)
        objects = list(store.objects(claim.prefix))
        for item in objects:
            if not item["name"].startswith(claim.prefix):
                raise BackendError("GCS listing returned an object outside the run; refusing deletion")
        for item in objects:
            if item["name"] != claim.key:
                store.delete(item["name"], item["generation"])
        return claim

    def update(self, index, handle=None, phase=None):
        child = self.record["instances"][index]
        if handle is not None:
            child["handle"] = handle
        if phase is not None:
            child["phase"] = phase
        self.generation = self.store.write(self.key, self.record, self.generation)

    @classmethod
    def extend(cls, store, run_uri, previous, replacements):
        claim = cls(store, run_uri, {}, None)
        existing = store.read(claim.key)
        if existing is None:
            raise MonitoringError("Cloud ownership record is missing; refusing rerun")
        record, generation = existing
        if (not isinstance(record, dict) or record.get("version") != 1
                or record.get("run_uri") != run_uri
                or not isinstance(record.get("instances"), list) or not record["instances"]):
            raise MonitoringError("Cloud ownership record is invalid; refusing rerun")
        cloud = {}
        for child in record["instances"]:
            if (not isinstance(child, dict) or child.get("phase") not in ("prepared", "submitted", "request-started")
                    or not isinstance(child.get("handle"), dict)
                    or not isinstance(child["handle"].get("data"), dict)
                    or child["handle"]["data"].get("run_uri") != run_uri
                    or not isinstance(child["handle"]["data"].get("request_id"), str)):
                raise MonitoringError("Cloud instance ownership record is invalid; refusing rerun")
        instance_ids([child.get("instance") for child in record["instances"]])
        cloud = {child["instance"]: child for child in record["instances"]}
        local = {child["instance"]: child for child in previous}
        if cloud.keys() != local.keys() or any(
            child["handle"]["data"]["request_id"] != local[instance]["handle"]["data"]["request_id"]
            for instance, child in cloud.items()
        ):
            raise MonitoringError("Cloud ownership differs from the saved run; refusing rerun")
        history = record.setdefault("history", [])
        if not isinstance(history, list):
            raise MonitoringError("Cloud ownership history is invalid; refusing rerun")
        for child in replacements:
            instance = child["instance"]
            if instance in cloud:
                history.append(copy.deepcopy(cloud[instance]))
            cloud[instance] = {
                "instance": instance, "handle": child["handle"], "phase": "prepared",
            }
        record["instances"] = list(cloud.values())
        claim.record = record
        claim.generation = store.write(claim.key, record, generation)
        return claim


class CloudWriter:
    def __init__(self, writer, claim, index):
        self.writer, self.claim, self.index = writer, claim, index
        self.path = writer.path

    def save_handle(self, handle, confirmed=False):
        self.writer.save_handle(handle, confirmed=confirmed)
        self.claim.update(self.index, asdict(handle), "submitted" if confirmed else "prepared")

    def update(self, **values):
        self.writer.update(**values)
        if "submission_phase" in values:
            self.claim.update(self.index, phase=values["submission_phase"])
