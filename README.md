# encap-dataproc

Run Python experiments on managed Dataproc machines with
[Encap](https://github.com/computational-quantum-science/encap) >=0.6.3 (plugin API 3).

## Setup

Requires Python 3.10+ and [Google Cloud CLI](https://cloud.google.com/sdk/docs/install).
Install the plugin:

```sh
pip install encap-dataproc
gcloud auth login
encap --help
```

Create a GCS bucket, private subnet with Google API access, worker service account
and custom Dataproc image. The image needs your Python executable, experiment
dependencies and `google-cloud-storage>=3.0`.

Encap uses your selected `gcloud` identity, not application-default credentials.
That identity needs:

- GCS read/write access and `storage.buckets.get` on output and input buckets.
- Dataproc workflow/job/operation access, including cancellation.
- Cluster creation/deletion and permission to act as the worker service account.
- Object get/list/create access for run records, and delete access for replacements.

The Dataproc service agent must be able to manage clusters. The worker service
account needs GCS read access to source and inputs, list access for input prefixes,
and read/write access to outputs. Bucket access is checked before submission;
input objects are downloaded on the worker.

## Configure

Use `.encap.conf` or `~/.encap/config.yml` and replace the infrastructure values.
Experiment settings override global defaults; CLI flags override both.

```yaml
backend: dataproc  # optional; otherwise pass -gcp
workspace_root: /path/to/workspace  # optional; mirrors local run paths in GCS
plugins:
  dataproc:
    bucket: your-bucket
    project: your-project
    region: europe-west1
    zone: europe-west1-b
    subnet: projects/host-project/regions/europe-west1/subnetworks/your-subnet
    python: /opt/python/bin/python
    image_uri: projects/image-project/global/images/family/your-image
    service_account: worker@your-project.iam.gserviceaccount.com
    network_tag: your-network-tag
    machine_type: n4-standard-4
    cluster_name: ml-cluster
    timeout: 24h
    output_prefix: encap
    console_authuser: "0"
```

The first ten plugin keys are required. `python` is an absolute path on the
remote machine.

Optional plugin settings:

| Setting | Default / purpose |
| --- | --- |
| `cluster_name` | `encap-cluster`; cluster name prefix |
| `output_prefix` | `encap`; bucket folder, including nested paths |
| `disk_size_gb` | `200` |
| `timeout` | `24h`; see below |
| `console_authuser` | Account used in Console links |
| `cluster_properties` | String-to-string Spark/YARN overrides |
| `inputs` | Destination-to-GCS-source mappings |
| `i` | `1`; machine count or instance IDs |

Each workflow uses one private-IP master and no workers. Default allocations are
2 YARN cores/8 GB and a 2 GB Spark driver, regardless of `machine_type`.
Cluster prefixes must be 2-35 characters: lowercase letters, digits and hyphens,
starting with a letter and ending with a letter or digit. Dataproc adds a random suffix.

### Timeouts

`timeout` accepts a positive integer in seconds or a duration such as `30s`,
`10m`, `24h`, `2d` or `1w`. Units must be lowercase; fractional and compound
durations are not supported. Use `unlimited` or `-1` to disable the timeout.

| Duration | Enforcement |
| --- | --- |
| 10 minutes through 24 hours | Dataproc workflow deadline (`dagTimeout`) |
| Under 10 minutes or over 24 hours | Worker-side computation timeout |
| `unlimited` or `-1` | No deadline |

The Dataproc deadline includes staging and output saving, but not cluster
provisioning. The worker timeout covers only computation, separately for each
instance. On expiry, it kills the computation process group, attempts to save
partial outputs and records exit code `124` with `timed_out: true`.
Dataproc cancellation can prevent final uploads.

Worker timeouts depend on the runner; use `encap kill` if it fails or hangs.
Normal completion deletes the managed cluster. Timeout changes affect new
submissions only. Legacy `timeout_seconds` accepts positive integer seconds;
do not configure it together with `timeout`.

### Mirror local run folders in GCS

Set `workspace_root` to an existing workspace directory to mirror saved run paths
under `output_prefix`:

```yaml
workspace_root: /path/to/workspace
plugins:
  dataproc:
    # Other infrastructure settings...
    output_prefix: encap
```

A local run at `/path/to/workspace/0encap_folder/experiment/trial/` maps to:

```text
gs://<bucket>/encap/0encap_folder/experiment/trial/
  run.py
  log
  result.json
  .encap.conf
  .encap_history.conf
  .encap-run/
```

Results sit directly in the run folder. Internal `.encap-run/` objects hold
source and recovery metadata and are excluded from pulls and GCS inputs.

The root may be absolute or relative to its configuration file. Saved runs must
be strictly beneath it; the filesystem root is not allowed. Without a root,
runs use UUID-based cloud paths. Existing runs keep their saved locations.

The root also anchors `{root}/` dependencies and GCS destinations.
Legacy `plugins.dataproc.workspace_root` still works, but must match the
top-level root if both are set. Root-relative source dependencies require the
top-level setting.

Replacing an occupied cloud run folder requires completed workflows and cluster
cleanup, then a separate deletion confirmation. `-y` does not bypass this
confirmation. Noninteractive replacement, untracked objects and uncertain
workflow state block deletion. Confirming removes previous cloud results,
including results referenced by local backups.

## Run and manage

Create `experiment/run.py`, then:

```sh
encap run ./experiment -n trial -gcp
```

File targets also work. Use `-sn train.py` or `script_name: train.py` for another
entrypoint, and `-a '--value "two words"'` for arguments. Set worker variables
with an Encap `environment` mapping; local shell variables are not inherited.

Dataproc job prefixes come from the experiment and run names, with dots replaced
by hyphens and a 50-character limit. Dataproc adds a random suffix.

Unsupported: native `-i`, VM/Slurm/Pueue, copy/archive/push, dry-run and
`--interpreter`. Set `plugins.dataproc.python` instead.

For saved runs, omit backend flags:

| Command | Behavior |
| --- | --- |
| `encap tail ./experiment -n trial` | Follow logs; download outputs on normal completion |
| `encap wait ./experiment -n trial` | Wait without logs/downloads; return the computation's exit code |
| `encap status ./experiment -n trial` | Show computation, output-saving and cleanup status |
| `encap pull ./experiment -n trial` | Download available outputs, including after failure |
| `encap kill ./experiment -n trial` | Cancel the run; Dataproc manages cleanup |

`run` follows logs and downloads outputs on normal completion. Ctrl-C only
detaches; use `kill` to cancel. Successful `wait` requires output saving, but
cluster deletion may still be pending. Forced termination can prevent uploads.

For multiple instances, `run`, `rerun` and `tail` follow only the first instance
selected by the latest launch, then wait quietly for the rest before downloading.
Other management commands cover the whole group; all instance logs are saved.

Logs include Console links for the temporary VM, Dataproc jobs and saved data.
Once the job ID is known, CLI logs and status link directly to its summary.
The VM link expires when the cluster is deleted.

### List active cloud jobs

```sh
encap status ./experiment -gcp
encap status -gcp --json
```

Without `-n`, status lists active Encap jobs visible to your `gcloud` identity in
the configured project and region, including jobs without local records. The
target selects configuration, not an experiment filter. Only `project` and
`region` are required; your identity needs `dataproc.jobs.list` and
`storage.objects.get` for saved request metadata.

The table shows experiment path, run name, state, time in the current state and
cluster. `--json` returns `backend: "dataproc"` and a `runs` list with job IDs,
timestamps, state, placement, labels and Console links. Empty results show
`No active jobs.` or `runs: []`; authentication, permission and partial-listing
errors exit with code 2.

Completed jobs and workflows still provisioning a cluster are not listed.
To inspect a saved run, including provisioning and output saving, use
`encap status ./experiment -n trial` without `-gcp`.

### Open an SSH terminal

```sh
encap ssh ./experiment -n trial
encap ssh ./experiment -n parallel --instance 2
encap ssh ./experiment -n trial --tunnel-through-iap
```

Requires Encap with `ssh` support and the local `.encap-run` record. Omit `-gcp`;
saved settings select the cluster. For multiple instances, use the original run
name and an instance ID with `--instance`.

The command opens `gcloud compute ssh` on the workflow's master VM. The cluster
must have finished provisioning and still exist. Exiting does not cancel the
run, download outputs or extend the cluster lifetime.

Private-IP clusters need `--tunnel-through-iap` unless your network has direct
access. Your identity needs workflow/cluster read access and Compute Engine SSH
permissions. IAP also needs tunnel access and firewall access to port 22.

## Dependencies and inputs

### Source dependencies

Add files or directories to the experiment configuration:

```yaml
workspace_root: /path/to/workspace
dependencies:
  - "{root}/src"
  - ../another_experiment
```

Relative paths resolve from the declaring configuration file; `{root}/` uses
the top-level `workspace_root`. Absolute paths also work. A child list replaces
an inherited list; `[]` clears it.

Encap snapshots selected sources, not the whole workspace. Dependencies keep
their root-relative locations, but the experiment runs from its saved-run path,
not its original source path. Locate shared files explicitly:

```python
import os
from pathlib import Path

root = Path(os.environ["ENCAP_WORKSPACE_ROOT"])
library = root / "src"
```

Python import paths are unchanged. Dependencies are source only: referenced
experiments are not run and their dependency lists are not followed.
Only files inside the running experiment directory are collected as outputs.
Missing/excluded dependencies and symlinks fail before submission; normal
secret/cache and `rsync_exclude`/`rsync_exclude_push` exclusions apply.

### GCS inputs

Map worker destinations to GCS sources under `plugins.dataproc.inputs`:

```yaml
workspace_root: /path/to/workspace
dependencies:
  - ../src
plugins:
  dataproc:
    # Other infrastructure settings...
    inputs:
      ../previous_run: gs://my-bucket/previous-run/
      data/train.csv: gs://my-bucket/datasets/train.csv
      "{root}/shared/seed": gs://my-bucket/seed/
```

Destinations are relative to the saved-run working directory, regardless of
which config declares them. Leading `../` is supported. `{root}/` anchors a
destination to the worker workspace root, not the VM's `/` or a local directory.
Root destinations need a nonempty path without `.` or `..` components and an
Encap version supplying snapshot-origin metadata.

Absolute paths, overlapping destinations, invalid placeholders and collisions
with packaged source or internal metadata are rejected. Remove a local
dependency if a GCS input replaces it. `{root}` is not expanded in GCS URIs,
arguments or other configuration values.

- A trailing `/` downloads a prefix recursively.
- A bucket-only URI downloads the bucket contents.
- An object URI without a trailing `/` downloads one file.
- For mirrored runs, use the run folder without `/outputs/`; internal
  `.encap-run/` objects are skipped.

Inputs download before computation using the worker service account. Missing
objects, empty prefixes and access failures fail the worker. Sources are read
at execution time; keep them immutable for reproducible runs.
Inputs are excluded from uploads, including new files written inside input
directories. Write results elsewhere.

## Multiple machines

```sh
encap run ./experiment -n two-machines -gcp_i 2
encap run ./experiment -n selected -gcp_i '[0, 2, 7]'
```

`-gcp_i` selects Dataproc and accepts a positive count, unique nonnegative IDs
or `'range(2, 5)'`. The default is `plugins.dataproc.i: 1`; the flag overrides it.
Move legacy top-level `backend_instances` settings to `plugins.dataproc.i`.

Each ID gets its own machine; `{i}` in arguments expands to that ID.
Outputs merge locally, so use `ENCAP_PROCID` in filenames:

```python
import os
from pathlib import Path

instance = os.environ["ENCAP_PROCID"]
Path(f"result_{instance}.txt").write_text(f"Hello from instance {instance}\n")
```

With `workspace_root`, instances share a cloud output folder. Identical files
are deduplicated; conflicting content is saved as
`<instance>_<stem>_dup<extension>` (for example `7_metrics_dup.csv`).
The original is not overwritten. A conflicting duplicate filename fails output
saving. Without a root, instances have separate cloud prefixes and same-name
files can overwrite each other during local pulls.

### Rerun a saved experiment

```sh
encap run ./experiment -n trial -gcp_i '[0]'
encap rerun ./experiment -n trial -sn run.py -gcp_i '[1, 2, 3]'
```

`rerun` uses the saved experiment directory, including local edits. It adds new
IDs or restarts selected completed IDs while keeping unselected instances and
existing results. Active, unknown or cleanup-pending instances cannot restart.
Without `-gcp_i`, it reuses the saved instance configuration.

Reruns save fresh request IDs and attempt history without deleting previous
cloud results. Restarted instances can replace their completed attempt's log;
other outputs use the duplicate-file policy above.

Partial submissions stay recorded and are not automatically retried. Check
`status` before explicitly selecting instances to rerun.

## Outputs and recovery

`run` and `rerun` register with Encap's local daemon, which downloads available
outputs every 60 seconds and performs a stable final pull after completion.
Workers upload logs every 30 seconds and other outputs at the end. Live copies
are partial; use `status` to check output finalization.

```sh
encap watch ./experiment -n trial  # register an existing run
encap daemon status
encap daemon stop
encap daemon start
```

The daemon is independent of terminal log attachment and does not cancel jobs.
`stop` pauses autostart until `start` or `watch`. After reboot, start it again or
submit another run. Errors appear in `daemon status` and
`~/.encap/daemon/daemon.log`; offline/authentication failures are retried.

- Folder runs save to `<parent>/0encap_folder/<folder>/<name>/`; file runs save to
  `<source-without-extension>/<name>/`. Logs are `log` for ID 0 or `log_<id>`.
- Pulls preserve source, configuration, history, `.encap-run` and unrelated
  local files. Use `rsync_exclude_pull: ['/models/', '*.pkl']` or
  `--rsync-exclude-pull '/models/' '*.pkl'` to omit outputs. Pass the flag without
  patterns for a full pull. `.encap-run/retrieval.json` records omissions.
- Keep `.encap-run` when moving runs: it stores remote settings and recovery
  state. `.encap-run/bucket_location` is a convenience GCS pointer, not proof
  that submission or computation succeeded.
- Local overwrite confirmation (`-y` accepts) deletes the previous local run.
  Set `backup_on_overwrite: true` to keep `<name>.backup-<suffix>`.
  Cloud deletion has its own confirmation.
- Pre-submission failures are recorded as `not-submitted`; retry the command or
  choose another `-n`. Interrupted/unconfirmed submissions are never retried
  automatically. Check `status` and workflow Console links; do not delete
  records to resubmit. Active, unknown and unconfirmed runs cannot be overwritten.
- Keep datasets and credentials out of source uploads. Use service-account
  access rather than secrets in arguments or environment variables. Common
  secret/cache folders and internal records are excluded; symlinks are rejected.
- Old standalone `.gcp` receipts are not imported; use their original scripts.

Tests (mocked cloud calls): `python -m pytest tests ../encap/tests/test_backends.py`.
