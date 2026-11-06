from unittest.mock import Mock

import pytest

from encap_dataproc.inputs import (
    check_input_collisions, download_inputs, input_layout, parse_input_uri, validate_inputs,
)


class Blob:
    def __init__(self, name, content=b"input"):
        self.name = name
        self.content = content
        self.generation = 42

    def download_to_file(self, output, if_generation_match):
        assert if_generation_match == self.generation
        output.write(self.content)


@pytest.mark.parametrize("uri,expected", [
    ("gs://my-bucket/path/file.csv", ("my-bucket", "path/file.csv")),
    ("gs://my-bucket/path/", ("my-bucket", "path/")),
    ("gs://my-bucket/", ("my-bucket", "")),
    ("gs://my-bucket", ("my-bucket", "")),
])
def test_parse_input_uri(uri, expected):
    assert parse_input_uri(uri) == expected


@pytest.mark.parametrize("inputs", [
    None, [], ["gs://bucket/path"],
    {"": "gs://bucket/path"}, {"/absolute": "gs://bucket/path"},
    {"dir\\file": "gs://bucket/path"}, {".encap-run/data": "gs://bucket/path"},
    {"x": "https://bucket/path"}, {"x": "gs://bucket/../path"},
    {"x": "gs://bucket/./path"}, {"x": "gs://bucket/path?query"},
    {"x": "gs://bucket/path#fragment"}, {"x": "gs://bucket\\path"},
    {"x": None}, {4: "gs://bucket/path"},
    {"{root}": "gs://bucket/path"}, {"{root}/": "gs://bucket/path"},
    {"{root}//absolute": "gs://bucket/path"}, {"{root}/../escape": "gs://bucket/path"},
    {"{root}/data/../../escape": "gs://bucket/path"},
    {"{root}/./data": "gs://bucket/path"}, {"data/{root}/x": "gs://bucket/path"},
    {"{unknown}/data": "gs://bucket/path"}, {"{root}/data/{unknown}": "gs://bucket/path"},
])
def test_invalid_inputs(inputs):
    with pytest.raises(ValueError):
        validate_inputs(inputs)


@pytest.mark.parametrize("experiment,inputs,prefix,expected,targets", [
    (".", {}, ".", ".", {}),
    (".", {"data": "gs://bucket/data/"}, ".", ".", {"data": "gs://bucket/data/"}),
    (".", {"../data": "gs://bucket/data/"}, "workspace", "workspace",
     {"data": "gs://bucket/data/"}),
    ("experiments/train", {"../../data": "gs://bucket/data/"}, ".", "experiments/train",
     {"data": "gs://bucket/data/"}),
    ("train", {"../../../data": "gs://bucket/data/"}, "workspace/workspace", "workspace/workspace/train",
     {"data": "gs://bucket/data/"}),
])
def test_input_layout(experiment, inputs, prefix, expected, targets):
    actual_prefix, actual_experiment, actual_targets = input_layout(experiment, inputs)
    assert str(actual_prefix) == prefix
    assert actual_experiment == expected
    assert actual_targets == targets


@pytest.mark.parametrize("root,expected", [(".", "data"), ("projects", "projects/data")])
def test_root_inputs_are_anchored_independently_of_experiment(root, expected):
    prefix, experiment, targets = input_layout("projects/train", {
        "{root}/data": "gs://bucket/root/",
        "local": "gs://bucket/local/",
    }, root_path=root)
    assert str(prefix) == "."
    assert experiment == "projects/train"
    assert targets == {expected: "gs://bucket/root/", "projects/train/local": "gs://bucket/local/"}


def test_relative_inputs_rebase_root_anchor_together_with_source():
    prefix, experiment, targets = input_layout("train", {
        "{root}/data": "gs://bucket/root/",
        "../../../outside": "gs://bucket/relative/",
    }, root_path=".")
    assert str(prefix) == "workspace/workspace"
    assert experiment == "workspace/workspace/train"
    assert targets == {
        "workspace/workspace/data": "gs://bucket/root/",
        "outside": "gs://bucket/relative/",
    }
    assert input_layout(experiment, {
        "{root}/data": "gs://bucket/root/",
        "../../../outside": "gs://bucket/relative/",
    }, root_path="workspace/workspace")[2] == targets


@pytest.mark.parametrize("root", [None, "/absolute", "../escape", "a/../escape", "a\\b", ".encap-run"])
def test_root_inputs_require_safe_root_metadata(root):
    with pytest.raises(ValueError):
        input_layout("train", {"{root}/data": "gs://bucket/input"}, root_path=root)


@pytest.mark.parametrize("inputs", [
    {"{root}/train": "gs://bucket/input"},
    {"{root}/train/log": "gs://bucket/input"},
    {"{root}/data": "gs://bucket/input", "../data": "gs://bucket/other"},
    {"{root}/data": "gs://bucket/input", "{root}/data/child": "gs://bucket/other"},
])
def test_root_inputs_cannot_replace_experiment_or_overlap(inputs):
    with pytest.raises(ValueError):
        input_layout("train", inputs, root_path=".")


def test_root_prefix_download_is_outside_experiment_and_skips_metadata(tmp_path):
    experiment = tmp_path / "projects/train"
    experiment.mkdir(parents=True)
    client = Mock()
    client.list_blobs.return_value = [
        Blob("seed/results/shared.json", b"seed"),
        Blob("seed/.encap-run/control.json", b"internal"),
    ]
    paths = download_inputs(client, tmp_path, "projects/train", {
        "{root}/projects/0encap_folder/seed": "gs://bucket/seed/",
    }, root_path=".")
    assert paths == [tmp_path / "projects/0encap_folder/seed"]
    assert (paths[0] / "results/shared.json").read_bytes() == b"seed"
    assert not (paths[0] / ".encap-run").exists()
    assert not (experiment / "projects").exists()


@pytest.mark.parametrize("destinations", [
    ["."], [".."], ["../a", "../a/b"], ["data", "./data"], ["data", "data/nested"],
    [".encap.conf"], ["log"], ["log_7"], [".encap_history.conf/child"],
])
def test_overlapping_or_reserved_destinations(destinations):
    with pytest.raises(ValueError):
        input_layout(".", {path: "gs://bucket/input/" for path in destinations})


@pytest.mark.parametrize("members", [
    [("data", False)], [("data/", True)], [("data/file", False)],
    [("data/parent", False)],
])
def test_collisions_with_packaged_source(members):
    with pytest.raises(ValueError, match="collides"):
        check_input_collisions({"data": "gs://bucket/prefix/"}, members)


def test_download_file_prefix_and_bucket_root(tmp_path):
    objects = {
        "tables/": [Blob("tables/train.csv", b"train"), Blob("tables/nested/test.csv", b"test"),
                    Blob("tables/empty/", b"")],
        "": [Blob("root.txt", b"root")],
    }
    client = Mock()
    client.bucket.return_value.get_blob.return_value = Blob("single.csv", b"single")
    client.list_blobs.side_effect = lambda bucket, prefix: iter(objects[prefix])
    inputs = {
        "../data": "gs://bucket/tables/",
        "inputs/single.csv": "gs://bucket/single.csv",
        "../all": "gs://bucket/",
    }
    experiment = tmp_path / "train"
    experiment.mkdir()
    paths = download_inputs(client, tmp_path, "train", inputs)
    assert paths == [tmp_path / "data", experiment / "inputs/single.csv", tmp_path / "all"]
    assert (tmp_path / "data/train.csv").read_bytes() == b"train"
    assert (tmp_path / "data/nested/test.csv").read_bytes() == b"test"
    assert (tmp_path / "data/empty").is_dir()
    assert (experiment / "inputs/single.csv").read_bytes() == b"single"
    assert (tmp_path / "all/root.txt").read_bytes() == b"root"
    client.bucket.return_value.get_blob.assert_called_once_with("single.csv")


@pytest.mark.parametrize("key", ["missing", "missing/"])
def test_missing_input_is_explicit(tmp_path, key):
    client = Mock()
    client.bucket.return_value.get_blob.return_value = None
    client.list_blobs.return_value = iter([])
    with pytest.raises(FileNotFoundError, match="missing"):
        download_inputs(client, tmp_path, ".", {"data": f"gs://bucket/{key}"})


@pytest.mark.parametrize("object_name", [
    "prefix/../escape", "prefix/./file", "prefix//absolute", "prefix/a\\b",
])
def test_object_names_cannot_escape_destination(tmp_path, object_name):
    client = Mock()
    client.list_blobs.return_value = iter([Blob(object_name)])
    with pytest.raises(ValueError, match="Unsafe GCS object"):
        download_inputs(client, tmp_path, ".", {"data": "gs://bucket/prefix/"})
    assert not (tmp_path / "escape").exists()


def test_worker_rejects_escape_without_rebasing(tmp_path):
    with pytest.raises(ValueError, match="escape"):
        download_inputs(Mock(), tmp_path, ".", {"../outside": "gs://bucket/prefix/"})


def test_download_does_not_replace_existing_files(tmp_path):
    (tmp_path / "data").write_text("original")
    client = Mock()
    with pytest.raises(ValueError, match="collides"):
        download_inputs(client, tmp_path, ".", {"data": "gs://bucket/file"})
    assert (tmp_path / "data").read_text() == "original"
    client.bucket.assert_not_called()


def test_download_permission_error_propagates(tmp_path):
    client = Mock()
    client.bucket.return_value.get_blob.side_effect = PermissionError("denied")
    with pytest.raises(PermissionError, match="denied"):
        download_inputs(client, tmp_path, ".", {"data": "gs://bucket/file"})


def test_previous_run_metadata_is_not_downloaded_as_input(tmp_path):
    client = Mock()
    client.list_blobs.return_value = iter([
        Blob("run/.encap-run/control.json", b"internal"),
        Blob("run/.encap-run/source/payload.zip", b"internal"),
        Blob("run/run.py", b"source"),
        Blob("run/result.csv", b"result"),
    ])
    download_inputs(client, tmp_path, ".", {"data": "gs://bucket/run/"})
    assert (tmp_path / "data/run.py").read_bytes() == b"source"
    assert (tmp_path / "data/result.csv").read_bytes() == b"result"
    assert not (tmp_path / "data/.encap-run").exists()


def test_download_rejects_symlink_parent(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "link").symlink_to(outside, target_is_directory=True)
    client = Mock()
    client.bucket.return_value.get_blob.return_value = Blob("file")
    with pytest.raises(ValueError, match="symlink"):
        download_inputs(client, workspace, ".", {"link/file": "gs://bucket/file"})
    assert not (outside / "file").exists()
