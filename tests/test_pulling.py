import re
import shutil
import subprocess

import pytest

from encap_lib.backends import ConfigurationError
from encap_dataproc.pulling import PullExclusions


FILES = (
    "model.pkl", "model.pickle", "models/a.pkl", "models/b.txt", "models/sub/c.pkl",
    "models-old/d.pkl", "nested/models/e.pkl", "data/frozen.csv", "data/keep.csv",
    "nested/data/frozen.csv", "checkpoints/a.pkl", "checkpoints/sub/b.pkl",
    "checkpoints/1.txt", "checkpoints/2.txt", "checkpoints/a.txt",
    "special/a,b.pkl", "special/a[1].pkl", "special/a+.pkl", "special/a].pkl",
    "special/it's.pkl", "other/models", "nested/model.pkl", "ab", "a/b",
    "nested/checkpoints/a.pkl", "nested/checkpoints/sub/b.pkl",
    "nested/a/b", "nested/a1/sub/b", "a1/sub/b",
)


@pytest.mark.parametrize("pattern", [
    "*.pkl", "model.pkl", "/model.pkl", "models/", "/models/", "models",
    "models/*.pkl", "/models/**/*.pkl", "**/models/", "data/frozen.csv",
    "checkpoints/**/*.pkl", "checkpoints/[0-2].txt", "checkpoints/[!0-2].txt",
    "checkpoints/[^0-2].txt", "special/a,b.pkl", r"special/a\[1\].pkl",
    "special/a[[]1].pkl", "special/a[]].pkl", "special/a+.pkl", "special/it's.pkl",
    "**.pkl", "a**/b", "*", "models/**", "/models/**", "**/a.pkl",
    "models/**/c.pkl", "**/checkpoints/**/*.pkl",
    "special/[.a]*", "special/[:a]*",
])
def test_exclusion_translation_agrees_with_rsync(tmp_path, pattern):
    executable = shutil.which("rsync")
    if executable is None:
        pytest.skip("rsync is not installed")
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    for name in FILES:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
    result = subprocess.run(
        [executable, "-r", "--dry-run", "--out-format=%n", "--exclude", pattern,
         str(source) + "/", str(destination) + "/"],
        check=True, capture_output=True, text=True,
    )
    transferred = set(result.stdout.splitlines()) & set(FILES)
    exclusions = PullExclusions([pattern])
    assert {name for name in FILES if not exclusions.matches(name)} == transferred
    assert {name for name in FILES if re.match(exclusions.regex, name)} == set(FILES) - transferred
    assert "," not in exclusions.regex


@pytest.mark.parametrize("values", [
    "*.pkl", [None], [""], ["line\nbreak"], ["../model.pkl"], ["a/../b"],
    ["/"], ["a//b"], ["[abc"], ["[z-a]"], ["***"], ["[[:digit:]]"],
    ["a\\b"], ["a*\\"], ["a[/]b"],
])
def test_invalid_or_unsupported_exclusions_fail_explicitly(values):
    with pytest.raises(ConfigurationError, match="rsync_exclude_pull"):
        PullExclusions(values)


def test_internal_metadata_is_always_excluded():
    exclusions = PullExclusions([])
    for name in (".encap-run/run.json", "nested/.encap-run/result.json"):
        assert exclusions.matches(name)
    for name in ("model.pkl", "nested/data.pkl", ".encap-run-other/run.json", "log"):
        assert not exclusions.matches(name)
