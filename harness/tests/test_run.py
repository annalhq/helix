import re
import subprocess

import pytest

from harness.run import git_describe, prepare_out_dir


def test_prepare_out_dir_creates_missing_directory(tmp_path):
    out = prepare_out_dir(tmp_path / "results" / "01-baseline", force=False)
    assert out.is_dir()


def test_prepare_out_dir_refuses_to_overwrite_results(tmp_path):
    out = tmp_path / "02-naive"
    out.mkdir()
    (out / "history.jsonl").write_text("{}\n")
    with pytest.raises(FileExistsError):
        prepare_out_dir(out, force=False)
    assert prepare_out_dir(out, force=True) == out


def test_prepare_out_dir_accepts_empty_existing_directory(tmp_path):
    assert prepare_out_dir(tmp_path, force=False) == tmp_path


def test_git_describe_marks_dirty_trees(tmp_path):
    def git(*args):
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    assert git_describe(tmp_path) == "unknown"
    git("init", "-q")
    (tmp_path / "f.txt").write_text("a")
    git("add", "f.txt")
    git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", "init")
    assert re.fullmatch(r"[0-9a-f]{7,}", git_describe(tmp_path))

    (tmp_path / "f.txt").write_text("b")
    assert git_describe(tmp_path).endswith("-dirty")
