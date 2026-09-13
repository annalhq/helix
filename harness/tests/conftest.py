import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def kvnode_binary(tmp_path_factory):
    if shutil.which("go") is None:
        pytest.skip("go toolchain not installed")
    binary = tmp_path_factory.mktemp("bin") / "kvnode"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/kvnode"], cwd=ROOT, check=True)
    return binary
