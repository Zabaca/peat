"""TEMPORARY, for one dispatch run (removed after): fail the teardown of the first attempt of
test_tx_abort_with_many_relations[release-pg18-v1], after the test's output is written, and
print the test filesystem's free space at the end of attempt 1 and the start of attempt 2."""
import os
import subprocess
from pathlib import Path

import pytest


def _df(label):
    out = subprocess.run(["df", "-h", "/mnt/test-output"], capture_output=True, text=True).stdout
    print(f"FORCED-TEARDOWN-DEMO {label}: {out.splitlines()[-1]}", flush=True)


@pytest.fixture(autouse=True)
def force_teardown_failure(request):
    demo = "test_tx_abort_with_many_relations[release-pg18-v1]" == request.node.name
    n = getattr(request.node, "execution_count", 1)
    if demo and n == 2:
        _df("start of attempt 2")
    yield
    if demo and n == 1:
        # What the real failure left: a failed teardown does not clean up, and a failed
        # attempt of this test holds about 12 GB of its output.
        d = Path(os.environ["TEST_OUTPUT"]) / f"{request.node.name}-1"
        subprocess.run(["fallocate", "-l", "12G", str(d / "filler.bin")], check=True)
        _df("end of attempt 1, before the rerun")
        raise RuntimeError("forced teardown failure (demonstration)")
