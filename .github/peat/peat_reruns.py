"""
CI: a pytest plugin that records each rerun of a failed test (pytest-rerunfailures),
so that a test which passed only on its rerun shows in the job's summary and
annotations instead of only as a RERUN line in a shard's log.

Loaded by .github/peat/run-shard.sh with `-p peat_reruns`. For every report whose
outcome is "rerun" it appends one JSON line (node id, phase, the failure's first error
line and its full text) to the file named by PEAT_RERUNS, and one more line if that test
then fails again. Only the controlling process writes: with pytest-xdist the workers'
reports reach it too.
.github/peat/report_reruns.py turns the file into the summary and annotations.

It also makes a rerun start from a clean test output directory. Neon's suite names a
test's output directory after the test and the attempt (<test>-1, <test>-2), so a failed
attempt's output would stay beside the rerun's; a test that writes 12 GB then cannot be
rerun on the 16 GB filesystem the CI gives it. When a test is about to be rerun (in the
process that ran it, before the next attempt starts) the failed attempt's *.log files of
up to 20 MB are copied to PEAT_RERUN_LOGS (default $RUNNER_TEMP/rerun-first-attempts) for
report_reruns.py, and its directory under TEST_OUTPUT is removed. Which failures are
rerun is pytest-rerunfailures' choice and is not touched here.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

_config: Any = None


def pytest_configure(config: Any) -> None:
    global _config
    _config = config


def _first_error_line(text: str) -> str:
    for line in text.splitlines():
        if line.startswith("E "):
            return line[1:].strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[-1] if lines else ""


_rerun: set[str] = set()

LOG_LIMIT = 20 * 1024 * 1024


def kept_logs_dir() -> Path:
    return Path(
        os.environ.get("PEAT_RERUN_LOGS")
        or f"{os.environ.get('RUNNER_TEMP') or '/tmp'}/rerun-first-attempts"
    )


def free_failed_attempts(nodeid: str) -> None:
    """Keep the logs of a failed attempt of this test and remove its output directory."""
    out = os.environ.get("TEST_OUTPUT")
    if not out or not Path(out).is_dir():
        return
    stem = nodeid.rsplit("::", 1)[-1].replace("/", "-") + "-"
    kept = kept_logs_dir()
    for entry in os.scandir(out):
        # <test>-<attempt>, exactly: another test's name may begin with this one's.
        if not (entry.is_dir(follow_symlinks=False) and entry.name.startswith(stem)):
            continue
        if not entry.name[len(stem) :].isdigit():
            continue
        top = Path(entry.path)
        for log in top.rglob("*.log"):
            if log.is_file() and log.stat().st_size <= LOG_LIMIT:
                dest = kept / entry.name / log.relative_to(top)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(log, dest)
        shutil.rmtree(top, ignore_errors=True)
        print(f"peat_reruns: removed {entry.name} before the rerun", flush=True)


def _write(path: str, record: dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def pytest_runtest_logreport(report: Any) -> None:
    in_worker = _config is not None and hasattr(_config, "workerinput")
    # The process that ran the test frees the failed attempt, before the next one starts:
    # an xdist worker, or the only process when there are no workers.
    runs_tests = in_worker or not getattr(getattr(_config, "option", None), "numprocesses", None)
    if report.outcome == "rerun" and runs_tests:
        free_failed_attempts(report.nodeid)
    path = os.environ.get("PEAT_RERUNS")
    if not path or in_worker:
        return
    if report.outcome != "rerun":
        # A test that was rerun and failed again: recorded so the report can tell it
        # from one that passed on its rerun.
        if report.failed and report.nodeid in _rerun:
            _write(path, {"nodeid": report.nodeid, "when": report.when, "failed_again": True})
        return
    _rerun.add(report.nodeid)
    text = report.longreprtext or ""
    crash = getattr(getattr(report, "longrepr", None), "reprcrash", None)
    message = getattr(crash, "message", None) or _first_error_line(text)
    record = {
        "nodeid": report.nodeid,
        "when": report.when,
        "message": message.splitlines()[0] if message else "",
        "longrepr": text,
    }
    _write(path, record)
