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
"""

from __future__ import annotations

import json
import os
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


def _write(path: str, record: dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def pytest_runtest_logreport(report: Any) -> None:
    path = os.environ.get("PEAT_RERUNS")
    if not path or (_config is not None and hasattr(_config, "workerinput")):
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
