"""
CI: report the reruns that .github/peat/peat_reruns.py recorded during a shard.

For each rerun it prints a GitHub `::warning::` annotation and adds a row to the job's
step summary; it copies the record into the results directory, and the logs of each
rerun test's first attempt into rerun-first-attempts.tar.gz there, so the first failure
survives a green job. Those logs are the ones peat_reruns.py kept when it removed the failed
attempt's directory before the rerun (PEAT_RERUN_LOGS), and any still in a "-1" directory.

Usage: report_reruns.py <reruns.jsonl> <test output dir> <results dir>
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tarfile
from pathlib import Path

LOG_LIMIT = 20 * 1024 * 1024
KEPT_LOGS = Path(
    os.environ.get("PEAT_RERUN_LOGS")
    or f"{os.environ.get('RUNNER_TEMP') or '/tmp'}/rerun-first-attempts"
)


def escape_data(s: str) -> str:
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(s: str) -> str:
    return escape_data(s).replace(":", "%3A").replace(",", "%2C")


def main() -> int:
    reruns_file, test_output, results = (Path(a) for a in sys.argv[1:4])
    records = []
    failed_again: set[str] = set()
    if reruns_file.exists():
        with reruns_file.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                if r.get("failed_again"):
                    failed_again.add(r["nodeid"])
                else:
                    records.append(r)

    lines = ["### Reruns", ""]
    if not records:
        lines.append("No test was rerun.")
    else:
        lines += [
            f"{len(records)} test(s) rerun. A rerun that then passed leaves the job green;"
            " each is listed here, and its first attempt's logs are in the shard's artifact"
            " (rerun-first-attempts.tar.gz).",
            "",
            "| Test | First failed in | First error | Rerun |",
            "|---|---|---|---|",
        ]
        for r in records:
            then = "failed again" if r["nodeid"] in failed_again else "passed"
            message = r["message"].replace("|", "\\|")[:300]
            lines.append(f"| `{r['nodeid']}` | {r['when']} | {message} | {then} |")
            print(
                f"::warning title={escape_property('Rerun: ' + r['nodeid'])}::"
                + escape_data(
                    f"{r['nodeid']} failed in {r['when']} ({r['message']}); its rerun {then}"
                )
            )

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    else:
        print("\n".join(lines))

    if not records:
        return 0
    results.mkdir(parents=True, exist_ok=True)
    shutil.copy(reruns_file, results / "reruns.jsonl")
    with tarfile.open(results / "rerun-first-attempts.tar.gz", "w:gz") as tar:
        if KEPT_LOGS.is_dir():
            for log in sorted(KEPT_LOGS.rglob("*.log")):
                tar.add(log, arcname=str(log.relative_to(KEPT_LOGS)))
        for name in sorted({r["nodeid"].split("::")[-1].replace("/", "-") for r in records}):
            first = test_output / f"{name}-1"
            if not first.is_dir():
                continue
            for log in first.rglob("*.log"):
                if log.is_file() and log.stat().st_size <= LOG_LIMIT:
                    tar.add(log, arcname=str(log.relative_to(test_output)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
