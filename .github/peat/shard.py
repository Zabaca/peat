#!/usr/bin/env python3
"""Split Neon's regression suite into N shards of about equal running time.

Usage: python3 -I .github/peat/shard.py --shards N --index K [--durations FILE] < collected.txt

Reads pytest node ids (one per line, as `pytest --collect-only -q` prints them) on
stdin and prints the ones that belong to shard K (1-based). Each test is weighed by
the seconds in the durations file (its id with the Postgres version written as pgNN;
a test missing there gets the median) and placed, heaviest first, on the least
loaded shard, so every shard of the same run computes the same split and every
collected test lands in exactly one shard. Tests are placed one by one, not by
function: the four upstream pg_regress runs are the suite's longest and most
disk-hungry tests, and would otherwise share one shard.

Tests listed in solo-tests.txt (node id prefixes) are left out of every shard: each
needs most of a runner's disk to itself and runs in a job of its own (--solo prints
them).
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from pathlib import Path

NODE_ID = re.compile(r"^test_runner/\S+\.py::\S+$")


def weight_key(node_id: str) -> str:
    return re.sub(r"release-pg\d+", "release-pgNN", node_id)


def split(node_ids: list[str], durations: dict[str, float], shards: int) -> list[list[str]]:
    default = statistics.median(durations.values()) if durations else 1.0
    weight = {n: durations.get(weight_key(n), default) for n in set(node_ids)}
    loads = [0.0] * shards
    out: list[list[str]] = [[] for _ in range(shards)]
    names: list[set[str]] = [set() for _ in range(shards)]
    for node_id in sorted(weight, key=lambda n: (-weight[n], n)):
        # Two tests with the same name (in different files) share a test output directory
        # (Neon's fixtures name it after the test alone), so they must not run at once:
        # keep them in different shards when there is one free of that name.
        name = node_id.split("::", 1)[1]
        free = [i for i in range(shards) if name not in names[i]] or list(range(shards))
        target = min(free, key=lambda i: (loads[i], i))
        loads[target] += weight[node_id]
        names[target].add(name)
        out[target].append(node_id)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument(
        "--durations", default=str(Path(__file__).with_name("test-durations.json"))
    )
    parser.add_argument("--solo-file", default=str(Path(__file__).with_name("solo-tests.txt")))
    parser.add_argument("--solo", action="store_true", help="print the solo tests instead")
    parser.add_argument("--summary", action="store_true", help="print each shard's weight instead")
    args = parser.parse_args()
    if not 1 <= args.index <= args.shards:
        sys.exit(f"--index must be between 1 and {args.shards}")
    durations = json.loads(Path(args.durations).read_text())["durations"]
    # Only node ids: collection also prints warnings that mention test ids.
    node_ids = [line.strip() for line in sys.stdin if NODE_ID.match(line.strip())]
    solo_prefixes = [
        line.strip()
        for line in Path(args.solo_file).read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    solo = [n for n in node_ids if any(n.startswith(p) for p in solo_prefixes)]
    if args.solo:
        print("\n".join(solo))
        return
    node_ids = [n for n in node_ids if n not in set(solo)]
    shards = split(node_ids, durations, args.shards)
    if args.summary:
        default = statistics.median(durations.values())
        for i, ids in enumerate(shards, 1):
            minutes = sum(durations.get(weight_key(n), default) for n in ids) / 60
            print(f"shard {i}: {len(ids)} tests, {minutes:.1f} test-minutes")
        return
    print("\n".join(shards[args.index - 1]))


if __name__ == "__main__":
    main()
