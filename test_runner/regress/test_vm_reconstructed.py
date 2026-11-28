"""
The visibility map against the heap pages as the pageserver gives them back.

Two ways the map is set: by VACUUM, and (on Postgres 19) by pruning on access, where a read of a
full page sets its map bits through the prune record. Then deletes and updates clear some bits,
the compute restarts so every page comes from the pageserver, and the map must agree with the heap:
no page with its map bit set and PD_ALL_VISIBLE clear, pg_check_visible and pg_check_frozen empty,
and an index-only count equal to a sequential one. With the compute's buffers small, pages also go
to and come back from the pageserver during the work. On 19 a page whose map bit is set while its
PD_ALL_VISIBLE is clear is reported on access ("page is not marked all-visible but visibility map
bit is set"); on 18 only VACUUM reports it.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log
from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from fixtures.neon_fixtures import Endpoint, NeonEnvBuilder

ROWS = 100_000


def check(endpoint: Endpoint, phase: str) -> dict[str, int]:
    found: dict[str, int] = {}
    with endpoint.cursor() as cur:
        cur.execute(
            "select count(*) from pg_visibility('t') where all_visible and not pd_all_visible"
        )
        found["map_set_page_clear"] = cur.fetchone()[0]
        cur.execute("select count(*) from pg_visibility('t') where all_visible")
        found["map_set"] = cur.fetchone()[0]
        cur.execute("select count(*) from pg_check_visible('t')")
        found["check_visible"] = cur.fetchone()[0]
        cur.execute("select count(*) from pg_check_frozen('t')")
        found["check_frozen"] = cur.fetchone()[0]
        cur.execute(
            "set enable_indexscan = off; set enable_indexonlyscan = off; set enable_bitmapscan = off"
        )
        cur.execute("select count(*) from t")
        found["seq_count"] = cur.fetchone()[0]
        cur.execute("reset all; set enable_seqscan = off; set enable_bitmapscan = off")
        cur.execute("explain select count(*) from t where id > 0")
        assert "Index Only Scan" in " ".join(r[0] for r in cur.fetchall())
        cur.execute("select count(*) from t where id > 0")
        found["index_only_count"] = cur.fetchone()[0]
    log.info(f"{phase}: {found}")
    return found


def restart_and_check(endpoint: Endpoint, phase: str) -> dict[str, int]:
    before = check(endpoint, f"{phase}, before the restart")
    # Every page from the pageserver
    endpoint.stop()
    endpoint.start()
    after = check(endpoint, f"{phase}, after the restart")
    assert after["seq_count"] == before["seq_count"], (before, after)
    for found in (before, after):
        assert found["map_set_page_clear"] == 0, found
        assert found["check_visible"] == 0, found
        assert found["check_frozen"] == 0, found
        assert found["index_only_count"] == found["seq_count"], found
    return after


@pytest.mark.parametrize("set_by", ["vacuum", "access"])
def test_vm_reconstructed(neon_env_builder: NeonEnvBuilder, pg_version: PgVersion, set_by: str):
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start(
        "main", config_lines=["shared_buffers = 1MB", "autovacuum = off"]
    )

    def set_map(cur):
        if set_by == "vacuum":
            cur.execute("vacuum freeze t")
        else:
            # Full pages with a prune xid (on 19, every insert leaves one) are pruned when read,
            # which sets their map bits
            for _ in range(2):
                cur.execute("select count(*) from t")

    with endpoint.cursor() as cur:
        cur.execute("create extension pg_visibility")
        cur.execute("create table t (id int primary key, v text)")
        # A bulk load, as pg_dump | psql gives it: COPY, no FREEZE
        data = io.StringIO("".join(f"{i}\t{'x' * 100}\n" for i in range(1, ROWS + 1)))
        cur.copy_expert("copy t from stdin", data)
        set_map(cur)
    loaded = restart_and_check(endpoint, f"loaded, map set by {set_by}")
    if set_by == "vacuum":
        assert loaded["map_set"] > 0, loaded

    with endpoint.cursor() as cur:
        # Changes on some pages, clearing their bits, then the map set again
        cur.execute("delete from t where id between 1000 and 3000 and id % 3 = 0")
        cur.execute("update t set v = 'y' where id between 5000 and 7000 and id % 5 = 0")
        cur.execute(
            f"insert into t select g, 'z' from generate_series({ROWS + 1}, {ROWS + 3000}) g"
        )
        set_map(cur)
        cur.execute("delete from t where id between 20000 and 21000")
        cur.execute("update t set v = 'w' where id between 30000 and 30500")
        cur.execute("select count(*) from t")
    changed = restart_and_check(endpoint, f"changed, map set by {set_by}")
    # Pages the pageserver rebuilds carry the prune xid of a delete or update (Neon's redo of an
    # insert leaves none), so on 19 the reads after the changes set the map
    if set_by == "vacuum" or pg_version >= PgVersion.V19:
        assert changed["map_set"] > 0, changed

    warned = endpoint.log_contains("visibility map bit is set")
    assert warned is None, warned
