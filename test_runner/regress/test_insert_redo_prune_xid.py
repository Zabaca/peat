"""
On Postgres 19 an insert marks its heap page prunable (pd_prune_xid), so that a later read of the
full page prunes it and sets its visibility map bits; 19's redo of an insert sets the same mark. A
page the pageserver rebuilds from Neon's insert records (redone by neon_rmgr) must carry it too, or
the page is never pruned on access.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pytest
from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv

ROWS = 2000


def test_insert_redo_prune_xid(neon_simple_env: NeonEnv, pg_version: PgVersion):
    if pg_version < PgVersion.V19:
        pytest.skip("inserts mark the page prunable from Postgres 19 on")
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main", config_lines=["autovacuum = off"])
    with endpoint.cursor() as cur:
        cur.execute("create extension pageinspect")
        cur.execute("create extension pg_visibility")
        # One insert record per row
        cur.execute("create table single (id int, v text)")
        cur.execute(
            f"insert into single select g, repeat('x', 100) from generate_series(1, {ROWS}) g"
        )
        # Multi-insert records, as COPY writes them
        cur.execute("create table multi (id int, v text)")
        data = io.StringIO("".join(f"{i}\t{'x' * 100}\n" for i in range(1, ROWS + 1)))
        cur.copy_expert("copy multi from stdin", data)
    # COPY FREEZE: multi-insert records that set the page all-frozen (the table must be made in
    # the same transaction)
    with endpoint.cursor(autocommit=False) as cur:
        cur.execute("create table frozen (id int, v text)")
        data = io.StringIO("".join(f"{i}\t{'x' * 100}\n" for i in range(1, ROWS + 1)))
        cur.copy_expert("copy frozen from stdin with (freeze)", data)
        cur.connection.commit()

    # Every page from the pageserver, rebuilt from the insert records
    endpoint.stop()
    endpoint.start()

    with endpoint.cursor() as cur:
        # An all-frozen page needs no pruning: no mark, and PD_ALL_VISIBLE set (0x0004)
        cur.execute("select prune_xid::text, flags from page_header(get_raw_page('frozen', 0))")
        prune_xid, flags = cur.fetchone()
        assert prune_xid == "0", (
            f"frozen: the rebuilt all-frozen page is marked prunable ({prune_xid})"
        )
        assert flags & 0x0004, f"frozen: the rebuilt all-frozen page lacks PD_ALL_VISIBLE ({flags})"

        for table in ["single", "multi"]:
            cur.execute(f"select prune_xid::text from page_header(get_raw_page('{table}', 0))")
            prune_xid = cur.fetchone()[0]
            assert prune_xid != "0", f"{table}: the rebuilt page is not marked prunable"
            # Reads prune the full pages on access, which sets their visibility map bits
            for _ in range(2):
                cur.execute(f"select count(*) from {table}")
            cur.execute(f"select count(*) from pg_visibility_map('{table}') where all_visible")
            set_ = cur.fetchone()[0]
            assert set_ > 0, f"{table}: no page pruned on access after the restart"
