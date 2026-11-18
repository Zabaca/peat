from __future__ import annotations

import re
from typing import TYPE_CHECKING

import psycopg2.errors
import pytest
from fixtures.neon_fixtures import NeonEnv, fork_at_current_lsn, wait_replica_caughtup
from fixtures.pg_version import PgVersion
from fixtures.utils import USE_LFC

if TYPE_CHECKING:
    from psycopg2.extensions import cursor as Cursor


#
# Test UNLOGGED tables/relations. Postgres copies init fork contents to main
# fork to reset them during recovery. In Neon, pageserver directly sends init
# fork contents as main fork during basebackup.
#
def test_unlogged(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()

    cur.execute("CREATE UNLOGGED TABLE iut (id int);")
    # create index to test unlogged index relations as well
    cur.execute("CREATE UNIQUE INDEX iut_idx ON iut (id);")
    cur.execute("CREATE INDEX ON iut USING gist (int4range(id, id, '[]'));")
    cur.execute("CREATE INDEX ON iut USING spgist (int4range(id, id, '[]'));")
    cur.execute("CREATE INDEX ON iut USING gin ((id::text::jsonb));")
    cur.execute("CREATE INDEX ON iut USING brin (id);")
    cur.execute("CREATE INDEX ON iut USING hash (id);")
    cur.execute("ALTER TABLE iut ADD COLUMN seq int GENERATED ALWAYS AS IDENTITY;")
    cur.execute("INSERT INTO iut (id) values (42);")

    # create another compute to fetch inital empty contents from pageserver
    fork_at_current_lsn(env, endpoint, "test_unlogged_basebackup", "main")
    endpoint2 = env.endpoints.create_start("test_unlogged_basebackup")

    conn2 = endpoint2.connect()
    cur2 = conn2.cursor()
    # after restart table should be empty but valid
    cur2.execute("PREPARE iut_plan (int) AS INSERT INTO iut (id) VALUES ($1)")
    cur2.execute("EXECUTE iut_plan (43);")
    cur2.execute("SELECT * FROM iut")
    results = cur2.fetchall()
    # Unlogged sequences were introduced in v15. On <= v14, the sequence created
    # for the GENERATED ALWAYS AS IDENTITY column is logged, and hence it keeps
    # the old value (2) on restart. While on v15 and above, it's unlogged, so it
    # gets reset to 1.
    if env.pg_version <= PgVersion.V14:
        assert results == [(43, 2)]
    else:
        assert results == [(43, 1)]

    # Flush all data and compact it, so we detect any errors related to
    # unlogged indexes materialization.
    ps_http = env.pageserver.http_client()
    ps_http.timeline_compact(
        tenant_id=env.initial_tenant,
        timeline_id=env.initial_timeline,
        force_image_layer_creation=True,
    )


#
# A read replica replays the init fork of an unlogged relation. Since PostgreSQL 18.7
# (upstream 62760571307) sequence redo flushes an unlogged sequence's init-fork page at
# once, and the hash index's init-page redo does the same; since v18 Neon flushes
# init-fork buffers as unlogged, that is to a local file, which a replica never created.
# The replica must replay each case, keep serving reads, and come back after a restart.
# Unlogged sequences exist from v15.
#
@pytest.mark.parametrize("case", ["sequence", "serial", "hash_index"])
def test_unlogged_init_fork_on_replica(neon_simple_env: NeonEnv, case: str):
    env = neon_simple_env
    if case != "hash_index" and env.pg_version <= PgVersion.V14:
        pytest.skip("unlogged sequences exist from v15")

    primary = env.endpoints.create_start("main", endpoint_id="primary")
    replica = env.endpoints.new_replica_start(origin=primary, endpoint_id="replica")

    pcur = primary.connect().cursor()
    pcur.execute("CREATE TABLE logged_marker (id int)")
    if case == "sequence":
        # Creation writes the init fork; RESTART rewrites the sequence.
        pcur.execute("CREATE UNLOGGED SEQUENCE useq")
        pcur.execute("SELECT nextval('useq')")
        pcur.execute("ALTER SEQUENCE useq RESTART WITH 100")
        relname = "useq"
    elif case == "serial":
        # A serial column of an unlogged table makes an unlogged sequence;
        # TRUNCATE ... RESTART IDENTITY rewrites it.
        pcur.execute("CREATE UNLOGGED TABLE userial (id serial, v int)")
        pcur.execute("INSERT INTO userial (v) VALUES (1), (2)")
        pcur.execute("TRUNCATE userial RESTART IDENTITY")
        relname = "userial_id_seq"
    else:
        pcur.execute("CREATE UNLOGGED TABLE uhash (v int)")
        pcur.execute("CREATE INDEX uhash_idx ON uhash USING hash (v)")
        relname = "uhash_idx"
    pcur.execute("INSERT INTO logged_marker VALUES (1)")

    def replica_serves() -> None:
        wait_replica_caughtup(primary, replica)
        rcur = replica.connect().cursor()
        rcur.execute("SELECT count(*) FROM logged_marker")
        assert rcur.fetchone() == (1,)
        rcur.execute("SELECT relpersistence FROM pg_class WHERE relname = %s", (relname,))
        assert rcur.fetchone() == ("u",)

    replica_serves()
    assert replica.log_contains(r'could not open file "base/\d+/\d+_init"') is None

    # The replica comes back after a restart, and replays the next such change too.
    replica.stop()
    replica.start()
    if case == "hash_index":
        pcur.execute("CREATE INDEX uhash_idx2 ON uhash USING hash (v)")
    else:
        pcur.execute("CREATE UNLOGGED SEQUENCE useq2")
    pcur.execute("INSERT INTO logged_marker VALUES (2)")
    wait_replica_caughtup(primary, replica)
    rcur = replica.connect().cursor()
    rcur.execute("SELECT count(*) FROM logged_marker")
    assert rcur.fetchone() == (2,)


#
# The fourth case of the replica test: promotion. A replica replays the creation of an
# unlogged table with a serial column while the primary writes to it; the replica is then
# promoted, by pg_promote() and by compute_ctl's /promote (the two ways
# test_replica_promotes promotes), and used. Promotion ends recovery, which resets every
# unlogged relation to its init fork, so the promoted node should see the table empty, its
# sequence restarted, and take inserts.
#
# A known limit, already in Neon on 17 (see README.md): the promoted node has no local file
# for the unlogged table (its creation was replayed through Neon's storage, which makes
# none), so the end of recovery has nothing to reset it from, and reading it fails with
# 'could not open file "base/N/M"' (the main fork). The test then restarts the promoted
# compute as a primary, which takes a fresh base backup carrying the table's files, and
# checks that the table works again; only after that does it raise PromotedUnloggedLimit,
# the one failure the marker expects. The marker is strict: a fix fails it.
#
class PromotedUnloggedLimit(Exception):
    """The known limit: a promoted hot standby cannot read an unlogged table it replayed."""


@pytest.mark.skipif(not USE_LFC, reason="promotion is tested with LFC, as test_replica_promotes")
@pytest.mark.parametrize("method", ["postgres", "compute-ctl"])
def test_unlogged_serial_on_promoted_replica(
    neon_simple_env: NeonEnv, method: str, request: pytest.FixtureRequest
):
    env = neon_simple_env
    if env.pg_version <= PgVersion.V14:
        pytest.skip("unlogged sequences exist from v15")
    request.node.add_marker(
        pytest.mark.xfail(
            strict=True,
            raises=PromotedUnloggedLimit,
            reason="known limit, as in Neon: a promoted replica cannot read an unlogged table it replayed",
        )
    )

    primary = env.endpoints.create_start("main", endpoint_id="primary")
    replica = env.endpoints.new_replica_start(origin=primary, endpoint_id="replica")

    pcur = primary.connect().cursor()
    if method == "compute-ctl":
        pcur.execute("CREATE SCHEMA neon; CREATE EXTENSION neon WITH SCHEMA neon")
    pcur.execute("CREATE TABLE logged_marker (id int)")
    pcur.execute("CREATE UNLOGGED TABLE userial (id serial, v int)")
    pcur.execute("INSERT INTO userial (v) VALUES (1), (2)")
    pcur.execute("INSERT INTO logged_marker VALUES (1)")
    pcur.execute("SHOW neon.safekeepers")
    row = pcur.fetchone()
    assert row is not None
    safekeepers = row[0]

    wait_replica_caughtup(primary, replica)
    rcur = replica.connect().cursor()
    rcur.execute("SELECT relpersistence FROM pg_class WHERE relname = 'userial'")
    assert rcur.fetchone() == ("u",)

    # Promote as test_replica_promotes does.
    if method == "compute-ctl":
        primary.http_client().offload_lfc()
    primary_spec = primary.get_compute_spec()
    primary_endpoint_id = primary.endpoint_id
    primary.stop(mode="immediate-terminate")
    if method == "compute-ctl":
        client = replica.http_client()
        client.prewarm_lfc(primary_endpoint_id)
        assert (lsn := primary.terminate_flush_lsn)
        assert (
            client.promote({"spec": primary_spec, "wal_flush_lsn": str(lsn)})["status"]
            == "completed"
        )
    else:
        promo_cur = replica.connect().cursor()
        promo_cur.execute(f"ALTER SYSTEM SET neon.safekeepers='{safekeepers}'")
        promo_cur.execute("SELECT pg_reload_conf()")
        promo_cur.execute("SELECT pg_promote()")
        assert promo_cur.fetchone() == (True,)

    def unlogged_table_works(cur: Cursor) -> None:
        cur.execute("SELECT count(*) FROM userial")
        assert cur.fetchone() == (0,)
        cur.execute("INSERT INTO userial (v) VALUES (3) RETURNING id")
        assert cur.fetchone() == (1,)

    limit = None
    with replica.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM logged_marker")
        assert cur.fetchone() == (1,)
        try:
            unlogged_table_works(cur)
        except psycopg2.errors.UndefinedFile as e:
            # Only the table's own main fork counts: a path with no fork suffix.
            if not re.search(r'could not open file "base/\d+/\d+"', str(e)):
                raise
            limit = str(e).splitlines()[0]

    # The way out: restarted as a primary, the compute takes a fresh base backup, which
    # carries the unlogged table's files, so the table works again.
    replica.stop()
    replica.respec(mode="Primary")
    replica.start()
    with replica.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM logged_marker")
        assert cur.fetchone() == (1,)
        unlogged_table_works(cur)

    if limit is not None:
        raise PromotedUnloggedLimit(limit)
