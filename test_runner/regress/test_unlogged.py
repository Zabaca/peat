from __future__ import annotations

import pytest
from fixtures.neon_fixtures import NeonEnv, fork_at_current_lsn, wait_replica_caughtup
from fixtures.pg_version import PgVersion


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
# A read replica replays an unlogged sequence's init fork. Since PostgreSQL 18.7
# (upstream 62760571307) sequence redo flushes that init-fork page at once; on a
# Neon replica the flush goes through Neon's storage manager. The replica must
# replay it and keep serving reads. Unlogged sequences exist from v15.
#
class ReplicaInitForkDefect(Exception):
    """The known Peat 18.7 defect: the replica died on a missing local _init file."""


def test_unlogged_sequence_on_replica(neon_simple_env: NeonEnv, request: pytest.FixtureRequest):
    env = neon_simple_env
    if env.pg_version <= PgVersion.V14:
        return
    if env.pg_version >= PgVersion.V18:
        # A known defect of Peat's Postgres 18.7, found by this test (see README.md):
        # the replica's startup process fails with 'could not open file
        # "base/N/M_init"' when it replays the sequence's init-fork flush, and the
        # replica goes down. 17 passes. Only that failure counts as expected: any
        # other failure of this test still fails it.
        request.node.add_marker(
            pytest.mark.xfail(
                strict=True,
                raises=ReplicaInitForkDefect,
                reason="Peat 18.7: a replica dies replaying an unlogged sequence's init-fork flush",
            )
        )

    primary = env.endpoints.create_start("main", endpoint_id="primary")
    replica = env.endpoints.new_replica_start(origin=primary, endpoint_id="replica")

    pcur = primary.connect().cursor()
    pcur.execute("CREATE TABLE logged_marker (id int)")
    pcur.execute("CREATE UNLOGGED SEQUENCE useq")
    pcur.execute("SELECT nextval('useq'), nextval('useq'), nextval('useq')")
    pcur.execute("ALTER SEQUENCE useq RESTART WITH 100")
    pcur.execute("SELECT nextval('useq')")
    assert pcur.fetchone() == (100,)
    pcur.execute("INSERT INTO logged_marker VALUES (1)")

    try:
        wait_replica_caughtup(primary, replica)
        rcur = replica.connect().cursor()
        rcur.execute("SELECT count(*) FROM logged_marker")
        assert rcur.fetchone() == (1,)
        rcur.execute("SELECT relpersistence FROM pg_class WHERE relname = 'useq'")
        assert rcur.fetchone() == ("u",)
    except Exception as e:
        found = replica.log_contains(r'could not open file "base/\d+/\d+_init"')
        if found is not None:
            raise ReplicaInitForkDefect(found[0].strip()) from e
        raise

    # The primary's sequence still works after the replica replayed it.
    pcur.execute("SELECT nextval('useq')")
    assert pcur.fetchone() == (101,)
