from __future__ import annotations

import os
import re
import shutil
from typing import TYPE_CHECKING

import pytest
from fixtures.pg_version import PgVersion
from fixtures.utils import subprocess_capture

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv, PgBin


def check_wal_segment(pg_waldump_path: str, segment_path: str, test_output_dir):
    # use special --ignore option to ignore the validation checks in pg_waldump
    # this is necessary, because neon WAL files contain gap at the beginning
    output_path, _, _ = subprocess_capture(
        test_output_dir, [pg_waldump_path, "--ignore", segment_path]
    )

    with open(f"{output_path}.stdout") as f:
        stdout = f.read()
        assert "ABORT" in stdout
        assert "COMMIT" in stdout


# Simple test to check that pg_waldump works with neon WAL files
def test_pg_waldump(neon_simple_env: NeonEnv, test_output_dir, pg_bin: PgBin):
    env = neon_simple_env
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    endpoint = env.endpoints.create_start("main")

    cur = endpoint.connect().cursor()
    cur.execute(
        """
        BEGIN;
        CREATE TABLE t1(i int primary key, n_updated int);
        INSERT INTO t1 select g, 0 from generate_series(1, 50) g;
        ROLLBACK;
    """
    )

    cur.execute(
        """
        BEGIN;
        CREATE TABLE t1(i int primary key, n_updated int);
        INSERT INTO t1 select g, 0 from generate_series(1, 50) g;
        COMMIT;
    """
    )

    # stop the endpoint to make sure that WAL files are flushed and won't change
    endpoint.stop()

    assert endpoint.pgdata_dir
    wal_path = os.path.join(endpoint.pgdata_dir, "pg_wal/000000010000000000000001")
    pg_waldump_path = os.path.join(pg_bin.pg_bin_path, "pg_waldump")
    # check segment on compute
    check_wal_segment(pg_waldump_path, wal_path, test_output_dir)

    # Check file on safekeepers as well. pg_waldump is strict about file naming, so remove .partial suffix.
    sk = env.safekeepers[0]
    sk_tli_dir = sk.timeline_dir(tenant_id, timeline_id)
    non_partial_path = os.path.join(sk_tli_dir, "000000010000000000000001")
    shutil.copyfile(os.path.join(sk_tli_dir, "000000010000000000000001.partial"), non_partial_path)
    check_wal_segment(pg_waldump_path, non_partial_path, test_output_dir)


# pg_waldump --ignore must go on past a record it skips as invalid. A copy of the
# compute's WAL segment gets one byte flipped inside a record in its middle (so the
# record's checksum fails); with --ignore, records after it must still be listed.
def test_pg_waldump_ignore_skips_corrupt_record(
    neon_simple_env: NeonEnv, test_output_dir, pg_bin: PgBin, request: pytest.FixtureRequest
):
    env = neon_simple_env
    if env.pg_version < PgVersion.V18:
        # The fix is in Peat's Postgres 18 tree only; the older trees keep the defect.
        request.node.add_marker(
            pytest.mark.xfail(strict=True, reason="pg_waldump --ignore fix is in Postgres 18 only")
        )
    endpoint = env.endpoints.create_start("main")
    cur = endpoint.connect().cursor()
    cur.execute("CREATE TABLE t(i int primary key, n int)")
    cur.execute("INSERT INTO t SELECT g, 0 FROM generate_series(1, 200) g")
    endpoint.stop()

    assert endpoint.pgdata_dir
    segment = os.path.join(endpoint.pgdata_dir, "pg_wal/000000010000000000000001")
    pg_waldump_path = os.path.join(pg_bin.pg_bin_path, "pg_waldump")

    def records(path: str) -> list[tuple[int, int]]:
        output_path, _, _ = subprocess_capture(test_output_dir, [pg_waldump_path, "--ignore", path])
        found = []
        with open(f"{output_path}.stdout") as f:
            for line in f:
                m = re.search(
                    r"len \(rec/tot\):\s+\d+/\s*(\d+), .*lsn: ([0-9A-F]+)/([0-9A-F]+),", line
                )
                if m:
                    found.append(
                        ((int(m.group(2), 16) << 32) | int(m.group(3), 16), int(m.group(1)))
                    )
        return found

    before = records(segment)
    assert len(before) > 100
    xlog_blcksz = 8192
    segment_start = 0x1000000  # segment 000000010000000000000001, 16 MB segments
    # A record in the middle that ends on the page it starts on, so that the byte
    # flipped is that record's own last byte and no page header is touched.
    middle = [
        (lsn, tot)
        for lsn, tot in before[len(before) // 3 : 2 * len(before) // 3]
        if lsn % xlog_blcksz + tot < xlog_blcksz
    ]
    assert middle
    bad_lsn, bad_len = middle[0]

    corrupt = os.path.join(test_output_dir, "000000010000000000000001")
    shutil.copyfile(segment, corrupt)
    with open(corrupt, "r+b") as f:
        f.seek(bad_lsn - segment_start + bad_len - 1)
        byte = f.read(1)
        f.seek(-1, 1)
        f.write(bytes([byte[0] ^ 0xFF]))

    after = records(corrupt)
    listed = {lsn for lsn, _ in after}
    assert bad_lsn not in listed
    # After skipping an invalid record, --ignore resumes at the next page: the rest
    # of the skipped record's page is not listed (the skip goes on reading the
    # buffer of the page it looked ahead to). Every record from the next page on
    # must be listed. Before the fix in peat-postgres the reader kept the skipped
    # record's error queued, returned one more record and then stopped.
    next_page = (bad_lsn // xlog_blcksz + 1) * xlog_blcksz
    expected_later = [lsn for lsn, _ in before if lsn >= next_page]
    assert expected_later
    missing = [lsn for lsn in expected_later if lsn not in listed]
    assert not missing, (
        f"{len(missing)} of the {len(expected_later)} records after the skipped record's page are missing"
    )
