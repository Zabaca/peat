#!/usr/bin/env bash
# CI: run shard <index> of <shards> of Neon's regression suite on Postgres <major>,
# release build, as Neon's own CI runs its release regress job (LFC on,
# tokio-epoll-uring, --dist=loadgroup, test_historic_storage_formats deselected: it
# needs Neon's private data). Expects the build's binaries and pg_install in the
# checkout, Python deps installed with ./scripts/pysync, and TEST_OUTPUT set.
# Usage: run-shard.sh <index, or solo> <shards> <major> <workers> <junit.xml>
set -euo pipefail
k=$1; n=$2; pg=$3; workers=$4; junit=$5
export BUILD_TYPE=release DEFAULT_PG_VERSION=$pg
export POSTGRES_DISTRIB_DIR=$PWD/pg_install NEON_BIN=$PWD/target/release
export LD_LIBRARY_PATH=$POSTGRES_DISTRIB_DIR/v$pg/lib
export USE_LFC=true PAGESERVER_VIRTUAL_FILE_IO_ENGINE=tokio-epoll-uring
export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
# compute_ctl raises its core-dump limit to unlimited at start and exits if it cannot.
if [ "$(ulimit -Hc)" != unlimited ]; then sudo prlimit --pid $$ --core=unlimited:unlimited; fi
ulimit -c unlimited
deselect=(--deselect test_runner/regress/test_compatibility.py::test_historic_storage_formats)
./scripts/pytest --collect-only -q test_runner/regress "${deselect[@]}" | grep -E '^test_runner/[^ ]+::[^ ]+$' > /tmp/collected.txt
if [ "$k" = solo ]; then
  python3 -I .github/peat/shard.py --shards "$n" --index 1 --solo < /tmp/collected.txt > /tmp/shard.txt
else
  python3 -I .github/peat/shard.py --shards "$n" --index "$k" < /tmp/collected.txt > /tmp/shard.txt
fi
echo "shard $k of $n: $(wc -l < /tmp/shard.txt) of $(wc -l < /tmp/collected.txt) tests"
mapfile -t ids < /tmp/shard.txt
# A failed test is rerun once (pytest-rerunfailures, which Neon's own CI used with
# --reruns 2): the rule Peat's suite is judged by reruns each failure once.
./scripts/pytest --junitxml="$junit" --tb=short -rA -n"$workers" --dist=loadgroup \
  --reruns 1 --session-timeout=10800 "${deselect[@]}" "${ids[@]}"
