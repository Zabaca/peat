#!/usr/bin/env bash
# CI: run shard <index> of <shards> of Neon's regression suite on Postgres <major>,
# release build, as Neon's own CI runs its release regress job (LFC on,
# tokio-epoll-uring, --dist=loadgroup, test_historic_storage_formats deselected: it
# needs Neon's private data). Expects the build's binaries and pg_install in the
# checkout, Python deps installed with ./scripts/pysync, and TEST_OUTPUT set.
# Usage: run-shard.sh <index, solo, or records> <shards> <major> <workers> <junit.xml>
#
# `records`: Neon's suites for the storage controller (its controller, sharding and generations
# files) with the controller's records in Kubernetes objects instead of its Postgres: a test API
# server with the record kinds applied (storage_controller/kubernetes/test-stores.sh, which needs
# KUBE_ASSETS), and neon_local starting the controller on it (STORAGE_CONTROLLER_RECORDS_KUBECONFIG).
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
wrap=()
if [ "$k" = records ]; then
  printf '%s\n' test_runner/regress/test_storage_controller.py test_runner/regress/test_sharding.py \
    test_runner/regress/test_pageserver_generations.py > /tmp/shard.txt
  export PG_BIN=$POSTGRES_DISTRIB_DIR/v$pg/bin
  # shellcheck disable=SC2016 # expanded by the inner shell, once test-stores.sh has set it
  wrap=(storage_controller/kubernetes/test-stores.sh bash -c \
    'export STORAGE_CONTROLLER_RECORDS_KUBECONFIG="$STORCON_TEST_KUBECONFIG"; exec "$@"' records)
elif [ "$k" = solo ]; then
  python3 -I .github/peat/shard.py --shards "$n" --index 1 --solo < /tmp/collected.txt > /tmp/shard.txt
else
  python3 -I .github/peat/shard.py --shards "$n" --index "$k" < /tmp/collected.txt > /tmp/shard.txt
fi
echo "shard $k of $n: $(wc -l < /tmp/shard.txt) of $(wc -l < /tmp/collected.txt) tests (or files)"
mapfile -t ids < /tmp/shard.txt
# A failed test is rerun once (pytest-rerunfailures; Neon's own CI used --reruns 2), so a
# timing failure on a shared runner does not fail the shard. A rerun that passes leaves the
# job green, so every rerun is recorded (.github/peat/peat_reruns.py, into $PEAT_RERUNS)
# and reported by .github/peat/report_reruns.py; CONTRIBUTING.md says what a rerun means.
export PEAT_RERUNS=${PEAT_RERUNS:-${RUNNER_TEMP:-/tmp}/reruns.jsonl}
export PYTHONPATH=$PWD/.github/peat${PYTHONPATH:+:$PYTHONPATH}
# A failed attempt's test output is removed before its rerun starts (the rerun of a test
# that writes 12 GB needs the room); its logs go to PEAT_RERUN_LOGS, for report_reruns.py.
export PEAT_RERUN_LOGS=${PEAT_RERUN_LOGS:-${RUNNER_TEMP:-/tmp}/rerun-first-attempts}
"${wrap[@]}" ./scripts/pytest --junitxml="$junit" --tb=short -rA -n"$workers" --dist=loadgroup \
  -p peat_reruns --reruns 1 --session-timeout=10800 "${deselect[@]}" "${ids[@]}"
