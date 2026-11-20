#!/usr/bin/env bash
# TEMPORARY: run the old and the changed test 30 times each, no reruns.
set -uo pipefail
export BUILD_TYPE=release DEFAULT_PG_VERSION=18
export POSTGRES_DISTRIB_DIR=$PWD/pg_install NEON_BIN=$PWD/target/release
export LD_LIBRARY_PATH=$POSTGRES_DISTRIB_DIR/v18/lib
export USE_LFC=true PAGESERVER_VIRTUAL_FILE_IO_ENGINE=tokio-epoll-uring
export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
if [ "$(ulimit -Hc)" != unlimited ]; then sudo prlimit --pid $$ --core=unlimited:unlimited; fi
ulimit -c unlimited
./scripts/pytest --tb=line -q -rA -n4 --dist=loadgroup --count=30 \
  test_runner/regress/test_peat_flake_demo.py::test_baseline_re_attach \
  test_runner/regress/test_storage_controller.py::test_re_attach_with_stuck_secondary > /tmp/flake.txt 2>&1
rc=$?
tail -3 /tmp/flake.txt
for t in test_baseline_re_attach test_re_attach_with_stuck_secondary; do
  echo "FLAKE-DEMO $t passed: $(grep -cE "^PASSED .*$t" /tmp/flake.txt) failed: $(grep -cE "^FAILED .*$t" /tmp/flake.txt)"
done
exit 0
