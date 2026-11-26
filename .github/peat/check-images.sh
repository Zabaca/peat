#!/usr/bin/env bash
# CI: check Peat's three images before they are saved for publishing: every program starts
# (--help), Postgres is 18.7 in the storage image's v18 and in the 18 compute image and 19.0
# in its v19 and in the 19 compute image, the storage image's initdb makes a cluster the way
# the pageserver runs it for each version (as its own user, its library path set, locale
# C.UTF-8, the builtin provider), each compute image carries Neon's extension, and no image
# carries its test extension neon_test_utils. Prints each image's size.
# Usage: check-images.sh <storage image> <compute image, Postgres 18> <compute image, Postgres 19>
set -euo pipefail
storage=$1; compute=$2; compute19=$3
run() { docker run --rm --network none "$@"; }
for bin in storage_broker storage_controller safekeeper pageserver; do
  run --entrypoint "/usr/local/bin/$bin" "$storage" --help > /dev/null
  echo "storage: $bin starts"
done
# Postgres 18 and 19 are the only versions in the storage image, so it creates 18 and 19
# timelines only.
run --entrypoint /bin/sh "$storage" -c 'for v in /usr/local/v*; do [ "$v" = /usr/local/v18 ] || [ "$v" = /usr/local/v19 ] || { echo "unexpected $v"; exit 1; }; done; echo "storage: Postgres 18 and 19 only"'
run --entrypoint /usr/local/v18/bin/postgres "$storage" --version | tee /dev/stderr | grep -q '^postgres (PostgreSQL) 18\.7 '
run --entrypoint /usr/local/v19/bin/postgres "$storage" --version | tee /dev/stderr | grep -q '^postgres (PostgreSQL) 19\.0 '
for v in 18 19; do
  run --entrypoint /bin/sh "$storage" -c \
    "LD_LIBRARY_PATH=/usr/local/v$v/lib /usr/local/v$v/bin/initdb -D /tmp/check --locale C.UTF-8 --locale-provider builtin -U cloud_admin --no-instructions > /tmp/initdb.log 2>&1 || { cat /tmp/initdb.log; exit 1; }; echo \"storage: initdb made \$(cat /tmp/check/PG_VERSION) (expected $v)\"; [ \"\$(cat /tmp/check/PG_VERSION)\" = $v ]"
done
check_compute() {   # <image> <Postgres major> <version pattern>
  run "$1" --help > /dev/null
  echo "compute $2: compute_ctl starts"
  run --entrypoint /usr/local/bin/postgres "$1" --version | tee /dev/stderr | grep -q "^postgres (PostgreSQL) $3 "
  run --entrypoint /bin/sh "$1" -c 'for f in neon neon_rmgr neon_utils neon_walredo; do test -f "/usr/local/lib/postgresql/$f.so" || { echo "missing $f.so"; exit 1; }; done; psql --version'
}
check_compute "$compute" 18 '18\.7'
check_compute "$compute19" 19 '19\.0'
# The test extension neon_test_utils (trusted, and able to crash a compute) is in no image:
# each of its paths is tested on its own (one `ls` of several patterns succeeds when
# only some are missing).
no_test_utils() {   # <image> <Postgres prefix>
  run --entrypoint /bin/sh "$1" -c '
    found=0
    for f in "$0"/lib/postgresql/neon_test_utils.so "$0"/share/postgresql/extension/neon_test_utils.control \
             "$0"/share/postgresql/extension/neon_test_utils--*.sql; do
      if [ -e "$f" ]; then echo "found $f"; found=1; fi
    done
    exit $found' "$2"
}
no_test_utils "$compute" /usr/local || { echo "neon_test_utils is in the compute image"; exit 1; }
no_test_utils "$compute19" /usr/local || { echo "neon_test_utils is in the 19 compute image"; exit 1; }
echo "compute: no neon_test_utils"
no_test_utils "$storage" /usr/local/v18 || { echo "neon_test_utils is in the storage image (18)"; exit 1; }
no_test_utils "$storage" /usr/local/v19 || { echo "neon_test_utils is in the storage image (19)"; exit 1; }
echo "storage: no neon_test_utils"
docker image ls --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -E "^(${storage}|${compute}|${compute19})( |$)" || true
