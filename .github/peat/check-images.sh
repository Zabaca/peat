#!/usr/bin/env bash
# CI: check Peat's two images before they are saved for publishing: every program starts
# (--help), Postgres is 18.7 in both, the storage image's initdb makes a cluster the way the
# pageserver runs it (as its own user, locale C.UTF-8, the builtin provider), and the compute
# image carries Neon's extension. Prints each image's size.
# Usage: check-images.sh <storage image> <compute image>
set -euo pipefail
storage=$1; compute=$2
run() { docker run --rm --network none "$@"; }
for bin in storage_broker storage_controller safekeeper pageserver; do
  run --entrypoint "/usr/local/bin/$bin" "$storage" --help > /dev/null
  echo "storage: $bin starts"
done
run --entrypoint /usr/local/v18/bin/postgres "$storage" --version | tee /dev/stderr | grep -qx 'postgres (PostgreSQL) 18.7'
run --entrypoint /bin/sh "$storage" -c \
  '/usr/local/v18/bin/initdb -D /tmp/check --locale C.UTF-8 --locale-provider builtin -U cloud_admin --no-instructions > /tmp/initdb.log 2>&1 || { cat /tmp/initdb.log; exit 1; }; echo "storage: initdb made $(cat /tmp/check/PG_VERSION)"'
run "$compute" --help > /dev/null
echo "compute: compute_ctl starts"
run --entrypoint /usr/local/bin/postgres "$compute" --version | tee /dev/stderr | grep -qx 'postgres (PostgreSQL) 18.7'
run --entrypoint /bin/sh "$compute" -c 'for f in neon neon_rmgr neon_utils neon_walredo; do test -f "/usr/local/lib/postgresql/$f.so" || { echo "missing $f.so"; exit 1; }; done; psql --version'
docker image ls --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep -E "^(${storage}|${compute})( |$)" || true
