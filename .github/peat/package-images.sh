#!/usr/bin/env bash
# CI: pack the build context of Peat's images from a release build made with the default
# features (not the regression suite's `--features testing`, which turns on the pageserver's
# test-only HTTP API and other test-only code): the four storage programs, compute_ctl, and the
# installs of Postgres 18 and 19 without their headers and static libraries, with debug
# information stripped (symbol names stay, so a backtrace still names its functions). Usage: package-images.sh <out.tar.zst>
set -euo pipefail
out=$1
stage=$(mktemp -d)
mkdir -p "$stage/bin" "$stage/pg"
for bin in storage_broker storage_controller safekeeper pageserver compute_ctl; do
  cp "target/release/$bin" "$stage/bin/$bin"
done
for v in v18 v19; do
  cp -a "pg_install/$v" "$stage/pg/$v"
  rm -rf "$stage/pg/$v/include"
  rm -f "$stage/pg/$v"/lib/*.a
  # neon_test_utils is a test extension marked trusted: a database owner could create it and
  # crash or stall the compute (trigger_panic, test_consume_memory and others). The suite's
  # build keeps it; the images leave it out.
  rm -f "$stage/pg/$v"/lib/postgresql/neon_test_utils.so "$stage/pg/$v"/share/postgresql/extension/neon_test_utils[.-]*
done
find "$stage/bin" "$stage/pg" -type f -print0 | while IFS= read -r -d '' f; do
  if [ "$(head -c 4 "$f" | od -An -tx1 | tr -d ' ')" = 7f454c46 ]; then
    strip --strip-debug "$f"
  fi
done
cp LICENSE NOTICE "$stage/"
{
  echo "commit: $(git rev-parse HEAD)"
  echo "postgres: $("$stage/pg/v18/bin/postgres" --version)"
  echo "postgres: $("$stage/pg/v19/bin/postgres" --version)"
  echo "built: $(date -u +%Y-%m-%dT%H:%M:%SZ) on $(uname -m)"
} > "$stage/PROVENANCE"
tar -C "$stage" -cf - . | zstd -T0 -10 -q -o "$out"
du -sh "$stage"/bin "$stage"/pg
rm -rf "$stage"
ls -l "$out"
