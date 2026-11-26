#!/usr/bin/env bash
# CI: pack what Neon's regression suite needs from a release build (every binary target,
# as Neon's own CI collects them, pg_install, and the Postgres build tree's regress and
# isolation test directories) into one zstd tarball for the shards.
# Usage: package-binaries.sh <output.tar.zst>
set -euo pipefail
out=$1
stage=$(mktemp -d)
mkdir -p "$stage/target/release"
for bin in $(cargo metadata --locked --format-version=1 --no-deps | jq -r '.packages[].targets[] | select(.kind | index("bin")) | .name'); do
  cp "target/release/$bin" "$stage/target/release/$bin"
done
cp -a pg_install "$stage/pg_install"
# The upstream pg_regress and isolation suites run from the Postgres build tree
# (test_runner/regress/test_pg_regress.py looks for pg_install/../build/vNN/src/test/...).
for t in build/v18/src/test/regress build/v18/src/test/isolation \
         build/v19/src/test/regress build/v19/src/test/isolation; do
  [ -d "$t" ] || continue
  mkdir -p "$stage/$(dirname "$t")"
  cp -a "$t" "$stage/$t"
done
tar -C "$stage" -cf - . | zstd -T0 -10 -q -o "$out"
rm -rf "$stage"
ls -l "$out"
