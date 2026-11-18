#!/usr/bin/env bash
# CI: pack what Neon's regression suite needs from a release build (every binary target,
# as Neon's own CI collects them, and pg_install) into one zstd tarball for the shards.
# Usage: package-binaries.sh <output.tar.zst>
set -euo pipefail
out=$1
stage=$(mktemp -d)
mkdir -p "$stage/target/release"
for bin in $(cargo metadata --locked --format-version=1 --no-deps | jq -r '.packages[].targets[] | select(.kind | index("bin")) | .name'); do
  cp "target/release/$bin" "$stage/target/release/$bin"
done
cp -a pg_install "$stage/pg_install"
tar -C "$stage" -cf - . | zstd -T0 -10 -q -o "$out"
rm -rf "$stage"
ls -l "$out"
