#!/usr/bin/env bash
# CI: give the regression suite a filesystem of its own for its test output. Neon's
# pageserver starts disk-pressure eviction (and the test then fails its log check) when
# its filesystem is over 80% used; on a hosted runner's root filesystem, already well
# used by the image, that point is near. A loop-mounted ext4 file of known size, with
# no reserved blocks, makes the threshold depend only on the tests.
# Usage: test-filesystem.sh <size, e.g. 11G> <mount point>
set -euo pipefail
size=$1; mnt=$2
img=/var/tmp/test-output.img
# The file is sparse, so the runner's disk must really hold what the tests write.
need_gb=${size%G}
free_gb=$(df -BG --output=avail /var/tmp | tail -1 | tr -dc 0-9)
if [ "$free_gb" -lt "$need_gb" ]; then
  echo "::error::This runner has ${free_gb} GB free on /var/tmp; the tests need a ${size} filesystem"
  exit 1
fi
sudo truncate -s "$size" "$img"
sudo mkfs.ext4 -q -m 0 -F "$img"
sudo mkdir -p "$mnt"
sudo mount -o loop "$img" "$mnt"
sudo chown "$(id -u):$(id -g)" "$mnt"
df -h "$mnt"
