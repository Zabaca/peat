#!/bin/bash
# CI: the newest commit of main's first-parent history, from <commit> back, whose images are published (`newest=`), and the published one
# before it (`before=`), as GitHub step outputs. The install test runs on the first and installs the second first, to upgrade from it.
set -euo pipefail
found=()
git fetch --no-tags origin +refs/heads/main:refs/remotes/origin/main >/dev/null 2>&1 || true
for sha in $(git rev-list --first-parent -n 40 "${1:?commit}"); do
  # every image the test runs: the storage and both computes
  if docker manifest inspect "ghcr.io/zabaca/peat/storage:$sha" > /dev/null 2>&1 && docker manifest inspect "ghcr.io/zabaca/peat/compute-pg18:$sha" > /dev/null 2>&1 && docker manifest inspect "ghcr.io/zabaca/peat/compute-pg19:$sha" > /dev/null 2>&1; then found+=("$sha"); [ "${#found[@]}" = 2 ] && break; fi
done
[ "${#found[@]}" -ge 1 ] || { echo "::error::no commit of the last 40 has published images" >&2; exit 1; }
echo "newest=${found[0]}"
echo "before=${found[1]:-}"
echo "published: ${found[*]}" >&2
