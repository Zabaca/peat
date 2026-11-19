#!/usr/bin/env bash
# CI (the publish job): push the two checked images of each architecture and join them into
# one multi-arch image per name and tag. For each of storage and compute-pg18: both
# architectures' images are pushed under <tag>-arm64 and <tag>-amd64, then one image index
# is made from the pushed digests under <tag> itself, so the tag a user pulls is the index
# and a pull picks the machine's architecture. If a push fails nothing is put under <tag>.
# The index is read back and must name exactly the two digests pushed, one per architecture.
# Expects the checked images loaded as peat-storage:ci-<arch> and peat-compute:ci-<arch>
# (the publish job compares their IDs with the ones that were checked) and a registry login.
# Usage: push-images.sh <registry path, e.g. ghcr.io/zabaca/peat> <tag>
set -euo pipefail
dest=$1; tag=$2
arches=(arm64 amd64)
summary=${GITHUB_STEP_SUMMARY:-/dev/null}
{
  echo "### Published"
  echo
  echo "| Image | Tag | Index digest | arm64 digest | amd64 digest |"
  echo "|---|---|---|---|---|"
} >> "$summary"
for pair in storage:peat-storage compute-pg18:peat-compute; do
  name=${pair%%:*}; local_name=${pair##*:}
  declare -A digest=()
  refs=()
  for arch in "${arches[@]}"; do
    docker tag "$local_name:ci-$arch" "$dest/$name:$tag-$arch"
    # The digest the registry gave this push, from the push's own output.
    out=$(docker push "$dest/$name:$tag-$arch")
    echo "$out"
    d=$(echo "$out" | sed -n 's/.*digest: \(sha256:[0-9a-f]\{64\}\).*/\1/p' | tail -1)
    [ -n "$d" ] || { echo "::error::no digest in the push output for $name ($arch)"; exit 1; }
    digest[$arch]=$d
    refs+=("$dest/$name@$d")
  done
  docker buildx imagetools create --tag "$dest/$name:$tag" "${refs[@]}"
  index=$(docker buildx imagetools inspect --format '{{json .Manifest}}' "$dest/$name:$tag" | jq -r .digest)
  raw=$(docker buildx imagetools inspect --raw "$dest/$name:$tag")
  [ "$(echo "$raw" | jq '.manifests | length')" = 2 ] || { echo "::error::$name:$tag does not hold exactly two images"; echo "$raw"; exit 1; }
  for arch in "${arches[@]}"; do
    got=$(echo "$raw" | jq -r --arg a "$arch" '[.manifests[] | select(.platform.os == "linux" and .platform.architecture == $a) | .digest] | join(",")')
    [ "$got" = "${digest[$arch]}" ] || { echo "::error::$name:$tag has $arch image '$got', pushed ${digest[$arch]}"; exit 1; }
  done
  echo "$dest/$name:$tag is $index: arm64 ${digest[arm64]}, amd64 ${digest[amd64]}"
  echo "| $dest/$name | $tag | $index | ${digest[arm64]} | ${digest[amd64]} |" >> "$summary"
done
