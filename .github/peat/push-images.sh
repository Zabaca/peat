#!/usr/bin/env bash
# CI (the publish job): push the checked images of both architectures and join them into
# one multi-arch image per name and tag, for storage and compute-pg18 (or the names in
# PEAT_IMAGE_NAMES, which the publish job sets to compute-pg19 for a second call). The order is what
# keeps a bad or half release off <tag>, the tag a user pulls:
#   1. Look at every <tag> first. One that exists must already be this very image (an
#      index of exactly linux/arm64 and linux/amd64 whose two images have the configs, so
#      the IDs, of the loaded images): then it is left alone, so a rerun is harmless; any
#      other content under an existing tag fails the job before anything is pushed, so a
#      rebuilt image (new IDs) never overwrites a published commit or release tag.
#   2. Push all four architecture images (<tag>-arm64, <tag>-amd64, for each name).
#   3. Dry-run both indexes and check what each would publish: exactly two manifests,
#      linux/arm64 and linux/amd64, the digests just pushed, nothing else (no attestation
#      manifests, say).
#   4. Only then create the indexes under <tag>, read each back and check it again.
# A failure before step 4 leaves nothing under any <tag> (the <tag>-<arch> tags may stay:
# a rerun overwrites them). Expects the checked images loaded as peat-storage:ci-<arch>
# and peat-compute:ci-<arch> (the publish job compares their IDs with the ones checked)
# and a registry login. Tested by test-push-images.sh.
# Usage: [PEAT_IMAGE_NAMES='storage compute-pg18'] push-images.sh <registry path, e.g. ghcr.io/zabaca/peat> <tag>
set -euo pipefail
dest=$1; tag=$2
arches=(arm64 amd64)
read -ra names <<< "${PEAT_IMAGE_NAMES:-storage compute-pg18}"
local_image() { case $1 in storage) echo peat-storage;; compute-pg18) echo peat-compute;; compute-pg19) echo peat-compute-pg19;; esac; }
summary=${GITHUB_STEP_SUMMARY:-/dev/null}
fail() { echo "::error::$*"; exit 1; }
inspect_raw() { docker buildx imagetools inspect --raw "$1"; }

# check_index <json of an index> <name> <arm64 digest> <amd64 digest>
check_index() {
  local json=$1 name=$2 want_arm64=$3 want_amd64=$4 n got
  n=$(echo "$json" | jq '.manifests | length')
  [ "$n" = 2 ] || { echo "$json"; fail "$name:$tag would hold $n manifests, not exactly two"; }
  got=$(echo "$json" | jq -r '[.manifests[] | select(.platform.os == "linux" and .platform.architecture == "arm64") | .digest] | join(",")')
  [ "$got" = "$want_arm64" ] || { echo "$json"; fail "$name:$tag has linux/arm64 '$got', expected $want_arm64"; }
  got=$(echo "$json" | jq -r '[.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64") | .digest] | join(",")')
  [ "$got" = "$want_amd64" ] || { echo "$json"; fail "$name:$tag has linux/amd64 '$got', expected $want_amd64"; }
}

declare -A done_already=() digest=() index=()
err=$(mktemp)

# 1. What is under each <tag> already.
for name in "${names[@]}"; do
  ref="$dest/$name:$tag"
  if raw=$(inspect_raw "$ref" 2>"$err"); then
    [ "$(echo "$raw" | jq '.manifests | length')" = 2 ] || fail "$ref exists and is not an index of exactly two images; not overwriting it"
    for arch in "${arches[@]}"; do
      d=$(echo "$raw" | jq -r --arg a "$arch" '[.manifests[] | select(.platform.os == "linux" and .platform.architecture == $a) | .digest] | join(",")')
      [[ $d =~ ^sha256:[0-9a-f]{64}$ ]] || fail "$ref exists and has no single linux/$arch image; not overwriting it"
      cfg=$(inspect_raw "$dest/$name@$d" | jq -r .config.digest)
      loaded=$(docker image inspect --format '{{.Id}}' "$(local_image "$name"):ci-$arch")
      [ "$cfg" = "$loaded" ] || fail "$ref exists with a different linux/$arch image ($cfg, this build is $loaded); not overwriting it. A rerun of the whole workflow rebuilds the images: rerun only the failed job."
      digest[$name,$arch]=$d
    done
    index[$name]=$(docker buildx imagetools inspect --format '{{json .Manifest}}' "$ref" | jq -r .digest)
    done_already[$name]=1
    echo "$ref is already published and is this build's image: leaving it"
  elif grep -qi 'not found' "$err"; then
    echo "$ref is not published yet"
  else
    cat "$err"
    fail "could not tell whether $ref exists"
  fi
done

# 2. Every architecture image of what is not published yet.
for name in "${names[@]}"; do
  [ -z "${done_already[$name]:-}" ] || continue
  for arch in "${arches[@]}"; do
    docker tag "$(local_image "$name"):ci-$arch" "$dest/$name:$tag-$arch"
  done
done
for name in "${names[@]}"; do
  [ -z "${done_already[$name]:-}" ] || continue
  for arch in "${arches[@]}"; do
    # The digest the registry gave this push, from the push's own output.
    out=$(docker push "$dest/$name:$tag-$arch")
    echo "$out"
    d=$(echo "$out" | sed -n 's/.*digest: \(sha256:[0-9a-f]\{64\}\).*/\1/p' | tail -1)
    [ -n "$d" ] || fail "no digest in the push output for $name ($arch)"
    digest[$name,$arch]=$d
  done
done

# 3. Both indexes as they would be made, checked before either is.
for name in "${names[@]}"; do
  [ -z "${done_already[$name]:-}" ] || continue
  planned=$(docker buildx imagetools create --dry-run --tag "$dest/$name:$tag" \
    "$dest/$name@${digest[$name,arm64]}" "$dest/$name@${digest[$name,amd64]}")
  check_index "$planned" "$name" "${digest[$name,arm64]}" "${digest[$name,amd64]}"
done

# 4. The indexes, each read back.
for name in "${names[@]}"; do
  [ -z "${done_already[$name]:-}" ] || continue
  docker buildx imagetools create --tag "$dest/$name:$tag" \
    "$dest/$name@${digest[$name,arm64]}" "$dest/$name@${digest[$name,amd64]}"
  readback=$(inspect_raw "$dest/$name:$tag")
  check_index "$readback" "$name" "${digest[$name,arm64]}" "${digest[$name,amd64]}"
  echo "$name:$tag read back: $(echo "$readback" | jq -r '[.manifests[] | "\(.platform.os)/\(.platform.architecture) \(.digest)"] | join(", ")')"
  index[$name]=$(docker buildx imagetools inspect --format '{{json .Manifest}}' "$dest/$name:$tag" | jq -r .digest)
done

for name in "${names[@]}"; do
  note=pushed; [ -z "${done_already[$name]:-}" ] || note="already published, unchanged"
  echo "$dest/$name:$tag is ${index[$name]}: arm64 ${digest[$name,arm64]}, amd64 ${digest[$name,amd64]} ($note)"
done
{
  echo "### Published"
  echo
  echo "| Image | Tag | Index digest | arm64 digest | amd64 digest | |"
  echo "|---|---|---|---|---|---|"
  for name in "${names[@]}"; do
    note=pushed; [ -z "${done_already[$name]:-}" ] || note="already published, unchanged"
    echo "| $dest/$name | $tag | ${index[$name]} | ${digest[$name,arm64]} | ${digest[$name,amd64]} | $note |"
  done
} >> "$summary"
