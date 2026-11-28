#!/usr/bin/env bash
# CI (the publish job): a release's image map and its install file, from the index digests push-images.sh wrote to its PEAT_DIGESTS file
# (a line "<name> <digest>" per image). A tag can be moved on the registry; a digest names one image, so what the release's install file
# runs is exactly what was checked and pushed. Writes, into <out dir>:
#   images.json  {release, registry, storage, control, compute: {18, 19}}, each image "<registry>/<name>:<tag>@sha256:<digest>"
#   peat.yaml    the install file made from it (control/src/install/install.ts --images): the operator runs the control image by digest
#                and is handed the map, so a Platform of this release runs every piece it does not pin by digest
# Fails unless each of the four images has exactly one digest. Tested by test-push-images.sh.
# Usage: image-map.sh <registry path, e.g. ghcr.io/zabaca/peat> <tag> <digests file> <out dir>
set -euo pipefail
dest=${1:?registry path}; tag=${2:?tag}; digests=${3:?digests file}; out=${4:?out dir}
root=$(cd "$(dirname "$0")/../.." && pwd)
fail() { echo "::error::$*" >&2; exit 1; }  # stderr: a failure inside $(ref ...) would be captured otherwise
ref() {
  local got
  got=$(awk -v n="$1" '$1 == n { print $2 }' "$digests" | sort -u)
  [[ $got =~ ^sha256:[0-9a-f]{64}$ ]] || fail "no single digest for $1 in $digests ('$(echo "$got" | tr '\n' ' ')')"
  echo "$dest/$1:$tag@$got"
}
storage=$(ref storage); control=$(ref control); c18=$(ref compute-pg18); c19=$(ref compute-pg19)
out=$(mkdir -p "$out" && cd "$out" && pwd)
jq -n --arg release "$tag" --arg registry "$dest" --arg storage "$storage" --arg control "$control" --arg c18 "$c18" --arg c19 "$c19" \
  '{release: $release, registry: $registry, storage: $storage, control: $control, compute: {"18": $c18, "19": $c19}}' > "$out/images.json"
(cd "$root/control" && bun src/install/install.ts --images "$out/images.json" --out "$out/peat.yaml")
echo "image map of $tag:"
cat "$out/images.json"
