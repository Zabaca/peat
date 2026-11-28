#!/usr/bin/env bash
# CI and local: test push-images.sh against a throwaway registry (registry:2 on this machine)
# with tiny stand-in images, nothing real. Cases: a first publish; the same again (left
# alone, nothing pushed); other images under an existing tag (refused, tag unchanged); an
# index that would not name one linux/arm64 and one linux/amd64 image (refused before anything is put
# under the tag); a push that fails for the second image (neither image under the tag),
# then the rerun that completes; an existing tag that is not an index (refused); storage's
# index made and compute's not, then the rerun that finishes compute; the digests the publish job keeps and the image map and install
# file image-map.sh makes from them (each image by its index digest, read back), a rerun's map the same, a digest missing or doubled refused.
# Needs docker with buildx, curl, jq and bun. Usage: test-push-images.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
real_docker=$(command -v docker)
cid=$(docker run -d --rm -p 127.0.0.1::5000 registry:2)
cleanup() {
  docker rm -f "$cid" > /dev/null 2>&1 || true
  docker image rm -f peat-storage:ci-arm64 peat-storage:ci-amd64 peat-compute:ci-arm64 peat-compute:ci-amd64 peat-compute-pg19:ci-arm64 peat-compute-pg19:ci-amd64 peat-control:ci-arm64 peat-control:ci-amd64 > /dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
port=$(docker port "$cid" 5000/tcp | head -1 | sed 's/.*://')
reg=localhost:$port/zabaca/peat
for _ in $(seq 1 30); do curl -sf "http://localhost:$port/v2/" > /dev/null && break; sleep 1; done
curl -sf "http://localhost:$port/v2/" > /dev/null

printf 'FROM busybox:1.37\nARG N\nLABEL n=$N\n' > "$work/Dockerfile"
# build_images <label> [amd64 image's platform]: the eight local images, each its own content.
build_images() {
  for img in peat-storage peat-compute peat-compute-pg19 peat-control; do
    for arch in arm64 amd64; do
      plat=$arch; [ "$arch" = amd64 ] && plat=${2:-amd64}
      docker buildx build -q --platform "linux/$plat" --build-arg "N=$1-$img-$arch" --load -t "$img:ci-$arch" "$work" > /dev/null
    done
  done
}
index_digest() { docker buildx imagetools inspect --format '{{json .Manifest}}' "$reg/$1:$2" | jq -r .digest; }
published() { docker buildx imagetools inspect --raw "$reg/$1:$2" > /dev/null 2>&1; }
run_push() { "$here/push-images.sh" "$reg" "$1" > "$work/out.txt" 2>&1; }
pass() { echo "ok: $*"; }
die() { echo "FAIL: $*"; sed 's/^/    /' "$work/out.txt" 2> /dev/null | tail -30; exit 1; }
expect_fail() {  # <tag> <pattern the output must match>
  if run_push "$1"; then die "push-images.sh succeeded; it should have failed ($2)"; fi
  grep -Eq "$2" "$work/out.txt" || die "failed, but not with '$2'"
}

build_images one
run_push t1 || die "first publish"
for n in storage compute-pg18; do
  [ "$(docker buildx imagetools inspect --raw "$reg/$n:t1" | jq '.manifests | length')" = 2 ] || die "$n:t1 is not two images"
done
grep -Eq "storage:t1 read back: linux/arm64 sha256:[0-9a-f]{64}, linux/amd64 sha256:" "$work/out.txt" || die "the read-back did not list both platforms"
s1=$(index_digest storage t1); c1=$(index_digest compute-pg18 t1)
pass "first publish: both indexes hold two images"

run_push t1 || die "rerun of the same images"
grep -q "already published" "$work/out.txt" || die "the rerun did not say it left the tag"
! grep -q "The push refers" "$work/out.txt" || die "the rerun pushed"
[ "$(index_digest storage t1)" = "$s1" ] && [ "$(index_digest compute-pg18 t1)" = "$c1" ] || die "the rerun changed an index"
pass "the same images again: left alone, nothing pushed"

build_images two
expect_fail t1 "not overwriting"
[ "$(index_digest storage t1)" = "$s1" ] && [ "$(index_digest compute-pg18 t1)" = "$c1" ] || die "a refused push changed an index"
pass "other images under an existing tag: refused, tag unchanged"

build_images three arm64   # the "amd64" images are arm64 ones: the index would name arm64 twice
expect_fail t2 "::error::storage:t2 (would hold|has linux/)"
! published storage t2 && ! published compute-pg18 t2 || die "an index that failed its check was published"
pass "an index without linux/amd64: refused before anything is put under the tag"

build_images four
mkdir "$work/shim"
cat > "$work/shim/docker" <<SHIM
#!/usr/bin/env bash
if [ "\${1:-}" = push ] && [[ "\${2:-}" == *compute-pg18:t3-amd64 ]]; then echo "simulated push failure" >&2; exit 1; fi
if [ "\${1:-} \${2:-} \${3:-}" = "buildx imagetools create" ] && [[ "\$*" == *compute-pg18:t5* && "\$*" != *--dry-run* ]]; then echo "simulated index failure" >&2; exit 1; fi
exec "$real_docker" "\$@"
SHIM
chmod +x "$work/shim/docker"
if PATH="$work/shim:$PATH" run_push t3; then die "a failed push did not fail the job"; fi
grep -q "simulated push failure" "$work/out.txt" || die "the push did not fail where it was made to"
! published storage t3 && ! published compute-pg18 t3 || die "a failed second push left an image under the tag"
pass "the second image's push fails: neither image is under the tag"
run_push t3 || die "rerun after the failed push"
published storage t3 && published compute-pg18 t3 || die "the rerun did not publish both"
pass "the rerun completes both"

docker tag peat-storage:ci-arm64 "$reg/storage:t4"
docker push -q "$reg/storage:t4" > /dev/null
expect_fail t4 "not an index"
pass "an existing tag that is not an index: refused"
# Half published: storage's index is made, compute's creation fails.
if PATH="$work/shim:$PATH" run_push t5; then die "a failed index creation did not fail the job"; fi
grep -q "simulated index failure" "$work/out.txt" || die "the index creation did not fail where it was made to"
published storage t5 && ! published compute-pg18 t5 || die "expected storage published and compute not"
s5=$(index_digest storage t5)
run_push t5 || die "rerun of a half-published tag"
grep -q "storage:t5 is already published" "$work/out.txt" || die "the rerun did not leave storage's index"
published compute-pg18 t5 && [ "$(index_digest storage t5)" = "$s5" ] || die "the rerun did not finish compute, or changed storage"
pass "storage's index made and compute's not: the rerun leaves storage and completes compute"
# A second call with other names (the publish job's call for the 19 compute image): only
# those names are published, and storage and compute-pg18 under the same tag are not touched.
build_images six
if ! PEAT_IMAGE_NAMES=compute-pg19 run_push t6; then die "publish of compute-pg19 alone"; fi
[ "$(docker buildx imagetools inspect --raw "$reg/compute-pg19:t6" | jq '.manifests | length')" = 2 ] || die "compute-pg19:t6 is not two images"
! published storage t6 && ! published compute-pg18 t6 || die "a call for compute-pg19 published other images"
pass "compute-pg19 on its own: two images under the tag, nothing else published"
# The publish job's call for the control image: the same, for control.
if ! PEAT_IMAGE_NAMES=control run_push t6; then die "publish of control alone"; fi
[ "$(docker buildx imagetools inspect --raw "$reg/control:t6" | jq '.manifests | length')" = 2 ] || die "control:t6 is not two images"
! published storage t6 && ! published compute-pg18 t6 || die "a call for control published other images"
pass "control on its own: two images under the tag, nothing else published"
# The publish job's three calls with PEAT_DIGESTS, then image-map.sh: the release's image map and install file name each image by the
# digest of the index under the tag, and each reference reads back from the registry.
build_images seven
digests=$work/digests.txt
PEAT_DIGESTS=$digests run_push t7 || die "publish of t7"
PEAT_DIGESTS=$digests PEAT_IMAGE_NAMES=compute-pg19 run_push t7 || die "publish of compute-pg19 t7"
PEAT_DIGESTS=$digests PEAT_IMAGE_NAMES=control run_push t7 || die "publish of control t7"
for n in storage compute-pg18 compute-pg19 control; do
  [ "$(awk -v n="$n" '$1 == n { print $2 }' "$digests")" = "$(index_digest "$n" t7)" ] || die "the digests file's $n is not the index under t7: $(cat "$digests")"
done
"$here/image-map.sh" "$reg" t7 "$digests" "$work/release" > "$work/out.txt" 2>&1 || die "image-map.sh"
map=$work/release/images.json
[ "$(jq -r .release "$map")" = t7 ] && [ "$(jq -r .registry "$map")" = "$reg" ] || die "the image map's release or registry: $(cat "$map")"
for f in storage control 'compute."18"' 'compute."19"'; do
  ref=$(jq -r ".$f" "$map")
  n=$(case $f in storage) echo storage;; control) echo control;; *18*) echo compute-pg18;; *) echo compute-pg19;; esac)
  [ "$ref" = "$reg/$n:t7@$(index_digest "$n" t7)" ] || die "the image map's $f is $ref, not $n by its index digest"
  [ "$(docker buildx imagetools inspect --raw "$ref" | jq '.manifests | length')" = 2 ] || die "$ref does not read back as the index"
done
grep -q "image: $(jq -r .control "$map")" "$work/release/peat.yaml" || die "the install file does not run the control image by digest"
grep -q "name: PEAT_RELEASE_IMAGES" "$work/release/peat.yaml" || die "the install file does not hand the operator the image map"
pass "the image map and the install file: every image by the digest under the tag, each read back"
# A rerun (every tag already published) writes the same digests again; a map is still made, and the same.
cp "$map" "$work/first.json"
PEAT_DIGESTS=$digests run_push t7 || die "rerun of t7"
"$here/image-map.sh" "$reg" t7 "$digests" "$work/release" > "$work/out.txt" 2>&1 || die "image-map.sh after the rerun"
cmp -s "$map" "$work/first.json" || die "the rerun's image map differs"
pass "a rerun: the same digests, the same image map"
# A digest missing, or two for one name, is no map.
grep -v '^control ' "$digests" > "$work/missing.txt"
if "$here/image-map.sh" "$reg" t7 "$work/missing.txt" "$work/bad" > "$work/out.txt" 2>&1; then die "image-map.sh made a map without control's digest"; fi
grep -q "no single digest for control" "$work/out.txt" || die "a missing digest failed, but not for its reason"
{ cat "$digests"; echo "storage sha256:$(printf '0%.0s' $(seq 64))"; } > "$work/two.txt"
if "$here/image-map.sh" "$reg" t7 "$work/two.txt" "$work/bad" > "$work/out.txt" 2>&1; then die "image-map.sh made a map from two storage digests"; fi
grep -q "no single digest for storage" "$work/out.txt" || die "two digests failed, but not for their reason"
pass "a digest missing, or two for one image: no map"
echo "all cases passed"
