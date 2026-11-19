#!/usr/bin/env bash
# CI and local: test push-images.sh against a throwaway registry (registry:2 on this machine)
# with tiny stand-in images, nothing real. Cases: a first publish; the same again (left
# alone, nothing pushed); other images under an existing tag (refused, tag unchanged); an
# index that would not name one linux/arm64 and one linux/amd64 image (refused before anything is put
# under the tag); a push that fails for the second image (neither image under the tag),
# then the rerun that completes; an existing tag that is not an index (refused).
# Needs docker with buildx, curl and jq. Usage: test-push-images.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
real_docker=$(command -v docker)
cid=$(docker run -d --rm -p 127.0.0.1::5000 registry:2)
cleanup() {
  docker rm -f "$cid" > /dev/null 2>&1 || true
  docker image rm -f peat-storage:ci-arm64 peat-storage:ci-amd64 peat-compute:ci-arm64 peat-compute:ci-amd64 > /dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
port=$(docker port "$cid" 5000/tcp | head -1 | sed 's/.*://')
reg=localhost:$port/zabaca/peat
for _ in $(seq 1 30); do curl -sf "http://localhost:$port/v2/" > /dev/null && break; sleep 1; done
curl -sf "http://localhost:$port/v2/" > /dev/null

printf 'FROM busybox:1.37\nARG N\nLABEL n=$N\n' > "$work/Dockerfile"
# build_images <label> [amd64 image's platform]: the four local images, each its own content.
build_images() {
  for img in peat-storage peat-compute; do
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
if [ "\${1:-}" = push ] && [[ "\${2:-}" == *compute-pg18:*-amd64 ]]; then echo "simulated push failure" >&2; exit 1; fi
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
echo "all cases passed"
