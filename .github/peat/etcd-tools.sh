#!/usr/bin/env bash
# CI: fetch etcdctl and etcdutl of etcd 3.5.15 (the release the envtest build of Kubernetes
# 1.31.0 runs), for storage_controller/kubernetes/test-etcd-restore.sh, which saves a snapshot of
# etcd and restores it under a running storage controller; checked against the SHA-256 the
# release publishes (SHA256SUMS of etcd-io/etcd's v3.5.15).
# Usage: etcd-tools.sh <directory>
set -euo pipefail
dir=$1
case "$(uname -m)" in
  aarch64|arm64)
    arch=arm64
    sha256=5aa28b435edb1f22bf6455f754e16a13e3e4eb1ad8fc7c22ad47aa8e722febf2
    ;;
  x86_64)
    arch=amd64
    sha256=3f6b48d8c2844699f2b19c1880508ecf63e1489769ed37ebb97495d5cd848a89
    ;;
  *) echo "no etcd build for $(uname -m)" >&2; exit 1 ;;
esac
archive=$(mktemp)
curl -fsSL --retry 3 -o "$archive" \
  "https://github.com/etcd-io/etcd/releases/download/v3.5.15/etcd-v3.5.15-linux-$arch.tar.gz"
echo "$sha256  $archive" | sha256sum -c -
mkdir -p "$dir"
tar -xzf "$archive" -C "$dir" --strip-components=1 "etcd-v3.5.15-linux-$arch/etcdctl" "etcd-v3.5.15-linux-$arch/etcdutl"
rm -f "$archive"
ls -l "$dir"
