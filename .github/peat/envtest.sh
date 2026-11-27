#!/usr/bin/env bash
# CI: fetch the Kubernetes API server, etcd and kubectl that the storage controller's record
# tests run against (storage_controller/kubernetes/test-stores.sh), the envtest build of
# Kubernetes 1.31.0 published by kubernetes-sigs/controller-tools, checked against the SHA-512
# its release index gives (envtest-releases.yaml in that repository).
# Usage: envtest.sh <directory>
set -euo pipefail
dir=$1
case "$(uname -m)" in
  aarch64|arm64)
    arch=arm64
    sha512=72f5c8fd615c9db62eeb66e30edfda0f3879bffa3577c5776ec83363c018d7f51c174ac5ea807414072a21f8151a7bdf9826f414543f690686550e49db202ca0
    ;;
  x86_64)
    arch=amd64
    sha512=5d96ae284610863ce5974e030aecd2eaad693f3210103ca778107aa0ea00f6f1d0a7b1b34aa74d7257cb0d7f713c2da365beba89b1d60823ce56c7b84b935423
    ;;
  *) echo "no envtest build for $(uname -m)" >&2; exit 1 ;;
esac
archive=$(mktemp)
curl -fsSL --retry 3 -o "$archive" \
  "https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v1.31.0/envtest-v1.31.0-linux-$arch.tar.gz"
echo "$sha512  $archive" | sha512sum -c -
mkdir -p "$dir"
tar -xzf "$archive" -C "$dir" --strip-components=2
rm -f "$archive"
ls -l "$dir"
