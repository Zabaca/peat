#!/bin/bash
# CI: what the install test needs beside Docker and Bun: kind (a release binary, checked against its published digest) and psql.
set -euo pipefail
version=v0.33.0
case "$(uname -m)" in
  x86_64) arch=amd64; sum=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d ;;
  aarch64) arch=arm64; sum=20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2 ;;
  *) echo "::error::no kind for $(uname -m)"; exit 1 ;;
esac
curl -fsSLo /tmp/kind "https://github.com/kubernetes-sigs/kind/releases/download/$version/kind-linux-$arch"
echo "$sum  /tmp/kind" | sha256sum -c -
sudo install -m 0755 /tmp/kind /usr/local/bin/kind
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends postgresql-client
kind version; psql --version; kubectl version --client
