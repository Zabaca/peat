#!/usr/bin/env bash
# CI (GitHub-hosted ubuntu-24.04 runners): the packages Neon's build and test suite need,
# cargo-nextest at a fixed version, and the Rust toolchain rust-toolchain.toml names.
set -euo pipefail
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -yq --no-install-recommends \
  build-essential libtool libreadline-dev zlib1g-dev flex bison libseccomp-dev \
  libssl-dev clang pkg-config libpq-dev cmake postgresql-client protobuf-compiler \
  libprotobuf-dev libcurl4-openssl-dev openssl lsof libicu-dev jq zstd mold \
  python3-venv python3-dev libffi-dev gettext-base
if ! command -v rustup >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal
  echo "$HOME/.cargo/bin" >> "$GITHUB_PATH"
  export PATH="$HOME/.cargo/bin:$PATH"
fi
rustup show active-toolchain || rustup toolchain install
NEXTEST_VERSION=0.9.152
arch=$(uname -m); case "$arch" in aarch64) nt=linux-arm ;; x86_64) nt=linux ;; *) echo "unsupported $arch"; exit 1 ;; esac
curl -LsSf "https://get.nexte.st/${NEXTEST_VERSION}/${nt}" | tar zxf - -C "$HOME/.cargo/bin"
cargo nextest --version
