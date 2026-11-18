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
# Every download is checked against a SHA-256 written here (rustup-init's from
# static.rust-lang.org's .sha256 files, nextest's from GitHub's release asset digests),
# so a changed file fails the job instead of running. rustup then verifies the toolchain
# it installs; apt verifies its packages' signatures.
RUSTUP_VERSION=1.29.3
NEXTEST_VERSION=0.9.152
case "$(uname -m)" in
  aarch64)
    triple=aarch64-unknown-linux-gnu
    rustup_sha256=9f608c142e0e38a78957d0661ba1dfaf0b6fb37184cfaa4b9a0aa57af6184282
    nextest_sha256=ca1f81a5b86533eb45ab97e0064aa88e925e53aab9a698ca6331a7cad1f85f07 ;;
  x86_64)
    triple=x86_64-unknown-linux-gnu
    rustup_sha256=b1c8cb14e30889e7e2999e6b69e62d9e9b9ffc0f4df8180564d306d9680e6c55
    nextest_sha256=d039b43c3e0958441b6ad0a39c8884d018b3d2e8fb9928f209074b1100b5e17a ;;
  *) echo "unsupported $(uname -m)"; exit 1 ;;
esac
dl=$(mktemp -d)
if ! command -v rustup >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSfL -o "$dl/rustup-init" \
    "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${triple}/rustup-init"
  echo "$rustup_sha256  $dl/rustup-init" | sha256sum -c -
  chmod +x "$dl/rustup-init"
  "$dl/rustup-init" -y --default-toolchain none --profile minimal
  echo "$HOME/.cargo/bin" >> "$GITHUB_PATH"
  export PATH="$HOME/.cargo/bin:$PATH"
fi
rustup show active-toolchain || rustup toolchain install
curl --proto '=https' --tlsv1.2 -sSfL -o "$dl/nextest.tar.gz" \
  "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${NEXTEST_VERSION}/cargo-nextest-${NEXTEST_VERSION}-${triple}.tar.gz"
echo "$nextest_sha256  $dl/nextest.tar.gz" | sha256sum -c -
tar zxf "$dl/nextest.tar.gz" -C "$HOME/.cargo/bin"
rm -rf "$dl"
cargo nextest --version
