#!/usr/bin/env bash
# CI: a standard GitHub-hosted runner has about 14 GB of free disk; Neon's release build
# needs more. Remove preinstalled toolchains this repository does not use.
set -uo pipefail
df -h /
for d in /usr/local/lib/android /usr/share/dotnet /opt/ghc /usr/local/.ghcup /usr/share/swift \
         /usr/local/share/boost /opt/hostedtoolcache/CodeQL /opt/hostedtoolcache/go \
         /opt/hostedtoolcache/PyPy /opt/hostedtoolcache/Ruby /usr/local/share/powershell \
         /usr/local/julia* /opt/microsoft /opt/google /usr/lib/jvm; do
  if [ -e "$d" ]; then sudo rm -rf "$d"; fi
done
docker image prune --all --force >/dev/null 2>&1 || true
df -h /
