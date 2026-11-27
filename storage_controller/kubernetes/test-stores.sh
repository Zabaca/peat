#!/usr/bin/env bash
# Run a command with both of the storage controller's record stores available for its tests
# (storage_controller/src/persistence/tests.rs): a Kubernetes API server with the record kinds
# applied, and a Postgres server. Both are temporary and stopped when the command ends.
#
# Usage: test-stores.sh <command> [args...]
#
#   KUBE_ASSETS  a directory holding etcd, kube-apiserver and kubectl (the envtest binaries, as
#                `setup-envtest use 1.31.x -p path` prints)
#   PG_BIN       a directory holding initdb, pg_ctl and postgres (default: the first on PATH)
#
# The command runs with STORCON_TEST_KUBECONFIG, STORCON_TEST_DATABASE_URL and
# STORCON_TEST_STORES_REQUIRED set.
set -euo pipefail

: "${KUBE_ASSETS:?KUBE_ASSETS must name a directory with etcd, kube-apiserver and kubectl}"
here=$(cd "$(dirname "$0")" && pwd)
if [ -n "${PG_BIN:-}" ]; then
  export PATH="$PG_BIN:$PATH"
fi

work=$(mktemp -d)
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  pg_ctl -D "$work/pg" -m immediate stop >/dev/null 2>&1 || true
  wait 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

# Ports from the environment, or picked at random so two runs on one machine do not meet.
base=$((20000 + RANDOM % 20000))
etcd_port=${STORCON_TEST_ETCD_PORT:-$base}
api_port=${STORCON_TEST_API_PORT:-$((base + 20))}
pg_port=${STORCON_TEST_PG_PORT:-$((base + 21))}

openssl genrsa -out "$work/sa.key" 2048 2>/dev/null
openssl rsa -in "$work/sa.key" -pubout -out "$work/sa.pub" 2>/dev/null
token=$(openssl rand -hex 16)
echo "$token,admin,admin,system:masters" > "$work/tokens.csv"

"$KUBE_ASSETS/etcd" --data-dir "$work/etcd" \
  --listen-client-urls "http://127.0.0.1:$etcd_port" --advertise-client-urls "http://127.0.0.1:$etcd_port" \
  --listen-peer-urls "http://127.0.0.1:$((etcd_port + 10))" > "$work/etcd.log" 2>&1 &
pids+=($!)
"$KUBE_ASSETS/kube-apiserver" --etcd-servers "http://127.0.0.1:$etcd_port" \
  --secure-port "$api_port" --bind-address 127.0.0.1 --cert-dir "$work/certs" \
  --token-auth-file "$work/tokens.csv" --authorization-mode RBAC \
  --service-account-key-file "$work/sa.pub" --service-account-signing-key-file "$work/sa.key" \
  --service-account-issuer https://kubernetes.default.svc --service-cluster-ip-range 10.0.0.0/24 \
  > "$work/apiserver.log" 2>&1 &
pids+=($!)

cat > "$work/kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: test
  cluster: {server: "https://127.0.0.1:$api_port", insecure-skip-tls-verify: true}
users:
- name: admin
  user: {token: $token}
contexts:
- name: test
  context: {cluster: test, user: admin, namespace: default}
current-context: test
EOF
kubectl=("$KUBE_ASSETS/kubectl" --kubeconfig "$work/kubeconfig")

ready=
for _ in $(seq 1 60); do
  if "${kubectl[@]}" get --raw /readyz >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
if [ -z "$ready" ]; then
  echo "the API server did not become ready; its log:" >&2
  tail -50 "$work/apiserver.log" >&2
  exit 1
fi
"${kubectl[@]}" apply -f "$here/crds.yaml" >/dev/null
"${kubectl[@]}" wait --for condition=established --timeout=60s -f "$here/crds.yaml" >/dev/null

initdb -D "$work/pg" -U postgres --auth trust >/dev/null
pg_ctl -D "$work/pg" -o "-p $pg_port -k $work -c listen_addresses=127.0.0.1 -c max_connections=500" \
  -l "$work/pg.log" -w start >/dev/null

export STORCON_TEST_KUBECONFIG="$work/kubeconfig"
export STORCON_TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:$pg_port/postgres"
export STORCON_TEST_STORES_REQUIRED=1
"$@"
