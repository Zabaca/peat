#!/usr/bin/env bash
# An etcd restore under a running storage controller, done for real: the controller on
# Kubernetes records with a copy in a local bucket, a pageserver registered, a snapshot of etcd,
# the pageserver's scheduling policy changed (a later revision), then etcd restored from the
# snapshot with the API server restarted on it, as an operator would. The controller must find
# that its records went back, write the went-back record to the bucket and exit, and a controller
# started again must refuse to start. Done twice: restored as is, and with --bump-revision
# --mark-compacted (the resource versions then go forward, not back).
#
# Usage: test-etcd-restore.sh <storage_controller binary>
#
#   KUBE_ASSETS  etcd, kube-apiserver and kubectl (the envtest binaries)
#   ETCD_TOOLS   etcdctl and etcdutl of the same etcd release
set -euo pipefail

controller=$1
: "${KUBE_ASSETS:?KUBE_ASSETS must name a directory with etcd, kube-apiserver and kubectl}"
: "${ETCD_TOOLS:?ETCD_TOOLS must name a directory with etcdctl and etcdutl}"
here=$(cd "$(dirname "$0")" && pwd)
unset HTTPS_PROXY HTTP_PROXY https_proxy http_proxy ALL_PROXY all_proxy

etcd_port=${STORCON_TEST_ETCD_PORT:-23791}
api_port=${STORCON_TEST_API_PORT:-26444}
listen_port=${STORCON_TEST_CONTROLLER_PORT:-19236}

run_case() {
  local bump=$1
  local work
  work=$(mktemp -d)
  local etcd_pid= api_pid= ctl_pid=
  cleanup() {
    for pid in $ctl_pid $api_pid $etcd_pid; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    rm -rf "$work"
  }
  trap cleanup RETURN

  openssl genrsa -out "$work/sa.key" 2048 2>/dev/null
  openssl rsa -in "$work/sa.key" -pubout -out "$work/sa.pub" 2>/dev/null
  local token
  token=$(openssl rand -hex 16)
  echo "$token,admin,admin,system:masters" > "$work/tokens.csv"

  start_etcd() {
    "$KUBE_ASSETS/etcd" --data-dir "$work/etcd" \
      --listen-client-urls "http://127.0.0.1:$etcd_port" --advertise-client-urls "http://127.0.0.1:$etcd_port" \
      --listen-peer-urls "http://127.0.0.1:$((etcd_port + 10))" >> "$work/etcd.log" 2>&1 &
    etcd_pid=$!
  }
  start_api() {
    "$KUBE_ASSETS/kube-apiserver" --etcd-servers "http://127.0.0.1:$etcd_port" \
      --secure-port "$api_port" --bind-address 127.0.0.1 --cert-dir "$work/certs" \
      --token-auth-file "$work/tokens.csv" --authorization-mode RBAC \
      --service-account-key-file "$work/sa.pub" --service-account-signing-key-file "$work/sa.key" \
      --service-account-issuer https://kubernetes.default.svc --service-cluster-ip-range 10.0.0.0/24 \
      >> "$work/apiserver.log" 2>&1 &
    api_pid=$!
  }
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
  context: {cluster: test, user: admin, namespace: records}
current-context: test
EOF
  local kubectl=("$KUBE_ASSETS/kubectl" --kubeconfig "$work/kubeconfig")
  wait_api() {
    for _ in $(seq 1 60); do
      if "${kubectl[@]}" get --raw /readyz >/dev/null 2>&1; then return 0; fi
      sleep 1
    done
    echo "the API server did not become ready" >&2
    tail -30 "$work/apiserver.log" >&2
    return 1
  }
  start_etcd
  start_api
  wait_api
  "${kubectl[@]}" apply -f "$here/crds.yaml" >/dev/null
  "${kubectl[@]}" wait --for condition=established --timeout=60s -f "$here/crds.yaml" >/dev/null
  "${kubectl[@]}" create namespace records >/dev/null
  mkdir -p "$work/bucket"

  start_controller() {
    KUBECONFIG="$work/kubeconfig" "$controller" -l "127.0.0.1:$listen_port" --dev \
      --address-for-peers "${1:-http://127.0.0.1:$listen_port}" \
      --records kubernetes --records-namespace records \
      --records-copy-storage "local_path = '$work/bucket'" >> "$work/controller.log" 2>&1 &
    ctl_pid=$!
  }
  # Wait for the copy in the bucket to hold $1
  copy_has() {
    for _ in $(seq 1 20); do
      grep -q "$1" "$work/bucket/records.json" 2>/dev/null && return 0
      sleep 0.5
    done
    echo "FAIL ($bump bump): the copy never held $1" >&2
    tail -20 "$work/controller.log" >&2
    return 1
  }
  wait_status() {
    for _ in $(seq 1 30); do
      curl -fsS "http://127.0.0.1:$listen_port/status" >/dev/null 2>&1 && break
      sleep 1
    done
  }
  # A first controller, then gone (a pod lost with its address): the Leader record still names it
  # when the next one starts, which writes the copy once it becomes leader
  start_controller "http://controller-0.invalid:1234"
  wait_status
  copy_has 'controller-0.invalid' || return 1
  kill "$ctl_pid"; wait "$ctl_pid" 2>/dev/null || true
  start_controller
  wait_status
  curl -fsS -X POST "http://127.0.0.1:$listen_port/control/v1/node" -H 'content-type: application/json' \
    -d '{"node_id":1,"listen_pg_addr":"localhost","listen_pg_port":6400,"listen_http_addr":"localhost","listen_http_port":9898,"availability_zone_id":"zone-a"}' >/dev/null
  sleep 2
  "$ETCD_TOOLS/etcdctl" --endpoints "http://127.0.0.1:$etcd_port" snapshot save "$work/snapshot.db" >/dev/null 2>&1
  curl -fsS -X PUT "http://127.0.0.1:$listen_port/control/v1/node/1/config" -H 'content-type: application/json' \
    -d '{"node_id":1,"scheduling":"Pause"}' >/dev/null
  copy_has '"schedulingPolicy":"pause"' || return 1

  # The restore, the controller still running
  kill "$api_pid" "$etcd_pid"; wait "$api_pid" "$etcd_pid" 2>/dev/null || true
  rm -rf "$work/etcd"
  if [ "$bump" = yes ]; then
    "$ETCD_TOOLS/etcdutl" snapshot restore "$work/snapshot.db" --data-dir "$work/etcd" \
      --bump-revision 1000000 --mark-compacted >/dev/null 2>&1
  else
    "$ETCD_TOOLS/etcdutl" snapshot restore "$work/snapshot.db" --data-dir "$work/etcd" >/dev/null 2>&1
  fi
  start_etcd
  start_api
  wait_api

  # The controller sees it, records it, and exits
  local exited=
  for _ in $(seq 1 60); do
    if ! kill -0 "$ctl_pid" 2>/dev/null; then exited=1; break; fi
    sleep 1
  done
  if [ -z "$exited" ]; then
    echo "FAIL ($bump bump): the controller did not exit after the restore" >&2
    tail -20 "$work/controller.log" >&2
    return 1
  fi
  local status=0
  wait "$ctl_pid" || status=$?
  ctl_pid=
  if [ "$status" = 0 ] || [ ! -f "$work/bucket/went-back.json" ]; then
    echo "FAIL ($bump bump): exit $status, went-back record: $(ls "$work/bucket")" >&2
    tail -20 "$work/controller.log" >&2
    return 1
  fi
  grep -m1 "records went back under this controller" "$work/controller.log" | cut -c1-240

  # A controller started again refuses
  start_controller
  for _ in $(seq 1 30); do
    if ! kill -0 "$ctl_pid" 2>/dev/null; then break; fi
    sleep 1
  done
  if kill -0 "$ctl_pid" 2>/dev/null; then
    echo "FAIL ($bump bump): a restarted controller did not refuse" >&2
    return 1
  fi
  wait "$ctl_pid" || true
  ctl_pid=
  grep -m1 "found the records went back" "$work/controller.log" | cut -c1-240
  echo "PASS ($bump bump): the controller found the restore, recorded it and exited; the next start refused"
}

run_case no
run_case yes
