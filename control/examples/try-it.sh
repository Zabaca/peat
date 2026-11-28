#!/bin/bash
# Try Peat on your own machine: a throwaway Kubernetes cluster (kind) with a MinIO as the store, Peat installed by its operator, one
# one-machine Platform (examples/trial.yaml), a database made, written, read with psql, put to sleep and woken by a connection.
#
#   control/examples/try-it.sh            # from a checkout of this repository
#
# Needs docker (running), kind, kubectl, bun and psql on the PATH, and a network that reaches ghcr.io and docker.io. See the README's
# Quickstart for the machine it needs and how long it takes.
# Everything it makes is in one kind cluster, named peat-try, and one kubeconfig file of its own (your own kubeconfig is never touched).
# Remove it all with:   kind delete cluster --name peat-try
#
# RELEASE is the Peat release the platform runs: a `release-*` tag, or a commit of main whose images are published. The default is a
# commit; the install file and the Platform's images come from the one release, so check this tree out at it for an exact match.
set -euo pipefail

RELEASE=${RELEASE:-3c9870c1d6f0e3e391c56cd2863837174eeb847c}
CLUSTER=${CLUSTER:-peat-try}
# a kubeconfig of its own: kind writes its context there
export KUBECONFIG=${PEAT_TRY_KUBECONFIG:-${TMPDIR:-/tmp}/peat-try.kubeconfig}
node_image='kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d'
minio_image='docker.io/bitnamilegacy/minio:2025.7.23-debian-12-r5@sha256:6dabb4a2088c9a79908de3bc05f4586c23ad2182c8908e7e3acbf61c1467fb20'
here=$(cd "$(dirname "$0")" && pwd)
control=$(cd "$here/.." && pwd)
started=$(date +%s)
k() { kubectl --context "kind-$CLUSTER" "$@"; }
peat() { bun "$control/src/cli/peat.ts" --context "kind-$CLUSTER" "$@"; }
say() { echo "== $* ($(( $(date +%s) - started )) s)"; }
pf=
trap '[ -z "$pf" ] || kill "$pf" 2>/dev/null || true' EXIT

for tool in docker kind kubectl bun psql; do
  command -v "$tool" >/dev/null || { echo "$tool is not on the PATH (needs docker, kind, kubectl, bun and psql)" >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "docker is not running" >&2; exit 1; }

say "a cluster"
kind create cluster --name "$CLUSTER" --image "$node_image" --wait 180s
k label node "$CLUSTER-control-plane" peat.zabaca.com/machine=true --overwrite

say "a store: MinIO in the cluster, with one bucket"
k create namespace minio
k -n minio apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: { name: minio }
spec:
  selector: { matchLabels: { app: minio } }
  template:
    metadata: { labels: { app: minio } }
    spec:
      containers:
        - name: minio
          image: $minio_image
          env:
            - { name: MINIO_ROOT_USER, value: peat-try }
            - { name: MINIO_ROOT_PASSWORD, value: peat-try-secret }
            - { name: MINIO_DEFAULT_BUCKETS, value: peat }
          ports: [ { containerPort: 9000 } ]
          readinessProbe: { httpGet: { path: /minio/health/ready, port: 9000 } }
---
apiVersion: v1
kind: Service
metadata: { name: minio }
spec: { selector: { app: minio }, ports: [ { port: 9000 } ] }
EOF
k -n minio rollout status deployment/minio --timeout=300s

say "Peat's install file (the operator, the object kinds) from release $RELEASE"
bun "$control/src/install/install.ts" --image "ghcr.io/zabaca/peat/control:$RELEASE" | k apply --server-side -f -
k -n peat-system rollout status deployment/peat-operator --timeout=300s

say "the platform: the store's key, then one Platform object"
k create namespace peat
k -n peat create secret generic peat-store --from-literal=AWS_ACCESS_KEY_ID=peat-try --from-literal=AWS_SECRET_ACCESS_KEY=peat-try-secret
sed "s/release-2026-12-01/$RELEASE/" "$here/trial.yaml" | k apply -f -
phase=
for _ in $(seq 1 120); do
  phase=$(k get platform peat -o jsonpath='{.status.phase}' 2>/dev/null || true)
  echo "  ${phase:-waiting}: $(k get platform peat -o jsonpath='{.status.message}' 2>/dev/null | cut -c1-160)"
  [ "$phase" = Ready ] && break
  sleep 10
done
[ "$phase" = Ready ] || { echo "the platform was not Ready in 20 minutes: kubectl --context kind-$CLUSTER get platform peat -o yaml" >&2; exit 1; }

say "a database"
peat make hello
k get database hello
login() { k get secret hello-login -o jsonpath="{.data.$1}" | base64 -d; }
k get configmap peat-ca -o jsonpath='{.data.ca\.crt}' > "${TMPDIR:-/tmp}/peat-try-ca.crt"
pass=$(login password); host=$(login host)

# the front door is a Service in the cluster; a tunnel brings it to this machine. libpq is given the database's own name for the
# certificate and the TLS name the door routes by (host), and the tunnel's address (hostaddr) to dial.
k -n peat-gateway port-forward svc/pg-front 15432:5432 >/dev/null 2>&1 &
pf=$!
sql() { PGPASSWORD="$pass" psql "host=$host hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=${TMPDIR:-/tmp}/peat-try-ca.crt connect_timeout=60" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
until sql 'select 1' >/dev/null 2>&1; do sleep 3; done
sql 'create table t (id int primary key, note text)'
sql "insert into t select g, md5(g::text) from generate_series(1, 100000) g"
echo "  wrote and read back: $(sql 'select count(*) from t') rows, over TLS verified against the platform's own authority"

say "asleep, then woken by a connection"
peat stop hello
k get database hello
t0=$(date +%s)
echo "  a connection wakes it: $(sql 'select count(*) from t') rows, $(( $(date +%s) - t0 )) s"

say "done"
cat <<EOF

Peat is running in the kind cluster '$CLUSTER'. To use it yourself:

  export KUBECONFIG=$KUBECONFIG
  kubectl get platform peat                                   # the platform's status
  kubectl get database hello                                  # the database you were just using
  kubectl -n peat-gateway port-forward svc/pg-front 15432:5432 &
  PGPASSWORD=\$(kubectl get secret hello-login -o jsonpath='{.data.password}' | base64 -d) \\
    psql "host=$host hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=${TMPDIR:-/tmp}/peat-try-ca.crt"

Branch it, restore it to a moment, move it to another Postgres version: control/README.md ("Databases", "The command").
When you are done:  kind delete cluster --name $CLUSTER
EOF
