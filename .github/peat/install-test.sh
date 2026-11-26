#!/bin/bash
# The install test: a whole platform installed from the install file and one Platform object on a throwaway Kubernetes cluster (kind), as a
# user installs it, then a database made, written, read back through the front door with sslmode=verify-full against the platform's own
# authority, put to sleep and woken by a connection, and read again.
#
#   .github/peat/install-test.sh <storage image> <compute 18 image> <compute 19 image> <control image>
#
# The control image must be in the local Docker (it is loaded into the cluster); the others are pulled by the cluster, or loaded from the
# local Docker when LOAD_IMAGES names them. The store is a MinIO
# in the cluster, the storage controller's records a Postgres in the cluster (records postgres: the Kubernetes records come with the
# controller that keeps them). The platform is a one-machine trial (spread none) on the cluster's one node. Needs kind, kubectl, docker,
# bun and psql on the PATH (KIND names another kind). KEEP=1 keeps the cluster afterwards; it is deleted otherwise, pass or fail.
set -euo pipefail
storage=${1:?storage image}; compute18=${2:?compute 18 image}; compute19=${3:?compute 19 image}; control=${4:?control image}
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
kind=${KIND:-kind}
cluster=${CLUSTER:-peat-install}
node_image='kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d'
minio_image='docker.io/bitnamilegacy/minio:2025.7.23-debian-12-r5@sha256:6dabb4a2088c9a79908de3bc05f4586c23ad2182c8908e7e3acbf61c1467fb20'
postgres_image='docker.io/library/postgres:17-alpine@sha256:f9cf3e1f0066f0eb0c48707d864f0ac5941e9ff454a072077d97a4d278d07d73'
work=$(mktemp -d)
# a kubeconfig of the test's own: kind writes its context there, and the user's own kubeconfig (and its current context) is never touched
export KUBECONFIG="$work/kubeconfig"
k() { kubectl --context "kind-$cluster" "$@"; }
started=$(date +%s)
since() { echo "$(( $(date +%s) - $1 ))"; }
pids=()
finish() {
  rc=$?
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  if [ "$rc" != 0 ]; then
    echo "::group::the cluster as the test left it"
    k get platform -o yaml 2>/dev/null | sed -n '/^  status:/,$p' || true
    k get pods -A -o wide 2>/dev/null || true
    k get events -A --sort-by=.lastTimestamp 2>/dev/null | tail -60 || true
    for ns in peat-system peat peat-gateway peat-compute; do
      for p in $(k -n "$ns" get pods -o name 2>/dev/null); do echo "--- $ns $p"; k -n "$ns" logs "$p" --all-containers --tail=80 2>/dev/null || true; done
    done
    echo "::endgroup::"
  fi
  [ "${KEEP:-}" = 1 ] || "$kind" delete cluster --name "$cluster" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$rc"
}
trap finish EXIT
fail() { echo "INSTALL TEST FAILED: $*" >&2; exit 1; }

echo "== a throwaway cluster"
"$kind" create cluster --name "$cluster" --image "$node_image" --wait 180s
"$kind" load docker-image "$control" --name "$cluster"
# images already in the local Docker (a CI run's own), loaded rather than pulled
for image in ${LOAD_IMAGES:-}; do "$kind" load docker-image "$image" --name "$cluster"; done
node="$cluster-control-plane"
k label node "$node" peat.zabaca.com/machine=true --overwrite

echo "== the store and the records' database (throwaways of their own)"
k create namespace store
k -n store apply -f - <<EOF
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
            - { name: MINIO_ROOT_USER, value: peat-test }
            - { name: MINIO_ROOT_PASSWORD, value: peat-test-secret }
            - { name: MINIO_DEFAULT_BUCKETS, value: peat }
          ports: [ { containerPort: 9000 } ]
          readinessProbe: { httpGet: { path: /minio/health/ready, port: 9000 } }
---
apiVersion: v1
kind: Service
metadata: { name: minio }
spec: { selector: { app: minio }, ports: [ { port: 9000 } ] }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: records-db }
spec:
  selector: { matchLabels: { app: records-db } }
  template:
    metadata: { labels: { app: records-db } }
    spec:
      containers:
        - name: postgres
          image: $postgres_image
          env:
            - { name: POSTGRES_PASSWORD, value: peat-test }
            - { name: POSTGRES_DB, value: storage_controller }
          ports: [ { containerPort: 5432 } ]
          readinessProbe: { exec: { command: [ pg_isready, -U, postgres ] } }
---
apiVersion: v1
kind: Service
metadata: { name: records-db }
spec: { selector: { app: records-db }, ports: [ { port: 5432 } ] }
EOF

echo "== the install file"
(cd "$root/control" && bun src/install/install.ts --image "$control") > "$work/peat.yaml"
k apply --server-side -f "$work/peat.yaml"
k -n peat-system rollout status deployment/peat-operator --timeout=180s

echo "== the platform: its namespace and the store's Secret first, then one Platform"
k create namespace peat
k -n peat create secret generic peat-store --from-literal=AWS_ACCESS_KEY_ID=peat-test --from-literal=AWS_SECRET_ACCESS_KEY=peat-test-secret
k -n peat create secret generic records-db --from-literal=uri='postgresql://postgres:peat-test@records-db.store.svc:5432/storage_controller'
k -n store rollout status deployment/minio --timeout=180s
k -n store rollout status deployment/records-db --timeout=180s
k apply -f - <<EOF
apiVersion: peat.zabaca.com/v1alpha1
kind: Platform
metadata: { name: peat }
spec:
  release: ci
  images: { storage: "$storage", control: "$control", compute: { "18": "$compute18", "19": "$compute19" } }
  spread: none
  objectStore: { endpoint: "http://minio.store.svc:9000", region: us-east-1, bucket: peat, credentialsSecret: peat-store }
  pageservers: { count: 1, storage: { hostPath: { path: /var/lib/peat/pageserver, create: true } } }
  safekeepers: { storage: { hostPath: { path: /var/lib/peat/safekeeper, create: true } } }
  controller: { records: postgres, postgres: { urlSecret: records-db } }
  gateways: { count: 1 }
  control: { replicas: 1 }
  computes: { idleMinutes: 1 }
  databases: { deleteHold: 1h }
EOF

echo "== every piece Ready"
t0=$(date +%s); phase=''
until [ "$phase" = Ready ]; do
  [ "$(since "$t0")" -lt 900 ] || fail "the platform was not Ready in 15 minutes: $(k get platform peat -o jsonpath='{.status.message}')"
  sleep 10
  phase=$(k get platform peat -o jsonpath='{.status.phase}' 2>/dev/null || true)
  echo "  $(since "$t0") s: ${phase:-none}: $(k get platform peat -o jsonpath='{.status.message}' 2>/dev/null | cut -c1-200)"
done
ready_s=$(since "$t0")
echo "  Ready $ready_s s after the Platform was applied; survives: $(k get platform peat -o jsonpath='{.status.survives}')"
k get platform peat -o jsonpath='{range .status.pieces[*]}{.kind} {.name} {.release} {.ready}{"\n"}{end}'

echo "== the guards, judged by the cluster's own API server, as the control layer's account"
as=(--as=system:serviceaccount:peat:peat-control)
refused() { # refused <words the refusal must hold> <kubectl arguments...>
  local want=$1; shift
  if out=$(k "${as[@]}" "$@" --dry-run=server 2>&1); then fail "the control layer's account was allowed: $*"; fi
  grep -q "$want" <<< "$out" || fail "refused for another reason than \"$want\": $out"
  echo "  refused: $* ($want)"
}
k "${as[@]}" taint node "$node" peat.zabaca.com/dead=true:NoSchedule --dry-run=server >/dev/null || fail 'the dead taint was refused on a platform machine'
echo '  allowed: the dead taint on a platform machine'
refused 'not its labels' label node "$node" example.com/extra=1
refused 'it may not cordon a node' patch node "$node" -p '{"spec":{"unschedulable":true}}'
refused 'not another taint' taint node "$node" example.com/other=true:NoSchedule
refused 'ClusterIP or ExternalName only' -n peat-db create service nodeport outside --tcp=5432:5432
refused "only of the platform's storage image" -n peat create -f - <<EOF
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: not-a-safekeeper }
spec:
  serviceName: x
  selector: { matchLabels: { app: x } }
  template:
    metadata: { labels: { app: x } }
    spec: { containers: [ { name: x, image: docker.io/library/busybox:1.37 } ] }
EOF

echo "== a database, through the control layer"
k -n peat port-forward svc/peat-control 18080:8080 >/dev/null 2>&1 & pids+=($!)
k -n peat-gateway port-forward svc/pg-front 15432:5432 >/dev/null 2>&1 & pids+=($!)
sleep 3
made=$(curl -fsS -X POST localhost:18080/db -H 'content-type: application/json' -d '{"name":"first"}') || fail "the control layer did not make a database"
echo "  $made" | cut -c1-300
k -n peat-compute get secret peat-ca -o jsonpath='{.data.ca\.crt}' | base64 -d > "$work/ca.crt"
password=$(k -n peat get secret db-first-login -o jsonpath='{.data.password}' | base64 -d)
conn="host=first.peat-db.svc hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/ca.crt connect_timeout=30"
sql() { PGPASSWORD="$password" psql "$conn" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }

t0=$(date +%s); until sql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 300 ] || fail 'the new database did not answer in 5 minutes'; sleep 3; done
first_s=$(since "$t0")
sql 'create table t (id int primary key, payload text)'
sql "insert into t select g, md5(g::text) from generate_series(1, 100000) g"
[ "$(sql 'select count(*) from t')" = 100000 ] || fail 'the rows written were not read back'
echo "  first answer $first_s s after the make; 100000 rows written and read back with verify-full"

echo "== asleep, then woken by a connection"
curl -fsS -X POST localhost:18080/db/first/stop >/dev/null || fail 'the stop was refused'
t0=$(date +%s); while k -n peat-compute get pods -l compute=first -o name 2>/dev/null | grep -q .; do [ "$(since "$t0")" -lt 120 ] || fail 'the compute did not go'; sleep 2; done
t0=$(date +%s)
count=$(sql 'select count(*) from t') || fail 'the sleeping database did not wake for a connection'
wake_s=$(since "$t0")
[ "$count" = 100000 ] || fail "after the wake the count was $count"
echo "  woken in $wake_s s by a connection; the rows are all there"

echo "== a database as an object, in a namespace of its own"
k create namespace shop
dbobj() { k apply -f - <<EOF
apiVersion: peat.zabaca.com/v1alpha1
kind: Database
metadata: { name: $1, namespace: shop }
spec: { $2 }
EOF
}
phase_is() { # phase_is <db> <phase> <seconds>
  local t0; t0=$(date +%s)
  until [ "$(k -n shop get database "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" = "$2" ]; do
    [ "$(since "$t0")" -lt "$3" ] || fail "$1 was not $2 in $3 s: $(k -n shop get database "$1" -o jsonpath='{.status.message}' 2>/dev/null)"
    sleep 2
  done
}
dbobj orders ''
phase_is orders Ready 120
t0=$(date +%s); until k -n shop get secret orders-login >/dev/null 2>&1 && k -n shop get configmap peat-ca >/dev/null 2>&1; do [ "$(since "$t0")" -lt 60 ] || fail 'the login and the certificate were not published beside the Database'; sleep 2; done
t0=$(date +%s); until [ "$(k -n shop get database orders -o jsonpath='{.status.loginSecret}/{.status.caConfigMap}')" = 'orders-login/peat-ca' ]; do [ "$(since "$t0")" -lt 60 ] || fail 'the status does not name the login and the certificate'; sleep 2; done
k -n shop get configmap peat-ca -o jsonpath='{.data.ca\.crt}' > "$work/shop-ca.crt"
shop_password=$(k -n shop get secret orders-login -o jsonpath='{.data.password}' | base64 -d)
shop_host=$(k -n shop get secret orders-login -o jsonpath='{.data.host}' | base64 -d)
shop() { PGPASSWORD="$shop_password" psql "host=$2 hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/shop-ca.crt connect_timeout=30" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
t0=$(date +%s); until shop 'select 1' "$shop_host" >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'orders did not answer'; sleep 3; done
shop 'create table items (id int primary key)' "$shop_host"
shop 'insert into items select generate_series(1, 1000)' "$shop_host"
sleep 2; moment=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 2
shop 'insert into items select generate_series(1001, 3000)' "$shop_host"
echo "  orders: made as an object, its login and the authority's certificate published in shop, 3000 rows (1000 before $moment)"

echo "== a branch and a restore to a moment"
dbobj orders-dev 'parent: orders'
dbobj orders-then "parent: orders, at: \"$moment\""
phase_is orders-dev Ready 120
phase_is orders-then Ready 120
t0=$(date +%s); until shop 'select 1' orders-dev.peat-db.svc >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'orders-dev did not answer'; sleep 3; done
[ "$(shop 'select count(*) from items' orders-dev.peat-db.svc)" = 3000 ] || fail "the branch does not read its parent's rows"
t0=$(date +%s); until shop 'select 1' orders-then.peat-db.svc >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'orders-then did not answer'; sleep 3; done
then_count=$(shop 'select count(*) from items' orders-then.peat-db.svc)
[ "$then_count" = 1000 ] || fail "the restore to $moment reads $then_count rows, not the 1000 of that moment"
[ "$(shop 'select count(*) from items' "$shop_host")" = 3000 ] || fail 'the parent changed'
echo "  the branch reads 3000 rows; the restore to $moment reads 1000; orders still 3000"

echo "== closed, then opened"
k -n shop patch database orders --type merge -p '{"spec":{"closed":true}}'
phase_is orders Closed 60
if shop 'select 1' "$shop_host" >/dev/null 2>&1; then fail 'a closed database admitted a connection'; fi
k -n shop patch database orders --type merge -p '{"spec":{"closed":false}}'
phase_is orders Ready 60
t0=$(date +%s); until shop 'select count(*) from items' "$shop_host" >/dev/null 2>&1; do [ "$(since "$t0")" -lt 120 ] || fail 'an opened database did not admit a connection'; sleep 3; done
echo "  a connection refused while closed, admitted once opened"

echo "== deleted: the compute at once, the data held"
k -n shop delete database orders-then --timeout=120s
if k -n peat-compute get deployment compute-orders-then >/dev/null 2>&1; then fail 'the compute of a deleted database is still there'; fi
held=$(k -n peat get configmap held-orders-then -o jsonpath='{.data.deleteAfter}') || fail 'no hold was recorded for the deleted data'
echo "  orders-then deleted: its compute gone, its data held until $held"

echo "== the peat command, through the test's own login"
peatc() { (cd "$root/control" && bun src/cli/peat.ts "$@"); }
peatc make cli-made -n shop --timeout 120 || fail 'peat make failed'
peatc list -n shop | grep -q '^shop/cli-made  Ready' || fail 'peat list does not show the database it made'
cli_url=$(peatc url cli-made -n shop) || fail 'peat url failed'
peatc ca -n shop > "$work/cli-ca.crt"
cli_password=$(sed -E 's|^postgresql://app:([^@]*)@.*$|\1|' <<< "$cli_url")
t0=$(date +%s); until PGPASSWORD="$cli_password" psql "host=cli-made.peat-db.svc hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/cli-ca.crt connect_timeout=30" -XAtqc 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'the database peat made did not answer'; sleep 3; done
peatc stop cli-made -n shop || fail 'peat stop failed'
peatc close cli-made -n shop || fail 'peat close failed'
peatc open cli-made -n shop || fail 'peat open failed'
peatc delete cli-made -n shop --yes || fail 'peat delete failed'
peatc status | grep -q '^peat: Ready' || fail 'peat status does not say Ready'
echo "  make, list, url, ca, stop, close, open, delete and status, each through the kubeconfig; the database answered with its published URL's login"

echo "== passed in $(since "$started") s: Ready in $ready_s s, a database's first answer in $first_s s, a wake in $wake_s s"
