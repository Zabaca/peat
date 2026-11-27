#!/bin/bash
# The install test: a whole platform installed from the install file and one Platform object on a throwaway Kubernetes cluster (kind), as a
# user installs it, then a database made, written, read back through the front door with sslmode=verify-full against the platform's own
# authority, put to sleep and woken by a connection, and read again.
#
#   .github/peat/install-test.sh <storage image> <compute 18 image> <compute 19 image> <control image>
#
# The control image must be in the local Docker (it is loaded into the cluster); the others are pulled by the cluster, or loaded from the
# local Docker when LOAD_IMAGES names them. The store is a MinIO
# in the cluster. The storage controller's records are the default, Kubernetes objects with their copy in the store (records
# kubernetes); RECORDS=postgres keeps them in a Postgres in the cluster instead, for an UPGRADE_FROM release whose controller predates
# the Kubernetes records. The platform is a one-machine trial (spread none) on the cluster's one node. Needs kind, kubectl, docker,
# bun and psql on the PATH (KIND names another kind). KEEP=1 keeps the cluster afterwards; it is deleted otherwise, pass or fail.
# UPGRADE_FROM=<a release in the registry> installs that release first (its storage and compute images, this control image) and, after
# the databases' checks, upgrades the platform to the images given, with a writer and a reader running throughout.
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
# the name of a pod that is ready and not on its way out: a deleted pod is replaced at once (a Deployment's Recreate holds only for a
# rollout), and the old one can still read ready beside the new one
ready_pod() { # ready_pod <namespace> <label selector>
  k -n "$1" get pods -l "$2" -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{" "}{.metadata.deletionTimestamp}{"\n"}{end}' 2>/dev/null | awk 'NF == 1 { print $1; exit }'
}
pids=()
finish() {
  rc=$?
  for p in ${pids[@]+"${pids[@]}"}; do pkill -P "$p" 2>/dev/null || true; kill "$p" 2>/dev/null || true; done
  if [ "$rc" != 0 ]; then
    echo "::group::the cluster as the test left it"
    k get platform -o yaml 2>/dev/null | sed -n '/^  status:/,$p' || true
    k get pods -A -o wide 2>/dev/null || true
    k get events -A --sort-by=.lastTimestamp 2>/dev/null | tail -60 || true
    for ns in peat-system peat peat-gateway peat-compute; do
      for p in $(k -n "$ns" get pods -o name 2>/dev/null); do echo "--- $ns $p"; k -n "$ns" logs "$p" --all-containers --tail=80 2>/dev/null || true; done
    done
    for f in "$work"/tunnel-*.log; do [ -f "$f" ] && { echo "--- $(basename "$f")"; tail -40 "$f"; }; done
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

echo "== the store (and, with RECORDS=postgres, the records' database), throwaways of their own"
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
EOF
records=${RECORDS:-kubernetes}
[ "$records" = postgres ] && k -n store apply -f - <<EOF
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
k -n store rollout status deployment/minio --timeout=180s
if [ "$records" = postgres ]; then
  k -n peat create secret generic records-db --from-literal=uri='postgresql://postgres:peat-test@records-db.store.svc:5432/storage_controller'
  k -n store rollout status deployment/records-db --timeout=180s
fi
k apply -f - <<EOF
apiVersion: peat.zabaca.com/v1alpha1
kind: Platform
metadata: { name: peat }
spec:
  release: ${UPGRADE_FROM:-ci}
  images: $(if [ -n "${UPGRADE_FROM:-}" ]; then echo "{ control: \"$control\" }"; else echo "{ storage: \"$storage\", control: \"$control\", compute: { \"18\": \"$compute18\", \"19\": \"$compute19\" } }"; fi)
  spread: none
  objectStore: { endpoint: "http://minio.store.svc:9000", region: us-east-1, bucket: peat, credentialsSecret: peat-store }
  pageservers: { count: 1, storage: { hostPath: { path: /var/lib/peat/pageserver, create: true } } }
  safekeepers: { storage: { hostPath: { path: /var/lib/peat/safekeeper, create: true } } }
  controller: $(if [ "$records" = postgres ]; then echo '{ records: postgres, postgres: { urlSecret: records-db } }'; else echo '{ records: kubernetes }'; fi)
  gateways: { count: 1 }
  control: { replicas: 1 }
  computes: { idleAfter: 1m }
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

echo "== steady: the operator writes nothing to a settled platform"
# the spec of every object it made (a generation where the kind has one, else the resourceVersion), read twice, three passes apart
sel='app.kubernetes.io/managed-by=peat-operator'
made_now() {
  k get deploy,sts,job,networkpolicy -A -l "$sel" -o jsonpath='{range .items[*]}{.kind}/{.metadata.namespace}/{.metadata.name} {.metadata.generation}{"\n"}{end}'
  k get validatingadmissionpolicy,validatingadmissionpolicybinding -l "$sel" -o jsonpath='{range .items[*]}{.kind}/{.metadata.name} {.metadata.generation}{"\n"}{end}'
  k get svc,configmap,secret,serviceaccount,role,rolebinding -A -l "$sel" -o jsonpath='{range .items[*]}{.kind}/{.metadata.namespace}/{.metadata.name} {.metadata.resourceVersion}{"\n"}{end}'
}
sleep 15; made_now | sort > "$work/steady-1"
sleep 35; made_now | sort > "$work/steady-2"
if ! diff -q "$work/steady-1" "$work/steady-2" >/dev/null; then diff "$work/steady-1" "$work/steady-2" | head -20; fail 'the operator changed objects of a settled platform'; fi
echo "  $(wc -l < "$work/steady-1" | tr -d ' ') objects of the operator's unchanged over three of its passes"

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
# a compute of an image the guard does not list, as one made on the release before an upgrade is: it may be stopped, never moved to another such image
k -n peat-compute apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: { name: guard-probe }
spec:
  replicas: 0
  selector: { matchLabels: { app: guard-probe } }
  template:
    metadata: { labels: { app: guard-probe } }
    spec: { automountServiceAccountToken: false, containers: [ { name: compute, image: docker.io/library/busybox:1.37 } ] }
EOF
k "${as[@]}" -n peat-compute patch deployment guard-probe --type merge -p '{"spec":{"replicas":1}}' --dry-run=server >/dev/null || fail "a change that keeps a compute's image was refused"
echo "  allowed: a change that keeps a compute's image, one the guard does not list"
refused "only of the platform's compute images" -n peat-compute patch deployment guard-probe --type json -p '[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"docker.io/library/busybox:1.36"}]'
k -n peat-compute delete deployment guard-probe --wait=false >/dev/null

echo "== the kinds' rules, judged by the cluster's own API server"
kind_refused() { # kind_refused <words the refusal must hold>, the object on standard input
  local want=$1 out
  if out=$(k apply --dry-run=server -f - 2>&1); then fail "the API server admitted an object its kind refuses ($want)"; fi
  grep -q "$want" <<< "$out" || fail "refused for another reason than \"$want\": $out"
  echo "  refused: $want"
}
kind_refused 'at is the moment of a parent' <<EOF
apiVersion: peat.zabaca.com/v1alpha1
kind: Database
metadata: { name: rule-at, namespace: peat-db }
spec: { at: "2026-12-01T00:00:00Z" }
EOF
kind_refused 'the namespaces cannot change' <<EOF
apiVersion: peat.zabaca.com/v1alpha1
kind: Platform
metadata: { name: peat }
spec:
  release: ci
  namespaces: { platform: elsewhere }
  objectStore: { endpoint: "http://minio.store.svc:9000", bucket: peat, credentialsSecret: peat-store }
EOF
kind_refused 'adopt is not in this release' <<EOF
apiVersion: peat.zabaca.com/v1alpha1
kind: Platform
metadata: { name: peat }
spec:
  release: ${UPGRADE_FROM:-ci}
  objectStore: { endpoint: "http://minio.store.svc:9000", bucket: peat, credentialsSecret: peat-store }
  adopt: [{ kind: Deployment, namespace: peat, name: storage-broker }]
EOF

# The tunnels to the control layer and the front door. kubectl port-forward ends when its stream is lost (seen under a client that
# reconnects ten times a second), so each runs in a loop that starts it again, its errors kept for the failure dump, and every step
# from here on first checks that both answer.
tunnel() { # tunnel <name> <namespace> <service> <ports>
  # (|| true: under set -e an ended kubectl would end the loop with it)
  ( while :; do k -n "$2" port-forward "svc/$3" "$4" >>"$work/tunnel-$1.log" 2>&1 || true; echo "$(date -u +%H:%M:%S) ended; started again" >>"$work/tunnel-$1.log"; sleep 1; done ) &
  pids+=($!)
  eval "tunnel_$1=$!"
}
# a port-forward is held to the one pod it picked: after that pod goes (a rollout), its kubectl is ended so the loop starts it on a new one
retunnel() { # retunnel <name> <port>
  local pid; eval "pid=\$tunnel_$1"
  pkill -P "$pid" 2>/dev/null || true
  sleep 2; tunnel_up "$2"
}
tunnel_up() { # tunnel_up <port>: it accepts a connection within 30 s
  local t0; t0=$(date +%s)
  until (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; do [ "$(since "$t0")" -lt 30 ] || fail "the tunnel to port $1 does not answer"; sleep 1; done
}
step() { echo "== $*"; tunnel_up 18080; tunnel_up 15432; }

echo "== a database, through the control layer"
tunnel control peat peat-control 18080:8080
tunnel front peat-gateway pg-front 15432:5432
tunnel_up 18080; tunnel_up 15432
made=$(curl -fsS -X POST localhost:18080/db -H 'content-type: application/json' -d '{"name":"first"}') || fail "the control layer did not make a database"
echo "  $made" | cut -c1-300
k -n peat-db get configmap peat-ca -o jsonpath='{.data.ca\.crt}' > "$work/ca.crt"
password=$(k -n peat get secret db-first-login -o jsonpath='{.data.password}' | base64 -d)
conn="host=first.peat-db.svc hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/ca.crt connect_timeout=30"
sql() { PGPASSWORD="$password" psql "$conn" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }

t0=$(date +%s); until sql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 300 ] || fail 'the new database did not answer in 5 minutes'; sleep 3; done
first_s=$(since "$t0")
sql 'create table t (id int primary key, payload text)'
sql "insert into t select g, md5(g::text) from generate_series(1, 100000) g"
[ "$(sql 'select count(*) from t')" = 100000 ] || fail 'the rows written were not read back'
echo "  first answer $first_s s after the make; 100000 rows written and read back with verify-full"

if [ "$records" = kubernetes ]; then
  step "the storage controller's pod deleted and made again: the new one leads at its own address, the copy current, a new database attached"
  old_ctl=$(k -n peat get pods -l app=storage-controller -o jsonpath='{.items[0].metadata.name}')
  k -n peat delete pod "$old_ctl" --wait=false >/dev/null
  t0=$(date +%s); until new_ctl=$(ready_pod peat app=storage-controller) && [ -n "$new_ctl" ] && [ "$new_ctl" != "$old_ctl" ]; do [ "$(since "$t0")" -lt 180 ] || fail 'no new storage controller was ready in 180 s'; sleep 2; done
  new_ip=$(k -n peat get pod "$new_ctl" -o jsonpath='{.status.podIP}')
  # the Leader record names the new pod by its own address, not the Service's
  t0=$(date +%s); until case "$(k -n peat get leaders.storagecontroller.peat.zabaca.com leader -o jsonpath='{.spec.address}' 2>/dev/null)" in "http://$new_ip:1234"*) true;; *) false;; esac; do
    [ "$(since "$t0")" -lt 60 ] || fail "the Leader record does not name the new controller ($new_ip): $(k -n peat get leaders.storagecontroller.peat.zabaca.com leader -o jsonpath='{.spec.address}' 2>&1)"
    sleep 2
  done
  # the copy in the store is written by the new leader: nothing behind (the pod's own port, so no network rule stands between)
  k -n peat port-forward "pod/$new_ctl" 21234:1234 >>"$work/tunnel-controller.log" 2>&1 &
  pids+=($!); ctl_tunnel=$!
  tunnel_up 21234
  t0=$(date +%s); until [ "$(curl -fsS localhost:21234/metrics 2>/dev/null | awk '/^storage_controller_records_copy_behind_seconds /{print $2}')" = 0 ]; do
    [ "$(since "$t0")" -lt 60 ] || fail "the new leader's copy of the records is behind: $(curl -fsS localhost:21234/metrics 2>&1 | grep records_copy_behind | head -3)"
    sleep 2
  done
  kill "$ctl_tunnel" 2>/dev/null || true
  sleep 30
  curl -fsS -X POST localhost:18080/db -H 'content-type: application/json' -d '{"name":"handover"}' >/dev/null || fail 'no database was made after the controller was replaced'
  ho_password=$(k -n peat get secret db-handover-login -o jsonpath='{.data.password}' | base64 -d)
  t0=$(date +%s); until [ "$(PGPASSWORD="$ho_password" psql "host=handover.peat-db.svc hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/ca.crt connect_timeout=30" -XAtqc 'select 1' 2>/dev/null)" = 1 ]; do [ "$(since "$t0")" -lt 300 ] || fail 'the database made after the controller was replaced did not answer'; sleep 3; done
  echo "  $new_ctl leads at $new_ip, its copy current; a database made 30 s after it answers"
fi

step "asleep, then woken by a connection"
curl -fsS -X POST localhost:18080/db/first/stop >/dev/null || fail 'the stop was refused'
t0=$(date +%s); while k -n peat-compute get pods -l compute=first -o name 2>/dev/null | grep -q .; do [ "$(since "$t0")" -lt 120 ] || fail 'the compute did not go'; sleep 2; done
t0=$(date +%s)
count=$(sql 'select count(*) from t') || fail 'the sleeping database did not wake for a connection'
wake_s=$(since "$t0")
[ "$count" = 100000 ] || fail "after the wake the count was $count"
echo "  woken in $wake_s s by a connection; the rows are all there"

step "a database as an object, in a namespace of its own"
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

step "a branch and a restore to a moment"
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

step "its status wiped, then written again from the platform's record"
before=$(k -n shop get database orders -o jsonpath='{.status.tenant}/{.status.timeline}/{.status.record.port}')
k -n shop patch database orders --subresource=status --type merge -p '{"status":null}'
phase_is orders Ready 60
after=$(k -n shop get database orders -o jsonpath='{.status.tenant}/{.status.timeline}/{.status.record.port}')
[ "$after" = "$before" ] || fail "the status was not written again from the record: $before became $after"
# while its status had no record the gateways had no route for it: a client is refused until the next pass writes it again
t0=$(date +%s); until [ "$(shop 'select count(*) from items' "$shop_host" 2>/dev/null)" = 3000 ]; do [ "$(since "$t0")" -lt 60 ] || fail 'after the status was written again, orders does not read its rows'; sleep 2; done
echo "  the same tenant, timeline and port, and the 3000 rows, $(since "$t0") s later"

step "closed with a client reconnecting, then opened"
# a pooled client: a connection every 100 ms, so one arrives the moment the close ends the sessions
( while :; do shop 'select 1' "$shop_host" >/dev/null 2>&1 || true; sleep 0.1; done ) &
pool=$!
sleep 3
k -n shop patch database orders --type merge -p '{"spec":{"closed":true}}'
phase_is orders Closed 60
sleep 10
kill "$pool" 2>/dev/null || true; wait "$pool" 2>/dev/null || true
tunnel_up 15432
[ "$(k -n peat-compute get deployment compute-orders -o jsonpath='{.spec.replicas}')" = 0 ] || fail 'a client reconnecting as the database closed woke it'
if k -n peat-compute get pods -l compute=orders -o name 2>/dev/null | grep -q .; then fail 'a closed database still runs a compute'; fi
[ "$(k -n shop get database orders -o jsonpath='{.status.awake}')" = false ] || fail 'a closed database reads awake'
if shop 'select 1' "$shop_host" >/dev/null 2>&1; then fail 'a closed database admitted a connection'; fi
k -n shop patch database orders --type merge -p '{"spec":{"closed":false}}'
phase_is orders Ready 60
t0=$(date +%s); until shop 'select count(*) from items' "$shop_host" >/dev/null 2>&1; do [ "$(since "$t0")" -lt 120 ] || fail 'an opened database did not admit a connection'; sleep 3; done
echo "  the compute stopped and stayed stopped with a client reconnecting; a connection refused while closed, admitted once opened"

step "deleted: the compute at once, the data held"
k -n shop delete database orders-then --timeout=120s
if k -n peat-compute get deployment compute-orders-then >/dev/null 2>&1; then fail 'the compute of a deleted database is still there'; fi
held=$(k -n peat get configmap -l peat-held=true -o jsonpath='{range .items[?(@.data.name=="orders-then")]}{.data.deleteAfter}{end}')
[ -n "$held" ] || fail 'no hold was recorded for the deleted data'
echo "  orders-then deleted: its compute gone, its data held until $held"

step "the peat command, through the test's own login"
# the one-file program, as users get it: compiled here for this machine, and every step below drives that file
(cd "$root/control" && bun build src/cli/peat.ts --compile --outfile "$work/peat" >/dev/null) || fail 'the peat command did not compile'
peatc() { "$work/peat" "$@"; }
peatc make cli-made -n shop --timeout 120 || fail 'peat make failed'
peatc list -n shop | grep -q '^shop/cli-made  Ready' || fail 'peat list does not show the database it made'
cli_url=$(peatc url cli-made -n shop) || fail 'peat url failed'
peatc ca cli-made -n shop > "$work/cli-ca.crt"
cli_password=$(sed -E 's|^postgresql://app:([^@]*)@.*$|\1|' <<< "$cli_url")
cli() { PGPASSWORD="$cli_password" psql "host=$2 hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/cli-ca.crt connect_timeout=30" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
t0=$(date +%s); until cli 'select 1' cli-made.peat-db.svc >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'the database peat made did not answer'; sleep 3; done
cli 'create table n (i int)' cli-made.peat-db.svc
cli 'insert into n select generate_series(1, 10)' cli-made.peat-db.svc
sleep 2; cli_moment=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 2
cli 'insert into n select generate_series(11, 30)' cli-made.peat-db.svc
peatc branch cli-made cli-dev -n shop --timeout 120 || fail 'peat branch failed'
peatc restore cli-made cli-then --at "$cli_moment" -n shop --timeout 120 || fail 'peat restore failed'
t0=$(date +%s); until [ "$(cli 'select count(*) from n' cli-dev.peat-db.svc 2>/dev/null)" = 30 ]; do [ "$(since "$t0")" -lt 180 ] || fail 'the branch peat made does not read its parent'\''s 30 rows'; sleep 3; done
t0=$(date +%s); until [ "$(cli 'select count(*) from n' cli-then.peat-db.svc 2>/dev/null)" = 10 ]; do [ "$(since "$t0")" -lt 180 ] || fail "the restore peat made does not read the 10 rows of $cli_moment"; sleep 3; done
peatc stop cli-made -n shop || fail 'peat stop failed'
peatc close cli-made -n shop || fail 'peat close failed'
peatc open cli-made -n shop || fail 'peat open failed'
peatc delete cli-dev -n shop --yes || fail 'peat delete failed'
peatc delete cli-then -n shop --yes || fail 'peat delete failed'
peatc delete cli-made -n shop --yes || fail 'peat delete failed'
peatc status | grep -q '^peat: Ready' || fail 'peat status does not say Ready'
echo "  the compiled command: make, list, url, ca, branch (30 rows), restore to a moment (10 rows), stop, close, open, delete and status, each through the kubeconfig"

upgrade_s=''; max_gap=''
step "the exec guard, judged by the API server, as the control layer's account"
as_control=(--as=system:serviceaccount:peat:peat-control)
# a compute awake to try against (first, woken now: the computes sleep after a minute)
curl -fsS -X POST localhost:18080/db/first/start >/dev/null || fail 'first was not woken for the guard checks'
t0=$(date +%s); until guard_pod=$(k -n peat-compute get pods -l compute=first --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) && [ -n "$guard_pod" ]; do [ "$(since "$t0")" -lt 120 ] || fail 'no compute to try the exec guard on'; sleep 2; done
k -n peat-compute wait --for=condition=Ready "pod/$guard_pod" --timeout=120s >/dev/null
refused_exec() { # refused_exec <what> <expected message part> <exec args...>
  local what=$1 want=$2; shift 2
  local out; out=$(k "${as_control[@]}" -n peat-compute exec "$@" 2>&1) && fail "the exec guard let through: $what"
  grep -qF "$want" <<< "$out" || fail "the exec guard refused $what, but not as expected: $out"
  echo "  refused: $what"
}
# the list is the operator's, there and empty from the install
k -n peat get configmap peat-move-pods >/dev/null 2>&1 || fail "the operator did not make the exec guard's list, peat-move-pods"
refused_exec 'an exec with no move under way (the list empty)' 'is not one' "$guard_pod" -c compute -- psql -h 127.0.0.1 -U cloud_admin -d postgres -Atc 'select 1'
k -n peat patch configmap peat-move-pods --type merge -p '{"data":{"probe":"some-other-pod"}}' >/dev/null
sleep 3
refused_exec 'an exec into a compute no move lists' 'is not one' "$guard_pod" -c compute -- psql -h 127.0.0.1 -U cloud_admin -d postgres -Atc 'select 1'
k -n peat patch configmap peat-move-pods --type merge -p "{\"data\":{\"probe\":\"$guard_pod\"}}" >/dev/null
sleep 3
refused_exec 'a command other than psql or the move script, in a listed pod' 'may run psql or bash /move/copy.sh only' "$guard_pod" -c compute -- ls /
refused_exec 'bash with a script of its own, in a listed pod' 'may run psql or bash /move/copy.sh only' "$guard_pod" -c compute -- bash -c id
[ "$(k "${as_control[@]}" -n peat-compute exec "$guard_pod" -c compute -- psql -h 127.0.0.1 -U cloud_admin -d postgres -Atc 'select 1')" = 1 ] || fail 'the exec guard refused psql in a listed pod'
echo "  allowed: psql in a listed pod"
# the operator holds exec only to grant it: its own rule refuses it any, in a listed pod too
if out=$(k --as=system:serviceaccount:peat-system:peat-operator -n peat-compute exec "$guard_pod" -c compute -- psql -h 127.0.0.1 -U cloud_admin -d postgres -Atc 'select 1' 2>&1); then fail "the operator's account was let exec"; fi
grep -qF "runs no exec" <<< "$out" || fail "the operator's exec was refused, but not by its rule: $out"
echo "  refused: an exec by the operator's account, into a listed pod"
# the list deleted by hand: the guard touches no account but the control layer's (an exec as the cluster's admin into a pod outside the
# platform's namespaces), the control layer is still held to psql in a compute, and the operator makes the list again
k -n peat delete configmap peat-move-pods >/dev/null
[ "$(k -n store exec deploy/minio -- echo reached)" = reached ] || fail "with the exec guard's list missing, the cluster's admin could not exec into a pod outside the platform"
echo "  allowed: an exec by the cluster's admin outside the platform, the list missing"
refused_exec 'a command other than psql or the move script, the list missing' 'may run psql or bash /move/copy.sh only' "$guard_pod" -c compute -- ls /
t0=$(date +%s); until k -n peat get configmap peat-move-pods >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'the operator did not make the list peat-move-pods again'; sleep 3; done
k -n peat patch configmap peat-move-pods --type merge -p '{"data":{"probe":null}}' >/dev/null
echo "  the list made again by the operator"

step "a database moved from Postgres 18 to 19 under its own name (P2), with a second database and a second role of its own"
dbobj mv18 'pgVersion: 18'
phase_is mv18 Ready 120
t0=$(date +%s); until k -n shop get secret mv18-login >/dev/null 2>&1; do [ "$(since "$t0")" -lt 60 ] || fail 'mv18 has no login'; sleep 2; done
mv_password=$(k -n shop get secret mv18-login -o jsonpath='{.data.password}' | base64 -d)
mvsql() { PGPASSWORD="${3:-$mv_password}" psql "host=mv18.peat-db.svc hostaddr=127.0.0.1 port=15432 user=${4:-app} dbname=${2:-app} sslmode=verify-full sslrootcert=$work/shop-ca.crt connect_timeout=30" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
t0=$(date +%s); until mvsql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'mv18 did not answer'; sleep 3; done
case "$(mvsql 'show server_version_num')" in 18*) ;; *) fail 'mv18 is not on Postgres 18';; esac
mvsql 'create table items (id int primary key, note text)'
mvsql "insert into items select g, md5(g::text) from generate_series(1, 500) g"
mvsql "create role reporter login password 'reporter-pw'"
mvsql 'grant reporter to app'
mvsql 'create database reports owner reporter'
mvsql 'create table r (i int)' reports reporter-pw reporter
mvsql 'insert into r select generate_series(1, 42)' reports reporter-pw reporter
mvsql 'create view r_view as select i from r where i > 40' reports reporter-pw reporter
# settings the dumps of the compute's own databases leave out: a database's, the platform login's, a role's in a database of its own
mvsql 'alter database app set search_path = myapp, public'
mvsql "alter role app set statement_timeout = '5min'"
mvsql "alter role reporter in database reports set work_mem = '16MB'"
settings_kept() { # settings_kept <when>
  [ "$(mvsql 'show search_path')/$(mvsql 'show statement_timeout')/$(mvsql 'show work_mem' reports reporter-pw reporter)" = 'myapp, public/5min/16MB' ] || fail "the settings of app, its login or reporter in reports were lost $1"
}
move_done() { # move_done <seconds>: the move's state once it ends
  local t0 state; t0=$(date +%s)
  until state=$(k -n shop get database mv18 -o jsonpath='{.status.move.state}') && case "$state" in done|failed|refused) true;; *) false;; esac; do
    [ "$(since "$t0")" -lt "$1" ] || fail "mv18's move did not end in $1 s: $(k -n shop get database mv18 -o jsonpath='{.status.move.state}: {.status.move.message}')"
    sleep 3
  done
  echo "$state"
}
k -n shop patch database mv18 --type merge -p '{"spec":{"requests":{"rehearseMove":"19@r1"}}}' >/dev/null
t0=$(date +%s); until [ "$(k -n shop get database mv18 -o jsonpath='{.status.move.request}')" = '19@r1' ]; do [ "$(since "$t0")" -lt 60 ] || fail 'the rehearsal was not taken up'; sleep 2; done
[ "$(move_done 600)" = 'done' ] || fail "the rehearsal failed: $(k -n shop get database mv18 -o jsonpath='{.status.move.message}')"
[ "$(k -n shop get database mv18 -o jsonpath='{.status.move.equal}/{.status.pgVersion}')" = 'true/18' ] || fail 'the rehearsal did not compare equal, or switched'
echo "  the rehearsal: $(k -n shop get database mv18 -o jsonpath='{.status.move.message}')"
t0=$(date +%s)
k -n shop patch database mv18 --type merge -p '{"spec":{"pgVersion":19}}' >/dev/null
t1=$(date +%s); until [ "$(k -n shop get database mv18 -o jsonpath='{.status.move.rehearsal}')" != true ]; do [ "$(since "$t1")" -lt 60 ] || fail 'the move was not taken up'; sleep 2; done
[ "$(move_done 900)" = 'done' ] || fail "the move failed: $(k -n shop get database mv18 -o jsonpath='{.status.move.message}')"
move_s=$(since "$t0")
[ "$(k -n shop get database mv18 -o jsonpath='{.status.pgVersion}/{.status.phase}')" = '19/Ready' ] || fail "mv18 is not Ready on 19: $(k -n shop get database mv18 -o jsonpath='{.status.pgVersion}/{.status.phase}: {.status.message}')"
[ "$(k -n shop get secret mv18-login -o jsonpath='{.data.password}' | base64 -d)" = "$mv_password" ] || fail 'the move changed the login'
t0=$(date +%s); until mvsql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 240 ] || fail 'mv18 did not answer after the move'; sleep 3; done
case "$(mvsql 'show server_version_num')" in 19*) ;; *) fail 'mv18 answers, but not on Postgres 19';; esac
[ "$(mvsql 'select count(*) from items')" = 500 ] || fail 'the rows of app are not all on 19'
[ "$(mvsql 'select count(*) from r_view' reports reporter-pw reporter)" = 2 ] || fail 'the second role, its password or its database did not come with the move'
settings_kept 'in the move'
[ "$(k -n shop get database mv18 -o jsonpath='{.status.move.verify}')" = counts ] || fail "a real move did not check by row counts, the default: $(k -n shop get database mv18 -o jsonpath='{.status.move.verify}')"
[ "$(k -n shop get database mv18-pg18 -o jsonpath='{.status.pgVersion}')" = 18 ] || fail 'the old copy mv18-pg18 was not kept'
if k -n peat get configmap peat-move-pods -o jsonpath='{.data}' 2>/dev/null | grep -q shop; then fail 'the move left pods listed for exec'; fi
echo "  moved in $move_s s, under its own name, address and login: 500 rows in app, reporter's database and view, the settings, its old copy mv18-pg18 kept on 18"
echo "  $(k -n peat logs -l app=peat-control --tail=-1 2>/dev/null | grep -o 'mv18: the old version stopped [0-9]* ms after the last look at it' | tail -1)"

step "the way back from the move, while nothing was written since"
k -n shop patch database mv18 --type merge -p '{"spec":{"requests":{"moveBack":"b1"}}}' >/dev/null
t0=$(date +%s); until [ "$(k -n shop get database mv18 -o jsonpath='{.status.move.state}')" = back ]; do
  [ "$(since "$t0")" -lt 600 ] || fail "the way back did not end in 600 s: $(k -n shop get database mv18 -o jsonpath='{.status.phase}: {.status.message}')"
  sleep 3
done
[ "$(k -n shop get database mv18 -o jsonpath='{.status.pgVersion}/{.status.phase}')" = '18/Ready' ] || fail "mv18 is not back on 18: $(k -n shop get database mv18 -o jsonpath='{.status.pgVersion}/{.status.phase}: {.status.message}')"
if k -n shop get database mv18-pg18 >/dev/null 2>&1; then fail 'the old copy is still there after the way back'; fi
t0=$(date +%s); until mvsql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 240 ] || fail 'mv18 did not answer after the way back'; sleep 3; done
case "$(mvsql 'show server_version_num')" in 18*) ;; *) fail 'mv18 answers, but not on Postgres 18';; esac
[ "$(mvsql 'select count(*) from items')" = 500 ] || fail 'the rows of app are not all there after the way back'
[ "$(mvsql 'select count(*) from r_view' reports reporter-pw reporter)" = 2 ] || fail 'reporter or its database did not come back'
echo "  back on 18 under its own name and login: the old copy's data is the database's again"

step "a move resumed after the control layer restarts mid-move, then its old copy deleted when keepOldFor ends"
# asked again (the move to 19 ended in a way back), with a keepOldFor of seconds
k -n shop patch database mv18 --type merge -p '{"spec":{"keepOldFor":"30s","requests":{"retryMove":"r1"}}}' >/dev/null
t0=$(date +%s); until case "$(k -n shop get database mv18 -o jsonpath='{.status.move.request}/{.status.move.state}')" in r1/preparing|r1/copying) true;; *) false;; esac; do
  [ "$(since "$t0")" -lt 120 ] || fail "the retried move did not start: $(k -n shop get database mv18 -o jsonpath='{.status.move.state}: {.status.move.message}')"
  sleep 1
done
cut_at=$(k -n shop get database mv18 -o jsonpath='{.status.move.state}')
k -n peat delete pod -l app=peat-control --wait=false >/dev/null
echo "  the control layer's pod deleted while the move was $cut_at"
[ "$(move_done 900)" = 'done' ] || fail "the resumed move did not end done: $(k -n shop get database mv18 -o jsonpath='{.status.move.message}')"
[ "$(k -n shop get database mv18 -o jsonpath='{.status.pgVersion}/{.status.phase}')" = '19/Ready' ] || fail 'the resumed move did not leave mv18 Ready on 19'
retunnel control 18080
t0=$(date +%s); until mvsql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 240 ] || fail 'mv18 did not answer after the resumed move'; sleep 3; done
case "$(mvsql 'show server_version_num')" in 19*) ;; *) fail 'mv18 answers, but not on Postgres 19';; esac
[ "$(mvsql 'select count(*) from items')" = 500 ] || fail 'the rows of app are not all on 19 after the resumed move'
settings_kept 'in the resumed move'
computes_now=$(k -n peat-compute get pods -l app=peat-compute -o name | wc -l | tr -d ' ')
if k -n peat-compute get deployments -o name | grep -q 'compute-mv-'; then fail "the move left its own compute behind"; fi
echo "  resumed and done on 19 (computes now: $computes_now); waiting for keepOldFor (30 s) to delete mv18-pg18"
t0=$(date +%s); while k -n shop get database mv18-pg18 >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'the old copy was not deleted after its keepOldFor'; sleep 5; done
held_old=$(k -n peat get configmap -l peat-held=true -o jsonpath='{range .items[?(@.data.name=="mv18-pg18")]}{.data.deleteAfter}{end}')
[ -n "$held_old" ] || fail "the old copy's data was not held as a deleted database's"
echo "  mv18-pg18 deleted after its keepOldFor; its data held until $held_old"

step "a session that got in after a move's door shut (as through a gateway whose route is out of date) is ended, and what it wrote is never lost"
dbobj ss18 'pgVersion: 18'
phase_is ss18 Ready 120
t0=$(date +%s); until k -n shop get secret ss18-login >/dev/null 2>&1; do [ "$(since "$t0")" -lt 60 ] || fail 'ss18 has no login'; sleep 2; done
ss_password=$(k -n shop get secret ss18-login -o jsonpath='{.data.password}' | base64 -d)
sssql() { PGPASSWORD="$ss_password" psql "host=ss18.peat-db.svc hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/shop-ca.crt connect_timeout=30" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
t0=$(date +%s); until sssql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'ss18 did not answer'; sleep 3; done
sssql 'create table w (id int primary key)'
# stopped first, so the first compute of ss18 that is ready after the version is raised is its source, started again for the copy
curl -fsS -X POST localhost:18080/db/ss18/stop >/dev/null || fail 'ss18 was not stopped'
t0=$(date +%s); while k -n peat-compute get pods -l compute=ss18 -o name 2>/dev/null | grep -q .; do [ "$(since "$t0")" -lt 120 ] || fail 'ss18 did not stop'; sleep 2; done
k -n shop patch database ss18 --type merge -p '{"spec":{"pgVersion":19}}' >/dev/null
t0=$(date +%s); until ss_pod=$(ready_pod peat-compute compute=ss18) && [ -n "$ss_pod" ]; do [ "$(since "$t0")" -lt 180 ] || fail 'the source of the move was not started'; sleep 1; done
[ "$(k -n shop get database ss18 -o jsonpath='{.status.phase}')" = Moving ] || fail 'the source was started, but the door is not shut'
# straight to the source, a session that writes a row every 0.3 s and prints each one written (psql stops at the first error)
k -n peat-compute port-forward "pod/$ss_pod" 25432:5432 >>"$work/tunnel-ss.log" 2>&1 &
pids+=($!); ss_tunnel=$!
tunnel_up 25432
( i=0; while :; do i=$((i+1)); printf 'insert into w values (%d);\n\\echo ok %d\n' "$i" "$i"; sleep 0.3; done ) | psql "host=127.0.0.1 port=25432 user=app dbname=app sslmode=disable connect_timeout=10" -XAtq -v ON_ERROR_STOP=1 >"$work/ss-written" 2>"$work/ss-writer.err" &
pids+=($!); ss_writer=$!
t0=$(date +%s); until state=$(k -n shop get database ss18 -o jsonpath='{.status.move.state}') && case "$state" in done|failed|refused) true;; *) false;; esac; do
  [ "$(since "$t0")" -lt 900 ] || fail "ss18's move did not end in 900 s: $state"
  sleep 2
done
written=$(grep -c '^ok ' "$work/ss-written" || true)
writer_left=no; kill -0 "$ss_writer" 2>/dev/null && writer_left=yes
kill "$ss_writer" "$ss_tunnel" 2>/dev/null || true
[ "$written" -gt 0 ] || fail "the session never wrote to the source: $(cat "$work/ss-writer.err")"
[ "$writer_left" = no ] || fail "the session on the source was still open after the move ended ($state)"
case "$state" in
  done) want=19;;
  failed) want=18; phase_is ss18 Ready 120;;
  *) fail "ss18's move ended $state: $(k -n shop get database ss18 -o jsonpath='{.status.move.message}')";;
esac
t0=$(date +%s); until got=$(sssql "select count(*) from w where id <= $written") 2>/dev/null; do [ "$(since "$t0")" -lt 240 ] || fail 'ss18 did not answer after its move'; sleep 3; done
case "$(sssql 'show server_version_num')" in "$want"*) ;; *) fail "ss18's move ended $state, but it is not on Postgres $want";; esac
[ "$got" = "$written" ] || fail "the move ended $state and $((written - got)) of the $written rows the session wrote are lost"
echo "  the session wrote $written rows and was ended; the move $state, and every row is on Postgres $want: $(k -n shop get database ss18 -o jsonpath='{.status.move.message}' | cut -c1-200)"
k -n shop delete database ss18 --wait=false >/dev/null

step "a client that got in after a move's door shut, wrote one row and left during the comparison fails the move"
dbobj sw18 'pgVersion: 18'
phase_is sw18 Ready 120
t0=$(date +%s); until k -n shop get secret sw18-login >/dev/null 2>&1; do [ "$(since "$t0")" -lt 60 ] || fail 'sw18 has no login'; sleep 2; done
sw_password=$(k -n shop get secret sw18-login -o jsonpath='{.data.password}' | base64 -d)
swsql() { PGPASSWORD="$sw_password" psql "host=sw18.peat-db.svc hostaddr=127.0.0.1 port=15432 user=app dbname=${2:-app} sslmode=verify-full sslrootcert=$work/shop-ca.crt connect_timeout=30" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
t0=$(date +%s); until swsql 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'sw18 did not answer'; sleep 3; done
swsql 'create table w (id int primary key)'
# a database compared after app, large enough (3M rows) that its comparison takes a while: a row written to app then is past app's own comparison
swsql 'create database zbig'
swsql "create table big as select g as id, md5(g::text) as note from generate_series(1, 3000000) g" zbig
curl -fsS -X POST localhost:18080/db/sw18/stop >/dev/null || fail 'sw18 was not stopped'
t0=$(date +%s); while k -n peat-compute get pods -l compute=sw18 -o name 2>/dev/null | grep -q .; do [ "$(since "$t0")" -lt 120 ] || fail 'sw18 did not stop'; sleep 2; done
# moveVerify full: a comparison of every row, long enough on zbig's 3M rows to write during it (row counts take a moment)
k -n shop patch database sw18 --type merge -p '{"spec":{"pgVersion":19,"moveVerify":"full"}}' >/dev/null
t0=$(date +%s); until sw_pod=$(ready_pod peat-compute compute=sw18) && [ -n "$sw_pod" ]; do [ "$(since "$t0")" -lt 180 ] || fail 'the source of the move was not started'; sleep 1; done
k -n peat-compute port-forward "pod/$sw_pod" 25433:5432 >>"$work/tunnel-sw.log" 2>&1 &
pids+=($!); sw_tunnel=$!
tunnel_up 25433
# the moment: the comparison is reading zbig (app's own comparison is over), seen as the session the compute's own superuser holds on the
# source's zbig while it reads it (the target's side is read after it). Looked for every 50 ms by one session of its own over the pod's
# 127.0.0.1 (a new session for each look took too long: a runner hashes 2M rows in about a second); 600 s at most (psql 16's \watch count)
watch_out=$(printf "select count(*) from pg_stat_activity where datname = 'zbig' and usename = 'cloud_admin' and pid <> pg_backend_pid();\n\\watch i=0.05 c=12000\n" \
  | psql "host=127.0.0.1 port=25433 user=cloud_admin dbname=postgres sslmode=disable connect_timeout=5" -XAt 2>/dev/null | grep -m1 -x '1' ) || true
[ "$watch_out" = 1 ] || fail "sw18's move ended before the comparison of zbig was seen: $(k -n shop get database sw18 -o jsonpath='{.status.move.state}: {.status.move.message}')"
# one session: in, one row, out
psql "host=127.0.0.1 port=25433 user=app dbname=app sslmode=disable connect_timeout=10" -XAtq -v ON_ERROR_STOP=1 -c 'insert into w values (1)' || fail "the client's row was not written during the comparison (the move was $(k -n shop get database sw18 -o jsonpath='{.status.move.state}'))"
kill "$sw_tunnel" 2>/dev/null || true
t0=$(date +%s); until state=$(k -n shop get database sw18 -o jsonpath='{.status.move.state}') && case "$state" in done|failed|refused) true;; *) false;; esac; do
  [ "$(since "$t0")" -lt 600 ] || fail "sw18's move did not end in 600 s: $state"
  sleep 2
done
sw_message=$(k -n shop get database sw18 -o jsonpath='{.status.move.message}')
[ "$state" = failed ] || fail "a row written during the comparison did not fail the move: it ended $state ($sw_message)"
grep -q 'written during the comparison' <<< "$sw_message" || fail "the move failed, but not on the write counts: $sw_message"
phase_is sw18 Ready 120
t0=$(date +%s); until got=$(swsql 'select count(*) from w') 2>/dev/null; do [ "$(since "$t0")" -lt 240 ] || fail 'sw18 did not answer after its move'; sleep 3; done
case "$(swsql 'show server_version_num')" in 18*) ;; *) fail 'sw18 is not on Postgres 18 after its failed move';; esac
[ "$got" = 1 ] || fail "the row written during the comparison is not on sw18"
echo "  the move failed, the row on 18: $(cut -c1-220 <<< "$sw_message")"
k -n shop delete database sw18 --wait=false >/dev/null

if [ -n "${UPGRADE_FROM:-}" ]; then
  step "the upgrade from $UPGRADE_FROM to this run's images, a writer and a reader running throughout"
  curl -fsS -X POST localhost:18080/db -H 'content-type: application/json' -d '{"name":"upg"}' >/dev/null || fail 'the upgrade test database was not made'
  upg_password=$(k -n peat get secret db-upg-login -o jsonpath='{.data.password}' | base64 -d)
  upg() { PGPASSWORD="$upg_password" psql "host=upg.peat-db.svc hostaddr=127.0.0.1 port=15432 user=app dbname=app sslmode=verify-full sslrootcert=$work/ca.crt connect_timeout=5" -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
  t0=$(date +%s); until upg 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 180 ] || fail 'upg did not answer'; sleep 3; done
  upg 'create table w (id int primary key, at timestamptz default now())'
  # a table larger than the compute's cache, so every read asks the pageservers for pages (a count of a small table would be served from memory)
  upg "create table big as select g, repeat(md5(g::text), 8) as pad from generate_series(1, 400000) g"
  : > "$work/acked"; : > "$work/acked-at"; : > "$work/reads"
  ( i=0; while [ ! -f "$work/stop" ]; do i=$((i + 1)); if upg "insert into w (id) values ($i)" >/dev/null 2>&1; then echo "$i" >> "$work/acked"; date +%s >> "$work/acked-at"; fi; sleep 0.1; done ) & pids+=($!)
  ( while [ ! -f "$work/stop" ]; do if upg "select count(*), sum(length(pad)) from big where g % 97 = $((RANDOM % 97))" >/dev/null 2>&1; then date +%s >> "$work/reads"; fi; sleep 1; done ) & pids+=($!)
  sleep 10
  t0=$(date +%s)
  k patch platform peat --type merge -p "{\"spec\":{\"release\":\"ci\",\"images\":{\"storage\":\"$storage\",\"control\":\"$control\",\"compute\":{\"18\":\"$compute18\",\"19\":\"$compute19\"}}}}"
  last=''
  until [ "$(k get platform peat -o jsonpath='{.status.release}/{.status.phase}')" = 'ci/Ready' ]; do
    [ "$(since "$t0")" -lt 900 ] || fail "the upgrade did not finish in 15 minutes: $(k get platform peat -o jsonpath='{.status.message}')"
    msg=$(k get platform peat -o jsonpath='{.status.phase}: {.status.message}' 2>/dev/null | cut -c1-200)
    [ "$msg" = "$last" ] || echo "  $(since "$t0") s: $msg"; last=$msg
    case "$msg" in Halted*) fail "the upgrade halted: $msg";; esac
    sleep 3
  done
  upgrade_s=$(since "$t0")
  sleep 10; touch "$work/stop"; sleep 3
  acked=$(wc -l < "$work/acked" | tr -d ' ')
  stored=$(upg 'select count(*) from w')
  missing=$(upg "select count(*) from unnest(array[$(paste -sd, "$work/acked")]::int[]) a(id) where not exists (select 1 from w where w.id = a.id)")
  [ "$missing" = 0 ] || fail "$missing acknowledged inserts are missing after the upgrade ($acked acknowledged, $stored stored)"
  max_gap=$(awk 'NR > 1 && $1 - p > m { m = $1 - p } { p = $1 } END { print m + 0 }' "$work/reads")
  max_write_gap=$(awk 'NR > 1 && $1 - p > m { m = $1 - p } { p = $1 } END { print m + 0 }' "$work/acked-at")
  # the proof's bound (P3): no gap a reader or a writer sees passes 30 seconds
  [ "$max_gap" -le 30 ] || fail "a reader waited $max_gap s during the upgrade (the bound is 30)"
  [ "$max_write_gap" -le 30 ] || fail "a writer waited $max_write_gap s during the upgrade (the bound is 30)"
  k get platform peat -o jsonpath='{range .status.pieces[*]}  {.kind} {.name} {.release}{"\n"}{end}'
  # a database woken after the upgrade runs the new compute image (the control layer and the gateways rolled: their tunnels start again)
  retunnel control 18080; retunnel front 15432
  curl -fsS -X POST localhost:18080/db/upg/stop >/dev/null || fail 'upg was not stopped'
  t0=$(date +%s); while k -n peat-compute get pods -l compute=upg -o name 2>/dev/null | grep -q .; do [ "$(since "$t0")" -lt 120 ] || fail 'upg did not go to sleep'; sleep 2; done
  # the first start on the new compute image may pull it first
  t0=$(date +%s); until upg 'select 1' >/dev/null 2>&1; do [ "$(since "$t0")" -lt 240 ] || fail 'upg did not wake after the upgrade'; sleep 3; done
  woke_on=$(k -n peat-compute get pods -l compute=upg -o jsonpath='{.items[0].spec.containers[0].image}')
  [ "$woke_on" = "$compute19" ] || fail "upg woke on $woke_on, not the new compute image $compute19"
  echo "  the upgrade took $upgrade_s s; $acked inserts acknowledged, none missing ($stored stored); the longest gaps: a reader's $max_gap s, a writer's $max_write_gap s; upg woke on the new compute image"
fi

echo "== passed in $(since "$started") s: Ready in $ready_s s, a database's first answer in $first_s s, a wake in $wake_s s${upgrade_s:+, an upgrade in $upgrade_s s (the longest gaps: $max_gap s reading, $max_write_gap s writing)}"
