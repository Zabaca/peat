# The scale measure: a storage server's restart

When a pageserver starts, it asks the storage controller to re-attach it, and the controller raises the generation of every shard attached to that pageserver before it answers. On Postgres that is one transaction. On the Kubernetes records it is one write per tenant (README, "Why the records can move"), so a pageserver with many databases on it waits for that many writes to the API server. This measure times that wait, at 100 and at 1,000 databases, on a real API server. Its result sets where the documentation advises Postgres instead.

## What it does

`storcon_records measure-restart` measures; the test `measure_a_storage_servers_restart` (`storage_controller/src/records_tool/tests.rs`, skipped unless asked for) runs the same code. Either one:

1. Makes a namespace of its own (`storcon-measure-<random>`) on the API server, and a copy of the records in a temporary folder, then starts the Kubernetes store there as the controller does: start checks, then the kinds' check.
2. Registers pageserver 1. Through the controller's own operations, 32 at a time, it makes the given number of tenants (1,000 by default) of one shard each, attached to pageserver 1 (a create and an increment per tenant). This is the **fill**.
3. Times pageserver 1's **re-attach**, the store's part of the restart, and checks that every tenant was re-attached.
4. Measures the copy: how long after the re-attach it held every new generation, its size, and how long one write of it takes.
5. Deletes its namespace, unless asked to keep it.
6. Given an empty Postgres database, runs steps 2 and 3 there too, for comparison.

It touches nothing outside its own namespace and the database it is given. It needs the record kinds applied ([`crds.yaml`](crds.yaml)), and an account that may make and delete a namespace and the record objects in any namespace (below).

## How to run it in a cluster

From the storage image, `ghcr.io/zabaca/peat/storage:<tag>`: the image of a merge to `main` that carries this command, or a later one. The account below holds what the measure needs, cluster-wide, so give it only on a cluster made for the proof. Apply the manifest, then read the Job's log; run it once with `--tenants=100` and once with `--tenants=1000`, deleting the Job between the two:

```yaml
apiVersion: v1
kind: Namespace
metadata: {name: storcon-measure}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: measure, namespace: storcon-measure}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: storcon-measure}
rules:
  - apiGroups: [""]
    resources: [namespaces]
    verbs: [create, delete]
  - apiGroups: [storagecontroller.peat.zabaca.com]
    resources: [tenants, metadatahealths, timelines, pageservers, safekeepers, leaders]
    verbs: [get, list, watch, create, update, delete]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: storcon-measure}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: storcon-measure}
subjects:
  - {kind: ServiceAccount, name: measure, namespace: storcon-measure}
---
apiVersion: batch/v1
kind: Job
metadata: {name: measure-restart, namespace: storcon-measure}
spec:
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: measure
      restartPolicy: Never
      containers:
        - name: measure
          image: ghcr.io/zabaca/peat/storage:<tag>
          command: [storcon_records, measure-restart, --tenants=1000]
```

```sh
kubectl -n storcon-measure wait --for=condition=complete --timeout=30m job/measure-restart
kubectl -n storcon-measure logs job/measure-restart | grep tenants:
kubectl -n storcon-measure delete job measure-restart
```

For the Postgres line, add `--database-url=postgresql://<user>:<password>@<host>:<port>/<database>`, naming an empty database the Job can reach (or set `DATABASE_URL` from a Secret). When the measure is done, delete the namespace `storcon-measure` and the ClusterRole and ClusterRoleBinding `storcon-measure`.

**Before this command is in a published image**, the binary comes from a full CI run of the pull request that adds it (artifact `peat-release-arm64` or `peat-release-amd64`, which holds `target/release/storcon_records`). Run a pod of an earlier storage image with the same account, `command: [sleep, infinity]` in place of the Job's, then:

```sh
gh run download <run id> -R Zabaca/peat -n peat-release-arm64 -D release
zstd -dc release/peat-release-arm64.tar.zst | tar -xf - ./target/release/storcon_records
kubectl -n storcon-measure cp target/release/storcon_records <pod>:/tmp/storcon_records
kubectl -n storcon-measure exec <pod> -- /tmp/storcon_records measure-restart --tenants=1000
```

## How to run it from a checkout

With Rust, against the API server a kubeconfig names:

```sh
KUBECONFIG=<kubeconfig> cargo run --release -p storage_controller --bin storcon_records -- measure-restart --tenants=1000
```

The test runs the same code. `test-stores.sh` starts a throwaway API server and Postgres for it, on a machine with no cluster:

```sh
KUBE_ASSETS=<envtest binaries> STORCON_MEASURE_TENANTS=1000 \
  storage_controller/kubernetes/test-stores.sh \
  cargo test --release -p storage_controller measure_a_storage_servers_restart -- --ignored --nocapture
```

That measures a local API server with no network between it and the controller, not a cluster's.

## What it prints

One line per store:

```
kubernetes, 100 tenants: fill 0.2 s; re-attach 0.06 s (1562 tenants/s); copy 61883 bytes, one write 0 ms, caught up 0.2 s after the re-attach
postgres, 100 tenants: fill 0.0 s; re-attach 0.00 s (47497 tenants/s)
kubernetes, 1000 tenants: fill 1.9 s; re-attach 0.65 s (1534 tenants/s); copy 614483 bytes, one write 1 ms, caught up 0.2 s after the re-attach
postgres, 1000 tenants: fill 0.2 s; re-attach 0.01 s (142601 tenants/s)
```

The figures above are from envtest's API server on a laptop, through `test-stores.sh`, as a check that the measure works. They are not the measure.

The number that matters is **re-attach**: how long a restarted pageserver waits before it serves any database attached to it. The pageserver itself puts no limit on this: its call to the controller has no request timeout and is retried until it succeeds (`pageserver/src/controller_upcall_client.rs`). The limit is how long the platform accepts a restarted storage server's databases being unavailable. Record both lines with the API server measured. Kubernetes stays the default unless its re-attach at 1,000 is past that limit; if it is, the line where Postgres is advised is written from these numbers.
