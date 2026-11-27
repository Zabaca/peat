# The scale measure: a storage server's restart

When a pageserver starts, it asks the storage controller to re-attach it, and the controller raises the generation of every shard attached to that pageserver before it answers. On Postgres that is one transaction. On the Kubernetes records it is one write per tenant (README, "Why the records can move"), so a pageserver with many databases on it waits for that many writes to the API server. This measure times that wait, at 1,000 and at 10,000 databases, on a real API server. Its result sets where the documentation advises Postgres instead.

## What it does

The test `measure_a_storage_servers_restart`, in `storage_controller/src/records_tool/tests.rs`, is skipped unless asked for. When run, it:

1. Makes a namespace of its own (`records-test-<random>`) on the API server, and a copy of the records in a temporary folder, then starts the Kubernetes store there as the controller does: start checks, then the kinds' check.
2. Registers pageserver 1. Through the controller's own operations, 32 at a time, it makes `STORCON_MEASURE_TENANTS` tenants (1,000 by default) of one shard each, attached to pageserver 1 (a create and an increment per tenant). This is the **fill**.
3. Times pageserver 1's **re-attach**, the store's part of the restart, and checks that every tenant was re-attached.
4. Measures the copy: how long after the re-attach it held every new generation, its size, and how long one write of it takes.
5. Deletes its namespace, unless `STORCON_MEASURE_KEEP` is set.
6. When `STORCON_TEST_DATABASE_URL` is set too, runs steps 2 and 3 on a new database there, for comparison.

It touches nothing outside its own namespace and its own database. It needs:
- a kubeconfig that may make and delete a namespace and the record objects in it;
- the record kinds applied ([`crds.yaml`](crds.yaml)).

## How to run it

From a checkout of this repository, with Rust:

```sh
STORCON_TEST_KUBECONFIG=<the kubeconfig of the API server to measure> \
STORCON_MEASURE_TENANTS=10000 \
  cargo test --release -p storage_controller measure_a_storage_servers_restart -- --ignored --nocapture
```

Run it once with `STORCON_MEASURE_TENANTS=1000`, and once with `10000`. Add `STORCON_TEST_DATABASE_URL=postgresql://<user>@<host>:<port>/<database>` for the Postgres line: the test makes a new database there, so the user needs `CREATEDB`. Without it, the test prints only the Kubernetes line.

On a machine with no cluster, `test-stores.sh` starts a throwaway API server and Postgres and sets both variables:

```sh
KUBE_ASSETS=<envtest binaries> STORCON_MEASURE_TENANTS=10000 \
  storage_controller/kubernetes/test-stores.sh \
  cargo test --release -p storage_controller measure_a_storage_servers_restart -- --ignored --nocapture
```

That measures a local API server with no network between it and the controller, not a cluster's.

## What it prints

One line per store:

```
kubernetes, 10000 tenants: fill 18.4 s; re-attach 7.29 s (1372 tenants/s); copy 6140483 bytes, one write 9 ms, caught up 1.8 s after the re-attach
postgres, 10000 tenants: fill 1.1 s; re-attach 0.12 s (86796 tenants/s)
```

The figures above are from envtest's API server on a laptop, through `test-stores.sh`, as a check that the measure works. They are not the measure.

The number that matters is **re-attach**: how long a restarted pageserver waits before it serves any database attached to it. The pageserver itself puts no limit on this: its call to the controller has no request timeout and is retried until it succeeds (`pageserver/src/controller_upcall_client.rs`). The limit is how long the platform accepts a restarted storage server's databases being unavailable. Record both lines with the API server measured. Kubernetes stays the default unless its re-attach at 10,000 is past that limit; if it is, the line where Postgres is advised is written from these numbers.
