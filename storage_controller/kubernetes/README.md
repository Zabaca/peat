# The storage controller's records in Kubernetes objects

The storage controller keeps a small set of durable records: every tenant's shards with their generation numbers, the pageservers and safekeepers it manages, each timeline's safekeeper membership and pending safekeeper operations, timeline imports, the scrubber's health records and the current leader. Neon keeps them in a Postgres database of their own (`persistence.rs`, `schema.rs`). Peat adds a second place for them: objects in the Kubernetes cluster the platform runs in, so an install on Kubernetes needs no Postgres beside it.

This file is the contract between the controller and whatever installs it (Peat's operator, or a person): the settings, the object kinds and their fields, the permissions the controller's account needs, and the command line of the copy tool. A change to anything here is a pull request that changes this file.

**Status:** the store itself is built: `--records kubernetes` keeps every record in these objects (`storage_controller/src/persistence/kubernetes.rs`), held to the Postgres store's behaviour by tests that run every operation on both (`storage_controller/src/persistence/tests.rs`, with [`test-stores.sh`](test-stores.sh)). The copy in the bucket and the copy tool are not built yet, so until they are the controller accepts `--records kubernetes` only with `--dev`, and `--records-copy-storage` does not exist. Postgres stays the controller's default when no setting names a store.

Files beside this one:

- [`crds.yaml`](crds.yaml): the six object kinds (CustomResourceDefinitions), for an install file to apply before the controller starts.
- [`rbac.yaml`](rbac.yaml): the Role the controller's account needs, and its RoleBinding, with the namespace and account name as placeholders.
- [`test-stores.sh`](test-stores.sh): starts a Kubernetes API server with these kinds and a Postgres server, for the tests of both stores.

## Why the records can move

Generations are what keep two pageservers from both acting as the owner of a shard's data: every attach takes a new, higher generation, and a pageserver's writes to the object store are valid only while its generation is the latest. The records' store must therefore never hand out the same generation twice, and must keep each multi-row change atomic. Every multi-row transaction that matters for this in Neon's controller (a generation's increment, a pageserver's re-attach for one tenant, a split's begin, completion and abort) touches the shards of one tenant only. So the records are grouped **one object per tenant, holding all its shards**, and each of those changes is one write of one object. Every write is a compare-and-swap on the object's `resourceVersion`; a conflict re-reads the object and retries, as a serialization failure does on Postgres. A create fails if the object exists; a delete carries the `resourceVersion` it read as a precondition.

A pageserver's re-attach, one transaction on Postgres, becomes one write per tenant attached to that pageserver. Each write is safe to repeat: a controller that stops halfway is finished by the pageserver's retry of its re-attach, and generations may then skip a number but never repeat one.

The controller reads every object at start, as it reads every row today. Reads that must be current (a generation, a timeline's membership) go to the API server, never to a cache.

## Settings

The controller's command line, beside Neon's:

| Setting | Meaning |
|---|---|
| `--records <postgres\|kubernetes>` | Where the records are kept. Default `postgres`. |
| `--database-url`, or `DATABASE_URL` | With `postgres` only: the database's address, as in Neon. Given with `kubernetes`, the controller refuses to start, so a setting is never silently ignored. |
| `--records-namespace <namespace>` | With `kubernetes` only: the namespace holding the record objects. Default: the namespace of the pod's own service account (`/var/run/secrets/kubernetes.io/serviceaccount/namespace`), and outside a pod the current namespace of the kubeconfig. |
| `--records-copy-storage <TOML>` | With `kubernetes` only: where the copy of the records goes (below), in the syntax of the pageserver's `remote_storage` setting, for example `{ bucket_name = "records", bucket_region = "auto", endpoint = "https://store.example.com", prefix_in_bucket = "storage-controller/" }`. Required unless `--dev`. |

The API server is reached as any Kubernetes client reaches it: in a pod, through the pod's service account; outside one, through the kubeconfig named by `KUBECONFIG` (or `~/.kube/config`).

The copy's bucket is reached with the same credentials the pageserver uses for its own: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the environment (or the other sources of the AWS SDK's default chain) for an S3-compatible store. The records' copy should be a bucket, or a prefix under a key, that only the controller writes: the controller needs no access to the pageservers' or safekeepers' data.

### What the controller refuses at start, with `kubernetes`

- **The kinds are missing:** the namespace answers 404 for any of the six kinds (the install file has not applied `crds.yaml`).
- **The records are empty but the copy is not:** the namespace holds no Tenant, Pageserver or Safekeeper object, while the bucket's copy holds some. A cluster whose objects were lost would otherwise start from nothing, tell every pageserver to drop its tenants, and later hand out generation numbers the object store has already seen. The way on is a restore (below). On a first install both are empty and the controller starts.
- **No copy is configured,** outside `--dev`.

With records present, the controller starts even if the bucket cannot be reached, and keeps the copy's writes waiting until it can (so a store outage does not also stop the controller).

## The copy in the bucket

Within a second of each change to the records, the controller writes the changed objects to the copy's bucket, one JSON file per object, and removes the file of a deleted object. At start it compares the bucket with the objects and rewrites whatever differs. The copy is for one use: rebuilding the records after the cluster's objects are lost, with `storcon_records restore` (below). The layout under the prefix is the controller's and may change between releases; read it through the tool.

The metric `storage_controller_records_copy_behind_seconds` is the age of the oldest change not yet in the copy (0 when it is current), for an alert.

The controller's database metrics (`storage_controller_database_query_latency`, `storage_controller_database_query_error`) keep their names and their `operation` label on either store.

## The object kinds

Group `storagecontroller.peat.zabaca.com`, version `v1alpha1`, all namespaced, all in the category `peat-records` (`kubectl get peat-records` lists them). Every field sits under `spec`; there is no `status`. Field names are camelCase; integers are JSON numbers; times are RFC 3339 strings in UTC with microseconds, as Postgres keeps them.

Three fields keep the exact text Neon's Postgres backend stores, so a copy between the two stores is exact and needs no translation: a tenant shard's `placementPolicy`, `config` and `schedulingPolicy` (each the JSON text serde writes, for example `"\"Active\""` for a shard's scheduling policy and `"{\"Attached\":1}"` for a placement), and an import's `shardStatuses` (JSON text).

These objects are the controller's own. Nothing else writes them, except the copy tool while the controller is stopped. Do not edit them by hand: an edit to a generation can let two pageservers own the same data.

The kinds' schemas are strict (an unknown field is dropped by the API server), so the controller compares every object the API server returns from a write with the object it sent, and stops with an error if a field was dropped: a controller newer than the installed kinds fails at its first write instead of losing data.

### Tenant

One per tenant, named by the tenant id (32 lowercase hex characters). Neon's `tenant_shards` rows of that tenant.

| Field | Type | Neon's column |
|---|---|---|
| `spec.tenantId` | string | `tenant_id` |
| `spec.shards[]` | list, one per shard (during a split, the parents and the children) | |
| `.shardNumber` | integer | `shard_number` |
| `.shardCount` | integer | `shard_count` |
| `.shardStripeSize` | integer | `shard_stripe_size` |
| `.generation` | integer, absent when none | `generation` |
| `.generationPageserver` | integer, absent when none | `generation_pageserver` |
| `.placementPolicy` | string, JSON text | `placement_policy` |
| `.splitting` | `Idle` or `Splitting` | `splitting` (0 or 1) |
| `.config` | string, JSON text | `config` |
| `.schedulingPolicy` | string, JSON text | `scheduling_policy` |
| `.preferredAzId` | string, absent when none | `preferred_az_id` |

Label kept by the controller: `generation.storagecontroller.peat.zabaca.com/<node id>: "true"` for every pageserver that some shard's `generationPageserver` names, so a re-attach lists only that pageserver's tenants.

### MetadataHealth

One per tenant, named by the tenant id. Neon's `metadata_health` rows of that tenant (the scrubber's findings), kept apart from the generations so the scrubber's writes never conflict with them.

| Field | Type | Neon's column |
|---|---|---|
| `spec.tenantId` | string | `tenant_id` |
| `spec.shards[]` | list, one per shard | |
| `.shardNumber`, `.shardCount` | integer | `shard_number`, `shard_count` |
| `.healthy` | boolean | `healthy` |
| `.lastScrubbedAt` | time | `last_scrubbed_at` |

### Timeline

One per timeline that has a record on safekeepers, pending safekeeper operations or an import, named `<tenant id>-<timeline id>`; and one per tenant with tenant-wide pending safekeeper operations (Neon's empty timeline id), named by the tenant id, with `timelineId` empty. The object exists while any of its three parts does; the controller deletes it when the last goes. A timeline's membership change and the pending operations it schedules are one write, as they are one transaction on Postgres.

| Field | Type | Neon's table and column |
|---|---|---|
| `spec.tenantId` | string | |
| `spec.timelineId` | string, empty for the tenant-wide object | |
| `spec.membership` | object, absent when the timeline has no row in `timelines` | `timelines` |
| `.startLsn` | string, as `0/16B3748` | `start_lsn` |
| `.generation` | integer | `generation` |
| `.skSet` | list of integers | `sk_set` |
| `.newSkSet` | list of integers, absent when none | `new_sk_set` |
| `.cplaneNotifiedGeneration` | integer | `cplane_notified_generation` |
| `.skSetNotifiedGeneration` | integer | `sk_set_notified_generation` |
| `.deletedAt` | time, absent when none | `deleted_at` |
| `spec.pendingOps[]` | list, at most one per safekeeper | `safekeeper_timeline_pending_ops` |
| `.safekeeperId` | integer | `sk_id` |
| `.generation` | integer | `generation` |
| `.kind` | `pull`, `exclude` or `delete` | `op_kind` |
| `spec.import` | object, absent when there is no import | `timeline_imports` |
| `.shardStatuses` | string, JSON text | `shard_statuses` |

Label kept by the controller: `storagecontroller.peat.zabaca.com/tenant: <tenant id>`.

### Pageserver

One per pageserver, named `pageserver-<node id>`, deleted ones included (a tombstone keeps a deleted node from registering again). Neon's `nodes` table.

| Field | Type | Neon's column |
|---|---|---|
| `spec.nodeId` | integer | `node_id` |
| `spec.schedulingPolicy` | string, as `active` or `pause_for_restart` | `scheduling_policy` |
| `spec.lifecycle` | `active` or `deleted` | `lifecycle` |
| `spec.listenHttpAddr`, `spec.listenHttpPort` | string, integer | `listen_http_addr`, `listen_http_port` |
| `spec.listenHttpsPort` | integer, absent when none | `listen_https_port` |
| `spec.listenPgAddr`, `spec.listenPgPort` | string, integer | `listen_pg_addr`, `listen_pg_port` |
| `spec.listenGrpcAddr`, `spec.listenGrpcPort` | string, integer, absent when none | `listen_grpc_addr`, `listen_grpc_port` |
| `spec.availabilityZoneId` | string | `availability_zone_id` |

### Safekeeper

One per safekeeper, named `safekeeper-<id>`. Neon's `safekeepers` table.

| Field | Type | Neon's column |
|---|---|---|
| `spec.id` | integer | `id` |
| `spec.regionId` | string | `region_id` |
| `spec.version` | integer | `version` |
| `spec.host`, `spec.port`, `spec.httpPort` | string, integer, integer | `host`, `port`, `http_port` |
| `spec.httpsPort` | integer, absent when none | `https_port` |
| `spec.availabilityZoneId` | string | `availability_zone_id` |
| `spec.schedulingPolicy` | string, as `active` | `scheduling_policy` |

### Leader

At most one, named `leader`. Neon's `controllers` table: the controller that leads, so a new controller started as a candidate asks it to step down and then takes its place by a compare-and-swap, Neon's own handover.

| Field | Type | Neon's column |
|---|---|---|
| `spec.address` | string | `address` |
| `spec.startedAt` | time | `started_at` |

Neon's two `hadron_*` tables are not read or written by the controller's code, and have no kind.

## Permissions

The controller's service account needs, in the records' namespace only, `get`, `list`, `watch`, `create`, `update` and `delete` on `tenants`, `metadatahealths`, `timelines`, `pageservers`, `safekeepers` and `leaders` of the group: the Role in [`rbac.yaml`](rbac.yaml). Nothing cluster-wide, and nothing on any other kind. The copy tool needs the same, run by an account that holds it. Installing `crds.yaml` needs a cluster administrator, as any CustomResourceDefinition does.

## The copy tool

`storcon_records`, a program beside `storage_controller` in the storage image. It reads and writes the records with the controller stopped: to move a controller from Postgres to Kubernetes objects and back, to compare the two, to take a baseline before a change, and to rebuild the objects from the bucket's copy.

It names a store as one of:

- `postgres`: the database at `--database-url` or `DATABASE_URL`, as the controller reaches it. The tool runs the controller's migrations on an empty database before writing to it.
- `kubernetes`: the objects in `--records-namespace`, reached as the controller reaches them.
- `bucket`: the copy at `--records-copy-storage`, with the controller's credentials. Read only, by `dump`, `compare` and `restore`.

Commands:

```
storcon_records dump --from <store> [--output <file>]
storcon_records compare <store> <store> [--json]
storcon_records copy --from <postgres|kubernetes> --to <postgres|kubernetes> [--replace]
storcon_records restore --to <postgres|kubernetes> --records-copy-storage <TOML> --pageserver-storage <TOML> [--replace]
```

- **`dump`** writes every record as one JSON document (to the file, or to standard output), sorted, the same shape from any store, for a baseline or a person to read.
- **`compare`** reads both stores and reports every difference, field by field (with `--json`, as one JSON document), and the count of each kind on each side.
- **`copy`** writes every record of one store into the other, then compares them. The destination must be empty unless `--replace` is given, which first removes what it holds. Generations are copied as they are.
- **`restore`** rebuilds the records from the bucket's copy, for a platform whose objects were lost. Every shard's generation is set above the highest of what the copy holds for it and what the pageservers' store (`--pageserver-storage`, the pageservers' own `remote_storage` setting, with their credentials) shows in its index files, and each timeline's membership is read back from its safekeepers (with `SAFEKEEPER_JWT_TOKEN` when they need one), keeping the highest generation; so no generation held by a pageserver can be issued again. Then it compares the result with the copy, generations aside.

Every command that writes refuses to start (exit 3) while the controller or any pageserver answers: it tries a connection to the leader's address and to each pageserver's HTTP port named in the records it reads (for `restore`, in the copy), and names whatever answered. A pageserver still attached could hold a generation the copy does not show yet; a running controller could write while the tool does.

Exit codes: 0 done (for `compare`, the stores are equal); 1 an error; 2 a wrong command line; 3 refused, with the reason (something still runs, or the destination is not empty without `--replace`); 4 the stores differ (from `compare`, or from the check at the end of `copy` and `restore`).
