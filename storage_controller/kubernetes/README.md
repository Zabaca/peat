# The storage controller's records in Kubernetes objects

The storage controller keeps a small set of durable records: every tenant's shards with their generation numbers, the pageservers and safekeepers it manages, each timeline's safekeeper membership and pending safekeeper operations, timeline imports, the scrubber's health records and the current leader. Neon keeps them in a Postgres database of their own (`persistence.rs`, `schema.rs`). Peat adds a second place for them: objects in the Kubernetes cluster the platform runs in, so an install on Kubernetes needs no Postgres beside it.

This file is the contract between the controller and whatever installs it (Peat's operator, or a person): the settings, the object kinds and their fields, the permissions the controller's account needs, and the command line of the copy tool. A change to anything here is a pull request that changes this file.

**Status:** the contract only. Nothing in this file is implemented yet; until it is, the controller runs on Postgres alone, as Neon's does. Postgres stays the controller's default when no setting names a store.

Files beside this one:

- [`crds.yaml`](crds.yaml): the six object kinds (CustomResourceDefinitions), for an install file to apply before the controller starts.
- [`rbac.yaml`](rbac.yaml): the Role the controller's account needs, and its RoleBinding, with the namespace and account name as placeholders.

## Why the records can move

Generations are what keep two pageservers from both acting as the owner of a shard's data: every attach takes a new, higher generation, and a pageserver's writes to the object store are valid only while its generation is the latest. The records' store must therefore never hand out the same generation twice, and must keep each multi-row change atomic. Every multi-row transaction that matters for this in Neon's controller (a generation's increment, a pageserver's re-attach for one tenant, a split's begin, completion and abort) touches the shards of one tenant only. So the records are grouped **one object per tenant, holding all its shards**, and each of those changes is one write of one object. Every write is a compare-and-swap on the object's `resourceVersion`; a conflict re-reads the object and retries, as a serialization failure does on Postgres. A create fails if the object exists; a delete carries the `resourceVersion` it read as a precondition.

A pageserver's re-attach, one transaction on Postgres, becomes one write per tenant attached to that pageserver. Each write is safe to repeat: a controller that stops halfway is finished by the pageserver's retry of its re-attach, and generations may then skip a number but never repeat one.

The controller reads every object at start, as it reads every row today. Reads that must be current (a generation, a timeline's membership) go to the API server, never to a cache.

Within one controller, writes to one tenant's object are taken one at a time (a lock per tenant), so a tenant with many shards reconciling at once does not spend its retries on conflicts with itself; the compare-and-swap stays the guard against a second controller. A pageserver's generation validation (a batch of shards, often many tenants') is answered from one list of the Tenant objects, not one read per tenant.

### Writes that touch more than one object

On Postgres these are one transaction; here they are several writes, in this order, each safe to repeat. A failure part way returns an error to the caller, whose retry finishes the work.

- **Re-attach:** read the Pageserver (a deleted one is refused, as on Postgres); one compare-and-swap per tenant labelled with that pageserver, in any order; then the Pageserver's scheduling policy, last, only once every tenant is written. A retry increments the tenants already written again: a generation may skip a number, never repeat one.
- **Creating a tenant's shards:** the Tenant (a shard already present is refused, as a unique violation is), then its MetadataHealth entries. A shard whose health entry is missing reads as having none, as before the scrubber's first run.
- **Removing shards** (deleting a tenant, completing a split, aborting one): the Tenant, then its MetadataHealth entries, in a second write. Postgres removes health rows with their shard by a cascade; here, until the second write lands, a health entry can outlive its shard, so every reader of health entries and the copy tool drop entries whose shard is gone, and the controller removes such entries at start.
- **Preferred zones and the scrubber's health updates,** which Postgres writes for many tenants in one transaction: one write per tenant, in tenant id order. Both are idempotent, and neither is a generation.

## Settings

The controller's command line, beside Neon's:

| Setting | Meaning |
|---|---|
| `--records <postgres\|kubernetes>` | Where the records are kept. Default `postgres`. |
| `--database-url`, or `DATABASE_URL` | With `postgres` only: the database's address, as in Neon. Given with `kubernetes`, the controller refuses to start, so a setting is never silently ignored. |
| `--records-namespace <namespace>` | With `kubernetes` only: the namespace holding the record objects. Default: the namespace of the pod's own service account (`/var/run/secrets/kubernetes.io/serviceaccount/namespace`), and outside a pod the current namespace of the kubeconfig. |
| `--records-copy-storage <TOML>` | With `kubernetes` only: where the copy of the records goes (below), in the syntax of the pageserver's `remote_storage` setting, for example `{ bucket_name = "records", bucket_region = "auto", endpoint = "https://store.example.com", prefix_in_bucket = "storage-controller/" }`. Required unless `--dev`. |

The API server is reached as any Kubernetes client reaches it: in a pod, through the pod's service account; outside one, through the kubeconfig named by `KUBECONFIG` (or `~/.kube/config`).

The copy's bucket is reached the way the pageserver reaches its own: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the environment (or the other sources of the AWS SDK's default chain) for an S3-compatible store. The records' copy should be a bucket, or a prefix under a key, that only the controller writes: the controller needs no access to the pageservers' or safekeepers' data. (The `remote_storage` syntax carries no credentials, so one process reaches one store's key; the copy tool's restore is split in two steps for that reason, below.)

### What the controller refuses at start, with `kubernetes`

- **The kinds are missing:** the namespace answers 404 for any of the six kinds (the install file has not applied `crds.yaml`).
- **The copy cannot be read.** The copy is how the controller knows its objects have not lost or undone anything, so it starts only once it has read it (or found that the bucket holds none yet), as a controller on Postgres starts only once it reaches its database. Once running, an unreachable bucket only delays the copy's writes.
- **The records are empty but the copy is not:** the namespace holds no Tenant, Pageserver or Safekeeper object, while the bucket's copy holds some. A cluster whose objects were lost would otherwise start from nothing, tell every pageserver to drop its tenants, and later hand out generation numbers the object store has already seen. The way on is a restore (below). On a first install both are empty and the controller starts.
- **The records went back:** for some shard the copy holds a higher generation than its object does, or for some tenant the copy has seen a higher generation (its highest-ever record, below) than any of the tenant's shards holds. That is what the objects look like after the cluster's store (etcd) is taken back to an older snapshot; starting would hand out generations that were already issued. The way on is a restore. (A tenant deleted and made again under the same id meets this check too, since its new shards start below what the copy saw; a restore is the way on there as well.)
- **No copy is configured,** outside `--dev`.

A running controller that sees a generation go back (an object's shard, in the watch the copy is written from, with a lower generation than before) stops writing the copy and exits with an error, so its restart meets the check above.

## The copy in the bucket

Within a second of each change to the records, the controller writes the copy: one document holding every record. It follows the API server rather than its own writes (it watches every kind and writes what the API server holds), so a leader and a candidate during a handover write the same copy, and one that has stepped down never writes back an older one. Beside the records, the document keeps, for every tenant, the highest generation it has ever seen for any of its shards; that entry outlives the tenant's record and is carried over from the copy a controller finds at start.

A copy is never replaced by one that goes back: the controller does not write a document in which some shard's generation is lower than in the copy it last read or wrote, or which holds no records over one that holds some (from inside the cluster, every record gone at once is what a loss looks like, and the copy is then the way back). Either case leaves the copy as it is and shows in the metric below; a generation going back also stops the controller (above).

The copy is for one use: rebuilding the records after the cluster's objects are lost or undone, with `storcon_records restore` (below). Its layout under the prefix is the controller's and may change between releases; read it through the tool.

The metric `storage_controller_records_copy_behind_seconds` is the age of the oldest change not yet in the copy (0 when it is current), for an alert.

The controller's database metrics (`storage_controller_database_query_latency`, `storage_controller_database_query_error`) keep their names and their `operation` label on either store.

## The object kinds

Group `storagecontroller.peat.zabaca.com`, version `v1alpha1`, all namespaced, all in the category `peat-records` (`kubectl get peat-records` lists them). The group is its own, apart from the operator's kinds (which sit under `peat.zabaca.com`), so permissions can be given for one without the other: the controller's account touches only these, and a person or tool allowed to manage databases is not thereby allowed to edit generations. Every field sits under `spec`; there is no `status`. Field names are camelCase; integers are JSON numbers; times are RFC 3339 strings in UTC with microseconds, as Postgres keeps them.

Three fields keep the exact text Neon's Postgres backend stores, so a copy between the two stores is exact and needs no translation: a tenant shard's `placementPolicy`, `config` and `schedulingPolicy` (each the JSON text serde writes, for example `"\"Active\""` for a shard's scheduling policy and `"{\"Attached\":1}"` for a placement), and an import's `shardStatuses` (JSON text; Postgres keeps it as `jsonb`, which reorders keys, so every comparison reads it as JSON, not as text).

These objects are the controller's own. Nothing else writes them, except the copy tool while the controller is stopped. Do not edit them by hand: an edit to a generation can let two pageservers own the same data.

The kinds' schemas are strict. Every write asks the API server for strict field validation (`fieldValidation=Strict`), so a field the installed kinds do not know is refused rather than dropped, and the controller also compares every object the API server returns from a write with the object it sent: a controller newer than the installed kinds fails at its first write instead of losing data. Lists keyed by a field (a tenant's shards by `shardNumber` and `shardCount`, a timeline's pending operations by `safekeeperId`) are declared as maps, so the API server refuses two entries with one key.

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

The controller's service account needs, in the records' namespace only, `get`, `list`, `watch` (the copy is written from a watch), `create`, `update` and `delete` on `tenants`, `metadatahealths`, `timelines`, `pageservers`, `safekeepers` and `leaders` of the group: the Role in [`rbac.yaml`](rbac.yaml). Nothing cluster-wide, and nothing on any other kind. The copy tool needs the same, run by an account that holds it. Installing `crds.yaml` needs a cluster administrator, as any CustomResourceDefinition does.

## The copy tool

`storcon_records`, a program beside `storage_controller` in the storage image. It reads and writes the records with the controller stopped: to move a controller from Postgres to Kubernetes objects and back, to compare the two, to take a baseline before a change, and to rebuild the objects from the bucket's copy.

**Where it runs:** inside the cluster's network, where the controller's and the pageservers' addresses resolve (a Job or a `kubectl exec` in the platform's namespace, with an account holding the Role below). Its check that nothing still runs (below) fails closed, so from a workstation, where those names do not resolve, every command that writes is refused.

It names a store as one of:

- `postgres`: the database at `--database-url` or `DATABASE_URL`, as the controller reaches it. The tool runs the controller's migrations on an empty database before writing to it.
- `kubernetes`: the objects in `--records-namespace`, reached as the controller reaches them.
- `bucket`: the copy at `--records-copy-storage`, with the controller's credentials. Read only, by `dump`, `compare` and `restore`. Health entries whose shard is gone are dropped from every store's records as they are read (Postgres has none), so they never reach a comparison or a write.

Commands:

```
storcon_records dump --from <store> [--output <file>]
storcon_records compare <store> <store> [--json]
storcon_records copy --from <postgres|kubernetes> --to <postgres|kubernetes> [--replace]
storcon_records scan-generations --pageserver-storage <TOML> --output <file>
storcon_records restore --to <postgres|kubernetes> --records-copy-storage <TOML> --generations <file>
                        [--replace] [--generation-margin <N>] [--accept-missing <list>]
                        [--accept-silent-safekeepers]
```

Every command that writes also takes `--controller-address <url>` and `--assume-stopped` (below).

- **`dump`** writes every record as one JSON document (to the file, or to standard output), sorted, the same shape from any store, for a baseline or a person to read.
- **`compare`** reads both stores and reports every difference, field by field (with `--json`, as one JSON document), and the count of each kind on each side.
- **`copy`** writes every record of one store into the other, then compares them. The destination must be empty unless `--replace` is given, which first removes what it holds. Generations are copied as they are.
- **`scan-generations`** reads the pageservers' store (`--pageserver-storage`, the pageservers' own `remote_storage` setting, run with the pageservers' credentials in its environment) and writes, per tenant shard found under `tenants/`, the highest generation any object under the shard's prefix carries as its suffix. It is a separate step so that each run holds one store's key: the pageservers' bucket and the records' copy have keys of their own.
- **`restore`** rebuilds the records from the bucket's copy (run with the copy's credentials), for a platform whose objects were lost or undone. Every shard of a tenant gets one generation, `--generation-margin` (1000 by default) above the highest of the copy's generations for the tenant's shards, the highest the copy has ever seen for the tenant, and the highest `scan-generations` found for any of its shards. The margin covers generations handed out after the copy was last written and never used to write. A shard the copy holds without a generation, of which the store holds nothing, stays without one. So no generation a pageserver held can be issued again.
  - It refuses when the store shows what the copy does not hold (a copy behind during a bucket outage): a tenant with data in the store and no record in the copy, or a tenant split further in the store (a shard count higher than any the copy holds for it). It lists them; `--accept-missing` with that list goes on without them (they keep their data in the store, with no record).
  - Each timeline's safekeeper membership is read back from its safekeepers (with `SAFEKEEPER_JWT_TOKEN` when they need one), keeping the highest configuration any holds. It refuses unless every member of the timeline's current and next sets answers, since a silent one may hold the highest configuration, and a configuration issued again under the same generation would split the timeline's safekeepers; `--accept-silent-safekeepers` keeps the copy's membership for those timelines, and the controller's next change of them may then be refused by the safekeepers until set by hand.
  - Then it reads what it wrote back and compares it with the copy, the shards' generations and the timelines' membership aside.

**The check that nothing still runs.** Every command that writes refuses (exit 3) while the controller runs, and `restore` and `copy --to postgres` also while any pageserver runs:

- The controller is found at the Leader record's address (`--address-for-peers`), or at `--controller-address` when given; with neither, the command is refused, since there is nothing to check.
- Pageservers are found at the HTTP ports their records name (for `restore`, the copy's). `restore` needs them stopped because a pageserver still attached could hold a generation the copy does not show; `copy --to postgres` is the way back of a move to Kubernetes, which the move's plan runs with every pageserver stopped. `copy --to kubernetes`, the move itself, runs with the pageservers up: with the controller stopped no generation can be issued, so the records cannot change under the copy.
- An address that accepts a connection runs; one that refuses it (the host answers, the port is closed, as a Service with no pod behind it does) is stopped; one that does not resolve, or does not answer within 5 seconds, is unknown, and unknown refuses. A name that vanished with its pod (a StatefulSet pod's own name) is unknown too: check that the pod is gone, then give `--assume-stopped`, which skips the whole check and says so in the output.

Exit codes: 0 done (for `compare`, the stores are equal); 1 an error; 2 a wrong command line; 3 refused, with the reason (something runs or may run, the destination is not empty without `--replace`, the store holds what the copy does not, or a safekeeper did not answer); 4 the stores differ (from `compare`, or from the check at the end of `copy` and `restore`).
