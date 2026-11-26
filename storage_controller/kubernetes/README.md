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

Within one controller, writes to one tenant's object are taken one at a time (a lock per tenant), so a tenant with many shards reconciling at once does not spend its retries on conflicts with itself; the compare-and-swap stays the guard against a second controller. A pageserver's generation validation (a batch of its shards) is answered from one list of the Tenant objects labelled with that pageserver (below), not one read per tenant; a shard whose tenant does not carry that pageserver's label is answered invalid, since its latest generation was issued to another pageserver.

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
| `--records-start-without-copy <until>` | With `kubernetes` only, a last resort chosen by a person: start without reading the copy, before the time given (below). |
| `--records-accept-deleted <kind/name,...>` | With `kubernetes` only: start although these records, in the copy, have no object and no deletion mark, taking them as deleted (below). |

The API server is reached as any Kubernetes client reaches it: in a pod, through the pod's service account; outside one, through the kubeconfig named by `KUBECONFIG` (or `~/.kube/config`).

The copy's bucket is reached the way the pageserver reaches its own: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the environment (or the other sources of the AWS SDK's default chain) for an S3-compatible store. The records' copy should be a bucket, or a prefix under a key, that only the controller writes: the controller needs no access to the pageservers' or safekeepers' data. (The `remote_storage` syntax carries no credentials, so one process reaches one store's key; the copy tool's restore is split in two steps for that reason, below.)

### Records that went back, or went missing

The controller is the only writer of its records, so in a healthy cluster the objects are always at or ahead of the copy in the bucket (below), which follows them within a second. If the cluster's store (etcd) is taken back to an older snapshot, or loses objects, records go back or go missing, and a controller that went on would hand out generations already held. To see it, the controller keeps three things beside the records, and one rule for each record it reads.

- **Revisions.** Every write of an object adds one to its revision, an integer in its annotation `storagecontroller.peat.zabaca.com/revision`. It is the controller's own count, not the API server's `resourceVersion`, which goes back with an etcd restore and is not a number to compare. The copy holds each record's revision. A create starts above the name's deletion mark, above the copy's revision for the name, and above any revision this controller has seen for it, so a name made again (a pageserver id registered again after its hard delete, a tenant's tenant-wide Timeline object, a tenant made again under its id) never starts at or below what it once was. An object with no revision, or one that is not an integer, fails closed: the controller refuses it, naming it.
- **Highest-ever values.** The copy keeps, for good, the highest generation it has seen for any shard of each tenant, and the highest safekeeper membership generation it has seen for each timeline, after the tenant or timeline is gone. They are the floor a restore sets every generation above (below), the one place they are used. They are not a start check: revisions and marks, which are kept for good, already see every record that went back, and a tenant made again under its id (as Neon's restore of a tenant to an earlier time does) starts its generations afresh, as on Postgres, without reading as gone back.
- **Deletion marks.** Before it deletes an object, the controller writes a mark in the bucket (`deleted/<kind>/<name>`) holding the revision it read and is deleting, and the delete waits for it: while the bucket cannot be written, deletes fail and their callers retry. A delete that loses its compare-and-swap to another write reads again and, if it still deletes, writes a new mark first. Marks are kept as long as the highest-ever values, for good, so a deleted record brought back by however old a snapshot is still seen for what it is. A mark **covers** a missing record only when its revision is at or above the copy's revision for that record; a mark below it is stale (the record was written again after the mark, by a write that won over the delete), and covers nothing.

How a record reads, compared with the copy and the marks:

| What is found | Means | Done |
|---|---|---|
| An object below the copy's revision for it, or below its mark's | went back | refused |
| An object exactly at its mark's revision | a delete decided and not done (a crash between the mark and the delete) | finished: the delete is done |
| An object above its mark's revision | a delete that lost to a later write; the mark is stale | nothing: the object lives |
| A record in the copy, no object, a mark covering it | deleted | nothing |
| A record in the copy, no object, no mark covering it | went missing | refused |
| An object with no record in the copy | the copy behind (a change in its last second, or made while the bucket was unreachable) | nothing |
| A MetadataHealth entry whose shard is gone | a removal half done (above) | the entry is removed |

What going back or going missing would do, kind by kind, and so why each refuses:

| Record | Went back | Went missing |
|---|---|---|
| Tenant | A shard's generation is below what was issued: the next attach hands out a generation a pageserver already held, and two pageservers can own the same data. A validation answered from it can let a pageserver that lost the shard delete what the new owner reads. | A pageserver that re-attaches is told nothing of it and drops it, and the copy's next write would lose the only record of its stripe size, placement and config. |
| Timeline | A membership generation is below what the safekeepers hold: the next change issues that generation again with another set, and the timeline's safekeepers split. A lost pending operation leaves a safekeeper unreconciled. | Its membership, and the operations owed to its safekeepers, are lost. |
| MetadataHealth | The scrubber's findings undone. | The same. |
| Pageserver | A scheduling policy or a tombstone undone: a deleted pageserver could register again. | A registered pageserver forgotten. |
| Safekeeper | A scheduling policy undone. | A safekeeper forgotten. |
| Leader | A handover undone. | The same. |

### At start, with `kubernetes`

The controller refuses to start, naming why, when:

- **The kinds are missing:** the namespace answers 404 for any of the six kinds (the install file has not applied `crds.yaml`).
- **No copy is configured,** outside `--dev`.
- **The copy or its marks cannot be read.** The copy is how the controller knows its objects did not go back, so it starts only once it has read it (or found that the bucket holds none yet), as a controller on Postgres starts only once it reaches its database. The cost: a controller that restarts while the bucket is unreachable stays down until it is reachable again, and meanwhile no tenant or timeline is made, branched, migrated or attached, and a pageserver that restarts cannot re-attach.
- **The records are empty but the copy is not:** a cluster whose objects were all lost would otherwise start from nothing, tell every pageserver to drop its tenants, and later hand out generations the store has already seen. On a first install both are empty and the controller starts.
- **A record went back or went missing** (the table above), listing every such record. The way on is a restore (below). For records that went missing because they were deleted without a mark (by hand, or while the bucket was being replaced), `--records-accept-deleted <kind/name,...>` starts with exactly those listed, writing their marks. Such a mark is flagged as accepted, and restore's check that the store holds nothing the copy lacks does not count it. Nothing that went back can be accepted.

The order of the reads: the copy, then the objects, then the marks. A controller started as a candidate runs these checks while the leader still writes; read in this order, the leader's writes meanwhile can only put objects ahead of the copy and add marks, never make a record look as if it went back. The checks run before anything else is done with the records, and before a candidate asks the current leader to step down, so a candidate that would refuse never takes the leader down first. They only read: what the table says to do (finishing a delete, removing an orphaned health entry, writing accepted marks) is done once the controller leads, after it reads again, and before it loads the records to work from.

`--records-start-without-copy <until>` starts without the copy, as a last resort chosen by a person; the operator never sets it. Its value is a time at most 24 hours ahead, and after that time the flag is refused, so a flag left in a manifest stops the next start rather than staying. The checks that need no copy (the kinds) still run before the step-down request. Until the copy is read, a restored etcd goes unseen, and a generation handed out meanwhile may repeat one a pageserver held. The controller writes no copy until it has read one; it tries every 10 seconds, and when it reads it, runs the checks above against it and exits if they fail.

### While it runs

Every object the controller reads, for a write or not (a generation validation, a list, a re-attach), is compared with the highest revision this controller has seen for that name (from the copy and marks at start and on taking over, its own reads and writes, and the watch the copy is written from). An object below it, or below the name's mark, or absent though this controller saw it and no mark covers it, means the store went back under it (an object at its mark is this controller's own delete under way, or one the start finished): it writes nothing more and exits with an error, so its restart meets the checks above. A running controller is not stopped by the bucket: its copy's writes wait, and its deletes fail (above).

A record deleted without a mark (by hand, or by another program) is not written out of the copy: the copy keeps it, the metric below shows the copy behind, and the next start refuses until it is accepted or restored.

### The four cases, walked

Each holds for all six kinds, since the rules above do not depend on the kind.

- **etcd restored while the controller runs.** Its next read of a restored object (the read before a write, a validation, a list, or the watch's list after the restore breaks it) is below what it saw, so it exits before writing or answering from it; the restart compares every object with the copy. An object it has not read since its start is compared at that restart.
- **A crash between the mark, the delete and the copy's write.** Mark written, delete not done: the object is at its mark's revision, and the next leader finishes the delete. Delete done, copy not yet written: the mark covers the record. A delete that lost its compare-and-swap, then a crash before it read again: the object is above its stale mark and lives; if the store later loses it, the copy's later revision means the stale mark does not cover it, and the start refuses. A Tenant removed and its health entries not yet: the entries are removed once the controller leads.
- **A name made again.** It starts above its mark, the copy's revision and this controller's, so it never reads as gone back, and its old mark no longer covers it (the copy's revision for it is now above the mark). Its generations start afresh, as on Postgres: the record that held the old ones was deleted with the tenant's data on the pageservers.
- **Two controllers in a handover.** The candidate checks before asking the leader to step down, reading the copy, then the objects, then the marks, so the leader's writes meanwhile only move things forward. Only the leader writes the copy: a leader asked to step down finishes the copy's write under way before it answers, and the new leader, on taking over, reads the copy's highest-ever values and the marks again and merges them into its own before it writes, so no value is lost between the two.

What is left: a change made in the last second before both the controller stops and etcd is taken back can be lost from both the objects and the copy, which follows them within that second. The metric below bounds it.

## The copy in the bucket

Within a second of each change to the records, the leader writes the copy: one document holding every record with its revision, the highest-ever values, and the time it was written. It follows the API server rather than its own writes: it watches every kind and writes what the API server holds. The marks are objects of their own beside it, written before each delete (above).

The leader does not write a document that goes back from the copy it last read or wrote: one in which a record's revision is lower, which lacks a record that copy holds and no mark covers, or which holds no records over one that holds some (from inside the cluster, every record gone at once is what a loss looks like, and the copy is then the way back). Such a document is not written, and the metric below shows the copy falling behind. The comparison is with what this controller last read or wrote, not a conditional write on the bucket; the handover's order (above) is what keeps two controllers from writing at once.

The copy is one document rewritten whole, so its size and write time grow with the records, and the highest-ever values and the marks grow with every tenant and timeline ever deleted. The scale measure records the copy's size, its write time and the metric below at 10,000 tenants, and the count of highest-ever values and marks after churn; if a write takes more than a second there, the copy is split into parts.

The copy is for one use: rebuilding the records after the cluster's objects are lost or undone, with `storcon_records restore` (below). Its layout under the prefix is the controller's and may change between releases; read it through the tool.

The metric `storage_controller_records_copy_behind_seconds` is the age of the oldest change not yet in the copy (0 when it is current), for an alert.

The controller's database metrics (`storage_controller_database_query_latency`, `storage_controller_database_query_error`) keep their names and their `operation` label on either store.

## The object kinds

Group `storagecontroller.peat.zabaca.com`, version `v1alpha1`, all namespaced, all in the category `peat-records` (`kubectl get peat-records` lists them). Every object carries the revision annotation (above). The group is its own, apart from the operator's kinds (which sit under `peat.zabaca.com`), so permissions can be given for one without the other: the controller's account touches only these, and a person or tool allowed to manage databases is not thereby allowed to edit generations. Every field sits under `spec`; there is no `status`. Field names are camelCase; integers are JSON numbers; times are RFC 3339 strings in UTC with microseconds, as Postgres keeps them.

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

Kept by the controller: the label `generation.storagecontroller.peat.zabaca.com/<node id>: "true"` for every pageserver that some shard's `generationPageserver` names, so a re-attach and a validation list only that pageserver's tenants; and the revision annotation.

### MetadataHealth

One per tenant, named by the tenant id. Neon's `metadata_health` rows of that tenant (the scrubber's findings), kept apart from the generations so the scrubber's writes never conflict with them. Kept by the controller: the revision annotation.

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

Kept by the controller: the label `storagecontroller.peat.zabaca.com/tenant: <tenant id>`, and the revision annotation.

### Pageserver

One per pageserver, named `pageserver-<node id>`, deleted ones included (a tombstone keeps a deleted node from registering again). Neon's `nodes` table. Kept by the controller: the revision annotation.

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

One per safekeeper, named `safekeeper-<id>`. Neon's `safekeepers` table. Kept by the controller: the revision annotation.

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

At most one, named `leader`. Neon's `controllers` table: the controller that leads, so a new controller started as a candidate asks it to step down and then takes its place by a compare-and-swap, Neon's own handover. Kept by the controller: the revision annotation.

| Field | Type | Neon's column |
|---|---|---|
| `spec.address` | string | `address` |
| `spec.startedAt` | time | `started_at` |

Neon's two `hadron_*` tables are not read or written by the controller's code, and have no kind.

## Permissions

The controller's service account needs, in the records' namespace only, `get`, `list`, `watch` (the copy is written from a watch), `create`, `update` and `delete` on `tenants`, `metadatahealths`, `timelines`, `pageservers`, `safekeepers` and `leaders` of the group: the Role in [`rbac.yaml`](rbac.yaml). Nothing cluster-wide, and nothing on any other kind. The copy tool needs the same, run by an account that holds it. Installing `crds.yaml` needs a cluster administrator, as any CustomResourceDefinition does.

## The copy tool

`storcon_records`, a program beside `storage_controller` in the storage image. It reads and writes the records with the controller stopped: to move a controller from Postgres to Kubernetes objects and back, to compare the two, to take a baseline before a change, and to rebuild the objects from the bucket's copy.

**Where it runs:** inside the cluster's network, where the controller's and the pageservers' addresses resolve (a Job or a `kubectl exec` in the platform's namespace, with an account holding the Role above). Its check that nothing still runs (below) fails closed, so from a workstation, where those names do not resolve, every command that writes is refused.

It names a store as one of:

- `postgres`: the database at `--database-url` or `DATABASE_URL`, as the controller reaches it. The tool runs the controller's migrations on an empty database before writing to it.
- `kubernetes`: the objects in `--records-namespace`, reached as the controller reaches them.
- `bucket`: the copy at `--records-copy-storage`, with the controller's credentials, and its marks. Read by `dump`, `compare` and `restore`, and written only by `restore` and `copy --to kubernetes`, which write a new copy of what they wrote (keeping the marks and the highest-ever values). Health entries whose shard is gone are dropped from every store's records as they are read (Postgres has none), so they never reach a comparison or a write.

Commands:

```
storcon_records dump --from <store> [--output <file>]
storcon_records compare <store> <store> [--json]
storcon_records copy --from <postgres|kubernetes> --to <postgres|kubernetes> [--replace]
                     [--records-copy-storage <TOML>]   (required with --to kubernetes)
storcon_records scan-generations --pageserver-storage <TOML> --output <file>
storcon_records restore --to <postgres|kubernetes> --records-copy-storage <TOML> --generations <file>
                        [--replace] [--generation-margin <N>] [--leave-out <list>]
                        [--accept-silent-safekeepers]
```

Every command that writes also takes `--controller-address <url>` and `--assume-stopped` (below).

- **`dump`** writes every record as one JSON document (to the file, or to standard output), sorted, the same shape from any store, for a baseline or a person to read.
- **`compare`** reads both stores and reports every difference, field by field (with `--json`, as one JSON document), and the count of each kind on each side. Revisions and marks are not records, and are not compared.
- **`copy`** writes every record of one store into the other, then compares them. The destination must be empty unless `--replace` is given, which first removes what it holds. Generations are copied as they are. Into `kubernetes` it also needs `--records-copy-storage`: it writes every object at a revision above both the copy's revision and the mark's for its name, with its generations as the source holds them, then writes a new copy of what it wrote (keeping the highest-ever values and the marks), so the controller's start checks meet a copy that matches its objects.
- **`scan-generations`** reads the pageservers' store (`--pageserver-storage`, the pageservers' own `remote_storage` setting, run with the pageservers' credentials in its environment) and writes, per tenant shard found under `tenants/`, the highest generation any object under the shard's prefix carries as its suffix, and the timelines found under it; with the storage it scanned (the setting, which holds no secret) and when it scanned. It is a separate step so that each run holds one store's key: the pageservers' bucket and the records' copy have keys of their own. Run it after every pageserver is stopped, so nothing writes a newer generation after the scan.
- **`restore`** rebuilds the records from the bucket's copy (run with the copy's credentials), for a platform whose objects were lost or undone. Every shard of a tenant gets one generation, `--generation-margin` (1000 by default) above the highest of the copy's generations for the tenant's shards, the highest the copy has ever seen for the tenant, and the highest `scan-generations` found for any of its shards. The margin covers generations handed out after the copy was last written and never used to write. A shard the copy holds without a generation, of which the store holds nothing, stays without one. So no generation a pageserver held can be issued again.
  - It prints the storage and the time the generations file records, and refuses a file that looks like the wrong storage (none of the copy's shards that hold a generation appears in it: a mistyped bucket or prefix) or is older than the copy's last write (a scan from before the loss).
  - It refuses when the store shows what the copy does not hold (a copy behind during a bucket outage), and no deletion mark covers it (an accepted mark does not count): a tenant with data in the store and no record in the copy; a tenant split further in the store (a shard count higher than any the copy holds for it); or, for a tenant of which the copy holds a safekeeper membership for some timeline, a timeline in the store with no record in the copy (a branch made while the bucket was unreachable). It lists them; `--leave-out` with that list goes on without them (they keep their data in the store, with no record).
  - Each timeline's safekeeper membership is read back from its safekeepers (with `SAFEKEEPER_JWT_TOKEN` when they need one), keeping the highest configuration any holds. It refuses unless every member of the timeline's current and next sets answers: a silent one may hold a higher configuration (generation g+1 with a set X while the copy says g), and the controller's next change would issue g+1 again with another set Y, splitting the timeline's safekeepers. `--accept-silent-safekeepers` goes on for those timelines with the copy's set and its membership generation raised by `--generation-margin`, so the next change issues a generation above any a silent member can hold; the real risk left is that the kept set is not the set that last held the timeline's data, so the change that follows must be checked by hand against the safekeepers once they answer.
  - It writes every Kubernetes object at a revision above both the copy's revision and the mark's for its name, and then a new copy of what it wrote (keeping the highest-ever values, raised to what it wrote, and the marks), so the controller's start checks pass on what the restore made and nothing else.
  - Then it reads what it wrote back and compares it with the copy, the shards' generations and the timelines' membership generations aside.

**The check that nothing still runs.** Every command that writes refuses (exit 3) while the controller runs, and `restore` and `copy --to postgres` also while any pageserver runs:

- The controller is found at the Leader record's address (`--address-for-peers`), or at `--controller-address` when given; with neither, the command is refused, since there is nothing to check.
- Pageservers are found at the HTTP ports their records name (for `restore`, the copy's). `restore` needs them stopped because a pageserver still attached could hold a generation the copy does not show; `copy --to postgres` is the way back of a move to Kubernetes, which the move's plan runs with every pageserver stopped. `copy --to kubernetes`, the move itself, runs with the pageservers up: with the controller stopped no generation can be issued, so the records cannot change under the copy.
- An address that accepts a connection runs; one that refuses it (the host answers, the port is closed, as a Service with no pod behind it does) is stopped; one that does not resolve, or does not answer within 5 seconds, is unknown, and unknown refuses. A name that vanished with its pod (a StatefulSet pod's own name) is unknown too: check that the pod is gone, then give `--assume-stopped`, which skips the whole check and says so in the output.

Exit codes: 0 done (for `compare`, the stores are equal); 1 an error; 2 a wrong command line; 3 refused, with the reason (something runs or may run, the destination is not empty without `--replace`, the generations file is of the wrong storage or older than the copy, the store holds what the copy does not, or a safekeeper did not answer); 4 the stores differ (from `compare`, or from the check at the end of `copy` and `restore`).
