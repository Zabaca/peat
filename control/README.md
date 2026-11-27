# control: Peat's control layer, gateways, drills and operator

The program that runs a Peat platform on Kubernetes, beside the storage code it runs: one TypeScript program, run by Bun, in one image with
an entry per role. It is the program that has run a production Peat platform since 2026-11-20, brought here with every name of that install
made a setting, and the operator that installs and keeps a whole platform from one object.

| Part | What it is | Where |
|---|---|---|
| The control layer | Turns "a database" into Neon's terms (tenant, timeline, compute); starts and stops computes; replaces a compute or a safekeeper that is lost; repoints computes when the storage controller moves a tenant; trims old WAL from the store. Two pods, one acting at a time. | `src/control.ts`, with its pure decisions in `src/storage-spec.ts` |
| The gateways | The one front door: a client connects to `<database>.<domain>`, the gateway reads the name from TLS (without decrypting), wakes the database if it sleeps, and passes the bytes through. Puts a database to sleep after its idle time. | `src/gateway/` |
| The drills | Failure drills runnable against an install: pods cut, disks wiped, machines cut (by a command the installer supplies), the store unreachable, the controller's records restored. Every cutting step goes through one guard. | `src/drills/` |
| The operator | Reads the one Platform object and makes every piece of it: the namespaces, the keys and a certificate authority when none are given, the storage (pinned to machines by the spread rules), the control layer, the gateways and the admission rules that hold the control layer's account. Keeps each as declared, removes what it made and no longer renders, never touches what it did not make, and reports each piece and its release in the Platform's status. | `src/operator/` |
| The install file | The kinds, the operator's namespace and account, and its Deployment. | `src/install/install.ts` |
| The object kinds | `Platform` and `Database` in `peat.zabaca.com/v1alpha1`. | `src/api/v1alpha1.ts`, examples in `examples/` |
| The storage's checks | What an upgrade reads between steps (every node active, every shard attached, every secondary warm) and a pageserver's drain and fill. | `src/storage/` |
| Shared names | Namespaces, the machine label, the dead taint, the API group: neutral defaults, each a setting. | `src/names.ts` |

Still to come in this folder: the move of a database to a newer Postgres major (`peat move` and `pgVersion` raised are refused until then).

## Upgrades

Changing the Platform's `release` (and its `images`, for an install that pins them) moves the platform one piece at a time: the storage
controller and the broker; each pageserver, drained first (its shards moved to the others), restarted and filled again; each safekeeper; the
gateways; the control layer. The whole platform is checked before and after every step (every piece ready, every pageserver Active, every
shard attached, nothing reconciling, every secondary copy warm, every safekeeper answering). A check that does not pass within
`upgrades.checkTimeout` (a duration, `10m` by default) halts the upgrade with the step and the reason in the status (`peat status`); it stays halted until `peat retry`
(it goes on from that step) or until the earlier release is set back (only what had moved goes back, last first, with the same checks).
No step starts inside `upgrades.quietHours`: an upgrade under way waits at the next step boundary. A compute takes the new image at its
next start, or, awake, inside `upgrades.restartHours`, one at a time. The state is in `status.upgrade`, so a restarted operator goes on.

## Adoption

A platform installed another way is taken over by naming its objects in the Platform's `adopt` list (kind, namespace, name). The operator
compares each with what it would make (the fields it sets, the annotations it sets, a Secret's values): one it would not change is
taken over as it is, and is the operator's from then on, listed or not; one it would change is left alone, marked adopted, and the change
is listed in `status.pendingChanges` until `requests.applyPending` asks for it (taking it off the list applies nothing). A change of a
field that cannot change in place (a selector, a StatefulSet's volume claims) is listed as one that cannot be applied. While anything is
pending the platform reads Installing, not Ready. An entry that names nothing the operator makes is said in the status. It never deletes
what it did not make or adopt.

## Databases

A database is a Database object, in any namespace; its name is unique across the platform. The control layer makes the storage behind
it (a new database, a branch of `parent`, a restore of `parent` to the moment `at`, a replica of `replicaOf`) and keeps its record in
the object's status, with a copy in the platform namespace (a ConfigMap `db-<name>`) from which a lost status is written again (so a
Database object deleted without its finalizer and made again, while that ConfigMap is there, attaches to the same data); the
operator publishes, in the object's namespace, the Secret `<name>-login` (username, password, host, URL) and the ConfigMap `peat-ca`
(the authority's certificate, for `sslmode=verify-full`; not published when `certificates.issuer` is set and `keys.caSecret` is not,
since the platform's own authority then signs nothing), and the status names both. `spec.closed: true` refuses every connection until it
is false again; `spec.requests.stop` puts it to sleep now. A deleted database's compute goes at once and its data after the Platform's
`databases.deleteHold` (`never` keeps it; a hold is fixed when the database is deleted); a database made from another waits for its
branches to be deleted first, and the name can be used again at once. The control layer's HTTP API (`POST /db`, `/branch`, `/restore`,
`/replica`, `DELETE /db/<name>`) writes the same objects. `POST /records/copy` makes a Database object for every record that has none
(an install whose records are the control layer's older ConfigMaps, or a cluster whose objects were lost) and compares the two
(`?dry=1` compares only); `?held=<name>` first brings a deleted database back from its held data while the hold lasts.

## Installing

The control image is `ghcr.io/zabaca/peat/control`, published with the storage and compute images on every merge to main, tagged with
the commit (`.github/peat/control.Dockerfile`; one program, the role named by its first argument: `operator`, `control` or `gateway`).

1. Apply the install file: `bun src/install/install.ts --image ghcr.io/zabaca/peat/control:<tag> | kubectl apply --server-side -f -`.
2. Label the machines the platform may use: `kubectl label node <node> peat.zabaca.com/machine=true` (three for the default spread; one
   for a trial with `spread: none`).
3. Make the platform namespace (`peat` by default) and put the store's Secret in it (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
4. Apply a Platform (`examples/platform.yaml`, or `examples/trial.yaml` for one machine) and read `kubectl get platform` until it says
   Ready.

`.github/peat/install-test.sh` does exactly this on a throwaway kind cluster (a one-machine trial with a MinIO store), then makes a
database, writes and reads it through the front door with `sslmode=verify-full`, puts it to sleep and wakes it by connecting, and has
the cluster's API server judge the admission rules as the control layer's account. CI runs it on every pull request (this change's
control image against main's published storage images) and on every merge (that run's images), and publishes nothing a failed run
built. It needs kind, kubectl, docker, bun and psql; locally, `KIND=<path to kind> .github/peat/install-test.sh <storage> <compute 18>
<compute 19> <control>` with the control image built into the local Docker.

## The objects

A whole install is one `Platform` (cluster-scoped, one per cluster) and its databases are `Database` objects (namespaced). Every field, its
meaning and its default is in `src/api/v1alpha1.ts`, which the CustomResourceDefinitions are rendered from, so `kubectl explain
platform.spec` and `kubectl explain database.spec` print the same text. In short:

- **Platform** `spec`: `release` (every piece runs it; changing it upgrades), `images`, `namespaces`, `machines` (the label that marks the
  platform's machines, their taint), `spread` (`machines`, or `none` for a one-machine trial), `objectStore` (one bucket and Secret, or one
  per piece), `pageservers`, `safekeepers` (three or five, their zone label, `outside` machines that may hold one member), `controller`
  (`records: kubernetes` or `postgres`), `gateways` (count, `domain`, how the front door is exposed), `certificates` (a cert-manager issuer,
  or the platform's own authority), `computes` (default Postgres version, `idleAfter`, memory), `databases.deleteHold`, `upgrades`
  (`quietHours`, `restartHours`, `checkTimeout`), `keys` (given, or made), `control`, `adopt`, `suspend`, `requests.retry`,
  `requests.applyPending`. Every duration is a number and a unit (`30m`, `7d`). Rules across fields (a namespace never changes; a
  version never lowered; `parent`, `at` and `replicaOf` fixed once made) are the API server's, in the CustomResourceDefinitions.
- **Database** `spec`: `pgVersion` (raising it moves the database to the newer major under its own name), `parent` and `at` (a branch, or
  a restore to a moment as a new database), `replicaOf`, `idleAfter`, `memory`, `closed`, `keepOldFor`, `requests.stop`,
  `requests.rehearseMove`. Its `status` says where to connect (`host`, `serviceHost`, `port`), the login Secret and the authority's
  certificate in the database's own namespace.

## The command

`peat` (`src/cli/peat.ts`: `bun src/cli/peat.ts`, or the one-file program for Linux and macOS that CI builds on main and attaches,
with its SHA256SUMS, to each `release-*` tag), working through the user's own Kubernetes login on Database objects (so the cluster's
permissions decide who may do what; a token, a client certificate or an exec credential plugin, and a KUBECONFIG of several files, as
kubectl reads them), each change read back until the control layer has answered it:
`peat list`, `peat show <db>`, `peat make <db> [--pg 18|19]`, `peat branch <from> <db>`, `peat restore <from> <db> --at <time>`,
`peat replica <of> <db>`, `peat stop <db>`, `peat close <db>`, `peat open <db>`, `peat delete <db> --yes`, `peat url <db>`,
`peat ca [<db>]`, and for the platform `peat status` and `peat retry [<platform>]`. A database is looked up in the kubeconfig context's
namespace, or `-n`'s; `-A` looks in every namespace and refuses a name more than one holds. A delete and a close name the context and
server they act on. `peat move` is refused in this release: the release that carries a move out turns it on.

## Settings

Each piece reads its settings from its environment, which the operator writes from the Platform; unset, each is the neutral default.
The names every piece shares are `NAMESPACE`, `COMPUTE_NAMESPACE`, `GATEWAY_NAMESPACE`, `DB_NAMESPACE`, `MACHINE_LABEL` (`key=value`),
`MACHINE_TAINT` and `DEAD_TAINT` (`key=value:Effect`), read by `src/names.ts`. The rest are read where they are used, at the top of each
program: `src/control.ts`, `src/gateway/settings.ts`, `src/drills/guard.ts` (the drills also take `--kubeconfig`, which `run` and `recover`
require), and the storage's scripts, `src/storage/api.ts` (`KEY_DIR`, the signing key's folder, `/keys` by default) and
`src/storage/roll.ts` (`ROLL_ACTION`, `ROLL_NODE`, `ROLL_WAIT_S`).

## Working here

```sh
cd control
bun install
bun test            # every test, the public-names check among them
bun run typecheck
```

Peat's CI runs the same on every pull request and every merge (the `control` job of `.github/workflows/peat-ci.yml`).

**Everything here is public.** Code, tests, comments and examples name no real person, company, host, address, bucket or record: hosts are
`example.com` and its kind, addresses are in the documentation ranges (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24), and machines are
`machine-1` and the like. `test/public-names.test.ts` fails on anything else it can recognise, in this folder and in the repository's
`.github` (`src/public-names.ts` says what it looks for and what it allows); the maintainer also runs a check with a private list of names before a change here merges. No em dashes or en dashes.
