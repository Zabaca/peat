# control: Peat's control layer, gateways, drills and operator

The program that runs a Peat platform on Kubernetes, beside the storage code it runs: one TypeScript program, run by Bun, in one image with
an entry per role. It is the program that has run a production Peat platform since 2026-11-20, brought here with every name of that install
made a setting; the operator that installs, keeps and upgrades a whole platform from one object is being built on it.

| Part | What it is | Where |
|---|---|---|
| The control layer | Turns "a database" into Neon's terms (tenant, timeline, compute); starts and stops computes; replaces a compute or a safekeeper that is lost; repoints computes when the storage controller moves a tenant; trims old WAL from the store. Two pods, one acting at a time. | `src/control.ts`, with its pure decisions in `src/storage-spec.ts` |
| The gateways | The one front door: a client connects to `<database>.<domain>`, the gateway reads the name from TLS (without decrypting), wakes the database if it sleeps, and passes the bytes through. Puts a database to sleep after its idle time. | `src/gateway/` |
| The drills | Failure drills runnable against an install: pods cut, disks wiped, machines cut (by a command the installer supplies), the store unreachable, the controller's records restored. Every cutting step goes through one guard. | `src/drills/` |
| The object kinds | `Platform` and `Database` in `peat.zabaca.com/v1alpha1`. | `src/api/v1alpha1.ts`, examples in `examples/` |
| The storage's checks | What an upgrade reads between steps (every node active, every shard attached, every secondary warm) and a pageserver's drain and fill. | `src/storage/` |
| Shared names | Namespaces, the machine label, the dead taint, the API group: neutral defaults, each a setting. | `src/names.ts` |

Still to come in this folder: the operator (installing and keeping every piece from a Platform, keys and a certificate authority made
when none are given, the ordered upgrade, adoption), the control layer's records as Database objects, the `peat` command, the move of a
database to a newer Postgres major, the install file and the control image.

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

`peat`, working through the user's own Kubernetes login on Database objects (so the cluster's permissions decide who may do what):
`peat list`, `peat show <db>`, `peat make <db> [--pg 18|19]`, `peat branch <from> <db>`, `peat restore <from> <db> --at <time>`,
`peat replica <of> <db>`, `peat stop <db>`, `peat close <db>`, `peat open <db>`, `peat move <db> --pg <version> [--rehearse]`,
`peat delete <db>`, `peat ca`, and for the platform `peat status` and `peat retry`.

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
`machine-1` and the like. `test/public-names.test.ts` fails on anything else it can recognise (`src/public-names.ts` says what it looks for
and what it allows); the maintainer also runs a check with a private list of names before a change here merges. No em dashes or en dashes.
