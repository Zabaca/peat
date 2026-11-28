# Peat

**Peat is a fork of [Neon](https://github.com/neondatabase/neon), the open-source serverless Postgres.** It keeps Neon's storage engine running on current Postgres releases (18 and 19) and adds a Kubernetes operator that installs and runs it for you.

## Why it exists

Neon's open-source repository is no longer kept up in the open: it stopped taking new Postgres releases in 2025. Peat keeps it alive and current. Its changes are written by AI coding agents, and each one is reviewed by a separate reviewing agent before it merges.

## What it is

Serverless Postgres, with storage separated from compute:

- **Scale to zero.** A database nobody is using is stopped (its data stays); the next connection wakes it, in seconds.
- **Instant branches.** Copy-on-write: a branch copies no data and costs only what it later changes.
- **Point-in-time recovery.** Restore a database, or branch it, from any moment in its history (seven days by default).

## What Peat adds

A control plane and a Kubernetes operator. You declare one `Platform` object (a bucket and a few machines) and the operator installs and runs the whole thing: the storage, the control plane and the front door that databases are reached through. Each database is a Kubernetes object. Upgrades are ordered and checked: change the release in the `Platform` and the platform moves one piece at a time. A database can be moved to a newer Postgres version the same way, by changing one field.

## Status

Alpha (`peat.zabaca.com/v1alpha1`). It runs Zabaca's own production databases.

## Quickstart

On a laptop, about two minutes. You need Docker (running), [kind](https://kind.sigs.k8s.io/), `kubectl`, [Bun](https://bun.sh) and `psql`:

```sh
git clone --depth 1 https://github.com/Zabaca/peat
peat/control/examples/try-it.sh
```

It makes a throwaway kind cluster, installs Peat, makes a database, writes and reads it with `psql`, puts it to sleep and wakes it by connecting, then prints how to connect yourself and how to delete the cluster (`kind delete cluster --name peat-try`).

On a real cluster: [Install on a real cluster](control/README.md#install-on-a-real-cluster).

## More

- [`control/README.md`](control/README.md): the operator, databases, upgrades, the `peat` command, and installing on a real cluster.
- [`FORK.md`](FORK.md): how Peat relates to Neon: what it is based on, what it changed, what has been tested.
- [`NEON-README.md`](NEON-README.md): Neon's own README, with its build and local-run instructions.

## Licence

Neon is licensed under the Apache License 2.0 (`LICENSE`); its `NOTICE` file is kept unchanged. The PostgreSQL trees in `vendor/` are under the PostgreSQL licence (`vendor/postgres-vX/COPYRIGHT`). Peat's changes are under the same licences. Peat is not affiliated with or endorsed by Neon or Databricks.
