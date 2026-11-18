# Peat

**Peat is a fork of [Neon](https://github.com/neondatabase/neon)'s open-source code**: Neon's storage for Postgres (pageserver, safekeepers, storage broker, storage controller) and the Postgres extension its computes run, kept running on the latest PostgreSQL release. It is forked from Neon's code as the community fork [`jclab-oss/neon`](https://github.com/jclab-oss/neon) carries it, at that fork's pull request 14 (`ae51d78b121`), which sits on the community fork's `main` (`4f53557c4f6`: its author's seven commits on Neon's own `main`, `fa504217c61` of 2026-08-31); its Postgres tree is forked from Neon's [`neondatabase/postgres`](https://github.com/neondatabase/postgres) at `REL_18_STABLE_neon` (`a616eefea76`). Neon's code is under the Apache License 2.0 and its Postgres trees under the PostgreSQL licence; Peat's changes are under the same licences. **Peat is not affiliated with or endorsed by Neon or Databricks**; "Neon" is their name, and the code it names below is theirs.

**Why it exists:** Neon's open repository stopped taking new PostgreSQL releases in 2025 (its newest vendored Postgres is 17.5, from July 2025). Neon's own Postgres repository went further, with `REL_17_STABLE_neon` at 17.8 and `REL_18_STABLE_neon` at 18.2 (both last changed in April 2026), but Neon's code never took them in; the community fork did (its pull requests 11 and 14), and Peat carries that work on to **Postgres 18.7** and is meant to follow later releases. It is new: nothing in it has run in production or as a deployed system, and its own changes are listed below.

**Who maintains and writes it.** Peat is maintained by Zabaca. Its changes are written by AI coding agents (Anthropic's Claude) working for Zabaca; each change is reviewed by a separate reviewing agent before it is merged, and Zabaca's founder, whose git identity the commits carry, decides the project's direction. Every commit of Peat's own says so in a `Co-Authored-By: Claude` trailer. That matters most for its changes to Postgres internals: the three fixes below are small C changes to PostgreSQL's B-tree search, WAL redo helpers and WAL reader, each listed with what was and was not tested.

**Two repositories:** this one (Neon's tree) and [`peat-postgres`](https://github.com/Zabaca/peat-postgres) (Neon's fork of PostgreSQL, with Peat's 18.7 move on branch `REL_18_STABLE_peat`), which `vendor/postgres-v18` points at.

Neon's own README, with its build and local-run instructions, is kept as [NEON-README.md](NEON-README.md); everything in it applies to Peat, with Peat's Postgres 18 in place of Neon's.

## What it is based on

| Piece | Where it comes from |
|---|---|
| Neon's code | `neondatabase/neon` `main` as of 2026-08-31 (`fa504217c61`, still Neon's `main`) |
| The community fork's `main` | [`jclab-oss/neon`](https://github.com/jclab-oss/neon) `main` (`4f53557c4f6`): seven commits of its author's on Neon's `main`, adapting Neon's CI and tests to run without Neon's infrastructure |
| Postgres 17.8 support | `jclab-oss/neon` pull request 11 (`28487c34845`, open, not merged upstream) |
| Postgres 18.2 support | `jclab-oss/neon` pull request 14 (`ae51d78b121`, open, not merged upstream), built on pull request 11; Peat's history starts from its head |
| Postgres 18 tree | Neon's `neondatabase/postgres` branch `REL_18_STABLE_neon` at 18.2 (`a616eefea76`, its last change 2026-04-08); Postgres 17 is Neon's `REL_17_STABLE_neon` at 17.8 (`56692dfb680`, 2026-04-09) |
| **Postgres 18.7** (Peat's own work) | PostgreSQL's `REL_18_7` (`f5c41f6681c`) merged into that branch, plus three fixes; in `peat-postgres`, branch `REL_18_STABLE_peat` |

Every commit above keeps its original author and hash; the history of both upstream repositories and of the community fork is intact.

## What Peat changed

**In the Postgres 18 tree** (`peat-postgres`, branch `REL_18_STABLE_peat`):

1. `f01e2df068f` **Merge PostgreSQL 18.7 into Neon's 18 branch.** 806 upstream commits (18.3 to 18.7). Ten files collided in 22 places; each was resolved keeping Neon's own WAL record formats. The one notable choice: 18.7 registers the visibility-map page in heap WAL records (upstream `f581fa729d8`); Neon writes its own heap records (`RM_NEON_ID`) whose format is unchanged here, so that registration is left out of them, and the pageserver keeps clearing visibility-map bits itself, as it did on 18.2.
2. `a9fd4c1bd18` **nbtree: pass Neon's parent argument in 18.7's new empty-index retry.** A build failure: 18.7 calls `_bt_get_endpoint()` with three arguments where Neon's tree has four. *Tested:* the build and Neon's regression suite on 18.7, which includes the upstream pg_regress and isolation suites. *Not tested:* the race this path exists for; no test reaches it yet (below).
3. `35611b70bb7` **xlogutils: skip the init-fork flush in Neon's WAL redo process.** 18.7 flushes an unlogged sequence's init fork during redo; Neon's WAL redo process has nowhere to flush it to and died, so basebackups failed. *Tested:* `test_unlogged` and the four `test_pg_regress` variants failed before it and pass after it; `test_unlogged_init_fork_on_replica` (added by Peat) checks that a read replica replays an unlogged relation's init fork and keeps serving: it passed on 17.8 and **failed on 18.7**, a second defect, fixed in this repository (below).
4. `9b226aa9479` **xlogreader: forget the queued error of a check skipped on request.** `pg_waldump --ignore` stopped after one record on a safekeeper's WAL segment. The defect was already in Neon's 18.2 tree; 18.7's WAL layout exposed it. *Tested:* `test_pg_waldump` failed before it and passes after it (the skipped page checks); `test_pg_waldump_ignore_skips_corrupt_record` (added by Peat) flips a byte inside a record of a copied WAL segment and checks that `pg_waldump --ignore` still lists every record after it (the skipped-record path); it fails on Neon's 17.8 tree, which lacks this fix. After a skipped record, `--ignore` resumes at the next page, so the rest of that page is not listed: Neon's behaviour, unchanged here.

Each commit message says what it fixes and how it was found.

**In this repository:** `vendor/postgres-v18` points at the tree above (its URL is the relative `../peat-postgres.git`, the way upstream Neon points at its own postgres repository), `vendor/revisions.json` says 18.7, Neon's storage manager lets a read replica replay an unlogged relation's init fork on 18 (`pgxn/neon/pagestore_smgr.c`, below), the CI workflows are set for GitHub's hosted runners (`.github/workflows/peat-ci.yml`), and this README (Neon's is kept as `NEON-README.md`). Nothing else on the Neon side was needed for 18.7.

Files Peat changed are the ones in the commits listed here; `git log fa504217c61..` shows every change since Neon's `main` (the community fork's and Peat's), and `git log ae51d78b121..` Peat's alone. Peat also replaced Neon's process files that would misdirect a contributor here (`CONTRIBUTING.md`, the pull request template; `CODEOWNERS`, the issue templates and the CI helpers of the removed workflows deleted) and trimmed Neon's README (`NEON-README.md`).

## A known defect, found and corrected before publishing

The first repair of the build failure (item 2 above, in its first form) passed `NULL` for Neon's parent argument. That compiled and passed the test suite, but left the scan's `parent` variable without a value on 18.7's new path: under `SERIALIZABLE` isolation, an index-only scan with no usable keys on an empty B-tree index that races the index's first insert could start its prefetch from an arbitrary block number. Review found it; the published commit (`a9fd4c1bd18`) passes `&parent` and initialises the variable. **No test reaches this path yet; one could be written** by holding a backend inside the race window: with PostgreSQL's injection points (which Neon's build does not enable) or with a debugger, as Neon's suite already does elsewhere. It is a to-do. On the defective code the outcome depended on an uninitialised value, so such a test could not have failed reliably on it; upstream added no test for the race either.

## A defect found after the port, fixed: a read replica and an unlogged relation on 18.7

Found by Peat's replica test before publishing and published as a known defect; fixed in Peat's first pull request.

**What triggered it.** On 18.7 a read replica went down when the primary created or rewrote an unlogged sequence (most `ALTER SEQUENCE` options, `TRUNCATE ... RESTART IDENTITY`), including the sequence behind a `serial` or identity column of an unlogged table, or created a hash index on an unlogged table. Run, by the test, each failing on 18.7 before the fix: `CREATE UNLOGGED SEQUENCE`, `CREATE UNLOGGED TABLE ... (id serial)`, and `CREATE INDEX ... USING hash` on an unlogged table. The replica's startup process stopped with `could not open file "base/N/M_init": No such file or directory` and the replica shut down. Only hot-standby read replicas replay the primary's WAL, so only they were affected; no data was lost on the primary.

**Why.** PostgreSQL 18.7 (upstream `62760571307`) makes sequence redo flush the sequence's init-fork page at once, and the hash index's init-page redo goes through the same helper. On a Neon replica, redo of an init-fork page the replica has not cached gets a zeroed buffer back, and redo's relation creation (opened as permanent) makes no local file. Since Postgres 18, Neon's buffer flush sends every init-fork buffer to the relation as unlogged, that is to a local `_init` file, which did not exist. On 17, the flush opened the relation with unknown persistence, found no local file and took Neon's own write path instead.

**The fix** (`pgxn/neon/pagestore_smgr.c`, `neon_writev`): on 18, an unlogged write to an init fork during recovery, with no local `_init` file, takes the path 17 took. Nothing else changes: the primary, and a replica that has the local file, write it locally as before.

**Tested:** `test_unlogged_init_fork_on_replica` covers an unlogged sequence, an unlogged table's `serial` column and a hash index on an unlogged table, each followed by a replica restart and one more such change replayed after it. On an arm64 Linux machine, release build: before the fix its three cases failed on 18.7 with the error above and passed on 17.8; after it they pass on both. The replica, standby and pg_regress test files (`test_unlogged`, `test_hot_standby`, `test_physical_replication`, `test_physical_and_logical_replicaiton`, `test_replica_promotes`, `test_replica_start`, `test_gin_redo`, `test_pg_regress`: 50 tests) pass on 18.7 and 17.8 with the fix. *Not tested:* identity columns and the other `ALTER SEQUENCE` forms (they reach the same redo); a replica promoted after replaying such a change, beyond what `test_replica_promotes` covers.

## What has been tested

On an arm64 Linux machine, release build, Postgres 18.7, this tree:

- The build: clean, no compiler warning.
- Neon's Rust tests: all pass (614 in the workspace without the pageserver, 285 in the pageserver, 15 doctests).
- Neon's Python regression suite, run the way the CI runs it (eight shards and the solo tests, each shard on 4 CPUs and 16 GiB with its test output on a filesystem of its own): 727 of the 830 tests passed and 100 were skipped: 66 that Neon marks to run only on its default Postgres (17), so they never run on 18, and 34 that are skipped on every version (some for the environment: no compatibility snapshot of a previous release, no gdb, a proxy built without one feature; others by Neon's own markers for known issues). Three failed: the two tests that need Docker (absent on that machine) and one storage controller test that passed when rerun alone, on Postgres 18 and on Postgres 17 from the same build. Each shard took 5 to 6 minutes on that machine's cores; its test output peaked at 6.7 GB, the solo tests at about 12 GB each.
- Before this tree, the same port with the earlier form of the fixes passed the suite with no failure that Postgres 17.8 from the same tree lacks.
- Not tested: amd64, debug and sanitizer builds, the compute image with its extensions, PostgreSQL 19, and any of the CI on GitHub itself (it runs once the repository is published).

## Continuous integration

All of it runs on GitHub's standard hosted runners, free for public repositories (arm64: `ubuntu-24.04-arm`, 4 vCPU, 16 GB of memory, 14 GB of disk); no job needs a machine of the maintainers'. The workflow is `.github/workflows/peat-ci.yml`, its helpers are in `.github/peat/`.

- **Every pull request and every push to `main`:** the release build (Neon and Postgres 14 to 18) and Neon's Rust tests.
- **Every merge to `main`, every `release-*` tag and manual runs:** also Neon's Python regression suite on Postgres 18, split by `.github/peat/shard.py` into eight shards balanced by measured test times (`.github/peat/test-durations.json`), plus a job of its own for the two tests that each write about 12 GB (`.github/peat/solo-tests.txt`), each on its own runner with four test workers and its test output on a filesystem of its own (the pageserver starts evicting, and the test fails its log check, when its filesystem is over 80% full).
- **Images** (Peat's, under Peat's name) are built and published only from this repository's own merges to `main` and release tags, and only once the `PUBLISH_IMAGES` repository variable is `true`.

Neon's upstream workflows were removed: they need Neon's own infrastructure (self-hosted runner labels, AWS and Azure accounts, Neon's cloud, a private test-data bucket), and two of them ran on `pull_request_target`. They remain in the history.

**For contributors:** pull requests from other repositories run on GitHub's machines with a read-only token and no secrets; the workflow token has no permission unless a job asks for one, and only the image job, which never runs for a pull request, can write. The workflow runs of every pull request from outside wait for a maintainer's approval before they start (a repository setting).

**Known external dependency:** Neon's proxy tests resolve names under `local.neon.build`, a public DNS name of Neon's that points at `127.0.0.1`; without DNS (or if that name goes away) those tests fail.

## Licence

Neon is licensed under the Apache License 2.0 (`LICENSE`); its `NOTICE` file is kept unchanged. The PostgreSQL trees in `vendor/` are under the PostgreSQL licence (`vendor/postgres-vX/COPYRIGHT`). Peat's changes are under the same licences.
