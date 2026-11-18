# Contributing to Peat

Peat is a fork of Neon's open-source code, kept on the latest PostgreSQL release (see [README.md](README.md)). It is maintained by Zabaca.

## How changes are made here

- **Who writes them.** Most changes are written by AI coding agents (Anthropic's Claude) working for Zabaca, and carry a `Co-Authored-By: Claude` trailer. A separate reviewing agent reads every change before it is merged, and a person at Zabaca decides the project's direction and what is merged.
- **One change, one branch, one pull request,** with what it fixes, how it was found and how it was tested in its commit message. A change to PostgreSQL itself goes to [`peat-postgres`](https://github.com/Zabaca/peat-postgres) (branch `REL_18_STABLE_peat`), with the matching submodule move here.
- **Tests.** Every pull request runs the release build and Neon's Rust tests; every merge to `main` also runs Neon's regression suite on Postgres 18 (`.github/workflows/peat-ci.yml`). A change does not merge with a failing Rust test, or with a regression test that fails on 18 and passes on the previous major from the same build.
- **Reruns** (below).
- **Style.** As upstream: `cargo fmt` and `cargo clippy` for Rust (`./run_clippy.sh`, `./scripts/reformat`), the Python checks in [docs/sourcetree.md](docs/sourcetree.md#obligatory-checks) for the test suite. `pre-commit.py` is a sample pre-commit hook (`ln -s ../../pre-commit.py .git/hooks/pre-commit`).

## Reruns

The regression suite reruns a failed test once, so a test that fails on a shared four-CPU runner for timing alone does not fail its shard. A rerun is not a pass: it is a failure that did not repeat. Every rerun is listed in the job's summary and as a warning on the run (`.github/peat/peat_reruns.py`, `.github/peat/report_reruns.py`), with the first attempt's error and logs kept in the shard's artifact (`reruns.jsonl`, `rerun-first-attempts.tar.gz`), whether the rerun passed or not.

What it means here:

- **A test that fails twice** fails its shard, and the change does not merge.
- **A test that passed only on its rerun** is read before the change merges. If its first error could come from the change, or the test touches what the change touches, it is run again on 18 and on the previous major from the same build (as the port's own gate was), and it counts as a failure if it fails on 18 only.
- **A test that keeps being rerun** across runs, on changes that do not touch it, gets a follow-up: an allowance with its reason, a fix, or a note in the README, rather than reruns forever.
- **A job rerun by hand** (GitHub's "Re-run failed jobs") is another attempt, and is read like a rerun: it is done only by a maintainer (someone with write access to this repository, including the agents working for Zabaca), only after the failed attempt has been read and its failing tests judged not to come from the change, at most once per run, and it is listed in the pull request with the tests that failed and why they are not the change's. The failed attempt's summary, warnings and artifacts stay on the run. If the same test fails again in the hand rerun, the change does not merge.

## Pull requests from outside

Welcome. They run on GitHub's own hosted runners with a read-only token and no secrets; a maintainer approves the workflow run of a pull request from outside before it starts. Contributions are accepted under the repository's licences: Apache-2.0 for this tree (`LICENSE`, with Neon's `NOTICE`), the PostgreSQL licence for the Postgres trees.

## Issues

Bugs and questions about Peat go to this repository's issues. Peat is not Neon: please do not report Peat's behaviour to Neon or ask Neon for support with it.
