# Contributing to Peat

Peat is a fork of Neon's open-source code, kept on the latest PostgreSQL release (see [README.md](README.md)). It is maintained by Zabaca.

## How changes are made here

- **Who writes them.** Most changes are written by AI coding agents (Anthropic's Claude) working for Zabaca, and carry a `Co-Authored-By: Claude` trailer. A separate reviewing agent reads every change before it is merged, and a person at Zabaca decides the project's direction and what is merged.
- **One change, one branch, one pull request,** with what it fixes, how it was found and how it was tested in its commit message. A change to PostgreSQL itself goes to [`peat-postgres`](https://github.com/Zabaca/peat-postgres) (branch `REL_18_STABLE_peat`), with the matching submodule move here.
- **Tests.** Every pull request runs the release build and Neon's Rust tests; every merge to `main` also runs Neon's regression suite on Postgres 18 (`.github/workflows/peat-ci.yml`). A change does not merge with a failing Rust test, or with a regression test that fails on 18 and passes on the previous major from the same build.
- **Style.** As upstream: `cargo fmt` and `cargo clippy` for Rust (`./run_clippy.sh`, `./scripts/reformat`), the Python checks in [docs/sourcetree.md](docs/sourcetree.md#obligatory-checks) for the test suite. `pre-commit.py` is a sample pre-commit hook (`ln -s ../../pre-commit.py .git/hooks/pre-commit`).

## Pull requests from outside

Welcome. They run on GitHub's own hosted runners with a read-only token and no secrets; a maintainer approves the workflow run of a pull request from outside before it starts. Contributions are accepted under the repository's licences: Apache-2.0 for this tree (`LICENSE`, with Neon's `NOTICE`), the PostgreSQL licence for the Postgres trees.

## Issues

Bugs and questions about Peat go to this repository's issues. Peat is not Neon: please do not report Peat's behaviour to Neon or ask Neon for support with it.
