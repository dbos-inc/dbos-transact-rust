# Developing DBOS Transact for Rust

Engineering notes for working on this repository: the toolchain, the gates CI enforces, and how
releases are cut. For what the library *does*, start with the [README](./README.md) and the
[documentation](https://docs.dbos.dev/).

## Setting up

Install Rust via [rustup](https://rustup.rs/). The toolchain is described by
[`rust-toolchain.toml`](./rust-toolchain.toml), which tracks `stable` and pulls in the `rustfmt`
and `clippy` components the format and lint gates need, so a plain `cargo build` in this
directory installs whatever is missing.

The MSRV is **latest stable minus two**, rolling, recorded as `rust-version` in the workspace
manifest and enforced by the `msrv` CI job. It is deliberately not pinned in
`rust-toolchain.toml`: a pin there would fight the rolling policy rather than express it. When
stable moves, bump `rust-version` and the toolchain named in the `msrv` job together.

**Docker is required to run the tests.** The suite starts real databases with
[testcontainers](https://docs.rs/testcontainers) rather than mocking the system database, so
there is no way to run the integration tests without a working Docker daemon. Everything else
builds without it.

## Build, format, lint

The workspace holds the two published crates, an unpublished test-support crate, and the demo
apps:

```bash
cargo build --workspace
cargo fmt --all              # --check in CI
cargo clippy --all-targets --all-features
cargo doc --no-deps --all-features
```

CI sets `RUSTFLAGS: -D warnings` and `RUSTDOCFLAGS: -D warnings`, so a warning anywhere is a
failed build. The documentation job exists to catch broken intra-doc links, which is how docs go
stale without anyone noticing: rename a type and every `[OldName]` reference quietly points
at nothing.

Dependency licenses and security advisories are checked by
[cargo-deny](https://embarkstudios.github.io/cargo-deny/), configured in
[`deny.toml`](./deny.toml):

```bash
cargo deny check
```

## Features

`dbos` has two features, both on by default:

| Feature | Covers |
| --- | --- |
| `engine` | The durable execution engine: lifecycle, registry, contexts, workflows, steps, events, recovery, queues, scheduler, messaging, client |
| `macros` | Re-exports the attribute macros and `select_step!` from `dbos-macros`. Implies `engine` |

With `engine` off, what remains is the system database and Conductor layers, the surface a future
FFI host would consume. That boundary is a standing CI gate rather than a convention:

```bash
cargo check -p dbos --no-default-features
```

Note the `-p dbos`. Run over the whole workspace, any member asking for `dbos`'s default features
would unify `engine` back on and the gate would pass no matter what had crossed the boundary.

## Tests

```bash
cargo test --all-features                 # everything, against Postgres
cargo test --all-features -- --nocapture  # with test output
cargo test --test queues                  # one test binary
```

Each file under `crates/dbos/tests/` compiles to its own binary, and the first to fail aborts the
rest, so CI passes `--no-fail-fast`. Use it locally too when you want the whole picture rather
than the first failure.

The harness recognises three environment variables:

| Variable | Effect |
| --- | --- |
| `DBOS_TEST_USE_COCKROACH_DB` | Run against CockroachDB instead of Postgres. Both are required backends and CI runs both legs |
| `DBOS_TEST_TIMINGS` | Write per-binary setup costs as JSON lines to the given path |
| `DBOS_TEST_NO_BASELINE_CACHE` | Skip the cached schema baseline and migrate from scratch. The escape hatch for anything that smells like a schema difference |

The CockroachDB leg is worth knowing about even if you rarely run it locally, since it has
already caught integer-width divergences that reading the SQL would not have surfaced. It is also
the slower leg by a wide margin, because its cost is baseline migration per test binary rather
than the queries themselves.

The harness shares one container by reference count and removes it when the last test drops it.
CI asserts that nothing is left behind, filtering on the `dev.dbos.test-harness` label the
harness stamps itself. If a local run dies hard, clean up with:

```bash
docker ps -aq --filter label=dev.dbos.test-harness=true | xargs -r docker rm -f
```

## Depending on an unreleased version

Cargo identifies a git dependency by its resolved commit rather than by the version in its
manifest, so tracking this repository needs no version coordination at all:

```toml
[dependencies]
dbos = { git = "https://github.com/dbos-inc/dbos-transact-rust", branch = "main" }
dbos = { git = "https://github.com/dbos-inc/dbos-transact-rust", tag = "v0.5.0" }
dbos = { git = "https://github.com/dbos-inc/dbos-transact-rust", rev = "e70f7f7" }
```

A branch dependency resolves to the head at first build and pins that commit in `Cargo.lock`, so
builds do not change under you when someone merges. Run `cargo update -p dbos` to advance. An
app that should always test the current tip is a CI job that runs `cargo update -p dbos` before
building, rather than a different manifest.

Cargo finds both `dbos` and `dbos-macros` inside this workspace and resolves the exact-version
pin between them from the same checkout, which makes a git dependency *easier* than the registry,
where the two crates must be published in order.

One hard limit: **a crate published to crates.io cannot have a git dependency**. Applications can
use one freely, but a library that depends on `dbos` and wants to publish itself needs a released
version. That is what release candidates are for.

To develop an app against a local checkout, override the git source with a path in the app's own
manifest. Patch both crates, or the exact-version pin will fail:

```toml
[patch."https://github.com/dbos-inc/dbos-transact-rust"]
dbos = { path = "../dbos-transact-rust/crates/dbos" }
dbos-macros = { path = "../dbos-transact-rust/crates/dbos-macros" }
```

## Release Versioning

DBOS Transact for Rust follows [semver](https://semver.org/) as Cargo implements it. Two crates
are published, `dbos` and `dbos-macros`, and they always share one version: `dbos` depends on
`=<version>` of the macros, because the macros expand into `dbos::__private`, this workspace's
own unstable surface. The pin lives in `[workspace.dependencies]` next to the version it pins to,
and the tooling moves both together.

`main` always carries the **next** release with a `-dev` suffix. After `0.5.0` ships, `main` is
`0.6.0-dev`. Two things follow from that. A git dependency on `main` reads as unreleased rather
than claiming to be a version that shipped, and a stray `cargo publish` from `main` fails the
exact-version pin instead of quietly shipping a release-numbered build.

Cargo will **never** resolve a caret requirement to a prerelease. Someone who writes
`dbos = "0.5"` will not get `0.5.0-rc.1`; they have to ask for it by exact version. That is what
makes release candidates safe to publish.

### Prerequisites

Releases are cut by a person from a clean `main`, using their own crates.io credentials:

```bash
cargo install cargo-release
cargo login   # a crates.io token with the publish scope
```

Publishing rights come from crate ownership. Both crates are owned by the `dbos-eng` GitHub team
alongside individual owners, so any team member can publish. Adding the team is a one-time step
per crate, run by a user owner with a token carrying the `change-owners` scope:

```bash
cargo owner --add github:dbos-inc:dbos-eng dbos
cargo owner --add github:dbos-inc:dbos-eng dbos-macros
cargo owner --list dbos
```

Teams can publish and yank but cannot manage ownership, which is why individual owners stay.

### Creating a release

From a clean `main`:

```bash
scripts/release.sh release --dry-run   # print every step, change nothing
scripts/release.sh release             # 0.5.0-dev -> 0.5.0, then main -> 0.6.0-dev
```

This bumps the workspace version and the exact-version pin, commits, tags `v<version>`, publishes
`dbos-macros`, waits for it to appear on the index, publishes `dbos`, pushes, and then commits
`main` at the next `-dev` version and pushes again.

The order is not optional: `dbos` requires `=<version>` of `dbos-macros` to already be on the
index, so publishing them the other way round fails verification.
[cargo-release](https://github.com/crate-ci/cargo-release), configured in
[`release.toml`](./release.toml), handles the ordering and the index wait. It also refuses to
release from any branch but `main` or from a dirty tree.

**Run the dry run first.** A crates.io publish is permanent. A version can be yanked, which stops
new resolutions from selecting it, but it cannot be deleted or replaced.

### Release candidates

```bash
scripts/release.sh rc     # 0.5.0-dev -> 0.5.0-rc.1, then rc.1 -> rc.2, ...
```

A release candidate publishes to crates.io like any other version and is invisible to anyone who
has not opted in by exact version. Cut one when a library needs to depend on unreleased work, or
when external testers want something more stable than a git branch.

Unlike a final release, an `rc` leaves `main` at the candidate version rather than bumping to
`-dev`. The next `rc` or the `release` that finalises it moves `main` on.

### Patch releases

`cargo release patch` cuts one, but only from a `main` that has not yet moved past the release
being patched. Backporting a fix onto an older minor version needs a release branch, and that
flow is not set up: no release branches exist, and `scripts/release.sh` does not know about them.
Add it when a release actually needs patching rather than in advance.

### Publishing from CI

Releases are triggered by a person today. Moving to tag-triggered publishing is a small change:
set `publish = false` in [`release.toml`](./release.toml) so the script only versions, tags, and
pushes, then add a workflow on `v*` tags that runs `cargo publish --workspace`. crates.io
supports Trusted Publishing for GitHub Actions, which issues a short-lived token and avoids
storing a long-lived secret. It is configured per crate on the crate's settings page by a user
owner.

### After a release

Check the crates.io page for the README and metadata, and that
[docs.rs](https://docs.rs/dbos) built cleanly. docs.rs builds with default features, which for
this crate is everything, so a green `cargo doc --all-features` locally is a good predictor.
