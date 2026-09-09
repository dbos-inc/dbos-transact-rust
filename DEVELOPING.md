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

Released lines live on their own branches. `cargo xtask release` creates `release/vX.Y` at
the release commit and pushes it, and patches to that line are made there rather than on `main` —
the same layout the Java and Python SDKs use.

Cargo will **never** resolve a caret requirement to a prerelease. Someone who writes
`dbos = "0.5"` will not get `0.5.0-rc.1`; they have to ask for it by exact version. That is what
makes release candidates safe to publish.

### Prerequisites

Releases are cut by a person from a clean `main`, using their own crates.io credentials:

```bash
cargo install cargo-release
cargo login   # a crates.io token with the publish scope
```

### Creating a release

From a clean `main`:

```bash
cargo xtask release --dry-run   # print every step, publish nothing
cargo xtask release             # 0.5.0-dev -> 0.5.0, then main -> 0.6.0-dev
```

It does all of this, in order. There are no manual git steps:

1. Bumps the workspace version and the exact-version pin, from `0.5.0-dev` to `0.5.0`.
2. Commits that as `Version 0.5.0`.
3. Publishes `dbos-macros`, waits for it to appear on the index, then publishes `dbos`.
4. Tags the release commit `v0.5.0`.
5. Pushes `main` and the tag.
6. **Creates the branch `release/v0.5`** at the release commit and pushes it. Patches to the
   `0.5` line are cut from there, so the branch is made now rather than when it is first needed.
7. Commits `main` at `0.6.0-dev` and pushes again.

Steps 6 and 7 run only for a final release. An `rc` stops after step 5, and so does a `patch` on
a release branch.

Those are cargo-release's per-step subcommands — `version`, `commit`, `publish`, `tag`, `push` —
rather than the all-in-one `cargo release <level>`, and the reason is step 1. cargo-release
checks dependency requirements against the manifests as they are **on disk**, never against the
versions it is planning, so with the bump unapplied it reads `dbos`'s pin as `=0.5.0-dev`, finds
that nothing it is about to publish satisfies it and that no such version was ever published, and
aborts with `dbos 0.5.0 depends on unpublished workspace package dbos-macros 0.5.0`. Bumping and
committing first leaves the pin naming the `dbos-macros` version that is about to go up, which is
what the check is actually asking about.

The publish order is not optional: `dbos` requires `=<version>` of `dbos-macros` to already be on
the index, so publishing them the other way round fails verification.
[cargo-release](https://github.com/crate-ci/cargo-release), configured in
[`release.toml`](./release.toml), handles the ordering and the index wait. It also refuses to
release from a dirty tree, or from any branch but `main` and `release/v*`.

**Run the dry run first.** A crates.io publish is permanent. A version can be yanked, which stops
new resolutions from selecting it, but it cannot be deleted or replaced.

The dry run does the local half for real — steps 1, 2 and 4, the bump, its commit and the tag —
and undoes all of it before returning. Only the two steps that leave your machine, the publish and
the push, are simulated. It works that way for the same reason step 1 does: a dry run that left
the manifests alone would report the unsatisfiable `-dev` pin and package crates still carrying
the `-dev` version, so it would never once look at the artifacts a release would upload. The tag
is cut for a similar reason: `cargo release push` refuses to run without it, and a tag name
already taken is exactly what a dry run is for — a real release only reaches its tag step after
the publish, which cannot be taken back.

All of that happens on a **detached HEAD**, so `main` itself never moves. An interrupted dry run
cannot leave a release commit sitting on a branch for someone to push later; the most it leaves is
a detached HEAD and a local tag, and `git checkout main` plus a `git tag -d v0.5.0` clears both.
On a normal exit the tag is deleted, the commit is reset away — it stays in the reflog — and the
branch is checked out again. The reset is exact because the tree was verified clean before
anything started, which is also why the dry run refuses to start on a dirty tree.

### Release candidates

```bash
cargo xtask rc     # 0.5.0-dev -> 0.5.0-rc.1, then rc.1 -> rc.2, ...
```

A release candidate publishes to crates.io like any other version and is invisible to anyone who
has not opted in by exact version. Cut one when a library needs to depend on unreleased work, or
when external testers want something more stable than a git branch.

Unlike a final release, an `rc` leaves `main` at the candidate version rather than bumping to
`-dev`. The next `rc` or the `release` that finalises it moves `main` on.

### Major releases

Nothing is ever promoted to a new major version automatically. A release from a `main` at
`0.9.0-dev` produces `0.9.0`, and the post-release bump moves `main` to `0.10.0-dev`, not to
`1.0.0-dev`. Crossing to `1.0` is always a deliberate act.

Because the `-dev` version on `main` *declares* what ships next, that is where the decision is
recorded. Retarget `main` in an ordinary reviewed PR:

```bash
cargo release version 1.0.0-dev --execute
```

That rewrites the workspace version and the exact-version pin together. Commit, review, merge.
This is the natural place to land the breaking-change notes, since merging it is the moment the
team agrees the next release is a major one. Then release exactly as usual: `cargo xtask rc`
for candidates, then `cargo xtask release` to ship `1.0.0` and move `main` to `1.1.0-dev`.

`cargo release major` would also get from `0.9.0-dev` to `1.0.0` in one step, but it decides the
bump at release time, on one person's machine, in a command nobody reviews. `cargo xtask`
accepts only `rc` and `release` for that reason. The same applies to `2.0` later: retarget `main`
to `2.0.0-dev` and release.

**Cargo's compatibility rules change at `1.0`.** Below it, every minor bump is breaking, so
`dbos = "0.5"` will not pick up `0.6` and each release is an explicit upgrade for users. From
`1.0` onward, minor and patch releases are compatible: `dbos = "1"` follows every `1.x`
automatically, and only a major bump asks users to do anything. So the post-release `-dev` bump
means something new after `1.0` — `1.1.0-dev` promises the next release is additive. A breaking
change then means retargeting `main` to `2.0.0-dev`, never shipping it in a minor.

### Patch releases

Patches are cut from a release branch, never from `main`. This is forced by the `-dev`
convention: the moment `0.5.0` ships, `main` declares `0.6.0-dev`, so there is no point on `main`
from which `0.5.1` is the next version. `cargo xtask` refuses `patch` anywhere but a
release branch for that reason.

`cargo xtask release` creates `release/vX.Y` and pushes it as part of every final
release, so the branch you need already exists and there is nothing to set up. Check out the one
for the line being patched, put the fix on it, and release:

```bash
git switch release/v0.5
git cherry-pick <sha>          # the fix, already reviewed and merged to main
cargo xtask patch       # 0.5.0 -> 0.5.1, then 0.5.1 -> 0.5.2, ...
```

Fix on `main` first, then cherry-pick onto the release branch. A fix that lands only on the
release branch is a fix that comes back as a regression in the next minor.

A release branch carries plain released versions with no `-dev` suffix, and `patch` bumps
straight off the last one. Nothing is merged back to `main`: the branch exists to hold the
released line, and the fix is already on `main` by way of the cherry-pick's source.

Older lines stay patchable indefinitely, since each has its own branch. Patching `0.4.2` after
`0.6.0` has shipped means checking out `release/v0.4` and running the same command.

### Publishing from CI

Releases are triggered by a person today. Moving to tag-triggered publishing is a small change:
set `publish = false` in [`release.toml`](./release.toml) so the command only versions, tags, and
pushes, then add a workflow on `v*` tags that runs `cargo publish --workspace`. crates.io
supports Trusted Publishing for GitHub Actions, which issues a short-lived token and avoids
storing a long-lived secret. It is configured per crate on the crate's settings page by a user
owner.

### After a release

Check the crates.io page for the README and metadata, and that
[docs.rs](https://docs.rs/dbos) built cleanly. docs.rs builds with default features, which for
this crate is everything, so a green `cargo doc --all-features` locally is a good predictor.
