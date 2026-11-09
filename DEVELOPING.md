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

`dbos` has three default features:

| Feature | Covers |
| --- | --- |
| `engine` | The durable execution engine: lifecycle, registry, contexts, workflows, steps, events, recovery, queues, scheduler, messaging, client |
| `macros` | Re-exports the attribute macros and `select_step!` from `dbos-macros`. Implies `engine` |
| `runtime-tokio` | The async runtime the crate spawns tasks and sets timers on. A runtime is required, with or without `engine` |

`runtime-smol` and `runtime-async-global-executor` are the off-by-default alternatives. Several
may be enabled at once: tokio is used when the caller is in a tokio runtime, and otherwise
`async-global-executor`, then smol.

Spawning and timers go through `crate::rt`, never tokio directly. `crates/dbos/clippy.toml` lists
what that rules out, and `cargo clippy` enforces it for the library (the integration tests are
exempt).

With `engine` off, what remains is the system database and Conductor layers, the surface a future
FFI host would consume. That boundary is a standing CI gate rather than a convention:

```bash
cargo check -p dbos --no-default-features --features runtime-tokio
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

The harness recognises four environment variables:

| Variable | Effect |
| --- | --- |
| `DBOS_TEST_USE_COCKROACH_DB` | Run against CockroachDB instead of Postgres. Both are required backends and CI runs both legs |
| `DBOS_TEST_RUNTIME` | Run `#[dbos_test_support::test]` tests on `tokio` (the default), `smol` or `async-global-executor`. See below |
| `DBOS_TEST_TIMINGS` | Write per-binary setup costs as JSON lines to the given path |
| `DBOS_TEST_NO_BASELINE_CACHE` | Skip the cached schema baseline and migrate from scratch. The escape hatch for anything that smells like a schema difference |

The CockroachDB leg is worth knowing about even if you rarely run it locally, since it has
already caught integer-width divergences that reading the SQL would not have surfaced. It is also
the slower leg by a wide margin, because its cost is baseline migration per test binary rather
than the queries themselves.

### Other runtimes

Most tests use `#[tokio::test]` and run on tokio only. The binaries that exercise spawning and
timers hardest use `#[dbos_test_support::test]` instead, and CI also runs them, against Postgres
only, on each non-tokio runtime. The list is in the `test-runtimes` job of `ci.yml`. To run one
locally, build with that runtime's feature and name it:

```bash
DBOS_TEST_RUNTIME=smol SMOL_THREADS=4 cargo test -p dbos --features runtime-smol --test shutdown
```

`SMOL_THREADS` because smol's executor otherwise runs on a single thread, which hides the
concurrency the other runtimes have.

The feature is what makes `dbos` use smol outside tokio; the variable takes the test outside
tokio. They must agree, and a test fails at once if they do not. In those binaries, spawn and
sleep through `dbos::__rt` rather than tokio, which would panic off tokio; each denies clippy's
`disallowed_methods` and `disallowed_types`, so `cargo clippy` enforces it. `tokio::sync`,
`select!` and `join!` are fine anywhere.

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

Releases are cut by a person from a clean `main`, and published by CI:

```bash
cargo install cargo-release
```

You need the repository's **admin** role, for two reasons. `main` and the `release/v*` branches
only accept pull requests, and admins are the only ones their ruleset lets push a release's version
commits. And the "Release tags" ruleset lets only admins create, move or delete a `v*` tag, since
pushing one is what publishes. You don't need a crates.io token: the `Release` workflow publishes
with [Trusted Publishing](#publishing-from-ci).

### Creating a release

From a clean `main`:

```bash
cargo xtask release --dry-run   # print every step, push nothing
cargo xtask release             # 0.5.0-dev -> 0.5.0, then main -> 0.6.0-dev
```

It does all of this, in order. There are no manual git steps:

1. Bumps the workspace version and the exact-version pin, from `0.5.0-dev` to `0.5.0`.
2. Commits that as `Version 0.5.0`, and tags it `v0.5.0`.
3. Pushes the commit to `main` and the tag, in one atomic push.
4. **Creates the branch `release/v0.5`** at the release commit and pushes it. Patches to the
   `0.5` line are cut from there, so the branch is made now rather than when it is first needed.
5. Commits `main` at `0.6.0-dev` and pushes again.

Steps 4 and 5 run only for a final release. An `rc` stops after step 3, and so does a `patch` on
a release branch.

The tag starts the [`Release` workflow](.github/workflows/release.yml), which publishes. It:

1. Checks that the tag names the version in the manifests, and that the tagged commit is on
   `main` or a `release/v*` branch.
2. Builds the docs the way docs.rs will, on the latest nightly.
3. Packages and verifies both crates, then publishes `dbos-macros`, waits for it to reach the
   index, and publishes `dbos`.
4. Drafts a GitHub release for the tag.

Watch it in the Actions tab. If it fails, what to do depends on where:

- **Before anything is uploaded** (the tag check, the docs build, or verifying the crates). If the
  cause is outside the code, such as Trusted Publishing not yet set up or a crates.io outage, fix
  that and use "Re-run failed jobs". If the code needs a change, the version is spent, because its
  tag and commit are already pushed: fix it on `main`, cherry-pick it onto the release branch and
  cut a [patch](#patch-releases) (for an `rc`, cut the next `rc` from `main`).
- **Between the two crates**, `dbos-macros` is on crates.io and `dbos` isn't. Re-running would try
  to publish `dbos-macros` again, which crates.io refuses, so publish `dbos` by hand from the tag
  instead, as in [Publishing from CI](#publishing-from-ci), and then draft the GitHub release.
- **Drafting the GitHub release**, after both crates are up. It is a job of its own, so "Re-run
  failed jobs" retries only that, or draft the release by hand.

If the xtask itself stops after the tag is pushed, while cutting the release branch or moving
`main` to the next `-dev` version, the release is already out. Finish those steps by hand:
`git push origin HEAD:refs/heads/release/v0.5` from the release commit if the branch is missing,
then `cargo release version 0.6.0-dev --execute`, commit it as `Version 0.6.0-dev`, and push
`main`.

The xtask drives cargo-release's per-step subcommands — `version`, `commit`, `tag` — rather than
the all-in-one `cargo release <level>`, and the reason is step 1. cargo-release checks dependency
requirements against the manifests as they are **on disk**, never against the versions it is
planning, so with the bump unapplied it reads `dbos`'s pin as `=0.5.0-dev`, finds that nothing it
is about to release satisfies it and that no such version was ever published, and aborts with
`dbos 0.5.0 depends on unpublished workspace package dbos-macros 0.5.0`. Bumping and committing
first leaves the pin naming the `dbos-macros` version that is about to go up, which is what the
check is actually asking about.

It pushes the commit and the tag atomically, not with `cargo release push`, because the tag is
what publishes. If `main` refused the commit while the tag went through, the workflow would
publish a commit that isn't on any branch. And it pushes before anything is published, so a
refused push costs nothing: delete the local tag (`git tag -d v0.5.0`) and reset the branch you
released from (`main`, or the release branch for a `patch`) to its `origin` counterpart.

The publish order is not optional: `dbos` requires `=<version>` of `dbos-macros` to already be on
the index, so publishing them the other way round fails verification. The workflow verifies both
with `cargo publish --dry-run -p dbos-macros -p dbos` in a job that can't obtain a publish token,
since verifying runs dependencies' build scripts and proc macros. The publish job compiles nothing:
it uploads them in one `cargo publish --no-verify -p dbos-macros -p dbos`, which orders them and
waits for the index.
[cargo-release](https://github.com/crate-ci/cargo-release), configured in
[`release.toml`](./release.toml), refuses to release from a dirty tree, or from any branch but
`main` and `release/v*`.

**Run the dry run first.** A crates.io publish is permanent. A version can be yanked, which stops
new resolutions from selecting it, but it cannot be deleted or replaced.

The dry run does the local half for real — the bump, its commit and the tag — and undoes all of it
before returning. On that bumped commit it runs `cargo publish --dry-run -p dbos-macros -p dbos`,
the same packaging and verification the workflow runs before it uploads. Only the push is
simulated. The one workflow check it doesn't repeat is the docs build on the latest nightly, which
the `Documentation as docs.rs builds it` CI job approximates on a dated one. It works that way because a dry run that left the manifests alone would report the
unsatisfiable `-dev` pin and package crates still carrying the `-dev` version, so it would never
once look at the artifacts a release would upload. The tag is cut because a tag name already taken
is exactly what a dry run is for.

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

Pushing the cherry-pick and running `cargo xtask patch` both push straight to the release branch,
which needs the admin role, as a release from `main` does.

`release/v0.5` predates the Release workflow: its xtask still publishes from your machine, so
patching `0.5` needs a `cargo login` as an owner of both crates. Every line from `0.6` on publishes
from CI.

Fix on `main` first, then cherry-pick onto the release branch. A fix that lands only on the
release branch is a fix that comes back as a regression in the next minor.

A release branch carries plain released versions with no `-dev` suffix, and `patch` bumps
straight off the last one. Nothing is merged back to `main`: the branch exists to hold the
released line, and the fix is already on `main` by way of the cherry-pick's source.

Older lines stay patchable indefinitely, since each has its own branch. Patching `0.4.2` after
`0.6.0` has shipped means checking out `release/v0.4` and running the same command.

### Publishing from CI

The [`Release` workflow](.github/workflows/release.yml) runs on every `v*` tag. It authenticates
to crates.io with [Trusted Publishing](https://crates.io/docs/trusted-publishing): GitHub issues
the workflow a short-lived OIDC token, and crates.io exchanges it for a publish token, so no
crates.io token is stored in the repository or on anyone's machine.

crates.io checks the repository, the workflow file and the GitHub environment a run claims, but
not the branch or tag it ran on. So the environment is what limits publishing to release tags: the
publish job runs in the `crates-io` environment, whose deployment policy admits only `v*` tags, and
only admins can push those. Without it, anyone with write access could push a branch with an edited
copy of `release.yml` and publish from it.

That needs a one-time setup, by an admin of this repository and a user who owns the crates:

1. In this repository's Settings → Environments, create `crates-io`. Under Deployment branches and
   tags, choose "Selected branches and tags" and add the tag rule `v*`, and nothing else.
2. On crates.io, for **each** of `dbos` and `dbos-macros`, open the crate's Settings, add a
   trusted publisher for GitHub, and enter:
   - Repository owner: `dbos-inc`
   - Repository name: `dbos-transact-rust`
   - Workflow filename: `release.yml`
   - Environment: `crates-io`

Until that is done, the workflow's publish job fails, and nothing is published. To publish by hand instead, from the tagged commit:

```bash
git checkout v0.5.0
cargo publish -p dbos-macros -p dbos   # needs `cargo login` as an owner of both crates
cargo publish -p dbos                  # if dbos-macros is already up
```

### After a release

1. **Watch the `Release` workflow** in the Actions tab until it finishes.
2. **Publish the GitHub release.** The workflow leaves a draft for the tag, marked as a
   pre-release for an `rc`: write the release notes into it and publish it.
3. **Check crates.io and docs.rs.** The crate pages should show the README and metadata. docs.rs
   builds each published version on its own, usually within minutes; check that
   [docs.rs/dbos](https://docs.rs/dbos) shows the new version and its feature badges, and that its
   build log is clean. It builds with the features in `[package.metadata.docs.rs]`, which is what
   the workflow's nightly docs build checked.
4. **Close the GitHub milestone** for the version, and open one for the next.
5. **Update what depends on the release**: the demo apps in
   [dbos-demo-apps](https://github.com/dbos-inc/dbos-demo-apps) depend on the published crate, so
   move them to the new version.
