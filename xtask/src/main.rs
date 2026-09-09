//! Cut a release. What is allowed depends on the branch you are on.
//!
//! From `main`:
//!
//!     cargo xtask rc                 # 0.5.0-dev -> 0.5.0-rc.1 (rc.1 -> rc.2, ...)
//!     cargo xtask release            # 0.5.0-dev | 0.5.0-rc.N -> 0.5.0
//!     cargo xtask release --dry-run  # print every step, publish nothing
//!
//! From a release branch (`release/v0.5`):
//!
//!     cargo xtask patch              # 0.5.0 -> 0.5.1 (0.5.1 -> 0.5.2, ...)
//!
//! `cargo release` (configured in `release.toml`) bumps the workspace version and the `=` pin on
//! `dbos-macros`, commits, tags `vX.Y.Z`, publishes `dbos-macros` then `dbos`, and pushes. This
//! drives its per-step subcommands rather than the all-in-one `cargo release <level>`, for the
//! reason spelled out on [`release_steps`].
//!
//! A final release from `main` does two more things. It cuts `release/vX.Y` at the release commit,
//! which is the branch any later patch to that line is built on, and it moves `main` to
//! `X.(Y+1).0-dev` — so a git dependency on `main` always reads as unreleased, and a stray
//! `cargo publish` from it fails the `=` pin rather than shipping a release-numbered build.
//!
//! An `rc` does neither: it leaves `main` at the candidate version, and the next `rc` or `release`
//! moves it on. A `patch` does neither either — a release branch carries plain released versions
//! with no `-dev` suffix, so the next `patch` bumps straight off the last one.
//!
//! Requires `cargo install cargo-release` and a `cargo login` as a crates.io owner of both crates.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const USAGE: &str = "\
usage: cargo xtask <level> [--dry-run]

  on main:              rc | release
  on release/vX.Y:      patch
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Usage) => {
            eprint!("{USAGE}");
            ExitCode::from(2)
        }
        Err(Failure::Message(message)) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<()> {
    let (level, dry_run) = parse(args)?;
    let root = workspace_root();

    check_branch(&root, level)?;
    // Every step below assumes it, and the dry run's restore is only exact because of it.
    ensure_clean(&root)?;

    // Both `release` levels strip the prerelease suffix, so this is the version about to be
    // published in either mode — including a dry run, where nothing survives on disk.
    let current = current_version(&root)?;
    let released = current.split('-').next().unwrap_or(&current).to_owned();

    if dry_run {
        dry_run_release(&root, level, &released)
    } else {
        execute_release(&root, level, &released)
    }
}

/// The release itself, in the order `cargo release <level>` would run it.
///
/// `run` applies from `publish` onward: the version bump and its commit always happen for real,
/// because a dry run is worthless without them — see [`dry_run_release`].
///
/// The version bump is applied and committed *before* `publish`, rather than left to the
/// all-in-one command, and that split is the whole reason these are separate steps. cargo-release
/// checks every dependency requirement against the manifests as they are **on disk**, never
/// against the versions it is planning — so with the bump unapplied it reads `dbos`'s pin as
/// `=<version>-dev`, finds that no package it is about to publish satisfies it and that no such
/// version was ever published, and aborts. Bumping first leaves the pin naming the `dbos-macros`
/// version that is about to go up, which is what the check is actually asking about.
fn release_steps(root: &Path, level: Level, run: Run) -> Result<()> {
    cargo_release(root, &["version", level.as_str()], Run::Execute)?;
    cargo_release(root, &["commit"], Run::Execute)?;
    cargo_release(root, &["publish"], run)?;
    cargo_release(root, &["tag"], run)?;
    cargo_release(root, &["push"], run)
}

fn execute_release(root: &Path, level: Level, released: &str) -> Result<()> {
    release_steps(root, level, Run::Execute)?;

    // Only a final release from main opens a new minor line and retargets main.
    if level != Level::Release {
        return Ok(());
    }
    post_release(root, released, Run::Execute)
}

/// A dry run of the same thing, which has to bump and commit for real to be worth anything.
///
/// cargo-release's own dry run leaves the manifests alone, so on this workspace it reports the
/// unsatisfiable `-dev` pin described on [`release_steps`] and packages crates still carrying the
/// `-dev` version — it never sees the artifacts a release would actually upload. Applying the bump
/// in a scratch commit gets a real answer out of it. The commit is local, never pushed, and reset
/// away before this returns; the tree was verified clean above, so the reset restores exactly the
/// state we started from, and the commit stays in the reflog either way.
fn dry_run_release(root: &Path, level: Level, released: &str) -> Result<()> {
    let head = capture(root, "git", &["rev-parse", "HEAD"])?;
    println!(
        "Dry run: bumping the version in a scratch commit, so the publish check and the packaged \
         crates are the real ones. Reset away before this returns; nothing is pushed."
    );

    let outcome = release_steps(root, level, Run::DryRun);
    // Unconditional, and before the `?`: whatever happened above, the commit is scratch.
    sh(root, "git", &["reset", "--hard", &head])?;
    outcome?;

    if level != Level::Release {
        return Ok(());
    }
    post_release(root, released, Run::DryRun)
}

/// Cut the release branch for the new minor line, then move `main` on to the next `-dev`.
///
/// The release branch is cut here rather than when a patch first needs it, so it always points at
/// the released commit itself — which is where HEAD is, the release having just happened.
fn post_release(root: &Path, released: &str, run: Run) -> Result<()> {
    let (major, minor) = major_minor(released)?;

    let branch = format!("release/v{major}.{minor}");
    if branch_exists(root, &branch)? {
        println!("Release branch {branch} already exists, leaving it alone");
    } else {
        println!("Cutting {branch} at v{released}");
        git(root, &["branch", &branch], run)?;
        git(root, &["push", "origin", &branch], run)?;
    }

    let next = format!("{major}.{}.0-dev", minor + 1);
    println!("Moving main from {released} to {next}");
    cargo_release(root, &["version", &next], run)?;
    cargo_release(root, &["commit"], run)?;
    git(root, &["push", "origin", "main"], run)
}

/// `cargo release` enforces the branch too, via `allow-branch` — but it cannot know that `rc` and
/// `release` are main's levels while `patch` is a release branch's, and getting that wrong
/// publishes a version from the wrong line rather than failing.
fn check_branch(root: &Path, level: Level) -> Result<()> {
    let branch = capture(root, "git", &["rev-parse", "--abbrev-ref", "HEAD"])?;
    match level {
        Level::Rc | Level::Release if branch == "main" => Ok(()),
        Level::Patch if branch.starts_with("release/v") => Ok(()),
        Level::Rc | Level::Release => Err(format!(
            "{} releases are cut from main, not {branch}",
            level.as_str()
        )
        .into()),
        Level::Patch => Err(format!(
            "patch releases are cut from a release branch (release/vX.Y), not {branch}\n       \
             to patch 0.5.x:  git switch -c release/v0.5 v0.5.0   # if it does not exist yet"
        )
        .into()),
    }
}

fn current_version(root: &Path) -> Result<String> {
    // `path+file:///…/crates/dbos#0.5.0-dev`, or `…#dbos@0.5.0-dev` when the crate name and the
    // directory name differ. Neither the path nor the name can contain a `#`.
    let id = capture(root, "cargo", &["pkgid", "--package", "dbos"])?;
    let version = id
        .rsplit('#')
        .next()
        .unwrap_or_default()
        .rsplit('@')
        .next()
        .unwrap_or_default();
    if !version.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(format!("could not read a version out of `cargo pkgid`: {id}").into());
    }
    Ok(version.to_owned())
}

fn major_minor(version: &str) -> Result<(u64, u64)> {
    let mut parts = version.split('.');
    let major = parts.next().and_then(|part| part.parse().ok());
    let minor = parts.next().and_then(|part| part.parse().ok());
    match (major, minor) {
        (Some(major), Some(minor)) => Ok((major, minor)),
        _ => Err(format!("could not read a major.minor out of the version {version}").into()),
    }
}

fn ensure_clean(root: &Path) -> Result<()> {
    if capture(root, "git", &["status", "--porcelain"])?.is_empty() {
        return Ok(());
    }
    Err(
        "the working tree has uncommitted changes; commit or discard them first"
            .to_owned()
            .into(),
    )
}

fn branch_exists(root: &Path, branch: &str) -> Result<bool> {
    let refname = format!("refs/heads/{branch}");
    let status = Command::new("git")
        .args(["show-ref", "--verify", "--quiet", &refname])
        .current_dir(root)
        .status()
        .map_err(|err| format!("failed to run git: {err}"))?;
    Ok(status.success())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask sits one level below the workspace root")
        .to_path_buf()
}

/// Whether a step does the thing or only says what it would do.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Run {
    Execute,
    DryRun,
}

/// `cargo release` has a dry run of its own, so it is handed the mode rather than described.
fn cargo_release(root: &Path, args: &[&str], run: Run) -> Result<()> {
    let mut argv = vec!["release"];
    argv.extend_from_slice(args);
    if run == Run::Execute {
        argv.extend_from_slice(&["--execute", "--no-confirm"]);
    }
    sh(root, "cargo", &argv)
}

/// git has no dry run, so a dry run describes the command instead of running it.
fn git(root: &Path, args: &[&str], run: Run) -> Result<()> {
    if run == Run::Execute {
        return sh(root, "git", args);
    }
    println!("would run: git {}", args.join(" "));
    Ok(())
}

fn sh(root: &Path, program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .current_dir(root)
        .status()
        .map_err(|err| format!("failed to run {program}: {err}"))?;
    if !status.success() {
        return Err(format!("{program} {} failed", args.join(" ")).into());
    }
    Ok(())
}

fn capture(root: &Path, program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|err| format!("failed to run {program}: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "{program} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Level {
    Rc,
    Release,
    Patch,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Rc => "rc",
            Level::Release => "release",
            Level::Patch => "patch",
        }
    }
}

fn parse(args: &[String]) -> Result<(Level, bool)> {
    let mut args = args.iter().map(String::as_str);
    let level = match args.next() {
        Some("rc") => Level::Rc,
        Some("release") => Level::Release,
        Some("patch") => Level::Patch,
        _ => return Err(Failure::Usage),
    };
    let mut dry_run = false;
    for arg in args {
        match arg {
            "--dry-run" => dry_run = true,
            _ => return Err(Failure::Usage),
        }
    }
    Ok((level, dry_run))
}

type Result<T> = std::result::Result<T, Failure>;

enum Failure {
    /// Print the usage and exit 2, the way a misused command-line tool does.
    Usage,
    Message(String),
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Failure::Message(message)
    }
}
