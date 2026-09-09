#!/usr/bin/env bash
# Cut a release. What is allowed depends on the branch you are on.
#
# From `main`:
#
#   scripts/release.sh rc                 # 0.5.0-dev -> 0.5.0-rc.1 (rc.1 -> rc.2, ...)
#   scripts/release.sh release            # 0.5.0-dev | 0.5.0-rc.N -> 0.5.0
#   scripts/release.sh release --dry-run  # print every step, change nothing
#
# From a release branch (`release/v0.5`):
#
#   scripts/release.sh patch              # 0.5.0 -> 0.5.1 (0.5.1 -> 0.5.2, ...)
#
# `cargo release` (configured in release.toml) bumps the workspace version and the `=` pin on
# `dbos-macros`, commits, tags `vX.Y.Z`, publishes `dbos-macros` then `dbos`, and pushes.
#
# A final release from `main` does two more things. It cuts `release/vX.Y` at the release commit,
# which is the branch any later patch to that line is built on, and it moves `main` to
# `X.(Y+1).0-dev` — so a git dependency on `main` always reads as unreleased, and a stray
# `cargo publish` from it fails the `=` pin rather than shipping a release-numbered build.
#
# An `rc` does neither: it leaves `main` at the candidate version, and the next `rc` or `release`
# moves it on. A `patch` does neither either — a release branch carries plain released versions
# with no `-dev` suffix, so the next `patch` bumps straight off the last one.
#
# Requires `cargo install cargo-release` and a `cargo login` as a crates.io owner of both crates.
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/release.sh <level> [--dry-run]

  on main:              rc | release
  on release/vX.Y:      patch
USAGE
  exit 2
}

[ $# -ge 1 ] || usage
level=$1; shift
execute=(--execute --no-confirm)
for a in "$@"; do case "$a" in --dry-run) execute=() ;; *) usage ;; esac; done

cd "$(dirname "$0")/.."

# `cargo release` enforces this too, via `allow-branch` — but it cannot know that `rc` and
# `release` are main's levels while `patch` is a release branch's, and getting that wrong
# publishes a version from the wrong line rather than failing.
branch=$(git rev-parse --abbrev-ref HEAD)
case "$level:$branch" in
  rc:main | release:main) ;;
  patch:release/v*) ;;
  rc:* | release:*)
    echo "error: $level releases are cut from main, not $branch" >&2; exit 1 ;;
  patch:*)
    echo "error: patch releases are cut from a release branch (release/vX.Y), not $branch" >&2
    echo "       to patch 0.5.x:  git switch -c release/v0.5 v0.5.0   # if it does not exist yet" >&2
    exit 1 ;;
  *) usage ;;
esac

version() {
  cargo metadata --no-deps --format-version 1 \
    | python3 -c 'import json,sys; m=json.load(sys.stdin); print(next(p["version"] for p in m["packages"] if p["name"]=="dbos"))'
}

run() {
  if [ ${#execute[@]} -gt 0 ]; then "$@"; else echo "would run: $*"; fi
}

# Both `release` levels strip the prerelease suffix, so this is the version about to be published
# in either mode — including a dry run, where nothing on disk changes.
current=$(version)
released="${current%%-*}"

cargo release "$level" "${execute[@]}"

# Only a final release from main opens a new minor line and retargets main.
[ "$level" = release ] || exit 0

IFS=. read -r major minor _ <<<"$released"

# The release branch is cut here rather than when a patch first needs it, so it always points at
# the released commit itself. Created after the release, so HEAD is the release commit.
release_branch="release/v$major.$minor"
if git show-ref --verify --quiet "refs/heads/$release_branch"; then
  echo "Release branch $release_branch already exists, leaving it alone"
else
  echo "Cutting $release_branch at v$released"
  run git branch "$release_branch"
  run git push origin "$release_branch"
fi

next="$major.$((minor + 1)).0-dev"
echo "Moving main from $released to $next"
cargo release version "$next" "${execute[@]}"
cargo release commit "${execute[@]}"
run git push origin main
