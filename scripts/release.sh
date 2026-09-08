#!/usr/bin/env bash
# Cut a release from a clean `main` and move `main` on to the next `-dev` version.
#
#   scripts/release.sh rc                 # 0.5.0-dev -> 0.5.0-rc.1 (rc.1 -> rc.2, ...)
#   scripts/release.sh release            # 0.5.0-dev | 0.5.0-rc.N -> 0.5.0, then main -> 0.6.0-dev
#   scripts/release.sh release --dry-run  # print every step, change nothing
#
# `cargo release` (configured in release.toml) bumps the workspace version and the `=` pin on
# `dbos-macros`, commits, tags `vX.Y.Z`, publishes `dbos-macros` then `dbos`, and pushes. A final
# release is followed by a second commit that sets `main` to `X.(Y+1).0-dev`, so a git dependency
# on `main` always reads as unreleased and a stray `cargo publish` from it fails the `=` pin.
# An `rc` leaves `main` at the rc version: the next `rc` or `release` moves it on.
#
# Requires `cargo install cargo-release` and a `cargo login` as a crates.io owner of both crates.
set -euo pipefail

usage() { echo "usage: $0 rc|release [--dry-run]" >&2; exit 2; }
[ $# -ge 1 ] || usage
level=$1; shift
case "$level" in rc|release) ;; *) usage ;; esac
execute=(--execute --no-confirm)
for a in "$@"; do case "$a" in --dry-run) execute=() ;; *) usage ;; esac; done

cd "$(dirname "$0")/.."
cargo release "$level" "${execute[@]}"

[ "$level" = release ] || exit 0

# The version just released is now the workspace version; the next dev version bumps minor.
released=$(cargo metadata --no-deps --format-version 1 \
  | python3 -c 'import json,sys; m=json.load(sys.stdin); print(next(p["version"] for p in m["packages"] if p["name"]=="dbos"))')
IFS=. read -r major minor _ <<<"$released"
next="$major.$((minor + 1)).0-dev"
echo "Moving main from $released to $next"
cargo release version "$next" "${execute[@]}"
cargo release commit "${execute[@]}"
if [ ${#execute[@]} -gt 0 ]; then git push origin main; else echo "would run: git push origin main"; fi
