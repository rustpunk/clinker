#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd -P)
cd -- "$REPO_ROOT"
exec cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- release build-bundle "$@"
