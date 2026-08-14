#!/usr/bin/env bash
# Type-check every combination of the `clinker` capability features.
#
# The default binary has all three, so an ordinary `cargo check` says nothing
# about the other seven builds. Each capability is gated at the call sites of
# one subsystem, and a `#[cfg]` that stops matching produces code that compiles
# in the combination someone happens to build and not in the others. This is
# what notices.
#
# `--all-targets`, so the test and bench targets are held to it too.
set -euo pipefail

cd "$(dirname "$0")/.."

combinations=(
  ""
  "rest"
  "otlp"
  "lineage"
  "rest,otlp"
  "rest,lineage"
  "otlp,lineage"
  "rest,otlp,lineage"
)

rc=0
for features in "${combinations[@]}"; do
  label="${features:-<none>}"
  echo "== clinker features: ${label}"
  if ! cargo check --locked --all-targets -p clinker \
    --no-default-features --features "${features}"; then
    echo "FAIL: clinker does not build with features '${label}'"
    rc=1
    continue
  fi
  # The unit tests only. Each combination has its own diagnostics for the
  # capabilities it lacks, and those assertions are the reason a build without
  # a capability is a supported build rather than a compiling one. The
  # integration suites are the default binary's and run once, elsewhere.
  if cargo test --locked --bins -p clinker \
    --no-default-features --features "${features}"; then
    echo "ok: ${label}"
  else
    echo "FAIL: clinker unit tests fail with features '${label}'"
    rc=1
  fi
done

# `clinker-net` is the crate the transports live in, and its own default is to
# have none of them. Both of its states are built by the loop above only
# indirectly, through whichever `clinker` feature happens to pull it in.
echo "== clinker-net: default (no transport)"
cargo check --locked --all-targets -p clinker-net || rc=1
echo "== clinker-net: transport"
cargo check --locked --all-targets -p clinker-net --features transport || rc=1

exit "${rc}"
