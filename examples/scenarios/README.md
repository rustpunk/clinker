# Scenario corpus

Realistic end-to-end pipelines, each solving a job someone would actually run.
They serve two purposes at once: worked examples to read and copy, and an
executed test gate that catches wrong answers.

This is the difference between these and `examples/pipelines/`. Those are
smoke-checked with `clinker run <f> --explain`, which is plan-only — it proves a
pipeline *compiles*. Every scenario here is **executed against real data**, and
every output byte is compared to a committed expected file. A pipeline that
plans cleanly, runs cleanly, exits 0 and writes silently wrong output fails here
and passes there.

## Running one

Inputs are generated rather than committed, so generate them once:

```bash
cargo run -p clinker-scenarios -- gen
```

Then run any scenario from its own directory:

```bash
cd examples/scenarios/01-storefront-orders
cargo run -p clinker -- run pipeline.yaml
```

Each scenario's `README.md` explains what it does, what to look at in the
output, and which engine behaviour it is demonstrating.

Useful variants:

```bash
cargo run -p clinker-scenarios -- list                        # the ladder
cargo run -p clinker-scenarios -- gen --scenario 03-support-triage
cargo run -p clinker-scenarios -- gen --force                 # ignore the cache
```

## The ladder

| Scenario | Formats | Introduces |
|---|---|---|
| [01-storefront-orders](01-storefront-orders/) | csv → csv | typed source schema, exact decimal money, `filter`, projection |
| [02-product-feed-normalize](02-product-feed-normalize/) | xml → csv, xml | repeated elements as a `multiple:` column, per-sink encoding, minor-unit money |
| [03-support-triage](03-support-triage/) | csv → csv, json | strict conversion, dead-letter queue, exclusive routing to three sinks |

More scenarios are landing in sequence — combine and aggregate, windowing,
multi-record flat files, envelope reconstruction, and the EDI family.

## Scenarios that currently fail

A scenario may be committed with goldens the engine does not yet produce, marked
`known_broken` in the harness against an issue. The goldens state the correct
answer; the marker records exactly how the engine disagrees, including a pinned
fail-loud diagnostic when the safe current behavior is to stop the run.

The marker names the specific outputs allowed to differ and, where relevant, the
run summary the engine currently prints. Every other output stays fully gated, so
parking a scenario for one reason cannot silently stop its working parts from
being checked. An undeclared exit code, drifted input digest, or unexpected
diagnostic always fails. The gate's counters record the *correct* summary, so
the run that fixes the underlying bug reports the marker as stale rather than a
counter mismatch.

A re-bless run will not replace correct goldens from a known failing run.

No scenarios are currently parked.

## Layout

```
NN-scenario-name/
  README.md        what it does and what to look at
  pipeline.yaml    the pipeline
  expected/        committed goldens — the reviewed statement of correct output
  data/            generated inputs        (git-ignored)
  output/          produced by running it  (git-ignored)
```

## Why inputs are generated but expectations are committed

A golden is an assertion about behaviour: its diff is what a reviewer reads to
decide whether a change was intended. Input data has nothing to review — it is
reproducible bytes — so committing it would add noise without adding signal.

That split creates one hazard worth knowing about. A committed golden is only
meaningful for the exact input that produced it, so a generator change that
slipped through unnoticed would leave every golden describing nothing. The test
harness therefore pins the digest of each scenario's generated input and checks
it **before** comparing any output. Drift fails as "the input changed", which is
actionable, rather than as an inexplicable diff.

## Changing a scenario

If you change a pipeline and the new output is correct, re-bless the goldens:

```bash
UPDATE_SCENARIO_GOLDENS=1 cargo test -p clinker --test scenarios -- --nocapture
```

`--nocapture` is required rather than optional: libtest swallows stdout on a
passing test, and a re-bless run passes, so without it the input digest the next
step tells you to copy is never printed.

Then **read the diff**. A few changed lines in the shape you intended is the
signal you want. Every row changing usually means the generator moved rather
than the pipeline, and the new goldens are not describing what you think.

If you changed a generator, bump `GENERATOR_VERSION` in
`crates/clinker-scenarios/src/lib.rs` and update the pinned digest the blessing
run prints.

## Determinism

Byte-exact comparison only works because nothing in a scenario varies between
runs. Every scenario follows these rules, and new ones should too:

- Generated data is seeded, never wall-clock derived. Dates are offsets from a
  fixed epoch; money is integer cents, never a float.
- Row order comes from the reader, which is deterministic per file. The
  Sink node's `sort_order:` key is deliberately unused: it is parsed and
  documented but currently inert ([#950](https://github.com/rustpunk/clinker/issues/950)).
- `merge` uses `concat`; an unseeded `interleave` is non-deterministic by design.
- No `now()` or `$pipeline.start_time` in a compared column.
- The harness pins `--batch-id`, whose default is a fresh UUID per run.

The dead-letter queue is the one place genuine per-run variation survives: each
entry carries a UUID and a wall-clock timestamp. The harness blanks those two
columns and compares the other eighteen — including the source row, the
triggering value, the error category, and the full original record — verbatim.
