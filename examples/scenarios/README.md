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
| [03-support-triage](03-support-triage/) | csv → csv, json | strict conversion, dead-letter queue, exclusive routing to three sinks |

More scenarios are landing in sequence — combine and aggregate, windowing,
multi-record flat files, envelope reconstruction, and the EDI family.

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
UPDATE_SCENARIO_GOLDENS=1 cargo test -p clinker --test scenarios
```

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
- Every output declares an explicit `sort_order`.
- `merge` uses `concat`; an unseeded `interleave` is non-deterministic by design.
- No `now()` or `$pipeline.start_time` in a compared column.
- The harness pins `--batch-id`, whose default is a fresh UUID per run.

The dead-letter queue is the one place genuine per-run variation survives: each
entry carries a UUID and a wall-clock timestamp. The harness blanks those two
columns and compares the other eighteen — including the source row, the
triggering value, the error category, and the full original record — verbatim.
