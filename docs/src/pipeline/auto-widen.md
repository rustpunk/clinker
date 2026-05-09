# Auto-Widen & Schema Drift

When an input file carries columns the source's declared `schema:`
block does not name, Clinker decides what to do with them via the
per-source `on_unmapped` policy. The engine-wide default is
`auto_widen` — schema drift is preserved end-to-end without
user-visible breakage. This chapter is the single source of truth
for the absorber design, propagation rules, output controls, and
diagnostics.

## The three modes

```yaml
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: "./data/orders.csv"
    on_unmapped:
      mode: auto_widen     # default; other values: drop, reject
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: numeric }
```

- **`auto_widen`** *(default)* — per-record undeclared fields are
  absorbed into a `Value::Map` payload carried by an
  engine-stamped `$widened` sidecar column appended to the
  source's schema. The sidecar's payload propagates through
  downstream nodes and can be expanded back to top-level columns
  at any Output sink via `include_widened: true`. Pattern
  precedent: Databricks Auto Loader's `_rescued_data` sidecar and
  ClickHouse's `JSON` column type.

- **`drop`** — undeclared input fields are silently stripped at
  read time. No sidecar; the source's plan-time schema equals the
  declared `schema:`. Matches Snowflake's
  `MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'` with
  `ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE` and dbt's
  `on_schema_change=ignore`.

- **`reject`** — any input record carrying a key not in the
  declared schema fails the source with a
  `FormatError::UndeclaredField` diagnostic naming the offending
  field. Strict; matches dlt's `freeze` mode.

## The `$widened` sidecar absorber

`auto_widen` is implemented as an *on-schema* sidecar: the
engine appends a single column named `$widened` to the source's
schema, marked with `FieldMetadata::WidenedSidecar`. Each record's
undeclared input fields are stored as the sidecar's
`Value::Map` payload — keyed by input field name, valued by the
read scalar.

The on-schema design is deliberate. An off-schema sidecar (a
parallel data structure outside `Schema`) is a silent-loss bug
class: any code path that reconstructs a `Record` from
`schema.columns()` and a value vector silently drops the
side-channel. The on-schema slot inherits the same
serialization, span propagation, sort/spill, and projection
machinery as user-declared columns — there is no "remember to
copy the sidecar" obligation on every consumer. CXL expressions
cannot read or write the sidecar (the typechecker is blind to
its contents); see [System variables → `$widened`](../cxl/system-variables.md)
for the parser-level rejection.

## Propagation through the DAG

The `$widened` sidecar follows these rules through downstream
nodes:

| Node type     | Sidecar behavior |
|---|---|
| Transform     | Inherits unchanged from input (transforms are row-preserving). |
| Aggregate     | Output's `$widened` slot is `Value::Null` — per-row payloads have no canonical aggregation. Users who need an unmapped field at aggregate output must add it to `group_by` or emit it explicitly via an aggregate function. |
| Combine       | Driver's sidecar rides through; build-side sidecars are dropped (mirrors `propagate_ck: Driver`). Build-side `iter_user_fields()` filters every engine-stamped column from `match: collect` array payloads, so build `$widened` cannot leak into the collect array. Users can lift a build-side unmapped field via `<build_qualifier>.<field>` in the combine body's CXL. |
| Route / Merge | Row-preserving — sidecar passes through. `Merge` requires every input source to share the same `on_unmapped` policy; mixing fails compile with **E315** (see below). |
| Composition   | Body inherits the parent's sidecar via the synthetic input port; whatever the body's terminal node carries flows back to the parent. The body's terminal-node propagation rule applies (e.g. an Aggregate terminal yields `Value::Null` at the parent boundary, a `match: first` Combine terminal carries the driver's payload). |
| Output        | Sidecar is stripped by default. Set `include_widened: true` to expand the map's keys back to top-level columns at the sink. |

## Output controls

```yaml
- type: output
  name: out
  input: src
  config:
    name: out
    type: json
    path: out.json
    include_widened: true    # default: false
```

When `true`, fields the source absorbed into `$widened` are
expanded back to top-level columns at the sink. Useful for
pass-through pipelines where every original input field should
reach the output regardless of whether it was declared in
`schema:`.

`include_widened` composes independently with
`include_correlation_keys: true` — each, both, or neither can be
set. `include_correlation_keys` does **not** surface
`$widened`; the two flags are orthogonal.

### Cross-format flow

The expansion happens at the projection layer, before the
writer sees the record. So a CSV source with `auto_widen` plus
a JSON output with `include_widened: true` produces JSON
objects whose top-level keys include both declared columns and
absorbed input columns:

```text
input.csv:    id,extra,city
              1,foo,Paris

output.json:  {"id": "1", "extra": "foo", "city": "Paris"}
```

The literal `$widened` slot is stripped during expansion; the
writer never sees a `Value::Map`.

### Writer rejection of `Value::Map` payloads

CSV, XML, and fixed-width writers refuse records carrying a
`Value::Map` payload at any column slot, raising
`FormatError::UnserializableMapValue { format, column }`. The
rejection lives in each writer's value-to-string helper —
single point of truth, no defensive prechecks. JSON serializes
`Value::Map` natively as a nested object and does not raise.

The most common cause: the `$widened` sidecar reaches the
writer because the Output node forgot to set `include_widened:
true`. Remediation is either to set the flag (so the projection
layer expands the map to top-level columns before write) or to
coerce the map to a scalar in CXL before the emit. The error
message lists both routes.

The DLQ writer applies the same filter at its own layer:
`dlq::dlq_user_columns` strips any column tagged
`FieldMetadata::WidenedSidecar`, so the DLQ CSV header never
contains `$widened` even when the DLQ entry's
`original_record` retains the auto_widen schema shape.
Correlation-lattice columns (`$ck.*`) are retained in the DLQ
output for collateral debugging.

## E315 — Merge inputs must agree on policy

Merge concatenates streams positionally against the merge node's
`output_schema` (taken from the first input). Every input must
agree on column shape — same column names, same `on_unmapped`
policy, same `correlation_key` set.

If two upstream sources disagree on whether they carry the
`$widened` sidecar (one source uses `auto_widen`, another uses
`drop` / `reject`), compile fails:

```text
E315: merge "merged": input schemas disagree on the `$widened` auto_widen sidecar column.
```

Remediation: set every merge upstream source to the same
`on_unmapped` policy. The engine-wide default is `auto_widen`;
for sources that should explicitly omit the sidecar, declare
`on_unmapped: { mode: drop }` (or `reject`) on each.

## Fixed-width sources are structurally inert

Fixed-width sources are positional — the schema is constructed
from `width` / `start..end` byte ranges, and bytes outside the
declared ranges are invisible to the reader. `auto_widen`
therefore can never populate the sidecar for fixed-width
sources; the slot stays `Value::Null` for every record.

The executor emits a `tracing::info` diagnostic at source-reader
construction time when `auto_widen` is the policy on a
fixed-width source, naming the source. The diagnostic fires
once per reader instance; a source used as a combine
build-side input across multiple combines may produce one log
per combine. To avoid the noise, switch to `on_unmapped: drop`
(or `reject`) for explicit scalar semantics, or accept the
empty sidecar.
