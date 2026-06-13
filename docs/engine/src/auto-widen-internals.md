# Schema Drift & the `$widened` Sidecar

*User-facing view: the User Guide's "Auto-Widen & Schema Drift" page.*

This page is the engine-internals reference for how Clinker absorbs input columns the source's declared `schema:` block does not name, carries them through the DAG, and either expands them back at the sink or refuses them at a writer that cannot serialize them. The mechanism is an *on-schema* sidecar column named `$widened`, stamped by the engine and propagated by the same machinery that carries every user-declared column. The depth here is the sidecar's data model (`FieldMetadata::WidenedSidecar`, `Value::Map`), the per-node-type propagation rules, the writer/DLQ rejection paths, and the structural reasons the design is on-schema rather than off-schema. The user-facing page documents the three policy modes and the YAML knobs; this page documents why the absorber is shaped the way it is.

## The three modes (context)

The per-source `on_unmapped` policy selects one of three behaviors for input fields absent from the declared schema. The engine-wide default is `auto_widen`.

- **`auto_widen`** *(default)* — per-record undeclared fields are absorbed into a `Value::Map` payload carried by an engine-stamped `$widened` sidecar column appended to the source's schema. The payload propagates downstream and the sink expands it back to top-level columns when `include_unmapped: true` on the Output node (the default). Pattern precedent: Databricks Auto Loader's `_rescued_data` sidecar and ClickHouse's `JSON` column type.
- **`drop`** — undeclared input fields are silently stripped at read time. No sidecar; the source's plan-time schema equals the declared `schema:`.
- **`reject`** — any input record carrying a key not in the declared schema fails the source with a `FormatError::UndeclaredField` diagnostic naming the offending field.

Everything below concerns `auto_widen`, the only mode that materializes a sidecar.

## The `$widened` sidecar absorber

`auto_widen` is implemented as an *on-schema* sidecar: the engine appends a single column named `$widened` to the source's schema, marked with `FieldMetadata::WidenedSidecar`. Each record's undeclared input fields are stored as the sidecar's `Value::Map` payload — keyed by input field name, valued by the read scalar.

The on-schema design is deliberate, and the reason is a silent-loss bug class. An off-schema sidecar — a parallel data structure living outside `Schema` — would be dropped by any code path that reconstructs a `Record` from `schema.columns()` and a value vector. The DAG has many such reconstruction points (projection, sort, spill round-trips, combine collect-array assembly), and each one would carry a standing obligation to "remember to copy the side-channel." The on-schema slot instead inherits the exact same serialization, span propagation, sort/spill, and projection machinery as a user-declared column: there is no separate copy obligation on any consumer, because to every consumer `$widened` *is* a column.

The trade-off the on-schema design accepts is that the sidecar occupies a real schema slot the typechecker can see by name. CXL expressions cannot read or write the sidecar — the typechecker is blind to its *contents* (the `Value::Map` interior is never type-resolved into addressable fields), and the parser rejects a literal `$widened` reference at the system-variable layer. The net effect is that the sidecar rides through every structural transform automatically while remaining unaddressable from user CXL.

## Propagation through the DAG

The `$widened` sidecar follows these rules through downstream nodes. The table is the propagation contract each node type's executor implements.

| Node type     | Sidecar behavior |
|---|---|
| Transform     | Inherits unchanged from input (transforms are row-preserving). |
| Aggregate     | Output's `$widened` slot is `Value::Null` — per-row payloads have no canonical aggregation. Users who need an unmapped field at aggregate output must add it to `group_by` or emit it explicitly via an aggregate function. |
| Combine       | Driver's sidecar rides through; build-side sidecars are dropped (mirrors `propagate_ck: Driver`). Build-side `iter_user_fields()` filters every engine-stamped column from `match: collect` array payloads, so build `$widened` cannot leak into the collect array. Users can lift a build-side unmapped field via `<build_qualifier>.<field>` in the combine body's CXL. |
| Route / Merge | Row-preserving — sidecar passes through. `Merge` requires every input source to share the same `on_unmapped` policy; mixing fails compile with **E315** (see below). |
| Composition   | Body inherits the parent's sidecar via the synthetic input port; whatever the body's terminal node carries flows back to the parent. The body's terminal-node propagation rule applies (e.g. an Aggregate terminal yields `Value::Null` at the parent boundary, a `match: first` Combine terminal carries the driver's payload). |
| Output        | Sidecar expands to top-level columns when `include_unmapped: true` (the default). Set `include_unmapped: false` to strip the sidecar (and every other unmapped input field) so only explicitly-emitted columns reach the writer. |

Two of these rows encode load-bearing internal mechanics worth restating:

- **Combine `propagate_ck: Driver` mirroring.** The sidecar follows the same provenance rule as correlation keys: only the driver (probe) side's payload survives the join, build-side payloads are dropped. The build-side `iter_user_fields()` iterator is the single filter that excludes every engine-stamped column — `$widened` and the `$ck.*` lattice alike — from the `match: collect` array, so a build record's sidecar can never appear as an element of a collect array even though build user fields can. The escape hatch for a genuinely needed build-side unmapped field is to lift it explicitly through the combine body CXL via the build qualifier.
- **Aggregate null-out.** There is no canonical way to fold N per-row `Value::Map` payloads into one, so the Aggregate output slot is deliberately `Value::Null` rather than (say) the first row's payload or a merged map. The explicit-emit path (`group_by` membership or an aggregate function) is the supported way to carry a specific unmapped field across an aggregation boundary.

## Output controls

When `include_unmapped: true` (the default), fields the source absorbed into `$widened` are expanded back to top-level columns at the sink. The expansion happens at the **projection layer, before the writer sees the record**, so the literal `$widened` slot is stripped during expansion and the writer never sees a `Value::Map` for a well-formed pass-through. Setting `include_unmapped: false` strips the sidecar (and every other input field not explicitly emitted upstream) so the writer sees only user-declared columns.

`include_unmapped` composes independently with `include_correlation_keys`; the two flags are orthogonal — `include_correlation_keys` does not surface `$widened`. Because expansion is a projection-layer operation, a CSV source under `auto_widen` feeding a JSON output under `include_unmapped: true` produces JSON objects whose top-level keys include both declared columns and absorbed input columns, with no sidecar key remaining.

## Writer rejection of `Value::Map` payloads

CSV, XML, and fixed-width writers refuse any record carrying a `Value::Map` payload at any column slot, raising `FormatError::UnserializableMapValue { format, column }`. The rejection lives in **each writer's value-to-string helper** — a single point of truth, with no defensive prechecks scattered ahead of it. JSON serializes `Value::Map` natively as a nested object and never raises.

The common trigger is the `$widened` sidecar reaching one of those three writers because the Output node set `include_unmapped: false` (which suppresses the projection-layer expansion that would otherwise have flattened the map to top-level scalars). Two remediations exist, and the error message names both: leave `include_unmapped` at its default `true` so projection expands the map before write, or coerce the map to a scalar in CXL before the emit.

### DLQ filtering

The dead-letter-queue writer applies the same exclusion at its own layer rather than relying on the main-path projection. `dlq::dlq_user_columns` strips every column tagged `FieldMetadata::WidenedSidecar`, so the DLQ CSV header never contains a `$widened` column even when a DLQ entry's `original_record` still carries the full `auto_widen` schema shape. Correlation-lattice columns (`$ck.*`) are deliberately *retained* in DLQ output for collateral debugging — the DLQ filter excludes only the unserializable sidecar, not the engine-stamped provenance columns.

## E315 — Merge inputs must agree on policy

Merge concatenates streams positionally against the merge node's `output_schema` (taken from the first input). Every input must agree on column shape — same column names, same `on_unmapped` policy, same `correlation_key` set. The `$widened` agreement is a special case of that rule: if one upstream source uses `auto_widen` (and therefore carries the sidecar column) while another uses `drop` or `reject` (and does not), the two input schemas disagree on the presence of the `$widened` slot, and the positional concatenation has no coherent column to align. Compile fails:

```text
E315: merge "merged": input schemas disagree on the `$widened` auto_widen sidecar column.
```

The remediation is to set every merge upstream source to the same `on_unmapped` policy; for sources that should explicitly omit the sidecar, declare `on_unmapped: { mode: drop }` (or `reject`) on each so the absent-sidecar shape is uniform across inputs.

## Fixed-width sources are structurally inert

Fixed-width sources are positional: the schema is constructed from `width` / `start..end` byte ranges, and bytes outside the declared ranges are invisible to the reader. There is no notion of an "undeclared field" to absorb — a byte either falls inside a declared range (and becomes a declared column) or is never read. `auto_widen` therefore can never populate the sidecar for a fixed-width source; the `$widened` slot stays `Value::Null` for every record.

Because the policy is silently inert rather than wrong, the executor emits a `tracing::info` diagnostic at source-reader construction time when `auto_widen` is the policy on a fixed-width source, naming the source. The diagnostic fires once per reader instance — a source used as a combine build-side input across multiple combines may produce one log line per combine. To avoid the noise, switch to `on_unmapped: drop` (or `reject`) for explicit scalar semantics, or accept the empty sidecar.
