# Auto-Widen & Schema Drift

When an input file carries columns the source's declared `schema:` block does not name, Clinker decides what to do with them via the per-source `on_unmapped` policy. The default is `auto_widen`, which preserves the extra columns end-to-end so schema drift never silently breaks a pipeline. This page covers the three modes, how undeclared columns flow downstream, the output controls, and the related diagnostics.

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

- **`auto_widen`** *(default)* — undeclared input fields are carried along with each record and re-expanded to top-level columns at the output (when the Output node's `include_unmapped` is left at its default of `true`). Nothing is lost, and you don't have to declare every column up front.
- **`drop`** — undeclared input fields are silently stripped at read time. The source carries only its declared `schema:`.
- **`reject`** — any record carrying a field not in the declared schema fails the source with a diagnostic naming the offending field. The strict choice when unexpected columns should be treated as errors.

CXL expressions can only read fields you declared in `schema:` — carried-along undeclared fields are not visible to CXL, only to the output. To use an undeclared field in an expression, add it to the source `schema:`.

## How undeclared columns flow downstream

Carried-along columns follow these rules through each node type:

| Node type     | Behavior |
|---|---|
| Transform     | Passed through unchanged (transforms are row-preserving). |
| Aggregate     | Dropped — per-row extra columns have no meaning on a grouped row. To keep one, add it to `group_by` or emit it explicitly. |
| Combine       | The driver's carried columns ride through; build-side ones are dropped. To keep a build-side field, emit it explicitly in the combine body via `<build_qualifier>.<field>`. |
| Route / Merge | Passed through. `Merge` requires every input to share the same `on_unmapped` policy — mixing fails with **E315** (see below). |
| Composition   | The body inherits the parent's carried columns and whatever the body's last node carries flows back out. |
| Output        | Expanded to top-level columns when `include_unmapped: true` (the default); stripped when `false`. |

## Output controls

```yaml
- type: output
  name: out
  input: src
  config:
    name: out
    type: json
    path: out.json
    include_unmapped: true    # default: true
```

When `true` (the default), undeclared fields the source carried along are expanded back to top-level columns at the sink — useful for pass-through pipelines where every original column should reach the output. Set `include_unmapped: false` to write only the columns explicitly emitted upstream.

`include_unmapped` is independent of `include_correlation_keys`: each can be set on its own, and `include_correlation_keys` never surfaces auto-widened columns.

### Cross-format flow

Expansion happens before the writer runs, so a CSV source with `auto_widen` feeding a JSON output with `include_unmapped: true` produces JSON objects whose keys include both the declared columns and the absorbed ones:

```text
input.csv:    id,extra,city
              1,foo,Paris

output.json:  {"id": "1", "extra": "foo", "city": "Paris"}
```

### Writer errors on unexpanded columns

The CSV, XML, and fixed-width writers can only write flat scalar columns. If carried-along columns reach one of these writers without being expanded — which happens when you set `include_unmapped: false` but a nested value is still present — the write fails with an `UnserializableMapValue` error naming the format and column. (JSON has no such limit; it writes nested values natively.)

The fix is to either leave `include_unmapped` at its default of `true`, so the columns are expanded to top-level before writing, or to convert the value to a scalar in CXL before emitting it. The error message lists both routes.

## E315 — Merge inputs must agree on policy

`Merge` concatenates its inputs positionally, so every input must agree on column shape — same column names, same `on_unmapped` policy, same `correlation_key` set. If two upstream sources disagree on whether they carry auto-widened columns (one uses `auto_widen`, another uses `drop` / `reject`), compilation fails:

```text
E315: merge "merged": input schemas disagree on the `$widened` auto_widen sidecar column.
```

The fix is to set every merge upstream source to the same `on_unmapped` policy.

## Fixed-width sources

Fixed-width sources are positional — the reader only sees the byte ranges your schema defines, so there are never any "extra" columns to absorb. `auto_widen` has no effect on a fixed-width source; use `on_unmapped: drop` (or `reject`) to make that explicit and silence the informational log the engine emits otherwise.
