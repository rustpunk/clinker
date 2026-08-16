# Output Nodes

Output nodes write processed records to files. They are the terminal nodes of a pipeline -- every pipeline path must end at an output (or records are silently dropped).

> **Terminal-node migration (not available yet):** The current binary accepts
> only `type: output`, and every runnable example on this page uses that
> spelling. Decision D-56 assigns a one-way, project-wide rename of the
> terminal destination concept to Sink and `type: sink` under AUTH-09.
> That atomic migration must finish before REST and SQL endpoint expansion;
> do not write `type: sink` in current pipelines. The rename is
> deliberately narrow: composition and node output ports, produced artifacts,
> files and paths, serialization formats, stdout, command or machine output,
> writer results, and OpenLineage output datasets keep the word “output.” See
> [Production Contracts](https://github.com/rustpunk/clinker/blob/main/docs/ai/15_PRODUCTION_CONTRACTS.md#terminal-destination-vocabulary) for
> the compatibility boundary.

## Basic structure

```yaml
- type: output
  name: result
  input: transform_node
  config:
    name: output_stage
    type: csv
    path: "./output/result.csv"
```

The `type:` field selects the output format: `csv`, `json`, `xml`, `fixed_width`, `edifact`, `x12`, `hl7`, or `swift`. The `edifact`, `x12`, and `swift` writers reconstruct one interchange/message envelope around emitted records; the `hl7` writer re-emits HL7 v2 segments and optionally wraps them in batch/file envelopes. See [EDIFACT Format](../formats/edifact.md), [X12 Format](../formats/x12.md), [HL7 v2 Format](../formats/hl7.md), and [SWIFT MT Format](../formats/swift.md).

Structured single-writer outputs (`edifact`, `x12`, `hl7`, and `swift`) accept one concrete document grain per output file. A multi-file source or multi-input merge feeding one of these outputs is rejected instead of being silently written as one merged envelope. To write multiple structured documents, consolidate them deliberately with an Envelope node first or route each document to a separate output path.

## Local and network-share destinations

An output path may be on a local filesystem or a mounted NFS/SMB share.
Clinker detects the filesystem behind the actual destination and applies its
contained-create and same-filesystem promotion rules there; users do not label
their production paths with a CI profile. The committed filesystem matrix
qualifies Clinker's semantics against specific loopback NFSv4.1 and SMB3.1.1
mounts, but it cannot certify every vendor appliance, mount option, outage
mode, or corporate network. Qualify representative production mounts before
depending on atomic promotion during an outage or failover.

Clinker creates Unix output files with owner-only mode `0600`. This prevents a
new file from accidentally inheriting broad access in a shared drop zone. If a
different service account or group must consume the result, arrange that access
explicitly with the destination's ACL/ownership policy; Clinker does not
currently expose an output-mode setting.

For performance, keep spill files and optional staged input copies on a local
disk when one is available. Blocking operators can create substantial random
I/O, and performing that work directly on a network share adds latency and
network traffic. The final output is still written as a hidden file on the
destination filesystem and promoted there, so the completed file never relies
on a cross-filesystem rename from local storage.

This commit lifecycle applies to single-file, per-source-file fan-out, and
`split:` outputs. Clinker does not open or truncate an existing final while a
replacement is running. Before publication, Clinker synchronizes and validates
the complete output set, then promotes each hidden file directly to its final
name. An overwrite is one atomic replacement rename: Clinker never moves the
previous final out of the way first. It also never claims that a multi-file set
can be rolled back after some replacements are already visible.

Publication is not one atomic filesystem operation for the whole set. Each
individual rename is atomic, but a reader may briefly observe a mixture while
the finite commit walks several destinations. If a promotion or directory sync
fails, Clinker stops, exits `4`, and reports three exact groups: finals that are
visible and synchronized, finals that are visible but whose parent sync failed,
and unpublished hidden partials. Already-visible finals stay visible; remaining
old finals stay untouched. A process or machine crash in that window can leave
the same mixed set plus `.partial` or `.reservation` siblings. Reconcile those
named paths before retrying or consuming the set. Clinker does not create or
use `.backup` files for output publication.

Every collision policy uses a hidden sibling reservation, including overwrite.
This ensures only one live publisher may mutate a final destination. `if_exists:
error` uses a no-replace promotion; `if_exists: overwrite` and `clinker run
--force` replace only at successful promotion; `if_exists: unique_suffix`
reserves candidate names until one wins. Reservations never expose zero-byte
final placeholders. A reservation holds an operating-system lock and records
its owner process. A later run reclaims it only after a short creation grace
period and only when the lock is acquirable, proving that no live publisher owns
it. If reservation cleanup fails after successful publication, Clinker exits
`4` and names both the visible final and stale reservation as cleanup debt.

When `unique_suffix` can find no name at all because the destination itself
refuses every candidate — the directory is not writable by this run — the
diagnostic names the path you wrote, not the numbered candidate the search
happened to stop on, and says that the destination rather than the name is what
refused. Fix the directory's permissions, or point `path:` somewhere this run
may write.

Rendered fan-out paths are validated as new output paths. Directory traversal,
an absolute result produced from a relative template, symbolic-link/reparse
ancestors, and cross-filesystem promotion fail before a final is touched. Create
the intended destination directories ahead of the run; Clinker does not follow
rendered paths while creating missing fan-out parents.

`{source_file}` and `{source_path}` create one output route per discovered
source file. Two source files that render to the same destination are rejected
before any output is staged, with both source paths in the diagnostic. Escape a
token as `{{source_file}}` or `{{source_path}}` when the braces are intended as
literal filename text. Runtime source names and paths are inserted as opaque
text: braces inside an actual filename are never interpreted as another token.
When fan-out is combined with `split:`, every source has its own segment
sequence: each starts at sequence 1 and rolls over independently.

With `write_meta: true`, Clinker writes a `.meta.json` sidecar for every actual
committed final. A split output therefore gets one sidecar per segment, and a
fan-out output gets one per rendered destination; no sidecar is written for the
unrendered base template. Main outputs and sidecars share the same publication
ledger, so a path collision between any two of them fails before publication
and names both producers. Counters that are not known at sidecar-preparation
time are omitted from the JSON rather than written as misleading zeroes.

## When two paths are the same destination

Two Output nodes — or an Output node and a DLQ path — that resolve to one file
are rejected at plan time with `E322`, before any record is read. Deciding that
means deciding when two differently-spelled paths name one file, and that
depends on the volume you are writing to, not on the text. Clinker measures the
volume rather than guessing from its type, by creating and removing a probe file
in the destination directory.

Two paths are the same destination when they differ only in:

- **`.` components, or relative versus absolute spelling.** `./out/errors.csv`
  and `/data/out/errors.csv` from `/data` are one file everywhere.
- **A symlinked parent directory.** The existing part of each path is resolved,
  so a link and its target are one place.
- **Letter case — only on a volume that ignores case.** The default macOS
  (APFS) and Windows (NTFS) volumes do; ext4, xfs, and btrfs do not. Where it
  applies, it covers the whole of Unicode, not just ASCII: `Ärger.csv` and
  `ärger.csv` are one file, as are `Σ.csv` and `σ.csv`. `straße.csv` and
  `strasse.csv` are always **two** files — no filesystem treats them as one.
- **Unicode normal form — only on a volume that ignores it.** APFS and HFS+ do,
  in both their case-sensitive and case-insensitive variants; ext4 and NTFS do
  not. Where it applies, `café.csv` written with a precomposed `é` and the same
  name written as `e` plus a combining accent are one file. This is independent
  of the case rule: a case-*sensitive* APFS volume ignores normal form while
  still telling `Café.csv` and `café.csv` apart.

Because the last two depend on the volume, the same pipeline can be accepted on
Linux and rejected on macOS. That is not an inconsistency — the two disks really
do behave differently, and the rejection is the one that prevented a file from
being written twice.

Two limits are worth knowing:

- Clinker may report a collision on a volume that would in fact have kept the
  two files apart — for instance on an older Windows volume whose case table
  predates a character you used. The run stops with both paths named, and
  renaming either one clears it.
- The reverse is possible on Windows, which folds some letters according to
  Turkish and Azeri rules that no locale-independent table reproduces. `İ.csv`
  and `i.csv` may be one file on such a volume while Clinker still sees two. If
  you write output paths that differ only in dotted or dotless `I`, give them
  distinct names.

## Direct broadcast to several outputs

Several Output nodes may name the same input. This is a broadcast: every
Output receives every upstream record, regardless of node declaration order.
The run report counts one write per sink, so five input records feeding a CSV
and a JSON Output produce `records_written: 10`.

Use a [Route](route.md) node when outputs should receive different subsets.
Writing a field such as `_route` does not select a destination; it is an
ordinary output column unless a Route condition explicitly reads it.

## Field control

Output nodes can either pass every upstream field through to the writer or restrict output to the fields the upstream transform explicitly emitted. Several options control which fields appear and how they are named.

### Unmapped input field passthrough

```yaml
    include_unmapped: false    # Default: true
```

When `true` (the default), every field on an input record that the upstream transform did not explicitly emit still passes through to the output unchanged. This includes fields the source's `on_unmapped: auto_widen` policy absorbed into the per-record `$widened` sidecar map -- their contents expand back to top-level columns at the sink.

When `false`, only fields named by an `emit` statement in the upstream transform appear in the output. The `$widened` sidecar slot is stripped and undeclared input fields are dropped.

When `true`, how a carried-along column reaches the writer depends on the output format. Self-describing formats (JSON / NDJSON / XML) write each record's own keys. A CSV output widens its header to the union of every record's columns when it can materialize the batch, and otherwise — on a bounded-memory streaming path (a `Merge`, a fused `Transform`, a single-branch `Route`, a streaming-strategy `Aggregate`, or the probe side of a hash-build-probe `Combine` feeding the output), or an envelope-reconstructing path — fails loudly with a `SchemaDrift` error rather than dropping a column it cannot fit under its already-committed header. A fixed-width output has no room for an undeclared column and likewise raises `SchemaDrift`. See [Auto-Widen & Schema Drift → Schema drift across records](../formats/auto-widen.md#schema-drift-across-records-tabular-formats).

#### Migration notice

The default flipped from `false` to `true` in a recent release (see [issue #90](https://github.com/rustpunk/clinker/issues/90)). Pipelines that relied on the previous behavior -- where output records contained only the fields explicitly emitted upstream -- must now set `include_unmapped: false` explicitly to restore that shape.

The flag composes independently with `include_correlation_keys: true` -- see below. See [Auto-Widen & Schema Drift -> Output controls](../formats/auto-widen.md#output-controls) for the full specification and cross-format flow examples.

#### Worked example

Suppose the upstream source emits records with `order_id`, `customer_id`, `amount`, and `region`, and a transform that emits only one derived field:

```yaml
- type: transform
  name: classify
  input: orders
  config:
    cxl: |
      emit amount_bucket = if amount >= 1000 then "high" else "low"
```

With `include_unmapped: true` (the default), each output record carries `order_id`, `customer_id`, `amount`, `region`, and `amount_bucket`. With `include_unmapped: false`, each output record carries only `amount_bucket`. The transform's CXL is unchanged in both cases -- the Output node decides the field set.

### Include correlation-key shadow columns

```yaml
    include_correlation_keys: true    # Default: false
```

When a source declares a `correlation_key:`, the engine tracks correlation-group identity on hidden columns that are stripped from output by default. Set `include_correlation_keys: true` to surface them in the writer output — typically for debugging correlation-group routing or auditing DLQ behavior. See [Correlation Keys](../pipelines/correlation-keys.md).

`include_correlation_keys` does **not** surface auto-widened columns -- `include_unmapped` is the separate flag for that. The two are independent: each, both, or neither can be set.

### Nested columns and writer capabilities

JSON and XML write map and array values recursively. JSON uses native objects
and arrays; XML maps ordinary keys to child elements, reserves unescaped
`@...`/`#text` keys for attributes/text, and repeats a child name for arrays.
CSV, fixed-width, EDIFACT, X12, and HL7 remain scalar formats and reject a map
that reaches a column slot. See [JSON](../formats/json.md#native-map-and-array-values),
[XML](../formats/xml.md#native-map-and-array-values), and
[Auto-Widen & Schema Drift](../formats/auto-widen.md#writer-errors-on-unexpanded-columns).

### Field mapping

`mapping:` declares the columns the file carries -- which columns, under what
names, in what order -- without changing upstream CXL. It is a sequence, one
item per output column:

```yaml
    mapping:
      - order_id                  # carried through under its own name
      - sold_to: customer_id      # written as `sold_to`, read from `customer_id`
      - contact_email: customer_email
      - channel
      - sku
```

Two item shapes:

- **A bare column name** emits that column unchanged. This is the common case,
  and it costs one line naming the column once.
- **A single-key pair** renames. The **output name is on the left**, the source
  column on the right -- the same side the bare form names. Reading an item
  left to right always tells you what appears in the file first.

The renames are the only items carrying a colon, so in a wide output they are
found by scanning for structure rather than by comparing two names per line.

#### Order and selection

**Declaration order is the output column order.** Listed columns are written
first, in the order the block declares them, whatever order they arrive in.

**`include_unmapped` governs everything the block does not list.** With
`include_unmapped: true` (the default) unlisted columns are appended after the
declared ones, in their existing relative order. With `include_unmapped: false`
they are dropped, so the block becomes the complete statement of the output:

```yaml
    include_unmapped: false
    mapping:
      - department
      - surname: last_name
      - first_name
```

Given upstream columns `first_name, last_name, department`, that writes exactly
`department,surname,first_name`.

**Every record carries every declared column.** When a record does not supply an
item's source column, that column is still written, empty. The file's shape
follows the block, not the data — so a stream whose records differ in shape (a
multi-record-type source, a column arriving through `auto_widen`, a composition
body's open row) still produces one stable column set in declaration order,
rather than one that depends on which record happened to arrive first.

One upstream column may feed two output columns -- `- sku` and
`- item_code: sku` -- because names must be unique on the output side, not the
source side. Declaring the same *output* name twice is rejected (**E364**): a
file cannot carry two columns under one header.

For the same reason, an output name that `include_unmapped: true` would also
carry through is rejected. If upstream already has a `sold_to` column, writing
`- sold_to: customer_id` under `include_unmapped: true` would put two `sold_to`
columns in the file and readers would resolve the wrong one. Rename the mapped
column, exclude the upstream one, or set `include_unmapped: false`.

Where the compiler cannot enumerate the upstream columns, the same collision
reaches the run. The mapped value wins -- the block is your explicit statement
of what the file carries -- and the displaced upstream column is named in a
**W366** warning at the end of the run. Applying one of the three fixes above
silences it.

#### Diagnostics

A `mapping:` item naming a column that does not exist at that point in the
pipeline is rejected at compile time (**E365**), with the available column list
and a `did you mean` when the name is a near miss. Nothing is renamed silently.

The compiler cannot always see the column set. Inside a composition body the
rows are open by construction, and under `on_unmapped: auto_widen` a column can
reach the sink through the sidecar without being declared anywhere. There an
item naming an unknown column compiles even when its name resembles a declared
column: spelling similarity cannot prove that a dynamic field is absent.
**W365** reports it after the run if no written record supplied it.

What catches the rest is the end of the run: if no record supplied an item's
source column, that item wrote an empty column in every row, and the run reports
it as **W365**, naming the column to correct. An item some records supply and
others do not is a sparse column, not a mistake, and is not reported.

Both **W365** and **W366** are advisory. They print to standard error when the
run finishes and do not change the exit code -- the file is written and readable
either way, and by the time a stream ends the run's other outputs have already
been flushed.

A column absent from the source's `schema:` reaches the sink only through the
`auto_widen` sidecar, which is expanded to top-level columns only under
`include_unmapped: true`. A `mapping:` item may name such a column when that
flag is set; under `include_unmapped: false` it cannot resolve and is rejected
at compile time.

An empty block -- `mapping: {}` or `mapping: []` -- is rejected (**E364**): it
declares an output with no columns. To write every upstream column, remove the
`mapping:` key rather than emptying it.

Writing the block as a YAML map instead of a sequence is rejected (**E364**);
the message prints your own block already rewritten. Run `clinker explain --code E364`
for the migration, and read the direction note there before pasting: releases
before this one documented `output_name: source_field` but *executed* the
reverse, so the rewrite swaps each pair's two sides to preserve what the
pipeline was actually writing.

### Excluding fields

Remove specific fields from output:

```yaml
    exclude: [internal_id, _debug_flag, temp_calc]
```

`exclude:` matches **incoming** column names, and runs before `mapping:`. Two
consequences:

- The columns that survive keep their relative order. Upstream `a, b, c, d` with
  `exclude: [b]` writes `a, c, d`.
- Naming a column that a `mapping:` item also *produces* is not a conflict --
  the exclusion removes the upstream column of that name and leaves the mapped
  one standing. That is the fix for the two-columns-under-one-header collision
  above: `- sold_to: customer_id` with `exclude: [sold_to]` writes one `sold_to`
  column, carrying `customer_id`'s value.

Excluding a column a `mapping:` item *reads* is a different matter, and is
rejected (**E364**): the exclusion removes the column before the item can read
it, so the item could never resolve.

### Header control (CSV)

```yaml
    include_header: true      # Default: true
```

Set to `false` to omit the CSV header row.

### Null handling

```yaml
    preserve_nulls: false     # Default: false
```

When `false`, null values are written as empty strings. When `true`, nulls are preserved in the output format's native null representation (e.g., `null` in JSON).

### Rounding decimals to a declared scale

An Output node's optional `schema:` may declare a column `type: decimal` with a
`scale`. A `decimal` value landing in that column is rounded to the declared
number of fractional places on write, using banker's rounding — the same
boundary contract a `decimal` *source* column applies on read.

```yaml
    schema:
      - { name: dept,    type: string }
      - { name: total,   type: decimal, scale: 2 }
      - { name: average, type: decimal, scale: 2 }
```

Decimals compute at full precision *inside* the pipeline (division and `avg`
keep every digit), so a declared output scale is how you pin a computed result
to fixed places at the sink: `avg(amount)` over `1.00, 1.00, 2.00` writes `1.33`
into a `scale: 2` column, while `sum(amount)` — already at scale 2 — stays
`4.00`. This works for every format (CSV, JSON, fixed-width); an output column
with no declared scale, or an output with no `schema:` block at all, keeps the
full-precision value. Only `decimal` values in `decimal`-declared columns are
affected — no other type is coerced. See [Decimal — arithmetic
rules](../cxl/types.md#arithmetic-rules) for the full boundary-contract model.

The same rounding applies to an Output node declared inside a
[composition](../pipelines/compositions.md) body. When its `schema:` names an
external `.schema.yaml` file, the path resolves relative to the composition
file's own directory (not the invoking pipeline's).

## Output format options

### CSV

```yaml
- type: output
  name: csv_out
  input: processed
  config:
    name: csv_out
    type: csv
    path: "./output/result.csv"
    options:
      delimiter: "|"
```

`delimiter` is a single byte on the wire, so it must be **exactly one ASCII
character** (for example `,`, `|`, or `\t`). An empty, multi-character, or
non-ASCII value is rejected at plan validation rather than silently truncated
to its first byte.

### JSON

```yaml
- type: output
  name: json_out
  input: processed
  config:
    name: json_out
    type: json
    path: "./output/result.json"
    options:
      format: ndjson           # array | ndjson
      pretty: true             # Pretty-print JSON
```

- `array` (default) -- writes a single JSON array containing all records.
- `ndjson` -- writes one JSON object per line.

JSON numbers cannot represent non-finite floats; a record carrying `NaN` or
an infinity fails the write with a JSON error instead of silently becoming
`null`. See [JSON Format](../formats/json.md#non-finite-floats).

### XML

```yaml
- type: output
  name: xml_out
  input: processed
  config:
    name: xml_out
    type: xml
    path: "./output/result.xml"
    options:
      root_element: "data"
      record_element: "row"
      attribute_prefix: "@"    # emit @-prefixed fields as XML attributes
```

Fields whose final path segment carries the `attribute_prefix` (default
`@`, matching the XML source option) are emitted as XML attributes of
their enclosing element, so attribute fields read from an XML source
round-trip. See [XML Format](../formats/xml.md#writing-xml) for details.

### Fixed-width

```yaml
- type: output
  name: fw_out
  input: processed
  config:
    name: fw_out
    type: fixed_width
    path: "./output/result.dat"
    schema: "./schemas/output.schema.yaml"
    options:
      line_separator: crlf
```

Fixed-width output requires a format schema defining field positions and
widths. Fields land at their declared byte ranges with gaps space-filled —
see [Fixed-Width Format](../formats/fixed-width.md#writing-fixed-width-output)
for the layout semantics.

### EDIFACT

```yaml
- type: output
  name: edi_out
  input: messages
  config:
    name: edi_out
    type: edifact
    path: "./out/result.edi"
    options:
      interchange: ["UNOA:1", "SENDER", "RECEIVER", "240101:1200", "REF1"]
      message_type: "ORDERS:D:96A:UN"
      write_una: false
      segment_newline: true
```

The EDIFACT writer reconstructs the interchange envelope around emitted
records, recomputing the `UNT`/`UNZ` control counts and echoing the
control references, and release-escapes any element data that carries a
service character. The `UNB` header comes from `interchange` (literal
elements) or `interchange_from_doc` (echoed from a `$doc` section). An
interchange is a single envelope, so an `edifact` output cannot be
combined with a `split:` block — the combination is rejected at
config-validation time (`E323`). See [EDIFACT Format](../formats/edifact.md) for the
full option reference, the record schema, and the round-trip semantics.

### HL7 v2

```yaml
- type: output
  name: hl7_out
  input: messages
  config:
    name: hl7_out
    type: hl7
    path: "./out/result.hl7"
    options:
      file_header: ["^~\\&", "LAB", "HOSP", "EHR", "HOSP", "20240102", "FILE7"]
      batch_header: ["^~\\&", "LAB", "HOSP", "EHR", "HOSP", "20240102", "BATCH3"]
      segment_newline: true
```

The HL7 writer re-emits the `MSH` and body segments from the record
stream, escaping any field data that carries a delimiter character (`|` →
`\F\`, `^` → `\S\`, and so on). When a `file_header` (or
`file_header_from_doc`) or `batch_header` is configured the writer wraps the
messages in an `FHS..FTS` file or `BHS..BTS` batch and recomputes the
closing `BTS`/`FTS` counts. A batch/file envelope is a single structure, so
an `hl7` output cannot be combined with a `split:` block — the combination
is rejected at config-validation time (`E339`). See
[HL7 v2 Format](../formats/hl7.md) for the full option reference, the record schema,
the MSH off-by-one, and the round-trip semantics.

## Sort order

Sort records before writing:

```yaml
    sort_order:
      - { field: "name", order: asc }
      - { field: "amount", order: desc, null_order: last }
```

| Sort option | Values | Default |
|-------------|--------|---------|
| `order` | `asc`, `desc` | `asc` |
| `null_order` | `first`, `last`, `drop` | `last` |

- `first` -- nulls sort before all non-null values.
- `last` -- nulls sort after all non-null values.
- `drop` -- records with null sort keys are excluded from output.

`drop` removes records, so a run using it writes fewer records than it read
and that is not a fault. A missing column counts as a null key: a record that
never carried the sort field is dropped the same as one carrying an explicit
null. With several dropping fields, a record is excluded if *any* of its keys
is null, and counts once however many of them are.

The excluded records are counted, separately from `records_dlq` and from
filter losses, so a short output can be attributed rather than guessed at. A
run that dropped any reports the number on completion:

```
1234 record(s) excluded by null_order: drop
```

and the same number is written as `records_null_dropped` in the metrics spool
when one is configured (see [Metrics](../ops/metrics.md)).

Under fan-out the count is per exclusion, not per source record: two Outputs
that each declare a dropping `sort_order` each drop their own copy, so one
source record excluded at both counts twice — the same multiplicity
`records_written` carries. Subtracting this from `records_total` is therefore
only sound on a pipeline with a single dropping Output.

Nothing else records these records. Unlike a DLQ entry, a dropped record
leaves no artifact to inspect afterwards -- if you need to see which records
were removed rather than only how many, route them out with a filter before
the sort instead of declaring `drop`.

Shorthand: a bare string defaults to ascending with nulls last:

```yaml
    sort_order:
      - "name"
      - { field: "amount", order: desc }
```

An Output `sort_order` materializes all records that reach that terminal and
re-establishes one order across them, including records from several physical
files or Merge inputs. The guarantee is exactly the authored field sequence,
direction, and null placement. `drop` is also part of the authored contract:
records with a null in a sort key do not reach the writer.

The sort is stable. Equal authored keys retain their upstream arrival order
within a given execution path, and the same path produces the same bytes in
resident and forced-spill operation. Clinker does not add a source-row,
filename, or canonical-record tie-breaker. If upstream strategies can produce
different arrival orders, equal-key rows have no cross-strategy relative-order
promise. Author enough fields for a total business order before using an exact
byte comparison; otherwise validate the decoded record multiset and aggregate
values instead.

### Physical writer boundaries

Planning derives the writer boundary from the finalized graph, not from how
many Output nodes appear in the YAML. The same ordering promise is therefore
enforced at every physical byte-emission path:

- ordinary single-file and split-file record output;
- one output per physical source file;
- reconstructed envelope output per document;
- document DLQ output after the whole document is known to be clean;
- deferred output per correlation group; and
- incremental streaming output.

Complete-population modes apply the exact authored key at their population
boundary using the same bounded-memory spill path. Incremental streaming
cannot truthfully promise a terminal whole-population sort. If a finalized
output mode is incompatible with an authored `sort_order`, planning rejects
the pipeline instead of weakening the promise. The diagnostic names the
Output, mode, authored keys, and last reordering stage, and includes a corrected
`sort_order` form that can be pasted into the source or upstream node.

## File splitting

Split output into multiple files based on record count, byte size, or group boundaries:

```yaml
- type: output
  name: split_output
  input: processed
  config:
    name: split_output
    type: csv
    path: "./output/result.csv"
    split:
      max_records: 10000
      max_bytes: 10485760           # 10 MB
      group_key: "department"       # Never split mid-group
      naming: "{stem}_{seq:04}.{ext}"
      repeat_header: true           # Repeat CSV header in each file
      oversize_group: warn          # warn | error | allow
```

### Split configuration fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `max_records` | No | -- | Soft record count limit per file |
| `max_bytes` | No | -- | Soft byte size limit per file |
| `group_key` | No | -- | Field name -- never split within a group sharing this key value |
| `naming` | No | `"{stem}_{seq:04}.{ext}"` | File naming pattern. It must contain exactly one `{seq:NN}` token, where `NN` is a decimal width from 1 through 20. `{stem}` is the base name and `{ext}` is the file extension. |
| `repeat_header` | No | `true` | Repeat CSV header row in each split file |
| `oversize_group` | No | `warn` | What to do when a single key group exceeds file limits |

At least one of `max_records` or `max_bytes` should be specified for splitting to have any effect.

The naming grammar is strict: `{stem}`, `{ext}`, and the one required
`{seq:NN}` token are the only placeholders. Unknown placeholders, a bare
`{seq}`, non-numeric or out-of-range widths, and duplicate or missing sequence
tokens are rejected during configuration validation. For example,
`{stem}_{seq:03}.{ext}` renders sequence 7 as `007`.

For formats whose output wraps the whole file in framing -- a JSON array or an XML root element -- each split file is a complete, independently valid document: the framing is closed at rotation and reopened for the next file.

### Oversize group policies

- `warn` (default) -- log a warning and allow the oversized file.
- `error` -- stop the pipeline.
- `allow` -- silently allow the oversized file.

When `group_key` is set, the split point is the first group boundary after the threshold is reached (greedy). Without `group_key`, files are split at the exact limit.

## Streaming writes after an interleave Merge

When a single Output sits directly after a `Merge` with `mode: interleave` whose inputs are all Sources, records are written to disk as they arrive rather than being buffered until the merge finishes. This keeps memory flat and lets a slow writer naturally pace the upstream readers.

```yaml
- type: source
  name: src_a
  config: { type: csv, path: a.csv, schema: ... }
- type: source
  name: src_b
  config: { type: csv, path: b.csv, schema: ... }
- type: merge
  name: merged
  inputs: [src_a, src_b]
  config:
    mode: interleave        # required
- type: output
  name: out
  input: merged
  config:
    name: out
    type: csv
    path: out.csv
```

This is automatic — there is no setting to enable it. It applies only to this
exact shape: one interleave Merge of Sources feeding one non-splitting Output,
in a pipeline without correlation keys. Any other topology buffers as usual.
Both paths preserve the same record multiset and writer semantics, but an
unseeded interleave does not promise one exact cross-input row sequence. Add an
Output `sort_order` with a total business key when exact bytes are required.

## Complete example

```yaml
- type: output
  name: department_reports
  input: enriched_employees
  config:
    name: department_reports
    type: csv
    path: "./output/employees.csv"
    # `include_unmapped: false` makes the mapping the whole output: these four
    # columns, in this order, and nothing else. Without it every unlisted
    # upstream column would still be appended after them, and an `exclude:`
    # would be needed to keep any of them out.
    include_unmapped: false
    mapping:
      - "Employee ID": employee_id
      - "Full Name": display_name
      - department
      - "Annual Salary": salary
    include_header: true
    sort_order:
      - { field: "department", order: asc }
      - { field: "display_name", order: asc }
    split:
      max_records: 5000
      group_key: "department"
      naming: "employees_{seq:03}.csv"
      repeat_header: true
```
