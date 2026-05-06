# Cross-Record Document-Level Transforms

Design plan for adding document-level cross-record transforms to clinker:
two new node variants (`Reshape`, `Cull`), a planner-wide skinny-pass stats
facility, and a typed analytic-window expression surface. Targets the
SCD-Type-2-shaped use case of "create missing records and/or modify trigger
rows based on criteria observed across other records in the same correlation
group" while honouring the existing memory budget and DLQ + correlation-key
rollback contracts.

Status: design locked, not yet implemented. This document is the single
source of truth for the sprint arc; per LD-011 the rip is full and is
sealed at the end of each sprint.

---

## 1. Motivation

Clinker today is row-shaped. The unified node taxonomy
(`Source`/`Transform`/`Aggregate`/`Route`/`Merge`/`Combine`/`Output`/`Composition`)
covers projection, filtering, hash/streaming aggregation, fan-out, equi+range
N-ary join, and reusable sub-pipelines — but every operator's per-record
output count is bounded by, or aggregated from, its per-record input.

Four real-world ETL shapes do not fit this model:

1. **Create missing records** — given a correlation group, observe what records
   exist and synthesize the records that *should* exist but don't. Requires
   group-level observation before per-record action.
2. **Mutate trigger rows alongside synthesis** — when synthesizing a missing
   record, simultaneously modify the existing record(s) whose state caused the
   synthesis to fire. SCD Type 2's canonical pattern: set the previous record's
   `end_date`/`is_current` while inserting the new historical row.
3. **Remove records by group predicate** — drop records (or whole groups) when
   a CXL predicate evaluated over the group is true. Dedupe-keep-latest,
   "drop the whole order if any line item errored", and similar.
4. **Both create and remove driven by observed state** — neither shape can be
   expressed without first observing the group. The skinny-pass already
   present in the analytic-window machinery is the right structural answer;
   it just needs to be generalized beyond its current single consumer.

## 2. Scope

In:

- New node `Reshape`: rule-based per-group synthesis + trigger-row mutation.
- New node `Cull`: per-group record removal with first-class side-output port.
- `IndexSpec` generalized from "analytic-window-only" to a planner-wide
  stats-and-projection facility driven by YAML field analysis.
- Typed `AnalyticWindowSpec` (rip the current opaque `serde_json::Value`).
- Group-level CXL predicates (`any`, `every`, `count`, `sum`, `max`, `min`,
  `exists`, `not_exists`) usable in Reshape rules, Cull predicates, and analytic
  windows.
- New DLQ category `MutationConflict`; new stage labels `reshape:<node>:<rule>`
  and `cull:<node>` on `DlqEntry.stage`.
- Reference pipeline at `examples/pipelines/employee_plan_backfill.yaml`
  exercising the SCD-Type-2 shape end-to-end.

Out (deferred to later phases, *not* shimmed for forward compatibility):

- `MATCH_RECOGNIZE`-style row-pattern matching with NFA execution.
- Watermarks, late-data handling, event-time vs processing-time semantics.
- Linear interpolation / non-LOCF derived fills.
- Bi-temporal versioning (`valid_time` vs `transaction_time`).
- Late-arriving dimension "default member" sentinels.
- Mutation cascade (rules seeing earlier rules' effects). See §6.
- Cross-CK reasoning ("drop record if any record in a *different* group has
  property X"). Use a `Combine` upstream of `Cull` for this.

## 3. Operator surface

### 3.1 `Reshape`

```yaml
- reshape: backfill_and_fix_plans
  partition_by: [employee_id]
  order_by:                            # required when any rule uses first/last
    - [updated_at, desc]
    - [source_row, asc]
  rules:
    - name: fix_long_plan_years
      when:                            # group-level CXL predicate
        all_of:
          - any(plan_start_date - plan_year_end_date > interval '1 year')
          - not exists(plan_date == '2024-01-01' and status == 'baseline')
      do:
        - mutate_rows:                 # modify rows matching selector
            select: plan_start_date - plan_year_end_date > interval '1 year'
            set:
              plan_year_end_date: plan_start_date + interval '1 year'
        - synthesize:                  # add new row(s)
            copy_from: trigger         # rows matched by mutate_rows.select
            tie_break: first           # required when copy_from yields >1 row
            overrides:                 # field-level overrides on top of copy
              plan_date: '2024-01-01'
              status: 'synthesized'
              audit_id: $synth.uuid()
```

#### Rule semantics

- `when` is a CXL boolean expression over the group. Group-aggregate
  functions (`any`, `every`, `count`, `exists`, …) are first-class; bare
  scalar references resolve against group columns; per-row references inside
  group aggregates use `record.<field>` form.
- `do:` is an ordered list of action blocks but **the order does not affect
  semantics** — see §6 (no cascade). Order in YAML is purely a stylistic
  authoring choice. The planner is free to reorder for execution.
- All rules in a Reshape node evaluate against the **same observation** of
  the group, computed once during the skinny pass. Mutations from earlier
  rules are not visible to later rules' predicates. To sequence
  dependent transformations, chain two `Reshape` nodes in the DAG.

#### `copy_from` enum

| Form | Determinism | Compile-time check |
|---|---|---|
| `first(order_by: [...])` | deterministic | `order_by` covers strict total order |
| `last(order_by: [...])` | deterministic | as above |
| `trigger` | deterministic if `mutate_rows.select` matches one row, else needs `tie_break` | tie_break required when selector may match >1 |
| `select_where(<cxl>, tie_break: first(...))` | deterministic with tie_break | tie_break required if predicate may match >1 |
| `any` | non-deterministic; planner picks | warning emitted |
| `none` | n/a; every field must appear in `overrides` | error if any field omitted |

`overrides` accepts: literal constants, schema-defaults (sentinel
`$default`), `null`, CXL expressions referencing the chosen sibling
(`sibling.<field>`) or constants (`$synth.uuid()`, `$pipeline.run_id`).

#### Atomicity, observability, restrictions

- Synthesized rows are stamped `$meta.synthetic = true` and
  `$meta.synthesized_by = "<node>:<rule>"`.
- Mutated rows accumulate `$meta.mutated_by = ["<node>:<rule>", ...]`.
- Synthetic records inherit the partition group's CK (`$ck.*`) — not minted
  fresh; they are members of the existing group and commit/rollback with it.
- `mutate_rows.set` cannot reference `$ck.*` or any column in
  `partition_by` — compile-time error. Otherwise records escape their group
  mid-pipeline and break atomic commit.
- A row selected by both `mutate_rows.set` of rule A and rule B must not
  receive conflicting writes to the same field. Statically detectable
  conflicts are compile-time errors; conflicts that depend on row content
  emit a `MutationConflict` DLQ entry at runtime, the whole CK group is
  rolled back per existing `CorrelationCommit` semantics.
- A row selected by both `cull` (in a downstream Cull node) and
  `mutate_rows` is mutated; cull happens later. If both happen in the same
  pipeline stage (separate nodes) the planner emits a warning when the
  selectors statically overlap.

### 3.2 `Cull`

```yaml
- cull: dedupe_and_drop_failed_orders
  partition_by: [order_id]
  rules:
    - name: drop_orders_with_any_error
      drop_group_when: any(status == 'error')
    - name: dedupe_keep_latest_line
      keep_row_where:
        rank_in_group(partition_by: [order_id, line_no],
                      order_by: [updated_at desc]) == 1
      tie_break: first
  removed_to: dropped_order_lines      # first-class side output port
```

- `drop_group_when` and `keep_row_where` are mutually exclusive within a
  single rule but a Cull node can hold a list of rules; effects compose
  intersectively (a row survives iff every rule keeps it).
- `removed_to` is a node-level output port (Talend `tMap` reject pattern,
  Flink `OutputTag`). It is **not** the DLQ. Records routed through
  `removed_to` are not failures; they are expected business-logic
  removals. Rationale and semantics: §7.
- `tie_break` is configurable, default `first`. With `keep_row_where` using
  `rank_in_group`, the rank function's `order_by` must be a strict total
  order (compile-time check); `tie_break` then resolves any residual ties.

## 4. Skinny-pass generalization

Today `IndexSpec` is built only when a `Transform` node has an
`analytic_window`. The skinny pass extracts the columns those window
expressions need into a columnar `Arena` plus a `SecondaryIndex` of partition
slices. Two new consumers (`Reshape`, `Cull`) make this a planner-wide
facility.

### 4.1 Field analysis (YAML-driven)

The compiler walks each Reshape/Cull/Transform-with-window node, computes
the **referenced field set** (partition keys, predicate operands, fill
sources, tie-break orderings, group-aggregate inputs), and emits an
`IndexSpec` whose arena projects only those columns. Cost is
`O(N × Σ widths(referenced fields))`, not `O(N × full row width)`.

### 4.2 Stats gathered in the same pass

- `row_count`
- `null_count` per indexed column
- `min`, `max` per indexed column with an ordered type
- `distinct_count_estimate` via HyperLogLog (14-bit registers, ±1% error,
  ~16KB per column)
- `top_k_keys` via Misra-Gries (k=64 default; configurable per node).
  Drives skew detection; downstream operators consult to decide
  grace-partitioning vs in-memory hash.
- `observed_keys` — full set of partition-key tuples. For `Reshape`'s
  anti-exists checks. Bounded by `correlation_max_group_buffer` (existing
  config); on overflow, falls back to a Bloom filter (10 bits/key for 1%
  FPR) plus a `GroupSizeExceeded`-style DLQ entry recorded against the
  pipeline run, not against any group.

### 4.3 When the skinny pass is skipped

Planner heuristic per Spark AQE: skip the skinny pass entirely when
`estimated_input_size < 2 × memory_budget` AND no node downstream needs
group-level observation. If at least one Reshape/Cull/window node is
present the skinny pass runs unconditionally; the planner only chooses to
skip its *stats* (not its index).

### 4.4 Hysteresis on spill thresholds

The existing `MemoryBudget::should_spill()` flips on at the soft threshold
(80% of hard limit). Cross-record operators trigger this much more often
than per-record transforms; without hysteresis, the pipeline ping-pongs
in/out of spill at every batch. Generalization adds a sticky 20% raise:
once spilling, the threshold to *stop* spilling is `soft + 0.20 × (hard − soft)`.

## 5. Typed `AnalyticWindowSpec`

The current shape — `analytic_window: Option<serde_json::Value>` — is opaque
plumbing without semantics. `keep_row_where` in Cull requires
`rank_in_group()` / `count_in_group()` etc. to actually exist as CXL
functions. The fix is a typed `AnalyticWindowSpec` enum with:

- `partition_by: Vec<CxlSource>`
- `order_by: Vec<(CxlSource, SortDir)>`
- `frame: WindowFrame { start: FrameBound, end: FrameBound, mode: Rows | Range }`

Frame modes in v1: `Rows` only. `Range` is a v2 follow-on. (Most analytic
needs are `Rows` over a sorted partition; the `Range` mode's "between
observed values" semantics interact badly with the no-watermark constraint.)

Window functions exposed in v1 CXL:

- `row_number()`, `rank()`, `dense_rank()` — ranking
- `lag(expr, n)`, `lead(expr, n)` — neighbourhood
- `first_value(expr)`, `last_value(expr)` — partition-bounded
- `count(expr)`, `sum(expr)`, `min(expr)`, `max(expr)`, `avg(expr)` — running
  aggregates within the frame
- `any(predicate)`, `every(predicate)`, `exists(predicate)`,
  `not_exists(predicate)` — group-level booleans (frame = entire partition)

The opaque `serde_json::Value` shape is removed in the closing commit of
the sprint that does this work; per LD-011 there is no parallel coexistence.
The RIP-LOG records the deletion.

## 6. Mutation cascade — explicitly *not* offered

Rules within a Reshape node evaluate against the original group state;
later rules do not see earlier rules' effects. Reasons (in priority order):

1. **Determinism.** With cascade, YAML rule order silently changes output.
   ETL pipelines run unattended; this is the worst class of bug.
2. **Skinny-pass invariant.** Without cascade, all decisions are computed
   from one observation. Cascade requires either fixpoint iteration
   (unbounded) or interleaved skinny+wide passes per rule (loses the
   amortization the skinny pass exists for).
3. **Plan-time analysis.** Field-projection analysis depends on knowing
   which fields each rule reads. Cascade makes that order-sensitive.
4. **Conflict detection.** Without cascade, "rule A and rule B write the
   same field on the same row" is a static union-of-selectors check. With
   cascade, rule B's selector can depend on rule A's mutation, so
   detection requires symbolic execution.
5. **Debuggability.** Per-rule diagnostics suffice when rules are
   independent. With cascade, a misbehaving row needs full execution trace.
6. **Clean escape hatch.** When a transform genuinely needs sequencing,
   the user chains two `Reshape` nodes in the DAG. Explicit, visible in
   `explain`, independently testable. One extra skinny pass per cascade
   step, paid only by users who actually need it.

This is the architecturally correct default. Adding a `cascade: true` flag
later would double the failure-mode matrix and split the codebase into two
evaluation models. Don't.

## 7. `removed_to` vs DLQ

These are structurally similar (tag-and-divert) but semantically distinct.
Conflating them costs observability.

| | DLQ | `removed_to` |
|---|---|---|
| Triggered by | failure (coercion error, validation, group overflow, …) | expected business rule |
| Implies | "investigate" | "rule fired N times today, as designed" |
| Metric semantics | alarm | normal KPI |
| Carries | error category, error message, originating stage, trigger flag | record + cull node + rule name |
| Recovery model | reprocess after fixing data | flow downstream as own output |
| Schema | `DlqEntry` wrapper | original record schema |

If `removed_to` were folded into DLQ as a `Removed` category, "DLQ rate
above threshold" alarms would page ops every time a dedupe rule fires.
First-class side output is the right shape; matches Talend `tMap`'s reject
port and Flink's `OutputTag` precedent.

Implementation note: `removed_to` extends `ExecutionPlanDag` with named
output ports per node. Today every node has exactly one logical output
edge per downstream consumer; this expands to `(NodeId, PortName) →
edges`. `Route` already does fan-out by predicate but produces unnamed
branches keyed by condition name; the new shape unifies the addressing.

## 8. Sprint sequencing

Five sprints, sealed at each boundary per LD-011 (no `Legacy*`, no
`#[allow(dead_code)]`, no `#[ignore]`, no parallel coexistence, no
"pragmatic deferred cleanup"). Every `pub` / `pub(crate)` item the sprint
introduces has a non-test intra-crate caller in the closing commit.

### Sprint 1 — `AnalyticWindowSpec` de-opaqueing

- Replace `analytic_window: Option<serde_json::Value>` with typed
  `AnalyticWindowSpec` enum + `WindowFrame { start, end, mode: Rows }`.
- CXL surface: register `row_number`, `rank`, `dense_rank`, `lag`, `lead`,
  `first_value`, `last_value`, `count`/`sum`/`min`/`max`/`avg` over a
  window, plus the group-level booleans `any`/`every`/`exists`/`not_exists`.
- Type inference recognises window-only call sites; existing
  `WindowRuntime` evaluator gets concrete dispatch instead of opaque-value
  threading.
- Existing analytic-window users in `examples/pipelines/` migrated.
- RIP-LOG entries for the deleted opaque-value path.
- Closing-commit invariants: no `serde_json::Value` on window types; every
  newly-public window function/struct has a non-test caller; CI green
  including benches.

### Sprint 2 — Skinny-pass generalization

- `IndexSpec` becomes a planner-wide facility:
  - Field-analysis pass walking all node configs to compute referenced
    field sets per source.
  - Stats catalog: `row_count`, `null_count`, `min`/`max`,
    HyperLogLog distinct count, Misra-Gries top-K (k=64), observed-keys
    set with Bloom-filter fallback.
  - Hysteresis on `should_spill()` (sticky 20% raise).
- Planner uses the AQE-style heuristic to decide whether to gather stats
  in the skinny pass (always runs the index when any consumer needs
  group-level observation).
- The `analytic_window` consumer that drove the previous `IndexSpec`
  becomes one consumer among several; its call site is unchanged
  externally but reads from the generalized facility.
- Closing-commit invariants: no node-type-specific code paths in
  `plan/index.rs`; the field-analysis pass is the single producer of
  `IndexSpec`s; existing window benchmarks regression-tested.

### Sprint 3 — `Reshape` node

- New `PipelineNode::Reshape` variant in
  `crates/clinker-core/src/config/pipeline_node.rs`:
  `ReshapeBody { partition_by, order_by, rules: Vec<ReshapeRule> }`,
  `ReshapeRule { name, when, do: Vec<ReshapeAction> }`,
  `ReshapeAction::MutateRows | ReshapeAction::Synthesize`.
- Compile-time validators:
  - `mutate_rows.set` may not reference `$ck.*` or partition-key columns.
  - `copy_from: first/last` requires `order_by`.
  - `copy_from: select_where` requires `tie_break` if predicate may match >1.
  - `copy_from: none` requires every field in `overrides`.
  - Static `MutationConflict` detection across rules.
- Runtime:
  - Reshape consumes the per-group skinny-pass observation, evaluates each
    rule's `when` predicate, computes per-row decisions
    (`{mutate_set, synthesize_template}`), then streams the wide pass
    applying decisions.
  - Synthetic rows allocated against the existing arena/budget; charged
    bytes accounted via `arena_bytes_charged`.
  - Synthetic records stamped `$meta.synthetic`, `$meta.synthesized_by`.
  - Mutated records stamped `$meta.mutated_by` (append).
  - Runtime `MutationConflict` → DLQ with category `MutationConflict`,
    stage `reshape:<node>:<rule_a>+<rule_b>`, both attempted values in
    the error message; whole CK group rolled back per `CorrelationCommit`.
- `NodeProperties` propagation: Reshape destroys ordering iff any rule
  synthesizes; preserves partitioning. Documented in
  `plan/properties.rs`.
- Closing-commit invariants: every new `pub`/`pub(crate)` item has a
  non-test caller; no opaque value types; `cargo deny check` passes.

### Sprint 4 — `Cull` node + side-output port

- New `PipelineNode::Cull` variant:
  `CullBody { partition_by, rules: Vec<CullRule>, removed_to: Option<PortName> }`,
  `CullRule { name, predicate: CullPredicate, tie_break }`,
  `CullPredicate::DropGroupWhen | CullPredicate::KeepRowWhere`.
- Generalize `ExecutionPlanDag` edge addressing to `(NodeId, PortName)`.
  All existing consumers updated (Route's branch names become first-class
  port names; default port name `"main"` everywhere else).
- Runtime: streaming filter per partition, sort-aware fast path when
  upstream `NodeProperties.ordering` covers `partition_by`. Skew handling
  via Misra-Gries top-K from the skinny pass: hot keys grace-partitioned
  using existing `pipeline/grace_spill.rs` machinery.
- `removed_to` writer registration parallel to main output;
  per-node-per-port byte/record counters surfaced in run-end summary.
- Compile-time: `keep_row_where` referencing `rank_in_group` requires
  strict total order on the rank function's `order_by`.
- Closing-commit invariants: no double-edged data flow (the DAG either
  uses ports everywhere or doesn't); deleted dead ad-hoc edge paths
  recorded in RIP-LOG.

### Sprint 5 — Reference pipeline + integration tests

- `examples/pipelines/employee_plan_backfill.yaml`:
  - Source: employee plan-history CSV (a fixture under
    `examples/pipelines/data/employee_plans.csv`).
  - Reshape: SCD-Type-2 fix-and-synthesize per the §3.1 example.
  - Cull: dedupe by `(employee_id, plan_date)`, keep latest by
    `updated_at`, rejected rows to `removed_to`.
  - Output: corrected plan-history table.
- Integration tests in `crates/clinker-core/tests/` exercising:
  - Happy path: trigger fires, mutation + synthesis produce expected output.
  - Existence check passes: no synthesis, no mutation.
  - Mutation conflict between two rules: DLQ entry, group rolled back,
    no partial output downstream.
  - Empty group / single-row group / huge group (skew) — last drives the
    grace-partition path.
  - Cull dedupe with deterministic tie-break: removed records flow to
    `removed_to`, kept record matches expected.
  - Idempotency check: running the same pipeline twice over the same
    input is a no-op (predicates are self-fulfilling once synthetic
    record exists).
- Snapshot tests via `insta` on the `clinker run --explain` output of the
  reference pipeline so plan-shape regressions are visible.
- Benchmark in `clinker-benchmarks` covering skinny-pass stats overhead
  vs the savings on the wide pass for a representative skewed input.

## 9. Failure modes addressed

In priority order with the mitigation each maps to.

1. **Hash skew on hot CK** — Misra-Gries top-K from skinny pass routes
   hot keys through grace-partition spill (existing
   `pipeline/grace_spill.rs`).
2. **Reduce-then-explode antipattern** — Reshape and Cull never
   materialize a whole group into a `Vec`. Skinny pass produces per-group
   decisions of bounded size; wide pass streams. Trait-level prohibition
   (operators take a streaming iterator, not a slice).
3. **Synthetic records with no `source_row`** — DLQ stage label
   `reshape:<node>:<rule>` carries the originating rule. CK is the
   partition group's existing CK; failures roll back the whole group atomically.
4. **Non-deterministic tie-breaks** — `first`/`last`/`select_where`/`trigger`
   require explicit `order_by` or `tie_break` at compile time; `any` emits
   a warning. Catches the SQL `ROW_NUMBER`-on-ties trap statically.
5. **MutationConflict** — static detection via union-of-selectors; runtime
   DLQ category for content-dependent collisions; whole-group rollback.
6. **Always-spilling pathology** — hysteresis on `should_spill()`.
7. **Cull-rate masquerading as DLQ-rate** — `removed_to` is its own port
   with its own counter, separate from DLQ alarms.
8. **Effective-dating gaps (SCD2 lesson)** — predicate language allows
   cross-action references (`new.start_date = trigger.end_date + 1d`) so
   mutation and synthesis are calculated against each other, not in
   isolation. Documented prominently in user-facing reference.
9. **Cartesian explosion on synthetic key spaces** — skinny-pass
   cardinality estimate at compile time; planner refuses (or warn-and-
   spill) above a threshold (configurable, default 100M synthetic rows
   per Reshape node).
10. **Lost updates on replay** — sink-side responsibility, not the
    transform's. Flagged loudly in `clinker run --explain` when a sink
    doesn't support transactional/idempotent writes and a Reshape node
    feeds it.
11. **CK / partition-key mutation** — compile-time error.
12. **Mutation cascade illusion** — documented absence (§6); no flag, no
    silent-ordering footgun.

## 10. Prior art references

- **SQL window functions** — `OVER (PARTITION BY ... ORDER BY ...)` evaluation
  model (sort-then-stream with frame buffer); DuckDB 0.9+ segment-tree
  windows for sub-quadratic wide-frame cost.
- **SQL `MATCH_RECOGNIZE`** (SQL:2016) — row-pattern matching; deferred to
  v2 but the right semantic model when it lands.
- **SQL `MERGE`** — multi-source-matches-single-target ambiguity; we
  deliberately reject the ambiguous shape via `tie_break` requirements.
- **Apache Flink keyed state + RocksDB backend** — bounded heap with
  disk-spilled state; matches our existing spill story.
- **Apache Beam stateful DoFn + `OutputTag`** — side-output pattern that
  validates `removed_to` as a first-class port.
- **Materialize first-class error relations** — DLQ as queryable, not
  log-buried; informs how Reshape/Cull DLQ entries must surface in
  `clinker run` output and `explain`.
- **Informatica `Update Strategy` transformation** — row-level INSERT/
  UPDATE/DELETE/REJECT tagging; closest prior art for the `do:` action
  sub-language.
- **SSIS Slowly Changing Dimension wizard** — 20+ years of production
  lore on the SCD Type 2 shape this plan targets directly.
- **Pentaho `Dimension Lookup/Update`** — single-component SCD2.
- **Talend `tMap` reject port** — first-class side output; precedent for
  `removed_to`.
- **dbt `snapshot` + `unique_key` + `updated_at`** — declarative SCD2
  contract.
- **MongoDB `$densify` + `$fill` + `$setWindowFields`** — closest
  document-DB prior art for "complete the document"; informed the
  per-field merge policy decomposition.
- **Spark AQE (Adaptive Query Execution)** — runtime sample-then-decide;
  informs the skinny-pass-skip heuristic.
- **Snowflake micro-partition statistics** — write-time stats colocated
  with data; informs the YAML-driven field-analysis pass.
- **ClickHouse primary-key index marks**, **Parquet column-chunk
  statistics + bloom filters** — generalization of stats from
  metadata-on-data to a planner facility.

## 11. Reference fixture

Path: `examples/pipelines/employee_plan_backfill.yaml`, with input at
`examples/pipelines/data/employee_plans.csv`.

Schema: `employee_id, plan_id, plan_start_date, plan_year_end_date,
plan_date, status, updated_at, source_row` plus a handful of payload
fields to make `copy_from: trigger` non-trivial.

The pipeline:

1. Reads the CSV, declaring `correlation_key: [employee_id]`.
2. `reshape: backfill_and_fix_plans` per §3.1, with the SCD2 mutate +
   synthesize rule.
3. `cull: dedupe_by_employee_plan` keeping latest by `updated_at`,
   removed records to a side output.
4. Writes the corrected stream to one CSV; the `removed_to` stream to
   another for audit.

Edge cases the fixture/tests cover:

- Employee with one valid record (no rule fires).
- Employee with a long-plan-year record but the specific record already
  exists (rule-A `not exists` clause holds; no synthesis).
- Employee with two long-plan-year records (multiple trigger rows;
  `tie_break: first` resolves which is the synthesis source).
- Two Reshape rules conflicting on a field for the same row → DLQ entry,
  whole employee group rolled back to DLQ.
- Hot employee with 100k records → grace-partition path exercised.
- Idempotent re-run: pipeline output equals first-run output byte-for-byte.

## 12. Sprint-close audit checklist

Run at each sprint boundary; sprint is sealed only when every box is
ticked against the working-tree state at HEAD.

- [ ] `cargo fmt --all` clean
- [ ] `cargo clippy --workspace -- -D warnings` clean
- [ ] `cargo test --workspace` green
- [ ] `cargo check --benches --workspace` clean
- [ ] `cargo check --features bench-alloc -p clinker-benchmarks` clean
- [ ] `cargo test --benches -p clinker-benchmarks` green
- [ ] `cargo deny check` clean
- [ ] No `Legacy*` / `Internal*` / `*Block` rename-instead-of-delete
- [ ] No `#[serde(default)]` on mandatory-post-rename fields
- [ ] No `#[ignore]` on tests verifying a cutover
- [ ] No `#[allow(dead_code)]` outside generated code
- [ ] No tombstone comments
- [ ] No "pragmatic" / "incremental" / "deferred cleanup" justifications
      in commit messages or comments
- [ ] Every new `pub`/`pub(crate)` item has a non-test intra-crate caller
- [ ] No parallel "new + old path" coexistence
- [ ] RIP-LOG appended for every deleted symbol
- [ ] No banned references in source comments or commit messages
      (phase/task/wave labels, LD codes, internal-doc paths,
      deletion tombstones)
- [ ] `/audit-shortcuts --range <sprint-base>..HEAD` clean

## 13. Deliberately not in this plan

- Per-sprint commit-by-commit decomposition. The sprint shape locks
  scope; commit boundaries are an authoring decision made during the
  sprint.
- Concrete CXL grammar for `any`/`every`/`exists`/`not_exists` —
  belongs in the Sprint 1 design notes alongside the parser changes.
- DLQ message-text formatting standards — track in the existing DLQ
  documentation, not here.
- Wire-format changes (postcard schema) for new shadow fields
  (`$meta.synthetic`, `$meta.synthesized_by`, `$meta.mutated_by`) —
  one line in Sprint 3's RIP-LOG entry; the existing `MetadataCapExceeded`
  cap (64 keys) is well above the three additions.
