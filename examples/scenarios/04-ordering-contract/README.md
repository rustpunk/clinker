# 04 — Ordering contract

**two CSV files → verified source order → exact CSV business order.** This
scenario separates two contracts that are easy to conflate: a Source verifies
records independently within each physical file, while an Output may author a
single total order across the whole result.

## Run it

```bash
cargo run -p clinker-scenarios -- gen --scenario 04-ordering-contract
cd examples/scenarios/04-ordering-contract
cargo run -p clinker -- run pipeline.yaml
```

The run completes with 24 total, 24 ok, 24 written, and 0 DLQ records. It emits
exactly one `W307` warning naming `02-needs-repair.csv`; the already ordered
`01-sorted.csv` emits none. The single output matches
[`expected/ordered.csv`](expected/ordered.csv) byte for byte.

## The input contract

The generated inputs share the order declaration:

```yaml
sort_order:
  - account_id
  - batch_seq
```

`01-sorted.csv` already follows that order. `02-needs-repair.csv` deliberately
places `ACCT-300` sequence 4 before sequence 3, creating one adjacent
inversion. The pipeline omits `on_unsorted`, so the default `warn` policy
stably repairs that one physical file before releasing it and reports the
repair once.

The declaration does **not** promise a global `(account_id, batch_seq)` order
across both files. Each file is checked on its own; beginning the second file at
`ACCT-100` after the first ends at `ACCT-600` is valid.

## The output contract

The Output authors a different total business order:

1. `region` ascending
2. `priority` descending
3. `account_id` ascending
4. `batch_seq` ascending
5. `event_id` ascending

Those keys make every row's position explicit. The scenario gate therefore
uses exact bytes, not a multiset comparison: changing terminal ordering,
skipping source repair, losing a record, or duplicating a record all fail the
same committed contract.

## Try changing it

- Add `on_unsorted: error`. The inverted file fails before any of its records
  are released.
- Put sequence 3 before 4 in `02-needs-repair.csv`. The warning disappears and
  the gate detects that its repair case is gone.
- Reverse the Output's priority direction and inspect the exact golden diff.
