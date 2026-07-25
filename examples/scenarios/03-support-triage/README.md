# 03 — Support ticket triage

**csv → csv, json, + dead-letter queue.** Normalise a messy helpdesk export,
send each ticket to exactly one of three queues, and dead-letter the rows that
cannot be processed at all.

Introduces two things scenario 01 does not have: routing, and failure that is
recorded rather than ignored.

## Run it

```bash
cargo run -p clinker-scenarios -- gen --scenario 03-support-triage
cd examples/scenarios/03-support-triage
cargo run -p clinker -- run pipeline.yaml
```

```
Pipeline complete: 60 total, 57 ok, 57 written, 3 dlq
```

**The process exits with code 2, not 0.** That is the documented code for
"completed but produced DLQ entries" — the run succeeded and some records were
rejected. An orchestrator that treats any non-zero code as failure will get this
wrong; see `clinker --help` for the full table.

Four files land in `output/`, all matching [`expected/`](expected/).

## The input

`data/tickets.csv` — 60 tickets. Two things are deliberately messy, because real
exports are:

```
ticket_id,opened_at,customer_email,raw_priority,category,subject,first_response_mins,satisfaction
TK-200000,2026-01-27T09:07:00Z,dana.ashgrove@example.org,p1,other,...,104,
TK-200015,2026-01-26T08:22:00Z,...,URGENT,returns,...,pending,4
```

`raw_priority` arrives as `P1`, `p1`, `High`, `URGENT`, `P2`, `Normal`, `p3`,
`Low`. And `first_response_mins` is supposed to be a number, but agents type
free text into it — `pending`, `n/a`, `--`, or nothing at all.

## What to look at

**Normalise before routing.** Folding the priority vocabulary happens first:

```
let p = raw_priority.upper()
let tier = if p == "P1" or p == "HIGH" or p == "URGENT" then "urgent"
           else if p == "P2" or p == "NORMAL" then "standard"
           else "backlog"
```

Routing on `raw_priority` directly would scatter `P1`, `p1` and `URGENT` across
three different queues. This is the single most common source of quietly wrong
ETL output: the pipeline works, and the data is in the wrong buckets.

**Strict conversion is what fills the DLQ.** The schema declares
`first_response_mins` as `string`, because that is what the file honestly
contains. The conversion is explicit and strict:

```
let mins = first_response_mins.to_int()
```

`to_int` raises on anything that is not an integer, so the three bad rows are
rejected with a reason. The lenient `try_int` would return null instead. Which
you want is a modelling decision: use `try_int` when a missing value is normal
and null is a fine answer; use `to_int` when an unreadable value means the record
cannot be processed and someone needs to see it.

Declaring the column `int` in the schema and letting the reader coerce is *not*
currently a working alternative — see
[#975](https://github.com/rustpunk/clinker/issues/975).

**`strategy: continue` keeps the run going.** Under the default `fail_fast`, the
first bad row would end the run. Here the other 57 tickets are still worth
triaging.

**Exclusive routing sends each record to exactly one place.** 32 + 12 + 13 = 57.
With `mode: inclusive` a ticket could land in several queues; exclusive means
first match wins and the `default:` catches the rest.

## Reading the DLQ

`output/rejected.csv` has 3 rows and 20 columns. The engine-stamped ones carry
the diagnosis:

| column | value |
|---|---|
| `_cxl_dlq_source_row` | `16` — the line in the input file |
| `_cxl_dlq_triggering_value` | `pending` — the value that failed |
| `_cxl_dlq_error_category` | `type_coercion_failure` |
| `_cxl_dlq_error_detail` | `row 16: conversion failed: cannot convert...` |

The original record follows in full, so a rejected row can be corrected and
replayed without going back to the source system.

In the committed golden the first two columns read `<volatile>`. Every DLQ entry
carries a fresh UUID and a wall-clock timestamp, which cannot be byte-compared;
the test harness blanks exactly those two and compares the other eighteen
verbatim. See the [corpus README](../README.md#determinism).

## Try changing it

- Swap `to_int()` for `try_int()`. The DLQ empties, the run exits 0, and three
  tickets get a null response time and `sla_breached = false` — an answer that
  looks fine and is not. This is worth doing once to see how quiet it is.
- Set `mode: inclusive` and give `standard_queue` an overlapping condition.
- Add `min_records` and `max_rate` under `dlq:` to abort when rejects exceed a
  share of the run.
