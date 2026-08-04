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
Pipeline complete: 60 total, 54 ok, 54 written, 6 dlq
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
TK-200000,2026-01-31T08:27:00Z,emeka.voss@example.com,P1,billing,...,70,3
TK-200007,2026-01-15T18:16:00Z,nadia.ilves@example.net,Normal,other,...,pending,4
```

`raw_priority` arrives as `P1`, `p1`, `High`, `URGENT`, `P2`, `Normal`, `p3`,
`Low`. And `first_response_mins` is supposed to be a number, but agents type
free text into it — `pending`, `n/a`, `--`, or nothing at all.

The source declares `opened_at` as `date_time`, so each RFC 3339 `Z` value is
validated and parsed before any transform runs. The internal value is
timezone-free; `canonicalize_opened_at` explicitly renders that typed value
back to the export's `%Y-%m-%dT%H:%M:%SZ` text contract. This keeps the normal
outputs and any later rejected-row evidence in the same canonical UTC spelling
without passing the raw source string through as a `date_time`.

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

`to_int` raises on anything that is not an integer, so the six bad rows are
rejected with a reason. The lenient `try_int` would return null instead. Which
you want is a modelling decision: use `try_int` when a missing value is normal
and null is a fine answer; use `to_int` when an unreadable value means the record
cannot be processed and someone needs to see it.

Declaring the column `int` in the schema is also strict: malformed values are
then rejected at source ingestion. This scenario keeps the source column
`string` and converts it in `normalize` deliberately, so it demonstrates an
explicit transform conversion and records `transform:normalize` as the DLQ
stage.

**`strategy: continue` keeps the run going.** Under the default `fail_fast`, the
first bad row would end the run. Here the other 54 tickets are still worth
triaging.

**Exclusive routing sends each record to exactly one place.** 27 + 13 + 14 = 54.
With `mode: inclusive` a ticket could land in several queues; exclusive means
first match wins and the `default:` catches the rest.

## Reading the DLQ

`output/rejected.csv` has 6 rows and 20 columns. The engine-stamped ones carry
the diagnosis — this is the first entry:

| column | value |
|---|---|
| `_cxl_dlq_source_row` | `8` — the line in the input file |
| `_cxl_dlq_triggering_value` | `pending` — the value that failed |
| `_cxl_dlq_error_category` | `type_coercion_failure` |
| `_cxl_dlq_error_detail` | `row 8: conversion failed: cannot convert 'pending' to Int` |

The original record follows in full, so a rejected row can be corrected and
replayed without going back to the source system.

In the committed golden the first two columns read `<volatile>`. Every DLQ entry
carries a fresh UUID and a wall-clock timestamp, which cannot be byte-compared;
the test harness blanks exactly those two and compares the other eighteen
verbatim. See the [corpus README](../README.md#determinism).

## Try changing it

- Swap `to_int()` for `try_int()`. The DLQ empties and the run exits 0, but look
  at what those six tickets get: `first_response_mins` is blank and
  `sla_breached` is blank too — **not** `false`. Every CXL comparison other than
  `==`/`!=` propagates null, so `mins > 60` on a null is null, and the boolean
  never becomes a definite answer. A downstream consumer filtering on
  `sla_breached == false` will not see these rows at all, and one filtering on
  `!= true` will. That silent third state is exactly what strict `to_int` avoids
  by refusing the row up front.
- Set `mode: inclusive` and give `standard_queue` an overlapping condition.
- Add `min_records` and `max_rate` under `dlq:` to abort when rejects exceed a
  share of the run.
