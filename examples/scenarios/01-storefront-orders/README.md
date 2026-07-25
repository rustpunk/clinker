# 01 — Storefront orders

**csv → csv.** Read an order export, drop the rows that should not be billed,
derive the money column downstream wants, and write a narrow file.

This is the shape most ETL jobs actually are. Start here.

## Run it

```bash
cargo run -p clinker-scenarios -- gen --scenario 01-storefront-orders
cd examples/scenarios/01-storefront-orders
cargo run -p clinker -- run pipeline.yaml
```

```
Pipeline complete: 48 total, 42 ok, 42 written, 0 dlq
```

Output lands in `output/billable_lines.csv` and should match
[`expected/billable_lines.csv`](expected/billable_lines.csv) byte for byte.

## The input

`data/orders.csv` — 48 order lines:

```
order_id,order_date,customer_id,customer_name,customer_email,channel,sku,quantity,unit_price,discount_pct,ship_country,status
SO-10000,2026-02-13,C-1204,Aoife Lindqvist,aoife.lindqvist@example.org,phone,SP-5000,1,7.20,0,CA,shipped
SO-10004,2026-02-09,C-1050,Rafael Marchetti,rafael.marchetti@example.com,phone,SP-5000,2,7.20,25,DE,shipped
```

## What to look at

**Money is `decimal`, not `float`.** The schema declares `unit_price` as
`decimal` and the arithmetic stays in decimal throughout:

```
let gross = unit_price * quantity.to_decimal()
let discount_amount = gross * discount_pct.to_decimal() / "100".to_decimal()
```

Decimal arithmetic is exact, so `18.99 × 3 = 56.97`, less 25% is `42.73`, on
every platform. The same computation in binary floating point would be
`42.727499999999996` before rounding, and would drift differently on different
hardware. Exactness is also what makes byte-comparing this scenario's output
meaningful at all.

Note `"100".to_decimal()` rather than a bare `100`. A numeric literal is an int
or a float; converting from a string is how you get a decimal literal.

**`filter` drops records without dead-lettering them.** Cancelled and refunded
orders are not errors — they are simply not billable:

```
filter status != "cancelled" and status != "refunded"
```

Six of the 48 rows go, and the run reports `0 dlq`. Compare with
[03-support-triage](../03-support-triage/), where rows leave through the DLQ
because something genuinely went wrong. Choosing between the two is a real
modelling decision: the DLQ is for records you could not process, not records
you chose not to.

**`include_unmapped: false` narrows the output.** `customer_name` is read and
never emitted, so it does not appear. Without that flag every source column
rides along.

**Column order.** Emitted columns that exist in the source schema come first in
schema order; derived columns follow in emit order, which is why
`gross_amount` and `line_total` are last. The Output node has a `mapping:` key
that looks like it should control this, but its behaviour is under review in
[#974](https://github.com/rustpunk/clinker/issues/974) — rename in the transform
instead, where the reader can see it happen.

**`sort_order` makes the output stable.** Without it, row order depends on how
the engine schedules the read. With it, this file is reproducible — which is the
precondition for comparing it against a golden.

## Try changing it

- Drop the `filter` line and rerun. 48 rows instead of 42, and the golden
  comparison in the test suite fails — that is the gate working.
- Change `unit_price` to `type: float` and watch `line_total` pick up trailing
  binary-fraction noise on the discounted rows.
- Add `emit margin = line_total - (line_total * "0.4".to_decimal())`.
