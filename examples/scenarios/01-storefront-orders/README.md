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
Pipeline complete: 48 total, 38 ok, 38 written, 0 dlq
```

Output lands in `output/billable_lines.csv` and should match
[`expected/billable_lines.csv`](expected/billable_lines.csv) byte for byte.

## The input

`data/orders.csv` — 48 order lines:

```
order_id,order_date,customer_id,customer_name,customer_email,channel,sku,quantity,unit_price,discount_pct,ship_country,status
SO-10000,2026-02-13,C-1229,Saskia Lindqvist,saskia.lindqvist@example.net,web,NT-2011,4,22.00,15,JP,delivered
SO-10001,2026-02-07,C-1030,Priya Bergstrom,priya.bergstrom@example.org,mobile,FC-4100,2,9.40,10,DE,delivered
```

## What to look at

**Money is `decimal`, not `float`.** The schema declares `unit_price` as
`decimal` and the arithmetic stays in decimal throughout:

```
let gross = unit_price * quantity.to_decimal()
let discount_amount = gross * discount_pct.to_decimal() / "100".to_decimal()
```

Decimal arithmetic is exact. Order `SO-10022` buys two units at `5.45` with a
25% discount: gross `10.90`, discount `2.725`, net `8.175`, which `round_to(2)`
writes as `8.18`. Every one of those intermediate values is exact, on every
platform. The same computation in binary floating point carries a small error
into the rounding step, where it can tip a half-cent the wrong way — and it can
tip differently on different hardware. That exactness is also what makes
byte-comparing this scenario's output meaningful at all.

Note `"100".to_decimal()` rather than a bare `100`. A numeric literal is an int
or a float; converting from a string is how you get a decimal literal.

**`filter` drops records without dead-lettering them.** Cancelled and refunded
orders are not errors — they are simply not billable:

```
filter status != "cancelled" and status != "refunded"
```

Ten of the 48 rows go, and the run reports `0 dlq`. Compare with
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

**Row order is stable because the read is.** A single-file source delivers
records in file order, deterministically, so this output is byte-comparable
against a committed golden without any sorting step. The Output node does have a
`sort_order:` key — do not reach for it yet: it is parsed and documented but
currently has no effect at all ([#950](https://github.com/rustpunk/clinker/issues/950)).

## Try changing it

- Drop the `filter` line and rerun. 48 rows instead of 38, and the golden
  comparison in the test suite fails — that is the gate working.
- Change `unit_price` to `type: float`. The pipeline no longer compiles:
  `cannot mix decimal and float without an explicit cast`. CXL will not silently
  promote one to the other, so the moment money stops being exact is a compile
  error rather than a rounding surprise found in production. To see the float
  behaviour you have to opt in explicitly with `.to_float()` on both operands.
- Add `emit margin = line_total - (line_total * "0.4".to_decimal())`.
