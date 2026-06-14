# Envelope Nodes

Envelope nodes frame a body stream into per-document documents. An Envelope is a discrete, composable stage you can place after **any** operator — a Transform, a Merge, a Combine, or an Aggregate — to declare "from here on, treat the records as belonging to framed documents." It mirrors the message/EDI/XML envelope-wrapper pattern (the Enterprise Integration Patterns *Envelope Wrapper*, XProc's `p:wrap-sequence`): the body is the payload, and the envelope is the document boundary around it.

This page documents the `preserve` and `concat` strategies. The synthesizing (header-folding) strategies arrive in later releases.

## Basic structure

```yaml
- type: envelope
  name: framed
  body: merged
  config:
    strategy: preserve
```

The node reads its `body:` input and emits the same records, framed per document. A downstream [Output](output.md) with `reconstruct_envelope: true` then writes one framed document per body grain.

## Inputs: `body`, and the not-yet-wired `header` / `trailer`

| Input | Required | Status |
|-------|----------|--------|
| `body` | yes | The records to frame into documents. |
| `header` | no | A stream whose records prepend to each framed document. **Accepted in config but not yet wired** — a wired value is rejected at plan validation this release. |
| `trailer` | no | A stream whose records append to each framed document. Same not-yet-wired status. |

Wiring an explicit `header:` or `trailer:` input is rejected with a clear "not yet supported" message:

```
envelope node 'framed': explicit `header` input wiring is not yet supported —
omit it to frame with the body's own envelope
```

Until those ports are wired, an Envelope frames each body record using the body's own **ambient envelope** — the document context every record already carries from its source — rather than a second input stream.

## `strategy: preserve`

`preserve` emits **one framed document per body grain**. It is a transparent framing stage: body records pass through with their document context and grain unchanged, and the document-boundary signals are forwarded verbatim. Inserting a `preserve` Envelope between a body stage and an Output is byte-identical to today's per-document framing — its value is being the *explicit, composable* stage that later strategies extend, not a change in output.

`preserve` is the default, so `config: { strategy: preserve }` and an empty `config: {}` are equivalent.

### Framing is keyed on the document grain, never the source file

The grain is the level at which one logical document is reconstructed — and it is not always one-per-file:

- A **nested X12 interchange** frames **once per interchange**. The `GS` functional-group and `ST` transaction-set levels inherit the interchange grain, so an `ISA … IEA` interchange is one framed document regardless of how many groups or transaction sets it nests.
- An **HL7 multi-message file** frames **once per message**. Each `MSH` message opens its own grain, so a single file containing several messages produces several framed documents.

Because framing keys on the grain rather than the source file, splitting or combining files never silently changes the document count.

## `strategy: concat`

`concat` does the opposite of `preserve`: it collapses a **multi-document body into one framed document**. Every body record is re-stamped onto a single consolidated document context, so the body opens and closes **exactly once** regardless of how many documents fed in. This is the strategy to use when several source documents — say two files joined by a Merge — should write as one consolidated document with a single header and footer.

```yaml
nodes:
  - type: merge
    name: both
    inputs: [file_a, file_b]
  - type: envelope
    name: framed
    body: both
    config: { strategy: concat }
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
```

Re-stamping changes only the **framing** (the grain) and the ambient `$doc.*` view a record sees — it does **not** disturb per-record fields. In particular `$source.file` is a real column stamped when each record is read, so it still reports the record's own originating file after a concat. Concat is lossless on per-record provenance; it changes only which document the record is framed inside.

### The consolidated header, and the two-headers conflict

One consolidated document can carry only **one** envelope header. `concat` derives it from the headers of the documents that contribute body records, taking one header per document:

- **Every header agrees** (or there is only one) → the consolidated document carries that **common header**.
- **No document carries a header** → the consolidated document is **headerless**.
- **A headed document and a headerless document** → the single header wins; the headerless document coexists with it (no conflict).

Only documents that contribute body records take part: a document that carries a header but no body records frames nothing once consolidated, so it never enters the comparison. Header identity is **structural** — two documents share a header when they declare the same sections, in the same order, with the same field values (including any raw content the reader preserves). Two files that differ only in an embedded control number therefore count as distinct headers.

When the body carries **two or more distinct non-empty headers**, `concat` refuses to silently keep one and drop the rest. The run fails with **E350** (run `clinker explain --code E350` for the full write-up):

```
envelope 'framed': concat collapses the body into one framed document, but the
body carried 2 distinct non-empty envelope headers — one document can frame only
one header, so concat will not silently drop the rest. Make the headers identical
upstream, or add a header-folding strategy that declares which header the
consolidated document keeps.
```

To resolve a conflict, either keep the documents separate with `preserve`, make the headers identical upstream (project them to the same sections and values), or — in a later release — declare a header-folding strategy that states which header the consolidated document keeps.

## Placement

An Envelope is a normal single-input, single-output node — put it anywhere a record stream flows:

```yaml
nodes:
  # … sources, a Combine that joins two streams into `merged` …
  - type: envelope
    name: framed
    body: merged
    config: { strategy: preserve }
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
```

Placing the Envelope **after** a Combine or Aggregate is the intended use: it declares the document framing for the combined or reduced result, which is exactly where the later consolidation and synthesizing strategies do their work.

## Memory model

Both strategies re-park the body into the node's own buffer slot, which the engine's memory arbitrator governs and spills to disk under pressure — so neither strategy is bounded by total input size held in RAM.

`preserve` is a transparent framing pass-through: it forwards records and their document boundaries unchanged. `concat` additionally re-stamps each record onto the one consolidated document context and replaces the per-document boundaries with a single open/close pair; the header consolidation it does first groups the body records by document — one document's worth of body records shares one grain and one header — so it does one pass over the records to collect the distinct headers, comparing only one envelope per document (the work is bounded by the number of documents, not the number of body rows).
