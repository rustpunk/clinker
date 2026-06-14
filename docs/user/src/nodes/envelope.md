# Envelope Nodes

Envelope nodes frame a body stream into per-document documents. An Envelope is a discrete, composable stage you can place after **any** operator — a Transform, a Merge, a Combine, or an Aggregate — to declare "from here on, treat the records as belonging to framed documents." It mirrors the message/EDI/XML envelope-wrapper pattern (the Enterprise Integration Patterns *Envelope Wrapper*, XProc's `p:wrap-sequence`): the body is the payload, and the envelope is the document boundary around it.

This page documents the `preserve` strategy. Consolidation (`concat`) and synthesizing strategies arrive in later releases.

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

`preserve` is a **streaming pass-through**. It does not buffer a whole document or the whole input — body records flow through one batch at a time, bounded the same way any intermediate stage is (the engine's memory arbitrator governs the node's buffer and spills to disk under pressure). The resident footprint is the records of the concurrently-open documents, not the total input size.
