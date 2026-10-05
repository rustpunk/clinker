# Envelope Nodes

Envelope nodes frame a body stream into per-document documents. An Envelope is a discrete, composable stage you can place after **any** operator — a Transform, a Merge, a Combine, or an Aggregate — to declare "from here on, treat the records as belonging to framed documents." It mirrors the message/EDI/XML envelope-wrapper pattern (the Enterprise Integration Patterns *Envelope Wrapper*, XProc's `p:wrap-sequence`): the body is the payload, and the envelope is the document boundary around it.

This page documents the `preserve` and `concat` framing strategies and the orthogonal `header:` / `footer:` synthesis that layers on top of either.

*Interactive companion: [How many documents?](envelope-explainer.html) shows the documents a Sink writes under `preserve` and `concat`, with and without a synthesized footer, how an Aggregate after the Envelope rolls up, and which shapes E347 and E355 reject.*

## Basic structure

```yaml
- type: envelope
  name: framed
  body: merged
  config:
    strategy: preserve
```

The node reads its `body:` input and emits the same records, framed per document. A downstream [Sink](sink.md) with `reconstruct_envelope: true` then writes one framed document per body grain.

## Inputs: `body`, the wired `header`, and the not-yet-wired `trailer`

| Input | Required | Status |
|-------|----------|--------|
| `body` | yes | The records to frame into documents. |
| `header` | no | A 1-row-per-grain header stream. A wired value **replaces** each body document's header with the matching header record, attached by document grain. |
| `trailer` | no | A stream whose records append to each framed document. **Accepted in config but not yet wired** — a wired value is rejected at plan validation this release. |

When you omit `header:`, an Envelope frames each body record using the body's own **ambient envelope** — the document context every record already carries from its source.

### Wiring a `header:` port replaces the document's header

A wired `header:` input is a second stream carrying one header record per document, each on the **same document grain** as the body it frames. The node attaches a header to a body strictly **by grain**, so the header record reaching it must carry the body's grain — and the node then **replaces** that document's ambient header with the wired header record (the framing grain is preserved; only the header changes). This is *transform-in-place* header replacement: rewrite a header's values upstream — for example, override a batch id or stamp a run date — and frame the body with the rewritten header instead of the source's original.

```yaml
nodes:
  # `rewrite_header` rewrites the source header's values while keeping each
  # record's grain, so the rewritten header still grounds to its body document.
  - type: envelope
    name: framed
    body: payments
    header: rewrite_header
    config: { strategy: preserve }
```

The header record must carry a body document's grain. A grain-preserving Transform of the source's promoted header keeps it; a replacement from a different source establishes it via a business-key join against the body. A header record whose grain matches **no** in-flight body document (or carries the synthetic, ungrounded grain a Transform stamps onto a record it builds from scratch) cannot be placed, so the run fails with **E351** (run `clinker explain --code E351` for the full write-up):

```
envelope "framed": a wired header record carries document grain <grain>, which
matches no in-flight body grain (or is a synthetic / ungrounded grain). The node
attaches a header to a body strictly by grain, so it cannot place a header that
grounds to no body document.
```

Exactly **one** header record may carry each body document's grain. When the wired header stream carries **two or more** records on the same grain, the node has no rule to fold a second header onto an already-framed document, so it refuses to silently keep one and drop the rest. The run fails with **E352** (run `clinker explain --code E352` for the full write-up):

```
envelope "framed": the wired header input carries two or more records for
document grain <grain> — exactly one header record per document grain is
required.
```

Deduplicate the header stream to one record per grain upstream — an aggregate or distinct on the grain's business key, or a Transform that emits a single rewritten header per source document.

A wired `trailer:` input is still rejected this release with a clear "not yet supported" message:

```
envelope node "framed": explicit `trailer` input wiring is not yet supported —
omit it to frame with the body's own envelope
```

## `strategy: preserve`

`preserve` emits **one framed document per body grain**. It is a transparent framing stage: body records pass through with their document context and grain unchanged, and the document-boundary signals are forwarded verbatim. Inserting a `preserve` Envelope between a body stage and a Sink is byte-identical to today's per-document framing — its value is being the *explicit, composable* stage that later strategies extend, not a change in output.

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
  - type: sink
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

Only documents that contribute body records take part: a document that carries a header but no body records frames nothing once consolidated, so it never enters the comparison. Header identity is **structural** — two documents share a header when they declare the same sections, in the same order, with the same field values. Engine-added fields whose names start with `$` (such as `$raw`, the raw segment text) are ignored, so two files whose headers differ only in raw content fold to one header, and the consolidated document keeps the first document's full header, `$raw` included. A difference in a field you declare (for example an extracted control number) makes the headers distinct.

When the body carries **two or more distinct non-empty headers**, `concat` refuses to silently keep one and drop the rest. The run fails with **E350** (run `clinker explain --code E350` for the full write-up):

```
envelope "framed": concat collapses the body into one framed document, but the
body carried 2 distinct non-empty envelope headers — one document can frame only
one header, so concat will not silently drop the rest. Make the headers identical
upstream, or add a header-folding strategy that declares which header the
consolidated document keeps.
```

To resolve a conflict, either keep the documents separate with `preserve`, or make the headers identical upstream (project them to the same sections and values). `header:` synthesis (below) does not resolve it: `concat` compares the input headers and raises E350 before any synthesis runs ([#1385](https://github.com/rustpunk/clinker/issues/1385)).

## Synthesizing a header and footer

`header:` and `footer:` are **orthogonal** to the framing strategy. The strategy decides *how many* output documents there are (`preserve` = one per body grain, `concat` = one consolidated); synthesis decides *what header and footer each of those documents carries*. The node computes a fresh header (declarative scalar expressions) and footer (streaming aggregates over the framed body) **per output document**, stamps them as named sections into the document's envelope, and the same `header_from_doc` / `footer_from_doc` writer path renders them.

Both maps are keyed `section -> field -> CXL expression`. The inner field map preserves declaration order, which is the rendered cell order. A downstream [Sink](sink.md) renders a section through `header_from_doc` / `footer_from_doc`, and those may name only a section that a feeding Source declares (**E346**), so in practice a synthesized section reuses a declared section name.

A synthesized section **replaces** the whole same-named section on the document; it does not add fields to it, so the section's original fields (for example a source's `interchange.tag`) are gone from that document. Other sections ride through untouched. Give `header:` and `footer:` **different** section names: if both name the same section, the header is applied last and replaces the footer, whose fields are lost without an error ([#1388](https://github.com/rustpunk/clinker/issues/1388)).

```yaml
- type: envelope
  name: framed
  body: merged
  config:
    strategy: concat            # or preserve — synthesis works the same on either
    header:                     # section -> field -> scalar CXL
      group:                    # a different section from the footer's
        sender: $vars.sender_id
        created: $pipeline.run_date
    footer:                     # section -> field -> aggregate CXL
      interchange:
        record_count: count()
        total: sum(amount)
```

### Header fields: evaluated once at document open

A `header:` field is a scalar expression evaluated **once per output document, before the body streams**. It may read only inputs known at document open — pipeline configuration (`$vars`), per-document provenance (`$source`), pipeline-scope state (`$pipeline`), and the ambient envelope (`$doc.*`). It may **not** read a body column, because there is no "current body record" when the header is emitted; a body-column reference is rejected at compile time with **E353** (run `clinker explain --code E353`). Put body-derived values in a footer aggregate instead.

### Footer fields: streaming aggregates over the body

A `footer:` field folds the document's body records into a footer value at the document's close. The fold is an **O(1) accumulator per open document**, so it supports exactly the streaming distributive/algebraic aggregates over a **bare field or `*`**:

| Aggregate | Example |
|-----------|---------|
| `count` | `count()`, `count(*)` |
| `sum` | `sum(amount)` |
| `avg` | `avg(amount)` |
| `min` | `min(amount)` |
| `max` | `max(amount)` |

Anything else is rejected at compile time. A holistic or unbounded aggregate (`collect`, `any`, `weighted_avg`) or a composed/multi-argument aggregate (`sum(amount * 1.1)`, `weighted_avg(value, weight)`) raises **E354** (run `clinker explain --code E354`); a non-aggregate function (`median`, `mode`) fails earlier as an unknown function. Project a composed value in an upstream Transform, or compute a holistic value in an upstream Aggregate, then aggregate the bare column here.

### Synthesis is orthogonal: the same footer differs across strategies

Because the strategy sets the grain cardinality, the **same** `footer: { interchange: { record_count: count() } }` produces a different result on each strategy over the same body:

- Under `strategy: preserve`, each body document frames its own output document, so each footer's `record_count` is **that document's** body count.
- Under `strategy: concat`, the whole body collapses to one document, so the single footer's `record_count` is the **merged** body count.

## Placement

An Envelope is a normal single-input, single-output node — put it anywhere a record stream flows:

```yaml
nodes:
  # … sources, a Combine that joins two streams into `merged` …
  - type: envelope
    name: framed
    body: merged
    config: { strategy: concat }   # preserve here is rejected with E347
  - type: sink
    name: out
    input: framed
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
```

After a Combine, Aggregate or Composition, a Sink with `reconstruct_envelope: true` needs a `concat` Envelope in between: those nodes' rows carry no document of their own, and `concat` frames them as one. A `preserve` Envelope there is rejected with **E347**. For an Aggregate that is right, since its rows would pass through unframed; for a Combine the check is stricter than it needs to be, because the joined rows do keep their driver row's document ([#1384](https://github.com/rustpunk/clinker/issues/1384)).

After a Combine this works: the joined rows keep their driver row's document, and `concat` writes them as one framed document. After an Aggregate it plans without error but does not frame yet: Aggregate rows carry no source file, so the consolidated document has none either, and the Sink writes the rows with no header or footer ([#603](https://github.com/rustpunk/clinker/issues/603)). Reshape output loses its document the same way ([#1317](https://github.com/rustpunk/clinker/issues/1317)).

## Memory model

Both strategies re-park the body into the node's own buffer slot, which the engine's memory arbitrator governs and spills to disk under pressure — so neither strategy is bounded by total input size held in RAM.

`preserve` is a transparent framing pass-through: it forwards records and their document boundaries unchanged. `concat` additionally re-stamps each record onto the one consolidated document context and replaces the per-document boundaries with a single open/close pair; the header consolidation it does first groups the body records by document — one document's worth of body records shares one grain and one header — so it does one pass over the records to collect the distinct headers, comparing only one envelope per document (the work is bounded by the number of documents, not the number of body rows).

`header:` / `footer:` synthesis adds, on top of the materialized body, one O(1) accumulator per footer field per open document — every allowed footer aggregate (`count` / `sum` / `avg` / `min` / `max`) holds a fixed-size state regardless of how many body rows it folds. So a document's footer state is independent of its body-row count, and synthesis stays within the node's bounded-memory model.
