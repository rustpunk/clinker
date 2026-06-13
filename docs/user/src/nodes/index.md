# Nodes

A Clinker pipeline is a single flat `nodes:` list. Every entry carries a
`type:` discriminator that selects a node kind — the **unified node
taxonomy**. There is no separate "join section" or "filter section":
records flow through one homogeneous graph of typed nodes, wired together
by [`input:` / `inputs:`](../pipelines/structure.md#wiring-input-and-inputs).
This part documents the nine record-processing node kinds; a tenth,
[Composition](../pipelines/compositions.md), is a call-site that inlines a
reusable sub-pipeline and is covered under Pipelines.

The pages in this part are ordered the way data flows through a DAG — a
record enters at a Source, fans through the record-level and combining
nodes, and leaves at an Output:

| Node | Role | Arity | Streaming vs blocking |
|------|------|-------|-----------------------|
| [Source](source.md) | Reads records from a file or network cursor; the entry point. | 0 → 1 | Streaming |
| [Transform](transform.md) | Record-level CXL projection, filter, and lookup. | 1 → 1 | Streaming |
| [Route](route.md) | Predicate-based fan-out into named branches. | 1 → N | Streaming |
| [Merge](merge.md) | Streamwise concatenation of inputs that share a schema. | N → 1 | Streaming |
| [Combine](combine.md) | N-ary record combining with mixed predicates (equi + range + arbitrary CXL). | N → 1 | Blocking (build side) |
| [Aggregate](aggregate.md) | Grouped or windowed reduction. | 1 → 1 | Blocking (or streaming when sorted) |
| [Reshape](reshape.md) | Pivot / unpivot between wide and long record shapes. | 1 → 1 | Blocking |
| [Cull](cull.md) | Per-correlation-group removal on a group-level predicate, with a `removed_to` side-output port. | 1 → 2 | Blocking |
| [Envelope](envelope.md) | Frames a body stream into per-document documents; a composable framing stage. | 1 → 1 | Streaming |
| [Output](output.md) | Writes records to a sink; the exit point. | 1 → 0 | Streaming |

## Streaming vs blocking

Stateless nodes (Transform, Route, Merge, the Combine probe side, Output)
evaluate records one at a time without accumulating per-record state.
Blocking nodes (Aggregate, sort, the grace-hash Combine build side)
accumulate state inside the RSS budget and spill to disk rather than OOM
the process. The [Streaming vs. Blocking Stages](../ops/streaming-vs-blocking.md)
page in the Operations Guide is the full memory model.

## Wiring and naming

Every node needs a unique `name:` (no dots — the dot is reserved for port
syntax). Single-input nodes use `input:`; Merge and Combine use `inputs:`.
Route branches are consumed downstream as `route_name.port`. The
[Pipeline YAML Structure](../pipelines/structure.md) page covers the full
wiring grammar, optional fields (`description:`, `_notes:`), and strict
parsing rules.
