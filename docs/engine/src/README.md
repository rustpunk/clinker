# Clinker Engine Internals

This book is for engineers working **on** Clinker — or operators who need to reason about *why* the engine behaves the way it does under load. It documents the execution model, memory arbitration, correlation-key retraction, operator strategies, storage durability, and CXL compilation in implementation detail.

If you only need to **author and run YAML pipelines**, you want the **Clinker User Guide** instead (the separate book under `docs/`). That book deliberately stays at the level of "what do I type and what happens"; this one explains the machinery beneath it.

## What's in scope here

- **The execution model** — which stages stream, which block, and how the memory arbitrator decides who pauses and who spills.
- **Correlation-key retraction** — the shadow-column lineage, per-source rollback narrowing, and the retraction protocol that lets a relaxed aggregate drop only the failing records.
- **Operator internals** — Combine join-strategy selection, Merge back-pressure, streaming Output writes, and the schema-drift sidecar.
- **Storage** — the staging cache, crash durability, and the locking protocol.
- **CXL compilation** — the compiler phases and the type-unification algorithm.

## How to read it alongside the User Guide

Most chapters here have a user-facing counterpart in the User Guide. The pattern is consistent: the User Guide tells you the knob and the observable outcome (e.g. "declare `sort_order` and the aggregate streams"); this book explains the mechanism that makes the outcome true (the streaming-ingest path, the group-table accumulation, the spill trigger). When a chapter has a user-facing sibling, it says so at the top.

## Architectural ground rules

Everything here is downstream of three permanent commitments — finite inputs, finite jobs, single process — covered in [Overview & Pillars](architecture.md). A mechanism that appears baroque (the single-process memory arbitrator, the in-process Rayon parallelism) usually makes sense only once you hold those three constraints fixed.
