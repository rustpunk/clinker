# Summary

[Introduction](README.md)

# Architecture

- [Overview & Pillars](architecture.md)

# Execution Model

- [Streaming vs. Blocking Stages](execution-model.md)
- [Memory Arbitration & Scheduling](memory-arbitration.md)

# Correlation Keys & Retraction

- [Lifecycle & Rollback Narrowing](correlation-lifecycle.md)
- [The Retraction Protocol](retraction-protocol.md)
- [Operator Retraction Cost Reference](retraction-cost-reference.md)

# Operator Internals

- [Combine Join Strategies](combine-internals.md)
- [Merge & Back-pressure](merge-internals.md)
- [Streaming Output Writes](output-internals.md)
- [Schema Drift & the `$widened` Sidecar](auto-widen-internals.md)

# Storage Internals

- [Staging, Crash Durability & Locks](storage-internals.md)

# CXL Compilation

- [Compiler Phases & Type Unification](cxl-internals.md)
