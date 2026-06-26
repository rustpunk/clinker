# Summary

[Introduction](README.md)
[Non-Goals](non-goals.md)

# Getting Started

- [Installation](getting-started/installation.md)
- [Your First Pipeline](getting-started/first-pipeline.md)
- [Key Concepts](getting-started/concepts.md)

# Pipelines

- [Pipeline YAML Structure](pipelines/structure.md)
- [Pipeline Variables](pipelines/variables.md)
- [Channels](pipelines/channels.md)
- [Compositions](pipelines/compositions.md)
- [Correlation Keys](pipelines/correlation-keys.md)
- [Document Envelope Context](pipelines/envelope-and-doc-context.md)
- [Error Handling & DLQ](pipelines/error-handling.md)

# Nodes

- [Node Taxonomy](nodes/index.md)
- [Source Nodes](nodes/source.md)
- [Transform Nodes](nodes/transform.md)
- [Route Nodes](nodes/route.md)
- [Merge Nodes](nodes/merge.md)
- [Combine Nodes](nodes/combine.md)
- [Aggregate Nodes](nodes/aggregate.md)
- [Reshape Nodes](nodes/reshape.md)
- [Cull Nodes](nodes/cull.md)
- [Envelope Nodes](nodes/envelope.md)
- [Output Nodes](nodes/output.md)

# Source Formats

- [CSV Format](formats/csv.md)
- [JSON Format](formats/json.md)
- [XML Format](formats/xml.md)
- [Fixed-Width Format](formats/fixed-width.md)
- [EDIFACT Format](formats/edifact.md)
- [X12 Format](formats/x12.md)
- [HL7 v2 Format](formats/hl7.md)
- [SWIFT MT Format](formats/swift.md)
- [Network Sources (REST)](formats/source-network.md)
- [Auto-Widen & Schema Drift](formats/auto-widen.md)

# CXL Language Guide

- [CXL Overview](cxl/overview.md)
- [Types & Literals](cxl/types.md)
- [Operators & Expressions](cxl/operators.md)
- [Statements](cxl/statements.md)
- [Conditionals](cxl/conditionals.md)
- [Built-in Methods](cxl/builtins.md)
    - [String Methods](cxl/builtins-string.md)
    - [Numeric Methods](cxl/builtins-numeric.md)
    - [Date & Time Methods](cxl/builtins-date.md)
    - [Conversion Methods](cxl/builtins-conversion.md)
    - [Introspection & Debug](cxl/builtins-introspection.md)
    - [Path Methods](cxl/builtins-path.md)
    - [Array Methods](cxl/builtins-array.md)
    - [Map Methods](cxl/builtins-map.md)
- [Window Functions](cxl/windows.md)
- [Aggregate Functions](cxl/aggregates.md)
- [Closures](cxl/closures.md)
- [Nested Paths](cxl/nested-paths.md)
- [Emit Each](cxl/emit-each.md)
- [System Variables](cxl/system-variables.md)
- [Null Handling](cxl/nulls.md)
- [Modules & use](cxl/modules.md)
- [The cxl CLI Tool](cxl/cxl-cli.md)

# Operations Guide

- [CLI Reference](ops/cli-reference.md)
- [Validation & Dry Run](ops/validation.md)
- [Explain Plans](ops/explain.md)
- [Column Lineage](ops/lineage.md)
- [Memory Tuning](ops/memory.md)
- [Storage & Spill Location](ops/storage.md)
- [Streaming vs. Blocking Stages](ops/streaming-vs-blocking.md)
- [Optimizing Pipelines](ops/optimizing.md)
- [Metrics & Monitoring](ops/metrics.md)
- [Exit Codes & Error Diagnosis](ops/exit-codes.md)
- [Production Deployment](ops/deployment.md)

# Cookbook

- [CSV-to-CSV Transform](cookbook/csv-transform.md)
- [Multi-Input Combine](cookbook/combine.md)
- [Routing to Multiple Outputs](cookbook/routing.md)
- [Aggregation & Rollups](cookbook/aggregation.md)
- [Slowly-Changing Dimensions (SCD Type 2)](cookbook/scd-type2.md)
- [Backfill, Then Cull for Review](cookbook/backfill-and-cull.md)
- [File Splitting](cookbook/file-splitting.md)
- [Intra-Record Closures](cookbook/intra-record-closures.md)
