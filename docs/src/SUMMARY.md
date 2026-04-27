# Summary

[Introduction](README.md)

# Getting Started

- [Installation](getting-started/installation.md)
- [Your First Pipeline](getting-started/first-pipeline.md)
- [Key Concepts](getting-started/concepts.md)

# Pipeline Reference

- [Pipeline YAML Structure](pipeline/structure.md)
- [Source Nodes](pipeline/source.md)
- [Transform Nodes](pipeline/transform.md)
- [Aggregate Nodes](pipeline/aggregate.md)
- [Route Nodes](pipeline/route.md)
- [Merge Nodes](pipeline/merge.md)
- [Combine Nodes](pipeline/combine.md)
- [Output Nodes](pipeline/output.md)
- [Error Handling & DLQ](pipeline/error-handling.md)
- [Correlation Keys](pipeline/correlation-keys.md)
- [Pipeline Variables](pipeline/variables.md)
- [Channels](pipeline/channels.md)
- [Compositions](pipeline/compositions.md)

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
- [Window Functions](cxl/windows.md)
- [Aggregate Functions](cxl/aggregates.md)
- [System Variables](cxl/system-variables.md)
- [Null Handling](cxl/nulls.md)
- [Modules & use](cxl/modules.md)
- [The cxl CLI Tool](cxl/cxl-cli.md)

# Operations Guide

- [CLI Reference](ops/cli-reference.md)
- [Validation & Dry Run](ops/validation.md)
- [Explain Plans](ops/explain.md)
- [Memory Tuning](ops/memory.md)
- [Metrics & Monitoring](ops/metrics.md)
- [Exit Codes & Error Diagnosis](ops/exit-codes.md)
- [Production Deployment](ops/deployment.md)

# Cookbook

- [CSV-to-CSV Transform](cookbook/csv-transform.md)
- [Multi-Input Combine](cookbook/combine.md)
- [Routing to Multiple Outputs](cookbook/routing.md)
- [Aggregation & Rollups](cookbook/aggregation.md)
- [File Splitting](cookbook/file-splitting.md)
