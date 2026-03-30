# Validation Report: Phase 7 — JSON + XML Readers/Writers

**Date:** 2026-03-30
**Validator:** validate-phase skill
**Verdict:** READY (0 blockers)

## Verdict rationale

Phase 7 passes all 8 validation dimensions with zero blockers. The highest-risk
item — serde-saphyr compatibility with `#[serde(flatten)]` + adjacently tagged
enums + `deny_unknown_fields` — was empirically validated and passes all 7 test
scenarios. Three warnings address missing edge-case tests, an unlisted refactor
call site, and security hardening for JSON Value parsing.

## Blocker resolution log

(No blockers found)

## Accepted warnings and known risks

| ID | Issue | Accepted by | Rationale |
|----|-------|------------|-----------|
| V-1-1 | `projection.rs` call sites not listed in 7.0.6 | User | Compiler catches missing match arms; implementer will find them |
| V-5-1 | Missing edge case tests (empty array, malformed JSON, CDATA, empty array_paths) | User | Add 4 tests during implementation |
| V-8-1 | No file-size guard on serde_json::Value parsing; no flattening depth limit | User | Add file-size check (100MB default) and max flatten depth (64) in Task 7.1 |

## Edge cases acknowledged

| ID | Scenario | Handling decision |
|----|---------|-----------------|
| V-5-1a | Empty JSON array `[]` | Add test; return 0 records with empty schema |
| V-5-1b | Malformed JSON input | Add test; propagate serde_json error as FormatError |
| V-5-1c | XML CDATA sections | Add test; treat as text content (quick-xml Event::CData) |
| V-5-1d | Empty array in array_paths explode mode | Add test; skip parent record (0 children = 0 output records) |

## Security flags

| ID | Risk | Resolved / Accepted | Notes |
|----|------|---------------------|-------|
| V-8-1a | serde_json::Value no size limit | Accepted — add guard | File-size check before Value parsing in record_path mode |
| V-8-1b | Recursive flattening stack overflow | Accepted — add guard | Max flatten depth 64 levels |
| V-8-1c | XML billion laughs | Safe | quick-xml Reader doesn't expand custom entities |
| V-8-1d | JSON pointer injection | Safe | Value::pointer() is tree traversal, no code execution |

## Feature coverage map

| Spec requirement | Task(s) | Coverage |
|-----------------|---------|---------|
| §4 JSON auto-detect (array/ndjson/object) | 7.1 | Full |
| §4 JSON record_path | 7.1 | Full |
| §4 JSON array_paths (explode/join) | 7.1 | Full |
| §4 JSON output (array/ndjson/pretty) | 7.2 | Full |
| §4 XML record_path | 7.3 | Full |
| §4 XML attribute_prefix | 7.3 | Full |
| §4 XML namespace_handling | 7.3 | Full |
| §4 XML output (root/record elements) | 7.4 | Full |
| §4 preserve_nulls (JSON/XML) | 7.2, 7.4 | Full |
| §4 Schema inference | 7.1, 7.3 | Full |
| §4 Nested flattening (. separator) | 7.1, 7.3 | Full |
| §4 Format-specific config validation | 7.0 | Full |

## Cross-phase coherence

Phase 7 depends only on Phase 4 (complete). Phases 8 and 9 depend on Phase 7.
The config refactor in Task 7.0 touches executor.rs and plan compiler call sites
from Phases 4-6, but these are mechanical type-adaptation fixups, not behavioral
changes. FormatReader/FormatWriter traits are stable — JSON/XML implement them
without modification. The `FormatKind` enum is subsumed by the new `InputFormat`/
`OutputFormat` tagged enums and should be explicitly removed or retained only for
non-config dispatch paths.

## Validation checks run

| Check | Agent | Result | Issues found |
|-------|-------|--------|-------------|
| Interface contracts | Agent 1 | Warning | 1 (projection.rs call sites) |
| Dependency coherence | Agent 2 | Pass | 0 |
| Scope drift | Agent 3 | Pass | 0 |
| Dead code / duplication | Agent 4 | Pass | 0 (note: FormatKind removal) |
| Test coverage | Agent 5 | Warning | 4 missing edge cases |
| Naming consistency | Agent 6 | Pass | 0 |
| Assumption conflicts + edge cases | Agent 7 | Pass | serde-saphyr empirically validated |
| Feature gaps + security | Agent 8 | Warning | 2 hardening items |
