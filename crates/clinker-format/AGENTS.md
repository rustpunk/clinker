# AGENTS.md

This file specializes the root `AGENTS.md` for `crates/clinker-format`.
Follow the root instructions first.

## Purpose

`clinker-format` owns streaming byte-format I/O for Clinker records. It
provides readers and writers for CSV, JSON/NDJSON, XML, fixed-width,
multi-record flat files, EDIFACT, X12, HL7 v2, and SWIFT MT, plus envelope,
document-context, BOM, byte-counting, source reopening, and split-output
helpers.

## Responsibilities

- Decode finite input bytes into `clinker_record::Record` values one record at a time.
- Encode records to output formats without owning runtime execution.
- Define the `FormatReader` and `FormatWriter` contracts consumed by executor source/output code.
- Preserve document context and envelope boundaries through reader and writer hooks.
- Keep JSON/XML envelope pre-scans bounded by retaining only declared `$doc.*` sections.
- Provide format-specific structural validation for EDI, HL7, SWIFT, and multi-record files.
- Provide format-agnostic wrappers for byte counting and split rotation.

## Important public APIs

- `FormatReader` and `FormatWriter`.
- `FormatError`.
- `ReopenableSource` and `source::SourceIdentity`.
- `DocArenaIndex`.
- `EnvelopeConfig`, `EnvelopeSection`, `EnvelopeExtract`, `EnvelopeFieldType`, `EnvelopeEvent`, `FrameRole`, and `NestedEnvelopeSection`.
- `OutputEnvelopeSpec` and `EnvelopeFramer`.
- `CountingWriter`, `CountedFormatWriter`, and `SharedByteCounter`.
- Format modules: `csv`, `json`, `xml`, `fixed_width`, `edifact`, `x12`, `hl7`, `swift`, and `multi_record`.
- Re-exported reader ceilings: `EDIFACT_DEFAULT_MAX_ELEMENTS`, `X12_DEFAULT_MAX_ELEMENTS`, and `HL7_DEFAULT_MAX_FIELDS`.

## Internal module map

- `traits.rs`: streaming reader/writer contracts and document/envelope hooks.
- `error.rs`: `FormatError` and structural-count helpers.
- `bom.rs`: leading UTF-8 BOM stripping for text readers.
- `source.rs`: path, buffered, and one-shot reopenable byte sources plus identity checks.
- `doc_index.rs`: path-pruned `$doc` section retention with `max_index_bytes`.
- `envelope.rs` / `envelope_writer.rs`: source envelope extraction config and output envelope reconstruction.
- `counting.rs`: byte-counting writer wrappers.
- `splitting/`: format-agnostic split rotation over writer factories.
- `csv/`, `json/`, `xml/`, and `fixed_width/`: generic data formats.
- `multi_record.rs`: heterogeneous flat-file header/body/trailer reader.
- `edifact/`, `x12/`, `hl7/`, and `swift/`: structured interchange/message formats.
- `segment_tokenizer.rs`: crate-private segment framing helper.

## Dependency rules

### Allowed dependencies

Existing normal dependencies are intentional evidence: `clinker-record`,
`cxl`, `serde`, `serde_json`, `csv`, `quick-xml`, `chrono`, `miette`,
`tracing`, and `indexmap`.

Existing dev/bench dependencies are expected: `criterion` and
`clinker-bench-support`.

### Forbidden or suspicious dependencies

- Do not add `clinker-plan`, `clinker-exec`, `clinker`, `clinker-net`, `clinker-channel`, or `clinker-schema` dependencies without architecture review.
- Do not add async runtimes, daemon assumptions, native TLS/OpenSSL, C build steps, or benchmark helpers to normal runtime paths without approval.
- `clinker-format -> cxl` is a current dependency for doc-path-aware envelope retention, but `docs/ai/80_OPEN_QUESTIONS.md` asks whether this edge is permanent.
- Keep output envelope config mirrored locally through `OutputEnvelopeSpec`; this crate should not depend on planner config types.

## Important invariants

- `FormatReader` and `FormatWriter` are `Send`, not `Sync`; readers and writers are single-thread-owned streaming objects.
- `FormatReader::schema(&mut self)` may peek or consume because some formats infer schema.
- Readers should stream records and avoid full-file/full-document buffering for file-backed sources.
- `ReopenableSource` reopens paths for multi-pass readers; one-shot buffering is the bounded pathless fallback.
- JSON/XML envelope pre-scans retain only declared `$doc.*` paths and enforce `max_index_bytes`.
- Reader/writer wrappers must delegate hooks such as `current_source_file`, `prepare_document`, `take_envelope_events`, `advance_to_next_file`, `begin_document`, `end_document`, and `bytes_written`.
- `FormatError::StructuralCount` is the only format error class marked for document-DLQ reclassification; other corruption remains fatal.
- Non-JSON writers reject `Value::Map` payloads instead of inventing scalar encodings.
- EDI/HL7 positional ceilings re-exported from readers must stay aligned with planner validation.
- `SplittingWriter` is a rotation orchestrator; format-specific headers, preambles, and footers belong in writer factories and concrete writers.

## Common mistakes for AI agents to avoid

- Buffering a whole JSON/XML file or collecting large record arrays when streaming helpers already exist.
- Adding executor/runtime behavior here instead of keeping this crate format-focused.
- Treating envelope events as an unordered side channel.
- Forgetting BOM handling when adding a text reader.
- Putting format lifecycle logic directly into split rotation.
- Adding a new format without reader/writer tests, error tests, docs, and envelope/document-context coverage when applicable.
- Copying stale internal docs or older format examples without checking current source and tests.

## Local commands

- **Inferred:** `cargo check -p clinker-format --locked --offline`
- **Inferred:** `cargo test -p clinker-format --locked --offline`
- **Inferred:** `cargo check --benches -p clinker-format --locked --offline`
- **Inferred:** `cargo test -p clinker-format --test streaming_doc_index_json --locked --offline`
- **Inferred:** `cargo test -p clinker-format --test streaming_doc_index_xml --locked --offline`
- **Inferred, performance-sensitive only:** `cargo bench -p clinker-format --bench io_throughput`

For Rust source changes, also run the workspace gates from the root
`AGENTS.md` unless the user explicitly scopes validation narrower.

## Documentation updates

Update relevant docs/examples when this crate changes format config, format
semantics, document context, envelopes, writer output, structural validation,
or public format APIs:

- `docs/user/src/formats/*.md`
- `docs/user/src/pipelines/envelope-and-doc-context.md`
- `docs/user/src/pipelines/error-handling.md`
- `docs/user/src/formats/auto-widen.md`
- `docs/engine/src/output-internals.md`
- Relevant `examples/pipelines/**` and `benches/pipelines/format/**`
- `docs/ai/*.md`, especially crate map, design rules, common patterns, testing, and open questions

## Unclear / ask human

- Is the `clinker-format -> cxl` dependency intended permanently, or should doc-path analysis move elsewhere?
- Hypothesis: broad non-UTF-8 or charset expansion beyond current format-specific handling needs human review before implementation or docs.
- Hypothesis: older internal format notes may be historical; verify against current source before using them as guidance.

## Evidence

- `crates/clinker-format/Cargo.toml`
- `crates/clinker-format/src/lib.rs`
- `crates/clinker-format/src/traits.rs`
- `crates/clinker-format/src/error.rs`
- `crates/clinker-format/src/bom.rs`
- `crates/clinker-format/src/source.rs`
- `crates/clinker-format/src/doc_index.rs`
- `crates/clinker-format/src/envelope.rs`
- `crates/clinker-format/src/envelope_writer.rs`
- `crates/clinker-format/src/counting.rs`
- `crates/clinker-format/src/splitting/mod.rs`
- `crates/clinker-format/src/json/reader.rs`
- `crates/clinker-format/src/xml/reader.rs`
- `crates/clinker-format/tests/streaming_doc_index_json.rs`
- `crates/clinker-format/tests/streaming_doc_index_xml.rs`
- `crates/clinker-format/benches/io_throughput.rs`
- `docs/ai/90_CRATE_AGENT_PLAN.md`
