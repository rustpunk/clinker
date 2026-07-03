pub mod bom;
pub mod charset;
pub mod counting;
pub mod csv;
pub mod doc_index;
pub mod edifact;
pub mod envelope;
pub mod envelope_writer;
pub mod error;
pub mod fixed_width;
pub mod hl7;
pub mod json;
pub mod multi_record;
pub mod schema;
pub(crate) mod segment_tokenizer;
pub mod source;
pub mod splitting;
pub mod swift;
pub mod traits;
pub mod x12;
pub mod xml;

pub use charset::Charset;
pub use counting::{CountedFormatWriter, CountingWriter, SharedByteCounter};
pub use doc_index::DocArenaIndex;
pub use envelope::{
    EnvelopeConfig, EnvelopeEvent, EnvelopeExtract, EnvelopeFieldType, EnvelopeSection, FrameRole,
    NestedEnvelopeSection,
};
pub use envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
pub use error::FormatError;
pub use schema::{
    Column, Discriminator, GeneratedSchema, RECORD_TYPE_COLUMN, RecordType, SourceSchema,
    StructureConstraint, multi_record_superset,
};
pub use source::ReopenableSource;
pub use traits::{FormatReader, FormatWriter};

// Default positional-element/field ceilings each reader enforces on a
// body segment, re-exported under format-disambiguated names so the
// planner's `$doc` positional bound references the reader's own constant
// instead of a duplicated literal that could silently drift.
pub use edifact::reader::DEFAULT_MAX_ELEMENTS as EDIFACT_DEFAULT_MAX_ELEMENTS;
pub use hl7::reader::DEFAULT_MAX_FIELDS as HL7_DEFAULT_MAX_FIELDS;
pub use x12::reader::DEFAULT_MAX_ELEMENTS as X12_DEFAULT_MAX_ELEMENTS;

// Positional-column synthesis for `SourceSchema::Generated` EDI/HL7/SWIFT
// sources. Each function is the single source of truth for its format's
// column layout: the runtime reader's schema derives its names from the same
// list the planner seeds a Generated source's compile-time bind from, so the
// typechecked row and the runtime record schema never disagree.
pub use edifact::reader::generated_columns as edifact_generated_columns;
pub use hl7::Hl7FieldSplit;
pub use hl7::reader::generated_columns as hl7_generated_columns;
pub use swift::reader::generated_columns as swift_generated_columns;
pub use x12::reader::generated_columns as x12_generated_columns;
