pub mod bom;
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
pub(crate) mod segment_tokenizer;
pub mod source;
pub mod splitting;
pub mod swift;
pub mod traits;
pub mod x12;
pub mod xml;

pub use counting::{CountedFormatWriter, CountingWriter, SharedByteCounter};
pub use doc_index::DocArenaIndex;
pub use envelope::{
    EnvelopeConfig, EnvelopeEvent, EnvelopeExtract, EnvelopeFieldType, EnvelopeSection, FrameRole,
    NestedEnvelopeSection,
};
pub use envelope_writer::{EnvelopeFramer, OutputEnvelopeSpec};
pub use error::FormatError;
pub use source::ReopenableSource;
pub use traits::{FormatReader, FormatWriter};

// Default positional-element/field ceilings each reader enforces on a
// body segment, re-exported under format-disambiguated names so the
// planner's `$doc` positional bound references the reader's own constant
// instead of a duplicated literal that could silently drift.
pub use edifact::reader::DEFAULT_MAX_ELEMENTS as EDIFACT_DEFAULT_MAX_ELEMENTS;
pub use hl7::reader::DEFAULT_MAX_FIELDS as HL7_DEFAULT_MAX_FIELDS;
pub use x12::reader::DEFAULT_MAX_ELEMENTS as X12_DEFAULT_MAX_ELEMENTS;
