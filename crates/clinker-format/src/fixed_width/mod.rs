//! Physical-byte readers and writers with explicit finite output resources.
//!
//! Resource-free writer construction is unavailable at either module path:
//!
//! ```compile_fail,E0432
//! use clinker_format::fixed_width::{FixedWidthWriter, FixedWidthWriterConfig};
//! let _ = FixedWidthWriter::new(Vec::<u8>::new(), vec![], FixedWidthWriterConfig::default());
//! ```
//!
//! ```compile_fail,E0432
//! use clinker_format::fixed_width::writer::{FixedWidthWriter, FixedWidthWriterConfig};
//! let _ = FixedWidthWriter::new(Vec::<u8>::new(), vec![], FixedWidthWriterConfig::default());
//! ```
//!
//! A prepared writer borrows its destination and admits retained configuration,
//! staged bytes and warning history through one finite resource provider:
//!
//! ```
//! use clinker_format::fixed_width::writer::{FixedWidthEncoder, FixedWidthWriterConfig};
//! use clinker_format::preparation::{MemoryOnlyResources, PreparedWriter};
//! use clinker_format::{Column, FormatWriter};
//! use clinker_record::{Record, Schema, Value, owned_storage::SharedStorage};
//! use cxl::typecheck::Type;
//! use std::{num::NonZeroUsize, sync::Arc};
//! # fn main() -> Result<(), clinker_format::FormatError> {
//! let mut column = Column::bare("name", Type::String);
//! column.width = Some(4);
//! let resources = MemoryOnlyResources::new(NonZeroUsize::new(1024 * 1024).unwrap());
//! let encoder = FixedWidthEncoder::new(&[column], &FixedWidthWriterConfig::default(), resources.resources())?;
//! let mut bytes = Vec::new();
//! {
//!     let mut writer = PreparedWriter::new(&mut bytes, encoder, resources.resources())?;
//!     let schema = SharedStorage::from_arc(Arc::new(Schema::new(vec!["name".into()])));
//!     writer.write_record(&Record::new(schema, vec![Value::String("é".into())]))?;
//!     writer.flush()?;
//!     assert_eq!(writer.encoder().record_count(), 1);
//!     assert!(writer.encoder().truncation_summary().is_none());
//! }
//! assert_eq!(bytes, "é  \n".as_bytes());
//! assert_eq!(resources.used(), 0);
//! # Ok(())
//! # }
//! ```

pub mod field;
pub mod reader;
pub mod writer;

pub use reader::{FixedWidthReader, FixedWidthReaderConfig};
pub use writer::{FixedWidthEncoder, FixedWidthEncoderConfig, FixedWidthWriterConfig};
