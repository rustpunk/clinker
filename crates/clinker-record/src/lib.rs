pub mod accumulator;
pub mod coercion;
pub mod counters;
pub mod group_key;
pub mod minimal;
pub mod provenance;
pub mod record;
pub mod record_view;
pub mod resolver;
pub mod schema;
pub mod schema_def;
pub mod storage;
pub mod value;

// Re-export primary types at crate root for ergonomic imports
pub use coercion::{
    CoercionError, DEFAULT_DATE_FORMATS, DEFAULT_DATETIME_FORMATS, coerce_to_bool, coerce_to_date,
    coerce_to_datetime, coerce_to_float, coerce_to_int, coerce_to_string,
};
pub use counters::PipelineCounters;
pub use group_key::{GroupByKey, GroupKeyError, value_to_group_key};
pub use minimal::MinimalRecord;
pub use provenance::RecordProvenance;
pub use record::{Record, RecordPayload};
pub use record_view::RecordView;
pub use resolver::{FieldResolver, HashMapResolver, WindowContext};
pub use schema::{Schema, SchemaBuilder};
pub use storage::RecordStorage;
pub use value::{NULL, Value};
