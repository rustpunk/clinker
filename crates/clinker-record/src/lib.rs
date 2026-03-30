pub mod coercion;
pub mod counters;
pub mod minimal;
pub mod provenance;
pub mod record;
pub mod resolver;
pub mod schema;
pub mod value;

// Re-export primary types at crate root for ergonomic imports
pub use coercion::{
    coerce_to_bool, coerce_to_date, coerce_to_datetime, coerce_to_float, coerce_to_int,
    coerce_to_string, CoercionError, DEFAULT_DATETIME_FORMATS, DEFAULT_DATE_FORMATS,
};
pub use counters::PipelineCounters;
pub use minimal::MinimalRecord;
pub use provenance::RecordProvenance;
pub use record::Record;
pub use resolver::{FieldResolver, WindowContext};
pub use schema::Schema;
pub use value::Value;
