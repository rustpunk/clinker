pub mod types;
pub mod pass;

pub use types::Type;
pub use pass::{TypedProgram, TypeDiagnostic, type_check};
