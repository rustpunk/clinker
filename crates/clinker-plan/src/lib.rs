//! Pipeline config parsing, schema resolution, and DAG planning.
//!
//! Sits below the execution engine: it turns YAML pipeline definitions and
//! CXL programs into a typed, validated [`plan::execution::ExecutionPlanDag`]
//! without depending on any runtime operator. The execution crate consumes
//! this plan to drive a run.

pub mod config;
pub mod error;
pub mod overlay_ops;
pub mod plan;
pub mod runtime_error;
pub mod schema;
pub mod security;
pub mod span;
pub mod validation;
pub mod yaml;

pub use config::{
    CompileContext, CompositionFile, CompositionSignature, CompositionSymbolTable, NodeRef,
    OutputAlias, ParamDecl, ParamName, ParamType, PortDecl, PortName, ResourceDecl, ResourceKind,
    ResourceName, SourceMap, SpannedNodeRef, WORKSPACE_COMPOSITION_BUDGET,
    scan_workspace_signatures, validate_signatures,
};
pub use error::PipelineError;
pub use overlay_ops::{
    AddOp, LayeredOp, OverlayLayer, OverlayOp, OverlayOpError, RemoveOp, ReplaceOp,
    apply_overlay_ops,
};
pub use plan::{
    BoundBody, ColumnLookup, CompositionBodyId, QualifiedField, Row, RowTail, TailVarId,
};
pub use runtime_error::{BudgetCategory, SpillError};

#[cfg(test)]
mod rename_gates {
    //! Source-text gates asserting retired identifiers stay gone: the old
    //! `cxl_compile` module name and the retired `E100` diagnostic code
    //! must not reappear in the config, error, or plan sources.

    #[test]
    fn cxl_compile_module_name_is_absent() {
        let config_mod = include_str!("config/mod.rs");
        assert!(
            !config_mod.contains("pub mod cxl_compile"),
            "config/mod.rs must not declare pub mod cxl_compile"
        );
        let compiled = include_str!("plan/compiled.rs");
        assert!(
            !compiled.contains("config::cxl_compile"),
            "compiled.rs must not reference config::cxl_compile"
        );
    }

    #[test]
    fn e100_diagnostic_code_is_absent() {
        for (name, src) in [
            ("config/mod.rs", include_str!("config/mod.rs")),
            ("error.rs", include_str!("error.rs")),
        ] {
            assert!(
                !src.contains("\"E100\""),
                "found \"E100\" string literal in {name} — E100 should be fully retired"
            );
        }
    }
}
