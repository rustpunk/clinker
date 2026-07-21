pub mod aggregate;
pub mod compile_context;
pub mod composition;
pub mod discovery;
pub mod error;
pub mod format;
pub mod fs_type;
pub mod multi_value;
pub mod node_header;
pub mod output;
pub mod patch;
pub mod path_template;
pub mod pipeline;
pub mod pipeline_node;
pub mod route;
pub mod scoped_var;
pub mod sort;
pub mod source;
pub mod storage;
pub mod transform;
pub mod utils;

pub use crate::plan::index::AnalyticWindowSpec;
pub use aggregate::*;
pub use clinker_format::{
    Column, Discriminator, RECORD_TYPE_COLUMN, RecordType, SourceSchema, StructureConstraint,
};
pub use compile_context::{CompileContext, ConfigOverrides};
pub use composition::{
    CompositionFile, CompositionSignature, CompositionSymbolTable, LayerKind, NodeRef, OutputAlias,
    ParamDecl, ParamName, ParamType, PortDecl, PortName, ProvenanceDb, ProvenanceLayer,
    ResolvedValue, Resource, ResourceDecl, ResourceKind, ResourceName, ScopedVarsSchema, SourceMap,
    SpannedNodeRef, WORKSPACE_COMPOSITION_BUDGET, scan_workspace_signatures, validate_signatures,
};
pub use discovery::{DiscoveredFile, DiscoveryError, DiscoveryOutcome, discover};
pub use error::*;
pub use format::*;
pub use fs_type::{FsKind, case_sensitive_dir, classify, collision_key, same_device};
pub use multi_value::{multi_value_columns, output_encodes_multi_value};
pub use node_header::{MergeHeader, NodeHeader, NodeInput, SourceHeader};
pub use output::*;
pub use patch::{
    BodySourcePatchMap, ColumnPatch, DiscriminatorPatch, EnvelopeFieldOp, NestedSectionOp,
    RecordTypeAdd, RecordTypeOp, RecordTypePatch, SchemaColumnOp, SourceConfigPatch, SplitFieldOp,
    SplitToRowsOp, SplitValuesOp, apply_source_patches,
};
pub use pipeline::*;
pub use pipeline_node::{
    AggregateBody, MergeBody, OutputBody, Phase, PipelineNode, RouteBody, SourceBody,
    TransformBody, VarScope,
};
pub use route::*;
pub use scoped_var::*;
pub use sort::*;
pub use source::*;
pub use storage::{
    ChannelLayout, Cleanup, ClinkerToml, CompressMode, GroupLayout, OnExisting, ShardScheme,
    SpillConfig, StagedPath, StagingMatcher, StagingPolicy, StagingVerify, StorageConfig,
    StorageConfigError,
};
pub use transform::*;
pub use utils::*;
