//! Compile-time CXL typecheck + schema propagation (`bind_schema`).
//!
//! Lifted from `config/cxl_compile.rs` in Phase 16c.1.4 (LD-16c-24).
//! Walks the unified `nodes:` DAG in topological order, seeds each
//! source's schema from its author-declared `schema:` block, typechecks
//! every Transform/Aggregate/Route body against the upstream Row, and
//! propagates the output Row downstream. Result: a `CompileArtifacts`
//! map keyed by node name holding one `Arc<TypedProgram>` per CXL-bearing
//! node.
//!
//! Errors surface as E200 diagnostics (CXL type error) and E201
//! diagnostics (missing source schema) with per-node spans.

use std::collections::HashMap;
use std::sync::Arc;

use cxl::typecheck::{AggregateMode, Row, Type, TypedProgram};
use indexmap::IndexMap;

use crate::config::pipeline_node::{PipelineNode, SchemaDecl};
use crate::error::{Diagnostic, LabeledSpan};
use crate::plan::bound_schemas::BoundSchemas;
use crate::span::Span;
use crate::yaml::Spanned;

/// Compile artifacts produced by `bind_schema` — one entry per node name
/// whose CXL body successfully type-checked, plus per-node row types.
#[derive(Debug, Default, Clone)]
pub struct CompileArtifacts {
    pub typed: HashMap<String, Arc<TypedProgram>>,
    /// Per-node bound row types (LD-16c-23). Populated during the
    /// `bind_schema` walk; persisted on `CompiledPlan` for downstream
    /// phases to consume.
    pub bound_schemas: BoundSchemas,
}

/// Run compile-time CXL typechecking over the unified `nodes:` DAG.
///
/// Walks nodes in topological order (declaration order is already
/// topologically sound per stage-3 validation), seeds source schemas
/// from each `SourceBody.schema` declaration, then typechecks and
/// propagates emitted columns downstream.
pub fn bind_schema(
    nodes: &[Spanned<PipelineNode>],
    diags: &mut Vec<Diagnostic>,
) -> CompileArtifacts {
    let mut artifacts = CompileArtifacts::default();

    // Schema state per node name (for sources: declared; for transforms:
    // propagated). Each entry is the Row available at the OUTPUT of that
    // node. Sources produce Closed rows; transforms propagate them.
    let mut schema_by_name: HashMap<String, Row> = HashMap::new();

    let span_for = |spanned: &Spanned<PipelineNode>| -> Span {
        let line = spanned.referenced.line() as u32;
        if line > 0 {
            Span::line_only(line)
        } else {
            // (c) serde-saphyr tagged-enum/flatten edge case.
            Span::SYNTHETIC
        }
    };

    for spanned in nodes {
        let node = &spanned.value;
        let name = node.name().to_string();
        let span = span_for(spanned);

        match node {
            PipelineNode::Source { config, .. } => {
                let schema_decl: &SchemaDecl = &config.schema;
                let columns = columns_from_decl(schema_decl);
                let cxl_span = cxl::lexer::Span::new(span.start as usize, span.start as usize);
                schema_by_name.insert(name, Row::closed(columns, cxl_span));
            }
            PipelineNode::Transform { header, config } => {
                let upstream = match upstream_schema(&header.input, &schema_by_name) {
                    Some(s) => s.clone(),
                    None => continue, // topology stage already diagnosed the break
                };
                match typecheck_cxl(
                    &name,
                    &config.cxl.source,
                    &upstream,
                    AggregateMode::Row,
                    span,
                ) {
                    Ok(typed) => {
                        let out = propagate_row(&upstream, &typed);
                        schema_by_name.insert(name.clone(), out);
                        artifacts.typed.insert(name, Arc::new(typed));
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Aggregate { header, config } => {
                let upstream = match upstream_schema(&header.input, &schema_by_name) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                // Validate group_by fields exist in the upstream schema.
                let mut missing = Vec::new();
                for gb in &config.group_by {
                    if !upstream.declared.contains_key(gb) {
                        missing.push(gb.clone());
                    }
                }
                if !missing.is_empty() {
                    diags.push(
                        Diagnostic::error(
                            "E200",
                            format!(
                                "aggregate {name:?}: group_by field(s) {missing:?} \
                                 not present in upstream schema"
                            ),
                            LabeledSpan::primary(span, String::new()),
                        )
                        .with_help(
                            "declare the column in the source's `schema:` block \
                             or emit it from an upstream transform",
                        ),
                    );
                    continue;
                }
                let agg_mode = AggregateMode::GroupBy {
                    group_by_fields: config.group_by.iter().cloned().collect(),
                };
                match typecheck_cxl(&name, &config.cxl.source, &upstream, agg_mode, span) {
                    Ok(typed) => {
                        let out = propagate_aggregate(&config.group_by, &upstream, &typed);
                        schema_by_name.insert(name.clone(), out);
                        artifacts.typed.insert(name, Arc::new(typed));
                    }
                    Err(d) => diags.push(d),
                }
            }
            PipelineNode::Route { header, config: _ } => {
                // Route pass-through: upstream schema flows through.
                if let Some(upstream) = upstream_schema(&header.input, &schema_by_name) {
                    let cloned = upstream.clone();
                    if let Ok(empty) = typecheck_cxl(&name, "", &cloned, AggregateMode::Row, span) {
                        artifacts.typed.insert(name.clone(), Arc::new(empty));
                    }
                    schema_by_name.insert(name, cloned);
                }
            }
            PipelineNode::Merge { header, .. } => {
                // Merge: union over all input schemas. Phase 16b uses
                // the first input's schema as the representative.
                if let Some(first) = header.inputs.first()
                    && let Some(upstream) = schema_by_name.get(input_target(first))
                {
                    schema_by_name.insert(name, upstream.clone());
                }
            }
            PipelineNode::Output { header, .. } => {
                // Output is a terminal sink; no downstream schema to propagate.
                if let Some(upstream) = upstream_schema(&header.input, &schema_by_name) {
                    schema_by_name.insert(name, upstream.clone());
                }
            }
            PipelineNode::Composition { .. } => {
                // Phase 16c stub — E100 already emitted by the topology
                // pre-pass. Skip.
            }
        }
    }

    // Persist all bound rows into BoundSchemas (LD-16c-23).
    for (node_name, row) in schema_by_name {
        artifacts.bound_schemas.set_output(node_name, row);
    }

    artifacts
}

fn columns_from_decl(decl: &SchemaDecl) -> IndexMap<String, Type> {
    decl.columns
        .iter()
        .map(|c| (c.name.clone(), c.ty.clone()))
        .collect()
}

fn upstream_schema<'a>(
    input: &crate::config::node_header::NodeInput,
    schemas: &'a HashMap<String, Row>,
) -> Option<&'a Row> {
    schemas.get(input_target(input))
}

fn input_target(input: &crate::config::node_header::NodeInput) -> &str {
    match input {
        crate::config::node_header::NodeInput::Single(s) => s.as_str(),
        crate::config::node_header::NodeInput::Port { node, .. } => node.as_str(),
    }
}

#[allow(clippy::result_large_err)]
fn typecheck_cxl(
    node_name: &str,
    source: &str,
    schema: &Row,
    mode: AggregateMode,
    span: Span,
) -> Result<TypedProgram, Diagnostic> {
    let parse_result = cxl::parser::Parser::parse(source);
    if !parse_result.errors.is_empty() {
        let messages: Vec<String> = parse_result
            .errors
            .iter()
            .map(|e| e.message.clone())
            .collect();
        return Err(Diagnostic::error(
            "E200",
            format!("CXL parse error in {node_name:?}: {}", messages.join("; ")),
            LabeledSpan::primary(span, String::new()),
        ));
    }
    let field_refs: Vec<&str> = schema.declared.keys().map(|s| s.as_str()).collect();
    let resolved =
        cxl::resolve::resolve_program(parse_result.ast, &field_refs, parse_result.node_count)
            .map_err(|diags| {
                Diagnostic::error(
                    "E200",
                    format!(
                        "CXL name resolution failed in {node_name:?}: {}",
                        diags
                            .into_iter()
                            .map(|d| d.message)
                            .collect::<Vec<_>>()
                            .join("; ")
                    ),
                    LabeledSpan::primary(span, String::new()),
                )
            })?;
    cxl::typecheck::type_check_with_mode(resolved, schema, mode).map_err(|diags| {
        let errors: Vec<String> = diags
            .iter()
            .filter(|d| !d.is_warning)
            .map(|d| d.message.clone())
            .collect();
        let joined = if errors.is_empty() {
            diags
                .into_iter()
                .map(|d| d.message)
                .collect::<Vec<_>>()
                .join("; ")
        } else {
            errors.join("; ")
        };
        Diagnostic::error(
            "E200",
            format!("CXL type error in {node_name:?}: {joined}"),
            LabeledSpan::primary(span, String::new()),
        )
    })
}

fn propagate_row(upstream: &Row, typed: &TypedProgram) -> Row {
    let mut out = upstream.declared.clone();
    for stmt in &typed.program.statements {
        if let cxl::ast::Statement::Emit { name, .. } = stmt
            && !out.contains_key(name.as_ref())
        {
            out.insert(name.to_string(), Type::Any);
        }
    }
    Row {
        declared: out,
        declared_span: upstream.declared_span,
        tail: upstream.tail.clone(),
    }
}

fn propagate_aggregate(group_by: &[String], upstream: &Row, typed: &TypedProgram) -> Row {
    let mut out: IndexMap<String, Type> = IndexMap::new();
    for gb in group_by {
        let t = upstream.declared.get(gb).cloned().unwrap_or(Type::Any);
        out.insert(gb.clone(), t);
    }
    for stmt in &typed.program.statements {
        if let cxl::ast::Statement::Emit { name, .. } = stmt
            && !out.contains_key(name.as_ref())
        {
            out.insert(name.to_string(), Type::Any);
        }
    }
    Row {
        declared: out,
        declared_span: upstream.declared_span,
        tail: upstream.tail.clone(),
    }
}

/// E201 diagnostic for a Source with no `schema:` field.
#[allow(dead_code)]
pub fn e201_missing_schema(source_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E201",
        format!("source {source_name:?} is missing required `schema:` field"),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "declare the source columns inline:\n  schema:\n    - { name: col1, type: string }\n    - { name: col2, type: int }",
    )
}
