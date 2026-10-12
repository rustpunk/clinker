//! What a composition body may name: the columns its input ports declare.
//!
//! A composition call behaves as its body written inline at the call site.
//! The body binds against the row the call site actually feeds each port, so
//! every column the upstream carries reaches the body in the upstream's
//! position, and a body node compiled against that row reads the values it
//! names. The port's declared schema is the body's vocabulary: the columns
//! the body may name. Every other author column passes through the body
//! unnamed. A body that names one is refused at compile time, so a body that
//! compiles at one call site compiles at every call site whose upstream
//! satisfies its ports.
//!
//! [`walk_body`] runs once per call site, after the body binds. It follows
//! each undeclared upstream column through the body in declaration order (the
//! order the bind walk binds in), drops it where a body node writes a column
//! of the same name, and reports every node that names one while it is still
//! the caller's column.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use clinker_core_types::span::Span;
use cxl::analyzer::visitor::{Visitor, walk_expr};
use cxl::ast::Expr;
use cxl::resolve::ResolvedBinding;
use cxl::typecheck::{Row, TypedProgram};

use crate::config::PipelineNode;
use crate::plan::PlanNodeId;
use crate::plan::bind_schema::CompileArtifacts;
use crate::yaml::Spanned;

/// One undeclared column a body node names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UndeclaredRead {
    /// The body node that names the column.
    pub node: String,
    /// The body node's line in the composition file.
    pub span: Span,
    /// The column it names.
    pub column: String,
    /// The input port the column reached the body through, undeclared.
    pub port: String,
}

/// The caller's columns that flow out of one body node unchanged.
#[derive(Debug, Clone, Default)]
struct Carried {
    /// Undeclared author columns still holding the caller's value, each with
    /// the port it entered through. Naming one of these is refused.
    undeclared: BTreeMap<String, String>,
    /// Every caller column still holding the caller's value, declared or
    /// not. Reported to an enclosing body as what this call passes through.
    unchanged: BTreeSet<String>,
}

impl Carried {
    fn union(mut self, other: &Carried) -> Carried {
        for (column, port) in &other.undeclared {
            self.undeclared
                .entry(column.clone())
                .or_insert_with(|| port.clone());
        }
        self.unchanged.extend(other.unchanged.iter().cloned());
        self
    }

    /// Keep only the columns `keep` admits.
    fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        self.undeclared.retain(|column, _| keep(column));
        self.unchanged.retain(|column| keep(column));
    }
}

/// One input port as the call site binds it.
pub(crate) struct PortEntry<'a> {
    pub port: &'a str,
    /// The row the call site feeds the port.
    pub upstream: &'a Row,
    /// The columns the port declares.
    pub declared: BTreeSet<String>,
}

/// The result of [`walk_body`] for one call site.
pub(crate) struct BodyWalk {
    /// Every undeclared column a body node names, in body order.
    pub reads: Vec<UndeclaredRead>,
    /// Per output port: the columns of its row that a body node wrote under
    /// the name of a caller column the port did not declare.
    pub shadows: Vec<(String, String)>,
    /// The caller columns the first output port carries unchanged.
    pub first_output_unchanged: BTreeSet<String>,
}

/// Whether `column` is an engine-stamped column (`$ck.*`, `$widened`,
/// `$source.*`, ...). Those are never author vocabulary, so a port never
/// declares them and a body may name the ones authors may name anywhere.
fn is_engine_column(column: &str) -> bool {
    column.starts_with('$')
}

/// Follow every caller column through `nodes` and report each undeclared one
/// a node names.
///
/// `rows` is the body's bound row per node name, as the bind walk left it.
/// `outputs` lists each output port with the body node it reads.
pub(crate) fn walk_body(
    nodes: &[Spanned<PipelineNode>],
    node_ids: &HashMap<String, PlanNodeId>,
    artifacts: &CompileArtifacts,
    rows: &HashMap<String, Row>,
    ports: &[PortEntry<'_>],
    outputs: &[(String, String)],
) -> BodyWalk {
    let mut carried: HashMap<String, Carried> = HashMap::new();
    let mut undeclared_anywhere: BTreeSet<String> = BTreeSet::new();
    for entry in ports {
        let mut state = Carried::default();
        for (field, _) in entry.upstream.fields() {
            let column = field.name.as_ref();
            if is_engine_column(column) {
                continue;
            }
            state.unchanged.insert(column.to_string());
            if !entry.declared.contains(column) {
                state
                    .undeclared
                    .insert(column.to_string(), entry.port.to_string());
                undeclared_anywhere.insert(column.to_string());
            }
        }
        carried.insert(entry.port.to_string(), state);
    }

    let mut reads = Vec::new();
    for spanned in nodes {
        let node = &spanned.value;
        let name = node.name();
        let Some(&id) = node_ids.get(name) else {
            continue;
        };
        let inputs: Vec<(Option<&str>, Carried)> = node_inputs(node)
            .into_iter()
            .map(|(qualifier, upstream)| {
                (
                    qualifier,
                    carried.get(upstream).cloned().unwrap_or_default(),
                )
            })
            .collect();
        let incoming = inputs
            .iter()
            .fold(Carried::default(), |acc, (_, state)| acc.union(state));

        let line = spanned.referenced.line() as u32;
        let span = if line > 0 {
            Span::line_only(line)
        } else {
            Span::SYNTHETIC
        };
        let mut named: Vec<String> = Vec::new();
        for column in node_reads(node, id, artifacts, rows) {
            let state = match &column.qualifier {
                Some(q) => inputs
                    .iter()
                    .find(|(qualifier, _)| *qualifier == Some(q.as_str()))
                    .map(|(_, state)| state)
                    .unwrap_or(&incoming),
                None => &incoming,
            };
            if let Some(port) = state.undeclared.get(&column.name)
                && !named.contains(&column.name)
            {
                named.push(column.name.clone());
                reads.push(UndeclaredRead {
                    node: name.to_string(),
                    span,
                    column: column.name,
                    port: port.clone(),
                });
            }
        }

        // What leaves this node unchanged: the incoming caller columns its
        // output row still carries and it did not write.
        let mut out = incoming;
        let writes = node_writes(node, id, artifacts);
        match rows.get(name) {
            Some(row) => out.retain(|column| row.has_field(column) && !writes.contains(column)),
            None => out = Carried::default(),
        }
        if let PipelineNode::Composition { .. } = node {
            // A nested call carries what its own body leaves unchanged.
            let passed = artifacts.composition_passthrough.get(&id);
            out.retain(|column| passed.is_some_and(|set| set.contains(column)));
        }
        carried.insert(name.to_string(), out);
    }

    let mut shadows = Vec::new();
    let mut first_output_unchanged = BTreeSet::new();
    for (index, (port, internal)) in outputs.iter().enumerate() {
        let state = carried.get(internal.as_str()).cloned().unwrap_or_default();
        if index == 0 {
            first_output_unchanged = state.unchanged.clone();
        }
        let Some(row) = rows.get(internal.as_str()) else {
            continue;
        };
        for (field, _) in row.fields() {
            let column = field.name.as_ref();
            if undeclared_anywhere.contains(column) && !state.undeclared.contains_key(column) {
                shadows.push((port.clone(), column.to_string()));
            }
        }
    }

    BodyWalk {
        reads,
        shadows,
        first_output_unchanged,
    }
}

/// Each input of `node` with the qualifier a Combine gives it, keyed by the
/// upstream node name (a `node.port` reference names `node`).
fn node_inputs(node: &PipelineNode) -> Vec<(Option<&str>, &str)> {
    match node {
        PipelineNode::Combine { header, .. } => header
            .input
            .iter()
            .map(|(qualifier, input)| (Some(qualifier.as_str()), input.value.name()))
            .collect(),
        // A Composition's call-site `inputs:` map is its wiring; each port
        // reads its own upstream, so the port name qualifies it.
        PipelineNode::Composition { inputs, .. } => inputs
            .iter()
            .map(|(port, upstream)| {
                (
                    Some(port.as_str()),
                    upstream.split('.').next().unwrap_or(upstream.as_str()),
                )
            })
            .collect(),
        other => other
            .direct_input_names()
            .into_iter()
            .map(|n| (None, n))
            .collect(),
    }
}

/// A column a node names: bare, or qualified by a Combine input.
struct ColumnRead {
    qualifier: Option<String>,
    name: String,
}

/// Every column `node` names: in its CXL programs and in its column-list
/// settings.
fn node_reads(
    node: &PipelineNode,
    id: PlanNodeId,
    artifacts: &CompileArtifacts,
    rows: &HashMap<String, Row>,
) -> Vec<ColumnRead> {
    let mut programs: Vec<&TypedProgram> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut qualified: Vec<ColumnRead> = Vec::new();
    let combine_qualifiers: Vec<&str> = match node {
        PipelineNode::Combine { header, .. } => header.input.keys().map(String::as_str).collect(),
        _ => Vec::new(),
    };
    if let Some(typed) = artifacts.typed_get(id) {
        programs.push(typed.as_ref());
    }
    match node {
        PipelineNode::Transform { config, .. } => {
            if let Some(window) = &config.analytic_window {
                names.extend(window.group_by.iter().cloned());
                names.extend(window.sort_by.iter().map(|s| s.field.clone()));
            }
            for directive in config.log.iter().flatten() {
                names.extend(directive.fields.iter().flatten().cloned());
            }
            for validation in config.validations.iter().flatten() {
                names.extend(validation.field.iter().cloned());
            }
            if let Some(conditions) = artifacts.transform_log_conditions.get(&id) {
                programs.extend(conditions.iter().flatten().map(|t| t.as_ref()));
            }
        }
        PipelineNode::Aggregate { config, .. } => {
            names.extend(config.group_by.iter().cloned());
        }
        PipelineNode::Route { .. } => {
            if let Some(branches) = artifacts.route_branch_typed.get(&id) {
                programs.extend(branches.iter().map(|t| t.as_ref()));
            }
        }
        PipelineNode::Combine { .. } => {
            if let Some(typed) = artifacts.combine_where_typed.get(&id) {
                programs.push(typed.as_ref());
            }
        }
        PipelineNode::Sink { config, .. } => {
            let sink = &config.sink;
            if let Some(mapping) = &sink.mapping {
                names.extend(mapping.entries().iter().map(|e| e.source.clone()));
            }
            names.extend(sink.exclude.iter().flatten().cloned());
            names.extend(
                sink.split
                    .iter()
                    .filter_map(|split| split.group_key.clone()),
            );
            names.extend(
                sink.sort_order
                    .iter()
                    .flatten()
                    .map(|spec| spec.clone().into_sort_field().field),
            );
        }
        PipelineNode::Reshape { config, .. } => {
            names.extend(config.partition_by.iter().cloned());
            names.extend(config.order_by.iter().map(|s| s.field.clone()));
            if let Some(rules) = artifacts.reshape_compiled.get(&id) {
                for rule in rules.iter() {
                    programs.push(rule.when.as_ref());
                    programs.extend(rule.set.iter().map(|(_, t)| t.as_ref()));
                    if let Some(synth) = &rule.synth {
                        programs.extend(synth.overrides.iter().map(|(_, t)| t.as_ref()));
                    }
                }
            }
        }
        PipelineNode::Cull { config, .. } => {
            names.extend(config.partition_by.iter().cloned());
            names.extend(config.order_by.iter().map(|s| s.field.clone()));
            if let Some(typed) = artifacts.cull_decision_typed.get(&id) {
                programs.push(typed.as_ref());
            }
        }
        PipelineNode::Envelope { header, .. } => {
            if let Some(synthesis) = artifacts.envelope_synthesis.get(&id) {
                programs.extend(synthesis.header.iter().map(|s| s.typed.as_ref()));
                // A footer keeps only its extracted aggregate: its field
                // arguments are positions in the body input row.
                let body_row = rows.get(header.body.value.name());
                for footer in &synthesis.footer {
                    for binding in &footer.compiled.bindings {
                        binding_arg_reads(&binding.arg, body_row, &mut names);
                    }
                }
            }
        }
        PipelineNode::Composition { .. } => {
            // A nested call names the columns its own ports declare.
            if let Some(body) = artifacts
                .composition_body_assignments
                .get(&id)
                .and_then(|body_id| artifacts.body_of(*body_id))
            {
                for (port, row) in &body.input_port_rows {
                    qualified.extend(row.field_names().map(|f| ColumnRead {
                        qualifier: Some(port.clone()),
                        name: f.name.to_string(),
                    }));
                }
            }
        }
        PipelineNode::Source { .. } | PipelineNode::Merge { .. } => {}
    }

    let mut reads: Vec<ColumnRead> = names
        .into_iter()
        .map(|name| ColumnRead {
            qualifier: None,
            name,
        })
        .collect();
    reads.extend(qualified);
    for program in programs {
        let mut collector = FieldReads {
            typed: program,
            combine_qualifiers: &combine_qualifiers,
            reads: &mut reads,
        };
        for statement in &program.program.statements {
            collector.visit_statement(statement);
        }
    }
    reads
}

/// The columns an extracted aggregate argument reads: a field position in
/// `row`, or the fields an expression argument names.
fn binding_arg_reads(arg: &cxl::plan::BindingArg, row: Option<&Row>, names: &mut Vec<String>) {
    use cxl::plan::BindingArg;
    match arg {
        BindingArg::Field(index) => {
            if let Some(field) = row.and_then(|r| r.field_names().nth(*index as usize)) {
                names.push(field.name.to_string());
            }
        }
        BindingArg::Wildcard => {}
        BindingArg::Expr(expr) => {
            let mut set = std::collections::HashSet::new();
            expr.support_into(&mut set);
            names.extend(set);
        }
        BindingArg::Pair(a, b) => {
            binding_arg_reads(a, row, names);
            binding_arg_reads(b, row, names);
        }
    }
}

/// The column names `node` writes: a column of that name leaving the node is
/// the body's, not the caller's.
fn node_writes(
    node: &PipelineNode,
    id: PlanNodeId,
    artifacts: &CompileArtifacts,
) -> BTreeSet<String> {
    let mut writes = BTreeSet::new();
    match node {
        PipelineNode::Transform { .. }
        | PipelineNode::Aggregate { .. }
        | PipelineNode::Combine { .. } => {
            if let Some(typed) = artifacts.typed_get(id) {
                cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, _| {
                    writes.insert(name.to_string());
                });
            }
        }
        PipelineNode::Reshape { config, .. } => {
            for rule in &config.rules {
                if let Some(mutate) = &rule.mutate {
                    writes.extend(mutate.set.keys().cloned());
                }
            }
            if let Some(rules) = artifacts.reshape_compiled.get(&id) {
                for rule in rules.iter() {
                    if let Some(synth) = &rule.synth {
                        writes.extend(synth.overrides.iter().map(|(name, _)| name.clone()));
                    }
                }
            }
        }
        _ => {}
    }
    writes
}

/// Collects every field a typed program reads as an input column: a field
/// reference the resolver bound to a column (never a `let`, a closure
/// parameter or a module member), and each `input.field` reference.
struct FieldReads<'a> {
    typed: &'a TypedProgram,
    combine_qualifiers: &'a [&'a str],
    reads: &'a mut Vec<ColumnRead>,
}

impl FieldReads<'_> {
    fn is_column(&self, node_id: cxl::ast::NodeId) -> bool {
        matches!(
            self.typed.bindings.get(node_id.0 as usize),
            Some(Some(ResolvedBinding::Field(_)))
        )
    }
}

impl Visitor for FieldReads<'_> {
    fn visit_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::FieldRef { node_id, name, .. } if self.is_column(*node_id) => {
                self.reads.push(ColumnRead {
                    qualifier: None,
                    name: name.to_string(),
                });
            }
            Expr::QualifiedFieldRef { node_id, parts, .. } if self.is_column(*node_id) => {
                if let [qualifier, field, ..] = parts.as_ref()
                    && self.combine_qualifiers.contains(&qualifier.as_ref())
                {
                    self.reads.push(ColumnRead {
                        qualifier: Some(qualifier.to_string()),
                        name: field.to_string(),
                    });
                } else if let Some(first) = parts.first() {
                    self.reads.push(ColumnRead {
                        qualifier: None,
                        name: first.to_string(),
                    });
                }
            }
            _ => {}
        }
        walk_expr(self, expr);
    }
}
