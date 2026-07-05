use crate::lexer::Span;

/// Unique identifier for an AST node. Assigned monotonically by the parser.
/// Used as index into side-tables (bindings, types, regexes).
/// Fits in alignment padding — size_of::<Expr>() stays at 64 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

/// A complete CXL program — a sequence of statements.
#[derive(Debug, Clone)]
pub struct Program {
    pub statements: Vec<Statement>,
    pub span: Span,
}

/// Trace severity level. Default is Trace when omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Where a `Statement::Emit` writes its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitTarget {
    /// Regular output column. `emit name = expr`.
    Field,
    /// Pipeline-scope declared state. `emit $pipeline.<key> = expr`.
    /// Routed through `StableEvalContext::pipeline_vars` at eval time;
    /// reader sees the value via `$pipeline.<key>` from any DAG
    /// descendant.
    Pipeline,
    /// Source-scope declared state. `emit $source.<key> = expr`.
    /// Keyed by per-record `source_file` Arc; multi-file Sources get
    /// per-file slots.
    Source,
    /// Record-scope declared state. `emit $record.<key> = expr`.
    /// Per-record private slot; never serializes to output.
    Record,
}

/// Top-level CXL statement.
#[derive(Debug, Clone)]
pub enum Statement {
    Let {
        node_id: NodeId,
        name: Box<str>,
        expr: Expr,
        span: Span,
    },
    Emit {
        node_id: NodeId,
        name: Box<str>,
        expr: Expr,
        /// Where the emitted value lands:
        /// - `Field`: regular output column on the record (`emit name = expr`).
        /// - `Pipeline`/`Source`/`Record`: producer-declared scoped state,
        ///   read downstream via `$pipeline.<key>` / `$source.<key>` /
        ///   `$record.<key>`. The variable must be declared in a Transform's
        ///   `config.declares:` block; the eval routing writes through
        ///   `StableEvalContext` (pipeline/source) or `Record::record_vars`
        ///   (record) per the variant.
        target: EmitTarget,
        span: Span,
    },
    Trace {
        node_id: NodeId,
        level: Option<TraceLevel>,
        guard: Option<Box<Expr>>,
        message: Expr,
        span: Span,
    },
    UseStmt {
        node_id: NodeId,
        path: Vec<Box<str>>,
        alias: Option<Box<str>>,
        span: Span,
    },
    ExprStmt {
        node_id: NodeId,
        expr: Expr,
        span: Span,
    },
    Filter {
        node_id: NodeId,
        predicate: Expr,
        span: Span,
    },
    Distinct {
        node_id: NodeId,
        /// None = bare `distinct` (all fields), Some = `distinct by <field>`
        field: Option<Box<str>>,
        span: Span,
    },
    /// Fan-out statement: `emit each <binding> in <source> { <body> }`.
    /// Evaluates `source` to a `Value::Array`, then for each element runs
    /// `body` with `binding` bound to the element value. Body emit
    /// statements produce one output record per iteration. Null source
    /// emits zero records. The body may contain further `emit each` /
    /// `emit each ... outer` blocks — fan-out within fan-out for one
    /// trigger row — governed by one cumulative `max_expansion` budget
    /// across all nesting levels.
    EmitEach {
        node_id: NodeId,
        binding: Box<str>,
        source: Expr,
        body: Vec<Statement>,
        span: Span,
    },
    /// Outer fan-out statement: `emit each <binding> in <source> outer { <body> }`.
    /// Identical to [`Statement::EmitEach`] for a non-empty array source,
    /// but a null or empty-array source still emits the trigger row once
    /// with `binding` bound to null rather than emitting zero records —
    /// the outer-join (`LATERAL VIEW OUTER EXPLODE`) shape that preserves
    /// a trigger row carrying no array elements. The body may nest
    /// further fan-out blocks, the same as `emit each`.
    ExplodeOuter {
        node_id: NodeId,
        binding: Box<str>,
        source: Expr,
        body: Vec<Statement>,
        span: Span,
    },
}

/// Visit every `Statement::Emit { target: Field, .. }` reachable from
/// `stmts`, descending through `Statement::EmitEach.body` and
/// `Statement::ExplodeOuter.body`. The visitor receives `(name, expr)`
/// for each field-targeted emit.
///
/// Use anywhere downstream code collects the bare field names a CXL
/// program writes to the record stream. A non-recursive walker is
/// silently wrong for programs whose only output emits live inside
/// `emit each` — bind-schema, executor route resolution, and the
/// transform write-set all diverge from runtime behavior, surfacing as
/// schema mismatches at the first downstream operator. Fan-out may
/// nest, so the descent recurses to arbitrary depth.
pub fn for_each_field_emit<'a>(stmts: &'a [Statement], visit: &mut dyn FnMut(&'a str, &'a Expr)) {
    for stmt in stmts {
        match stmt {
            Statement::Emit {
                name,
                expr,
                target: EmitTarget::Field,
                ..
            } => visit(name.as_ref(), expr),
            Statement::EmitEach { body, .. } | Statement::ExplodeOuter { body, .. } => {
                for_each_field_emit(body, visit)
            }
            _ => {}
        }
    }
}

/// True when `stmts` contains at least one `Statement::Emit` (any
/// target), recursing through `Statement::EmitEach.body` and
/// `Statement::ExplodeOuter.body`. Mirrors the "does this program emit
/// anything" check while staying coherent in the presence of fan-out.
pub fn contains_emit(stmts: &[Statement]) -> bool {
    stmts.iter().any(|stmt| match stmt {
        Statement::Emit { .. } => true,
        Statement::EmitEach { body, .. } | Statement::ExplodeOuter { body, .. } => {
            contains_emit(body)
        }
        _ => false,
    })
}

/// CXL expression — the core of the language. All variants carry a NodeId and Span.
#[derive(Debug, Clone)]
pub enum Expr {
    Binary {
        node_id: NodeId,
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        span: Span,
    },
    Unary {
        node_id: NodeId,
        op: UnaryOp,
        operand: Box<Expr>,
        span: Span,
    },
    Literal {
        node_id: NodeId,
        value: LiteralValue,
        span: Span,
    },
    FieldRef {
        node_id: NodeId,
        name: Box<str>,
        span: Span,
    },
    /// Qualified field reference — N-part dotted path.
    /// 2 parts: source.field (existing). 3 parts: source.record_type.field (multi-record).
    QualifiedFieldRef {
        node_id: NodeId,
        parts: Box<[Box<str>]>,
        span: Span,
    },
    MethodCall {
        node_id: NodeId,
        receiver: Box<Expr>,
        method: Box<str>,
        args: Vec<Expr>,
        span: Span,
    },
    Match {
        node_id: NodeId,
        subject: Option<Box<Expr>>,
        arms: Vec<MatchArm>,
        span: Span,
    },
    IfThenElse {
        node_id: NodeId,
        condition: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Option<Box<Expr>>,
        span: Span,
    },
    Coalesce {
        node_id: NodeId,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        span: Span,
    },
    WindowCall {
        node_id: NodeId,
        function: Box<str>,
        args: Vec<Expr>,
        span: Span,
    },
    PipelineAccess {
        node_id: NodeId,
        field: Box<str>,
        span: Span,
    },
    /// Static configuration knob: `$vars.<key>`. Declared in the
    /// pipeline's top-level `vars:` block with an optional default;
    /// resolves to a value frozen at pipeline start (channel overrides
    /// applied once at startup). Distinct from `$pipeline.*` /
    /// `$source.*` / `$record.*` which are producer-written scoped
    /// state with DAG-descendant read semantics; `$vars.*` is read-only
    /// and visible from any node.
    VarsAccess {
        node_id: NodeId,
        key: Box<str>,
        span: Span,
    },
    /// Composition configuration parameter: `$config.<param>`. Reads a
    /// value declared in the enclosing composition's `_compose.config_schema`
    /// and resolved for this instantiation (signature default, call-site
    /// `config:`, or channel/group `config:` clobber). Valid only inside a
    /// composition body; a top-level pipeline declares no config schema, so
    /// the resolver rejects `$config.*` there.
    ///
    /// This variant never reaches evaluation: the planner constant-folds
    /// each `$config.<param>` to the instantiation's resolved value (an
    /// [`Expr::Literal`]) at compile time, so two instantiations with
    /// different config compile to different bodies with no runtime
    /// plumbing.
    ConfigAccess {
        node_id: NodeId,
        param: Box<str>,
        span: Span,
    },
    /// Per-record source provenance: `$source.file`, `$source.row`.
    /// Distinct namespace from `$pipeline.*` (pipeline-stable per-run state)
    /// because per-record values cannot be pipeline-scope when one Source
    /// matches multiple files or multiple Sources feed a join.
    SourceAccess {
        node_id: NodeId,
        field: Box<str>,
        span: Span,
    },
    /// User-declared per-record scoped variable: `$record.<key>`.
    ///
    /// Typed, declared-at-pipeline-top, multi-writer slot. The
    /// variable must be declared in a Transform's `config.declares:`
    /// block with `scope: record`.
    RecordAccess {
        node_id: NodeId,
        field: Box<str>,
        span: Span,
    },
    /// Qualified per-input source-scope read: `$source.<input_name>.<field>`.
    ///
    /// Used downstream of `Merge` / `Combine` to disambiguate which
    /// upstream Source's source-scope value to read. The plain
    /// [`Expr::SourceAccess`] form is rejected by E172 in that
    /// context — this variant is the explicit
    /// alternative. `input_name` matches the upstream Merge/Combine
    /// input name; `field` resolves against the same
    /// `vars.source.<key>` declaration as the unqualified form.
    QualifiedSourceAccess {
        node_id: NodeId,
        input_name: Box<str>,
        field: Box<str>,
        span: Span,
    },
    /// Envelope-section read: `$doc.<section>.<field>`.
    ///
    /// `<section>` names a user-declared envelope section from the
    /// source's `envelope.sections:` config (no engine-reserved
    /// names). `<field>` names a field within that section's parsed
    /// payload. Resolves through the per-record
    /// `Arc<DocumentContext>`'s sections map at eval time. Sections
    /// are populated by the reader's envelope pre-scan before any
    /// body record streams, so every body record sees every declared
    /// `$doc.<section>.<field>` value throughout the body stream.
    DocAccess {
        node_id: NodeId,
        section: Box<str>,
        field: Box<str>,
        span: Span,
    },
    /// The `now` keyword — wall-clock DateTime at the point of evaluation.
    Now {
        node_id: NodeId,
        span: Span,
    },
    Wildcard {
        node_id: NodeId,
        span: Span,
    },
    /// Free-standing aggregate function call (sum, count, avg, min, max,
    /// collect, weighted_avg). Parsed via NUD lookahead: Ident + LParen where
    /// the identifier matches a known aggregate name. Distinct from
    /// MethodCall (which is receiver-based via `.`).
    AggCall {
        node_id: NodeId,
        name: Box<str>,
        args: Vec<Expr>,
        span: Span,
    },
    /// Extractor-produced leaf: references the Nth accumulator slot in a
    /// CompiledAggregate. Never emitted by the parser; introduced by
    /// `extract_aggregates` when rewriting an emit RHS that contains AggCalls.
    /// At finalize time, the aggregate evaluator resolves this to the
    /// accumulator's `finalize()` result.
    AggSlot {
        node_id: NodeId,
        slot: u32,
        span: Span,
    },
    /// Extractor-produced leaf: references the Nth group-by key column.
    /// Introduced when a bare `FieldRef` in an emit RHS matches a group-by
    /// field name — reverses the `value_to_group_key → Value` conversion at
    /// finalize time.
    GroupKey {
        node_id: NodeId,
        slot: u32,
        span: Span,
    },
    /// Bracket-index access: `arr[0]` (array element) or `map["key"]`
    /// (map field). The runtime dispatches by receiver type — integer
    /// indices apply to `Value::Array`, string indices to `Value::Map`.
    /// Out-of-bounds or type mismatch returns `Value::Null`.
    IndexAccess {
        node_id: NodeId,
        receiver: Box<Expr>,
        index: Box<Expr>,
        span: Span,
    },
    /// Arrow-syntax closure: `it => body`. The closure parameter name
    /// is `it` by convention. Free-standing closures are rejected at
    /// resolve time; closures appear only as arguments to closure-bearing
    /// builtins (`filter`, `map`, `find`, `any`, `flat_map`).
    Closure {
        node_id: NodeId,
        param: Box<str>,
        body: Box<Expr>,
        span: Span,
    },
}

impl Expr {
    /// Get the span of this expression.
    pub fn span(&self) -> Span {
        match self {
            Expr::Binary { span, .. }
            | Expr::Unary { span, .. }
            | Expr::Literal { span, .. }
            | Expr::FieldRef { span, .. }
            | Expr::QualifiedFieldRef { span, .. }
            | Expr::MethodCall { span, .. }
            | Expr::Match { span, .. }
            | Expr::IfThenElse { span, .. }
            | Expr::Coalesce { span, .. }
            | Expr::WindowCall { span, .. }
            | Expr::PipelineAccess { span, .. }
            | Expr::VarsAccess { span, .. }
            | Expr::ConfigAccess { span, .. }
            | Expr::SourceAccess { span, .. }
            | Expr::RecordAccess { span, .. }
            | Expr::QualifiedSourceAccess { span, .. }
            | Expr::DocAccess { span, .. }
            | Expr::Now { span, .. }
            | Expr::Wildcard { span, .. }
            | Expr::AggCall { span, .. }
            | Expr::AggSlot { span, .. }
            | Expr::GroupKey { span, .. }
            | Expr::IndexAccess { span, .. }
            | Expr::Closure { span, .. } => *span,
        }
    }

    /// Get the NodeId of this expression.
    pub fn node_id(&self) -> NodeId {
        match self {
            Expr::Binary { node_id, .. }
            | Expr::Unary { node_id, .. }
            | Expr::Literal { node_id, .. }
            | Expr::FieldRef { node_id, .. }
            | Expr::QualifiedFieldRef { node_id, .. }
            | Expr::MethodCall { node_id, .. }
            | Expr::Match { node_id, .. }
            | Expr::IfThenElse { node_id, .. }
            | Expr::Coalesce { node_id, .. }
            | Expr::WindowCall { node_id, .. }
            | Expr::PipelineAccess { node_id, .. }
            | Expr::VarsAccess { node_id, .. }
            | Expr::ConfigAccess { node_id, .. }
            | Expr::SourceAccess { node_id, .. }
            | Expr::RecordAccess { node_id, .. }
            | Expr::QualifiedSourceAccess { node_id, .. }
            | Expr::DocAccess { node_id, .. }
            | Expr::Now { node_id, .. }
            | Expr::Wildcard { node_id, .. }
            | Expr::AggCall { node_id, .. }
            | Expr::AggSlot { node_id, .. }
            | Expr::GroupKey { node_id, .. }
            | Expr::IndexAccess { node_id, .. }
            | Expr::Closure { node_id, .. } => *node_id,
        }
    }

    /// Accumulate the set of input column references this expression
    /// (and its sub-expressions) reads. Used by the planner's
    /// deferred-region column-pruning pass to compute the minimum
    /// buffer schema a producing Aggregate needs to carry forward to
    /// commit time.
    ///
    /// `$pipeline.*` and `$ck.*` are skipped — they are system
    /// namespaces, not record-schema columns. The deferred buffer
    /// carries `$ck.*` shadow columns implicitly via row identity, so
    /// they don't need tracking here. This mirrors the analyzer's
    /// `walk_expr` namespace-exclusion convention.
    pub fn support_into(&self, fields: &mut std::collections::HashSet<String>) {
        match self {
            Expr::FieldRef { name, .. } => {
                if !is_system_namespace(name) {
                    fields.insert(name.to_string());
                }
            }
            Expr::QualifiedFieldRef { parts, .. } => {
                if let Some(first) = parts.first()
                    && !is_system_namespace(first)
                {
                    fields.insert(
                        parts
                            .iter()
                            .map(|p| p.as_ref())
                            .collect::<Vec<_>>()
                            .join("."),
                    );
                }
            }
            Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
                lhs.support_into(fields);
                rhs.support_into(fields);
            }
            Expr::Unary { operand, .. } => operand.support_into(fields),
            Expr::IfThenElse {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                condition.support_into(fields);
                then_branch.support_into(fields);
                if let Some(e) = else_branch {
                    e.support_into(fields);
                }
            }
            Expr::Match { subject, arms, .. } => {
                if let Some(s) = subject {
                    s.support_into(fields);
                }
                for arm in arms {
                    arm.pattern.support_into(fields);
                    arm.body.support_into(fields);
                }
            }
            Expr::MethodCall { receiver, args, .. } => {
                receiver.support_into(fields);
                for a in args {
                    a.support_into(fields);
                }
            }
            Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
                for a in args {
                    a.support_into(fields);
                }
            }
            Expr::IndexAccess {
                receiver, index, ..
            } => {
                receiver.support_into(fields);
                index.support_into(fields);
            }
            // The closure parameter `it` is a local binding rather than
            // an upstream column reference, so its body's reads are
            // walked but `it` itself is never injected into the field
            // set (the FieldRef arm sees it as a name without a
            // backing column, which is already filtered out elsewhere).
            Expr::Closure { body, .. } => body.support_into(fields),
            Expr::PipelineAccess { .. }
            | Expr::VarsAccess { .. }
            | Expr::ConfigAccess { .. }
            | Expr::SourceAccess { .. }
            | Expr::RecordAccess { .. }
            | Expr::QualifiedSourceAccess { .. }
            | Expr::DocAccess { .. }
            | Expr::Now { .. }
            | Expr::Wildcard { .. }
            | Expr::Literal { .. }
            | Expr::AggSlot { .. }
            | Expr::GroupKey { .. } => {}
        }
    }
}

fn is_system_namespace(name: &str) -> bool {
    name.starts_with("$pipeline")
        || name.starts_with("$source")
        || name.starts_with("$doc")
        || name.starts_with("$ck")
        || name.starts_with("$widened")
}

/// Accumulate the input column references read by every statement in a
/// [`Program`], folding [`Expr::support_into`] over each statement's
/// expressions. Only the columns the program *reads* are added — `emit`
/// and `let` targets are write positions and are never inserted. `emit
/// each` / `emit each ... outer` bodies are walked recursively.
///
/// The program-level companion of [`Expr::support_into`]: lineage and
/// column-pruning consumers use it to recover a typechecked program's
/// read-set (qualified `input.field` references included) without
/// re-parsing CXL source. `$pipeline.*` / `$source.*` / `$doc.*` /
/// `$ck.*` / `$widened.*` system namespaces are excluded, per
/// `Expr::support_into`.
pub fn program_support_into(program: &Program, fields: &mut std::collections::HashSet<String>) {
    for stmt in &program.statements {
        match stmt {
            Statement::Emit { expr, .. } | Statement::Let { expr, .. } => {
                expr.support_into(fields);
            }
            Statement::Filter { predicate, .. } => predicate.support_into(fields),
            Statement::Trace { guard, message, .. } => {
                if let Some(g) = guard.as_deref() {
                    g.support_into(fields);
                }
                message.support_into(fields);
            }
            Statement::ExprStmt { expr, .. } => expr.support_into(fields),
            Statement::Distinct { .. } | Statement::UseStmt { .. } => {}
            Statement::EmitEach { source, body, .. }
            | Statement::ExplodeOuter { source, body, .. } => {
                source.support_into(fields);
                // Recurse via a fresh Program-shaped wrapper so the
                // same statement walker covers body statements
                // without duplicating the per-statement match.
                let inner = Program {
                    statements: body.clone(),
                    span: Span::new(0, 0),
                };
                program_support_into(&inner, fields);
            }
        }
    }
}

/// Constant-fold every `$config.<param>` ([`Expr::ConfigAccess`]) in a
/// [`Program`] to the literal value `resolve` returns for that param.
///
/// The planner calls this once per composition instantiation, after the
/// body program has typechecked, so `$config` never reaches evaluation:
/// two instantiations with different resolved config fold to different
/// literals — and therefore different compiled bodies — with no runtime
/// plumbing. `resolve` returns `None` only for a param outside the
/// composition's declared config schema; the resolver has already
/// rejected such a read, so a valid typechecked program never leaves a
/// `ConfigAccess` unfolded.
pub fn fold_config_program(program: &mut Program, resolve: &impl Fn(&str) -> Option<LiteralValue>) {
    for stmt in &mut program.statements {
        fold_config_stmt(stmt, resolve);
    }
}

fn fold_config_stmt(stmt: &mut Statement, resolve: &impl Fn(&str) -> Option<LiteralValue>) {
    match stmt {
        Statement::Emit { expr, .. }
        | Statement::Let { expr, .. }
        | Statement::ExprStmt { expr, .. } => fold_config_expr(expr, resolve),
        Statement::Filter { predicate, .. } => fold_config_expr(predicate, resolve),
        Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard.as_mut() {
                fold_config_expr(g, resolve);
            }
            fold_config_expr(message, resolve);
        }
        Statement::EmitEach { source, body, .. } | Statement::ExplodeOuter { source, body, .. } => {
            fold_config_expr(source, resolve);
            for inner in body.iter_mut() {
                fold_config_stmt(inner, resolve);
            }
        }
        Statement::Distinct { .. } | Statement::UseStmt { .. } => {}
    }
}

/// Recursively fold `$config.<param>` inside one expression, mirroring the
/// recursion shape of [`walk_expr`](crate::analyzer::visitor::walk_expr).
fn fold_config_expr(expr: &mut Expr, resolve: &impl Fn(&str) -> Option<LiteralValue>) {
    match expr {
        Expr::ConfigAccess {
            node_id,
            param,
            span,
        } => {
            if let Some(value) = resolve(param) {
                *expr = Expr::Literal {
                    node_id: *node_id,
                    value,
                    span: *span,
                };
            }
        }
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            fold_config_expr(lhs, resolve);
            fold_config_expr(rhs, resolve);
        }
        Expr::Unary { operand, .. } => fold_config_expr(operand, resolve),
        Expr::MethodCall { receiver, args, .. } => {
            fold_config_expr(receiver, resolve);
            for a in args.iter_mut() {
                fold_config_expr(a, resolve);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject.as_deref_mut() {
                fold_config_expr(s, resolve);
            }
            for arm in arms.iter_mut() {
                fold_config_expr(&mut arm.pattern, resolve);
                fold_config_expr(&mut arm.body, resolve);
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            fold_config_expr(condition, resolve);
            fold_config_expr(then_branch, resolve);
            if let Some(e) = else_branch.as_deref_mut() {
                fold_config_expr(e, resolve);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args.iter_mut() {
                fold_config_expr(a, resolve);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            fold_config_expr(receiver, resolve);
            fold_config_expr(index, resolve);
        }
        Expr::Closure { body, .. } => fold_config_expr(body, resolve),
        // Leaves and non-`$config` namespaces hold no `ConfigAccess`.
        Expr::Literal { .. }
        | Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

/// Add `base` to every [`NodeId`] in `expr` and all of its
/// sub-expressions (and match-arm nodes), in place.
///
/// Splicing an independently parsed expression into a larger synthetic
/// program requires relocating its node ids: each parse numbers its
/// nodes from zero, so two separately parsed exprs share the low ids.
/// Offsetting one expr past the ids already consumed by the combined
/// program keeps the per-node side-tables the resolver and typechecker
/// index by [`NodeId`] (bindings, types, regexes) dense and
/// collision-free — every distinct node keeps a distinct index below the
/// combined node count. Spans are left untouched: only the id-space is
/// relocated, so callers must not treat the shifted ids as byte offsets.
///
/// `base` of zero is a no-op. Ids saturate rather than wrap at
/// [`u32::MAX`], though a real program stays orders of magnitude below
/// that ceiling.
pub fn offset_node_ids(expr: &mut Expr, base: u32) {
    if base == 0 {
        return;
    }
    fn shift(id: &mut NodeId, base: u32) {
        id.0 = id.0.saturating_add(base);
    }
    match expr {
        Expr::Binary {
            node_id, lhs, rhs, ..
        }
        | Expr::Coalesce {
            node_id, lhs, rhs, ..
        } => {
            shift(node_id, base);
            offset_node_ids(lhs, base);
            offset_node_ids(rhs, base);
        }
        Expr::Unary {
            node_id, operand, ..
        } => {
            shift(node_id, base);
            offset_node_ids(operand, base);
        }
        Expr::MethodCall {
            node_id,
            receiver,
            args,
            ..
        } => {
            shift(node_id, base);
            offset_node_ids(receiver, base);
            for a in args.iter_mut() {
                offset_node_ids(a, base);
            }
        }
        Expr::Match {
            node_id,
            subject,
            arms,
            ..
        } => {
            shift(node_id, base);
            if let Some(s) = subject.as_deref_mut() {
                offset_node_ids(s, base);
            }
            for arm in arms.iter_mut() {
                shift(&mut arm.node_id, base);
                offset_node_ids(&mut arm.pattern, base);
                offset_node_ids(&mut arm.body, base);
            }
        }
        Expr::IfThenElse {
            node_id,
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            shift(node_id, base);
            offset_node_ids(condition, base);
            offset_node_ids(then_branch, base);
            if let Some(e) = else_branch.as_deref_mut() {
                offset_node_ids(e, base);
            }
        }
        Expr::WindowCall { node_id, args, .. } | Expr::AggCall { node_id, args, .. } => {
            shift(node_id, base);
            for a in args.iter_mut() {
                offset_node_ids(a, base);
            }
        }
        Expr::IndexAccess {
            node_id,
            receiver,
            index,
            ..
        } => {
            shift(node_id, base);
            offset_node_ids(receiver, base);
            offset_node_ids(index, base);
        }
        Expr::Closure { node_id, body, .. } => {
            shift(node_id, base);
            offset_node_ids(body, base);
        }
        // Leaf variants: shift the id, no children to descend.
        Expr::Literal { node_id, .. }
        | Expr::FieldRef { node_id, .. }
        | Expr::QualifiedFieldRef { node_id, .. }
        | Expr::PipelineAccess { node_id, .. }
        | Expr::VarsAccess { node_id, .. }
        | Expr::ConfigAccess { node_id, .. }
        | Expr::SourceAccess { node_id, .. }
        | Expr::RecordAccess { node_id, .. }
        | Expr::QualifiedSourceAccess { node_id, .. }
        | Expr::DocAccess { node_id, .. }
        | Expr::Now { node_id, .. }
        | Expr::Wildcard { node_id, .. }
        | Expr::AggSlot { node_id, .. }
        | Expr::GroupKey { node_id, .. } => shift(node_id, base),
    }
}

#[derive(Debug, Clone)]
pub struct MatchArm {
    pub node_id: NodeId,
    pub pattern: Expr,
    pub body: Expr,
    pub span: Span,
}

/// Module-level function declaration. Pure: single expression body.
#[derive(Debug, Clone)]
pub struct FnDecl {
    pub node_id: NodeId,
    pub name: Box<str>,
    pub params: Vec<Box<str>>,
    pub body: Box<Expr>,
    pub span: Span,
}

/// A parsed CXL module file (only fn declarations and let constants).
#[derive(Debug, Clone)]
pub struct Module {
    pub functions: Vec<FnDecl>,
    pub constants: Vec<ModuleConst>,
    pub span: Span,
}

/// A module-level constant binding.
#[derive(Debug, Clone)]
pub struct ModuleConst {
    pub node_id: NodeId,
    pub name: Box<str>,
    pub expr: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(Box<str>),
    Date(chrono::NaiveDate),
    Bool(bool),
    Null,
}

// Compile-time thread safety assertions
static_assertions::assert_impl_all!(Program: Send, Sync);
static_assertions::assert_impl_all!(Statement: Send, Sync);
static_assertions::assert_impl_all!(Expr: Send, Sync);
static_assertions::assert_impl_all!(FnDecl: Send, Sync);
static_assertions::assert_impl_all!(Module: Send, Sync);
static_assertions::assert_impl_all!(ModuleConst: Send, Sync);
static_assertions::assert_impl_all!(MatchArm: Send, Sync);
static_assertions::assert_impl_all!(NodeId: Send, Sync);

#[cfg(test)]
mod tests {
    use super::*;

    /// Dummy NodeId for test construction (tests don't need unique IDs).
    const NID: NodeId = NodeId(0);

    #[test]
    fn test_ast_program_construction() {
        let span = Span::new(0, 10);
        let program = Program {
            statements: vec![
                Statement::Let {
                    node_id: NID,
                    name: "x".into(),
                    expr: Expr::Literal {
                        node_id: NID,
                        value: LiteralValue::Int(1),
                        span,
                    },
                    span,
                },
                Statement::Emit {
                    node_id: NID,
                    name: "out".into(),
                    expr: Expr::FieldRef {
                        node_id: NID,
                        name: "x".into(),
                        span,
                    },
                    target: EmitTarget::Field,
                    span,
                },
                Statement::ExprStmt {
                    node_id: NID,
                    expr: Expr::Literal {
                        node_id: NID,
                        value: LiteralValue::Null,
                        span,
                    },
                    span,
                },
            ],
            span,
        };
        assert_eq!(program.statements.len(), 3);
    }

    #[test]
    fn test_program_support_into_reads_only_no_targets() {
        let span = Span::new(0, 1);
        let field = |n: &str| Expr::FieldRef {
            node_id: NID,
            name: n.into(),
            span,
        };
        let program = Program {
            statements: vec![
                // Filter predicate reads `a`.
                Statement::Filter {
                    node_id: NID,
                    predicate: field("a"),
                    span,
                },
                // Emit reads `b` + `c`; its target `out` is a write, not a read.
                Statement::Emit {
                    node_id: NID,
                    name: "out".into(),
                    expr: Expr::Binary {
                        node_id: NID,
                        op: BinOp::Add,
                        lhs: Box::new(field("b")),
                        rhs: Box::new(field("c")),
                        span,
                    },
                    target: EmitTarget::Field,
                    span,
                },
                // `emit each` source reads `items`; the recursive body walk
                // reaches `base`. The body emit target `y` is a write.
                Statement::EmitEach {
                    node_id: NID,
                    binding: "elem".into(),
                    source: field("items"),
                    body: vec![Statement::Emit {
                        node_id: NID,
                        name: "y".into(),
                        expr: field("base"),
                        target: EmitTarget::Field,
                        span,
                    }],
                    span,
                },
            ],
            span,
        };
        let mut got = std::collections::HashSet::new();
        program_support_into(&program, &mut got);
        let mut sorted: Vec<&str> = got.iter().map(String::as_str).collect();
        sorted.sort();
        assert_eq!(sorted, vec!["a", "b", "base", "c", "items"]);
        assert!(!got.contains("out"), "emit target is not a read");
        assert!(!got.contains("y"), "body emit target is not a read");
    }

    #[test]
    fn test_ast_emit_has_name() {
        let span = Span::new(0, 20);
        let stmt = Statement::Emit {
            node_id: NID,
            name: "member_name".into(),
            expr: Expr::FieldRef {
                node_id: NID,
                name: "label".into(),
                span,
            },
            target: EmitTarget::Field,
            span,
        };
        if let Statement::Emit { name, .. } = stmt {
            assert_eq!(&*name, "member_name");
        } else {
            panic!("expected Emit");
        }
    }

    #[test]
    fn test_ast_trace_with_level_and_guard() {
        let span = Span::new(0, 30);
        let stmt = Statement::Trace {
            node_id: NID,
            level: Some(TraceLevel::Warn),
            guard: Some(Box::new(Expr::Literal {
                node_id: NID,
                value: LiteralValue::Bool(true),
                span,
            })),
            message: Expr::Literal {
                node_id: NID,
                value: LiteralValue::String("alert".into()),
                span,
            },
            span,
        };
        if let Statement::Trace { level, guard, .. } = stmt {
            assert_eq!(level, Some(TraceLevel::Warn));
            assert!(guard.is_some());
        } else {
            panic!("expected Trace");
        }
    }

    #[test]
    fn test_ast_use_stmt_structured() {
        let span = Span::new(0, 25);
        let stmt = Statement::UseStmt {
            node_id: NID,
            path: vec!["shared".into(), "dates".into()],
            alias: Some("d".into()),
            span,
        };
        if let Statement::UseStmt { path, alias, .. } = stmt {
            assert_eq!(path.len(), 2);
            assert_eq!(&*path[0], "shared");
            assert_eq!(alias.as_deref(), Some("d"));
        } else {
            panic!("expected UseStmt");
        }
    }

    #[test]
    fn test_ast_fn_decl_single_expr() {
        let span = Span::new(0, 20);
        let decl = FnDecl {
            node_id: NID,
            name: "double".into(),
            params: vec!["x".into()],
            body: Box::new(Expr::Binary {
                node_id: NID,
                op: BinOp::Mul,
                lhs: Box::new(Expr::FieldRef {
                    node_id: NID,
                    name: "x".into(),
                    span,
                }),
                rhs: Box::new(Expr::Literal {
                    node_id: NID,
                    value: LiteralValue::Int(2),
                    span,
                }),
                span,
            }),
            span,
        };
        assert_eq!(&*decl.name, "double");
        assert_eq!(decl.params.len(), 1);
    }

    #[test]
    fn test_ast_expr_binary_nested() {
        let span = Span::new(0, 1);
        let leaf = || {
            Box::new(Expr::Literal {
                node_id: NID,
                value: LiteralValue::Int(1),
                span,
            })
        };
        let _deep = Expr::Binary {
            node_id: NID,
            op: BinOp::Add,
            lhs: Box::new(Expr::Binary {
                node_id: NID,
                op: BinOp::Mul,
                lhs: leaf(),
                rhs: leaf(),
                span,
            }),
            rhs: leaf(),
            span,
        };
    }

    #[test]
    fn test_ast_expr_size() {
        let size = std::mem::size_of::<Expr>();
        assert!(size <= 72, "Expr is {} bytes, budget is 72", size);
    }

    #[test]
    fn test_ast_all_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Program>();
        assert_send_sync::<Statement>();
        assert_send_sync::<Expr>();
        assert_send_sync::<MatchArm>();
        assert_send_sync::<FnDecl>();
        assert_send_sync::<NodeId>();
    }

    #[test]
    fn test_ast_match_condition_form() {
        let span = Span::new(0, 1);
        let m = Expr::Match {
            node_id: NID,
            subject: None,
            arms: vec![MatchArm {
                node_id: NID,
                pattern: Expr::Literal {
                    node_id: NID,
                    value: LiteralValue::Bool(true),
                    span,
                },
                body: Expr::Literal {
                    node_id: NID,
                    value: LiteralValue::String("yes".into()),
                    span,
                },
                span,
            }],
            span,
        };
        if let Expr::Match { subject, .. } = m {
            assert!(subject.is_none());
        }
    }

    #[test]
    fn test_ast_match_value_form() {
        let span = Span::new(0, 1);
        let m = Expr::Match {
            node_id: NID,
            subject: Some(Box::new(Expr::FieldRef {
                node_id: NID,
                name: "status".into(),
                span,
            })),
            arms: vec![],
            span,
        };
        if let Expr::Match { subject, .. } = m {
            assert!(subject.is_some());
        }
    }

    #[test]
    fn test_ast_debug_output() {
        let span = Span::new(0, 2);
        let expr = Expr::Literal {
            node_id: NID,
            value: LiteralValue::Int(42),
            span,
        };
        let dbg = format!("{:?}", expr);
        assert!(dbg.contains("42"));
    }

    #[test]
    fn test_ast_now_variant() {
        let span = Span::new(0, 3);
        let expr = Expr::Now { node_id: NID, span };
        assert_eq!(expr.span(), span);
        assert_eq!(expr.node_id(), NID);
    }

    #[test]
    fn test_ast_node_id_on_expr() {
        let span = Span::new(0, 1);
        let id = NodeId(42);
        let expr = Expr::Literal {
            node_id: id,
            value: LiteralValue::Int(1),
            span,
        };
        assert_eq!(expr.node_id(), id);
    }
}

#[cfg(test)]
mod support_into_tests {
    use super::*;
    use crate::parser::Parser;
    use std::collections::HashSet;

    /// Parse a single `emit out = <source>` and run `support_into` over the RHS.
    /// Covers every `Expr` variant the parser can construct from concrete syntax.
    fn fields_of(source: &str) -> HashSet<String> {
        let parsed = Parser::parse(&format!("emit out = {source}"));
        assert!(parsed.errors.is_empty(), "{:?}", parsed.errors);
        let stmt = parsed.ast.statements.first().expect("one stmt");
        let Statement::Emit { expr, .. } = stmt else {
            panic!("not emit")
        };
        let mut fields = HashSet::new();
        expr.support_into(&mut fields);
        fields
    }

    #[test]
    fn bare_field_ref() {
        assert_eq!(fields_of("amount"), HashSet::from(["amount".into()]));
    }

    #[test]
    fn binary_two_fields() {
        assert_eq!(fields_of("a + b"), HashSet::from(["a".into(), "b".into()]));
    }

    #[test]
    fn nested_if_coalesce() {
        let f = fields_of("if x > 0 then y ?? z else w");
        assert_eq!(
            f,
            HashSet::from(["x".into(), "y".into(), "z".into(), "w".into()])
        );
    }

    #[test]
    fn window_call_with_field_args() {
        let f = fields_of("$window.sum(amount)");
        assert!(f.contains("amount"));
    }

    #[test]
    fn method_chain_on_window_call() {
        let f = fields_of("$window.first(score).name");
        assert!(f.contains("score"));
    }

    #[test]
    fn qualified_field_ref() {
        let f = fields_of("orders.total");
        assert!(f.contains("orders.total"));
    }

    #[test]
    fn pipeline_namespace_excluded() {
        let f = fields_of("$pipeline.run_id");
        assert!(f.is_empty());
    }

    /// `$ck.*` references appear only as engine-stamped FieldRef
    /// names — the parser rejects `$ck.field` syntax (only
    /// `$pipeline` and `$window` are recognized system namespaces).
    /// Construct the FieldRef directly to exercise the
    /// namespace-exclusion path that fires on engine-injected
    /// shadow-column references.
    #[test]
    fn ck_namespace_excluded() {
        let span = Span::new(0, 0);
        let expr = Expr::FieldRef {
            node_id: NodeId(0),
            name: "$ck.employee_id".into(),
            span,
        };
        let mut f = HashSet::new();
        expr.support_into(&mut f);
        assert!(f.is_empty());
    }

    /// `$widened` is the engine-stamped sidecar absorber for
    /// `on_unmapped: auto_widen`. The parser rejects
    /// `$widened.<key>` syntax (catch-all "unknown system
    /// namespace"), but the column itself appears as a real
    /// Schema entry (`FieldMetadata::WidenedSidecar`); any
    /// engine-injected FieldRef whose name starts with `$widened`
    /// must be excluded from `support_into`'s buffer-schema
    /// computation. Constructs the FieldRef directly to exercise
    /// the namespace-exclusion arm in `is_system_namespace`.
    #[test]
    fn widened_namespace_excluded() {
        let span = Span::new(0, 0);
        let expr = Expr::FieldRef {
            node_id: NodeId(0),
            name: "$widened".into(),
            span,
        };
        let mut f = HashSet::new();
        expr.support_into(&mut f);
        assert!(
            f.is_empty(),
            "$widened FieldRef must be excluded; got {f:?}"
        );
    }
}

#[cfg(test)]
mod offset_node_ids_tests {
    use super::*;
    use crate::parser::Parser;

    /// Recursively collect every `NodeId` reachable from `e`, mirroring the
    /// recursion shape of [`offset_node_ids`] so the two stay in lockstep.
    fn collect_ids(e: &Expr, out: &mut Vec<u32>) {
        out.push(e.node_id().0);
        match e {
            Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
                collect_ids(lhs, out);
                collect_ids(rhs, out);
            }
            Expr::Unary { operand, .. } => collect_ids(operand, out),
            Expr::MethodCall { receiver, args, .. } => {
                collect_ids(receiver, out);
                for a in args {
                    collect_ids(a, out);
                }
            }
            Expr::Match { subject, arms, .. } => {
                if let Some(s) = subject {
                    collect_ids(s, out);
                }
                for arm in arms {
                    out.push(arm.node_id.0);
                    collect_ids(&arm.pattern, out);
                    collect_ids(&arm.body, out);
                }
            }
            Expr::IfThenElse {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                collect_ids(condition, out);
                collect_ids(then_branch, out);
                if let Some(e) = else_branch {
                    collect_ids(e, out);
                }
            }
            Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
                for a in args {
                    collect_ids(a, out);
                }
            }
            Expr::IndexAccess {
                receiver, index, ..
            } => {
                collect_ids(receiver, out);
                collect_ids(index, out);
            }
            Expr::Closure { body, .. } => collect_ids(body, out),
            _ => {}
        }
    }

    fn rhs_of(source: &str) -> Expr {
        let parsed = Parser::parse(&format!("emit out = {source}"));
        assert!(parsed.errors.is_empty(), "{:?}", parsed.errors);
        let stmt = parsed.ast.statements.into_iter().next().expect("one stmt");
        match stmt {
            Statement::Emit { expr, .. } => expr,
            _ => panic!("not an emit"),
        }
    }

    /// Every node id in a parsed expression shifts by exactly `base`, and
    /// the node count is unchanged — a pure relocation of the id-space.
    #[test]
    fn shifts_every_node_id_by_base() {
        // Covers Binary, AggCall, IfThenElse, FieldRef and Literal arms.
        let mut expr = rhs_of("sum(if amount > 0 then amount else 0) > count(id)");
        let mut before = Vec::new();
        collect_ids(&expr, &mut before);

        offset_node_ids(&mut expr, 100);

        let mut after = Vec::new();
        collect_ids(&expr, &mut after);
        assert_eq!(before.len(), after.len(), "node count is preserved");
        for (b, a) in before.iter().zip(&after) {
            assert_eq!(*a, b + 100, "each id shifts by exactly the base");
        }
    }

    /// A base of zero leaves every id untouched.
    #[test]
    fn zero_base_is_a_noop() {
        let mut expr = rhs_of("a + b * c");
        let mut before = Vec::new();
        collect_ids(&expr, &mut before);
        offset_node_ids(&mut expr, 0);
        let mut after = Vec::new();
        collect_ids(&expr, &mut after);
        assert_eq!(before, after);
    }

    /// Match-arm node ids relocate alongside the arms' pattern/body exprs,
    /// so a directly constructed `Match` shifts wholesale.
    #[test]
    fn shifts_match_arm_ids() {
        let span = Span::new(0, 1);
        let mut expr = Expr::Match {
            node_id: NodeId(1),
            subject: Some(Box::new(Expr::FieldRef {
                node_id: NodeId(2),
                name: "status".into(),
                span,
            })),
            arms: vec![MatchArm {
                node_id: NodeId(3),
                pattern: Expr::Literal {
                    node_id: NodeId(4),
                    value: LiteralValue::String("x".into()),
                    span,
                },
                body: Expr::Literal {
                    node_id: NodeId(5),
                    value: LiteralValue::Int(1),
                    span,
                },
                span,
            }],
            span,
        };
        offset_node_ids(&mut expr, 10);
        let Expr::Match {
            node_id,
            subject,
            arms,
            ..
        } = &expr
        else {
            panic!("expected Match");
        };
        assert_eq!(node_id.0, 11);
        assert_eq!(subject.as_ref().unwrap().node_id().0, 12);
        assert_eq!(arms[0].node_id.0, 13);
        assert_eq!(arms[0].pattern.node_id().0, 14);
        assert_eq!(arms[0].body.node_id().0, 15);
    }
}
