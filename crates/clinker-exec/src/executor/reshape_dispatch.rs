//! `PlanNode::Reshape` dispatch arm.
//!
//! Per-correlation-group synthesis and trigger-row mutation. Blocking
//! grouping operator: it drains the predecessor's full output, groups
//! records by `partition_by`, optionally orders each group by `order_by`,
//! and per rule mutates the rows whose `when` predicate fires while
//! synthesizing new rows derived from those trigger rows. The dispatcher's
//! `Reshape` arm is a single delegating call into [`dispatch_reshape`].
//!
//! # Memory model
//!
//! The whole input materializes into per-group buffers before any output
//! row leaves — group observation must see the complete group. Disk-spill
//! governance for these buffers lands with the follow-on memory work; this
//! arm accumulates in memory bounded by the correlation group cap.
//!
//! # No-cascade contract
//!
//! Every rule observes the **original** group snapshot. A row mutated by
//! rule A is not re-observed by rule B; conflicting writes are detected and
//! the whole group is rolled back rather than silently order-dependent.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{GroupByKey, Record, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator};
use cxl::typecheck::{QualifiedField, Row, Type};
use petgraph::graph::NodeIndex;

use crate::executor::DlqEntry;
use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, drain_node_buffer_slot, node_buffer_spill_allowed,
    push_dlq, source_file_arc_of, source_name_arc_of, tee_emit_to_region_input_buffers,
};
use clinker_core_types::dlq::{DlqErrorCategory, stage_reshape_mutation_conflict};
use clinker_plan::config::pipeline_node::{
    CopyFrom, RESHAPE_MUTATED_BY_COLUMN, RESHAPE_SYNTHESIZED_BY_COLUMN, RESHAPE_SYNTHETIC_COLUMN,
    ReshapeBody,
};
use clinker_plan::config::{SortField, SortOrder};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, single_predecessor};

use crate::executor::NullStorage;

/// A compiled rule: the trigger predicate plus the per-field mutation and
/// synthesis assignment evaluators, all built against the live input
/// schema at dispatch time (the same condition-compile seam Route uses).
struct CompiledRule {
    name: String,
    /// `filter <when>` — `Emit` iff the row is a trigger row.
    when: ProgramEvaluator,
    /// `mutate.set` field → `emit <field> = <expr>` evaluator.
    set: Vec<(String, ProgramEvaluator)>,
    /// Present iff the rule synthesizes rows.
    synth: Option<CompiledSynth>,
}

/// Compiled synthesis action.
struct CompiledSynth {
    copy_from: CopyFrom,
    /// `overrides` field → `emit <field> = <expr>` evaluator.
    overrides: Vec<(String, ProgramEvaluator)>,
}

/// A detected runtime mutation conflict: two rules wrote the same field on
/// the same group row. Captured as owned data so the DLQ push (which needs
/// `&mut ExecutorContext`) runs after the eval context's immutable borrow
/// is released.
struct MutationConflict {
    rule_a: String,
    rule_b: String,
    field: String,
    record: Record,
    row_num: u64,
}

/// Execute the `Reshape` arm for `node_idx`. Drains the predecessor,
/// groups by `partition_by`, applies each rule's mutation and synthesis
/// per group, routes mutation conflicts to the DLQ (rolling back the whole
/// group), and emits originals (mutated, audit-stamped) followed by
/// synthesized rows. Blocking: the full input materializes before the
/// first output row leaves.
pub(crate) fn dispatch_reshape(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Reshape {
        ref name,
        ref config,
        ref output_schema,
        ..
    } = *node
    else {
        unreachable!("dispatch_reshape called with non-Reshape node");
    };

    let pred = single_predecessor(current_dag, node_idx, "reshape", name)?;
    let (input, input_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = match drain_node_buffer_slot(ctx, pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };

    if input.is_empty() {
        tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &[])?;
        let nb = admit_node_buffer(
            ctx,
            name,
            node_idx,
            Vec::new(),
            input_puncts,
            node_buffer_spill_allowed(current_dag, node_idx),
        )?;
        ctx.node_buffers.insert(node_idx, nb);
        return Ok(());
    }

    // Compile every rule's predicate / assignment program against the live
    // input schema. The schema is uniform across a node_buffer slot, so the
    // first record's schema drives the typecheck row.
    let input_schema = input[0].0.schema().clone();
    let mut rules = compile_rules(name, config, &input_schema)?;

    // Group records by `partition_by`, preserving first-seen group order
    // and within-group arrival order. Each entry is `(record, row_num)`.
    let mut group_order: Vec<Vec<GroupByKey>> = Vec::new();
    let mut groups: HashMap<Vec<GroupByKey>, Vec<(Record, u64)>> = HashMap::new();
    for (record, row_num) in input {
        let key = partition_key(&record, &config.partition_by);
        groups
            .entry(key.clone())
            .or_insert_with(|| {
                group_order.push(key.clone());
                Vec::new()
            })
            .push((record, row_num));
    }

    let mut out: Vec<(Record, u64)> = Vec::new();
    for key in &group_order {
        let mut group = groups.remove(key).expect("group key present in order");
        if !config.order_by.is_empty() {
            sort_group(&mut group, &config.order_by);
        }
        process_group(ctx, name, &mut rules, output_schema, group, &mut out)?;
    }

    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &out)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        out,
        input_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);
    Ok(())
}

/// Apply every rule to one group against its original snapshot, detect
/// cross-rule mutation conflicts, and append the group's output rows.
///
/// On a mutation conflict the whole group rolls back: a `MutationConflict`
/// DLQ entry is pushed for the colliding row and no output row from this
/// group reaches the buffer (matching the correlation-group atomicity
/// contract).
fn process_group(
    ctx: &mut ExecutorContext<'_>,
    node_name: &str,
    rules: &mut [CompiledRule],
    output_schema: &Arc<clinker_record::Schema>,
    group: Vec<(Record, u64)>,
    out: &mut Vec<(Record, u64)>,
) -> Result<(), PipelineError> {
    // Per original row: accumulated field writes (field → (value, rule
    // name)). A second write to an already-written field by a different
    // rule is a conflict; the stored rule name names the prior writer.
    let mut mutations: Vec<HashMap<String, (Value, String)>> =
        (0..group.len()).map(|_| HashMap::new()).collect();
    // Per original row: the rule labels that mutated it, for the
    // `$meta.mutated_by` audit stamp.
    let mut mutated_by: Vec<Vec<String>> = (0..group.len()).map(|_| Vec::new()).collect();
    // Synthesized rows accumulated across rules: `(record, source_row_num)`.
    let mut synthesized: Vec<(Record, u64)> = Vec::new();
    // First detected cross-rule mutation conflict, captured as owned data
    // so the whole group can roll back to the DLQ once the immutable
    // `ctx` borrow held by the eval context is released.
    let mut conflict: Option<MutationConflict> = None;

    'rules: for rule in rules.iter_mut() {
        for (row_idx, (record, row_num)) in group.iter().enumerate() {
            // Per-record evaluation context carries the record's own
            // source attribution and envelope. The Arcs live for the
            // record's loop iteration so the borrowing `EvalContext` is
            // valid across every rule-program eval below.
            let source_file = source_file_arc_of(record);
            let source_name = source_name_arc_of(record);
            let eval_ctx =
                ctx.eval_ctx_for_record(&source_file, &source_name, *row_num, record.doc_ctx());

            // `when` predicate: a trigger row emits.
            let triggered = matches!(
                rule.when
                    .eval_record::<NullStorage>(&eval_ctx, record, None)
                    .map_err(|e| reshape_eval_error(node_name, &rule.name, "when", e))?,
                EvalResult::Emit { .. } | EvalResult::EmitMany { .. }
            );
            if !triggered {
                continue;
            }

            // Mutation: evaluate each `set` expression and stage the write.
            let mut rule_mutated_this_row = false;
            for (field, evaluator) in rule.set.iter_mut() {
                let value = eval_scalar(evaluator, &eval_ctx, record, field)
                    .map_err(|e| reshape_eval_error(node_name, &rule.name, field, e))?;
                if let Some((_, prior_rule)) = mutations[row_idx].get(field) {
                    // Content-dependent collision two rules could not be
                    // proven disjoint at compile time. Capture the conflict
                    // and stop — the whole group rolls back to the DLQ.
                    conflict = Some(MutationConflict {
                        rule_a: prior_rule.clone(),
                        rule_b: rule.name.clone(),
                        field: field.clone(),
                        record: record.clone(),
                        row_num: *row_num,
                    });
                    break 'rules;
                }
                mutations[row_idx].insert(field.clone(), (value, rule.name.clone()));
                rule_mutated_this_row = true;
            }
            if rule_mutated_this_row {
                mutated_by[row_idx].push(rule_label(node_name, &rule.name));
            }

            // Synthesis: derive a new row from this trigger row.
            if let Some(synth) = rule.synth.as_mut() {
                let mut new_row = match synth.copy_from {
                    CopyFrom::Trigger => clone_into_schema(record, output_schema),
                    CopyFrom::None => Record::new(Arc::clone(output_schema), Vec::new()),
                };
                for (field, evaluator) in synth.overrides.iter_mut() {
                    let value = eval_scalar(evaluator, &eval_ctx, record, field)
                        .map_err(|e| reshape_eval_error(node_name, &rule.name, field, e))?;
                    new_row.set(field, value);
                }
                stamp_audit(&mut new_row, true, &rule_label(node_name, &rule.name), &[]);
                synthesized.push((new_row, *row_num));
            }
        }
    }

    // Conflict: the whole correlation group rolls back. No output row from
    // this group reaches the buffer.
    if let Some(c) = conflict {
        return dlq_group_conflict(ctx, node_name, &c);
    }

    // No conflict: emit originals (with staged mutations + audit stamps)
    // followed by synthesized rows, preserving within-group order.
    for (row_idx, (record, row_num)) in group.into_iter().enumerate() {
        let mut row = clone_into_schema(&record, output_schema);
        for (field, (value, _)) in mutations[row_idx].drain() {
            row.set(&field, value);
        }
        stamp_audit(&mut row, false, "", &mutated_by[row_idx]);
        out.push((row, row_num));
    }
    out.append(&mut synthesized);
    Ok(())
}

/// Push a `MutationConflict` DLQ entry for the colliding row and roll the
/// whole group back. Returns `Ok(())` so the caller skips emitting this
/// group's rows.
fn dlq_group_conflict(
    ctx: &mut ExecutorContext<'_>,
    node_name: &str,
    conflict: &MutationConflict,
) -> Result<(), PipelineError> {
    let MutationConflict {
        rule_a,
        rule_b,
        field,
        record,
        row_num,
    } = conflict;
    let stage = stage_reshape_mutation_conflict(node_name, rule_a, rule_b);
    let source_name = source_name_arc_of(record);
    push_dlq(
        ctx,
        DlqEntry {
            source_row: *row_num,
            category: DlqErrorCategory::MutationConflict,
            error_message: format!(
                "reshape {node_name:?}: rules {rule_a:?} and {rule_b:?} both write field \
                 {field:?} on the same row — the correlation group is rolled back"
            ),
            original_record: record.clone(),
            stage: Some(stage),
            route: None,
            trigger: true,
            source_name,
            triggering_field: Some(Arc::from(field.as_str())),
            triggering_value: None,
        },
    )
}

/// Evaluate a single-`emit` scalar program and extract the assigned value.
fn eval_scalar(
    evaluator: &mut ProgramEvaluator,
    eval_ctx: &EvalContext<'_>,
    record: &Record,
    field: &str,
) -> Result<Value, cxl::eval::EvalError> {
    match evaluator.eval_record::<NullStorage>(eval_ctx, record, None)? {
        EvalResult::Emit { mut fields, .. } => {
            Ok(fields.shift_remove(field).unwrap_or(Value::Null))
        }
        EvalResult::EmitMany { .. } | EvalResult::Skip(_) => Ok(Value::Null),
    }
}

/// Compile each rule's `when` / `set` / `overrides` CXL against `schema`.
fn compile_rules(
    node_name: &str,
    config: &ReshapeBody,
    schema: &Arc<clinker_record::Schema>,
) -> Result<Vec<CompiledRule>, PipelineError> {
    let type_cols: indexmap::IndexMap<QualifiedField, Type> = schema
        .columns()
        .iter()
        .map(|c| (QualifiedField::bare(c.as_ref()), Type::Any))
        .collect();
    let row = Row::closed(type_cols, cxl::lexer::Span::new(0, 0));
    let field_refs: Vec<&str> = schema.columns().iter().map(|c| c.as_ref()).collect();

    let mut compiled = Vec::with_capacity(config.rules.len());
    for rule in &config.rules {
        let when = compile_program(
            node_name,
            &rule.name,
            "when",
            &format!("filter {}", rule.when.source),
            &row,
            &field_refs,
        )?;
        let mut set = Vec::new();
        if let Some(mutate) = &rule.mutate {
            for (field, value) in &mutate.set {
                let prog = compile_program(
                    node_name,
                    &rule.name,
                    field,
                    &format!("emit {field} = {}", value.source),
                    &row,
                    &field_refs,
                )?;
                set.push((field.clone(), prog));
            }
        }
        let synth = match &rule.synthesize {
            None => None,
            Some(s) => {
                let mut overrides = Vec::new();
                for (field, value) in &s.overrides {
                    let prog = compile_program(
                        node_name,
                        &rule.name,
                        field,
                        &format!("emit {field} = {}", value.source),
                        &row,
                        &field_refs,
                    )?;
                    overrides.push((field.clone(), prog));
                }
                Some(CompiledSynth {
                    copy_from: s.copy_from,
                    overrides,
                })
            }
        };
        compiled.push(CompiledRule {
            name: rule.name.clone(),
            when,
            set,
            synth,
        });
    }
    Ok(compiled)
}

/// Parse → resolve → typecheck a single CXL fragment into a
/// [`ProgramEvaluator`]. Type errors here are a planner invariant violation
/// (bind_schema already typechecked the same fragment), so they surface as
/// `PipelineError::Compilation`.
fn compile_program(
    node_name: &str,
    rule_name: &str,
    field: &str,
    source: &str,
    row: &Row,
    field_refs: &[&str],
) -> Result<ProgramEvaluator, PipelineError> {
    let scoped_vars = cxl::resolve::ScopedVarsRegistry::default();
    let make_err = |messages: Vec<String>| PipelineError::Compilation {
        transform_name: format!("reshape:{node_name}:{rule_name}:{field}"),
        messages,
    };

    let parse_result = cxl::parser::Parser::parse(source);
    if !parse_result.errors.is_empty() {
        return Err(make_err(
            parse_result
                .errors
                .iter()
                .map(|e| e.message.clone())
                .collect(),
        ));
    }
    let resolved = cxl::resolve::resolve_program_with_modules_and_vars(
        parse_result.ast,
        field_refs,
        parse_result.node_count,
        &std::collections::HashMap::new(),
        &scoped_vars,
    )
    .map_err(|diags| make_err(diags.into_iter().map(|d| d.message).collect()))?;
    let typed = cxl::typecheck::pass::type_check_with_mode_and_vars(
        resolved,
        row,
        cxl::typecheck::pass::AggregateMode::Row,
        &scoped_vars,
    )
    .map_err(|diags| {
        make_err(
            diags
                .into_iter()
                .filter(|d| !d.is_warning)
                .map(|d| d.message)
                .collect(),
        )
    })?;
    Ok(ProgramEvaluator::new(Arc::new(typed), false))
}

/// Extract the `partition_by` key tuple from a record. Mirrors the
/// correlation-buffer keying: an empty string and a null both map to
/// `GroupByKey::Null` so a missing partition value groups consistently.
fn partition_key(record: &Record, partition_by: &[String]) -> Vec<GroupByKey> {
    partition_by
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let v = record.get(f).unwrap_or(&Value::Null);
            if v.is_null() || matches!(v, Value::String(s) if s.is_empty()) {
                return GroupByKey::Null;
            }
            clinker_record::value_to_group_key(v, f, None, idx as u64)
                .ok()
                .flatten()
                .unwrap_or(GroupByKey::Null)
        })
        .collect()
}

/// Sort a group in place by `order_by`, stable across equal keys (so
/// arrival order breaks ties deterministically).
fn sort_group(group: &mut [(Record, u64)], order_by: &[SortField]) {
    group.sort_by(|(a, _), (b, _)| {
        for sf in order_by {
            let av = a.get(&sf.field).unwrap_or(&Value::Null);
            let bv = b.get(&sf.field).unwrap_or(&Value::Null);
            let ord = match (av, bv) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                // Nulls sort last regardless of direction (SQL convention).
                (Value::Null, _) => std::cmp::Ordering::Greater,
                (_, Value::Null) => std::cmp::Ordering::Less,
                _ => av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal),
            };
            let ord = match sf.order {
                SortOrder::Asc => ord,
                SortOrder::Desc => ord.reverse(),
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Re-key a record onto the (audit-widened) output schema, carrying every
/// matching column value through and defaulting new columns to null.
fn clone_into_schema(record: &Record, schema: &Arc<clinker_record::Schema>) -> Record {
    let mut values = Vec::with_capacity(schema.column_count());
    for col in schema.columns() {
        values.push(record.get(col.as_ref()).cloned().unwrap_or(Value::Null));
    }
    Record::new(Arc::clone(schema), values)
}

/// Write the three `$meta.*` audit columns onto a row.
fn stamp_audit(row: &mut Record, synthetic: bool, synthesized_by: &str, mutated_by: &[String]) {
    row.set(RESHAPE_SYNTHETIC_COLUMN, Value::Bool(synthetic));
    row.set(
        RESHAPE_SYNTHESIZED_BY_COLUMN,
        Value::String(synthesized_by.into()),
    );
    row.set(
        RESHAPE_MUTATED_BY_COLUMN,
        Value::String(mutated_by.join(",").into()),
    );
}

/// `<node>:<rule>` audit label.
fn rule_label(node_name: &str, rule_name: &str) -> String {
    format!("{node_name}:{rule_name}")
}

/// Wrap a CXL eval error from a rule fragment as a hard pipeline error.
/// Rule-fragment eval errors are setup-invariant violations (the fragment
/// typechecked at bind time), so they abort rather than route to the DLQ.
fn reshape_eval_error(
    node_name: &str,
    rule_name: &str,
    field: &str,
    e: cxl::eval::EvalError,
) -> PipelineError {
    PipelineError::Internal {
        op: "reshape",
        node: node_name.to_string(),
        detail: format!("rule {rule_name:?} field {field:?} eval failed: {e}"),
    }
}
