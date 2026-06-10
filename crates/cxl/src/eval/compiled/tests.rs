//! Golden and property tests for the compiled evaluator: each case pins
//! the [`EvalResult`] (or surfaced error) the compiled path must produce
//! for a given program and record.
//!
//! The compiled closures are now the sole per-record path — there is no
//! second evaluator to differentiate against — so every case is a single
//! evaluation whose projection is asserted against a hand-reasoned
//! expected literal derived from the CXL semantics of the program, not a
//! snapshot of whatever the evaluator currently emits. A correct expected
//! literal therefore catches a future regression in the lowering.
//!
//! The corpus exercises the full row-level surface the evaluator defines:
//! the scalar core, record-level method calls and closure-bearing array
//! builtins, window reads (aggregate / positional-chain / rank /
//! predicate-fold), `distinct` (across a record stream with shared dedup
//! state and partition resets), and `emit each` fan-out. A case projects
//! the result to plain owned data — the field map, the `$record.*`
//! channel, the skip reason, the `EmitMany` fan-out records, the
//! `$pipeline.*` / `$source.*` side-effect channel, and (on the error
//! path) the error kind, span, and boundary provenance — and asserts it
//! against the expected literal. Float results are NaN-canonicalized
//! first so an expected NaN compares equal rather than false-failing.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{RecordStorage, Value};
use proptest::prelude::*;

use super::program_has_distinct;
use crate::eval::context::{EvalContext, StableEvalContext};
use crate::eval::error::EvalError;
use crate::eval::{EvalResult, ProgramEvaluator, SkipReason};
use crate::parser::Parser;
use crate::resolve::HashMapResolver;
use crate::resolve::pass::resolve_program;
use crate::typecheck::pass::{TypedProgram, type_check};
use crate::typecheck::row::Row;

/// Window storage stand-in for the no-window scalar-core programs under
/// test. Every method is unreachable here; the scalar core never reads
/// the window, so the golden tests always pass `None`.
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u64, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u64) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u64 {
        0
    }
}

fn empty_row() -> Row {
    Row::closed(indexmap::IndexMap::new(), crate::lexer::Span::new(0, 0))
}

/// Parse → resolve → typecheck a CXL program against the given input
/// field names, attaching the source text so error spans render with the
/// program source through the compiled evaluator. Returns `None` when the
/// program fails to parse, resolve, or typecheck — the proptest uses this
/// to discard generated programs that are not well-typed CXL (e.g. a field
/// used as both numeric and string), since the evaluator is not defined on
/// them.
fn try_type_program(src: &str, fields: &[&str]) -> Option<TypedProgram> {
    let parsed = Parser::parse(src);
    if !parsed.errors.is_empty() {
        return None;
    }
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).ok()?;
    Some(
        type_check(resolved, &empty_row())
            .ok()?
            .with_source(Arc::from(src)),
    )
}

/// Like [`try_type_program`] but panics on any failure. Used by the
/// hand-written corpus, where every program is valid CXL by
/// construction.
fn type_program(src: &str, fields: &[&str]) -> TypedProgram {
    let parsed = Parser::parse(src);
    assert!(
        parsed.errors.is_empty(),
        "parse errors: {:?}",
        parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap_or_else(|d| {
        panic!(
            "resolve errors: {:?}",
            d.iter().map(|e| &e.message).collect::<Vec<_>>()
        )
    });
    type_check(resolved, &empty_row())
        .unwrap_or_else(|d| {
            panic!(
                "type errors: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        })
        .with_source(Arc::from(src))
}

/// Ordered `(key, value)` pairs from an emit channel, captured for
/// equality comparison.
type Pairs = Vec<(String, Value)>;

/// One emitted record's two channels (fields, record-vars) in a
/// comparable form.
type EmitRecordProjection = (Pairs, Pairs);

/// Canonical, comparable projection of an [`EvalResult`] (or the error
/// it surfaced). Reduces the evaluator's output to plain owned data so
/// a single `assert_eq!` covers fields, record-vars, skip reason, and —
/// on the error path — the error kind string, span, and boundary
/// provenance fields.
#[derive(Debug, PartialEq)]
enum ResultProjection {
    Emit {
        fields: Pairs,
        record_vars: Pairs,
    },
    EmitMany {
        records: Vec<EmitRecordProjection>,
    },
    Skip(SkipReason),
    Err {
        kind: String,
        span: crate::lexer::Span,
        source_row: Option<u64>,
        source_expr: Option<String>,
        triggering_field: Option<String>,
    },
}

/// Replace every NaN float in `v` (including those nested inside arrays
/// and maps) with one canonical NaN bit pattern.
///
/// `Value`'s own `PartialEq` compares floats by `to_bits()`, so two NaNs
/// that agree only on "is a NaN" but carry different payload bits would
/// compare unequal and false-fail the assertion even though
/// an expected NaN would be wanted. Canonicalizing here mirrors the
/// distinct-key path's NaN handling so an expected NaN compares equal.
fn canonicalize_nan(v: &Value) -> Value {
    match v {
        Value::Float(f) if f.is_nan() => Value::Float(f64::NAN),
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_nan).collect()),
        Value::Map(m) => {
            let mut out: indexmap::IndexMap<Box<str>, Value> = indexmap::IndexMap::new();
            for (k, mv) in m.iter() {
                out.insert(k.clone(), canonicalize_nan(mv));
            }
            Value::Map(Box::new(out))
        }
        other => other.clone(),
    }
}

fn project(result: &Result<EvalResult, EvalError>) -> ResultProjection {
    fn pairs(m: &indexmap::IndexMap<String, Value>) -> Pairs {
        m.iter()
            .map(|(k, v)| (k.clone(), canonicalize_nan(v)))
            .collect()
    }
    match result {
        Ok(EvalResult::Emit {
            fields,
            record_vars,
        }) => ResultProjection::Emit {
            fields: pairs(fields),
            record_vars: pairs(record_vars),
        },
        Ok(EvalResult::EmitMany { records }) => ResultProjection::EmitMany {
            records: records
                .iter()
                .map(|r| (pairs(&r.fields), pairs(&r.record_vars)))
                .collect(),
        },
        Ok(EvalResult::Skip(reason)) => ResultProjection::Skip(*reason),
        Err(e) => ResultProjection::Err {
            // The kind enum is not PartialEq; its Debug form captures
            // every payload field deterministically, so comparing the
            // rendered string is an exact-equality check on the kind.
            kind: format!("{:?}", e.kind),
            span: e.span,
            source_row: e.source_row,
            source_expr: e.source_expr.as_ref().map(|s| s.to_string()),
            triggering_field: e.triggering_field.as_ref().map(|s| s.to_string()),
        },
    }
}

/// The post-run side-effect channel: the `$pipeline.*` and `$source.*`
/// variable maps a run wrote through its [`StableEvalContext`]. These
/// writes are pure side effects — they never appear in [`EvalResult`] —
/// so capturing them lets a case pin emit routing for the pipeline/source
/// targets, which an `EvalResult`-only assertion cannot see.
///
/// `pipeline` is the ordered `(key, value)` list from `pipeline_vars`.
/// `source` is keyed by source-file path (sorted for determinism, since
/// the underlying store is a `HashMap`), each entry carrying that file's
/// ordered `(key, value)` list.
#[derive(Debug, PartialEq)]
struct SideEffects {
    pipeline: Pairs,
    source: Vec<(String, Pairs)>,
}

/// Snapshot the `$pipeline.*` / `$source.*` writes a run left on its
/// `StableEvalContext`, NaN-canonicalizing every value so an expected NaN
/// compares equal rather than false-failing (mirroring [`project`]).
fn snapshot_side_effects(stable: &StableEvalContext) -> SideEffects {
    let pipeline: Pairs = stable
        .pipeline_vars
        .read()
        .expect("pipeline_vars lock poisoned")
        .iter()
        .map(|(k, v)| (k.clone(), canonicalize_nan(v)))
        .collect();
    let mut source: Vec<(String, Pairs)> = stable
        .source_vars
        .read()
        .expect("source_vars lock poisoned")
        .iter()
        .map(|(file, vars)| {
            let pairs: Pairs = vars
                .iter()
                .map(|(k, v)| (k.clone(), canonicalize_nan(v)))
                .collect();
            (file.to_string(), pairs)
        })
        .collect();
    // The source store is a `HashMap`, so its iteration order is
    // per-instance randomized; sort by file path so two snapshots compare
    // structurally rather than by incidental bucket order.
    source.sort_by(|a, b| a.0.cmp(&b.0));
    SideEffects { pipeline, source }
}

/// Evaluate `src` against `record` through the compiled evaluator and
/// return the projected [`EvalResult`]. Callers assert it against the
/// expected literal.
fn eval_project(src: &str, fields: &[&str], record: HashMap<String, Value>) -> ResultProjection {
    eval_project_capturing(src, fields, record).0
}

/// Like [`eval_project`] but also returns the `$pipeline.*` / `$source.*`
/// side-effect snapshot, so a caller can additionally assert the concrete
/// values written through that channel.
///
/// The program runs through the live `ProgramEvaluator::eval_record`
/// entry — the same wiring (and error boundary) production uses — so the
/// case exercises the real path, not a test-local re-implementation.
fn eval_project_capturing(
    src: &str,
    fields: &[&str],
    record: HashMap<String, Value>,
) -> (ResultProjection, SideEffects) {
    let typed = type_program(src, fields);
    let has_distinct = program_has_distinct(&typed);

    let stable = StableEvalContext::test_default();
    let resolver = HashMapResolver::new(record);

    let ctx = EvalContext::test_default_borrowed(&stable);
    let mut eval = ProgramEvaluator::new(Arc::new(typed), has_distinct);
    let result = eval.eval_record::<NullStorage>(&ctx, &resolver, None);

    (project(&result), snapshot_side_effects(&stable))
}

// ── Corpus: the scalar core ───────────────────────────────

/// The full three-valued Kleene truth table for `and` / `or`, every cell
/// pinned to the hand-reasoned Kleene result. Each operand is forced to
/// true / false / null via a field so the program references no method
/// calls.
///
/// Kleene `and`: a single `false` forces `false` (even against `null`);
/// `null and true` / `null and null` stay `null`. Kleene `or`: a single
/// `true` forces `true` (even against `null`); `null or false` /
/// `null or null` stay `null`.
#[test]
fn kleene_truth_table() {
    let t = || Value::Bool(true);
    let f = || Value::Bool(false);
    let n = || Value::Null;
    // (left, right, and-result, or-result), one row per Kleene cell.
    let table: [(Value, Value, Value, Value); 9] = [
        (t(), t(), t(), t()),
        (t(), f(), f(), t()),
        (t(), n(), n(), t()),
        (f(), t(), f(), t()),
        (f(), f(), f(), f()),
        (f(), n(), f(), n()),
        (n(), t(), n(), t()),
        (n(), f(), f(), n()),
        (n(), n(), n(), n()),
    ];
    for (lval, rval, and_res, or_res) in table {
        for (op, expected) in [("and", &and_res), ("or", &or_res)] {
            let src = format!("emit out = a {op} b");
            let record = HashMap::from([
                ("a".to_string(), lval.clone()),
                ("b".to_string(), rval.clone()),
            ]);
            assert_eq!(
                eval_project(&src, &["a", "b"], record),
                ResultProjection::Emit {
                    fields: vec![("out".to_string(), expected.clone())],
                    record_vars: vec![],
                },
                "Kleene `{lval:?} {op} {rval:?}`",
            );
        }
    }
}

/// `and` / `or` short-circuit: the right operand is a division-by-zero
/// that errors if evaluated. `false and X` and `true or X` must NOT
/// evaluate `X`, yielding the short-circuited boolean with no error; the
/// non-short-circuiting state evaluates `X` and surfaces the error.
#[test]
fn kleene_short_circuit_side_effect() {
    let emit_bool = |b: bool| ResultProjection::Emit {
        fields: vec![("out".to_string(), Value::Bool(b))],
        record_vars: vec![],
    };
    // false and (1/0) → false, RHS unread, no error.
    assert_eq!(
        eval_project(
            "emit out = a and (1 / z)",
            &["a", "z"],
            HashMap::from([
                ("a".to_string(), Value::Bool(false)),
                ("z".to_string(), Value::Integer(0)),
            ]),
        ),
        emit_bool(false),
    );
    // true or (1/0) → true, RHS unread, no error.
    assert_eq!(
        eval_project(
            "emit out = a or (1 / z)",
            &["a", "z"],
            HashMap::from([
                ("a".to_string(), Value::Bool(true)),
                ("z".to_string(), Value::Integer(0)),
            ]),
        ),
        emit_bool(true),
    );
    // true and (1/0) → RHS evaluated → DivisionByZero surfaces.
    match eval_project(
        "emit out = a and (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Bool(true)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    ) {
        ResultProjection::Err { kind, .. } => assert_eq!(kind, "DivisionByZero"),
        other => panic!("expected DivisionByZero from the evaluated RHS, got {other:?}"),
    }
}

/// Arithmetic and comparison null-propagation: a null operand yields
/// null, except equality which never nulls.
#[test]
fn null_propagation() {
    let with_null = |op: &str| {
        eval_project(
            &format!("emit out = a {op} b"),
            &["a", "b"],
            HashMap::from([
                ("a".to_string(), Value::Null),
                ("b".to_string(), Value::Integer(3)),
            ]),
        )
    };
    for op in ["+", "-", "*", "/", "%", ">", "<", ">=", "<="] {
        let proj = with_null(op);
        assert_eq!(
            proj,
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Null)],
                record_vars: vec![],
            },
        );
    }
}

/// Eq / Neq never null: a null operand still produces a boolean.
#[test]
fn eq_neq_on_null() {
    let proj = eval_project(
        "emit out = a == b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(3)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );
    // null == null → true
    assert_eq!(
        eval_project(
            "emit out = a == b",
            &["a", "b"],
            HashMap::from([
                ("a".to_string(), Value::Null),
                ("b".to_string(), Value::Null),
            ]),
        ),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // null != 3 → true
    let proj = eval_project(
        "emit out = a != b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(3)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
}

/// Coalesce short-circuits on the first non-null operand and never
/// evaluates the RHS in that case.
#[test]
fn coalesce_short_circuit() {
    // a is non-null → coalesce yields a (7); RHS (1/0) unread, no error.
    assert_eq!(
        eval_project(
            "emit out = a ?? (1 / z)",
            &["a", "z"],
            HashMap::from([
                ("a".to_string(), Value::Integer(7)),
                ("z".to_string(), Value::Integer(0)),
            ]),
        ),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(7))],
            record_vars: vec![],
        },
    );
    // a is null → fall through to b.
    let proj = eval_project(
        "emit out = a ?? b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(9)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(9))],
            record_vars: vec![],
        },
    );
}

/// `if`/`then`/`else` — true branch, false branch, and a null condition
/// (which is not `true`, so it takes the else branch).
#[test]
fn if_then_else() {
    let src = "emit out = if a > 0 then \"pos\" else \"nonpos\"";
    let emit_str = |s: &str| ResultProjection::Emit {
        fields: vec![("out".to_string(), Value::String(s.into()))],
        record_vars: vec![],
    };
    // a = 5 → cond true → "pos".
    assert_eq!(
        eval_project(
            src,
            &["a"],
            HashMap::from([("a".to_string(), Value::Integer(5))])
        ),
        emit_str("pos"),
    );
    // a = -5 → cond false → "nonpos".
    assert_eq!(
        eval_project(
            src,
            &["a"],
            HashMap::from([("a".to_string(), Value::Integer(-5))]),
        ),
        emit_str("nonpos"),
    );
    // a = null → `null > 0` is null → not true → else branch → "nonpos".
    assert_eq!(
        eval_project(src, &["a"], HashMap::from([("a".to_string(), Value::Null)])),
        emit_str("nonpos"),
    );
}

/// Value-form and condition-form `match`, including wildcard fall-through.
#[test]
fn match_forms() {
    // Value form, status = "b" → the second arm → 2.
    assert_eq!(
        eval_project(
            "emit out = match status { \"a\" => 1, \"b\" => 2, _ => 0 }",
            &["status"],
            HashMap::from([("status".to_string(), Value::String("b".into()))]),
        ),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(2))],
            record_vars: vec![],
        },
    );
    // Value form, no non-wildcard arm matches → wildcard fall-through → 0.
    // (The typechecker requires the `_` catch-all, so an unmatched
    // scrutinee always lands on it rather than the implicit null.)
    assert_eq!(
        eval_project(
            "emit out = match status { \"a\" => 1, \"b\" => 2, _ => 0 }",
            &["status"],
            HashMap::from([("status".to_string(), Value::String("z".into()))]),
        ),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(0))],
            record_vars: vec![],
        },
    );
    // Condition form, a = 5: first guard `a > 10` is false, second
    // `a > 0` is true → "small".
    assert_eq!(
        eval_project(
            "emit out = match { a > 10 => \"big\", a > 0 => \"small\", _ => \"neg\" }",
            &["a"],
            HashMap::from([("a".to_string(), Value::Integer(5))]),
        ),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::String("small".into()))],
            record_vars: vec![],
        },
    );
}

/// Unary negation and logical not, including null-propagation.
#[test]
fn unary_ops() {
    let emit = |v: Value| ResultProjection::Emit {
        fields: vec![("out".to_string(), v)],
        record_vars: vec![],
    };
    // -4 → -4.
    assert_eq!(
        eval_project(
            "emit out = -a",
            &["a"],
            HashMap::from([("a".to_string(), Value::Integer(4))]),
        ),
        emit(Value::Integer(-4)),
    );
    // not true → false.
    assert_eq!(
        eval_project(
            "emit out = not a",
            &["a"],
            HashMap::from([("a".to_string(), Value::Bool(true))]),
        ),
        emit(Value::Bool(false)),
    );
    // not null → null (null propagates through logical not).
    assert_eq!(
        eval_project(
            "emit out = not a",
            &["a"],
            HashMap::from([("a".to_string(), Value::Null)]),
        ),
        emit(Value::Null),
    );
}

/// All four emit targets land in the right channel: Field → output map,
/// Record → record_vars, Pipeline / Source → context (neither surfaces
/// in the result); the surfaced shape is pinned.
#[test]
fn emit_targets() {
    // Field.
    let proj = eval_project("emit out = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(1))],
            record_vars: vec![],
        },
    );
    // Record.
    let proj = eval_project("emit $record.tag = 7", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![("tag".to_string(), Value::Integer(7))],
        },
    );
    // Pipeline / Source write through the context — neither appears in
    // the surfaced fields or record_vars, so each surfaces an empty emit.
    let proj = eval_project("emit $pipeline.seen = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
    let proj = eval_project("emit $source.seen = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
}

/// Convenience: the `$source.*` writes captured for the default test
/// source file, since every no-window `eval_project` run keys source
/// writes under the same `test.csv` placeholder.
fn source_writes_for_default_file(effects: &SideEffects) -> Pairs {
    effects
        .source
        .iter()
        .find(|(file, _)| file == "test.csv")
        .map(|(_, pairs)| pairs.clone())
        .unwrap_or_default()
}

/// `$pipeline.*` / `$source.*` emits write *values*, not just the empty
/// presence the `emit_targets` smoke check covers. The `EvalResult` is
/// blind to these writes — they live only on the context — so this drives
/// the evaluator and asserts the post-run pipeline/source maps carry the
/// exact computed values. Without the side-effect snapshot the test would
/// pass even if the compiled node routed a `$pipeline.*` write to the
/// wrong key, wrote the wrong value, or dropped the write entirely.
#[test]
fn pipeline_source_emit_values() {
    // A single computed pipeline write: `count = a * 2` over a = 21 → 42.
    let (proj, effects) = eval_project_capturing(
        "emit $pipeline.count = a * 2",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(21))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
    assert_eq!(
        effects.pipeline,
        vec![("count".to_string(), Value::Integer(42))],
    );
    assert!(effects.source.is_empty());

    // A single computed source write under the default source file.
    let (_, effects) = eval_project_capturing(
        "emit $source.tag = label.upper()",
        &["label"],
        HashMap::from([("label".to_string(), Value::String("hot".into()))]),
    );
    assert!(effects.pipeline.is_empty());
    assert_eq!(
        source_writes_for_default_file(&effects),
        vec![("tag".to_string(), Value::String("HOT".into()))],
    );

    // Multiple writes to both channels in one program; insertion order is
    // preserved on the pipeline channel, so the two `$pipeline.*` keys
    // appear in statement order.
    let (_, effects) = eval_project_capturing(
        "emit $pipeline.first = a\nemit $source.s = a + 1\nemit $pipeline.second = a + 100",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
    assert_eq!(
        effects.pipeline,
        vec![
            ("first".to_string(), Value::Integer(5)),
            ("second".to_string(), Value::Integer(105)),
        ],
    );
    assert_eq!(
        source_writes_for_default_file(&effects),
        vec![("s".to_string(), Value::Integer(6))],
    );

    // Last-write-wins on a repeated key: the second write to `dup` must be
    // the surviving value.
    let (_, effects) = eval_project_capturing(
        "emit $pipeline.dup = 1\nemit $pipeline.dup = 2",
        &[],
        HashMap::new(),
    );
    assert_eq!(
        effects.pipeline,
        vec![("dup".to_string(), Value::Integer(2))],
    );

    // A conditional write: the `if` selects which branch's value lands, so
    // the recorded pipeline value tracks the taken branch.
    let (_, effects) = eval_project_capturing(
        "emit $pipeline.bucket = if a > 0 then \"pos\" else \"nonpos\"",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(-3))]),
    );
    assert_eq!(
        effects.pipeline,
        vec![("bucket".to_string(), Value::String("nonpos".into()))],
    );
}

/// A `$pipeline.*` write *before* a later statement that errors must still
/// be observable on the context: the emit routes its value through the
/// context immediately, then the subsequent division-by-zero aborts the
/// record. The early write must have landed before the run errored —
/// proving the compiled node writes the side-effect channel at the point
/// in the statement sequence it reaches the emit, not all at once at the
/// end.
#[test]
fn pipeline_emit_before_error_is_observable() {
    let (proj, effects) = eval_project_capturing(
        "emit $pipeline.seen = a\nemit out = 1 / z",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Integer(9)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    match proj {
        ResultProjection::Err { kind, .. } => assert_eq!(kind, "DivisionByZero"),
        other => panic!("expected DivisionByZero, got {other:?}"),
    }
    // The pre-error write is present on the context.
    assert_eq!(
        effects.pipeline,
        vec![("seen".to_string(), Value::Integer(9))],
    );
}

/// `filter` short-circuits the record to `Skip(Filtered)` on a non-true
/// predicate (null included), and passes on `true`.
#[test]
fn filter_skip() {
    let proj = eval_project(
        "filter a > 10\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
    assert_eq!(proj, ResultProjection::Skip(SkipReason::Filtered));

    // Null predicate is not true → skip.
    let proj = eval_project(
        "filter a > b\nemit out = a",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(1)),
        ]),
    );
    assert_eq!(proj, ResultProjection::Skip(SkipReason::Filtered));

    // Passing predicate → emit the field (a = 5).
    assert_eq!(
        eval_project(
            "filter a > 0\nemit out = a",
            &["a"],
            HashMap::from([("a".to_string(), Value::Integer(5))]),
        ),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(5))],
            record_vars: vec![],
        },
    );
}

/// `let` writes the shared env and a later field-ref reads it; field
/// resolution prefers env over the input record.
#[test]
fn let_binding_and_env_shadow() {
    // Env value shadows the input field of the same name.
    let proj = eval_project(
        "let a = 100\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(1))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(100))],
            record_vars: vec![],
        },
    );
}

/// System-access leaves: `$source.*`, `$pipeline.*`, `now`. The test
/// context fixes `$source.row = 1`, `$pipeline.name = "test_pipeline"`,
/// and the clock to 2026-01-15 12:00:00, so each leaf resolves to a known
/// value.
#[test]
fn system_access() {
    let emit = |v: Value| ResultProjection::Emit {
        fields: vec![("out".to_string(), v)],
        record_vars: vec![],
    };
    assert_eq!(
        eval_project("emit out = $source.row", &[], HashMap::new()),
        emit(Value::Integer(1)),
    );
    assert_eq!(
        eval_project("emit out = $pipeline.name", &[], HashMap::new()),
        emit(Value::String("test_pipeline".into())),
    );
    let fixed_now = chrono::NaiveDate::from_ymd_opt(2026, 1, 15)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    assert_eq!(
        eval_project("emit out = now", &[], HashMap::new()),
        emit(Value::DateTime(fixed_now)),
    );
}

/// Index access on an array-valued field: in-bounds returns the element,
/// out-of-bounds and negative both resolve to `Null`.
#[test]
fn index_access() {
    let nums = || {
        Value::Array(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ])
    };
    let emit = |v: Value| ResultProjection::Emit {
        fields: vec![("out".to_string(), v)],
        record_vars: vec![],
    };
    // In bounds: nums[1] → 20.
    assert_eq!(
        eval_project(
            "emit out = nums[1]",
            &["nums"],
            HashMap::from([("nums".to_string(), nums())]),
        ),
        emit(Value::Integer(20)),
    );
    // Out of bounds → null.
    assert_eq!(
        eval_project(
            "emit out = nums[9]",
            &["nums"],
            HashMap::from([("nums".to_string(), nums())]),
        ),
        emit(Value::Null),
    );
    // Negative index → null.
    assert_eq!(
        eval_project(
            "emit out = nums[0 - 1]",
            &["nums"],
            HashMap::from([("nums".to_string(), nums())]),
        ),
        emit(Value::Null),
    );
}

/// Error path: division by zero must surface the same error kind, span,
/// boundary `source_row`, `source_expr`, and `triggering_field` from
/// the compiled evaluator.
#[test]
fn error_span_fidelity_division_by_zero() {
    let proj = eval_project(
        "emit out = 1 / z",
        &["z"],
        HashMap::from([("z".to_string(), Value::Integer(0))]),
    );
    match proj {
        ResultProjection::Err {
            kind,
            source_row,
            source_expr,
            triggering_field,
            ..
        } => {
            assert_eq!(kind, "DivisionByZero");
            assert_eq!(source_row, Some(1));
            assert!(source_expr.is_some());
            assert_eq!(triggering_field.as_deref(), Some("out"));
        }
        other => panic!("expected error projection, got {other:?}"),
    }
}

/// Error path: negating `i64::MIN` overflows and surfaces an
/// `IntegerOverflow` error.
#[test]
fn error_span_fidelity_overflow() {
    let proj = eval_project(
        "emit out = -a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(i64::MIN))]),
    );
    match proj {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("IntegerOverflow"),
            "expected IntegerOverflow, got {kind}"
        ),
        other => panic!("expected error projection, got {other:?}"),
    }
}

/// Multiple statements and emits accumulate in statement order; trace and
/// bare-expression statements run without altering the field channel.
/// With a = 4: `let x = a + 1` binds x = 5, so `emit p = x` → 5 and
/// `emit q = x * 2` → 10, in that order, and the trailing `trace` and bare
/// `x + 1` leave the field map untouched.
#[test]
fn multi_statement_ordering() {
    assert_eq!(
        eval_project(
            "let x = a + 1\nemit p = x\nemit q = x * 2\ntrace \"hi\"\nx + 1",
            &["a"],
            HashMap::from([("a".to_string(), Value::Integer(4))]),
        ),
        ResultProjection::Emit {
            fields: vec![
                ("p".to_string(), Value::Integer(5)),
                ("q".to_string(), Value::Integer(10)),
            ],
            record_vars: vec![],
        },
    );
}

// ── Corpus: method calls ──────────────────────────────────

/// String builtins delegate to the shared `dispatch_method`; each case
/// pins the hand-reasoned result of the method on its input string.
#[test]
fn string_methods() {
    let case = |src: &str, s: &str, expected: Value| {
        assert_eq!(
            eval_project(
                src,
                &["s"],
                HashMap::from([("s".to_string(), Value::String(s.into()))]),
            ),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "string method `{src}` on {s:?}",
        );
    };
    case("emit out = s.upper()", "abc", Value::String("ABC".into()));
    case("emit out = s.lower()", "ABC", Value::String("abc".into()));
    case("emit out = s.trim()", "  hi  ", Value::String("hi".into()));
    case("emit out = s.length()", "hello", Value::Integer(5));
    case("emit out = s.contains(\"ell\")", "hello", Value::Bool(true));
    case(
        "emit out = s.starts_with(\"he\")",
        "hello",
        Value::Bool(true),
    );
    case(
        "emit out = s.replace(\"l\", \"L\")",
        "hello",
        Value::String("heLLo".into()),
    );
    // substring(1, 3): skip 1 char, take 3 → "ell".
    case(
        "emit out = s.substring(1, 3)",
        "hello",
        Value::String("ell".into()),
    );
    // Chained: trim "  hi  " → "hi", then upper → "HI".
    case(
        "emit out = s.trim().upper()",
        "  hi  ",
        Value::String("HI".into()),
    );
}

/// Numeric builtins delegate to the shared `dispatch_method`; each case
/// pins the hand-reasoned result. `ceil`/`floor` round to an `Integer`;
/// `round` keeps a `Float`; `min`/`max` return whichever operand wins by
/// value (the receiver on a tie-or-loss respectively).
#[test]
fn numeric_methods() {
    let case = |src: &str, n: Value, expected: Value| {
        assert_eq!(
            eval_project(src, &["n"], HashMap::from([("n".to_string(), n.clone())])),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "numeric method `{src}` on {n:?}",
        );
    };
    case("emit out = n.abs()", Value::Integer(-7), Value::Integer(7));
    case("emit out = n.abs()", Value::Float(-2.5), Value::Float(2.5));
    // ceil/floor return an Integer.
    case("emit out = n.ceil()", Value::Float(2.1), Value::Integer(3));
    case("emit out = n.floor()", Value::Float(2.9), Value::Integer(2));
    // round half away from zero, result stays a Float.
    case("emit out = n.round()", Value::Float(2.5), Value::Float(3.0));
    case(
        "emit out = n.to_float()",
        Value::Integer(3),
        Value::Float(3.0),
    );
    case(
        "emit out = n.to_string()",
        Value::Integer(42),
        Value::String("42".into()),
    );
    // 3.min(10) → 3 (the smaller); 3.max(10) → 10 (the larger).
    case("emit out = n.min(10)", Value::Integer(3), Value::Integer(3));
    case(
        "emit out = n.max(10)",
        Value::Integer(3),
        Value::Integer(10),
    );
}

/// The null-receiver gate: for every method *except* the four exceptions
/// a null receiver short-circuits to `Null`. The gate lives inside the
/// shared `dispatch_method`, so the compiled node delegates to it rather
/// than re-deriving the rule.
#[test]
fn method_null_receiver_gate() {
    let null_recv =
        |src: &str| eval_project(src, &["s"], HashMap::from([("s".to_string(), Value::Null)]));
    // Gated methods → Null.
    for src in [
        "emit out = s.upper()",
        "emit out = s.length()",
        "emit out = s.trim()",
        "emit out = s.contains(\"x\")",
    ] {
        let proj = null_recv(src);
        assert_eq!(
            proj,
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Null)],
                record_vars: vec![],
            },
            "null receiver should gate `{src}` to Null",
        );
    }
}

/// The four gate exceptions (`is_null` / `type_of` / `is_empty` /
/// `catch`) run on a null receiver instead of short-circuiting, each
/// producing a concrete (non-null) result.
#[test]
fn method_null_receiver_exceptions() {
    let null_recv = |src: &str, expected: Value| {
        assert_eq!(
            eval_project(src, &["s"], HashMap::from([("s".to_string(), Value::Null)])),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "null-receiver exception `{src}`",
        );
    };
    // is_null → true on a null receiver.
    null_recv("emit out = s.is_null()", Value::Bool(true));
    // catch(default) → the default, since the receiver is null.
    null_recv("emit out = s.catch(99)", Value::Integer(99));
    // type_of names the null type; is_empty treats null as empty.
    null_recv("emit out = s.type_of()", Value::String("null".into()));
    null_recv("emit out = s.is_empty()", Value::Bool(true));
}

/// Regex methods use the pre-compiled regex captured by value at
/// lowering. `matches` is a full (anchored) match → bool; `find` is a
/// substring search → bool; `capture` with no group index returns group 0
/// (the whole match) as a string. Each case pins the hand-reasoned
/// result.
#[test]
fn regex_methods() {
    let case = |src: &str, s: &str, expected: Value| {
        assert_eq!(
            eval_project(
                src,
                &["s"],
                HashMap::from([("s".to_string(), Value::String(s.into()))]),
            ),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "regex method `{src}` on {s:?}",
        );
    };
    // `^[a-z]+$` fully matches "abc" but not "abc123".
    case(
        "emit out = s.matches(\"^[a-z]+$\")",
        "abc",
        Value::Bool(true),
    );
    case(
        "emit out = s.matches(\"^[a-z]+$\")",
        "abc123",
        Value::Bool(false),
    );
    // `[0-9]+` is present in "abc123", absent in "abc".
    case("emit out = s.find(\"[0-9]+\")", "abc123", Value::Bool(true));
    case("emit out = s.find(\"[0-9]+\")", "abc", Value::Bool(false));
    // capture with no group index → group 0 → the whole match.
    case(
        "emit out = s.capture(\"([a-z]+)([0-9]+)\")",
        "abc123",
        Value::String("abc123".into()),
    );
}

// ── Corpus: closure-bearing array builtins ────────────────

/// Build an array `Value`, the receiver shape the closure builtins
/// require.
fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

/// Each closure builtin (`filter` / `map` / `find` / `any` / `flat_map`)
/// runs the host loop over a separately-compiled closure body and must
/// produce the expected value on a populated array.
///
/// CXL has no array-literal syntax, so `flat_map`'s array-producing body
/// uses a string element split into an array (`it.split(",")`); the
/// other four use an integer array.
#[test]
fn closure_builtins_populated() {
    let nums = || {
        arr(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])
    };
    let with_nums = |src: &str, expected: Value| {
        assert_eq!(
            eval_project(
                src,
                &["nums"],
                HashMap::from([("nums".to_string(), nums())]),
            ),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "closure builtin `{src}` over [1, 2, 3]",
        );
    };
    let i = Value::Integer;
    // filter keeps elements where the body is true → [2, 3].
    with_nums(
        "emit out = nums.filter(it => it > 1)",
        arr(vec![i(2), i(3)]),
    );
    // map applies the body to each element → [11, 12, 13].
    with_nums(
        "emit out = nums.map(it => it + 10)",
        arr(vec![i(11), i(12), i(13)]),
    );
    // find returns the first element whose body is true → 2.
    with_nums("emit out = nums.find(it => it > 1)", i(2));
    // any is false: no element is > 5.
    with_nums("emit out = nums.any(it => it > 5)", Value::Bool(false));
    // find with no match falls through to Null.
    with_nums("emit out = nums.find(it => it > 99)", Value::Null);
    // any with no match is false.
    with_nums("emit out = nums.any(it => it > 99)", Value::Bool(false));

    // flat_map flattens each element's array-valued body result:
    // "a,b".split(",") → ["a","b"], "c".split(",") → ["c"], flattened.
    assert_eq!(
        eval_project(
            "emit out = strs.flat_map(it => it.split(\",\"))",
            &["strs"],
            HashMap::from([(
                "strs".to_string(),
                arr(vec![Value::String("a,b".into()), Value::String("c".into())]),
            )]),
        ),
        ResultProjection::Emit {
            fields: vec![(
                "out".to_string(),
                arr(vec![
                    Value::String("a".into()),
                    Value::String("b".into()),
                    Value::String("c".into()),
                ]),
            )],
            record_vars: vec![],
        },
    );
}

/// A null receiver short-circuits every closure builtin to `Null`; the
/// body never runs (the null-propagation policy).
#[test]
fn closure_builtins_null_receiver() {
    let null_recv_nums = |src: &str| {
        let proj = eval_project(
            src,
            &["nums"],
            HashMap::from([("nums".to_string(), Value::Null)]),
        );
        assert_eq!(
            proj,
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Null)],
                record_vars: vec![],
            },
            "null receiver should gate `{src}` to Null",
        );
    };
    null_recv_nums("emit out = nums.filter(it => it > 1)");
    null_recv_nums("emit out = nums.map(it => it + 10)");
    null_recv_nums("emit out = nums.find(it => it > 1)");
    null_recv_nums("emit out = nums.any(it => it > 1)");

    let proj = eval_project(
        "emit out = strs.flat_map(it => it.split(\",\"))",
        &["strs"],
        HashMap::from([("strs".to_string(), Value::Null)]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// An empty array runs no iterations: `filter` / `map` / `flat_map`
/// return an empty array, `find` returns `Null`, `any` returns `false`.
#[test]
fn closure_builtins_empty_array() {
    let with_empty = |src: &str| {
        eval_project(
            src,
            &["nums"],
            HashMap::from([("nums".to_string(), arr(vec![]))]),
        )
    };
    let proj = with_empty("emit out = nums.filter(it => it > 1)");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), arr(vec![]))],
            record_vars: vec![],
        },
    );
    // map over an empty array → empty array.
    assert_eq!(
        with_empty("emit out = nums.map(it => it + 1)"),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), arr(vec![]))],
            record_vars: vec![],
        },
    );
    let proj = with_empty("emit out = nums.find(it => it > 1)");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
    let proj = with_empty("emit out = nums.any(it => it > 1)");
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );

    // flat_map on an empty string array → empty array.
    let proj = eval_project(
        "emit out = strs.flat_map(it => it.split(\",\"))",
        &["strs"],
        HashMap::from([("strs".to_string(), arr(vec![]))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), arr(vec![]))],
            record_vars: vec![],
        },
    );
}

/// A closure body that errors on one element propagates that error
/// (kind + boundary provenance), and the per-element binding is removed
/// on the error path so the env never leaks. The `find`-style early break
/// is not reached because the error fires first.
#[test]
fn closure_body_error_propagates() {
    let proj = eval_project(
        "emit out = nums.map(it => 1 / it)",
        &["nums"],
        HashMap::from([(
            "nums".to_string(),
            arr(vec![Value::Integer(1), Value::Integer(0)]),
        )]),
    );
    match proj {
        ResultProjection::Err {
            kind,
            triggering_field,
            ..
        } => {
            assert_eq!(kind, "DivisionByZero");
            assert_eq!(triggering_field.as_deref(), Some("out"));
        }
        other => panic!("expected error projection, got {other:?}"),
    }
}

// ── Corpus: hardening — mixed numerics, ordering, Kleene ───

/// Mixed int/float comparison widens the int to float before comparing;
/// the widened ordering is pinned.
#[test]
fn mixed_int_float_comparison() {
    // Integer(2) > Float(1.5) → true.
    let proj = eval_project(
        "emit out = a > b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Integer(2)),
            ("b".to_string(), Value::Float(1.5)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // Float(1.5) < Integer(2) → true.
    let proj = eval_project(
        "emit out = a < b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Float(1.5)),
            ("b".to_string(), Value::Integer(2)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // Integer(2) == Float(2.0) → true (cross-type equality widens).
    let proj = eval_project(
        "emit out = a == b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Integer(2)),
            ("b".to_string(), Value::Float(2.0)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
}

/// String ordering compares lexicographically.
#[test]
fn string_ordering() {
    let proj = eval_project(
        "emit out = a < b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::String("apple".into())),
            ("b".to_string(), Value::String("banana".into())),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // "zoo" >= "zoo" → equal strings → true.
    assert_eq!(
        eval_project(
            "emit out = a >= b",
            &["a", "b"],
            HashMap::from([
                ("a".to_string(), Value::String("zoo".into())),
                ("b".to_string(), Value::String("zoo".into())),
            ]),
        ),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
}

/// The four `null or <bool>` Kleene cells, pinned deterministically:
/// `null or true → true`, `null or false → null`, and the symmetric
/// `null and false → false`, `null and true → null`.
#[test]
fn null_or_kleene_cells() {
    let cell = |op: &str, b: Value| {
        eval_project(
            &format!("emit out = a {op} b"),
            &["a", "b"],
            HashMap::from([("a".to_string(), Value::Null), ("b".to_string(), b)]),
        )
    };
    // null or true → true.
    assert_eq!(
        cell("or", Value::Bool(true)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
    // null or false → null.
    assert_eq!(
        cell("or", Value::Bool(false)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
    // null and false → false.
    assert_eq!(
        cell("and", Value::Bool(false)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );
    // null and true → null.
    assert_eq!(
        cell("and", Value::Bool(true)),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// A NaN *result* projects to a canonicalized NaN float, so it compares
/// equal to an expected NaN rather than false-failing on differing payload
/// bits. The arithmetic must actually reach the float op: `0.0 / 0.0`
/// cannot — the division arm rejects a zero divisor as `DivisionByZero`
/// before any float divide runs — so this feeds a pre-existing `f64::NAN`
/// field through `+`, `*`, and a non-zero divisor, each producing a NaN
/// `Value::Float` (not an error), confirming the float op ran.
#[test]
fn nan_result_is_canonicalized() {
    let with_nan = |src: &str| {
        assert_eq!(
            eval_project(
                src,
                &["a", "b"],
                HashMap::from([
                    ("a".to_string(), Value::Float(f64::NAN)),
                    ("b".to_string(), Value::Float(2.0)),
                ]),
            ),
            // Both the projected result and this expected value pass
            // through `canonicalize_nan`, so the equality holds on a NaN.
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Float(f64::NAN))],
                record_vars: vec![],
            },
            "NaN result for `{src}`",
        );
    };
    // NaN propagates through addition and multiplication.
    with_nan("emit out = a + b");
    with_nan("emit out = a * b");
    // A NaN dividend with a non-zero divisor reaches the float divide
    // (the divisor guard only rejects zero), yielding NaN rather than an
    // error.
    with_nan("emit out = a / b");
}

/// The `Any`-receiver method shapes the proptest generator emits
/// (`to_string` / `is_null` / `type_of` / `catch`) typecheck on the
/// generated leaf set and evaluate to their hand-reasoned values over a
/// concrete record (`a = 5`, `b = "x"`) — pinned here so the property
/// test's method coverage cannot silently collapse to all-rejected
/// without this deterministic case also failing.
#[test]
fn generated_method_shapes() {
    let record = || {
        HashMap::from([
            ("a".to_string(), Value::Integer(5)),
            ("b".to_string(), Value::String("x".into())),
        ])
    };
    let case = |src: &str, expected: Value| {
        assert_eq!(
            eval_project(src, &["a", "b"], record()),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "`{src}` over a = 5, b = \"x\"",
        );
    };
    // 5.to_string() → "5".
    case("emit out = (a).to_string()", Value::String("5".into()));
    // 5 is not null.
    case("emit out = (a).is_null()", Value::Bool(false));
    // type of an integer.
    case("emit out = (a).type_of()", Value::String("int".into()));
    // (5 + 1).to_string() → "6".
    case(
        "emit out = ((a + 1)).to_string()",
        Value::String("6".into()),
    );
    // catch on a non-null receiver yields the receiver (5), not b.
    case("emit out = (a).catch(b)", Value::Integer(5));
    // false or (5 > 0) → false or true → true.
    case("emit out = ((a).is_null() or (a > 0))", Value::Bool(true));
}

// ── Carried-over hardening: null-receiver regex, error-surfacing catch ──

/// A regex method on a null receiver is gated to `Null` by the shared
/// `dispatch_method` — the regex is never consulted, rather than
/// attempting a match against a null receiver.
#[test]
fn regex_method_null_receiver() {
    let proj = eval_project(
        "emit out = s.matches(\"^[a-z]+$\")",
        &["s"],
        HashMap::from([("s".to_string(), Value::Null)]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// `catch(default)` is a null-coalesce, not an error swallow: its receiver
/// is evaluated eagerly *before* `dispatch_method` runs, so an error in
/// the receiver expression propagates and `catch` never substitutes its
/// default. The receiver here is non-null-shaped but raises a real error
/// (division by zero); the run must surface that error
/// rather than returning the `catch` default.
#[test]
fn catch_does_not_swallow_receiver_error() {
    let proj = eval_project(
        "emit out = (1 / z).catch(99)",
        &["z"],
        HashMap::from([("z".to_string(), Value::Integer(0))]),
    );
    match proj {
        ResultProjection::Err { kind, .. } => assert_eq!(kind, "DivisionByZero"),
        other => panic!("expected DivisionByZero to propagate through catch, got {other:?}"),
    }
}

// ── Corpus: window leaves ─────────────────────────────────

/// In-memory partition storage for the window corpus: a list of records,
/// each a field map. Indexed by record position; the window context reads
/// fields off it by index, exactly as the arena-backed production storage
/// does.
struct VecStorage {
    rows: Vec<indexmap::IndexMap<String, Value>>,
}

impl RecordStorage for VecStorage {
    fn resolve_field(&self, index: u64, name: &str) -> Option<&Value> {
        self.rows.get(index as usize).and_then(|r| r.get(name))
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, index: u64) -> Vec<&str> {
        self.rows
            .get(index as usize)
            .map(|r| r.keys().map(String::as_str).collect())
            .unwrap_or_default()
    }
    fn record_count(&self) -> u64 {
        self.rows.len() as u64
    }
}

/// Window over the whole `VecStorage`, with an explicit current-row index
/// so `lag`/`lead` resolve relative to a known position. Aggregate methods
/// compute real sums/averages over the numeric fields of the partition so
/// the test exercises the trait surface with non-trivial values; the
/// expected window results in the tests are hand-computed from these
/// definitions.
struct VecWindow<'a> {
    storage: &'a VecStorage,
    current: usize,
}

impl<'a> VecWindow<'a> {
    fn numeric(&self, field: &str) -> Vec<f64> {
        self.storage
            .rows
            .iter()
            .filter_map(|r| match r.get(field) {
                Some(Value::Integer(n)) => Some(*n as f64),
                Some(Value::Float(f)) => Some(*f),
                _ => None,
            })
            .collect()
    }
}

impl<'a> clinker_record::WindowContext<'a, VecStorage> for VecWindow<'a> {
    fn first(&self) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        (!self.storage.rows.is_empty()).then(|| clinker_record::RecordView::new(self.storage, 0))
    }
    fn last(&self) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        self.storage
            .rows
            .len()
            .checked_sub(1)
            .map(|i| clinker_record::RecordView::new(self.storage, i as u64))
    }
    fn lag(&self, offset: usize) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        self.current
            .checked_sub(offset)
            .map(|i| clinker_record::RecordView::new(self.storage, i as u64))
    }
    fn lead(&self, offset: usize) -> Option<clinker_record::RecordView<'a, VecStorage>> {
        let i = self.current + offset;
        (i < self.storage.rows.len())
            .then(|| clinker_record::RecordView::new(self.storage, i as u64))
    }
    fn count(&self) -> i64 {
        self.storage.rows.len() as i64
    }
    fn sum(&self, field: &str) -> Value {
        let v = self.numeric(field);
        if v.is_empty() {
            Value::Null
        } else {
            Value::Float(v.iter().sum())
        }
    }
    fn cumulative_sum(&self, field: &str) -> Value {
        let s: f64 = self.numeric(field).iter().take(self.current + 1).sum();
        Value::Float(s)
    }
    fn avg(&self, field: &str) -> Value {
        let v = self.numeric(field);
        if v.is_empty() {
            Value::Null
        } else {
            Value::Float(v.iter().sum::<f64>() / v.len() as f64)
        }
    }
    fn min(&self, field: &str) -> Value {
        self.numeric(field)
            .into_iter()
            .reduce(f64::min)
            .map_or(Value::Null, Value::Float)
    }
    fn max(&self, field: &str) -> Value {
        self.numeric(field)
            .into_iter()
            .reduce(f64::max)
            .map_or(Value::Null, Value::Float)
    }
    fn partition_len(&self) -> usize {
        self.storage.rows.len()
    }
    fn partition_record(&self, index: usize) -> clinker_record::RecordView<'a, VecStorage> {
        clinker_record::RecordView::new(self.storage, index as u64)
    }
    fn collect(&self, field: &str) -> Value {
        Value::Array(
            self.storage
                .rows
                .iter()
                .filter_map(|r| r.get(field).cloned())
                .collect(),
        )
    }
    fn distinct(&self, field: &str) -> Value {
        let mut seen = Vec::new();
        for r in &self.storage.rows {
            if let Some(v) = r.get(field)
                && !seen.contains(v)
            {
                seen.push(v.clone());
            }
        }
        Value::Array(seen)
    }
    fn row_number(&self) -> i64 {
        (self.current + 1) as i64
    }
    fn rank(&self) -> i64 {
        1
    }
    fn dense_rank(&self) -> i64 {
        1
    }
    fn first_value(&self, field: &str) -> Value {
        self.storage
            .rows
            .first()
            .and_then(|r| r.get(field).cloned())
            .unwrap_or(Value::Null)
    }
    fn last_value(&self, field: &str) -> Value {
        self.storage
            .rows
            .last()
            .and_then(|r| r.get(field).cloned())
            .unwrap_or(Value::Null)
    }
}

/// Type-check a window program against an explicit field-type row (window
/// programs reference fields whose types the typecheck pass needs to see).
fn type_program_with_row(
    src: &str,
    fields: &[(&str, crate::typecheck::types::Type)],
) -> TypedProgram {
    use crate::typecheck::row::QualifiedField;
    let parsed = Parser::parse(src);
    assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
    let names: Vec<&str> = fields.iter().map(|(n, _)| *n).collect();
    let resolved = resolve_program(parsed.ast, &names, parsed.node_count)
        .unwrap_or_else(|d| panic!("resolve: {:?}", d));
    let declared: indexmap::IndexMap<QualifiedField, crate::typecheck::types::Type> = fields
        .iter()
        .map(|(n, t)| (QualifiedField::bare(*n), t.clone()))
        .collect();
    let row = Row::closed(declared, crate::lexer::Span::new(0, 0));
    type_check(resolved, &row)
        .unwrap_or_else(|d| panic!("type: {:?}", d))
        .with_source(Arc::from(src))
}

/// Evaluate a window program through the compiled evaluator with a
/// `VecWindow` over `partition` at the given current-row index, returning
/// the projected result. The transform's own input record is empty (the
/// scalar core reads nothing here); every read flows through the window.
///
/// Running through the live `ProgramEvaluator::eval_record` with a real
/// `WindowContext` and a non-`NullStorage` `S` exercises the per-`S`
/// program cache and window-leaf monomorphization the production entry
/// uses.
fn eval_window_project(
    src: &str,
    fields: &[(&str, crate::typecheck::types::Type)],
    partition: Vec<indexmap::IndexMap<String, Value>>,
    current: usize,
) -> ResultProjection {
    let typed = type_program_with_row(src, fields);
    let has_distinct = program_has_distinct(&typed);
    let stable = StableEvalContext::test_default();
    let resolver = HashMapResolver::new(HashMap::new());
    let storage = VecStorage { rows: partition };
    let window = VecWindow {
        storage: &storage,
        current,
    };

    let ctx = EvalContext::test_default_borrowed(&stable);
    let mut eval = ProgramEvaluator::new(Arc::new(typed), has_distinct);
    let result = eval.eval_record::<VecStorage>(&ctx, &resolver, Some(&window));

    project(&result)
}

/// One partition row from `(field, value)` pairs.
fn row(pairs: Vec<(&str, Value)>) -> indexmap::IndexMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// Bare aggregate / cardinality / rank window builtins lower to leaf trait
/// calls that dispatch to the `VecWindow` methods. Each case pins the
/// hand-computed result over the partition `[10, 20, 30]` at current = 1.
/// `count`/`row_number`/`rank`/`dense_rank` yield integers; the numeric
/// aggregates yield floats (the `VecWindow` aggregates over `f64`);
/// `collect`/`first_value`/`last_value` carry the field values through
/// unchanged (still integers).
#[test]
fn window_bare_calls() {
    use crate::typecheck::types::Type;
    let partition = || {
        vec![
            row(vec![("amount", Value::Integer(10))]),
            row(vec![("amount", Value::Integer(20))]),
            row(vec![("amount", Value::Integer(30))]),
        ]
    };
    let fields: &[(&str, Type)] = &[("amount", Type::Int)];
    let case = |src: &str, expected: Value| {
        assert_eq!(
            eval_window_project(src, fields, partition(), 1),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "window builtin `{src}` over [10, 20, 30] at current = 1",
        );
    };
    case("emit out = $window.count()", Value::Integer(3));
    case("emit out = $window.sum(amount)", Value::Float(60.0));
    case("emit out = $window.avg(amount)", Value::Float(20.0));
    case("emit out = $window.min(amount)", Value::Float(10.0));
    case("emit out = $window.max(amount)", Value::Float(30.0));
    // cumulative_sum at current = 1 sums the first two rows → 10 + 20 = 30.
    case(
        "emit out = $window.cumulative_sum(amount)",
        Value::Float(30.0),
    );
    // row_number is 1-based off current = 1 → 2.
    case("emit out = $window.row_number()", Value::Integer(2));
    case("emit out = $window.rank()", Value::Integer(1));
    case("emit out = $window.dense_rank()", Value::Integer(1));
    case(
        "emit out = $window.collect(amount)",
        Value::Array(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ]),
    );
    case("emit out = $window.first_value(amount)", Value::Integer(10));
    case("emit out = $window.last_value(amount)", Value::Integer(30));
}

/// The `$window.<fn>().<field>` chain resolves a field off a positional
/// window row with no intermediate `Value`; the read goes through the same
/// `RecordView`. Covers `first`/`last`/`lag`/`lead`, including an
/// out-of-range offset that resolves to `Null`. Each case pins the
/// hand-computed `price` read off the partition `[100, 200, 300]`.
#[test]
fn window_chain_field() {
    use crate::typecheck::types::Type;
    let partition = || {
        vec![
            row(vec![("price", Value::Integer(100))]),
            row(vec![("price", Value::Integer(200))]),
            row(vec![("price", Value::Integer(300))]),
        ]
    };
    let fields: &[(&str, Type)] = &[("price", Type::Int)];
    let case = |src: &str, current: usize, expected: Value| {
        assert_eq!(
            eval_window_project(src, fields, partition(), current),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "window chain `{src}` at current = {current}",
        );
    };
    // first row → 100, last row → 300.
    case("emit out = $window.first().price", 1, Value::Integer(100));
    case("emit out = $window.last().price", 1, Value::Integer(300));
    // lag(1) from current = 2 → row 1 → 200.
    case("emit out = $window.lag(1).price", 2, Value::Integer(200));
    // lead(1) from current = 0 → row 1 → 200.
    case("emit out = $window.lead(1).price", 0, Value::Integer(200));
    // lag past the start → Null.
    case("emit out = $window.lag(5).price", 0, Value::Null);
    // lead past the end → Null.
    case("emit out = $window.lead(5).price", 2, Value::Null);
}

/// Window reads against an empty partition: aggregates → `Null`, count
/// → 0, the chain forms → `Null` (no positional row).
#[test]
fn window_empty_partition() {
    use crate::typecheck::types::Type;
    let fields: &[(&str, Type)] = &[("amount", Type::Int)];
    let proj = eval_window_project("emit out = $window.count()", fields, vec![], 0);
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(0))],
            record_vars: vec![],
        },
    );
    let proj = eval_window_project("emit out = $window.sum(amount)", fields, vec![], 0);
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
    let proj = eval_window_project("emit out = $window.first().amount", fields, vec![], 0);
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// A window read with no window context available surfaces the same typed
/// error (rather than a silent `Null`). Driven
/// through the no-window `eval_project` path, which threads `None`.
#[test]
fn window_missing_context_errors() {
    let proj = eval_project("emit out = $window.count()", &[], HashMap::new());
    match proj {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("TypeMismatch"),
            "expected a window-context TypeMismatch, got {kind}"
        ),
        other => panic!("expected window-context error, got {other:?}"),
    }
}

/// The `$window` predicate folds (`any`/`every`/`exists`/`not_exists`)
/// run a compiled predicate over every partition record with the same
/// three-valued Kleene collapse — including the
/// short-circuit, the null-taint, and the iterated-operator identity on
/// an empty fold.
#[test]
fn window_predicate_folds() {
    use crate::typecheck::types::Type;
    let fields: &[(&str, Type)] = &[("flag", Type::Bool)];
    let part = |flags: Vec<Value>| flags.into_iter().map(|f| row(vec![("flag", f)])).collect();
    let b = Value::Bool;
    let nullv = Value::Null;
    let case = |src: &str, flags: Vec<Value>, expected: Value| {
        assert_eq!(
            eval_window_project(src, fields, part(flags.clone()), 0),
            ResultProjection::Emit {
                fields: vec![("out".to_string(), expected)],
                record_vars: vec![],
            },
            "window fold `{src}` over {flags:?}",
        );
    };

    // any short-circuits true at the first true row, before the later null
    // can taint → true.
    case(
        "emit out = $window.any(flag)",
        vec![b(true), nullv.clone(), b(false)],
        Value::Bool(true),
    );
    // any with a null and no true → null taint.
    case(
        "emit out = $window.any(flag)",
        vec![b(false), nullv.clone(), b(false)],
        Value::Null,
    );
    // any over all-false → false (iterated-or identity).
    case(
        "emit out = $window.any(flag)",
        vec![b(false), b(false)],
        Value::Bool(false),
    );
    // every short-circuits false at the first false row → false.
    case(
        "emit out = $window.every(flag)",
        vec![b(true), b(false), b(true)],
        Value::Bool(false),
    );
    // every over all-true → true (iterated-and identity).
    case(
        "emit out = $window.every(flag)",
        vec![b(true), b(true)],
        Value::Bool(true),
    );
    // exists is an alias of any: a true row is present → true.
    case(
        "emit out = $window.exists(flag)",
        vec![b(false), b(true)],
        Value::Bool(true),
    );
    // not_exists inverts each row then folds as every: no flag is true, so
    // every inverted row is true → true.
    case(
        "emit out = $window.not_exists(flag)",
        vec![b(false), b(false)],
        Value::Bool(true),
    );
    // Empty partition → iterated-operator identity, no taint: any → false,
    // every → true.
    assert_eq!(
        eval_window_project("emit out = $window.any(flag)", fields, vec![], 0),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );
    assert_eq!(
        eval_window_project("emit out = $window.every(flag)", fields, vec![], 0),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
}

// ── Corpus: distinct ──────────────────────────────────────

/// One record in a distinct stream: an optional partition key to set
/// before evaluating, paired with the record's field map.
type DistinctStreamRecord = (
    Option<Vec<clinker_record::GroupByKey>>,
    HashMap<String, Value>,
);

/// Drive a stream of records through one compiled evaluator with shared
/// dedup state, optionally setting a partition key before each record.
/// Returns the per-record projections so callers can assert the concrete
/// skip/emit sequence.
///
/// Driving through `ProgramEvaluator` (not the raw program) exercises the
/// production state-threading: `set_partition` reaches the shared
/// `EvalState`, and per-record dedup runs against it, so distinct and
/// partition-reset behave as production does.
fn eval_distinct_stream(
    src: &str,
    fields: &[&str],
    records: Vec<DistinctStreamRecord>,
) -> Vec<ResultProjection> {
    let typed = type_program(src, fields);
    let has_distinct = program_has_distinct(&typed);
    assert!(
        has_distinct,
        "distinct stream helper requires a distinct program"
    );
    let stable = StableEvalContext::test_default();

    let mut eval = ProgramEvaluator::new(Arc::new(typed), has_distinct);

    let mut out = Vec::new();
    for (partition_key, record) in records {
        if let Some(key) = &partition_key {
            eval.set_partition(key);
        }
        let resolver = HashMapResolver::new(record);
        let ctx = EvalContext::test_default_borrowed(&stable);
        let result = eval.eval_record::<NullStorage>(&ctx, &resolver, None);
        out.push(project(&result));
    }
    out
}

/// `distinct by <field>`: the first occurrence of a key emits its value,
/// later duplicates skip with `Skip(Duplicate)`, across the stream against
/// the shared dedup set.
#[test]
fn distinct_duplicate_skip() {
    let stream = vec![
        (None, HashMap::from([("k".to_string(), Value::Integer(1))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(2))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(1))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(2))])),
        (None, HashMap::from([("k".to_string(), Value::Integer(3))])),
    ];
    let projs = eval_distinct_stream("distinct by k\nemit out = k", &["k"], stream);
    let emit_k = |k: i64| ResultProjection::Emit {
        fields: vec![("out".to_string(), Value::Integer(k))],
        record_vars: vec![],
    };
    // First 1, first 2 emit their key; repeat 1, repeat 2 skip as
    // duplicates; first 3 emits.
    assert_eq!(
        projs,
        vec![
            emit_k(1),
            emit_k(2),
            ResultProjection::Skip(SkipReason::Duplicate),
            ResultProjection::Skip(SkipReason::Duplicate),
            emit_k(3),
        ],
    );
}

/// A partition-key change clears the dedup set (window-scoped distinct):
/// a key seen in the previous partition is *not* a duplicate in the next.
#[test]
fn distinct_partition_reset() {
    let p0 = Some(vec![clinker_record::GroupByKey::Int(0)]);
    let p1 = Some(vec![clinker_record::GroupByKey::Int(1)]);
    let stream = vec![
        (
            p0.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
        (
            p0.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
        // Partition boundary: the dedup set clears, so 7 is fresh again.
        (
            p1.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
        (
            p1.clone(),
            HashMap::from([("k".to_string(), Value::Integer(7))]),
        ),
    ];
    let projs = eval_distinct_stream("distinct by k\nemit out = k", &["k"], stream);
    let emit_7 = || ResultProjection::Emit {
        fields: vec![("out".to_string(), Value::Integer(7))],
        record_vars: vec![],
    };
    let dup = || ResultProjection::Skip(SkipReason::Duplicate);
    // Within each partition the second 7 is a duplicate; the partition
    // boundary resets, so the first 7 of partition 1 emits again.
    assert_eq!(projs, vec![emit_7(), dup(), emit_7(), dup()]);
}

/// A NaN distinct key surfaces a `GroupKeyError`-derived error from the
/// run (NaN is not hashable as a group key), rather than silently
/// emitting or skipping.
#[test]
fn distinct_nan_key_errors() {
    let stream = vec![(
        None,
        HashMap::from([("k".to_string(), Value::Float(f64::NAN))]),
    )];
    let projs = eval_distinct_stream("distinct by k\nemit out = k", &["k"], stream);
    match &projs[0] {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("TypeMismatch"),
            "expected NaN group-key error, got {kind}"
        ),
        other => panic!("expected NaN distinct error, got {other:?}"),
    }
}

/// A bare `distinct` (no field) keys on every input field through the
/// resolver's `iter_fields`, hashing the full record. A single input field
/// keeps the all-fields key order deterministic (the test resolver
/// iterates a `HashMap`, whose multi-field order is per-instance
/// randomized), so the concrete dedup outcome is stable while still
/// routing through the all-fields path.
#[test]
fn distinct_bare_all_fields() {
    let rec = |a: i64| HashMap::from([("a".to_string(), Value::Integer(a))]);
    let stream = vec![
        (None, rec(1)),
        (None, rec(2)),
        (None, rec(1)),
        (None, rec(2)),
    ];
    let projs = eval_distinct_stream("distinct\nemit out = a", &["a"], stream);
    let emit_a = |a: i64| ResultProjection::Emit {
        fields: vec![("out".to_string(), Value::Integer(a))],
        record_vars: vec![],
    };
    // First 1 and first 2 emit; the repeat 1 and repeat 2 are duplicates of
    // their first occurrences and skip.
    assert_eq!(
        projs,
        vec![
            emit_a(1),
            emit_a(2),
            ResultProjection::Skip(SkipReason::Duplicate),
            ResultProjection::Skip(SkipReason::Duplicate),
        ],
    );
}

// ── Corpus: emit each ─────────────────────────────────────

/// `emit each` fans an array source into one output record per element;
/// the run produces the expected `EmitMany` records. The per-element
/// binding is visible to the body, and a field emit before the block seeds
/// every produced record. Over `nums = [10, 20, 30]`, the body
/// `emit val = n + 1` yields val 11 / 21 / 31, each record also carrying
/// the seeded `tag = "t"` (seed first, then the body's `val`).
#[test]
fn emit_each_fanout_and_seed() {
    let nums = Value::Array(vec![
        Value::Integer(10),
        Value::Integer(20),
        Value::Integer(30),
    ]);
    // `emit tag = "t"` before the block seeds every fanned record.
    let proj = eval_project(
        "emit tag = \"t\"\nemit each n in nums {\n  emit val = n + 1\n}",
        &["nums"],
        HashMap::from([("nums".to_string(), nums)]),
    );
    let record = |val: i64| {
        (
            vec![
                ("tag".to_string(), Value::String("t".into())),
                ("val".to_string(), Value::Integer(val)),
            ],
            vec![],
        )
    };
    assert_eq!(
        proj,
        ResultProjection::EmitMany {
            records: vec![record(11), record(21), record(31)],
        },
    );
}

/// A null `emit each` source fans out zero records — the block is a no-op,
/// and a preceding field emit produces the single non-fanned record.
#[test]
fn emit_each_null_source() {
    let proj = eval_project(
        "emit base = 1\nemit each n in nums {\n  emit val = n\n}",
        &["nums"],
        HashMap::from([("nums".to_string(), Value::Null)]),
    );
    // No fan-out: the result is a plain Emit carrying the pre-block field.
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("base".to_string(), Value::Integer(1))],
            record_vars: vec![],
        },
    );
}

/// An empty array source runs zero body iterations; the run
/// produce zero fanned records (a plain `Emit` with any pre-block writes).
#[test]
fn emit_each_empty_array() {
    let proj = eval_project(
        "emit each n in nums {\n  emit val = n\n}",
        &["nums"],
        HashMap::from([("nums".to_string(), Value::Array(vec![]))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
}

/// The fan-out ceiling fires *before* the body when the running emit count
/// reaches `max_expansion`. A low ceiling threaded through
/// `with_max_expansion` makes the boundary observable; the run must
/// surface `ExpansionLimitExceeded`.
#[test]
fn emit_each_ceiling_boundary() {
    let src = "emit each n in nums {\n  emit val = n\n}";
    let typed = type_program(src, &["nums"]);
    let stable = StableEvalContext::test_default();
    let nums = Value::Array(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);
    let resolver = HashMapResolver::new(HashMap::from([("nums".to_string(), nums)]));

    // Ceiling of 2 over a 3-element array → the third element trips it.
    // `with_max_expansion` threads the ceiling into the shared `EvalState`.
    let ctx = EvalContext::test_default_borrowed(&stable);
    let mut eval = ProgramEvaluator::with_max_expansion(Arc::new(typed), false, 2);
    let result = eval.eval_record::<NullStorage>(&ctx, &resolver, None);

    match project(&result) {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("ExpansionLimitExceeded"),
            "expected ExpansionLimitExceeded, got {kind}"
        ),
        other => panic!("expected ceiling error, got {other:?}"),
    }
}

/// A `trace` inside an `emit each` body is a no-op: its guard and message
/// are never evaluated. The guard here divides by a zero-valued field and
/// the message reads `1 / z`, so if the trace *were* evaluated it would
/// surface `DivisionByZero` instead of fanning out cleanly. The run must
/// produce the clean fan-out, proving the compiled emit-each body no-ops
/// the trace.
#[test]
fn emit_each_body_trace_is_noop() {
    let nums = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
    let proj = eval_project(
        "emit each n in nums {\n  trace if (1 / z) == 0 \"would error: \" + (1 / z).to_string()\n  emit val = n\n}",
        &["nums", "z"],
        HashMap::from([
            ("nums".to_string(), nums),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    // The trace no-ops, so the fan-out is clean: one record per element,
    // each carrying just its `val` (n).
    let record = |val: i64| (vec![("val".to_string(), Value::Integer(val))], vec![]);
    assert_eq!(
        proj,
        ResultProjection::EmitMany {
            records: vec![record(1), record(2)],
        },
    );
}

// ── Corpus: system-access leaves with resolvable data ─────────────────

/// Evaluate a program that references declared scoped vars / sources,
/// with the resolve and typecheck passes threaded the same
/// `ScopedVarsRegistry` so `$vars.<key>` / `$source.<input>.<field>` /
/// qualified field refs resolve. The eval context's `static_vars` /
/// `source_input_arcs` stay empty, so the declared reads resolve to
/// `Null` at run time; returns the projected result.
fn eval_project_with_vars(
    src: &str,
    fields: &[&str],
    qualified_sources: &[&str],
    registry: &crate::resolve::scoped_vars::ScopedVarsRegistry,
    record: HashMap<String, Value>,
) -> ResultProjection {
    use crate::resolve::pass::resolve_program_with_modules_and_vars;
    use crate::typecheck::pass::{AggregateMode, type_check_with_mode_and_vars};
    let parsed = Parser::parse(src);
    assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
    let resolved = resolve_program_with_modules_and_vars(
        parsed.ast,
        fields,
        parsed.node_count,
        &std::collections::HashMap::new(),
        registry,
    )
    .unwrap_or_else(|d| {
        panic!(
            "resolve: {:?}",
            d.iter().map(|e| &e.message).collect::<Vec<_>>()
        )
    });
    let typed = type_check_with_mode_and_vars(resolved, &empty_row(), AggregateMode::Row, registry)
        .unwrap_or_else(|d| {
            panic!(
                "type: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        })
        .with_source(Arc::from(src));

    // Declare each qualified-source input name with an empty Arc list so
    // `$source.<input>.<field>` resolves to Null at eval rather than
    // panicking on a missing map entry.
    let mut input_arcs = std::collections::HashMap::new();
    for s in qualified_sources {
        input_arcs.insert(s.to_string(), Vec::new());
    }
    let stable = StableEvalContext {
        source_input_arcs: Arc::new(input_arcs),
        ..StableEvalContext::test_default()
    };
    let resolver = HashMapResolver::new(record);

    let ctx = EvalContext::test_default_borrowed(&stable);
    let mut eval = ProgramEvaluator::new(Arc::new(typed), false);
    let result = eval.eval_record::<NullStorage>(&ctx, &resolver, None);

    project(&result)
}

// ── Corpus: the `trace` log channel ───────────────────────────────────

/// One captured `trace` event: the level the statement chose and the
/// rendered message text. `source_row` / `source_file` ride along on the
/// same event but the message and level are the channel a wrong-level or
/// wrong-guard compiled node would corrupt, so those two are what the test
/// pins.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedTrace {
    level: tracing::Level,
    message: String,
}

/// Pulls the `message` field out of a `trace`/`debug`/... event. The
/// `tracing` macro records the format string under the well-known
/// `message` field name; every other field on the event (`source_row`,
/// `source_file`) is ignored because the level and message are the
/// fidelity channel under test.
struct MessageVisitor(Option<String>);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0 = Some(value.to_string());
        }
    }
}

/// A `tracing` layer that appends every event's `(level, message)` to a
/// shared buffer. Installed via `with_default` around one eval call so the
/// capture is scoped to that single run.
struct CapturingLayer(Arc<std::sync::Mutex<Vec<CapturedTrace>>>);

impl<S> tracing_subscriber::Layer<S> for CapturingLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = MessageVisitor(None);
        event.record(&mut visitor);
        self.0.lock().unwrap().push(CapturedTrace {
            level: *event.metadata().level(),
            message: visitor.0.unwrap_or_default(),
        });
    }
}

/// Run one record through the compiled evaluator with a capturing
/// subscriber scoped to just this call, returning the trace events
/// emitted. `with_default` installs the subscriber for the current thread
/// only for the duration of the closure, so the capture stays isolated.
fn capture_traces(
    src: &str,
    fields: &[&str],
    record: HashMap<String, Value>,
) -> Vec<CapturedTrace> {
    use tracing_subscriber::layer::SubscriberExt;
    let typed = type_program(src, fields);
    let has_distinct = program_has_distinct(&typed);
    let stable = StableEvalContext::test_default();
    let resolver = HashMapResolver::new(record);
    let ctx = EvalContext::test_default_borrowed(&stable);
    let mut eval = ProgramEvaluator::new(Arc::new(typed), has_distinct);

    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(CapturingLayer(Arc::clone(&captured)));
    tracing::subscriber::with_default(subscriber, || {
        // Discard the EvalResult — the field/skip/error channel is pinned
        // by the other tests; this helper isolates the trace channel. The
        // corpus programs emit cleanly, so a clean evaluate is expected.
        let _ = eval
            .eval_record::<NullStorage>(&ctx, &resolver, None)
            .expect("trace-fidelity program evaluates cleanly");
    });
    Arc::try_unwrap(captured).unwrap().into_inner().unwrap()
}

/// The `trace` statement's emitted log line and level. This is the one
/// fidelity channel the `EvalResult` cannot see: `trace` writes to the
/// `tracing` sink, not to `fields` / `$record` / `$pipeline`, so a
/// compiled node that fired at the wrong level, dropped a guarded trace,
/// or rendered a different computed message would pass every other test
/// yet corrupt observability. Each case pins the exact `(level, message)`
/// sequence the compiled path must emit.
///
/// Cases carry a guard plus a computed (concatenated, field-derived)
/// message so both the guard-branch and the message-evaluation paths are
/// exercised: a guard that passes, a guard that suppresses, an explicit
/// level, and a multi-statement program whose two traces must appear in
/// order.
#[test]
fn trace_channel_golden() {
    use tracing::Level;
    let line = |level: Level, message: &str| CapturedTrace {
        level,
        message: message.to_string(),
    };

    // Guard true → the warn-level trace fires with a computed message.
    assert_eq!(
        capture_traces(
            "trace warn if amount > 100 \"high amount: \" + amount.to_string()\nemit ok = 1",
            &["amount"],
            HashMap::from([("amount".to_string(), Value::Integer(250))]),
        ),
        vec![line(Level::WARN, "high amount: 250")],
    );
    // Guard false → no trace event.
    assert_eq!(
        capture_traces(
            "trace warn if amount > 100 \"high amount: \" + amount.to_string()\nemit ok = 1",
            &["amount"],
            HashMap::from([("amount".to_string(), Value::Integer(50))]),
        ),
        vec![],
    );
    // Unguarded info-level trace with a field-interpolated message.
    assert_eq!(
        capture_traces(
            "trace info \"row \" + label\nemit ok = 1",
            &["label"],
            HashMap::from([("label".to_string(), Value::from("alpha"))]),
        ),
        vec![line(Level::INFO, "row alpha")],
    );
    // Two traces at distinct levels must appear in statement order.
    assert_eq!(
        capture_traces(
            "trace debug \"first\"\ntrace error if amount > 0 \"second: \" + amount.to_string()\nemit ok = 1",
            &["amount"],
            HashMap::from([("amount".to_string(), Value::Integer(7))]),
        ),
        vec![line(Level::DEBUG, "first"), line(Level::ERROR, "second: 7"),],
    );
    // Default-level (bare `trace`) with a computed string message.
    assert_eq!(
        capture_traces(
            "trace \"value=\" + (amount * 2).to_string()\nemit ok = 1",
            &["amount"],
            HashMap::from([("amount".to_string(), Value::Integer(21))]),
        ),
        vec![line(Level::TRACE, "value=42")],
    );
}

/// `$source.<member>` builtins and `$doc.<section>.<field>` resolve
/// without a declared registry, including the
/// `Null` an empty envelope resolves `$doc` to.
#[test]
fn system_access_builtins_and_doc() {
    // `$source.row` is a builtin → Integer(1) under the test context.
    let proj = eval_project("emit out = $source.row", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(1))],
            record_vars: vec![],
        },
    );
    // `$source.file` is a builtin string → the test context's "test.csv".
    assert_eq!(
        eval_project("emit out = $source.file", &[], HashMap::new()),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::String("test.csv".into()))],
            record_vars: vec![],
        },
    );
    // `$doc.<section>.<field>` with an empty envelope → Null.
    let proj = eval_project("emit out = $doc.header.title", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// `$vars.<key>` (declared static config) resolves through the shared
/// `static_vars` map; with the map empty at run time the read returns
/// `Null`. Pinned because `VarsAccess` otherwise has zero coverage.
#[test]
fn vars_access() {
    use crate::resolve::scoped_vars::{ScopedVarType, ScopedVarsRegistry};
    let mut registry = ScopedVarsRegistry::default();
    registry
        .static_vars
        .insert("threshold".to_string(), ScopedVarType::Int);
    let proj = eval_project_with_vars(
        "emit out = $vars.threshold",
        &[],
        &[],
        &registry,
        HashMap::new(),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// `$source.<input_name>.<field>` (qualified source access) resolves
/// through the plan-time `source_input_arcs` map. The input is declared so
/// the read type-checks; with no source-var writes the read resolves
/// `Null`. Pinned because `QualifiedSourceAccess` otherwise has zero
/// coverage.
#[test]
fn qualified_source_access() {
    use crate::resolve::scoped_vars::{ScopedVarType, ScopedVarsRegistry};
    let mut registry = ScopedVarsRegistry::default();
    registry
        .source
        .insert("tag".to_string(), ScopedVarType::String);
    let proj = eval_project_with_vars(
        "emit out = $source.upstream.tag",
        &[],
        &["upstream"],
        &registry,
        HashMap::new(),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Null)],
            record_vars: vec![],
        },
    );
}

/// `$record.<key>` reads resolve through the dedicated record-vars channel
/// (the resolver, under the `$record.` prefix). A declared-and-seeded key
/// returns its value; a declared-but-unseeded key resolves `Null`. Pinned
/// because the `RecordAccess` read form otherwise has zero coverage.
#[test]
fn record_access_read() {
    use crate::resolve::scoped_vars::{ScopedVarType, ScopedVarsRegistry};
    let mut registry = ScopedVarsRegistry::default();
    registry
        .record
        .insert("tier".to_string(), ScopedVarType::String);
    registry
        .record
        .insert("absent".to_string(), ScopedVarType::String);
    // Seed only `tier` into the record-vars channel; `absent` is declared
    // (so the read type-checks) but never written, so it must read `Null`.
    let record = HashMap::from([("$record.tier".to_string(), Value::String("gold".into()))]);
    let proj = eval_project_with_vars(
        "emit present = $record.tier\nemit missing = $record.absent",
        &[],
        &[],
        &registry,
        record,
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![
                ("present".to_string(), Value::String("gold".into())),
                ("missing".to_string(), Value::Null),
            ],
            record_vars: vec![],
        },
    );
}

/// A 3-part qualified field reference (`source.record_type.field`)
/// resolves through `resolve_qualified` with the first two parts joined as
/// the compound source key. Pinned because the 3-part form otherwise has
/// zero coverage.
#[test]
fn qualified_field_ref_three_part() {
    let resolver = HashMapResolver::new(HashMap::new()).with_qualified(
        "orders.line",
        "amount",
        Value::Integer(42),
    );
    let src = "emit out = orders.line.amount";
    let typed = type_program(src, &[]);
    let stable = StableEvalContext::test_default();

    let ctx = EvalContext::test_default_borrowed(&stable);
    let mut eval = ProgramEvaluator::new(Arc::new(typed), false);
    let result = eval.eval_record::<NullStorage>(&ctx, &resolver, None);

    // The compound `orders.line` + `amount` resolves to the seeded value.
    assert_eq!(
        project(&result),
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(42))],
            record_vars: vec![],
        },
    );
}

// ── Corpus: real example-pipeline transform bodies ─────────────────────

/// One example-pipeline corpus case: source text, input field set, the
/// input record, and the projection hand-reasoned for that program.
type ExampleCase = (
    &'static str,
    &'static [&'static str],
    HashMap<String, Value>,
    ResultProjection,
);

/// The row-transform CXL bodies lifted verbatim from `examples/pipelines/`,
/// each paired with the projection hand-reasoned from the program's CXL
/// semantics on the given inputs. Any pipeline body that type-checks as a
/// flat row transform must produce that exact projection now that every
/// node lowers; aggregate / combine bodies (qualified refs, `sum(..)`) are
/// not row transforms and are out of scope for this evaluator, so they are
/// excluded rather than expected to type-check here.
#[test]
fn example_pipeline_transform_bodies() {
    let str_field = |k: &str, v: &str| (k.to_string(), Value::String(v.into()));
    let emit = |fields: Vec<(String, Value)>| ResultProjection::Emit {
        fields,
        record_vars: vec![],
    };
    // (src, input field set, record, expected projection).
    let cases: Vec<ExampleCase> = vec![
        (
            // status == "active" → true.
            "emit is_active = status == \"active\"",
            &["status"],
            HashMap::from([str_field("status", "active")]),
            emit(vec![("is_active".to_string(), Value::Bool(true))]),
        ),
        (
            // "25000".to_int() = 25000 > 10000 → "gold".
            "emit tier = if lifetime_value.to_int() > 10000 then \"gold\" else \"standard\"",
            &["lifetime_value"],
            HashMap::from([str_field("lifetime_value", "25000")]),
            emit(vec![str_field("tier", "gold")]),
        ),
        (
            // A fiscal-year derivation: parse a date field, then branch on
            // its month/year. Exercises the date-method surface (to_date /
            // month / year) as a real flat row transform — the prior
            // `days_since(...)` free-function form has no CXL call syntax,
            // so it never type-checked and was silently skipped.
            //
            // 2026-05-01 → month 5, not < 4 → year + 1 = 2027.
            "let parsed = invoice_date.to_date(\"%Y-%m-%d\")\nemit fiscal_year = if parsed.month() < 4 then parsed.year() else parsed.year() + 1",
            &["invoice_date"],
            HashMap::from([str_field("invoice_date", "2026-05-01")]),
            emit(vec![("fiscal_year".to_string(), Value::Integer(2027))]),
        ),
        (
            // priority == "high" passes the filter → emit p = "high".
            "filter priority == \"high\"\nemit p = priority",
            &["priority"],
            HashMap::from([str_field("priority", "high")]),
            emit(vec![str_field("p", "high")]),
        ),
        (
            // upper("12 main st") = "12 MAIN ST";
            // "abcdef0123456789".substring(0, 8) = "abcdef01";
            // upper("high").concat(": ", "please expedite")
            //   = "HIGH: please expedite";
            // "please expedite".length() = 15 chars.
            "emit address_upper = shipping_address.upper()\nemit uuid_prefix = ticket_uuid.substring(0, 8)\nemit tagged_comment = priority.upper().concat(\": \", comment)\nemit comment_length = comment.length()",
            &["shipping_address", "ticket_uuid", "priority", "comment"],
            HashMap::from([
                str_field("shipping_address", "12 main st"),
                str_field("ticket_uuid", "abcdef0123456789"),
                str_field("priority", "high"),
                str_field("comment", "please expedite"),
            ]),
            emit(vec![
                str_field("address_upper", "12 MAIN ST"),
                str_field("uuid_prefix", "abcdef01"),
                str_field("tagged_comment", "HIGH: please expedite"),
                ("comment_length".to_string(), Value::Integer(15)),
            ]),
        ),
        (
            // "ORD-" + pad_left("42", 8, "0") = "ORD-00000042";
            // quantity (int 3) * unit_price (float 9.99) widens → 29.97.
            "emit order_ref = \"ORD-\" + order_id.to_string().pad_left(8, \"0\")\nemit line_total = quantity * unit_price",
            &["order_id", "quantity", "unit_price"],
            HashMap::from([
                ("order_id".to_string(), Value::Integer(42)),
                ("quantity".to_string(), Value::Integer(3)),
                ("unit_price".to_string(), Value::Float(9.99)),
            ]),
            emit(vec![
                str_field("order_ref", "ORD-00000042"),
                ("line_total".to_string(), Value::Float(29.97)),
            ]),
        ),
        (
            // "urgent" == "urgent" → true → "priority_report".
            "emit _route = if priority_level == \"urgent\" or priority_level == \"high\" then \"priority_report\" else \"fulfilled_orders\"",
            &["priority_level"],
            HashMap::from([str_field("priority_level", "urgent")]),
            emit(vec![str_field("_route", "priority_report")]),
        ),
    ];
    // Fail-closed: every listed body must type-check as a flat row
    // transform and produce its expected projection. A silent skip would
    // let a body that stops type-checking (a grammar change, a renamed
    // builtin) vanish from the corpus unnoticed, so a non-typeable body is
    // a hard failure here, and the run count is asserted equal to the case
    // count so a dropped case cannot pass as a no-op.
    let case_count = cases.len();
    let mut ran = 0usize;
    for (src, fields, record, expected) in cases {
        assert!(
            try_type_program(src, fields).is_some(),
            "corpus body failed to type-check as a flat row transform: `{src}`"
        );
        assert_eq!(
            eval_project(src, fields, record),
            expected,
            "example pipeline body `{src}`",
        );
        ran += 1;
    }
    assert_eq!(
        ran, case_count,
        "every corpus body must run; ran {ran} of {case_count}"
    );
}

// ── Property test: random scalar-core expressions ──────────────────────

/// A generated scalar-core expression. Lowers to CXL source text and is
/// evaluated by the compiled path. The grammar covers literals (including
/// nulls), the three input field refs, nested binary / unary / coalesce
/// / if, with real depth — but no method calls, closures, windows, or
/// other deferred nodes.
#[derive(Debug, Clone)]
enum GenExpr {
    IntLit(i64),
    BoolLit(bool),
    NullLit,
    StrLit(String),
    Field(&'static str),
    /// A reference to an earlier `let v<i>` binding, rendered as the bare
    /// identifier `v<i>`. Only the statement-sequence proptest emits this,
    /// and only for an index already bound earlier in the same program, so
    /// the reference is always in scope and resolves through the env.
    LetVar(usize),
    Binary(&'static str, Box<GenExpr>, Box<GenExpr>),
    Not(Box<GenExpr>),
    Neg(Box<GenExpr>),
    Coalesce(Box<GenExpr>, Box<GenExpr>),
    If(Box<GenExpr>, Box<GenExpr>, Box<GenExpr>),
    /// A zero-argument method on an arbitrary receiver. Restricted to the
    /// `TypeTag::Any`-receiver builtins (`to_string` / `is_null` /
    /// `type_of`) so the method does not constrain the receiver's type
    /// and over-reject otherwise-valid generated programs. Exercises the
    /// compiled `MethodCall` node and its delegation to `dispatch_method`
    /// — including the null-receiver gate and its exceptions — over the
    /// same nested receivers the scalar core generates.
    Method(&'static str, Box<GenExpr>),
    /// `catch(default)`: a one-argument `Any`-receiver method, exercising
    /// the eager-argument path of the compiled `MethodCall` node.
    Catch(Box<GenExpr>, Box<GenExpr>),
}

impl GenExpr {
    /// Render to CXL source text. Every binary/unary form is fully
    /// parenthesized so the generated precedence is unambiguous and the
    /// parser reconstructs exactly the generated tree.
    fn render(&self) -> String {
        match self {
            GenExpr::IntLit(n) => n.to_string(),
            GenExpr::BoolLit(b) => b.to_string(),
            GenExpr::NullLit => "null".to_string(),
            GenExpr::StrLit(s) => format!("\"{s}\""),
            GenExpr::Field(name) => name.to_string(),
            GenExpr::LetVar(i) => format!("v{i}"),
            GenExpr::Binary(op, l, r) => format!("({} {} {})", l.render(), op, r.render()),
            GenExpr::Not(e) => format!("(not {})", e.render()),
            GenExpr::Neg(e) => format!("(-{})", e.render()),
            GenExpr::Coalesce(l, r) => format!("({} ?? {})", l.render(), r.render()),
            GenExpr::If(c, t, e) => {
                format!(
                    "(if {} then {} else {})",
                    c.render(),
                    t.render(),
                    e.render()
                )
            }
            GenExpr::Method(name, recv) => format!("({}).{}()", recv.render(), name),
            GenExpr::Catch(recv, default) => {
                format!("({}).catch({})", recv.render(), default.render())
            }
        }
    }
}

/// Leaf generator: literals (with nulls and strings) and the three input
/// field refs `a` / `b` / `c`.
fn leaf_strategy() -> impl Strategy<Value = GenExpr> {
    prop_oneof![
        (-1000i64..1000).prop_map(GenExpr::IntLit),
        any::<bool>().prop_map(GenExpr::BoolLit),
        Just(GenExpr::NullLit),
        "[a-z]{0,4}".prop_map(GenExpr::StrLit),
        Just(GenExpr::Field("a")),
        Just(GenExpr::Field("b")),
        Just(GenExpr::Field("c")),
    ]
}

/// Recursive expression generator with real nesting: up to 4 levels
/// deep, branching into binary / unary / coalesce / if forms over the
/// full operator set the scalar core supports.
fn expr_strategy() -> impl Strategy<Value = GenExpr> {
    leaf_strategy().prop_recursive(4, 48, 4, |inner| {
        prop_oneof![
            (
                prop_oneof![
                    Just("+"),
                    Just("-"),
                    Just("*"),
                    Just("/"),
                    Just("%"),
                    Just("=="),
                    Just("!="),
                    Just(">"),
                    Just("<"),
                    Just(">="),
                    Just("<="),
                    Just("and"),
                    Just("or"),
                ],
                inner.clone(),
                inner.clone(),
            )
                .prop_map(|(op, l, r)| GenExpr::Binary(op, Box::new(l), Box::new(r))),
            inner.clone().prop_map(|e| GenExpr::Not(Box::new(e))),
            inner.clone().prop_map(|e| GenExpr::Neg(Box::new(e))),
            (inner.clone(), inner.clone())
                .prop_map(|(l, r)| GenExpr::Coalesce(Box::new(l), Box::new(r))),
            (inner.clone(), inner.clone(), inner.clone()).prop_map(|(c, t, e)| GenExpr::If(
                Box::new(c),
                Box::new(t),
                Box::new(e)
            )),
            (
                prop_oneof![Just("to_string"), Just("is_null"), Just("type_of")],
                inner.clone(),
            )
                .prop_map(|(name, recv)| GenExpr::Method(name, Box::new(recv))),
            (inner.clone(), inner.clone())
                .prop_map(|(recv, default)| GenExpr::Catch(Box::new(recv), Box::new(default))),
        ]
    })
}

/// Random input values for the `a` / `b` / `c` fields, spanning every
/// scalar type plus null so generated expressions hit mixed-type and
/// null-propagation paths.
fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        (-1000i64..1000).prop_map(Value::Integer),
        (-1000.0f64..1000.0).prop_map(Value::Float),
        "[a-z]{0,4}".prop_map(|s| Value::String(s.into())),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// For any generated scalar-core expression and any random a/b/c
    /// inputs, the compiled evaluator must evaluate without panicking and
    /// produce a *deterministic* result: two fresh runs of the same
    /// program against the same record yield byte-identical projections
    /// and `$pipeline`/`$source` side effects. This is the broad fuzz net
    /// over the lowering vocabulary — a node that panics, leaks state
    /// across runs, or depends on `HashMap` iteration order fails here.
    #[test]
    fn compiled_eval_is_deterministic(
        expr in expr_strategy(),
        a in value_strategy(),
        b in value_strategy(),
        c in value_strategy(),
    ) {
        let src = format!("emit out = {}", expr.render());
        // Discard programs that are not well-typed CXL (e.g. a field used
        // as both numeric and string): the evaluator is not defined on
        // them, so they are out of scope.
        let typed_a = match try_type_program(&src, &["a", "b", "c"]) {
            Some(t) => t,
            None => return Err(TestCaseError::reject("not well-typed")),
        };
        let typed_b = try_type_program(&src, &["a", "b", "c"]).expect("second type-check agrees");
        let record = HashMap::from([
            ("a".to_string(), a),
            ("b".to_string(), b),
            ("c".to_string(), c),
        ]);
        let resolver = HashMapResolver::new(record);
        let has_distinct = program_has_distinct(&typed_a);

        let stable_a = StableEvalContext::test_default();
        let ctx_a = EvalContext::test_default_borrowed(&stable_a);
        let mut eval_a = ProgramEvaluator::new(Arc::new(typed_a), has_distinct);
        let first = eval_a.eval_record::<NullStorage>(&ctx_a, &resolver, None);

        let stable_b = StableEvalContext::test_default();
        let ctx_b = EvalContext::test_default_borrowed(&stable_b);
        let mut eval_b = ProgramEvaluator::new(Arc::new(typed_b), has_distinct);
        let second = eval_b.eval_record::<NullStorage>(&ctx_b, &resolver, None);

        prop_assert_eq!(project(&first), project(&second), "non-deterministic on `{}`", src);
        prop_assert_eq!(
            snapshot_side_effects(&stable_a),
            snapshot_side_effects(&stable_b),
            "$pipeline/$source side effects non-deterministic on `{}`",
            src
        );
    }
}

// ── Property test: random statement sequences ──────────────────────────

/// One generated statement in a randomized program. Covers the four emit
/// targets (so emit routing is property-checked, not just example-checked)
/// plus `let` bindings, with an optional leading `filter`. `Filter` only
/// appears as the first statement — a mid-program filter would skip the
/// record and make the remaining statements unreachable, which is valid
/// but uninteresting for the routing/ordering property this exercises.
#[derive(Debug, Clone)]
enum GenStmt {
    /// `let v<i> = <expr>` — exercises the env-write path; the binding is
    /// uniquely named so it never shadows an input field.
    Let(usize, GenExpr),
    /// `emit out<i> = <expr>` — Field target → output map.
    EmitField(usize, GenExpr),
    /// `emit $record.r<i> = <expr>` — Record target → record_vars channel.
    EmitRecord(usize, GenExpr),
    /// `emit $pipeline.p<i> = <expr>` — Pipeline target → context, not
    /// surfaced in the result.
    EmitPipeline(usize, GenExpr),
    /// `emit $source.s<i> = <expr>` — Source target → context, not
    /// surfaced in the result.
    EmitSource(usize, GenExpr),
}

impl GenStmt {
    fn render(&self) -> String {
        match self {
            GenStmt::Let(i, e) => format!("let v{i} = {}", e.render()),
            GenStmt::EmitField(i, e) => format!("emit out{i} = {}", e.render()),
            GenStmt::EmitRecord(i, e) => format!("emit $record.r{i} = {}", e.render()),
            GenStmt::EmitPipeline(i, e) => format!("emit $pipeline.p{i} = {}", e.render()),
            GenStmt::EmitSource(i, e) => format!("emit $source.s{i} = {}", e.render()),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// For any generated statement *sequence* — an optional leading
    /// `filter`, then one-to-six `let` / `emit` statements spanning all
    /// four emit targets — the compiled evaluator must run without
    /// panicking and produce a *deterministic* result: two fresh runs
    /// yield byte-identical projections and `$pipeline`/`$source` side
    /// effects. This property-checks emit routing (which channel each
    /// target lands in) and statement ordering (the order field emits
    /// accumulate, and the filter short-circuit) under fuzz.
    #[test]
    fn compiled_statement_seq_is_deterministic(
        leading_filter in proptest::option::of(expr_strategy()),
        // Each statement carries its own independent expr (kind, rhs,
        // ref_earlier_let) so the fuzz varies the RHS across the sequence
        // rather than reusing one shared filler for every statement.
        stmts in proptest::collection::vec(
            (0usize..5, expr_strategy(), any::<bool>()),
            1..6,
        ),
        a in value_strategy(),
        b in value_strategy(),
        c in value_strategy(),
    ) {
        let mut lines: Vec<String> = Vec::new();
        if let Some(f) = &leading_filter {
            lines.push(format!("filter {}", f.render()));
        }
        // Track which `let v<i>` indices are in scope so an emit RHS can
        // reference an earlier binding — exercising env-read after env-write
        // within one record's statement run, not just leaf field reads.
        let mut bound_lets: Vec<usize> = Vec::new();
        for (i, (kind, rhs, ref_earlier_let)) in stmts.iter().enumerate() {
            // When the statement is an emit and asks to reference an earlier
            // let, swap its RHS for the most recent in-scope `v<i>`; lets
            // always use their freshly generated expr.
            let rhs = if *kind != 0 && *ref_earlier_let {
                match bound_lets.last() {
                    Some(&j) => GenExpr::LetVar(j),
                    None => rhs.clone(),
                }
            } else {
                rhs.clone()
            };
            let stmt = match kind {
                0 => {
                    bound_lets.push(i);
                    GenStmt::Let(i, rhs)
                }
                1 => GenStmt::EmitField(i, rhs),
                2 => GenStmt::EmitRecord(i, rhs),
                3 => GenStmt::EmitPipeline(i, rhs),
                _ => GenStmt::EmitSource(i, rhs),
            };
            lines.push(stmt.render());
        }
        // Ensure at least one field emit so a non-skipped program produces
        // an observable output channel (otherwise every emit could route
        // to pipeline/source and the result is trivially empty).
        lines.push("emit final = a".to_string());
        let src = lines.join("\n");

        let typed_a = match try_type_program(&src, &["a", "b", "c"]) {
            Some(t) => t,
            None => return Err(TestCaseError::reject("not well-typed")),
        };
        let typed_b = try_type_program(&src, &["a", "b", "c"]).expect("second type-check agrees");
        let record = HashMap::from([
            ("a".to_string(), a),
            ("b".to_string(), b),
            ("c".to_string(), c),
        ]);
        let resolver = HashMapResolver::new(record);
        let has_distinct = program_has_distinct(&typed_a);

        let stable_a = StableEvalContext::test_default();
        let ctx_a = EvalContext::test_default_borrowed(&stable_a);
        let mut eval_a = ProgramEvaluator::new(Arc::new(typed_a), has_distinct);
        let first = eval_a.eval_record::<NullStorage>(&ctx_a, &resolver, None);

        let stable_b = StableEvalContext::test_default();
        let ctx_b = EvalContext::test_default_borrowed(&stable_b);
        let mut eval_b = ProgramEvaluator::new(Arc::new(typed_b), has_distinct);
        let second = eval_b.eval_record::<NullStorage>(&ctx_b, &resolver, None);

        prop_assert_eq!(project(&first), project(&second), "non-deterministic on `{}`", src);
        // The generator emits to all four targets, so this run almost
        // always writes through the $pipeline/$source channel; the two
        // runs must agree on it too.
        prop_assert_eq!(
            snapshot_side_effects(&stable_a),
            snapshot_side_effects(&stable_b),
            "$pipeline/$source side effects non-deterministic on `{}`",
            src
        );
    }
}
