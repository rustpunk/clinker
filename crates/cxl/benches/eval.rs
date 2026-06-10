use clinker_bench_support::{CxlComplexity, MEDIUM, RecordFactory, SMALL};
use clinker_record::{Record, RecordStorage, Schema, Value};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use cxl::eval::ProgramEvaluator;
use cxl::eval::context::{EvalContext, StableEvalContext};
use cxl::lexer::Span;
use cxl::parser::Parser;
use cxl::resolve::{HashMapResolver, resolve_program};
use cxl::typecheck::TypedProgram;
use cxl::typecheck::{Row, type_check};
use std::collections::HashMap;
use std::sync::Arc;

/// Dummy storage for eval_record's generic parameter when no window is used.
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

/// The two per-record evaluation paths benchmarked side by side. The
/// compiled path is selected on `ProgramEvaluator` via `set_use_compiled`;
/// running both under the same workload makes the parity ratio a direct
/// read of the compiled-vs-tree-walk timings.
const PATHS: [(&str, bool); 2] = [("tree_walk", false), ("compiled", true)];

/// Compile a CXL source string into a TypedProgram.
fn compile(source: &str, fields: &[&str]) -> TypedProgram {
    let parsed = Parser::parse(source);
    assert!(
        parsed.errors.is_empty(),
        "parse errors: {:?}",
        parsed.errors
    );
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
    type_check(
        resolved,
        &Row::closed(indexmap::IndexMap::new(), Span::new(0, 0)),
    )
    .unwrap()
}

/// Build a HashMapResolver from a Record.
fn resolver_from_record(record: &Record, schema: &Schema) -> HashMapResolver {
    let mut map = HashMap::new();
    for col in schema.columns() {
        if let Some(val) = record.get(col) {
            map.insert(col.to_string(), val.clone());
        }
    }
    HashMapResolver::new(map)
}

/// Drive one full pass over `records` through a freshly-built evaluator on
/// the requested path. A fresh evaluator per iteration mirrors the
/// per-node construction the executor does and keeps the compiled-program
/// build cost inside the measured window — the compiled path must clear
/// the tree-walk including that one-time lowering.
fn run_path(
    typed: &Arc<TypedProgram>,
    records: &[Record],
    schema: &Schema,
    ctx: &EvalContext<'_>,
    use_compiled: bool,
) {
    let mut evaluator = ProgramEvaluator::new(Arc::clone(typed), false);
    evaluator.set_use_compiled(use_compiled);
    for rec in records {
        let resolver = resolver_from_record(rec, schema);
        let result = evaluator.eval_record::<NullStorage>(ctx, &resolver, None);
        black_box(&result);
    }
}

/// Benchmark `source` over both paths at each record count, emitting
/// `<group>/<path>/<count>` ids so the parity ratio reads directly off the
/// two paths' timings for the same workload.
fn bench_counts(
    c: &mut Criterion,
    group_name: &str,
    source: &str,
    fields: &[&str],
    field_count: usize,
    str_len: usize,
    null_ratio: f64,
) {
    let mut group = c.benchmark_group(group_name);
    let typed = Arc::new(compile(source, fields));

    for count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(field_count, str_len, null_ratio, 42);
        let records = factory.generate(count);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(count as u64));
        for (path_label, use_compiled) in PATHS {
            group.bench_with_input(BenchmarkId::new(path_label, count), &count, |b, _| {
                b.iter(|| run_path(&typed, &records, &schema, &ctx, use_compiled));
            });
        }
    }
    group.finish();
}

// ── Simple emit ────────────────────────────────────────────────────

fn bench_eval_simple_emit(c: &mut Criterion) {
    let complexity = CxlComplexity::Simple;
    bench_counts(
        c,
        "eval_simple_emit",
        complexity.source(),
        &complexity.required_fields(),
        10,
        16,
        0.0,
    );
}

// ── String methods ─────────────────────────────────────────────────

fn bench_eval_string_methods(c: &mut Criterion) {
    let source = r#"
emit out = f1.upper().trim().replace("A", "X")
"#;
    bench_counts(c, "eval_string_methods", source, &["f1"], 10, 32, 0.0);
}

// ── Numeric chain ──────────────────────────────────────────────────

fn bench_eval_numeric_chain(c: &mut Criterion) {
    let source = r#"
let x = f0 + f2 * 3
emit out = x.to_float().round(2).abs()
"#;
    bench_counts(c, "eval_numeric_chain", source, &["f0", "f2"], 10, 16, 0.0);
}

// ── Conditional ────────────────────────────────────────────────────

fn bench_eval_conditional(c: &mut Criterion) {
    let source = r#"
emit out = if f0 > 500000 then "high" else if f0 > 250000 then "medium" else "low"
"#;
    bench_counts(c, "eval_conditional", source, &["f0"], 10, 16, 0.0);
}

// ── Match expression ───────────────────────────────────────────────

fn bench_eval_match(c: &mut Criterion) {
    let source = r#"
emit out = match f1 {
    "alpha" => 1
    "beta" => 2
    "gamma" => 3
    "delta" => 4
    "epsilon" => 5
    _ => 0
}
"#;
    bench_counts(c, "eval_match", source, &["f1"], 10, 16, 0.0);
}

// ── Coalesce with varying null ratios ──────────────────────────────

fn bench_eval_coalesce(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_coalesce");
    let source = r#"
emit out = f1 ?? f3 ?? f5 ?? "default"
"#;
    let typed = Arc::new(compile(source, &["f1", "f3", "f5"]));

    for null_pct in [0, 50, 100] {
        let null_ratio = null_pct as f64 / 100.0;
        let mut factory = RecordFactory::new(10, 16, null_ratio, 42);
        let records = factory.generate(SMALL);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(SMALL as u64));
        for (path_label, use_compiled) in PATHS {
            group.bench_with_input(
                BenchmarkId::new(format!("{path_label}_null_pct"), null_pct),
                &null_pct,
                |b, _| {
                    b.iter(|| run_path(&typed, &records, &schema, &ctx, use_compiled));
                },
            );
        }
    }
    group.finish();
}

// ── Filter with varying selectivity ────────────────────────────────

fn bench_eval_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_filter");

    // 10% selectivity: most records pass (f0 > 100_000 on 0..1M range → ~90% pass)
    // 50% selectivity: f0 > 500_000
    // 90% selectivity: f0 > 900_000 → ~10% pass
    for (label, threshold) in [
        ("pass_90pct", 100_000),
        ("pass_50pct", 500_000),
        ("pass_10pct", 900_000),
    ] {
        let source = format!("emit out = f0\nfilter f0 > {threshold}");
        let typed = Arc::new(compile(&source, &["f0"]));

        let mut factory = RecordFactory::new(10, 16, 0.0, 42);
        let records = factory.generate(SMALL);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(SMALL as u64));
        for (path_label, use_compiled) in PATHS {
            group.bench_with_input(BenchmarkId::new(path_label, label), &label, |b, _| {
                b.iter(|| run_path(&typed, &records, &schema, &ctx, use_compiled));
            });
        }
    }
    group.finish();
}

// ── Full realistic transform ───────────────────────────────────────

fn bench_eval_full_transform(c: &mut Criterion) {
    let complexity = CxlComplexity::Complex;
    bench_counts(
        c,
        "eval_full_transform",
        complexity.source(),
        &complexity.required_fields(),
        20,
        32,
        0.05,
    );
}

criterion_group!(
    benches,
    bench_eval_simple_emit,
    bench_eval_string_methods,
    bench_eval_numeric_chain,
    bench_eval_conditional,
    bench_eval_match,
    bench_eval_coalesce,
    bench_eval_filter,
    bench_eval_full_transform,
);
criterion_main!(benches);
