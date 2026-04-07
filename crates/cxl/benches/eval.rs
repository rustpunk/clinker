use clinker_bench_support::{CxlComplexity, MEDIUM, RecordFactory, SMALL};
use clinker_record::{RecordStorage, Schema, Value};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use cxl::eval::ProgramEvaluator;
use cxl::eval::context::{EvalContext, StableEvalContext};
use cxl::parser::Parser;
use cxl::resolve::{HashMapResolver, resolve_program};
use cxl::typecheck::type_check;
use std::collections::HashMap;
use std::sync::Arc;

/// Dummy storage for eval_record's generic parameter when no window is used.
struct NullStorage;

impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u32 {
        0
    }
}

/// Compile a CXL source string into a TypedProgram.
fn compile(source: &str, fields: &[&str]) -> cxl::typecheck::TypedProgram {
    let parsed = Parser::parse(source);
    assert!(
        parsed.errors.is_empty(),
        "parse errors: {:?}",
        parsed.errors
    );
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
    type_check(resolved, &indexmap::IndexMap::new()).unwrap()
}

/// Build a HashMapResolver from a Record.
fn resolver_from_record(record: &clinker_record::Record, schema: &Schema) -> HashMapResolver {
    let mut map = HashMap::new();
    for col in schema.columns() {
        if let Some(val) = record.get(col) {
            map.insert(col.to_string(), val.clone());
        }
    }
    HashMapResolver::new(map)
}

// ── Simple emit ────────────────────────────────────────────────────

fn bench_eval_simple_emit(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_simple_emit");
    let complexity = CxlComplexity::Simple;
    let typed = Arc::new(compile(complexity.source(), &complexity.required_fields()));

    for count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(10, 16, 0.0, 42);
        let records = factory.generate(count);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
    }
    group.finish();
}

// ── String methods ─────────────────────────────────────────────────

fn bench_eval_string_methods(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_string_methods");
    let source = r#"
emit out = f1.upper().trim().replace("A", "X")
"#;
    let typed = Arc::new(compile(source, &["f1"]));

    for count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(10, 32, 0.0, 42);
        let records = factory.generate(count);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
    }
    group.finish();
}

// ── Numeric chain ──────────────────────────────────────────────────

fn bench_eval_numeric_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_numeric_chain");
    let source = r#"
let x = f0 + f2 * 3
emit out = x.to_float().round(2).abs()
"#;
    let typed = Arc::new(compile(source, &["f0", "f2"]));

    for count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(10, 16, 0.0, 42);
        let records = factory.generate(count);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
    }
    group.finish();
}

// ── Conditional ────────────────────────────────────────────────────

fn bench_eval_conditional(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_conditional");
    let source = r#"
emit out = if f0 > 500000 then "high" else if f0 > 250000 then "medium" else "low"
"#;
    let typed = Arc::new(compile(source, &["f0"]));

    for count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(10, 16, 0.0, 42);
        let records = factory.generate(count);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
    }
    group.finish();
}

// ── Match expression ───────────────────────────────────────────────

fn bench_eval_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_match");
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
    let typed = Arc::new(compile(source, &["f1"]));

    for count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(10, 16, 0.0, 42);
        let records = factory.generate(count);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
    }
    group.finish();
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
        group.bench_with_input(BenchmarkId::new("null_pct", null_pct), &null_pct, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
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
        group.bench_with_input(BenchmarkId::from_parameter(label), &label, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
    }
    group.finish();
}

// ── Full realistic transform ───────────────────────────────────────

fn bench_eval_full_transform(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_full_transform");
    let complexity = CxlComplexity::Complex;
    let typed = Arc::new(compile(complexity.source(), &complexity.required_fields()));

    for count in [SMALL, MEDIUM] {
        let mut factory = RecordFactory::new(20, 32, 0.05, 42);
        let records = factory.generate(count);
        let schema = factory.schema().clone();
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let mut evaluator = ProgramEvaluator::new(Arc::clone(&typed), false);
                for rec in &records {
                    let resolver = resolver_from_record(rec, &schema);
                    let _result = evaluator.eval_record::<NullStorage>(&ctx, &resolver, None);
                    black_box(&_result);
                }
            });
        });
    }
    group.finish();
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
