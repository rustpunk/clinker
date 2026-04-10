use clinker_bench_support::CxlComplexity;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use cxl::lexer::Lexer;
use cxl::parser::Parser;
use cxl::resolve::resolve_program;
use cxl::typecheck::type_check;
use std::collections::HashMap;

fn bench_lex(c: &mut Criterion) {
    let mut group = c.benchmark_group("cxl_lex");
    for (name, complexity) in [
        ("simple", CxlComplexity::Simple),
        ("medium", CxlComplexity::Medium),
        ("complex", CxlComplexity::Complex),
    ] {
        let source = complexity.source();
        group.bench_with_input(BenchmarkId::from_parameter(name), &source, |b, &src| {
            b.iter(|| Lexer::tokenize(src));
        });
    }
    group.finish();
}

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("cxl_parse");
    for (name, complexity) in [
        ("simple", CxlComplexity::Simple),
        ("medium", CxlComplexity::Medium),
        ("complex", CxlComplexity::Complex),
    ] {
        let source = complexity.source();
        group.bench_with_input(BenchmarkId::from_parameter(name), &source, |b, &src| {
            b.iter(|| Parser::parse(src));
        });
    }
    group.finish();
}

fn bench_typecheck(c: &mut Criterion) {
    let mut group = c.benchmark_group("cxl_typecheck");
    for (name, complexity) in [
        ("simple", CxlComplexity::Simple),
        ("medium", CxlComplexity::Medium),
        ("complex", CxlComplexity::Complex),
    ] {
        let source = complexity.source();
        let fields = complexity.required_fields();

        // Validate that the source compiles before benchmarking
        let parsed = Parser::parse(source);
        assert!(
            parsed.errors.is_empty(),
            "parse errors in {name}: {:?}",
            parsed.errors
        );
        let resolved =
            resolve_program(parsed.ast, &fields, parsed.node_count).expect("resolve failed");
        type_check(resolved, &indexmap::IndexMap::new()).expect("typecheck failed");

        // Benchmark: resolve + typecheck (since ResolvedProgram isn't Clone,
        // we include resolve in the measured path)
        group.bench_with_input(BenchmarkId::from_parameter(name), &source, |b, &src| {
            b.iter(|| {
                let parsed = Parser::parse(src);
                let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
                type_check(resolved, &indexmap::IndexMap::new()).unwrap()
            });
        });
    }
    group.finish();
}

fn bench_full_compile(c: &mut Criterion) {
    let mut group = c.benchmark_group("cxl_full_compile");
    for (name, complexity) in [
        ("simple", CxlComplexity::Simple),
        ("medium", CxlComplexity::Medium),
        ("complex", CxlComplexity::Complex),
    ] {
        let source = complexity.source();
        let fields = complexity.required_fields();
        group.bench_with_input(BenchmarkId::from_parameter(name), &source, |b, &src| {
            b.iter(|| {
                let parsed = Parser::parse(src);
                let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
                type_check(resolved, &indexmap::IndexMap::new()).unwrap()
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_lex,
    bench_parse,
    bench_typecheck,
    bench_full_compile
);
criterion_main!(benches);
