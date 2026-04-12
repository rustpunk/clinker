use clinker_core::config::{LayerKind, ProvenanceDb, ResolvedValue};
use clinker_core::span::Span;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn test_span() -> Span {
    Span::SYNTHETIC
}

/// Bench applying a full 4-layer provenance stack.
fn bench_apply_layer_full_stack(c: &mut Criterion) {
    c.bench_function("apply_layer_full_stack", |b| {
        b.iter(|| {
            let mut rv = ResolvedValue::new(
                serde_json::json!(42),
                LayerKind::CompositionDefault,
                test_span(),
            );
            rv.apply_layer(
                serde_json::json!(43),
                LayerKind::ChannelDefault,
                test_span(),
            );
            rv.apply_layer(serde_json::json!(44), LayerKind::ChannelFixed, test_span());
            rv.apply_layer(serde_json::json!(45), LayerKind::InspectorEdit, test_span());
            black_box(&rv);
        })
    });
}

/// Bench inserting 50 entries into a ProvenanceDb (matches WORKSPACE_COMPOSITION_BUDGET).
fn bench_provenance_db_insert_50(c: &mut Criterion) {
    c.bench_function("provenance_db_insert_50", |b| {
        b.iter(|| {
            let mut db = ProvenanceDb::default();
            for i in 0..50 {
                db.insert(
                    format!("node_{i}"),
                    format!("param_{i}"),
                    ResolvedValue::new(
                        serde_json::json!(i),
                        LayerKind::CompositionDefault,
                        test_span(),
                    ),
                );
            }
            black_box(&db);
        })
    });
}

criterion_group!(
    benches,
    bench_apply_layer_full_stack,
    bench_provenance_db_insert_50
);
criterion_main!(benches);
