use clinker_record::{
    Value,
    owned_storage::{OwnedMap, OwnedValues},
};
use cxl::eval::{EvalContext, StableEvalContext, builtins_impl::dispatch_method};
use cxl::lexer::Span;

#[test]
fn nested_owned_mutation_tracer() {
    let original = Value::Map(OwnedMap::from_map(indexmap::IndexMap::from([(
        "items".into(),
        Value::Array(OwnedValues::from_vec(vec![
            Value::Integer(1),
            Value::Integer(2),
        ])),
    )])));
    let stable = StableEvalContext::test_default();
    let context = EvalContext::test_default_borrowed(&stable);
    let result = dispatch_method(
        &original,
        "set",
        &[Value::from("items[0]"), Value::Integer(7)],
        None,
        Span::new(0, 3),
        &context,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        result.get_field("items").unwrap(),
        &Value::Array(OwnedValues::from_vec(vec![
            Value::Integer(7),
            Value::Integer(2)
        ]))
    );
    assert_eq!(
        original.get_field("items").unwrap(),
        &Value::Array(OwnedValues::from_vec(vec![
            Value::Integer(1),
            Value::Integer(2)
        ]))
    );
}

use clinker_record::{
    FieldStr,
    owned_storage::{
        AllocationAuthority, AllocationLease, AllocationResources, AllocationScope, OwnedKey,
        OwnerId, ResourceError, ResourceErrorKind,
    },
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

thread_local! { static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) }; }
struct Observer;
// SAFETY: every pointer and layout is delegated unchanged to the system allocator.
unsafe impl GlobalAlloc for Observer {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        // SAFETY: forward the caller's valid allocation layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forward the original pointer and allocation layout.
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Observer = Observer;
fn count_allocations<T>(work: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATIONS.with(|count| count.set(None));
        }
    }
    ALLOCATIONS.with(|count| {
        assert!(count.get().is_none());
        count.set(Some(0));
    });
    let _reset = Reset;
    let result = work();
    let count = ALLOCATIONS.with(|count| count.get().unwrap());
    (result, count)
}
struct Ledger {
    used: AtomicUsize,
}
impl AllocationAuthority for Ledger {
    fn try_reserve(
        self: Arc<Self>,
        owner: OwnerId,
        layout: Layout,
    ) -> Result<AllocationLease, ResourceError> {
        self.used
            .fetch_update(SeqCst, SeqCst, |used| {
                used.checked_add(layout.size()).filter(|n| *n <= 1 << 20)
            })
            .map_err(|_| ResourceError::new(ResourceErrorKind::Budget, layout.size(), 0))?;
        AllocationLease::admitted(self, owner, layout.size())
    }
    fn release(&self, _: OwnerId, bytes: usize) {
        self.used.fetch_sub(bytes, SeqCst);
    }
    fn check_cancelled(&self) -> Result<(), ResourceError> {
        Ok(())
    }
}
const TEXT: &str = "shared text retained through nested evaluation — café";
fn nested(governed: bool, scope: &AllocationScope) -> Value {
    fn map(
        entries: impl IntoIterator<Item = (&'static str, Value)>,
        governed: bool,
        scope: &AllocationScope,
    ) -> OwnedMap {
        if governed {
            let mut map = OwnedMap::try_with_capacity(2, scope).unwrap();
            for (key, value) in entries {
                map.try_insert(OwnedKey::try_new(key, scope).unwrap(), value, scope)
                    .unwrap();
            }
            map
        } else {
            OwnedMap::from_map(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.into(), value))
                    .collect(),
            )
        }
    }
    let text = FieldStr::try_new(TEXT, scope).unwrap();
    let inner = Value::Map(map(
        [
            ("text", Value::String(text.clone())),
            ("n", Value::Integer(1)),
        ],
        governed,
        scope,
    ));
    let values = if governed {
        let mut values = OwnedValues::try_with_capacity(2, scope).unwrap();
        values.try_push(inner, scope).unwrap();
        values.try_push(Value::Integer(99), scope).unwrap();
        values
    } else {
        OwnedValues::from_vec(vec![inner, Value::Integer(99)])
    };
    Value::Map(map(
        [
            ("rows", Value::Array(values)),
            ("tail", Value::String(text)),
        ],
        governed,
        scope,
    ))
}
fn call(value: &Value, method: &str, args: &[Value], context: &EvalContext<'_>) -> Value {
    dispatch_method(value, method, args, None, Span::new(2, 9), context)
        .unwrap()
        .unwrap()
}

#[test]
fn nested_set_unset_make_one_container_copy_and_preserve_shared_grants() {
    for governed in [false, true] {
        for drop_original_first in [false, true] {
            let ledger = Arc::new(Ledger {
                used: AtomicUsize::new(0),
            });
            let scope = AllocationResources::new(ledger.clone()).scope().unwrap();
            let original = nested(governed, &scope);
            let snapshot = original.clone();
            let stable = StableEvalContext::test_default();
            let context = EvalContext::test_default_borrowed(&stable);
            let charge = ledger.used.load(SeqCst);
            let (copy, copy_allocations) = count_allocations(|| original.clone());
            drop(copy);
            let set_args = [Value::from("rows[0].n"), Value::Integer(7)];
            let (result, allocations) =
                count_allocations(|| call(&original, "set", &set_args, &context));
            // The path Vec and the same two temporary map keys used by the old
            // entry/insert algorithm accompany exactly one recursive copy.
            assert_eq!(allocations, copy_allocations + 3);
            assert_eq!(original, snapshot);
            let Value::Array(rows) = result.get_field("rows").unwrap() else {
                panic!()
            };
            assert_eq!(rows[0].get_field("n"), Some(&Value::Integer(7)));
            assert_eq!(rows.capacity(), 2);
            let unset_args = [Value::from("rows[0].n")];
            let (removed, allocations) =
                count_allocations(|| call(&original, "unset", &unset_args, &context));
            assert_eq!(
                allocations,
                copy_allocations + 1,
                "only the parsed path accompanies the one copy"
            );
            let Value::Array(rows) = removed.get_field("rows").unwrap() else {
                panic!()
            };
            assert!(rows[0].get_field("n").is_none());
            assert_eq!(
                rows[0].as_map().unwrap().get_index(0).unwrap().0.as_str(),
                "text"
            );
            assert_eq!(ledger.used.load(SeqCst), charge);
            let Value::String(escaped) = result.get_field("tail").unwrap() else {
                panic!()
            };
            let escaped = escaped.clone();
            let Value::String(source) = original.get_field("tail").unwrap() else {
                panic!()
            };
            assert_eq!(escaped.as_ptr(), source.as_ptr());
            let text_charge = escaped.heap_size();
            drop((snapshot, removed));
            if drop_original_first {
                drop(original);
                assert_eq!(ledger.used.load(SeqCst), text_charge);
                drop(result);
            } else {
                drop(result);
                assert_eq!(ledger.used.load(SeqCst), charge);
                drop(original);
            }
            assert_eq!(ledger.used.load(SeqCst), text_charge);
            assert_eq!(escaped.as_str(), TEXT);
            drop(escaped);
            assert_eq!(ledger.used.load(SeqCst), 0);
        }
    }
}

#[test]
fn nested_mutation_conflicts_misses_and_array_removal_keep_existing_results() {
    let ledger = Arc::new(Ledger {
        used: AtomicUsize::new(0),
    });
    let scope = AllocationResources::new(ledger.clone()).scope().unwrap();
    let original = nested(true, &scope);
    let stable = StableEvalContext::test_default();
    let context = EvalContext::test_default_borrowed(&stable);
    for path in ["rows[9].n", "rows.n", "rows[0].n.field", "rows["] {
        assert_eq!(
            call(
                &original,
                "set",
                &[Value::from(path), Value::Null],
                &context
            ),
            Value::Null
        );
        assert_eq!(
            call(&original, "unset", &[Value::from(path)], &context),
            original
        );
    }
    let removed = call(&original, "unset", &[Value::from("rows[0]")], &context);
    let Value::Array(rows) = removed.get_field("rows").unwrap() else {
        panic!()
    };
    assert_eq!(rows.as_slice(), &[Value::Integer(99)]);
    assert_eq!(rows.capacity(), 2);
    let added = call(
        &original,
        "set",
        &[Value::from("added.child"), Value::Integer(5)],
        &context,
    );
    assert_eq!(
        added.get_field("added").unwrap().get_field("child"),
        Some(&Value::Integer(5))
    );
    assert!(original.get_field("added").is_none());
    assert_eq!(
        added.as_map().unwrap().get_index(2).unwrap().0.as_str(),
        "added"
    );
    drop((removed, added, original));
    assert_eq!(ledger.used.load(SeqCst), 0);
}

#[test]
fn shared_document_nested_read_uses_public_compiled_evaluator_and_file_identity() {
    use clinker_record::{DocumentContext, DocumentId, EnvelopeRecord, RecordStorage};
    use cxl::{
        eval::{EvalResult, ProgramEvaluator},
        parser::Parser,
        resolve::{HashMapResolver, resolve_program},
        typecheck::{Row, type_check},
    };
    struct NoWindow;
    impl RecordStorage for NoWindow {
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
    let ledger = Arc::new(Ledger {
        used: AtomicUsize::new(0),
    });
    let scope = AllocationResources::new(ledger.clone()).scope().unwrap();
    let file: Arc<str> = Arc::from("input.data");
    let envelope = EnvelopeRecord::from_sections(indexmap::IndexMap::from([(
        "custom_section".into(),
        nested(true, &scope),
    )]));
    let document =
        DocumentContext::try_new(DocumentId::next(), file.clone(), envelope, &scope).unwrap();
    let source = r#"emit result = $doc.custom_section.rows[0]["text"]"#;
    let parsed = Parser::parse(source);
    assert!(parsed.errors.is_empty());
    let resolved = resolve_program(parsed.ast, &[], parsed.node_count).unwrap();
    let typed = type_check(
        resolved,
        &Row::closed(indexmap::IndexMap::new(), Span::new(0, 0)),
    )
    .unwrap();
    let mut evaluator = ProgramEvaluator::new(Arc::new(typed), false);
    let stable = StableEvalContext::test_default();
    let mut context = EvalContext::test_with_file(&stable, &file, 1);
    context.doc_ctx = &document;
    assert!(Arc::ptr_eq(context.source_file, document.source_file()));
    let resolver = HashMapResolver::new(std::collections::HashMap::new());
    let EvalResult::Emit { fields, .. } = evaluator
        .eval_record::<NoWindow>(&context, &resolver, None)
        .unwrap()
    else {
        panic!()
    };
    let Value::String(leaf) = &fields["result"] else {
        panic!()
    };
    assert_eq!(leaf.as_str(), TEXT);
    let charge = leaf.heap_size();
    drop(document);
    assert_eq!(ledger.used.load(SeqCst), charge);
    drop(fields);
    assert_eq!(ledger.used.load(SeqCst), 0);
}
