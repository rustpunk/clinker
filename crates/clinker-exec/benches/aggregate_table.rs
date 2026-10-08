//! Group-table benchmarks for the hash Aggregate.
//!
//! Models the hash Aggregate's group table on its own, to measure what growing
//! the table costs and how much of that a table storing each key's hash would
//! recover. A slot is the operator's key (`Vec<GroupByKey>`, built through
//! `value_to_group_key`, so the key form and its hash are the operator's) plus a
//! filler the size of `AggregatorGroupState`, so slots move on growth as the
//! operator's do. Every variant uses the hasher today's map uses.
//!
//! The variant is read once from `CLINKER_GROUP_TABLE_VARIANT` (default `map`):
//!
//! - `map`: `hashbrown::HashMap::new()`, the table the operator holds today;
//! - `presized_map`: the same map created with capacity for every group, so it
//!   never grows;
//! - `stored_hash`: a `hashbrown::HashTable` whose slots keep the key's hash,
//!   finding by that hash first and then key equality, and rehashing on growth
//!   from the stored hash.
//!
//! Benchmark IDs never name the variant, so criterion compares variants across
//! saved baselines: `map` minus `presized_map` is the whole growth cost, and
//! `map` minus `stored_hash` is what storing the hash recovers. For example
//! `CLINKER_GROUP_TABLE_VARIANT=stored_hash cargo bench -p clinker-exec --bench
//! aggregate_table -- --baseline map`.
//!
//! `group_table_insert` inserts `groups` distinct keys into an empty table.
//! `group_table_probe` is the few-groups, many-records shape: 1,000 groups and
//! 1,000,000 records, each record passing an owned key that is dropped when its
//! group already exists, as the operator's per-record path does.

use chrono::NaiveDate;
use clinker_exec::aggregation::AggregatorGroupState;
use clinker_record::{GroupByKey, Value, value_to_group_key};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use hashbrown::hash_table::Entry;
use hashbrown::{DefaultHashBuilder, HashMap, HashTable};
use rust_decimal::Decimal;
use std::hash::BuildHasher;
use std::sync::OnceLock;

/// Words in a filler the size of the operator's per-group state.
const STATE_WORDS: usize = std::mem::size_of::<AggregatorGroupState>().div_ceil(8);

/// Stands in for `AggregatorGroupState`: same size, 8-byte aligned.
#[derive(Clone, Copy)]
struct StateFiller([u64; STATE_WORDS]);

impl StateFiller {
    fn new() -> Self {
        Self([0; STATE_WORDS])
    }

    /// Touch the state as folding a record into its group would.
    fn fold(&mut self) {
        self.0[0] = self.0[0].wrapping_add(1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    Map,
    PresizedMap,
    StoredHash,
}

fn variant() -> Variant {
    static VARIANT: OnceLock<Variant> = OnceLock::new();
    *VARIANT.get_or_init(
        || match std::env::var("CLINKER_GROUP_TABLE_VARIANT").as_deref() {
            Err(_) | Ok("map") => Variant::Map,
            Ok("presized_map") => Variant::PresizedMap,
            Ok("stored_hash") => Variant::StoredHash,
            Ok(other) => panic!(
                "CLINKER_GROUP_TABLE_VARIANT={other}: expected map, presized_map or stored_hash"
            ),
        },
    )
}

type Key = Vec<GroupByKey>;

/// An odd multiplier coprime with every count below, so `i * PERMUTE % n`
/// visits each of `0..n` once in a scrambled order.
const PERMUTE: u64 = 0x9E37_79B1;

fn permuted(i: usize, n: usize) -> u64 {
    (i as u64).wrapping_mul(PERMUTE) % n as u64
}

fn group_key(values: &[Value]) -> Key {
    values
        .iter()
        .map(|value| {
            value_to_group_key(value, "k", 0)
                .expect("a groupable value")
                .unwrap_or(GroupByKey::Null)
        })
        .collect()
}

/// The `n`-th distinct key of `shape` (distinct for every `n` below the count).
fn shape_key(shape: &str, n: u64) -> Key {
    let values = match shape {
        "str16" => vec![Value::String(format!("k{n:015}").into())],
        "int" => vec![Value::Integer(n as i64)],
        "decimal" => {
            // Scale 0-4; the fractional digits encode the scale, so no two
            // `n` normalize to the same value.
            let scale = (n % 5) as u32;
            let mantissa = (n as i64) * 10i64.pow(scale) + i64::from(scale);
            vec![Value::Decimal(Decimal::new(mantissa, scale))]
        }
        "mixed3" => {
            let epoch = NaiveDate::from_ymd_opt(2020, 1, 1).expect("a valid date");
            vec![
                Value::String(format!("name-{}", n % 1_000).into()),
                Value::Integer((n / 1_000) as i64),
                Value::Date(epoch + chrono::Days::new(n % 365)),
            ]
        }
        other => unreachable!("unknown group-table shape {other}"),
    };
    group_key(&values)
}

fn distinct_keys(shape: &str, groups: usize) -> Vec<Key> {
    (0..groups)
        .map(|i| shape_key(shape, permuted(i, groups)))
        .collect()
}

/// Today's table, optionally presized. Entry-or-insert as the operator does.
fn fill_map(keys: Vec<Key>, capacity: usize) -> HashMap<Key, StateFiller> {
    let mut map = if capacity == 0 {
        HashMap::new()
    } else {
        HashMap::with_capacity(capacity)
    };
    for key in keys {
        map.entry(key).or_insert_with(StateFiller::new).fold();
    }
    map
}

/// A table whose slots keep each key's hash: one hash per lookup, compare
/// stored hashes before keys, and rehash on growth from the stored hash.
fn fill_stored_hash(keys: Vec<Key>) -> HashTable<(u64, Key, StateFiller)> {
    let hasher = DefaultHashBuilder::default();
    let mut table: HashTable<(u64, Key, StateFiller)> = HashTable::new();
    for key in keys {
        let hash = hasher.hash_one(key.as_slice());
        let slot = match table.entry(
            hash,
            |(stored, existing, _)| *stored == hash && *existing == key,
            |(stored, _, _)| *stored,
        ) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert((hash, key, StateFiller::new())).into_mut(),
        };
        slot.2.fold();
    }
    table
}

/// Builds the selected variant's table from `keys` and returns it, so dropping
/// it falls outside the timed region.
enum Filled {
    Map(HashMap<Key, StateFiller>),
    StoredHash(HashTable<(u64, Key, StateFiller)>),
}

fn fill(keys: Vec<Key>, groups: usize) -> Filled {
    match variant() {
        Variant::Map => Filled::Map(fill_map(keys, 0)),
        Variant::PresizedMap => Filled::Map(fill_map(keys, groups)),
        Variant::StoredHash => Filled::StoredHash(fill_stored_hash(keys)),
    }
}

fn filled_len(filled: &Filled) -> usize {
    match filled {
        Filled::Map(map) => map.len(),
        Filled::StoredHash(table) => table.len(),
    }
}

fn bench_group_table_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_table_insert");
    for shape in ["str16", "int", "decimal", "mixed3"] {
        for groups in [10_000usize, 100_000, 1_000_000] {
            let keys = distinct_keys(shape, groups);
            group.sample_size(if groups >= 1_000_000 { 10 } else { 100 });
            group.throughput(Throughput::Elements(groups as u64));
            group.bench_with_input(BenchmarkId::new(shape, groups), &groups, |b, &groups| {
                b.iter_batched(
                    || keys.clone(),
                    |keys| {
                        let filled = fill(keys, groups);
                        debug_assert_eq!(filled_len(&filled), groups);
                        black_box(filled)
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

fn bench_group_table_probe(c: &mut Criterion) {
    const GROUPS: usize = 1_000;
    const RECORDS: usize = 1_000_000;
    let mut group = c.benchmark_group("group_table_probe");
    group.sample_size(10);
    for shape in ["str16", "mixed3"] {
        let groups = distinct_keys(shape, GROUPS);
        let records: Vec<Key> = (0..RECORDS)
            .map(|r| groups[permuted(r, GROUPS) as usize].clone())
            .collect();
        group.throughput(Throughput::Elements(RECORDS as u64));
        group.bench_with_input(
            BenchmarkId::new(shape, format!("{GROUPS}x{RECORDS}")),
            &GROUPS,
            |b, &groups| {
                b.iter_batched(
                    || records.clone(),
                    |records| {
                        let filled = fill(records, groups);
                        debug_assert_eq!(filled_len(&filled), groups);
                        black_box(filled)
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_group_table_insert, bench_group_table_probe);
criterion_main!(benches);
