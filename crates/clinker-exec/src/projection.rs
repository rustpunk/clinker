use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{Record, SchemaBuilder, Value, round_decimal_to_scale};
use cxl::typecheck::Type;
use indexmap::IndexMap;

use clinker_plan::config::OutputConfig;

/// Apply schema aliases to emitted fields: rename keys from original to alias names.
///
/// Alias creates an identity boundary (SQL model): CXL uses original field names,
/// but output-facing code (mapping, writers) sees the post-alias names.
pub fn apply_aliases(emitted: &mut IndexMap<String, Value>, aliases: &HashMap<String, String>) {
    if aliases.is_empty() {
        return;
    }
    let entries: Vec<(String, Value)> = emitted.drain(..).collect();
    for (name, value) in entries {
        let output_name = aliases.get(&name).cloned().unwrap_or(name);
        emitted.insert(output_name, value);
    }
}

/// Apply output projection: gather → exclude → mapping.
///
/// 1. **Gather**: Start with CXL-emitted fields. If `include_unmapped`,
///    add all input record fields not already emitted (the path that
///    surfaces `OnUnmapped::AutoWiden`-discovered columns at the sink).
/// 2. **Exclude**: Remove any field in `exclude` list (by current name).
/// 3. **Mapping**: Emit the columns `mapping` lists, in declaration
///    order, under their declared output names; append whatever the
///    block did not list when `include_unmapped` is set.
pub fn project_output(
    input_record: &Record,
    emitted: &IndexMap<String, Value>,
    config: &OutputConfig,
) -> Record {
    project_output_with_meta(input_record, emitted, &IndexMap::new(), config)
}

/// Project output directly from a Record (Invariant 3 — no parallel
/// bookkeeping map input).
///
/// Gather order follows `Record::iter_all_fields`: schema columns in
/// declaration order. Builds the output record in one pass when the
/// config has no exclude / mapping — the hot path avoids an
/// intermediate `IndexMap` entirely. Config-driven rewrites fall into
/// the slow path, which keeps an owned `IndexMap` only for the
/// duration of the call.
pub fn project_output_with_meta(
    input_record: &Record,
    _emitted: &IndexMap<String, Value>,
    _metadata: &IndexMap<String, Value>,
    config: &OutputConfig,
) -> Record {
    project_output_from_record(input_record, config, None)
}

/// Record-driven projection (Invariant 3 implementation).
///
/// `include_unmapped: true` (the default) surfaces every column on the
/// record. With `OnUnmapped::AutoWiden` at the source, the record's
/// schema includes both user-declared columns and probe-discovered
/// columns; this flag lets the sink emit all of them.
///
/// `include_unmapped: false`: when `cxl_emit_names` is `Some`, the
/// output is restricted to those names — upstream passthroughs the user
/// did NOT explicitly emit are dropped. This matches the documented
/// Output projection semantic. When `cxl_emit_names` is `None` (caller
/// has no upstream PlanNode handle), all upstream columns survive — the
/// permissive fallback used by tests and ad-hoc projections.
///
/// A declared `mapping:` overrides the `cxl_emit_names` restriction for
/// the columns it lists: the block is the author's own statement of
/// which columns the file carries, so a listed column is emitted whether
/// or not the upstream transform named it in an `emit`. `include_unmapped`
/// still governs everything the block does not list.
pub fn project_output_from_record(
    input_record: &Record,
    config: &OutputConfig,
    cxl_emit_names: Option<&[String]>,
) -> Record {
    let drop_unmapped =
        !config.include_unmapped && cxl_emit_names.is_some() && config.mapping.is_none();
    let needs_rewrite = config.exclude.is_some()
        || config.mapping.is_some()
        || config.include_unmapped
        || drop_unmapped;
    // `include_correlation_keys: true` surfaces `$ck.<field>` source-CK
    // shadows and `$ck.aggregate.<name>` synthetic-CK lineage to the
    // sink — but NOT the `$widened` sidecar absorber. Sidecar
    // expansion is gated independently by `include_unmapped: true`,
    // which expands the `Value::Map` payload to top-level fields.
    // Routing both through one engine-stamped toggle would surface
    // the raw `$widened` `Value::Map` as a literal column to the
    // downstream writer, which CSV/XML writers JSON-encode and the
    // fixed-width writer silently empties — neither is the user's
    // intent when they opt into CK visibility.
    let include_correlation_keys = config.include_correlation_keys;

    if !needs_rewrite {
        // Fast path: no exclude, no mapping — emit all Record fields in
        // natural iteration order, no intermediate allocation.
        // Engine-stamped columns are dropped unless the Output node
        // opts in via the appropriate flag (CK → include_correlation_keys;
        // sidecar → include_unmapped, handled in the slow path because
        // expansion needs IndexMap-keyed access).
        let field_count = input_record.total_field_count();
        let mut schema_builder = SchemaBuilder::with_capacity(field_count);
        let mut values: Vec<Value> = Vec::with_capacity(field_count);
        if include_correlation_keys {
            for (name, value) in input_record.iter_user_and_correlation_fields() {
                schema_builder = schema_builder.with_field(name);
                values.push(value.clone());
            }
        } else {
            for (name, value) in input_record.iter_user_fields() {
                schema_builder = schema_builder.with_field(name);
                values.push(value.clone());
            }
        }
        let mut out = Record::new(schema_builder.build(), values);
        // The projected row is the same document's row — carry its
        // envelope context forward so a writer that reconstructs the
        // document envelope on output (e.g. EDIFACT `interchange_from_doc`
        // echoing the source `UNB` header) can still resolve
        // `$doc.<section>.<field>` after projection drops engine columns.
        out.set_doc_ctx(Arc::clone(input_record.doc_ctx()));
        round_declared_output_decimals(&mut out, config);
        return out;
    }

    // Slow path: config requires rewriting field names / dropping
    // fields, which wants the temporary IndexMap's keyed access.
    let mut fields: IndexMap<String, Value> =
        IndexMap::with_capacity(input_record.total_field_count());
    if include_correlation_keys {
        for (name, value) in input_record.iter_user_and_correlation_fields() {
            fields.insert(name.to_string(), value.clone());
        }
    } else {
        for (name, value) in input_record.iter_user_fields() {
            fields.insert(name.to_string(), value.clone());
        }
    }

    // `include_unmapped: true` expands the `auto_widen` sidecar
    // absorber column (`$widened`, carrying `Value::Map`) back into
    // top-level fields at the sink. Pattern precedent: Auto Loader's
    // `_rescued_data` JSON column expands to top-level when the
    // destination schema accepts it. The sidecar is engine-stamped so
    // `iter_user_fields` skips it by default; this branch is the
    // opt-in path (default-on per the new passthrough semantic).
    if config.include_unmapped {
        let sidecar_payload = input_record
            .get(clinker_plan::config::pipeline_node::WIDENED_SIDECAR_COLUMN)
            .cloned();
        // Strip the sidecar slot itself — its payload is being
        // expanded; the slot name should never appear in output.
        fields.swap_remove(clinker_plan::config::pipeline_node::WIDENED_SIDECAR_COLUMN);
        if let Some(Value::Map(map)) = sidecar_payload {
            for (k, v) in map.iter() {
                fields.entry(k.to_string()).or_insert_with(|| v.clone());
            }
        }
    }

    // Restrict to user-emitted columns when the caller supplied the
    // upstream node's emit-name list and `include_unmapped: false`.
    // Sidecar-expanded fields land in `fields` *before* this filter
    // and survive it because they're not in `cxl_emit_names`; the
    // filter below would drop them. Restrict only when the sidecar
    // was not expanded.
    if drop_unmapped {
        let allowed: std::collections::HashSet<&str> =
            cxl_emit_names.unwrap().iter().map(|s| s.as_str()).collect();
        fields.retain(|k, _| allowed.contains(k.as_str()));
    }

    if let Some(ref exclude_list) = config.exclude {
        for name in exclude_list {
            fields.swap_remove(name.as_str());
        }
    }

    // `mapping:` is the ordered declaration of the columns this output
    // carries: listed columns first, in declaration order, under their
    // declared names, then — when `include_unmapped` is set — everything
    // the block did not claim, in its existing relative order.
    //
    // Per-record cost: one probe per listed column plus one
    // `claims_source` probe per surviving column. Every index the loop
    // consults is resolved once when the config is parsed, and a listed
    // column's value is MOVED out of `fields` rather than copied — the
    // slot is left holding a `Null` placeholder that the append loop
    // below never reaches, because it skips claimed sources. Only a
    // source feeding two output columns is cloned, and only for the
    // readers before the last. So this allocates nothing per record
    // beyond the output record itself; the previous rename pass
    // allocated a whole second `IndexMap` on every record.
    //
    // The placeholder trick is what keeps the append order intact:
    // removing the slot outright would need `swap_remove` (which
    // reorders the passthrough columns) or `shift_remove` (which is
    // O(n) per listed column).
    let (names, values): (Vec<Box<str>>, Vec<Value>) = match config.mapping.as_ref() {
        Some(mapping) => {
            let mut names: Vec<Box<str>> = Vec::with_capacity(mapping.entries().len());
            let mut values: Vec<Value> = Vec::with_capacity(mapping.entries().len());
            for (index, entry) in mapping.entries().iter().enumerate() {
                // A listed column absent from this record is skipped rather
                // than emitted empty. The plan-time E365 gate rejects a name
                // the graph cannot supply where it can see the column set, and
                // the write boundary rejects one it cannot, so what reaches
                // here is a column that exists in the schema but not on this
                // row.
                let Some(slot) = fields.get_mut(entry.source.as_str()) else {
                    continue;
                };
                let value = if mapping.is_last_reader(index) {
                    std::mem::replace(slot, Value::Null)
                } else {
                    slot.clone()
                };
                names.push(Box::<str>::from(entry.output.as_str()));
                values.push(value);
            }
            if config.include_unmapped {
                for (name, value) in fields {
                    // `claims_output` is the collision guard: a passthrough
                    // column whose name the block already writes must not be
                    // appended beside it. `SchemaBuilder` does not dedupe and
                    // the schema's name index is last-write-wins, so the
                    // appended column would answer `Record::get` for the
                    // mapped one and serve its value under the mapped header.
                    // The plan gate catches this where it can enumerate the
                    // upstream columns; open rows and sidecar-expanded columns
                    // reach here unchecked, so this guard is not redundant.
                    if !mapping.claims_source(name.as_str())
                        && !mapping.claims_output(name.as_str())
                    {
                        names.push(Box::<str>::from(name.as_str()));
                        values.push(value);
                    }
                }
            } else {
                // `include_unmapped: false` drops the unlisted USER columns.
                // The engine-stamped correlation shadows are governed by their
                // own flag, so a mapping must not silently defeat an explicit
                // `include_correlation_keys: true` — they are appended after
                // the declared columns instead. A `$`-prefixed key can only be
                // in `fields` because that flag admitted it: `$` is a reserved
                // engine namespace no source column may claim, the `$widened`
                // slot was removed above, and sidecar expansion (the other
                // source of extra keys) runs only on the branch above.
                for (name, value) in fields {
                    if name.starts_with('$')
                        && !mapping.claims_source(name.as_str())
                        && !mapping.claims_output(name.as_str())
                    {
                        names.push(Box::<str>::from(name.as_str()));
                        values.push(value);
                    }
                }
            }
            (names, values)
        }
        None => {
            let mut names: Vec<Box<str>> = Vec::with_capacity(fields.len());
            let mut values: Vec<Value> = Vec::with_capacity(fields.len());
            for (name, value) in fields {
                names.push(Box::<str>::from(name.as_str()));
                values.push(value);
            }
            (names, values)
        }
    };

    let schema = names.into_iter().collect::<SchemaBuilder>().build();
    let mut out = Record::new(schema, values);
    // Same document's row after the rename/exclude rewrite — carry the
    // envelope context forward so document-reconstructing writers still
    // resolve `$doc.<section>.<field>` on the projected record.
    out.set_doc_ctx(Arc::clone(input_record.doc_ctx()));
    round_declared_output_decimals(&mut out, config);
    out
}

/// Every `mapping:` output name absent from an established output schema.
///
/// Empty for a well-formed stream. A name is missing exactly when the entry's
/// source column was not on the records the schema was derived from, so this
/// is the write-boundary half of **E365**: the plan-time gate answers the
/// question wherever it can enumerate the upstream columns, and this answers it
/// where it cannot — inside a composition body (whose rows are open by
/// construction, and where an open tail may legitimately supply anything) and
/// for columns that arrive only through the `auto_widen` sidecar.
///
/// Checked against the SCHEMA, once per output stream, not per record: whether
/// a column exists is a property of the stream. Under selection semantics a
/// mapping entry that resolves to nothing deletes a column from the file, and
/// with `include_unmapped: false` a wholly unresolvable block writes an empty
/// header and blank rows — so silence here is data loss, not a no-op.
pub fn missing_mapping_outputs(
    schema: &clinker_record::Schema,
    config: &OutputConfig,
) -> Vec<String> {
    let Some(mapping) = config.mapping.as_ref() else {
        return Vec::new();
    };
    let present: std::collections::HashSet<&str> = schema.columns().iter().map(|c| &**c).collect();
    let mut missing: Vec<String> = Vec::new();
    for entry in mapping.entries() {
        if !present.contains(entry.output.as_str()) && !missing.contains(&entry.output) {
            missing.push(entry.output.clone());
        }
    }
    missing
}

/// Raise the write-boundary **E365** for an established output schema that the
/// `mapping:` block does not fully cover, or `Ok(())` when it does.
pub fn check_mapping_against_schema(
    schema: &clinker_record::Schema,
    config: &OutputConfig,
    output_name: &str,
) -> Result<(), clinker_plan::error::PipelineError> {
    let missing = missing_mapping_outputs(schema, config);
    if missing.is_empty() {
        return Ok(());
    }
    let mapping = config
        .mapping
        .as_ref()
        .expect("missing_mapping_outputs only reports names when a mapping is declared");
    let sources: Vec<String> = missing
        .iter()
        .filter_map(|out| {
            mapping
                .entries()
                .iter()
                .find(|e| &e.output == out)
                .map(|e| e.source.clone())
        })
        .collect();
    Err(
        clinker_plan::error::PipelineError::OutputMappingColumnMissing {
            output: output_name.to_string(),
            columns: sources,
            available: schema.columns().iter().map(|c| c.to_string()).collect(),
        },
    )
}

/// Enforce a declared output-column `scale` at the write boundary: each
/// `Value::Decimal` landing in a column the output `schema:` declares as
/// `type: decimal` with a `scale` is rescaled to that many fractional digits
/// with the house banker's rounding ([`round_decimal_to_scale`], round-half-to-
/// even) — bit-identical to what a source column's `scale` does on ingest.
///
/// This is the write side of the decimal boundary contract: decimals compute at
/// full precision inside the pipeline (`avg`, `a / b` keep every digit) and are
/// pinned to a declared scale only at the edge they are declared on. An output
/// without a `schema:`, or a column without `scale`, is left at full precision.
///
/// Keyed by the post-mapping (output-facing) column names, so it runs after the
/// rename/exclude rewrite has produced the final record. Only `Value::Decimal`
/// in a `decimal`-declared, scaled column is touched — no other type coercion is
/// performed, and a non-decimal value or a decimal in an unscaled column passes
/// through untouched. Blocking/streaming: pure per-record transform, no
/// buffering; cost is proportional to the declared column count and is skipped
/// entirely for the schema-less outputs (CSV/JSON without a `schema:` block).
fn round_declared_output_decimals(record: &mut Record, config: &OutputConfig) {
    let Some(columns) = config.schema.as_ref().and_then(|s| s.as_columns()) else {
        return;
    };
    for col in columns {
        let Some(scale) = col.scale else {
            continue;
        };
        // A nullable declaration (`type: { nullable: decimal }`) still names a
        // decimal column — unwrap it the same way the fixed-width writer
        // classifies numeric fields.
        if !matches!(col.ty.unwrap_nullable(), Type::Decimal) {
            continue;
        }
        // Copy the value out so the immutable `get` borrow is released before
        // the `set`; a non-decimal value in the slot is left untouched. A
        // `multiple: true` column arrives as an array whose declared `type:`
        // and `scale:` describe each ELEMENT, so the scale applies element-wise
        // — the same rule the read-side coercion follows. Non-decimal elements
        // pass through, matching the scalar arm.
        let rounded = match record.get(&col.name) {
            Some(&Value::Decimal(d)) => Value::Decimal(round_decimal_to_scale(d, Some(scale))),
            Some(Value::Array(items)) => Value::Array(
                items
                    .iter()
                    .map(|item| match item {
                        Value::Decimal(d) => {
                            Value::Decimal(round_decimal_to_scale(*d, Some(scale)))
                        }
                        other => other.clone(),
                    })
                    .collect(),
            ),
            _ => continue,
        };
        record.set(&col.name, rounded);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::config::{MappingEntry, OutputMapping};
    use clinker_record::Schema;

    fn make_input() -> Record {
        // Schema is pre-widened to include every field the post-transform
        // Record would carry — `full_name` is declared up front so
        // `Record::set` hits a known slot.
        let schema = Arc::new(Schema::new(vec![
            "first_name".into(),
            "last_name".into(),
            "secret".into(),
            "full_name".into(),
        ]));
        Record::new(
            schema,
            vec![
                Value::String("Alice".into()),
                Value::String("Smith".into()),
                Value::String("password123".into()),
                Value::Null,
            ],
        )
    }

    #[test]
    fn test_gather_emitted_plus_widened() {
        // project_output drives off the Record itself, so emitted fields
        // must land on the Record before the projection is invoked. The
        // widened schema guarantees every `Record::set` at emit sites
        // hits a known slot.
        let mut input = make_input();
        input.set("full_name", Value::String("Alice Smith".into()));
        let emitted = IndexMap::new();

        let config = OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: true,
            include_header: None,
            mapping: None,
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert_eq!(
            result.get("full_name"),
            Some(&Value::String("Alice Smith".into()))
        );
        assert_eq!(
            result.get("first_name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            result.get("secret"),
            Some(&Value::String("password123".into()))
        );
    }

    #[test]
    fn test_exclude_removes_fields() {
        let input = make_input();
        let emitted = IndexMap::new();

        let config = OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: true,
            include_header: None,
            mapping: None,
            exclude: Some(vec!["secret".into()]),
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert!(result.get("secret").is_none());
        assert!(result.get("first_name").is_some());
    }

    /// The direction contract: the pair is `output_name: source_column`, so
    /// `given_name: first_name` reads `first_name` and writes `given_name`.
    #[test]
    fn test_mapping_renames() {
        let input = make_input();
        let emitted = IndexMap::new();

        let mapping = OutputMapping::new(vec![MappingEntry::rename("given_name", "first_name")]);

        let config = OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: true,
            include_header: None,
            mapping: Some(mapping),
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema: None,
            split: None,
            notes: None,
        };

        let result = project_output(&input, &emitted, &config);
        assert!(result.get("first_name").is_none());
        assert_eq!(
            result.get("given_name"),
            Some(&Value::String("Alice".into()))
        );
    }

    fn correlation_key_input() -> Record {
        use clinker_record::FieldMetadata;
        use clinker_record::SchemaBuilder;
        let schema = SchemaBuilder::new()
            .with_field("id")
            .with_field("name")
            .with_field_meta("$ck.id", FieldMetadata::source_correlation("id"))
            .build();
        Record::new(
            schema,
            vec![
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Integer(1),
            ],
        )
    }

    fn fast_path_output_config(include_correlation_keys: bool) -> OutputConfig {
        OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: false,
            include_header: None,
            mapping: None,
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema: None,
            split: None,
            notes: None,
        }
    }

    #[test]
    fn test_projection_fast_path_strips_engine_stamped_by_default() {
        let input = correlation_key_input();
        let config = fast_path_output_config(false);
        let result = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = result.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id", "name"]);
        assert!(result.get("$ck.id").is_none());
    }

    #[test]
    fn test_projection_fast_path_keeps_engine_stamped_on_opt_in() {
        let input = correlation_key_input();
        let config = fast_path_output_config(true);
        let result = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = result.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id", "name", "$ck.id"]);
        assert_eq!(result.get("$ck.id"), Some(&Value::Integer(1)));
    }

    /// `include_correlation_keys: true` surfaces `$ck.<field>` shadow
    /// columns to the sink but does NOT leak the `$widened` sidecar
    /// absorber. Sidecar expansion is a separate `include_unmapped: true`
    /// concern; routing both through one engine-stamped toggle would
    /// surface the raw `Value::Map` payload as a literal column to
    /// the writer (CSV/XML JSON-encode, fixed-width silently empties),
    /// which is never the user's intent when they opt into CK
    /// visibility. Verified on both the fast path (no rewrite) and
    /// the slow path (rewrite triggered by `include_unmapped: true`).
    #[test]
    fn test_include_correlation_keys_does_not_leak_widened_sidecar() {
        use clinker_record::FieldMetadata;
        use clinker_record::SchemaBuilder;
        let schema = SchemaBuilder::new()
            .with_field("id")
            .with_field("name")
            .with_field_meta("$ck.id", FieldMetadata::source_correlation("id"))
            .with_field_meta("$widened", FieldMetadata::widened_sidecar())
            .build();
        let mut sidecar = IndexMap::new();
        sidecar.insert("extra".into(), Value::String("payload".into()));
        let input = Record::new(
            schema,
            vec![
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Integer(1),
                Value::Map(Box::new(sidecar)),
            ],
        );

        // Fast path: include_correlation_keys=true, include_unmapped=false,
        // no rewrite. Output gets [id, name, $ck.id] — `$widened`
        // dropped, sidecar payload not surfaced.
        let config = fast_path_output_config(true);
        let result = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = result.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(
            cols,
            vec!["id", "name", "$ck.id"],
            "include_correlation_keys must surface $ck.* but never $widened"
        );
        assert!(
            result.get("$widened").is_none(),
            "$widened must not appear on the output schema"
        );
        assert!(
            result.get("extra").is_none(),
            "sidecar payload must not be expanded — that is the include_unmapped flag"
        );

        // Slow path: same flags, but force the rewrite with an `exclude:` of a
        // column this record does not carry. A `mapping:` would force it too,
        // but a mapping under `include_unmapped: false` emits only what it
        // lists, which would conflate this test with the selection semantic it
        // is not about.
        let config_slow = OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: false,
            include_header: None,
            mapping: None,
            exclude: Some(vec!["not_on_this_record".to_string()]),
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys: true,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema: None,
            split: None,
            notes: None,
        };
        let result_slow = project_output_from_record(&input, &config_slow, None);
        let cols_slow: Vec<&str> = result_slow
            .schema()
            .columns()
            .iter()
            .map(|c| &**c)
            .collect();
        assert_eq!(
            cols_slow,
            vec!["id", "name", "$ck.id"],
            "slow path: include_correlation_keys must surface $ck.* but never $widened"
        );
        assert!(
            result_slow.get("$widened").is_none(),
            "slow path: $widened must not appear on the output schema"
        );

        // A `mapping:` under `include_unmapped: false` selects the USER
        // columns; it must not silently defeat an explicit
        // `include_correlation_keys: true`. The shadows are appended after the
        // declared columns, and `$widened` still never appears.
        let mut config_mapped = config_slow.clone();
        config_mapped.exclude = None;
        config_mapped.mapping = Some(OutputMapping::new(vec![MappingEntry::rename(
            "person", "name",
        )]));
        let result_mapped = project_output_from_record(&input, &config_mapped, None);
        let cols_mapped: Vec<&str> = result_mapped
            .schema()
            .columns()
            .iter()
            .map(|c| &**c)
            .collect();
        assert_eq!(
            cols_mapped,
            vec!["person", "$ck.id"],
            "a mapping selects the user columns; include_correlation_keys still governs \
             the correlation shadows"
        );
        assert!(result_mapped.get("$widened").is_none());

        // Same combination with a `cxl_emit_names` list supplied — the term
        // `drop_unmapped` gained when a declared mapping started overriding the
        // emit-name restriction. Without a mapping the restriction would cut
        // this to the emitted names alone; with one, the mapping decides the
        // user columns and the CK shadow still rides the `include_correlation_keys`
        // flag. A regression here silently adds or removes an engine-namespaced
        // column from a delivered file.
        let emit_names = vec!["id".to_string()];
        let result_emit =
            project_output_from_record(&input, &config_mapped, Some(emit_names.as_slice()));
        let cols_emit: Vec<&str> = result_emit
            .schema()
            .columns()
            .iter()
            .map(|c| &**c)
            .collect();
        assert_eq!(
            cols_emit,
            vec!["person", "$ck.id"],
            "a declared mapping overrides the cxl-emit-name restriction for the columns it \
             lists, and include_correlation_keys still governs the shadows"
        );

        // The contrast case that makes the clause load-bearing: drop the
        // mapping and the same emit-name list DOES restrict the output.
        let mut config_no_mapping = config_mapped.clone();
        config_no_mapping.mapping = None;
        let result_no_mapping =
            project_output_from_record(&input, &config_no_mapping, Some(emit_names.as_slice()));
        let cols_no_mapping: Vec<&str> = result_no_mapping
            .schema()
            .columns()
            .iter()
            .map(|c| &**c)
            .collect();
        assert_eq!(
            cols_no_mapping,
            vec!["id"],
            "without a mapping, `include_unmapped: false` restricts to the emitted names"
        );
    }

    /// Column ORDER of the appended passthrough columns, pinned against the
    /// value-move optimisation. Taking a mapped value out of the gathered map
    /// leaves a placeholder rather than removing the slot, precisely so the
    /// remaining columns keep their relative order — `IndexMap::swap_remove`
    /// would move the last column into the hole and reorder the file's header.
    #[test]
    fn passthrough_columns_keep_their_relative_order_around_a_mapped_column() {
        let schema = SchemaBuilder::new()
            .with_field("a")
            .with_field("b")
            .with_field("c")
            .with_field("d")
            .build();
        let input = Record::new(
            schema,
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
            ],
        );
        let mut config = fast_path_output_config(false);
        config.include_unmapped = true;
        // Claim a column from the MIDDLE: a swap-remove would pull `d` into
        // `b`'s slot and emit `a, d, c` instead of `a, c, d`.
        config.mapping = Some(OutputMapping::new(vec![MappingEntry::rename("bee", "b")]));

        let out = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = out.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["bee", "a", "c", "d"]);
        assert_eq!(out.get("bee"), Some(&Value::Integer(2)));
        assert_eq!(out.get("c"), Some(&Value::Integer(3)));
        assert_eq!(out.get("d"), Some(&Value::Integer(4)));
    }

    /// One source column feeding two output columns: the earlier reader copies,
    /// the last reader takes. Both must carry the real value — a take-then-read
    /// ordering bug would serve the second column a `Null`.
    #[test]
    fn a_source_read_twice_delivers_its_value_to_both_output_columns() {
        let schema = SchemaBuilder::new().with_field("sku").build();
        let input = Record::new(schema, vec![Value::String("A-1".into())]);
        let mut config = fast_path_output_config(false);
        config.include_unmapped = false;
        config.mapping = Some(OutputMapping::new(vec![
            MappingEntry::passthrough("sku"),
            MappingEntry::rename("item_code", "sku"),
        ]));

        let out = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = out.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["sku", "item_code"]);
        assert_eq!(out.get("sku"), Some(&Value::String("A-1".into())));
        assert_eq!(out.get("item_code"), Some(&Value::String("A-1".into())));
    }

    /// An output name that an appended passthrough column would duplicate: the
    /// mapped column wins and the passthrough is dropped. Two same-named columns
    /// on one schema resolve last-write-wins through `Record::get`, so appending
    /// it would serve the passthrough's value under the mapped header.
    #[test]
    fn a_passthrough_column_never_shadows_a_mapped_output_name() {
        let schema = SchemaBuilder::new()
            .with_field("order_id")
            .with_field("customer")
            .with_field("sold_to")
            .build();
        let input = Record::new(
            schema,
            vec![
                Value::Integer(7),
                Value::String("the-right-value".into()),
                Value::String("the-stale-value".into()),
            ],
        );
        let mut config = fast_path_output_config(false);
        config.include_unmapped = true;
        config.mapping = Some(OutputMapping::new(vec![MappingEntry::rename(
            "sold_to", "customer",
        )]));

        let out = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = out.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(
            cols,
            vec!["sold_to", "order_id"],
            "the upstream `sold_to` must not be appended beside the mapped one"
        );
        assert_eq!(
            out.get("sold_to"),
            Some(&Value::String("the-right-value".into())),
            "the mapped value must be what the column resolves to"
        );
    }

    /// The write-boundary half of E365: a mapping entry whose column the stream
    /// never supplied leaves its output name off the established schema, and
    /// that is refused rather than written as a file missing a column.
    #[test]
    fn missing_mapping_output_is_reported_against_an_established_schema() {
        let mut config = fast_path_output_config(false);
        config.include_unmapped = false;
        config.mapping = Some(OutputMapping::new(vec![
            MappingEntry::passthrough("id"),
            MappingEntry::rename("given_name", "firstname"),
        ]));
        let established = Schema::new(vec!["id".into()]);

        assert_eq!(
            missing_mapping_outputs(&established, &config),
            vec!["given_name".to_string()]
        );
        let err = check_mapping_against_schema(&established, &config, "out")
            .expect_err("an unresolved entry must be refused");
        let rendered = err.to_string();
        assert!(rendered.contains("E365"), "{rendered}");
        assert!(
            rendered.contains("'firstname'"),
            "the message names the SOURCE column the author has to correct: {rendered}"
        );
    }

    /// Over-rejection guard for the same check: a fully covered schema passes.
    #[test]
    fn a_fully_resolved_mapping_passes_the_write_boundary_check() {
        let mut config = fast_path_output_config(false);
        config.mapping = Some(OutputMapping::new(vec![MappingEntry::rename(
            "given_name",
            "first_name",
        )]));
        let established = Schema::new(vec!["given_name".into(), "id".into()]);
        assert!(missing_mapping_outputs(&established, &config).is_empty());
        assert!(check_mapping_against_schema(&established, &config, "out").is_ok());
    }

    /// `include_unmapped: true` expands the sidecar map even when
    /// `include_correlation_keys: false`. The two flags are
    /// independent: each gates a distinct engine-stamped surface.
    #[test]
    fn test_include_unmapped_expands_independently_of_correlation_keys() {
        use clinker_record::FieldMetadata;
        use clinker_record::SchemaBuilder;
        let schema = SchemaBuilder::new()
            .with_field("id")
            .with_field_meta("$ck.id", FieldMetadata::source_correlation("id"))
            .with_field_meta("$widened", FieldMetadata::widened_sidecar())
            .build();
        let mut sidecar = IndexMap::new();
        sidecar.insert("extra".into(), Value::String("payload".into()));
        let input = Record::new(
            schema,
            vec![
                Value::Integer(7),
                Value::Integer(7),
                Value::Map(Box::new(sidecar)),
            ],
        );
        let config = OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: true,
            include_header: None,
            mapping: None,
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema: None,
            split: None,
            notes: None,
        };
        let result = project_output_from_record(&input, &config, None);
        // Output: id (declared) + extra (expanded sidecar). $ck.id is
        // dropped because include_correlation_keys is false; $widened
        // slot is stripped before the map expansion.
        let cols: Vec<&str> = result.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["id", "extra"]);
        assert_eq!(
            result.get("extra"),
            Some(&Value::String("payload".into())),
            "sidecar map's `extra` key must expand to a top-level field"
        );
        assert!(
            result.get("$ck.id").is_none(),
            "include_correlation_keys: false must drop $ck.* even when include_unmapped: true"
        );
        assert!(
            result.get("$widened").is_none(),
            "$widened slot must be stripped after expansion"
        );
    }

    #[test]
    fn fast_path_carries_doc_context_forward() {
        use clinker_record::{DocumentContext, DocumentId};
        use indexmap::IndexMap as RecIndexMap;

        // The default Output config (no mapping/exclude, include_unmapped
        // false, cxl_emit_names None) takes the fast path. A document-
        // reconstructing writer downstream — e.g. EDIFACT
        // `interchange_from_doc` echoing the source `UNB` — must still
        // resolve `$doc.<section>.<field>` on the projected record, so the
        // fast path has to carry the envelope context forward.
        let schema = Arc::new(Schema::new(vec!["seg_id".into(), "e01".into()]));
        let mut input = Record::new(
            schema,
            vec![Value::String("BGM".into()), Value::String("220".into())],
        );
        let mut unb: RecIndexMap<Box<str>, Value> = RecIndexMap::new();
        unb.insert("e01".into(), Value::String("UNOA:1".into()));
        let mut sections: RecIndexMap<Box<str>, Value> = RecIndexMap::new();
        sections.insert("unb".into(), Value::Map(Box::new(unb)));
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("orders.edi"),
            clinker_record::EnvelopeRecord::from_sections(sections),
        ));
        input.set_doc_ctx(Arc::clone(&ctx));

        let config = OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: false,
            include_header: None,
            mapping: None,
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema: None,
            split: None,
            notes: None,
        };

        // cxl_emit_names None keeps drop_unmapped false, so needs_rewrite
        // is false and the fast path runs.
        let result = project_output_from_record(&input, &config, None);
        assert_eq!(
            result.doc_ctx().get_section_field("unb", "e01"),
            Some(Value::String("UNOA:1".into())),
            "fast path must carry the source document context forward"
        );
    }

    // ── Output-column decimal `scale` enforcement (write boundary) ──

    use clinker_format::{Column, SourceSchema};
    use rust_decimal::Decimal;

    /// A minimal fast-path Output config (no mapping/exclude, no correlation
    /// keys) carrying an optional output `schema:`. `cxl_emit_names = None`
    /// keeps every user field, so a declared decimal column reaches the
    /// rounding pass.
    fn scale_config(schema: Option<SourceSchema>) -> OutputConfig {
        OutputConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            include_unmapped: false,
            include_header: None,
            mapping: None,
            exclude: None,
            sort_order: None,
            preserve_nulls: None,
            include_correlation_keys: false,
            correlation_fanout_policy: None,
            if_exists: Default::default(),
            unique_suffix_width: 0,
            write_meta: false,
            reconstruct_envelope: false,
            join_values: None,
            schema,
            split: None,
            notes: None,
        }
    }

    fn decimal_col(name: &str, scale: Option<u8>) -> Column {
        Column {
            scale,
            ..Column::bare(name, Type::Decimal)
        }
    }

    fn one_field_record(name: &str, value: Value) -> Record {
        let schema = Arc::new(Schema::new(vec![name.into()]));
        Record::new(schema, vec![value])
    }

    /// A full-precision computed quotient (4 / 3 keeps 28 digits) is rescaled to
    /// the declared output-column scale with banker's rounding: `1.33`.
    #[test]
    fn output_scale_rounds_computed_decimal() {
        let quotient = Decimal::from(4)
            .checked_div(Decimal::from(3))
            .expect("4 / 3");
        let input = one_field_record("average", Value::Decimal(quotient));
        let schema = SourceSchema::Columns(vec![decimal_col("average", Some(2))]);
        let out = project_output_from_record(&input, &scale_config(Some(schema)), None);
        assert_eq!(
            out.get("average"),
            Some(&Value::Decimal(Decimal::new(133, 2)))
        );
    }

    /// A `multiple: true` column arrives as an array whose declared `type:` and
    /// `scale:` describe each ELEMENT, so the scale applies element-wise —
    /// otherwise a multi-value decimal column is the one place a declared scale
    /// is silently not honored, writing full internal precision into the sink.
    /// Non-decimal elements pass through, matching the scalar arm.
    #[test]
    fn output_scale_rounds_each_element_of_a_multi_value_column() {
        let quotient = Decimal::from(4)
            .checked_div(Decimal::from(3))
            .expect("4 / 3");
        let input = one_field_record(
            "amounts",
            Value::Array(vec![
                Value::Decimal(quotient),
                Value::Decimal(Decimal::new(2125, 3)),
                Value::String("n/a".into()),
            ]),
        );
        let schema = SourceSchema::Columns(vec![Column {
            multiple: Some(true),
            ..decimal_col("amounts", Some(2))
        }]);
        let out = project_output_from_record(&input, &scale_config(Some(schema)), None);
        assert_eq!(
            out.get("amounts"),
            Some(&Value::Array(vec![
                Value::Decimal(Decimal::new(133, 2)),
                Value::Decimal(Decimal::new(212, 2)),
                Value::String("n/a".into()),
            ]))
        );
    }

    /// The write boundary uses the same round-half-to-even as ingest: a `.xx5`
    /// midpoint rounds to the even neighbor (`2.125` → `2.12`), not away from
    /// zero.
    #[test]
    fn output_scale_uses_bankers_midpoint() {
        let input = one_field_record("m", Value::Decimal(Decimal::new(2125, 3)));
        let schema = SourceSchema::Columns(vec![decimal_col("m", Some(2))]);
        let out = project_output_from_record(&input, &scale_config(Some(schema)), None);
        assert_eq!(out.get("m"), Some(&Value::Decimal(Decimal::new(212, 2))));

        let input = one_field_record("m", Value::Decimal(Decimal::new(2135, 3)));
        let schema = SourceSchema::Columns(vec![decimal_col("m", Some(2))]);
        let out = project_output_from_record(&input, &scale_config(Some(schema)), None);
        assert_eq!(out.get("m"), Some(&Value::Decimal(Decimal::new(214, 2))));
    }

    /// The write boundary only rounds off excess precision; it never pads a
    /// shorter-scale value up. This is bit-identical to ingest (`2.5` coerced
    /// into a `scale: 2` source column also stays `2.5`) — both edges route
    /// through the same `round_decimal_to_scale`. Decimal equality ignores
    /// scale, so the string form is the load-bearing assertion.
    #[test]
    fn output_scale_does_not_pad_shorter_scale() {
        let input = one_field_record("v", Value::Decimal(Decimal::new(25, 1)));
        let schema = SourceSchema::Columns(vec![decimal_col("v", Some(2))]);
        let out = project_output_from_record(&input, &scale_config(Some(schema)), None);
        let Some(Value::Decimal(d)) = out.get("v") else {
            panic!("expected a decimal");
        };
        assert_eq!(
            d.to_string(),
            "2.5",
            "already within scale — left untouched"
        );
    }

    /// A `decimal` column with no declared `scale` keeps full precision — the
    /// contract only fires where the user declared a scale.
    #[test]
    fn output_decimal_without_scale_keeps_full_precision() {
        let quotient = Decimal::from(4)
            .checked_div(Decimal::from(3))
            .expect("4 / 3");
        let input = one_field_record("average", Value::Decimal(quotient));
        let schema = SourceSchema::Columns(vec![decimal_col("average", None)]);
        let out = project_output_from_record(&input, &scale_config(Some(schema)), None);
        assert_eq!(out.get("average"), Some(&Value::Decimal(quotient)));
    }

    /// A schema-less output (CSV/JSON without a `schema:` block) never rounds —
    /// today's full-precision behavior is preserved.
    #[test]
    fn output_without_schema_keeps_full_precision() {
        let quotient = Decimal::from(4)
            .checked_div(Decimal::from(3))
            .expect("4 / 3");
        let input = one_field_record("average", Value::Decimal(quotient));
        let out = project_output_from_record(&input, &scale_config(None), None);
        assert_eq!(out.get("average"), Some(&Value::Decimal(quotient)));
    }

    /// Scope guard: only a `Value::Decimal` in a decimal-declared column is
    /// touched. A `Value::Float` that lands in such a column is NOT coerced —
    /// write-side type coercion is deliberately out of scope.
    #[test]
    fn output_scale_leaves_non_decimal_value_untouched() {
        let input = one_field_record("average", Value::Float(1.3333));
        let schema = SourceSchema::Columns(vec![decimal_col("average", Some(2))]);
        let out = project_output_from_record(&input, &scale_config(Some(schema)), None);
        assert_eq!(out.get("average"), Some(&Value::Float(1.3333)));
    }

    /// The rounding pass runs on the slow path too (mapping/exclude rewrite),
    /// keyed by the post-mapping output name: a field renamed to a scaled
    /// decimal column is rescaled.
    #[test]
    fn output_scale_applies_after_mapping_rename() {
        let quotient = Decimal::from(4)
            .checked_div(Decimal::from(3))
            .expect("4 / 3");
        let input = one_field_record("raw_avg", Value::Decimal(quotient));
        let mut config = scale_config(Some(SourceSchema::Columns(vec![decimal_col(
            "average",
            Some(2),
        )])));
        config.mapping = Some(OutputMapping::new(vec![MappingEntry::rename(
            "average", "raw_avg",
        )]));
        let out = project_output_from_record(&input, &config, None);
        assert!(out.get("raw_avg").is_none(), "field was renamed");
        assert_eq!(
            out.get("average"),
            Some(&Value::Decimal(Decimal::new(133, 2)))
        );
    }

    /// A nullable decimal column (`type: { nullable: decimal }`) still rounds —
    /// the underlying type is unwrapped before the decimal check, matching how
    /// the writers classify a nullable numeric field.
    #[test]
    fn output_scale_rounds_nullable_decimal_column() {
        let quotient = Decimal::from(4)
            .checked_div(Decimal::from(3))
            .expect("4 / 3");
        let input = one_field_record("average", Value::Decimal(quotient));
        let col = Column {
            scale: Some(2),
            ..Column::bare("average", Type::nullable(Type::Decimal))
        };
        let out = project_output_from_record(
            &input,
            &scale_config(Some(SourceSchema::Columns(vec![col]))),
            None,
        );
        assert_eq!(
            out.get("average"),
            Some(&Value::Decimal(Decimal::new(133, 2)))
        );
    }
}
