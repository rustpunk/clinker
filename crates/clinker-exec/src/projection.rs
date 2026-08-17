use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{Record, SchemaBuilder, Value, round_decimal_to_scale};
use cxl::typecheck::Type;
use indexmap::IndexMap;

use clinker_plan::config::SinkConfig;

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
    config: &SinkConfig,
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
    config: &SinkConfig,
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
    config: &SinkConfig,
    cxl_emit_names: Option<&[String]>,
) -> Record {
    project_output_probed(input_record, config, cxl_emit_names, None)
}

/// [`project_output_from_record`] with the per-Sink [`MappingProbe`] the
/// end-of-stream report is built from.
///
/// Every per-record projection for one Output must feed the SAME probe, or an
/// entry that resolved only on the records routed elsewhere reads as never
/// resolved. Callers with no stream-scoped home for one pass `None`; those are
/// the ad-hoc and test projections, which produce no report.
pub fn project_output_probed(
    input_record: &Record,
    config: &SinkConfig,
    cxl_emit_names: Option<&[String]>,
    mut probe: Option<&mut MappingProbe>,
) -> Record {
    if let Some(probe) = probe.as_deref_mut() {
        probe.note_record();
    }
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
        // Engine-stamped columns are dropped unless the Sink node
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
            // Order-preserving. `swap_remove` moves the map's last entry into
            // the hole, so `a,b,c,d` minus `b` would emit `a,d,c` and break the
            // relative order the passthrough columns are documented to keep.
            // `exclude:` lists are author-sized, so the shift is cheap.
            fields.shift_remove(name.as_str());
        }
    }

    // `mapping:` is the ordered declaration of the columns this output
    // carries: every listed column, in declaration order, under its
    // declared name, then — when `include_unmapped` is set — everything
    // the block did not claim, in its existing relative order.
    //
    // A listed column absent from THIS record is written as `Value::Null`,
    // not skipped. That is what makes the output schema a function of the
    // config alone rather than of whichever record happened to arrive
    // first: `mapping:` states which columns the file carries and in what
    // order, and a column that vanishes on some rows contradicts both.
    // Heterogeneous streams — an `auto_widen` sidecar, a multi-record-type
    // source, a composition body's open row — are exactly the shapes where
    // a per-record column set would otherwise leak into the file's shape.
    //
    // Per-record cost: one lookup per listed column plus one `claims_*`
    // lookup per surviving column. Every index the loop consults is
    // resolved once when the config is parsed, and a listed column's value
    // is MOVED out of `fields` rather than copied — the slot is left
    // holding a `Null` placeholder that the append loop below never
    // reaches, because it skips claimed sources. Only a source feeding two
    // output columns is cloned, and only for the readers before the last.
    // So this allocates nothing per record beyond the output record itself.
    //
    // The placeholder trick is what keeps the append order intact:
    // removing the slot outright would need `swap_remove` (which reorders
    // the passthrough columns) or a shift per listed column.
    let (names, values): (Vec<Box<str>>, Vec<Value>) = match config.mapping.as_ref() {
        Some(mapping) => {
            let mut names: Vec<Box<str>> = Vec::with_capacity(mapping.entries().len());
            let mut values: Vec<Value> = Vec::with_capacity(mapping.entries().len());
            for (index, entry) in mapping.entries().iter().enumerate() {
                let value = match fields.get_mut(entry.source.as_str()) {
                    Some(slot) => {
                        if let Some(probe) = probe.as_deref_mut() {
                            probe.note_resolved(index);
                        }
                        if mapping.is_last_reader(index) {
                            std::mem::replace(slot, Value::Null)
                        } else {
                            slot.clone()
                        }
                    }
                    // Absent on this row. The column is still written, empty,
                    // so the file's shape does not depend on the data. An entry
                    // that resolves on NO record is a typo rather than a sparse
                    // column, and the probe's end-of-stream report says so.
                    None => Value::Null,
                };
                names.push(Box::<str>::from(entry.output.as_str()));
                values.push(value);
            }
            if config.include_unmapped {
                for (name, value) in fields {
                    if mapping.claims_source(name.as_str()) {
                        continue;
                    }
                    // Collision guard: a passthrough column whose name the block
                    // already writes must not be appended beside it.
                    // `SchemaBuilder` does not dedupe and the schema's name
                    // index is last-write-wins, so the appended column would
                    // answer `Record::get` for the mapped one and serve its
                    // value under the mapped header. The mapping is the author's
                    // explicit statement, so the mapped column wins — but the
                    // displaced one is recorded, because dropping real upstream
                    // data silently is the thing this must not do. The plan gate
                    // catches this where it can enumerate the upstream columns;
                    // open rows and sidecar-expanded columns reach here
                    // unchecked, so the guard is not redundant.
                    if mapping.claims_output(name.as_str()) {
                        if let Some(probe) = probe.as_deref_mut() {
                            probe.note_shadowed(name.as_str());
                        }
                        continue;
                    }
                    names.push(Box::<str>::from(name.as_str()));
                    values.push(value);
                }
            } else {
                // `include_unmapped: false` drops the unlisted USER columns.
                // The engine-stamped correlation shadows are governed by their
                // own flag, so a mapping must not silently defeat an explicit
                // `include_correlation_keys: true` — they are appended after the
                // declared columns instead.
                //
                // Selected by FieldMetadata, never by a name prefix: `$` is not
                // reserved on the input side, so a source column named `$id` or
                // `$schema` (ordinary in JSON-Schema and MongoDB-shaped
                // payloads) is a user column and must stay dropped here. The
                // record's correlation fields sit after its user fields in
                // schema order, which is the order `fields` was gathered in, so
                // walking them directly preserves the append order.
                for (name, _) in input_record.iter_correlation_fields() {
                    if mapping.claims_source(name) {
                        continue;
                    }
                    if mapping.claims_output(name) {
                        if let Some(probe) = probe.as_deref_mut() {
                            probe.note_shadowed(name);
                        }
                        continue;
                    }
                    // `exclude:` may have removed it, and a CK column is present
                    // in `fields` only when `include_correlation_keys` admitted
                    // it in the first place. `swap_remove` is safe here where it
                    // was not above: the append order comes from the record's
                    // schema walk, not from `fields`, and `fields` is dead after
                    // this loop.
                    let Some(value) = fields.swap_remove(name) else {
                        continue;
                    };
                    names.push(Box::<str>::from(name));
                    values.push(value);
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

/// Project one record while staging its mapping evidence until the caller
/// knows whether the writer accepted it.
///
/// The scratch flags live on `probe` and are reused for every record, so this
/// adds no record-rate allocation and does not repeat the mapping lookups the
/// projection already performs. Call exactly one of
/// [`MappingProbe::commit_staged_record`] or
/// [`MappingProbe::discard_staged_record`] before staging another record.
pub(crate) fn project_output_staged(
    input_record: &Record,
    config: &SinkConfig,
    cxl_emit_names: Option<&[String]>,
    probe: &mut MappingProbe,
) -> Record {
    probe.begin_staged_record();
    project_output_probed(input_record, config, cxl_emit_names, Some(probe))
}

/// Per-Output evidence about how a `mapping:` block resolved over a whole
/// stream, and the source of its end-of-stream report.
///
/// Exists because absence is a property of the *stream*, not of the output
/// schema. Every record now carries every declared column (an unresolved entry
/// writes `Value::Null`), so the schema can no longer answer "did this entry
/// ever find its column" — and the schema was the wrong place to ask anyway: it
/// is derived from the first record on every arm except the buffered CSV union,
/// and a first-record-derived answer is either too strict (a legitimately sparse
/// column aborts the run) or too loose (a column absent from every LATER record
/// goes unreported).
///
/// Asking over the whole stream is precise instead. A typo is supplied by no
/// record, so it reports; a sparse column in a heterogeneous stream is supplied
/// by some record, so it does not.
///
/// Bounded: fixed vectors sized by the author's column count, allocated once
/// per Output. Nothing here grows with input cardinality, and the per-record
/// path only clears and sets reusable flags.
///
/// Self-describing on purpose: it carries the source names it was built from
/// rather than re-reading them out of an [`SinkConfig`] at report time. The
/// report is produced after the dispatch walk, where the only handle left is
/// the Output's name, and matching that name back to a config is a lookup that
/// can miss — a miss would report one Output's entries under another's name.
#[derive(Debug, Clone)]
pub struct MappingProbe {
    /// The source column each mapping entry reads, in declaration order, and
    /// whether that source resolved on some record. Empty for an Output with
    /// no `mapping:` block.
    sources: Vec<Box<str>>,
    outputs: Vec<Box<str>>,
    resolved: Vec<bool>,
    /// Whether each mapping output name displaced an upstream passthrough.
    /// Parallel to `outputs`; findings preserve mapping declaration order.
    shadowed: Vec<bool>,
    /// Records projected through this Output. Zero means the stream was empty,
    /// which is not evidence of anything.
    records: u64,
    /// Fixed per-Sink scratch used when projection precedes a fallible write.
    /// These vectors are allocated with the probe, then cleared and reused for
    /// every record; they never grow with input cardinality.
    staged_resolved: Vec<bool>,
    staged_shadowed: Vec<bool>,
    staging: bool,
}

impl MappingProbe {
    /// A probe sized for `config`'s mapping block. Cheap for an Output with no
    /// `mapping:` — it allocates nothing and reports nothing.
    pub fn for_config(config: &SinkConfig) -> Self {
        let (sources, outputs): (Vec<Box<str>>, Vec<Box<str>>) =
            config.mapping.as_ref().map_or_else(
                || (Vec::new(), Vec::new()),
                |m| {
                    m.entries()
                        .iter()
                        .map(|e| {
                            (
                                Box::<str>::from(e.source.as_str()),
                                Box::<str>::from(e.output.as_str()),
                            )
                        })
                        .unzip()
                },
            );
        Self {
            resolved: vec![false; sources.len()],
            shadowed: vec![false; outputs.len()],
            staged_resolved: vec![false; sources.len()],
            staged_shadowed: vec![false; outputs.len()],
            sources,
            outputs,
            records: 0,
            staging: false,
        }
    }

    fn note_record(&mut self) {
        if !self.staging {
            self.records = self.records.saturating_add(1);
        }
    }

    fn note_resolved(&mut self, index: usize) {
        let slots = if self.staging {
            &mut self.staged_resolved
        } else {
            &mut self.resolved
        };
        if let Some(slot) = slots.get_mut(index) {
            *slot = true;
        }
    }

    fn note_shadowed(&mut self, column: &str) {
        let Some(index) = self.outputs.iter().position(|name| &**name == column) else {
            return;
        };
        let slots = if self.staging {
            &mut self.staged_shadowed
        } else {
            &mut self.shadowed
        };
        if let Some(slot) = slots.get_mut(index) {
            *slot = true;
        }
    }

    fn begin_staged_record(&mut self) {
        debug_assert!(
            !self.staging,
            "previous mapping observation was not finished"
        );
        self.staged_resolved.fill(false);
        self.staged_shadowed.fill(false);
        self.staging = true;
    }

    /// Commit the evidence staged by [`project_output_staged`] after a
    /// successful write.
    pub(crate) fn commit_staged_record(&mut self) {
        debug_assert!(self.staging, "no mapping observation is staged");
        if !self.staging {
            return;
        }
        self.records = self.records.saturating_add(1);
        for (resolved, staged) in self.resolved.iter_mut().zip(&self.staged_resolved) {
            *resolved |= *staged;
        }
        for (shadowed, staged) in self.shadowed.iter_mut().zip(&self.staged_shadowed) {
            *shadowed |= *staged;
        }
        self.staging = false;
    }

    /// Drop the evidence staged by [`project_output_staged`] after a rejected
    /// or dead-lettered write.
    pub(crate) fn discard_staged_record(&mut self) {
        debug_assert!(self.staging, "no mapping observation is staged");
        self.staging = false;
    }

    /// Observe one record without rebuilding its projected output.
    ///
    /// The correlation-buffer path projects before it knows whether the group
    /// will commit. It calls this only for clean groups at commit time so a
    /// record later rejected to the DLQ cannot make an all-empty written
    /// column look populated. The checks mirror the gather/exclude/mapping
    /// visibility rules in [`project_output_probed`] and allocate no
    /// per-record state.
    pub(crate) fn observe_committed_record(&mut self, record: &Record, config: &SinkConfig) {
        self.note_record();
        let Some(mapping) = config.mapping.as_ref() else {
            return;
        };

        for (index, entry) in mapping.entries().iter().enumerate() {
            if projection_field_is_present(record, config, &entry.source) {
                self.note_resolved(index);
            }
        }

        for entry in mapping.entries() {
            let name = entry.output.as_str();
            if mapping.claims_source(name) {
                continue;
            }
            let would_be_appended = if config.include_unmapped {
                projection_field_is_present(record, config, name)
            } else {
                correlation_field_is_present(record, config, name)
            };
            if would_be_appended {
                self.note_shadowed(name);
            }
        }
    }

    /// Fold another probe for the same Output into this one. Used where an
    /// Output's records are projected off the dispatcher's thread — the
    /// streaming arm accumulates locally and folds back once joined.
    ///
    /// Both sides are built from the same Output's block, so their entry
    /// vectors agree; `zip` stopping at the shorter one is the safe reading of
    /// a disagreement, not a silent truncation of a real signal.
    pub fn merge(&mut self, other: &MappingProbe) {
        self.records = self.records.saturating_add(other.records);
        for (slot, hit) in self.resolved.iter_mut().zip(other.resolved.iter()) {
            *slot |= *hit;
        }
        for (slot, hit) in self.shadowed.iter_mut().zip(other.shadowed.iter()) {
            *slot |= *hit;
        }
    }

    /// The advisory findings for one Output, empty when the block resolved
    /// cleanly.
    ///
    /// Advisory, never fatal: by the time a stream ends its sibling Outputs have
    /// written, so aborting here would leave a half-written run behind for a
    /// fault that is visible in the file itself.
    pub fn findings(&self, output_name: &str) -> Vec<String> {
        let mut out = Vec::new();
        // An empty stream resolves nothing and proves nothing.
        if self.records == 0 {
            return out;
        }
        // Duplicated source names collapse: two entries reading the same missing
        // column are one thing for the author to fix.
        let mut unresolved: Vec<&str> = Vec::new();
        for (index, source) in self.sources.iter().enumerate() {
            if !self.resolved[index] && !unresolved.contains(&&**source) {
                unresolved.push(source);
            }
        }
        if !unresolved.is_empty() {
            let listed = unresolved
                .iter()
                .map(|c| format!("'{c}'"))
                .collect::<Vec<_>>()
                .join(", ");
            out.push(format!(
                "W365 output '{output_name}': `mapping:` reads column(s) {listed}, which no \
                 record carried — those output columns are empty in every row. Check the \
                 spelling, or remove the item if the column is gone from upstream"
            ));
        }
        let shadowed: Vec<&str> = self
            .outputs
            .iter()
            .zip(&self.shadowed)
            .filter_map(|(name, hit)| hit.then_some(&**name))
            .collect();
        if !shadowed.is_empty() {
            let listed = shadowed
                .iter()
                .map(|c| format!("'{c}'"))
                .collect::<Vec<_>>()
                .join(", ");
            out.push(format!(
                "W366 output '{output_name}': upstream column(s) {listed} were dropped because \
                 `mapping:` writes an output column of the same name; the mapped value is what \
                 the file carries. Rename the mapped column, or add the upstream name to this \
                 output's `exclude:` to state the intent"
            ));
        }
        out
    }
}

/// Whether `name` is present in the temporary field map built by the output
/// projection after its visibility and `exclude:` rules run.
fn projection_field_is_present(record: &Record, config: &SinkConfig, name: &str) -> bool {
    if config
        .exclude
        .as_ref()
        .is_some_and(|excluded| excluded.iter().any(|column| column == name))
    {
        return false;
    }

    if let Some(index) = record.schema().index(name) {
        use clinker_record::FieldMetadata;
        match record.schema().field_metadata(index) {
            None => return true,
            Some(
                FieldMetadata::SourceCorrelation { .. } | FieldMetadata::AggregateGroupIndex { .. },
            ) => return config.include_correlation_keys,
            Some(_) => {}
        }
    }

    if config.include_unmapped
        && let Some(Value::Map(sidecar)) =
            record.get(clinker_plan::config::pipeline_node::WIDENED_SIDECAR_COLUMN)
    {
        return sidecar.contains_key(name);
    }

    false
}

/// Under `include_unmapped: false`, only correlation columns can be appended
/// after the declared mapping, and only when the author explicitly opts in.
fn correlation_field_is_present(record: &Record, config: &SinkConfig, name: &str) -> bool {
    if !config.include_correlation_keys
        || config
            .exclude
            .as_ref()
            .is_some_and(|excluded| excluded.iter().any(|column| column == name))
    {
        return false;
    }
    let Some(index) = record.schema().index(name) else {
        return false;
    };
    matches!(
        record.schema().field_metadata(index),
        Some(
            clinker_record::FieldMetadata::SourceCorrelation { .. }
                | clinker_record::FieldMetadata::AggregateGroupIndex { .. }
        )
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
fn round_declared_output_decimals(record: &mut Record, config: &SinkConfig) {
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

        let config = SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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

        let config = SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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

        let config = SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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

    fn fast_path_output_config(include_correlation_keys: bool) -> SinkConfig {
        SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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
        let config_slow = SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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

        // `exclude:` removes from the same map and must preserve order too. A
        // `swap_remove` here would pull `d` into `b`'s slot and emit `x,d,c`.
        let mut excluding = fast_path_output_config(false);
        excluding.include_unmapped = true;
        excluding.exclude = Some(vec!["b".to_string()]);
        excluding.mapping = Some(OutputMapping::new(vec![MappingEntry::rename("x", "a")]));
        let out = project_output_from_record(&input, &excluding, None);
        let cols: Vec<&str> = out.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(cols, vec!["x", "c", "d"]);
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

    fn one_field(name: &str, value: Value) -> Record {
        Record::new(SchemaBuilder::new().with_field(name).build(), vec![value])
    }

    /// The core of the redesign: a record missing a mapped source still carries
    /// the declared column, empty. The output schema is a function of the
    /// config, not of whichever record arrived first.
    #[test]
    fn an_absent_mapping_source_yields_a_null_column_not_a_missing_one() {
        let mut config = fast_path_output_config(false);
        config.include_unmapped = false;
        config.mapping = Some(OutputMapping::new(vec![
            MappingEntry::passthrough("id"),
            MappingEntry::rename("given_name", "first_name"),
        ]));

        // A record carrying both, and a record carrying only `id` — the shape a
        // heterogeneous stream produces.
        let full = Record::new(
            SchemaBuilder::new()
                .with_field("id")
                .with_field("first_name")
                .build(),
            vec![Value::Integer(1), Value::String("Alice".into())],
        );
        let sparse = one_field("id", Value::Integer(2));

        let a = project_output_from_record(&full, &config, None);
        let b = project_output_from_record(&sparse, &config, None);

        let cols = |r: &Record| -> Vec<String> {
            r.schema().columns().iter().map(|c| c.to_string()).collect()
        };
        assert_eq!(cols(&a), vec!["id", "given_name"]);
        assert_eq!(
            cols(&b),
            cols(&a),
            "every record must project the same declared column set, whatever it carries"
        );
        assert_eq!(b.get("given_name"), Some(&Value::Null));
    }

    /// The probe distinguishes a typo from a sparse column. `first_name`
    /// resolved on one of the two records, so it is not reported; `nickname`
    /// resolved on neither, so it is.
    #[test]
    fn the_probe_reports_only_entries_no_record_resolved() {
        let mut config = fast_path_output_config(false);
        config.include_unmapped = false;
        config.mapping = Some(OutputMapping::new(vec![
            MappingEntry::passthrough("first_name"),
            MappingEntry::rename("goes_by", "nickname"),
        ]));

        let mut probe = MappingProbe::for_config(&config);
        let with_name = one_field("first_name", Value::String("Alice".into()));
        let without = one_field("other", Value::Integer(1));
        project_output_probed(&without, &config, None, Some(&mut probe));
        project_output_probed(&with_name, &config, None, Some(&mut probe));

        let findings = probe.findings("out");
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].contains("W365"), "{}", findings[0]);
        assert!(
            findings[0].contains("'nickname'"),
            "names the unresolved source column: {}",
            findings[0]
        );
        assert!(
            !findings[0].contains("'first_name'"),
            "a column some record carried is sparse, not a typo: {}",
            findings[0]
        );
    }

    /// An empty stream resolves nothing and proves nothing — it must not warn.
    #[test]
    fn the_probe_stays_silent_on_an_empty_stream() {
        let mut config = fast_path_output_config(false);
        config.mapping = Some(OutputMapping::new(vec![MappingEntry::rename(
            "goes_by", "nickname",
        )]));
        let probe = MappingProbe::for_config(&config);
        assert!(probe.findings("out").is_empty());
    }

    /// The displaced passthrough is reported rather than dropped in silence.
    #[test]
    fn the_probe_reports_a_passthrough_the_mapping_displaced() {
        let mut config = fast_path_output_config(false);
        config.include_unmapped = true;
        config.mapping = Some(OutputMapping::new(vec![MappingEntry::rename(
            "sold_to", "customer",
        )]));
        let input = Record::new(
            SchemaBuilder::new()
                .with_field("customer")
                .with_field("sold_to")
                .build(),
            vec![Value::String("right".into()), Value::String("stale".into())],
        );

        let mut probe = MappingProbe::for_config(&config);
        let out = project_output_probed(&input, &config, None, Some(&mut probe));
        assert_eq!(out.get("sold_to"), Some(&Value::String("right".into())));

        let findings = probe.findings("out");
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].contains("W366"), "{}", findings[0]);
        assert!(findings[0].contains("'sold_to'"), "{}", findings[0]);
    }

    /// A user column named `$id` — ordinary in JSON-Schema and MongoDB-shaped
    /// payloads — is a user column, not an engine one, so `include_unmapped:
    /// false` drops it like any other unlisted column. Engine-stamped fields
    /// are selected by metadata; a `$` prefix proves nothing.
    #[test]
    fn a_dollar_prefixed_user_column_is_not_treated_as_engine_stamped() {
        use clinker_record::FieldMetadata;
        let schema = SchemaBuilder::new()
            .with_field("$id")
            .with_field("name")
            .with_field_meta("$ck.name", FieldMetadata::source_correlation("name"))
            .build();
        let input = Record::new(
            schema,
            vec![
                Value::String("doc-1".into()),
                Value::String("Alice".into()),
                Value::String("Alice".into()),
            ],
        );
        let mut config = fast_path_output_config(true);
        config.include_unmapped = false;
        config.mapping = Some(OutputMapping::new(vec![MappingEntry::rename(
            "person", "name",
        )]));

        let out = project_output_from_record(&input, &config, None);
        let cols: Vec<&str> = out.schema().columns().iter().map(|c| &**c).collect();
        assert_eq!(
            cols,
            vec!["person", "$ck.name"],
            "`$id` is a user column and stays dropped; only the metadata-stamped \
             correlation shadow rides the include_correlation_keys flag"
        );
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
        let config = SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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

        // The default Sink config (no mapping/exclude, include_unmapped
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

        let config = SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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

    /// A minimal fast-path Sink config (no mapping/exclude, no correlation
    /// keys) carrying an optional output `schema:`. `cxl_emit_names = None`
    /// keeps every user field, so a declared decimal column reaches the
    /// rounding pass.
    fn scale_config(schema: Option<SourceSchema>) -> SinkConfig {
        SinkConfig {
            name: "out".into(),
            format: clinker_plan::config::OutputFormat::Csv(None),
            path: "/tmp/out.csv".into(),
            resolved_path_template: None,
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
