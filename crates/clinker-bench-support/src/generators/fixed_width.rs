//! Deterministic fixed-width payload generator for benchmarks.

use clinker_record::schema_def::{FieldDef, FieldType, Justify};

/// Lightweight field descriptor for benchmark fixed-width data.
///
/// Use `Into<FieldDef>` to convert for production reader consumption (Phase 3).
/// Derives `Hash` for blake3 cache key computation (D-7).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BenchFieldSpec {
    pub name: String,
    pub start: usize,
    pub width: usize,
    pub field_type: BenchFieldType,
    pub justify: BenchJustify,
}

/// Field type for benchmark generation (maps to `clinker_record::FieldType`).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BenchFieldType {
    Integer,
    String,
}

/// Justification for benchmark generation (maps to `clinker_record::Justify`).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BenchJustify {
    Left,
    Right,
}

impl From<&BenchFieldSpec> for FieldDef {
    fn from(spec: &BenchFieldSpec) -> Self {
        FieldDef {
            name: spec.name.clone(),
            field_type: Some(match spec.field_type {
                BenchFieldType::Integer => FieldType::Integer,
                BenchFieldType::String => FieldType::String,
            }),
            start: Some(spec.start),
            width: Some(spec.width),
            justify: Some(match spec.justify {
                BenchJustify::Left => Justify::Left,
                BenchJustify::Right => Justify::Right,
            }),
            // FieldDef does not derive Default — explicit None for all remaining fields
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            end: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        }
    }
}

/// Generate deterministic fixed-width bytes and field layout.
///
/// Integer fields: 10 chars, right-justified, space-padded.
/// String fields: `string_len` chars, left-justified, space-padded.
/// Line separator: `\n` (D-8).
pub fn generate_fixed_width(
    record_count: usize,
    field_count: usize,
    string_len: usize,
    seed: u64,
) -> (Vec<u8>, Vec<BenchFieldSpec>) {
    let specs = field_specs_for(field_count, string_len);
    let line_len = specs.iter().map(|s| s.width).sum::<usize>() + 1; // +1 for \n
    let mut buf = Vec::with_capacity(record_count * line_len);
    let mut rng = fastrand::Rng::with_seed(seed);

    for _ in 0..record_count {
        for spec in &specs {
            match spec.field_type {
                BenchFieldType::Integer => {
                    let val = rng.i64(0..1_000_000).to_string();
                    // Right-justify: pad left with spaces
                    buf.extend(std::iter::repeat_n(b' ', spec.width - val.len()));
                    buf.extend_from_slice(val.as_bytes());
                }
                BenchFieldType::String => {
                    // Left-justify: value fills entire width (no padding needed)
                    for _ in 0..spec.width {
                        buf.push(rng.u8(b'a'..=b'z'));
                    }
                }
            }
        }
        buf.push(b'\n');
    }

    (buf, specs)
}

/// Compute the field specs for a given configuration without generating data.
///
/// Pure computation — deterministic from parameters. Used by cache system
/// to provide FieldSpec without re-reading cached files (D-11).
pub fn field_specs_for(field_count: usize, string_len: usize) -> Vec<BenchFieldSpec> {
    let int_width = 10;
    let mut specs = Vec::with_capacity(field_count);
    let mut offset = 0;
    for i in 0..field_count {
        let (width, field_type, justify) = if i % 2 == 0 {
            (int_width, BenchFieldType::Integer, BenchJustify::Right)
        } else {
            (string_len, BenchFieldType::String, BenchJustify::Left)
        };
        specs.push(BenchFieldSpec {
            name: format!("f{i}"),
            start: offset,
            width,
            field_type,
            justify,
        });
        offset += width;
    }
    specs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that BenchFieldSpec start/width positions align with actual data
    /// column boundaries in the generated output.
    #[test]
    fn test_fixed_width_generator_field_positions_match() {
        let (bytes, specs) = generate_fixed_width(10, 6, 8, 42);
        let lines: Vec<&[u8]> = bytes
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();

        assert_eq!(lines.len(), 10);
        for spec in &specs {
            let field_bytes = &lines[0][spec.start..spec.start + spec.width];
            match spec.field_type {
                BenchFieldType::Integer => {
                    let s = std::str::from_utf8(field_bytes).unwrap();
                    assert!(
                        s.trim().parse::<i64>().is_ok(),
                        "field {} not a valid integer: {:?}",
                        spec.name,
                        s
                    );
                }
                BenchFieldType::String => {
                    assert!(
                        field_bytes.iter().all(|&b| b.is_ascii_lowercase()),
                        "field {} contains non-lowercase bytes",
                        spec.name
                    );
                }
            }
        }
    }

    /// Verify deterministic output — same parameters always produce identical bytes.
    #[test]
    fn test_fixed_width_generator_deterministic() {
        let (a, specs_a) = generate_fixed_width(50, 4, 6, 42);
        let (b, specs_b) = generate_fixed_width(50, 4, 6, 42);
        assert_eq!(a, b);
        assert_eq!(specs_a, specs_b);
    }

    /// Verify From<&BenchFieldSpec> for FieldDef produces a valid FieldDef
    /// with the expected fixed-width fields populated.
    #[test]
    fn test_bench_field_spec_into_field_def() {
        let spec = BenchFieldSpec {
            name: "f0".to_string(),
            start: 0,
            width: 10,
            field_type: BenchFieldType::Integer,
            justify: BenchJustify::Right,
        };
        let def: FieldDef = FieldDef::from(&spec);
        assert_eq!(def.name, "f0");
        assert_eq!(def.start, Some(0));
        assert_eq!(def.width, Some(10));
        assert_eq!(def.field_type, Some(FieldType::Integer));
        assert_eq!(def.justify, Some(Justify::Right));
    }
}
