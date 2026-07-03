//! Deterministic fixed-width payload generator for benchmarks.

use crate::FieldKind;

/// Lightweight field descriptor for benchmark fixed-width data.
///
/// Derives `Hash` for blake3 cache key computation (D-7).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BenchFieldSpec {
    pub name: String,
    pub start: usize,
    pub width: usize,
    pub field_type: BenchFieldType,
    pub justify: BenchJustify,
}

/// Field type for benchmark generation — selects the generator's per-field
/// justification and value shape.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BenchFieldType {
    Integer,
    String,
}

/// Justification for benchmark generation.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BenchJustify {
    Left,
    Right,
}

/// Generate deterministic fixed-width bytes and field layout.
///
/// Integer fields: 10 chars, right-justified, space-padded.
/// String/Date/Float/Bool fields: `string_len` chars, left-justified, space-padded.
/// Line separator: `\n` (D-8).
pub fn generate_fixed_width(
    record_count: usize,
    field_types: &[FieldKind],
    string_len: usize,
    seed: u64,
) -> (Vec<u8>, Vec<BenchFieldSpec>) {
    let specs = field_specs_for(field_types, string_len);
    let line_len = specs.iter().map(|s| s.width).sum::<usize>() + 1; // +1 for \n
    let mut buf = Vec::with_capacity(record_count * line_len);
    let mut rng = fastrand::Rng::with_seed(seed);

    for _ in 0..record_count {
        for (idx, spec) in specs.iter().enumerate() {
            let kind = field_types[idx];
            match spec.field_type {
                BenchFieldType::Integer => {
                    let mut val_buf = Vec::new();
                    crate::write_field_value(&mut val_buf, kind, &mut rng, string_len);
                    // Right-justify: pad left with spaces
                    if val_buf.len() < spec.width {
                        buf.extend(std::iter::repeat_n(b' ', spec.width - val_buf.len()));
                    }
                    buf.extend_from_slice(&val_buf);
                }
                BenchFieldType::String => {
                    let start = buf.len();
                    crate::write_field_value(&mut buf, kind, &mut rng, string_len);
                    let written = buf.len() - start;
                    // Left-justify: pad right with spaces
                    if written < spec.width {
                        buf.extend(std::iter::repeat_n(b' ', spec.width - written));
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
pub fn field_specs_for(field_types: &[FieldKind], string_len: usize) -> Vec<BenchFieldSpec> {
    let int_width = 10;
    let mut specs = Vec::with_capacity(field_types.len());
    let mut offset = 0;
    for (i, &kind) in field_types.iter().enumerate() {
        let (width, field_type, justify) = match kind {
            FieldKind::Int => (int_width, BenchFieldType::Integer, BenchJustify::Right),
            FieldKind::Float => (14, BenchFieldType::String, BenchJustify::Right),
            _ => (string_len, BenchFieldType::String, BenchJustify::Left),
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
        let ft = FieldKind::default_layout(6);
        let (bytes, specs) = generate_fixed_width(10, &ft, 8, 42);
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
        let ft = FieldKind::default_layout(4);
        let (a, specs_a) = generate_fixed_width(50, &ft, 6, 42);
        let (b, specs_b) = generate_fixed_width(50, &ft, 6, 42);
        assert_eq!(a, b);
        assert_eq!(specs_a, specs_b);
    }
}
