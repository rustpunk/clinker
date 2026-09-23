//! End-of-output account of values a writer cut to fit a column under
//! `truncation: warn`.
//!
//! A writer keeps this account in storage sized by its schema when it is
//! built, so recording a truncation neither allocates nor fails: exact counts
//! per column, and the output record numbers of the first few records that
//! truncated. No value text is ever copied. `truncation: silent` records
//! nothing, and `truncation: error` rejects the record instead.

/// Output records listed per column before later ones are only counted.
pub const TRUNCATION_EXAMPLE_LIMIT: usize = 8;

/// Truncations one writer (or one split output, across its files) performed.
/// Columns appear in output layout order; only columns that truncated appear.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TruncationSummary {
    pub columns: Vec<ColumnTruncation>,
}

/// Truncations in one column. For a repeating-group child, `column` is
/// `group.child`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTruncation {
    pub column: String,
    /// Declared width, in bytes, values were cut to.
    pub width: usize,
    /// Exact number of values truncated.
    pub cells: u64,
    /// Longest original value, in bytes.
    pub longest_bytes: usize,
    /// 1-based output record numbers of the first records that truncated in
    /// this column, ascending, at most [`TRUNCATION_EXAMPLE_LIMIT`].
    pub example_records: Vec<u64>,
    /// Further records truncated in this column but were not listed.
    pub more_records: bool,
}

impl TruncationSummary {
    /// Exact number of truncated values across every column.
    pub fn total_cells(&self) -> u64 {
        self.columns
            .iter()
            .fold(0u64, |total, column| total.saturating_add(column.cells))
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Fold in the summary of a later writer on the same output whose record
    /// numbers restart at 1, shifting them by `records_before` so every
    /// example names its record across the whole output. Examples stay
    /// ascending and capped: a later writer's records all follow this one's.
    pub fn merge_after(&mut self, later: TruncationSummary, records_before: u64) {
        for column in later.columns {
            let example_records = column
                .example_records
                .iter()
                .map(|record| record.saturating_add(records_before));
            match self
                .columns
                .iter_mut()
                .find(|existing| existing.column == column.column)
            {
                Some(existing) => {
                    existing.cells = existing.cells.saturating_add(column.cells);
                    existing.longest_bytes = existing.longest_bytes.max(column.longest_bytes);
                    let room = TRUNCATION_EXAMPLE_LIMIT - existing.example_records.len();
                    let offered = column.example_records.len();
                    existing.example_records.extend(example_records.take(room));
                    existing.more_records |= column.more_records || offered > room;
                }
                None => self.columns.push(ColumnTruncation {
                    example_records: example_records.collect(),
                    ..column
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, cells: u64, longest: usize, examples: &[u64]) -> ColumnTruncation {
        ColumnTruncation {
            column: name.into(),
            width: 4,
            cells,
            longest_bytes: longest,
            example_records: examples.to_vec(),
            more_records: false,
        }
    }

    #[test]
    fn merge_offsets_later_records_and_keeps_the_cap() {
        let mut first = TruncationSummary {
            columns: vec![column("name", 6, 9, &[1, 2, 3, 4, 5, 6])],
        };
        let later = TruncationSummary {
            columns: vec![
                column("name", 3, 12, &[1, 2, 3]),
                column("city", 1, 5, &[2]),
            ],
        };
        first.merge_after(later, 10);

        let name = &first.columns[0];
        assert_eq!(name.cells, 9);
        assert_eq!(name.longest_bytes, 12);
        assert_eq!(name.example_records, vec![1, 2, 3, 4, 5, 6, 11, 12]);
        assert!(name.more_records, "record 13 was dropped by the cap");
        assert_eq!(first.columns[1].example_records, vec![12]);
        assert!(!first.columns[1].more_records);
        assert_eq!(first.total_cells(), 10);
    }
}
