#![allow(dead_code)] // Types consumed starting Phase 2+

use std::collections::{HashMap, HashSet};
use std::fmt;

use clinker_record::Value;
use dioxus::prelude::*;

/// Drives title bar control visibility and canvas node styling.
/// Transitions: Idle → Running → Paused → Running → Completed → Idle.
#[derive(Clone, Debug, PartialEq)]
pub enum DebugRunState {
    Idle,
    Running { active_stage: String, progress: f32 },
    Paused { at_stage: String },
    Completed,
}

/// Active tab in the debug drawer.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum DebugTab {
    Input,
    #[default]
    Output,
    Dropped,
    Diff,
}

/// Cell value for debug data tables. Implements Display for rendering
/// and cell_css_class() for styling. Array variant supports JSON/XML
/// nested data with element-level diff and truncated display.
#[derive(Clone, Debug, PartialEq)]
pub enum CellValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<CellValue>),
}

/// Arrays with this many elements or fewer are shown in full.
const ARRAY_INLINE_MAX: usize = 5;
/// When truncating, show this many elements before the "... N items" suffix.
const ARRAY_TRUNCATE_SHOW: usize = 3;

impl fmt::Display for CellValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Str(s) => write!(f, "{s}"),
            Self::Array(arr) if arr.len() <= ARRAY_INLINE_MAX => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Self::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().take(ARRAY_TRUNCATE_SHOW).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, ", ... {} items]", arr.len())
            }
        }
    }
}

impl CellValue {
    /// Comparison for diff tab: treats NaN == NaN (bitwise) and -0.0 == +0.0 (display).
    /// Use this instead of PartialEq when detecting visual mutations.
    pub fn diff_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Float(a), Self::Float(b)) => {
                if a.is_nan() && b.is_nan() {
                    return true;
                }
                if *a == 0.0 && *b == 0.0 {
                    return true;
                }
                a == b
            }
            (Self::Array(a), Self::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.diff_eq(y))
            }
            _ => self == other,
        }
    }

    /// Returns CSS class name for data table cell styling.
    pub fn cell_css_class(&self) -> &'static str {
        match self {
            Self::Null => "kiln-debug-td--null",
            Self::Bool(_) => "kiln-debug-td--bool",
            Self::Int(_) | Self::Float(_) => "kiln-debug-td--number",
            Self::Str(_) => "kiln-debug-td--string",
            Self::Array(_) => "kiln-debug-td--array",
        }
    }
}

impl From<Value> for CellValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(b),
            Value::Integer(n) => Self::Int(n),
            Value::Float(f) => Self::Float(f),
            Value::String(s) => Self::Str((*s).into()),
            Value::Date(d) => Self::Str(d.to_string()),
            Value::DateTime(dt) => Self::Str(dt.to_string()),
            Value::Array(arr) => Self::Array(arr.into_iter().map(CellValue::from).collect()),
        }
    }
}

/// Per-stage performance metrics.
#[derive(Clone, Debug)]
pub struct StagePerfStats {
    pub rows_in: u32,
    pub rows_out: u32,
    pub elapsed_ms: u32,
    pub mem: String,
}

// ── Compact Display wrapper ───────────────────────────────

/// Default compact depth for table cell rendering.
/// Depth 1 = flat arrays inline, depth 2 = one nesting level shown.
pub const COMPACT_DEFAULT_DEPTH: usize = 2;

impl CellValue {
    /// Depth-limited display wrapper for table cell rendering.
    /// Scalars pass through to full Display. Arrays collapse to
    /// `[N items]` when max_depth reaches 0.
    pub fn compact(&self, max_depth: usize) -> CompactCellValue<'_> {
        CompactCellValue {
            value: self,
            max_depth,
        }
    }
}

/// Zero-cost newtype wrapper for depth-limited CellValue display.
pub struct CompactCellValue<'a> {
    value: &'a CellValue,
    max_depth: usize,
}

impl fmt::Display for CompactCellValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value {
            CellValue::Null
            | CellValue::Bool(_)
            | CellValue::Int(_)
            | CellValue::Float(_)
            | CellValue::Str(_) => fmt::Display::fmt(self.value, f),
            CellValue::Array(items) if items.is_empty() => write!(f, "[]"),
            CellValue::Array(items) if self.max_depth == 0 => {
                write!(f, "[{} items]", items.len())
            }
            CellValue::Array(items) if items.len() <= ARRAY_INLINE_MAX => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    CompactCellValue {
                        value: item,
                        max_depth: self.max_depth - 1,
                    }
                    .fmt(f)?;
                }
                write!(f, "]")
            }
            CellValue::Array(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().take(ARRAY_TRUNCATE_SHOW).enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    CompactCellValue {
                        value: item,
                        max_depth: self.max_depth - 1,
                    }
                    .fmt(f)?;
                }
                write!(f, ", ... {} items]", items.len())
            }
        }
    }
}

// ── Stage data structs ────────────────────────────────────

/// A row that was filtered/dropped by a pipeline stage, with the reason.
/// Follows the DlqEntry pattern from clinker_core::executor.
#[derive(Clone, Debug)]
pub struct DroppedRow {
    pub cells: Vec<CellValue>,
    pub reason: String,
}

/// Cached debug data for a single pipeline stage.
/// Keyed by stage label in DebugState.stage_cache.
/// Clone-on-read semantics — not shared.
#[derive(Clone, Debug)]
pub struct StageDebugData {
    pub columns: Vec<String>,
    pub input_rows: Vec<Vec<CellValue>>,
    pub output_rows: Vec<Vec<CellValue>>,
    pub dropped_rows: Vec<DroppedRow>,
    pub added_columns: Vec<String>,
    pub mutated_columns: Vec<String>,
    pub stats: StagePerfStats,
}

/// A user-defined watch expression evaluated at each stage boundary.
/// values[i] corresponds to stage i; None = not yet reached.
#[derive(Clone, Debug)]
pub struct WatchExpression {
    pub expr: String,
    pub values: Vec<Option<f64>>,
}

// ── Debug lifecycle context ───────────────────────────────

/// Debug lifecycle state, provided at AppShell root.
/// Copy-able — all fields are Signal<T> (cheap handles).
#[derive(Clone, Copy)]
pub struct DebugState {
    pub run_state: Signal<DebugRunState>,
    pub breakpoints: Signal<HashSet<String>>,
    pub cond_breakpoints: Signal<HashMap<String, String>>,
    pub drawer_open: Signal<bool>,
    pub drawer_stage: Signal<Option<String>>,
    pub drawer_tab: Signal<DebugTab>,
    pub stage_cache: Signal<HashMap<String, StageDebugData>>,
    pub watches: Signal<Vec<WatchExpression>>,
    pub watch_collapsed: Signal<bool>,
    pub editing_bp_stage: Signal<Option<String>>,
    pub downstream_dim_set: Signal<HashSet<String>>,
}

/// Convenience accessor — same pattern as use_app_state() in state.rs.
pub fn use_debug_state() -> DebugState {
    use_context::<DebugState>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    // ── CellValue Display ──────────────────────────────────────

    #[test]
    fn test_cell_value_display_null_shows_null() {
        assert_eq!(CellValue::Null.to_string(), "null");
    }

    #[test]
    fn test_cell_value_display_formats_correctly() {
        assert_eq!(CellValue::Bool(true).to_string(), "true");
        assert_eq!(CellValue::Bool(false).to_string(), "false");
        assert_eq!(CellValue::Int(42).to_string(), "42");
        assert_eq!(CellValue::Float(3.14).to_string(), "3.14");
        assert_eq!(CellValue::Str("hello".into()).to_string(), "hello");
    }

    #[test]
    fn test_cell_value_display_edge_cases() {
        assert_eq!(CellValue::Str(String::new()).to_string(), "");
        assert_eq!(CellValue::Int(-1).to_string(), "-1");
        assert_eq!(CellValue::Float(f64::NAN).to_string(), "NaN");
        assert_eq!(CellValue::Float(f64::INFINITY).to_string(), "inf");
        assert_eq!(CellValue::Float(f64::NEG_INFINITY).to_string(), "-inf");
    }

    // ── CellValue CSS class ────────────────────────────────────

    #[test]
    fn test_cell_value_css_class_mapping() {
        assert_eq!(CellValue::Null.cell_css_class(), "kiln-debug-td--null");
        assert_eq!(
            CellValue::Bool(false).cell_css_class(),
            "kiln-debug-td--bool"
        );
        assert_eq!(CellValue::Int(1).cell_css_class(), "kiln-debug-td--number");
        assert_eq!(
            CellValue::Float(1.0).cell_css_class(),
            "kiln-debug-td--number"
        );
        assert_eq!(
            CellValue::Str("x".into()).cell_css_class(),
            "kiln-debug-td--string"
        );
        assert_eq!(
            CellValue::Array(vec![]).cell_css_class(),
            "kiln-debug-td--array"
        );
    }

    // ── CellValue From<Value> ──────────────────────────────────

    #[test]
    fn test_cell_value_from_record_value() {
        assert_eq!(CellValue::from(Value::Null), CellValue::Null);
        assert_eq!(CellValue::from(Value::Bool(true)), CellValue::Bool(true));
        assert_eq!(CellValue::from(Value::Integer(42)), CellValue::Int(42));
        assert_eq!(CellValue::from(Value::Float(3.14)), CellValue::Float(3.14));
        assert_eq!(
            CellValue::from(Value::String("hello".into())),
            CellValue::Str("hello".into())
        );
    }

    #[test]
    fn test_cell_value_from_record_value_date_to_str() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        let cv = CellValue::from(Value::Date(date));
        assert!(matches!(cv, CellValue::Str(_)));
        assert_eq!(cv.to_string(), "2026-03-31");

        let dt = date.and_hms_opt(12, 0, 0).unwrap();
        let cv = CellValue::from(Value::DateTime(dt));
        assert!(matches!(cv, CellValue::Str(_)));
        assert!(cv.to_string().starts_with("2026-03-31"));
    }

    // ── CellValue Array Display ───────────────────────────────

    #[test]
    fn test_cell_value_display_array_short() {
        let arr = CellValue::Array(vec![CellValue::Int(1), CellValue::Int(2)]);
        assert_eq!(arr.to_string(), "[1, 2]");

        let single = CellValue::Array(vec![CellValue::Str("hello".into())]);
        assert_eq!(single.to_string(), "[hello]");

        let five = CellValue::Array(vec![
            CellValue::Str("a".into()),
            CellValue::Str("b".into()),
            CellValue::Str("c".into()),
            CellValue::Str("d".into()),
            CellValue::Str("e".into()),
        ]);
        assert_eq!(five.to_string(), "[a, b, c, d, e]");
    }

    #[test]
    fn test_cell_value_display_array_truncated() {
        let arr = CellValue::Array(vec![
            CellValue::Str("a".into()),
            CellValue::Str("b".into()),
            CellValue::Str("c".into()),
            CellValue::Str("d".into()),
            CellValue::Str("e".into()),
            CellValue::Str("f".into()),
        ]);
        assert_eq!(arr.to_string(), "[a, b, c, ... 6 items]");
    }

    #[test]
    fn test_cell_value_display_array_nested() {
        let arr = CellValue::Array(vec![
            CellValue::Array(vec![CellValue::Int(1), CellValue::Int(2)]),
            CellValue::Array(vec![CellValue::Int(3)]),
        ]);
        assert_eq!(arr.to_string(), "[[1, 2], [3]]");
    }

    #[test]
    fn test_cell_value_display_array_empty() {
        assert_eq!(CellValue::Array(vec![]).to_string(), "[]");
    }

    #[test]
    fn test_cell_value_css_class_array() {
        assert_eq!(
            CellValue::Array(vec![CellValue::Int(1)]).cell_css_class(),
            "kiln-debug-td--array"
        );
    }

    #[test]
    fn test_cell_value_from_record_value_array() {
        let arr = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
        let cv = CellValue::from(arr);
        assert_eq!(
            cv,
            CellValue::Array(vec![CellValue::Int(1), CellValue::Int(2)])
        );
    }

    #[test]
    fn test_cell_value_from_record_value_array_nested() {
        let inner = Value::Array(vec![Value::Bool(true), Value::Null]);
        let outer = Value::Array(vec![inner, Value::String("x".into())]);
        let cv = CellValue::from(outer);
        assert_eq!(
            cv,
            CellValue::Array(vec![
                CellValue::Array(vec![CellValue::Bool(true), CellValue::Null]),
                CellValue::Str("x".into()),
            ])
        );
    }

    // ── CompactCellValue ──────────────────────────────────────

    #[test]
    fn test_compact_scalars_pass_through() {
        assert_eq!(CellValue::Null.compact(2).to_string(), "null");
        assert_eq!(CellValue::Int(42).compact(2).to_string(), "42");
        assert_eq!(
            CellValue::Str("hello".into()).compact(0).to_string(),
            "hello"
        );
    }

    #[test]
    fn test_compact_depth_2_shows_one_nesting() {
        let nested = CellValue::Array(vec![
            CellValue::Array(vec![CellValue::Int(1), CellValue::Int(2)]),
            CellValue::Array(vec![CellValue::Int(3)]),
        ]);
        assert_eq!(nested.compact(2).to_string(), "[[1, 2], [3]]");
    }

    #[test]
    fn test_compact_depth_1_collapses_inner() {
        let nested = CellValue::Array(vec![
            CellValue::Array(vec![CellValue::Int(1), CellValue::Int(2)]),
            CellValue::Array(vec![CellValue::Int(3)]),
        ]);
        assert_eq!(nested.compact(1).to_string(), "[[2 items], [1 items]]");
    }

    #[test]
    fn test_compact_depth_0_placeholder() {
        let arr = CellValue::Array(vec![CellValue::Int(1), CellValue::Int(2)]);
        assert_eq!(arr.compact(0).to_string(), "[2 items]");
    }

    #[test]
    fn test_compact_empty_array_depth_0() {
        assert_eq!(CellValue::Array(vec![]).compact(0).to_string(), "[]");
    }

    #[test]
    fn test_compact_depth_3_nesting() {
        let deep = CellValue::Array(vec![CellValue::Array(vec![CellValue::Array(vec![
            CellValue::Int(1),
            CellValue::Int(2),
        ])])]);
        assert_eq!(deep.compact(2).to_string(), "[[[2 items]]]");
    }

    // ── CellValue diff_eq ─────────────────────────────────────

    #[test]
    fn test_diff_eq_nan_is_equal() {
        let a = CellValue::Float(f64::NAN);
        let b = CellValue::Float(f64::NAN);
        assert!(a.diff_eq(&b));
        assert_ne!(a, b);
    }

    #[test]
    fn test_diff_eq_zero_signs_equal() {
        let pos = CellValue::Float(0.0);
        let neg = CellValue::Float(-0.0);
        assert!(pos.diff_eq(&neg));
    }

    #[test]
    fn test_diff_eq_array_with_nan() {
        let a = CellValue::Array(vec![CellValue::Float(f64::NAN), CellValue::Int(1)]);
        let b = CellValue::Array(vec![CellValue::Float(f64::NAN), CellValue::Int(1)]);
        assert!(a.diff_eq(&b));
        assert_ne!(a, b);
    }

    // ── DebugRunState ──────────────────────────────────────────

    #[test]
    fn test_debug_run_state_clone_and_eq() {
        let idle = DebugRunState::Idle;
        assert_eq!(idle.clone(), DebugRunState::Idle);

        let running = DebugRunState::Running {
            active_stage: "normalize".into(),
            progress: 0.5,
        };
        assert_eq!(running.clone(), running);

        let paused = DebugRunState::Paused {
            at_stage: "filter".into(),
        };
        assert_eq!(paused.clone(), paused);

        let completed = DebugRunState::Completed;
        assert_eq!(completed.clone(), DebugRunState::Completed);

        assert_ne!(DebugRunState::Idle, DebugRunState::Completed);
    }

    #[test]
    fn test_debug_run_state_not_copy() {
        let state = DebugRunState::Running {
            active_stage: "test".into(),
            progress: 0.0,
        };
        let moved = state;
        assert_eq!(
            moved,
            DebugRunState::Running {
                active_stage: "test".into(),
                progress: 0.0,
            }
        );
    }

    // ── DebugTab ───────────────────────────────────────────────

    #[test]
    fn test_debug_tab_default_is_output() {
        assert_eq!(DebugTab::default(), DebugTab::Output);
    }

    #[test]
    fn test_debug_tab_is_copy() {
        let tab = DebugTab::Input;
        let copied = tab;
        assert_eq!(tab, copied);
    }

    // ── StagePerfStats ─────────────────────────────────────────

    #[test]
    fn test_stage_perf_stats_clone() {
        let stats = StagePerfStats {
            rows_in: 100,
            rows_out: 95,
            elapsed_ms: 12,
            mem: "1.2 MB".into(),
        };
        let cloned = stats.clone();
        assert_eq!(cloned.rows_in, 100);
        assert_eq!(cloned.rows_out, 95);
        assert_eq!(cloned.elapsed_ms, 12);
        assert_eq!(cloned.mem, "1.2 MB");
    }

    // ── DroppedRow ─────────────────────────────────────────────

    #[test]
    fn test_dropped_row_preserves_reason() {
        let row = DroppedRow {
            cells: vec![CellValue::Int(1), CellValue::Str("Alice".into())],
            reason: "filter: status != active".into(),
        };
        assert_eq!(row.cells.len(), 2);
        assert_eq!(row.reason, "filter: status != active");
        assert!(
            row.cells
                .iter()
                .all(|c| !matches!(c, CellValue::Str(s) if s.contains("filter")))
        );
    }

    #[test]
    fn test_dropped_row_empty_cells() {
        let row = DroppedRow {
            cells: vec![],
            reason: "parse error: invalid CSV".into(),
        };
        assert!(row.cells.is_empty());
        assert!(!row.reason.is_empty());
    }

    // ── StageDebugData ─────────────────────────────────────────

    #[test]
    fn test_stage_debug_data_clone() {
        let data = StageDebugData {
            columns: vec!["id".into(), "name".into()],
            input_rows: vec![vec![CellValue::Int(1), CellValue::Str("Alice".into())]],
            output_rows: vec![vec![CellValue::Int(1), CellValue::Str("ALICE".into())]],
            dropped_rows: vec![DroppedRow {
                cells: vec![CellValue::Int(2), CellValue::Str("Bob".into())],
                reason: "filter: status != active".into(),
            }],
            added_columns: vec![],
            mutated_columns: vec!["name".into()],
            stats: StagePerfStats {
                rows_in: 2,
                rows_out: 1,
                elapsed_ms: 12,
                mem: "1.2 MB".into(),
            },
        };
        let cloned = data.clone();
        assert_eq!(cloned.columns, data.columns);
        assert_eq!(cloned.input_rows.len(), 1);
        assert_eq!(cloned.output_rows.len(), 1);
        assert_eq!(cloned.dropped_rows.len(), 1);
        assert_eq!(cloned.dropped_rows[0].reason, "filter: status != active");
        assert_eq!(cloned.mutated_columns, vec!["name"]);
        assert_eq!(cloned.stats.rows_in, 2);
    }

    #[test]
    fn test_stage_debug_data_empty() {
        let data = StageDebugData {
            columns: vec![],
            input_rows: vec![],
            output_rows: vec![],
            dropped_rows: vec![],
            added_columns: vec![],
            mutated_columns: vec![],
            stats: StagePerfStats {
                rows_in: 0,
                rows_out: 0,
                elapsed_ms: 0,
                mem: "0 B".into(),
            },
        };
        assert!(data.columns.is_empty());
        assert!(data.dropped_rows.is_empty());
    }

    #[test]
    fn test_stage_debug_data_dropped_rows_typed() {
        let data = StageDebugData {
            columns: vec!["x".into()],
            input_rows: vec![],
            output_rows: vec![],
            dropped_rows: vec![
                DroppedRow {
                    cells: vec![CellValue::Int(1)],
                    reason: "reason A".into(),
                },
                DroppedRow {
                    cells: vec![CellValue::Int(2)],
                    reason: "reason B".into(),
                },
            ],
            added_columns: vec![],
            mutated_columns: vec![],
            stats: StagePerfStats {
                rows_in: 2,
                rows_out: 0,
                elapsed_ms: 5,
                mem: "0.1 MB".into(),
            },
        };
        assert_eq!(data.dropped_rows[0].reason, "reason A");
        assert_eq!(data.dropped_rows[1].reason, "reason B");
    }

    // ── WatchExpression ────────────────────────────────────────

    #[test]
    fn test_watch_expression_none_values() {
        let watch = WatchExpression {
            expr: "count(*)".into(),
            values: vec![None, None, None],
        };
        assert_eq!(watch.values.len(), 3);
        assert!(watch.values.iter().all(|v| v.is_none()));
    }

    #[test]
    fn test_watch_expression_mixed_values() {
        let watch = WatchExpression {
            expr: "sum(amount)".into(),
            values: vec![Some(100.0), Some(250.0), None, None],
        };
        assert_eq!(watch.values[0], Some(100.0));
        assert_eq!(watch.values[1], Some(250.0));
        assert_eq!(watch.values[2], None);
    }
}
