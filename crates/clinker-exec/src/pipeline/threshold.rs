//! Error threshold checker for pipeline chunk boundaries.
//!
//! Denominator strategy depends on execution mode:
//! - **TwoPass:** denominator is total_records from Phase 1 Arena (known upfront).
//! - **Streaming:** denominator is records_processed_so_far (running ratio).

/// Error threshold checker.
///
/// Checked at chunk boundaries. Returns true when error rate strictly
/// exceeds the configured threshold.
pub struct ErrorThreshold {
    /// Error rate limit in [0.0, 1.0]. 0.0 = any error halts. 1.0 = never halts.
    limit: f64,
    /// `Some(n)` for TwoPass (Arena count), `None` for Streaming.
    total_records: Option<u64>,
}

impl ErrorThreshold {
    pub fn new(limit: f64, total_records: Option<u64>) -> Self {
        Self {
            limit,
            total_records,
        }
    }

    /// Returns true if error rate strictly exceeds the threshold.
    pub fn exceeded(&self, error_count: u64, records_processed: u64) -> bool {
        // 1.0 means unlimited — never halt on errors
        if self.limit >= 1.0 {
            return false;
        }
        // 0.0 means zero tolerance — any error halts
        if self.limit <= 0.0 && error_count > 0 {
            return true;
        }
        let denominator = self.total_records.unwrap_or(records_processed);
        if denominator == 0 {
            return false;
        }
        let rate = error_count as f64 / denominator as f64;
        rate > self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_threshold_below_limit() {
        let t = ErrorThreshold::new(0.10, Some(1000));
        assert!(!t.exceeded(50, 1000)); // 5% < 10%
    }

    #[test]
    fn test_error_threshold_exceeded() {
        let t = ErrorThreshold::new(0.10, Some(1000));
        assert!(t.exceeded(150, 1000)); // 15% > 10%
    }

    #[test]
    fn test_error_threshold_zero_means_no_errors() {
        let t = ErrorThreshold::new(0.0, Some(1000));
        assert!(t.exceeded(1, 1)); // Any error halts
    }

    #[test]
    fn test_error_threshold_one_means_unlimited() {
        let t = ErrorThreshold::new(1.0, Some(100));
        assert!(!t.exceeded(100, 100)); // 100% is not > 1.0
    }

    #[test]
    fn test_error_threshold_streaming_running_ratio() {
        let t = ErrorThreshold::new(0.10, None); // Streaming — no Arena total
        assert!(t.exceeded(2, 10)); // 20% > 10%
        assert!(!t.exceeded(1, 100)); // 1% < 10%
    }

    #[test]
    fn test_error_threshold_twopass_uses_total() {
        let t = ErrorThreshold::new(0.10, Some(1000));
        // 10 errors after processing only 10 records: 10/1000 = 1%, not 10/10 = 100%
        assert!(!t.exceeded(10, 10));
    }

    #[test]
    fn test_error_threshold_zero_denominator() {
        let t = ErrorThreshold::new(0.10, None);
        assert!(!t.exceeded(0, 0)); // No records processed, no errors
    }

    #[test]
    fn test_error_threshold_exact_boundary() {
        let t = ErrorThreshold::new(0.10, Some(100));
        // Exactly 10% — not strictly greater, so should NOT halt
        assert!(!t.exceeded(10, 100));
        // 11% — strictly greater
        assert!(t.exceeded(11, 100));
    }
}
