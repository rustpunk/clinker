/// Compute Levenshtein distance between two strings.
/// Returns `None` (bails early) if the distance would exceed `threshold`.
pub fn levenshtein_bounded(a: &str, b: &str, threshold: usize) -> Option<usize> {
    let a_len = a.chars().count();
    let b_len = b.chars().count();

    // Quick bail: length difference alone exceeds threshold
    if a_len.abs_diff(b_len) > threshold {
        return None;
    }

    // Use single-row DP with early-bail
    let mut row: Vec<usize> = (0..=b_len).collect();

    for (i, ca) in a.chars().enumerate() {
        let mut prev = i; // row[0] before overwrite
        row[0] = i + 1;
        let mut min_in_row = row[0];

        for (j, cb) in b.chars().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            let val = (row[j + 1] + 1) // deletion
                .min(row[j] + 1) // insertion
                .min(prev + cost); // substitution
            prev = row[j + 1];
            row[j + 1] = val;
            min_in_row = min_in_row.min(val);
        }

        // Early bail: if every value in this row exceeds threshold,
        // the final distance will too (values only increase or stay)
        if min_in_row > threshold {
            return None;
        }
    }

    let dist = row[b_len];
    if dist <= threshold { Some(dist) } else { None }
}

/// Find the best fuzzy match for `target` among `candidates`.
/// Returns `Some(best_match)` if the best Levenshtein distance is <= `threshold`.
pub fn best_match<'a>(target: &str, candidates: &[&'a str], threshold: usize) -> Option<&'a str> {
    let mut best: Option<(&str, usize)> = None;
    for &candidate in candidates {
        if let Some(dist) = levenshtein_bounded(target, candidate, threshold)
            && (best.is_none() || dist < best.unwrap().1)
        {
            best = Some((candidate, dist));
        }
    }
    best.map(|(name, _)| name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein_identical() {
        assert_eq!(levenshtein_bounded("hello", "hello", 3), Some(0));
    }

    #[test]
    fn test_levenshtein_one_off() {
        assert_eq!(levenshtein_bounded("naem", "name", 3), Some(2));
    }

    #[test]
    fn test_levenshtein_exceeds_threshold() {
        assert_eq!(levenshtein_bounded("abc", "xyz", 2), None);
    }

    #[test]
    fn test_levenshtein_empty() {
        assert_eq!(levenshtein_bounded("", "abc", 3), Some(3));
        assert_eq!(levenshtein_bounded("", "abcd", 3), None);
    }

    #[test]
    fn test_best_match_found() {
        let candidates = &["name", "age", "status"];
        assert_eq!(best_match("naem", candidates, 3), Some("name"));
    }

    #[test]
    fn test_best_match_none() {
        let candidates = &["name", "age"];
        assert_eq!(best_match("zzzzz", candidates, 3), None);
    }
}
