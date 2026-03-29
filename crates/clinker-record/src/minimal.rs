use crate::value::Value;

/// A schema-free record: positional access only.
/// Position stability is guaranteed by the Arena that wraps it (Phase 5).
#[derive(Debug, Clone)]
pub struct MinimalRecord {
    pub fields: Vec<Value>,
}

impl MinimalRecord {
    pub fn new(fields: Vec<Value>) -> Self {
        Self { fields }
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.fields.get(index)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_record_positional_access() {
        let rec = MinimalRecord::new(vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Bool(true),
        ]);
        assert_eq!(rec.get(0), Some(&Value::Integer(1)));
        assert_eq!(rec.get(1), Some(&Value::String("Alice".into())));
        assert_eq!(rec.get(2), Some(&Value::Bool(true)));
        assert_eq!(rec.get(3), None);
        assert_eq!(rec.len(), 3);
    }

    #[test]
    fn test_minimal_record_empty() {
        let rec = MinimalRecord::new(vec![]);
        assert!(rec.is_empty());
        assert_eq!(rec.len(), 0);
        assert_eq!(rec.get(0), None);
    }
}
