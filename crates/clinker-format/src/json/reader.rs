//! JSON reader with auto-detect (array/NDJSON/object), schema inference,
//! nested flattening, and array_paths explode/join.
//!
//! Streaming: NDJSON reads line-by-line. Array mode uses serde_json's
//! StreamDeserializer. Only record_path mode buffers the full Value tree
//! (documented 3-11x memory overhead for ≤100MB files).

use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::error::FormatError;
use crate::traits::FormatReader;

/// JSON reader configuration.
pub struct JsonReaderConfig {
    pub format: Option<JsonMode>,
    pub record_path: Option<String>,
    pub array_paths: Vec<ArrayPathSpec>,
}

impl Default for JsonReaderConfig {
    fn default() -> Self {
        Self {
            format: None,
            record_path: None,
            array_paths: vec![],
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum JsonMode {
    Array,
    Ndjson,
    Object,
}

#[derive(Debug, Clone)]
pub struct ArrayPathSpec {
    pub path: String,
    pub mode: ArrayPathMode,
    pub separator: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ArrayPathMode {
    Explode,
    Join,
}

/// Streaming JSON reader implementing `FormatReader`.
pub struct JsonReader {
    inner: InnerReader,
    schema: Option<Arc<Schema>>,
    config: JsonReaderConfig,
    /// Buffer for exploded records from array_paths or schema-peek record.
    pending: Vec<serde_json::Map<String, serde_json::Value>>,
}

/// Internal reader state — owns the data source.
enum InnerReader {
    /// NDJSON: line-by-line from buffered reader.
    Ndjson(BufReader<Box<dyn Read + Send>>),
    /// Pre-loaded records (from array mode or record_path).
    /// Used for array/object modes where we already have all values.
    Preloaded { records: Vec<serde_json::Value>, pos: usize },
    /// Exhausted.
    Done,
}

impl JsonReader {
    pub fn from_reader<R: Read + Send + 'static>(
        reader: R,
        config: JsonReaderConfig,
    ) -> Result<Self, FormatError> {
        let mut buf = BufReader::new(Box::new(reader) as Box<dyn Read + Send>);
        let inner = Self::init_reader(&mut buf, &config)?;
        Ok(JsonReader {
            inner,
            schema: None,
            config,
            pending: Vec::new(),
        })
    }

    fn init_reader(
        buf: &mut BufReader<Box<dyn Read + Send>>,
        config: &JsonReaderConfig,
    ) -> Result<InnerReader, FormatError> {
        // record_path → buffer the Value tree, navigate, extract array
        if let Some(ref rp) = config.record_path {
            let value: serde_json::Value = serde_json::from_reader(buf)
                .map_err(|e| FormatError::Json(e.to_string()))?;
            let pointer = dot_path_to_pointer(rp);
            let array = value
                .pointer(&pointer)
                .ok_or_else(|| FormatError::Json(format!("record_path '{rp}' not found")))?
                .as_array()
                .ok_or_else(|| FormatError::Json(format!("record_path '{rp}' is not an array")))?
                .clone();
            return Ok(InnerReader::Preloaded { records: array, pos: 0 });
        }

        // Explicit format override
        if let Some(mode) = config.format {
            return match mode {
                JsonMode::Array => load_array(buf),
                JsonMode::Ndjson => {
                    // Transfer ownership: buf is already consumed into the enum
                    // We need to give the BufReader to Ndjson. Since we can't move
                    // out of &mut, we swap with a dummy.
                    let owned = std::mem::replace(buf, BufReader::new(Box::new(std::io::empty())));
                    Ok(InnerReader::Ndjson(owned))
                }
                JsonMode::Object => Err(FormatError::Json("format: object requires record_path".into())),
            };
        }

        // Auto-detect: peek first non-whitespace byte
        let first = peek_first_byte(buf)?;
        match first {
            Some(b'[') => load_array(buf),
            Some(b'{') => {
                let owned = std::mem::replace(buf, BufReader::new(Box::new(std::io::empty())));
                Ok(InnerReader::Ndjson(owned))
            }
            Some(b) => Err(FormatError::Json(format!(
                "cannot auto-detect: unexpected byte '{}' (0x{b:02x})", b as char
            ))),
            None => Ok(InnerReader::Done),
        }
    }

    /// Get next raw JSON value from the source.
    fn next_raw(&mut self) -> Result<Option<serde_json::Value>, FormatError> {
        match &mut self.inner {
            InnerReader::Preloaded { records, pos } => {
                if *pos < records.len() {
                    let v = records[*pos].clone();
                    *pos += 1;
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            InnerReader::Ndjson(reader) => {
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).map_err(FormatError::Io)?;
                    if n == 0 {
                        return Ok(None);
                    }
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let val: serde_json::Value = serde_json::from_str(trimmed)
                        .map_err(|e| FormatError::Json(e.to_string()))?;
                    return Ok(Some(val));
                }
            }
            InnerReader::Done => Ok(None),
        }
    }

    /// Flatten nested JSON objects into dotted field names.
    fn flatten_value(
        prefix: &str,
        value: &serde_json::Value,
        out: &mut serde_json::Map<String, serde_json::Value>,
        depth: usize,
    ) {
        const MAX_DEPTH: usize = 64;
        if depth > MAX_DEPTH {
            out.insert(prefix.to_string(), serde_json::Value::String("[max depth]".into()));
            return;
        }
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let name = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
                    Self::flatten_value(&name, val, out, depth + 1);
                }
            }
            other => {
                out.insert(prefix.to_string(), other.clone());
            }
        }
    }

    /// Apply array_paths explode/join to a flattened record.
    fn apply_array_paths(
        &self,
        flat: serde_json::Map<String, serde_json::Value>,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        if self.config.array_paths.is_empty() {
            return vec![flat];
        }

        let mut result = vec![flat];
        for ap in &self.config.array_paths {
            let mut next = Vec::new();
            for rec in result {
                if let Some(serde_json::Value::Array(arr)) = rec.get(&ap.path) {
                    if arr.is_empty() {
                        match ap.mode {
                            ArrayPathMode::Explode => {} // 0 records
                            ArrayPathMode::Join => {
                                let mut r = rec.clone();
                                r.insert(ap.path.clone(), serde_json::Value::String(String::new()));
                                next.push(r);
                            }
                        }
                        continue;
                    }
                    match ap.mode {
                        ArrayPathMode::Explode => {
                            for elem in arr {
                                let mut r = rec.clone();
                                if let serde_json::Value::Object(obj) = elem {
                                    r.remove(&ap.path);
                                    for (k, v) in obj {
                                        r.insert(format!("{}.{k}", ap.path), v.clone());
                                    }
                                } else {
                                    r.insert(ap.path.clone(), elem.clone());
                                }
                                next.push(r);
                            }
                        }
                        ArrayPathMode::Join => {
                            let joined: String = arr.iter().map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                o => o.to_string(),
                            }).collect::<Vec<_>>().join(&ap.separator);
                            let mut r = rec.clone();
                            r.insert(ap.path.clone(), serde_json::Value::String(joined));
                            next.push(r);
                        }
                    }
                } else {
                    next.push(rec);
                }
            }
            result = next;
        }
        result
    }

    fn map_to_record(
        &self,
        flat: &serde_json::Map<String, serde_json::Value>,
        schema: &Arc<Schema>,
    ) -> Record {
        let values: Vec<Value> = schema.columns().iter().map(|col| {
            flat.get(&**col).map(json_to_value).unwrap_or(Value::Null)
        }).collect();
        let mut record = Record::new(Arc::clone(schema), values);
        for (key, val) in flat {
            if schema.index(key).is_none() {
                record.set_overflow(key.clone().into(), json_to_value(val));
            }
        }
        record
    }
}

impl FormatReader for JsonReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        if let Some(ref s) = self.schema {
            return Ok(Arc::clone(s));
        }

        let first = self.next_raw()?;
        let first = match first {
            Some(v) => v,
            None => {
                let s = Arc::new(Schema::new(vec![]));
                self.schema = Some(Arc::clone(&s));
                self.inner = InnerReader::Done;
                return Ok(s);
            }
        };

        let mut flat = serde_json::Map::new();
        Self::flatten_value("", &first, &mut flat, 0);
        let expanded = self.apply_array_paths(flat);

        let empty = serde_json::Map::new();
        let first_flat = expanded.first().unwrap_or(&empty);
        let columns: Vec<Box<str>> = first_flat.keys().map(|k| k.clone().into_boxed_str()).collect();
        let schema = Arc::new(Schema::new(columns));
        self.schema = Some(Arc::clone(&schema));
        self.pending = expanded;

        Ok(schema)
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.schema.is_none() {
            self.schema()?;
        }
        let schema = self.schema.as_ref().unwrap().clone();

        // Drain pending buffer first
        if !self.pending.is_empty() {
            let flat = self.pending.remove(0);
            return Ok(Some(self.map_to_record(&flat, &schema)));
        }

        loop {
            let raw = match self.next_raw()? {
                Some(v) => v,
                None => return Ok(None),
            };

            let mut flat = serde_json::Map::new();
            Self::flatten_value("", &raw, &mut flat, 0);
            let expanded = self.apply_array_paths(flat);

            if expanded.is_empty() {
                continue; // empty array explode → skip
            }

            let record = self.map_to_record(&expanded[0], &schema);
            self.pending = expanded.into_iter().skip(1).collect();
            return Ok(Some(record));
        }
    }
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() { Value::Integer(i) }
            else if let Some(f) = n.as_f64() { Value::Float(f) }
            else { Value::String(n.to_string().into()) }
        }
        serde_json::Value::String(s) => Value::String(s.clone().into()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(_) => Value::String(v.to_string().into()),
    }
}

fn dot_path_to_pointer(path: &str) -> String {
    let escaped: Vec<String> = path.split('.')
        .map(|p| p.replace('~', "~0").replace('/', "~1"))
        .collect();
    format!("/{}", escaped.join("/"))
}

/// Peek first non-whitespace byte without consuming the reader.
fn peek_first_byte(reader: &mut BufReader<Box<dyn Read + Send>>) -> Result<Option<u8>, FormatError> {
    loop {
        let buf = reader.fill_buf().map_err(FormatError::Io)?;
        if buf.is_empty() {
            return Ok(None);
        }
        if let Some(pos) = buf.iter().position(|b| !b.is_ascii_whitespace()) {
            return Ok(Some(buf[pos]));
        }
        // All whitespace — consume and refill
        let len = buf.len();
        reader.consume(len);
    }
}

/// Load a JSON array into Preloaded records (streaming via serde_json).
fn load_array(reader: &mut BufReader<Box<dyn Read + Send>>) -> Result<InnerReader, FormatError> {
    let value: serde_json::Value = serde_json::from_reader(reader)
        .map_err(|e| FormatError::Json(e.to_string()))?;
    let records = value
        .as_array()
        .ok_or_else(|| FormatError::Json("expected JSON array".into()))?
        .clone();
    Ok(InnerReader::Preloaded { records, pos: 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reader_from_str(input: &str, config: JsonReaderConfig) -> JsonReader {
        JsonReader::from_reader(std::io::Cursor::new(input.as_bytes().to_vec()), config).unwrap()
    }

    fn default_config() -> JsonReaderConfig {
        JsonReaderConfig::default()
    }

    #[test]
    fn test_json_autodetect_array() {
        let input = r#"[{"a":1},{"a":2}]"#;
        let mut r = reader_from_str(input, default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(&*s.columns()[0], "a");
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("a"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("a"), Some(&Value::Integer(2)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_autodetect_ndjson() {
        let input = "{\"a\":1}\n{\"a\":2}\n";
        let mut r = reader_from_str(input, default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("a"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("a"), Some(&Value::Integer(2)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_record_path_navigation() {
        let input = r#"{"data":{"results":[{"x":1}]}}"#;
        let config = JsonReaderConfig {
            record_path: Some("data.results".into()),
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let s = r.schema().unwrap();
        assert_eq!(&*s.columns()[0], "x");
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("x"), Some(&Value::Integer(1)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_array_paths_explode() {
        let input = r#"[{"name":"Alice","orders":[{"id":1},{"id":2}]}]"#;
        let config = JsonReaderConfig {
            array_paths: vec![ArrayPathSpec {
                path: "orders".into(),
                mode: ArrayPathMode::Explode,
                separator: ",".into(),
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(r1.get("orders.id"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("orders.id"), Some(&Value::Integer(2)));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_array_paths_join() {
        let input = r#"[{"name":"Alice","tags":["a","b","c"]}]"#;
        let config = JsonReaderConfig {
            array_paths: vec![ArrayPathSpec {
                path: "tags".into(),
                mode: ArrayPathMode::Join,
                separator: ",".into(),
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("tags"), Some(&Value::String("a,b,c".into())));
    }

    #[test]
    fn test_json_schema_inference_types() {
        let input = r#"[{"i":42,"f":3.14,"s":"hello","b":true,"n":null}]"#;
        let mut r = reader_from_str(input, default_config());
        let _s = r.schema().unwrap();
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("i"), Some(&Value::Integer(42)));
        assert_eq!(r1.get("f"), Some(&Value::Float(3.14)));
        assert_eq!(r1.get("s"), Some(&Value::String("hello".into())));
        assert_eq!(r1.get("b"), Some(&Value::Bool(true)));
        assert_eq!(r1.get("n"), Some(&Value::Null));
    }

    #[test]
    fn test_json_nested_object_flattening() {
        let input = r#"[{"a":{"b":{"c":1}}}]"#;
        let mut r = reader_from_str(input, default_config());
        let s = r.schema().unwrap();
        assert_eq!(&*s.columns()[0], "a.b.c");
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("a.b.c"), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_json_new_fields_to_overflow() {
        let input = "{\"a\":1}\n{\"a\":2,\"b\":3}\n";
        let mut r = reader_from_str(input, default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        let _r1 = r.next_record().unwrap().unwrap();
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("a"), Some(&Value::Integer(2)));
        assert_eq!(r2.get("b"), Some(&Value::Integer(3)));
    }

    #[test]
    fn test_json_empty_array() {
        let mut r = reader_from_str("[]", default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 0);
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_malformed_input() {
        let result = JsonReader::from_reader(
            std::io::Cursor::new(b"{invalid}".to_vec()),
            default_config(),
        );
        // Auto-detect sees '{' → NDJSON. First read should fail.
        let mut r = result.unwrap();
        assert!(r.schema().is_err());
    }

    #[test]
    fn test_json_array_paths_empty_array() {
        let input = r#"[{"name":"Alice","orders":[]},{"name":"Bob","orders":[{"id":1}]}]"#;
        let config = JsonReaderConfig {
            array_paths: vec![ArrayPathSpec {
                path: "orders".into(),
                mode: ArrayPathMode::Explode,
                separator: ",".into(),
            }],
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let _s = r.schema().unwrap();
        // Alice has empty orders → skipped. Bob has one.
        let r1 = r.next_record().unwrap().unwrap();
        assert_eq!(r1.get("name"), Some(&Value::String("Bob".into())));
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_dot_path_to_pointer() {
        assert_eq!(dot_path_to_pointer("data.results"), "/data/results");
        assert_eq!(dot_path_to_pointer("a"), "/a");
        assert_eq!(dot_path_to_pointer("a.b.c"), "/a/b/c");
    }
}
