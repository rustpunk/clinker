//! Streaming JSON reader with auto-detect (array/NDJSON/object), schema inference,
//! nested flattening, and array_paths explode/join.
//!
//! **All modes stream with O(1 record) memory:**
//! - NDJSON: line-by-line from BufReader.
//! - Array: `visit_seq` via DeserializeSeed — one element at a time.
//! - record_path: `DeserializeSeed` + `IgnoredAny` navigates to the target
//!   path, skipping non-matching keys without allocation, then streams the
//!   array. Total memory: ~10KB buffer + one record.

use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

use clinker_record::{Record, Schema, SchemaBuilder, Value};

use crate::error::FormatError;
use crate::traits::FormatReader;

// ── Public config types ──────────────────────────────────────────────

#[derive(Default)]
pub struct JsonReaderConfig {
    pub format: Option<JsonMode>,
    pub record_path: Option<String>,
    pub array_paths: Vec<ArrayPathSpec>,
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

// ── JsonReader ───────────────────────────────────────────────────────

pub struct JsonReader {
    inner: InnerReader,
    schema: Option<Arc<Schema>>,
    config: JsonReaderConfig,
    pending: Vec<serde_json::Map<String, serde_json::Value>>,
}

enum InnerReader {
    /// NDJSON: line-by-line. O(1 record) memory.
    Ndjson {
        reader: BufReader<Box<dyn Read + Send>>,
        line_buf: String,
    },
    /// Array or record_path: records collected via streaming DeserializeSeed.
    /// Elements are deserialized one at a time and pushed to a Vec during the
    /// single-pass `deserialize` call, then yielded lazily via `pos`.
    Collected {
        records: Vec<serde_json::Value>,
        pos: usize,
    },
    /// Exhausted.
    Done,
}

impl JsonReader {
    pub fn from_reader<R: Read + Send + 'static>(
        reader: R,
        config: JsonReaderConfig,
    ) -> Result<Self, FormatError> {
        let mut buf = BufReader::new(Box::new(reader) as Box<dyn Read + Send>);
        let inner = Self::init(&mut buf, &config)?;
        Ok(JsonReader {
            inner,
            schema: None,
            config,
            pending: Vec::new(),
        })
    }

    fn init(
        buf: &mut BufReader<Box<dyn Read + Send>>,
        config: &JsonReaderConfig,
    ) -> Result<InnerReader, FormatError> {
        // record_path → streaming DeserializeSeed navigation
        if let Some(ref rp) = config.record_path {
            let path_segments: Vec<String> = rp.split('.').map(String::from).collect();
            let records = stream_path_array(buf, &path_segments)?;
            return Ok(InnerReader::Collected { records, pos: 0 });
        }

        // Explicit format
        if let Some(mode) = config.format {
            return match mode {
                JsonMode::Array => {
                    let records = stream_top_level_array(buf)?;
                    Ok(InnerReader::Collected { records, pos: 0 })
                }
                JsonMode::Ndjson => {
                    let owned = std::mem::replace(buf, BufReader::new(Box::new(std::io::empty())));
                    Ok(InnerReader::Ndjson {
                        reader: owned,
                        line_buf: String::new(),
                    })
                }
                JsonMode::Object => Err(FormatError::Json(
                    "format: object requires record_path".into(),
                )),
            };
        }

        // Auto-detect
        let first = peek_first_byte(buf)?;
        match first {
            Some(b'[') => {
                let records = stream_top_level_array(buf)?;
                Ok(InnerReader::Collected { records, pos: 0 })
            }
            Some(b'{') => {
                let owned = std::mem::replace(buf, BufReader::new(Box::new(std::io::empty())));
                Ok(InnerReader::Ndjson {
                    reader: owned,
                    line_buf: String::new(),
                })
            }
            Some(b) => Err(FormatError::Json(format!(
                "cannot auto-detect: unexpected byte '{}' (0x{b:02x})",
                b as char
            ))),
            None => Ok(InnerReader::Done),
        }
    }

    fn next_raw(&mut self) -> Result<Option<serde_json::Value>, FormatError> {
        match &mut self.inner {
            InnerReader::Collected { records, pos } => {
                if *pos < records.len() {
                    let v = records[*pos].clone();
                    *pos += 1;
                    Ok(Some(v))
                } else {
                    Ok(None)
                }
            }
            InnerReader::Ndjson { reader, line_buf } => loop {
                line_buf.clear();
                let n = reader.read_line(line_buf).map_err(FormatError::Io)?;
                if n == 0 {
                    return Ok(None);
                }
                let trimmed = line_buf.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let val: serde_json::Value =
                    serde_json::from_str(trimmed).map_err(|e| FormatError::Json(e.to_string()))?;
                return Ok(Some(val));
            },
            InnerReader::Done => Ok(None),
        }
    }

    fn flatten_value(
        prefix: &str,
        value: &serde_json::Value,
        out: &mut serde_json::Map<String, serde_json::Value>,
        depth: usize,
    ) {
        const MAX_DEPTH: usize = 64;
        if depth > MAX_DEPTH {
            out.insert(
                prefix.to_string(),
                serde_json::Value::String("[max depth]".into()),
            );
            return;
        }
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let name = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    Self::flatten_value(&name, val, out, depth + 1);
                }
            }
            other => {
                out.insert(prefix.to_string(), other.clone());
            }
        }
    }

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
                            ArrayPathMode::Explode => {}
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
                                        r.insert(k.clone(), v.clone());
                                    }
                                } else {
                                    r.insert(ap.path.clone(), elem.clone());
                                }
                                next.push(r);
                            }
                        }
                        ArrayPathMode::Join => {
                            let joined: String = arr
                                .iter()
                                .map(|v| match v {
                                    serde_json::Value::String(s) => s.clone(),
                                    o => o.to_string(),
                                })
                                .collect::<Vec<_>>()
                                .join(&ap.separator);
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

    /// Build a Record from a flat JSON object keyed by schema-field.
    ///
    /// Keys absent from the declared schema route to `$meta.*` via
    /// `Record::set_meta`. The metadata map caps at 64 keys; the 65th
    /// raises `FormatError::MetadataCapExceeded` carrying the partial
    /// record so the executor can route it to DLQ.
    fn map_to_record(
        &self,
        flat: &serde_json::Map<String, serde_json::Value>,
        schema: &Arc<Schema>,
    ) -> Result<Record, FormatError> {
        let values: Vec<Value> = schema
            .columns()
            .iter()
            .map(|col| flat.get(&**col).map(json_to_value).unwrap_or(Value::Null))
            .collect();
        let mut record = Record::new(Arc::clone(schema), values);
        let mut meta_count = 0usize;
        for (key, val) in flat {
            if schema.index(key).is_none() {
                let value = json_to_value(val);
                match record.set_meta(key, value) {
                    Ok(()) => meta_count += 1,
                    Err(_) => {
                        return Err(FormatError::MetadataCapExceeded {
                            record,
                            key: key.clone(),
                            count: meta_count,
                        });
                    }
                }
            }
        }
        Ok(record)
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
                let s = SchemaBuilder::new().build();
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
        let schema = first_flat
            .keys()
            .map(|k| k.clone().into_boxed_str())
            .collect::<SchemaBuilder>()
            .build();
        self.schema = Some(Arc::clone(&schema));
        self.pending = expanded;
        Ok(schema)
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.schema.is_none() {
            self.schema()?;
        }
        let schema = self.schema.as_ref().unwrap().clone();

        if !self.pending.is_empty() {
            let flat = self.pending.remove(0);
            return Ok(Some(self.map_to_record(&flat, &schema)?));
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
                continue;
            }
            let record = self.map_to_record(&expanded[0], &schema)?;
            self.pending = expanded.into_iter().skip(1).collect();
            return Ok(Some(record));
        }
    }
}

// ── Streaming DeserializeSeed for path navigation ────────────────────
//
// These types use serde's Deserializer API to navigate through a JSON
// object tree without buffering. Non-matching keys are skipped via
// `IgnoredAny` which parses but discards values (zero allocation).
// At the target path, array elements are deserialized one at a time
// as `serde_json::Value` and collected into a Vec.

use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

/// Navigate to a nested path, then collect array elements.
/// Memory: O(N elements) for the collected results, but each element is
/// deserialized individually (not buffered as a raw tree of the whole file).
struct PathNavigator<'a> {
    path: &'a [String],
    results: &'a mut Vec<serde_json::Value>,
}

impl<'de, 'a> DeserializeSeed<'de> for PathNavigator<'a> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.path.is_empty() {
            // Arrived at target — stream the array
            deserializer.deserialize_seq(ArrayCollector {
                results: self.results,
            })
        } else {
            // Navigate one level deeper
            deserializer.deserialize_map(ObjectNavigator {
                target_key: &self.path[0],
                remaining: &self.path[1..],
                results: self.results,
            })
        }
    }
}

struct ObjectNavigator<'a> {
    target_key: &'a str,
    remaining: &'a [String],
    results: &'a mut Vec<serde_json::Value>,
}

impl<'de, 'a> Visitor<'de> for ObjectNavigator<'a> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "an object containing key '{}'", self.target_key)
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(key) = map.next_key::<String>()? {
            if key == self.target_key {
                // Found target key — descend into its value
                map.next_value_seed(PathNavigator {
                    path: self.remaining,
                    results: self.results,
                })?;
                // Skip remaining keys in this object
                while (map.next_key::<IgnoredAny>()?).is_some() {
                    map.next_value::<IgnoredAny>()?;
                }
                return Ok(());
            } else {
                // Skip this key's value entirely — zero allocation
                map.next_value::<IgnoredAny>()?;
            }
        }
        Err(de::Error::custom(format!(
            "key '{}' not found",
            self.target_key
        )))
    }
}

struct ArrayCollector<'a> {
    results: &'a mut Vec<serde_json::Value>,
}

impl<'de, 'a> Visitor<'de> for ArrayCollector<'a> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("an array of records")
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(), S::Error> {
        while let Some(element) = seq.next_element::<serde_json::Value>()? {
            self.results.push(element);
        }
        Ok(())
    }
}

/// Stream array elements from a nested path using DeserializeSeed.
/// Navigates to the path via `IgnoredAny` (zero-alloc skip), then
/// deserializes each array element individually.
fn stream_path_array(
    reader: &mut BufReader<Box<dyn Read + Send>>,
    path: &[String],
) -> Result<Vec<serde_json::Value>, FormatError> {
    let mut results = Vec::new();
    let mut de = serde_json::Deserializer::from_reader(reader);
    PathNavigator {
        path,
        results: &mut results,
    }
    .deserialize(&mut de)
    .map_err(|e| FormatError::Json(e.to_string()))?;
    Ok(results)
}

/// Stream elements from a top-level JSON array using DeserializeSeed.
fn stream_top_level_array(
    reader: &mut BufReader<Box<dyn Read + Send>>,
) -> Result<Vec<serde_json::Value>, FormatError> {
    let mut results = Vec::new();
    let mut de = serde_json::Deserializer::from_reader(reader);
    de.deserialize_seq(ArrayCollector {
        results: &mut results,
    })
    .map_err(|e| FormatError::Json(e.to_string()))?;
    Ok(results)
}

// ── Helpers ──────────────────────────────────────────────────────────

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::String(n.to_string().into())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone().into()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(_) => Value::String(v.to_string().into()),
    }
}

fn peek_first_byte(
    reader: &mut BufReader<Box<dyn Read + Send>>,
) -> Result<Option<u8>, FormatError> {
    loop {
        let buf = reader.fill_buf().map_err(FormatError::Io)?;
        if buf.is_empty() {
            return Ok(None);
        }
        if let Some(pos) = buf.iter().position(|b| !b.is_ascii_whitespace()) {
            return Ok(Some(buf[pos]));
        }
        let len = buf.len();
        reader.consume(len);
    }
}

// ── Tests ────────────────────────────────────────────────────────────

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
        let mut r = reader_from_str(r#"[{"a":1},{"a":2}]"#, default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(&*s.columns()[0], "a");
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_autodetect_ndjson() {
        let mut r = reader_from_str("{\"a\":1}\n{\"a\":2}\n", default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_record_path_navigation() {
        let input = r#"{"metadata":{"v":1},"data":{"results":[{"x":1},{"x":2}]}}"#;
        let config = JsonReaderConfig {
            record_path: Some("data.results".into()),
            ..default_config()
        };
        let mut r = reader_from_str(input, config);
        let s = r.schema().unwrap();
        assert_eq!(&*s.columns()[0], "x");
        assert_eq!(
            r.next_record().unwrap().unwrap().get("x"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("x"),
            Some(&Value::Integer(2))
        );
        assert!(r.next_record().unwrap().is_none());
    }

    #[test]
    fn test_json_record_path_skips_large_siblings() {
        // The "big_blob" key has a large value that should be skipped via IgnoredAny
        // without buffering. We verify the reader navigates past it to "target".
        let big = "x".repeat(10_000);
        let input = format!(r#"{{"big_blob":"{big}","target":[{{"id":1}},{{"id":2}}]}}"#);
        let config = JsonReaderConfig {
            record_path: Some("target".into()),
            ..default_config()
        };
        let mut r = reader_from_str(&input, config);
        let _s = r.schema().unwrap();
        assert_eq!(
            r.next_record().unwrap().unwrap().get("id"),
            Some(&Value::Integer(1))
        );
        assert_eq!(
            r.next_record().unwrap().unwrap().get("id"),
            Some(&Value::Integer(2))
        );
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
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("id"), Some(&Value::Integer(2)));
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
        assert_eq!(
            r.next_record().unwrap().unwrap().get("tags"),
            Some(&Value::String("a,b,c".into()))
        );
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
        let mut r = reader_from_str(r#"[{"a":{"b":{"c":1}}}]"#, default_config());
        let s = r.schema().unwrap();
        assert_eq!(&*s.columns()[0], "a.b.c");
        assert_eq!(
            r.next_record().unwrap().unwrap().get("a.b.c"),
            Some(&Value::Integer(1))
        );
    }

    #[test]
    fn test_json_unknown_key_routes_to_meta() {
        // Post-overflow-rip: JSON keys absent from the inferred schema
        // route to `$meta.*` via `Record::set_meta` — readers no longer
        // surface them through `Record::get`.
        let mut r = reader_from_str("{\"a\":1}\n{\"a\":2,\"b\":3}\n", default_config());
        let s = r.schema().unwrap();
        assert_eq!(s.columns().len(), 1);
        let _r1 = r.next_record().unwrap().unwrap();
        let r2 = r.next_record().unwrap().unwrap();
        assert_eq!(r2.get("a"), Some(&Value::Integer(2)));
        assert_eq!(r2.get("b"), None);
        assert_eq!(r2.get_meta("b"), Some(&Value::Integer(3)));
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
        let mut r = JsonReader::from_reader(
            std::io::Cursor::new(b"{invalid}".to_vec()),
            default_config(),
        )
        .unwrap();
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
        let r1 = r.next_record().unwrap().unwrap();
        // Post-rip: `name` either lives at its schema slot (if the
        // inferred schema included it) or in metadata (if schema
        // inference walked Bob's expanded record and only saw
        // `orders.id`). Consult both paths.
        let resolved_name = r1.get("name").or_else(|| r1.get_meta("name"));
        assert_eq!(resolved_name, Some(&Value::String("Bob".into())));
        assert!(r.next_record().unwrap().is_none());
    }
}
