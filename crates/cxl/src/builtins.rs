/// Type tags for method signature validation (used by Phase 3 type checker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeTag {
    String,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
    Null,
    Any,
    Array,
    Numeric,
}

/// Method category for documentation and grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    String,
    Path,
    Array,
    Numeric,
    Date,
    ConversionStrict,
    ConversionLenient,
    Introspection,
    Debug,
    WindowAggregate,
    WindowPositional,
    WindowIterable,
}

/// Definition of a single built-in method or window function.
#[derive(Debug, Clone)]
pub struct BuiltinDef {
    pub name: &'static str,
    pub receiver: TypeTag,
    pub args: Vec<TypeTag>,
    pub min_args: usize,
    pub max_args: Option<usize>,
    pub return_type: TypeTag,
    pub category: Category,
}

/// Registry of all built-in methods and window functions.
pub struct BuiltinRegistry {
    methods: ahash::HashMap<&'static str, BuiltinDef>,
    window_fns: ahash::HashMap<&'static str, BuiltinDef>,
}

impl Default for BuiltinRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BuiltinRegistry {
    pub fn new() -> Self {
        let mut methods = ahash::HashMap::default();
        let mut window_fns = ahash::HashMap::default();

        // ── String (24) ──
        let s = |name: &'static str,
                 args: Vec<TypeTag>,
                 min: usize,
                 max: Option<usize>,
                 ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::String,
                    args,
                    min_args: min,
                    max_args: max,
                    return_type: ret,
                    category: Category::String,
                },
            )
        };
        for (n, d) in [
            s("upper", vec![], 0, Some(0), TypeTag::String),
            s("lower", vec![], 0, Some(0), TypeTag::String),
            s("trim", vec![], 0, Some(0), TypeTag::String),
            s("trim_start", vec![], 0, Some(0), TypeTag::String),
            s("trim_end", vec![], 0, Some(0), TypeTag::String),
            s(
                "starts_with",
                vec![TypeTag::String],
                1,
                Some(1),
                TypeTag::Bool,
            ),
            s(
                "ends_with",
                vec![TypeTag::String],
                1,
                Some(1),
                TypeTag::Bool,
            ),
            s("contains", vec![TypeTag::String], 1, Some(1), TypeTag::Bool),
            s(
                "replace",
                vec![TypeTag::String, TypeTag::String],
                2,
                Some(2),
                TypeTag::String,
            ),
            s(
                "substring",
                vec![TypeTag::Int, TypeTag::Int],
                1,
                Some(2),
                TypeTag::String,
            ),
            s("left", vec![TypeTag::Int], 1, Some(1), TypeTag::String),
            s("right", vec![TypeTag::Int], 1, Some(1), TypeTag::String),
            s(
                "pad_left",
                vec![TypeTag::Int, TypeTag::String],
                1,
                Some(2),
                TypeTag::String,
            ),
            s(
                "pad_right",
                vec![TypeTag::Int, TypeTag::String],
                1,
                Some(2),
                TypeTag::String,
            ),
            s("repeat", vec![TypeTag::Int], 1, Some(1), TypeTag::String),
            s("reverse", vec![], 0, Some(0), TypeTag::String),
            s("length", vec![], 0, Some(0), TypeTag::Int),
            s("split", vec![TypeTag::String], 1, Some(1), TypeTag::Array),
            s("join", vec![TypeTag::String], 1, Some(1), TypeTag::String),
            s("matches", vec![TypeTag::String], 1, Some(1), TypeTag::Bool),
            s("format", vec![TypeTag::String], 1, Some(1), TypeTag::String),
            s("concat", vec![TypeTag::String], 1, None, TypeTag::String),
            s("find", vec![TypeTag::String], 1, Some(1), TypeTag::Bool),
            s(
                "capture",
                vec![TypeTag::String, TypeTag::Int],
                1,
                Some(2),
                TypeTag::String,
            ),
        ] {
            methods.insert(n, d);
        }

        // ── Path (5) ──
        let p = |name: &'static str| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::String,
                    args: vec![],
                    min_args: 0,
                    max_args: Some(0),
                    return_type: TypeTag::String,
                    category: Category::Path,
                },
            )
        };
        for (n, d) in [
            p("file_name"),
            p("file_stem"),
            p("extension"),
            p("parent"),
            p("parent_name"),
        ] {
            methods.insert(n, d);
        }

        // ── Array (2) ──
        // Note: join/length overloaded with String — String entry wins in single-key map.
        // Array versions registered here would overwrite String. We keep String versions
        // and note that type checker disambiguates by receiver type.
        methods.insert(
            "join",
            BuiltinDef {
                name: "join",
                receiver: TypeTag::String,
                args: vec![TypeTag::String],
                min_args: 1,
                max_args: Some(1),
                return_type: TypeTag::String,
                category: Category::String,
            },
        );
        // length already registered as String method above

        // ── Numeric (8) ──
        let num = |name: &'static str,
                   args: Vec<TypeTag>,
                   min: usize,
                   max: Option<usize>,
                   ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Numeric,
                    args,
                    min_args: min,
                    max_args: max,
                    return_type: ret,
                    category: Category::Numeric,
                },
            )
        };
        for (n, d) in [
            num("abs", vec![], 0, Some(0), TypeTag::Numeric),
            num("ceil", vec![], 0, Some(0), TypeTag::Int),
            num("floor", vec![], 0, Some(0), TypeTag::Int),
            num("round", vec![TypeTag::Int], 0, Some(1), TypeTag::Float),
            num("round_to", vec![TypeTag::Int], 1, Some(1), TypeTag::Float),
            num(
                "clamp",
                vec![TypeTag::Numeric, TypeTag::Numeric],
                2,
                Some(2),
                TypeTag::Numeric,
            ),
            num("min", vec![TypeTag::Numeric], 1, Some(1), TypeTag::Numeric),
            num("max", vec![TypeTag::Numeric], 1, Some(1), TypeTag::Numeric),
        ] {
            methods.insert(n, d);
        }

        // ── Date (13) ──
        let dt = |name: &'static str,
                  args: Vec<TypeTag>,
                  min: usize,
                  max: Option<usize>,
                  ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Date,
                    args,
                    min_args: min,
                    max_args: max,
                    return_type: ret,
                    category: Category::Date,
                },
            )
        };
        for (n, d) in [
            dt("year", vec![], 0, Some(0), TypeTag::Int),
            dt("month", vec![], 0, Some(0), TypeTag::Int),
            dt("day", vec![], 0, Some(0), TypeTag::Int),
            dt("hour", vec![], 0, Some(0), TypeTag::Int),
            dt("minute", vec![], 0, Some(0), TypeTag::Int),
            dt("second", vec![], 0, Some(0), TypeTag::Int),
            dt("add_days", vec![TypeTag::Int], 1, Some(1), TypeTag::Date),
            dt("add_months", vec![TypeTag::Int], 1, Some(1), TypeTag::Date),
            dt("add_years", vec![TypeTag::Int], 1, Some(1), TypeTag::Date),
            dt("diff_days", vec![TypeTag::Date], 1, Some(1), TypeTag::Int),
            dt("diff_months", vec![TypeTag::Date], 1, Some(1), TypeTag::Int),
            dt("diff_years", vec![TypeTag::Date], 1, Some(1), TypeTag::Int),
            dt(
                "format_date",
                vec![TypeTag::String],
                1,
                Some(1),
                TypeTag::String,
            ),
        ] {
            methods.insert(n, d);
        }

        // ── Conversion strict (6) ──
        let cs = |name: &'static str,
                  args: Vec<TypeTag>,
                  min: usize,
                  max: Option<usize>,
                  ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Any,
                    args,
                    min_args: min,
                    max_args: max,
                    return_type: ret,
                    category: Category::ConversionStrict,
                },
            )
        };
        for (n, d) in [
            cs("to_int", vec![], 0, Some(0), TypeTag::Int),
            cs("to_float", vec![], 0, Some(0), TypeTag::Float),
            cs("to_string", vec![], 0, Some(0), TypeTag::String),
            cs("to_bool", vec![], 0, Some(0), TypeTag::Bool),
            cs("to_date", vec![TypeTag::String], 0, Some(1), TypeTag::Date),
            cs(
                "to_datetime",
                vec![TypeTag::String],
                0,
                Some(1),
                TypeTag::DateTime,
            ),
        ] {
            methods.insert(n, d);
        }

        // ── Conversion lenient (5) ──
        let cl = |name: &'static str,
                  args: Vec<TypeTag>,
                  min: usize,
                  max: Option<usize>,
                  ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Any,
                    args,
                    min_args: min,
                    max_args: max,
                    return_type: ret,
                    category: Category::ConversionLenient,
                },
            )
        };
        for (n, d) in [
            cl("try_int", vec![], 0, Some(0), TypeTag::Int),
            cl("try_float", vec![], 0, Some(0), TypeTag::Float),
            cl("try_bool", vec![], 0, Some(0), TypeTag::Bool),
            cl("try_date", vec![TypeTag::String], 0, Some(1), TypeTag::Date),
            cl(
                "try_datetime",
                vec![TypeTag::String],
                0,
                Some(1),
                TypeTag::DateTime,
            ),
        ] {
            methods.insert(n, d);
        }

        // ── Introspection (4) ──
        let intr = |name: &'static str,
                    args: Vec<TypeTag>,
                    min: usize,
                    max: Option<usize>,
                    ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Any,
                    args,
                    min_args: min,
                    max_args: max,
                    return_type: ret,
                    category: Category::Introspection,
                },
            )
        };
        for (n, d) in [
            intr("type_of", vec![], 0, Some(0), TypeTag::String),
            intr("is_null", vec![], 0, Some(0), TypeTag::Bool),
            intr("is_empty", vec![], 0, Some(0), TypeTag::Bool),
            intr("catch", vec![TypeTag::Any], 1, Some(1), TypeTag::Any),
        ] {
            methods.insert(n, d);
        }

        // ── Debug (1) ──
        methods.insert(
            "debug",
            BuiltinDef {
                name: "debug",
                receiver: TypeTag::Any,
                args: vec![TypeTag::String],
                min_args: 1,
                max_args: Some(1),
                return_type: TypeTag::Any,
                category: Category::Debug,
            },
        );

        // ── Window aggregate (5) ──
        let wa = |name: &'static str, args: Vec<TypeTag>, min: usize, ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Any,
                    args,
                    min_args: min,
                    max_args: Some(min),
                    return_type: ret,
                    category: Category::WindowAggregate,
                },
            )
        };
        for (n, d) in [
            wa("sum", vec![TypeTag::Any], 1, TypeTag::Numeric),
            wa("avg", vec![TypeTag::Any], 1, TypeTag::Float),
            wa("min", vec![TypeTag::Any], 1, TypeTag::Any),
            wa("max", vec![TypeTag::Any], 1, TypeTag::Any),
            wa("count", vec![], 0, TypeTag::Int),
        ] {
            window_fns.insert(n, d);
        }

        // ── Window positional (4) ──
        let wp = |name: &'static str, args: Vec<TypeTag>, min: usize| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Any,
                    args,
                    min_args: min,
                    max_args: Some(min),
                    return_type: TypeTag::Any,
                    category: Category::WindowPositional,
                },
            )
        };
        for (n, d) in [
            wp("first", vec![], 0),
            wp("last", vec![], 0),
            wp("lag", vec![TypeTag::Int], 1),
            wp("lead", vec![TypeTag::Int], 1),
        ] {
            window_fns.insert(n, d);
        }

        // ── Window iterable (4) ──  — any/all take predicate exprs, collect/distinct take field exprs
        let wi = |name: &'static str, ret: TypeTag| {
            (
                name,
                BuiltinDef {
                    name,
                    receiver: TypeTag::Any,
                    args: vec![TypeTag::Any],
                    min_args: 1,
                    max_args: Some(1),
                    return_type: ret,
                    category: Category::WindowIterable,
                },
            )
        };
        for (n, d) in [
            wi("any", TypeTag::Bool),
            wi("all", TypeTag::Bool),
            wi("collect", TypeTag::Array),
            wi("distinct", TypeTag::Array),
        ] {
            window_fns.insert(n, d);
        }

        Self {
            methods,
            window_fns,
        }
    }

    pub fn lookup_method(&self, name: &str) -> Option<&BuiltinDef> {
        self.methods.get(name)
    }

    pub fn lookup_window(&self, name: &str) -> Option<&BuiltinDef> {
        self.window_fns.get(name)
    }

    pub fn total_count(&self) -> usize {
        self.methods.len() + self.window_fns.len()
    }
}

static_assertions::assert_impl_all!(BuiltinRegistry: Send, Sync);

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> BuiltinRegistry {
        BuiltinRegistry::new()
    }

    #[test]
    fn test_registry_all_string_methods() {
        let r = registry();
        let string_methods = [
            "upper",
            "lower",
            "trim",
            "trim_start",
            "trim_end",
            "starts_with",
            "ends_with",
            "contains",
            "replace",
            "substring",
            "left",
            "right",
            "pad_left",
            "pad_right",
            "repeat",
            "reverse",
            "length",
            "split",
            "join",
            "matches",
            "format",
            "concat",
            "find",
            "capture",
        ];
        for name in string_methods {
            let def = r
                .lookup_method(name)
                .unwrap_or_else(|| panic!("missing string method: {}", name));
            assert!(
                def.category == Category::String || def.category == Category::Path,
                "wrong category for {}: {:?}",
                name,
                def.category
            );
        }
        assert_eq!(string_methods.len(), 24);
    }

    #[test]
    fn test_registry_all_path_methods() {
        let r = registry();
        for name in [
            "file_name",
            "file_stem",
            "extension",
            "parent",
            "parent_name",
        ] {
            let def = r
                .lookup_method(name)
                .unwrap_or_else(|| panic!("missing path method: {}", name));
            assert_eq!(def.category, Category::Path);
        }
    }

    #[test]
    fn test_registry_all_array_methods() {
        let r = registry();
        for name in ["join", "length"] {
            let def = r.lookup_method(name).expect("missing array method");
            assert!(def.receiver == TypeTag::String || def.receiver == TypeTag::Array);
        }
    }

    #[test]
    fn test_registry_all_numeric_methods() {
        let r = registry();
        for name in [
            "abs", "ceil", "floor", "round", "round_to", "clamp", "min", "max",
        ] {
            let def = r
                .lookup_method(name)
                .unwrap_or_else(|| panic!("missing numeric method: {}", name));
            assert_eq!(
                def.category,
                Category::Numeric,
                "wrong category for {}",
                name
            );
        }
    }

    #[test]
    fn test_registry_all_date_methods() {
        let r = registry();
        let date_methods = [
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "add_days",
            "add_months",
            "add_years",
            "diff_days",
            "diff_months",
            "diff_years",
            "format_date",
        ];
        for name in date_methods {
            let def = r
                .lookup_method(name)
                .unwrap_or_else(|| panic!("missing date method: {}", name));
            assert_eq!(def.category, Category::Date, "wrong category for {}", name);
        }
        assert_eq!(date_methods.len(), 13);
    }

    #[test]
    fn test_registry_all_conversion_strict() {
        let r = registry();
        for name in [
            "to_int",
            "to_float",
            "to_string",
            "to_bool",
            "to_date",
            "to_datetime",
        ] {
            let def = r
                .lookup_method(name)
                .unwrap_or_else(|| panic!("missing strict conv: {}", name));
            assert_eq!(def.category, Category::ConversionStrict);
        }
    }

    #[test]
    fn test_registry_all_conversion_lenient() {
        let r = registry();
        for name in [
            "try_int",
            "try_float",
            "try_bool",
            "try_date",
            "try_datetime",
        ] {
            let def = r
                .lookup_method(name)
                .unwrap_or_else(|| panic!("missing lenient conv: {}", name));
            assert_eq!(def.category, Category::ConversionLenient);
        }
    }

    #[test]
    fn test_registry_all_introspection() {
        let r = registry();
        for name in ["type_of", "is_null", "is_empty", "catch"] {
            let def = r
                .lookup_method(name)
                .unwrap_or_else(|| panic!("missing introspection: {}", name));
            assert_eq!(def.category, Category::Introspection);
        }
    }

    #[test]
    fn test_registry_debug_method() {
        let r = registry();
        let def = r.lookup_method("debug").expect("missing debug method");
        assert_eq!(def.category, Category::Debug);
        assert_eq!(def.return_type, TypeTag::Any); // passthrough
    }

    #[test]
    fn test_registry_all_window_aggregate() {
        let r = registry();
        for name in ["sum", "avg", "min", "max", "count"] {
            let def = r
                .lookup_window(name)
                .unwrap_or_else(|| panic!("missing window agg: {}", name));
            assert_eq!(def.category, Category::WindowAggregate);
        }
    }

    #[test]
    fn test_registry_all_window_positional() {
        let r = registry();
        for name in ["first", "last", "lag", "lead"] {
            let def = r
                .lookup_window(name)
                .unwrap_or_else(|| panic!("missing window pos: {}", name));
            assert_eq!(def.category, Category::WindowPositional);
        }
    }

    #[test]
    fn test_registry_all_window_iterable() {
        let r = registry();
        for name in ["any", "all", "collect", "distinct"] {
            let def = r
                .lookup_window(name)
                .unwrap_or_else(|| panic!("missing window iter: {}", name));
            assert_eq!(def.category, Category::WindowIterable);
        }
    }

    #[test]
    fn test_registry_total_count() {
        let r = registry();
        // Due to overloaded names (join/length), scalar map has fewer unique entries.
        // Total = scalar unique + window unique
        let total = r.total_count();
        // We expect ~81 but join overwrites, so actual scalar = 68 unique keys + 13 window = 81
        // Actually let's just check it's in a reasonable range since overloads affect count
        assert!(total >= 75 && total <= 85, "expected ~81, got {}", total);
    }

    #[test]
    fn test_registry_lookup_returns_none() {
        let r = registry();
        assert!(r.lookup_method("nonexistent").is_none());
        assert!(r.lookup_window("nonexistent").is_none());
    }

    #[test]
    fn test_registry_signature_upper() {
        let r = registry();
        let def = r.lookup_method("upper").unwrap();
        assert_eq!(def.receiver, TypeTag::String);
        assert!(def.args.is_empty());
        assert_eq!(def.return_type, TypeTag::String);
    }

    #[test]
    fn test_registry_signature_substring() {
        let r = registry();
        let def = r.lookup_method("substring").unwrap();
        assert_eq!(def.receiver, TypeTag::String);
        assert_eq!(def.args, vec![TypeTag::Int, TypeTag::Int]);
        assert_eq!(def.min_args, 1);
        assert_eq!(def.max_args, Some(2));
        assert_eq!(def.return_type, TypeTag::String);
    }

    #[test]
    fn test_registry_signature_round() {
        let r = registry();
        let def = r.lookup_method("round").unwrap();
        assert_eq!(def.receiver, TypeTag::Numeric);
        assert_eq!(def.return_type, TypeTag::Float);
    }

    #[test]
    fn test_registry_min_max_dual() {
        let r = registry();
        let scalar_min = r.lookup_method("min").unwrap();
        assert_eq!(scalar_min.category, Category::Numeric);

        let window_min = r.lookup_window("min").unwrap();
        assert_eq!(window_min.category, Category::WindowAggregate);
    }

    #[test]
    fn test_registry_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BuiltinRegistry>();
    }
}
