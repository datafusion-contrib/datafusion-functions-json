use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::plan_err;
use datafusion::common::tree_node::Transformed;
use datafusion::common::Column;
use datafusion::common::DFSchema;
use datafusion::common::Result;
use datafusion::logical_expr::expr::{Alias, Cast, Expr, ScalarFunction, TryCast};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::{ExprSchemable, ScalarUDF};
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &'static str {
        "JsonFunctionRewriter"
    }

    fn rewrite(&self, expr: Expr, schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        let transform = match &expr {
            Expr::Cast(cast) => {
                optimise_json_get(cast.field.data_type(), &cast.expr, schema).map(|folded| match folded {
                    Folded::Exact(accessor) => accessor,
                    Folded::Narrowing(accessor) => Expr::Cast(Cast {
                        expr: Box::new(accessor),
                        field: cast.field.clone(),
                    }),
                })
            }
            Expr::TryCast(try_cast) => {
                optimise_json_get(try_cast.field.data_type(), &try_cast.expr, schema).map(|folded| match folded {
                    Folded::Exact(accessor) => accessor,
                    Folded::Narrowing(accessor) => Expr::TryCast(TryCast {
                        expr: Box::new(accessor),
                        field: try_cast.field.clone(),
                    }),
                })
            }
            Expr::ScalarFunction(func) => {
                optimise_json_get_arrow_cast(func, schema).or_else(|| unnest_json_calls(func))
            }
            _ => None,
        };
        Ok(transform.map_or_else(|| Transformed::no(expr), Transformed::yes))
    }
}

/// The accessor call that replaces a `json_get` under a cast, and whether the cast is still needed.
enum Folded {
    /// The accessor already returns the type the query asked for; the cast can go.
    Exact(Expr),
    /// The accessor returns a wider type; the cast has to stay or both the type of the expression
    /// and, for a narrowing cast, its value would change.
    Narrowing(Expr),
}

/// Replace the `json_get` under a cast to `cast_to` with the typed accessor that reads that type
/// out of the JSON directly, so the JSON union never has to be materialized just to be cast away.
///
/// The accessors return one Arrow type per JSON type — `json_get_int` is always `Int64`, and a
/// dictionary of it for a dictionary-encoded JSON argument — which is not necessarily the type
/// that was asked for. Where it is, the cast is dropped and this is the plain substitution it has
/// always been. Where it is not, only the cast's *input* is replaced: dropping the cast there would
/// silently change the type of the expression, and for a narrowing cast its value too, while
/// keeping it costs one primitive-to-primitive cast and still never materializes the union.
///
/// `TRY_CAST` gets the same treatment, so a value outside the target type's range still becomes
/// NULL rather than being handed back as the accessor's wider type.
fn optimise_json_get(cast_to: &DataType, cast_expr: &Expr, schema: &DFSchema) -> Option<Folded> {
    let scalar_func = extract_scalar_function(cast_expr)?;
    if !is_json_get(scalar_func) {
        return None;
    }
    let accessor = Expr::ScalarFunction(ScalarFunction {
        func: typed_accessor(cast_to)?,
        args: scalar_func.args.clone(),
    });
    // the type the accessor returns for these particular arguments; if it can't be worked out,
    // keeping the cast is always correct
    let returns = accessor.get_type(schema).ok();
    Some(if returns.as_ref() == Some(cast_to) {
        Folded::Exact(accessor)
    } else {
        Folded::Narrowing(accessor)
    })
}

/// The accessor that reads `cast_to` out of JSON, if there is one.
fn typed_accessor(cast_to: &DataType) -> Option<Arc<ScalarUDF>> {
    Some(match cast_to {
        DataType::Boolean => crate::json_get_bool::json_get_bool_udf(),
        DataType::Float64 | DataType::Float32 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            crate::json_get_float::json_get_float_udf()
        }
        DataType::Int64 | DataType::Int32 => crate::json_get_int::json_get_int_udf(),
        // a string cast reads any JSON value as text, as `->>` does: a string without its quotes,
        // a number, boolean, object or array as its JSON text, and a JSON null as NULL
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => crate::json_as_text::json_as_text_udf(),
        _ => return None,
    })
}

/// The same rewrite for `arrow_cast(json_get(foo, bar), 'Int64')` and its `arrow_try_cast` sibling.
///
/// These two are still scalar function calls when this rewriter runs. `DataFusion` lowers them to
/// `Expr::Cast` / `Expr::TryCast` in `SimplifyExpressions`, an optimizer rule, whereas function
/// rewrites are applied by `ApplyFunctionRewrites` at the start of the analyzer. The analyzer never
/// runs again afterwards, so without this the JSON union is materialized only to be cast away.
///
/// Only the call's first argument is replaced, so the named type is still what comes out. That
/// lowering then drops the cast by itself when the accessor already returns the named type.
fn optimise_json_get_arrow_cast(func: &ScalarFunction, schema: &DFSchema) -> Option<Expr> {
    if !matches!(func.func.name(), "arrow_cast" | "arrow_try_cast") {
        return None;
    }
    let [cast_expr, type_arg] = func.args.as_slice() else {
        return None;
    };
    let Expr::Literal(ScalarValue::Utf8(Some(type_name)), _) = type_arg else {
        return None;
    };
    // `arrow_cast` names its target as an Arrow type string, which is how DataFusion itself reads
    // it back in `ArrowCastFunc::return_field_from_args`.
    let cast_to = type_name.parse::<DataType>().ok()?;
    // the call keeps its type argument either way, and `ArrowCastFunc::simplify` drops the cast
    // itself when the accessor already returns that type
    let accessor = match optimise_json_get(&cast_to, cast_expr, schema)? {
        Folded::Exact(accessor) | Folded::Narrowing(accessor) => accessor,
    };
    Some(Expr::ScalarFunction(ScalarFunction {
        func: func.func.clone(),
        args: vec![accessor, type_arg.clone()],
    }))
}

// Replace nested JSON functions e.g. `json_get(json_get(col, 'foo'), 'bar')` with `json_get(col, 'foo', 'bar')`
fn unnest_json_calls(func: &ScalarFunction) -> Option<Expr> {
    if !matches!(
        func.func.name(),
        "json_get"
            | "json_get_bool"
            | "json_get_float"
            | "json_get_int"
            | "json_get_json"
            | "json_get_str"
            | "json_as_text"
    ) {
        return None;
    }
    let mut outer_args_iter = func.args.iter();
    let first_arg = outer_args_iter.next()?;
    let inner_func = extract_scalar_function(first_arg)?;

    // only json_get preserves a JSON value for the outer function. json_as_text returns SQL text,
    // so flattening through it changes semantics for JSON strings that contain JSON.
    if !is_json_get(inner_func) {
        return None;
    }

    let mut args = inner_func.args.clone();
    args.extend(outer_args_iter.cloned());
    // See #23, unnest only when all lookup arguments are literals
    if args.iter().skip(1).all(|arg| matches!(arg, Expr::Literal(_, _))) {
        Some(Expr::ScalarFunction(ScalarFunction {
            func: func.func.clone(),
            args,
        }))
    } else {
        None
    }
}

fn is_json_get(func: &ScalarFunction) -> bool {
    func.func.inner().is::<crate::json_get::JsonGet>()
}

fn extract_scalar_function(expr: &Expr) -> Option<&ScalarFunction> {
    match expr {
        Expr::ScalarFunction(func) => Some(func),
        Expr::Alias(alias) => extract_scalar_function(&alias.expr),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
enum JsonOperator {
    Arrow,
    LongArrow,
    Question,
    HashArrow,
    HashLongArrow,
}

impl TryFrom<&BinaryOperator> for JsonOperator {
    type Error = ();

    fn try_from(op: &BinaryOperator) -> Result<Self, Self::Error> {
        match op {
            BinaryOperator::Arrow => Ok(JsonOperator::Arrow),
            BinaryOperator::LongArrow => Ok(JsonOperator::LongArrow),
            BinaryOperator::Question => Ok(JsonOperator::Question),
            BinaryOperator::HashArrow => Ok(JsonOperator::HashArrow),
            BinaryOperator::HashLongArrow => Ok(JsonOperator::HashLongArrow),
            _ => Err(()),
        }
    }
}

impl From<JsonOperator> for Arc<ScalarUDF> {
    fn from(op: JsonOperator) -> Arc<ScalarUDF> {
        match op {
            // the path operators use the same functions as their single key counterparts, with
            // the path passed as a list
            JsonOperator::Arrow | JsonOperator::HashArrow => crate::udfs::json_get_udf(),
            JsonOperator::LongArrow | JsonOperator::HashLongArrow => crate::udfs::json_as_text_udf(),
            JsonOperator::Question => crate::udfs::json_contains_udf(),
        }
    }
}

impl std::fmt::Display for JsonOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonOperator::Arrow => write!(f, "->"),
            JsonOperator::LongArrow => write!(f, "->>"),
            JsonOperator::Question => write!(f, "?"),
            JsonOperator::HashArrow => write!(f, "#>"),
            JsonOperator::HashLongArrow => write!(f, "#>>"),
        }
    }
}

/// Convert an Expr to a String representatiion for use in alias names.
fn expr_to_sql_repr(expr: &Expr) -> String {
    match expr {
        Expr::Column(Column {
            name,
            relation: _,
            spans: _,
        }) => name.clone(),
        Expr::Alias(alias) => alias.name.clone(),
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Utf8(Some(v)) | ScalarValue::Utf8View(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                format!("'{v}'")
            }
            ScalarValue::UInt8(Some(v)) => v.to_string(),
            ScalarValue::UInt16(Some(v)) => v.to_string(),
            ScalarValue::UInt32(Some(v)) => v.to_string(),
            ScalarValue::UInt64(Some(v)) => v.to_string(),
            ScalarValue::Int8(Some(v)) => v.to_string(),
            ScalarValue::Int16(Some(v)) => v.to_string(),
            ScalarValue::Int32(Some(v)) => v.to_string(),
            ScalarValue::Int64(Some(v)) => v.to_string(),
            _ => scalar.to_string(),
        },
        Expr::Cast(cast) => expr_to_sql_repr(&cast.expr),
        _ => expr.to_string(),
    }
}

/// Implement a custom SQL planner to replace postgres JSON operators with custom UDFs
#[derive(Debug, Default)]
pub struct JsonExprPlanner;

impl ExprPlanner for JsonExprPlanner {
    fn plan_binary_op(&self, expr: RawBinaryExpr, _schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        let Ok(op) = JsonOperator::try_from(&expr.op) else {
            return Ok(PlannerResult::Original(expr));
        };

        let left_repr = expr_to_sql_repr(&expr.left);
        let right_repr = expr_to_sql_repr(&expr.right);

        let alias_name = format!("{left_repr} {op} {right_repr}");

        let right = match op {
            JsonOperator::HashArrow | JsonOperator::HashLongArrow => text_array_path(expr.right)?,
            JsonOperator::Arrow | JsonOperator::LongArrow | JsonOperator::Question => expr.right,
        };

        // we put the alias in so that default column titles are `foo -> bar` instead of `json_get(foo, bar)`
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(
            Expr::ScalarFunction(ScalarFunction {
                func: op.into(),
                args: vec![expr.left, right],
            }),
            None::<&str>,
            alias_name,
        ))))
    }
}

/// The path of `#>` or `#>>` as a list, when it is written as a string literal like `j #> '{a,b}'`.
///
/// Postgres reads such an untyped literal as the `text[]` the operators expect, so it is parsed
/// rather than used as a single key, which would silently find nothing.
fn text_array_path(path: Expr) -> Result<Expr> {
    let Expr::Literal(scalar, _) = &path else {
        return Ok(path);
    };
    let Some(Some(literal)) = scalar.try_as_str() else {
        return Ok(path);
    };
    let elements = parse_text_array(literal)?
        .into_iter()
        .map(ScalarValue::Utf8)
        .collect::<Vec<_>>();
    Ok(Expr::Literal(
        ScalarValue::List(ScalarValue::new_list_nullable(&elements, &DataType::Utf8)),
        None,
    ))
}

/// Parse a one-dimensional postgres array literal, as postgres's `array_in` does for a `text[]`.
///
/// Elements are separated by commas and the whitespace around them is dropped. An element is
/// either double quoted, and kept verbatim, or unquoted, in which case an unquoted `NULL` (in any
/// case) is a null element and an unquoted element cannot be empty. A backslash escapes the next
/// character in both forms.
///
/// The dimension decoration (`[1:2]={a,b}`) and multidimensional arrays are not supported.
fn parse_text_array(literal: &str) -> Result<Vec<Option<String>>> {
    let malformed = || plan_err!("malformed array literal: '{literal}'");
    let Some(inner) = literal
        .trim_matches(is_array_whitespace)
        .strip_prefix('{')
        .and_then(|inner| inner.strip_suffix('}'))
    else {
        return malformed();
    };
    let mut elements = Vec::new();
    if inner.chars().all(is_array_whitespace) {
        return Ok(elements);
    }
    let mut chars = inner.chars().peekable();
    loop {
        while chars.next_if(|&c| is_array_whitespace(c)).is_some() {}
        let mut element = String::new();
        if chars.next_if_eq(&'"').is_some() {
            loop {
                match chars.next() {
                    Some('"') => break,
                    Some('\\') => {
                        let Some(c) = chars.next() else {
                            return malformed();
                        };
                        element.push(c);
                    }
                    Some(c) => element.push(c),
                    None => return malformed(),
                }
            }
            while chars.next_if(|&c| is_array_whitespace(c)).is_some() {}
            elements.push(Some(element));
        } else {
            // the length of `element` without its unescaped trailing whitespace
            let mut kept_len = 0;
            let mut escaped = false;
            while let Some(c) = chars.next_if(|&c| c != ',') {
                match c {
                    '"' | '{' | '}' => return malformed(),
                    '\\' => {
                        let Some(c) = chars.next() else {
                            return malformed();
                        };
                        element.push(c);
                        kept_len = element.len();
                        escaped = true;
                    }
                    c => {
                        element.push(c);
                        if !is_array_whitespace(c) {
                            kept_len = element.len();
                        }
                    }
                }
            }
            element.truncate(kept_len);
            if element.is_empty() {
                return malformed();
            }
            elements.push((escaped || !element.eq_ignore_ascii_case("NULL")).then_some(element));
        }
        match chars.next() {
            Some(',') => {}
            None => return Ok(elements),
            Some(_) => return malformed(),
        }
    }
}

/// The whitespace postgres skips around array elements, that of C's `isspace`.
fn is_array_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0B' | '\x0C')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_array_as_postgres() {
        // the arrays postgres reads these `text[]` literals as
        let cases: &[(&str, &[Option<&str>])] = &[
            ("{}", &[]),
            (" { } ", &[]),
            ("{a,b}", &[Some("a"), Some("b")]),
            ("{ a , b }", &[Some("a"), Some("b")]),
            ("{a b}", &[Some("a b")]),
            (r#"{"a b",c}"#, &[Some("a b"), Some("c")]),
            (r#"{"a,b"}"#, &[Some("a,b")]),
            (r#"{"a\"b"}"#, &[Some("a\"b")]),
            (r#"{"a\\b"}"#, &[Some(r"a\b")]),
            (r"{a\,b}", &[Some("a,b")]),
            (r"{ a\  }", &[Some("a ")]),
            (r#"{NULL,null,"NULL",\NULL}"#, &[None, None, Some("NULL"), Some("NULL")]),
            (r#"{"",a}"#, &[Some(""), Some("a")]),
            (r#"{"output.fgb",0}"#, &[Some("output.fgb"), Some("0")]),
            (r#"{é,"ü x"}"#, &[Some("é"), Some("ü x")]),
        ];
        for (literal, expected) in cases {
            let expected = expected.iter().map(|e| e.map(str::to_owned)).collect::<Vec<_>>();
            assert_eq!(parse_text_array(literal).unwrap(), expected, "{literal}");
        }
    }

    #[test]
    fn parse_text_array_malformed() {
        // postgres rejects all of these but the multidimensional and decorated arrays
        for literal in [
            "a,b",
            "{a",
            "{a}b",
            r"{a\}",
            "{a,,b}",
            "{a,}",
            "{,a}",
            r#"{"a"b}"#,
            r#"{"a" b}"#,
            r#"{a"b"}"#,
            r#"{a"b}"#,
            "{a,{b}}",
            "{{a,b}}",
            "[1:2]={a,b}",
        ] {
            assert!(parse_text_array(literal).is_err(), "{literal}");
        }
    }
}
