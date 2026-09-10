use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::Column;
use datafusion::common::DFSchema;
use datafusion::common::Result;
use datafusion::logical_expr::expr::{Alias, Cast, Expr, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::ScalarUDF;
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &'static str {
        "JsonFunctionRewriter"
    }

    fn rewrite(&self, expr: Expr, _schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        let transform = match &expr {
            Expr::Cast(cast) => optimise_json_get(cast.field.data_type(), &cast.expr).map(|folded| match folded {
                Folded::Exact(accessor) => accessor,
                Folded::Narrowing(accessor) => Expr::Cast(Cast {
                    expr: Box::new(accessor),
                    field: cast.field.clone(),
                }),
            }),
            Expr::ScalarFunction(func) => unnest_json_calls(func),
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
/// The accessors return one Arrow type per JSON type — `json_get_int` is always `Int64` — which is
/// not necessarily the type that was asked for. Where it is, the cast is dropped and this is the
/// plain substitution it has always been. Where it is not, only the cast's *input* is replaced:
/// dropping the cast there would silently change the type of the expression, and for a narrowing
/// cast its value too, while keeping it costs one primitive-to-primitive cast and still never
/// materializes the union.
fn optimise_json_get(cast_to: &DataType, cast_expr: &Expr) -> Option<Folded> {
    let scalar_func = extract_scalar_function(cast_expr)?;
    if !is_json_get(scalar_func) {
        return None;
    }
    let (func, returns) = typed_accessor(cast_to)?;
    let accessor = Expr::ScalarFunction(ScalarFunction {
        func,
        args: scalar_func.args.clone(),
    });
    Some(if returns == *cast_to {
        Folded::Exact(accessor)
    } else {
        Folded::Narrowing(accessor)
    })
}

/// The accessor that reads `cast_to` out of JSON, if there is one, and the type it returns.
fn typed_accessor(cast_to: &DataType) -> Option<(Arc<ScalarUDF>, DataType)> {
    Some(match cast_to {
        DataType::Boolean => (crate::json_get_bool::json_get_bool_udf(), DataType::Boolean),
        DataType::Float64 | DataType::Float32 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            (crate::json_get_float::json_get_float_udf(), DataType::Float64)
        }
        DataType::Int64 => (crate::json_get_int::json_get_int_udf(), DataType::Int64),
        DataType::Int32 => (crate::json_get_int32::json_get_int32_udf(), DataType::Int32),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
            (crate::json_get_str::json_get_str_udf(), DataType::Utf8)
        }
        _ => return None,
    })
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
}

impl TryFrom<&BinaryOperator> for JsonOperator {
    type Error = ();

    fn try_from(op: &BinaryOperator) -> Result<Self, Self::Error> {
        match op {
            BinaryOperator::Arrow => Ok(JsonOperator::Arrow),
            BinaryOperator::LongArrow => Ok(JsonOperator::LongArrow),
            BinaryOperator::Question => Ok(JsonOperator::Question),
            _ => Err(()),
        }
    }
}

impl From<JsonOperator> for Arc<ScalarUDF> {
    fn from(op: JsonOperator) -> Arc<ScalarUDF> {
        match op {
            JsonOperator::Arrow => crate::udfs::json_get_udf(),
            JsonOperator::LongArrow => crate::udfs::json_as_text_udf(),
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

        // we put the alias in so that default column titles are `foo -> bar` instead of `json_get(foo, bar)`
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(
            Expr::ScalarFunction(ScalarFunction {
                func: op.into(),
                args: vec![expr.left, expr.right],
            }),
            None::<&str>,
            alias_name,
        ))))
    }
}
