use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::DFSchema;
use datafusion::common::Result;
use datafusion::logical_expr::expr::{Alias, Cast, Expr, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;

pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &str {
        "JsonFunctionRewriter"
    }

    fn rewrite(&self, expr: Expr, _schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        let transform = match &expr {
            Expr::Cast(cast) => optimise_json_get_cast(cast),
            Expr::ScalarFunction(func) => unnest_json_calls(func),
            _ => None,
        };
        Ok(transform.unwrap_or_else(|| Transformed::no(expr)))
    }
}

/// This replaces `get_json(foo, bar)::int` with `json_get_int(foo, bar)` so the JSON function can take care of
/// extracting the right value type from JSON without the need to materialize the JSON union.
fn optimise_json_get_cast(cast: &Cast) -> Option<Transformed<Expr>> {
    let scalar_func = extract_scalar_function(&cast.expr)?;
    if scalar_func.func.name() != "json_get" {
        return None;
    }
    let func = match &cast.data_type {
        DataType::Boolean => crate::json_get_bool::json_get_bool_udf(),
        DataType::Float64 | DataType::Float32 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            crate::json_get_float::json_get_float_udf()
        }
        DataType::Int64 | DataType::Int32 => crate::json_get_int::json_get_int_udf(),
        DataType::Utf8 => crate::json_get_str::json_get_str_udf(),
        _ => return None,
    };
    Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
        func,
        args: scalar_func.args.clone(),
    })))
}

// Replace nested JSON functions e.g. `json_get(json_get(col, 'foo'), 'bar')` with `json_get(col, 'foo', 'bar')`
fn unnest_json_calls(func: &ScalarFunction) -> Option<Transformed<Expr>> {
    if !matches!(
        func.func.name(),
        "json_get" | "json_get_bool" | "json_get_float" | "json_get_int" | "json_get_json" | "json_get_str"
    ) {
        return None;
    }
    let mut outer_args_iter = func.args.iter();
    let first_arg = outer_args_iter.next()?;
    let inner_func = extract_scalar_function(first_arg)?;
    if inner_func.func.name() != "json_get" {
        return None;
    }

    let mut args = inner_func.args.clone();
    args.extend(outer_args_iter.cloned());
    // See #23, unnest only when all lookup arguments are literals
    if args.iter().skip(1).all(|arg| matches!(arg, Expr::Literal(_))) {
        Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
            func: func.func.clone(),
            args,
        })))
    } else {
        None
    }
}

fn extract_scalar_function(expr: &Expr) -> Option<&ScalarFunction> {
    match expr {
        Expr::ScalarFunction(func) => Some(func),
        Expr::Alias(alias) => extract_scalar_function(&*alias.expr),
        _ => None,
    }
}

/// Implement a custom SQL planner to replace postgres JSON operators with custom UDFs
#[derive(Debug, Default)]
pub struct JsonExprPlanner;

impl ExprPlanner for JsonExprPlanner {
    fn plan_binary_op(&self, expr: RawBinaryExpr, _schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        let (func, op_display) = match &expr.op {
            BinaryOperator::Arrow => (crate::json_get::json_get_udf(), "->"),
            BinaryOperator::LongArrow => (crate::json_as_text::json_as_text_udf(), "->>"),
            BinaryOperator::Question => (crate::json_contains::json_contains_udf(), "?"),
            _ => return Ok(PlannerResult::Original(expr)),
        };
        let alias_name = match &expr.left {
            Expr::Alias(alias) => format!("{} {} {}", alias.name, op_display, expr.right),
            left_expr => format!("{} {} {}", left_expr, op_display, expr.right),
        };

        // we put the alias in so that default column titles are `foo -> bar` instead of `json_get(foo, bar)`
        Ok(PlannerResult::Planned(Expr::Alias(Alias::new(
            Expr::ScalarFunction(ScalarFunction {
                func,
                args: vec![expr.left, expr.right],
            }),
            None::<&str>,
            alias_name,
        ))))
    }
}
