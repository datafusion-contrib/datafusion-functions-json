use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::expr::{Cast, Expr, ScalarFunction};
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::{PlannerResult, RawBinaryExpr, UserDefinedSQLPlanner};
use datafusion_expr::sqlparser::ast::BinaryOperator;

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

fn optimise_json_get_cast(cast: &Cast) -> Option<Transformed<Expr>> {
    let Expr::ScalarFunction(scalar_func) = &*cast.expr else {
        return None;
    };
    if scalar_func.func.name() != "json_get" {
        return None;
    }
    let func = match &cast.data_type {
        DataType::Boolean => crate::json_get_bool::json_get_bool_udf(),
        DataType::Float64 | DataType::Float32 => crate::json_get_float::json_get_float_udf(),
        DataType::Int64 | DataType::Int32 => crate::json_get_int::json_get_int_udf(),
        DataType::Utf8 => crate::json_get_str::json_get_str_udf(),
        _ => return None,
    };
    let f = ScalarFunction {
        func,
        args: scalar_func.args.clone(),
    };
    Some(Transformed::yes(Expr::ScalarFunction(f)))
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
    let Expr::ScalarFunction(inner_func) = first_arg else {
        return None;
    };
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

#[derive(Debug, Default)]
pub struct JsonSQLPlanner;

impl UserDefinedSQLPlanner for JsonSQLPlanner {
    fn plan_binary_op(&self, expr: RawBinaryExpr, _schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>> {
        let func = match &expr.op {
            BinaryOperator::Arrow => crate::json_get::json_get_udf(),
            BinaryOperator::LongArrow => crate::json_get_str::json_get_str_udf(),
            BinaryOperator::Question => crate::json_contains::json_contains_udf(),
            _ => return Ok(PlannerResult::Original(expr)),
        };

        Ok(PlannerResult::Planned(Expr::ScalarFunction(ScalarFunction {
            func,
            args: vec![expr.left.clone(), expr.right.clone()],
        })))
    }
}
