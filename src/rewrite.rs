use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::Expr;

pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &str {
        "JsonFunctionRewriter"
    }

    fn rewrite(&self, expr: Expr, _schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        if let Expr::Cast(cast) = &expr {
            if let Expr::ScalarFunction(func) = &*cast.expr {
                if func.func.name() == "json_get" {
                    if let Some(t) = switch_json_get(&cast.data_type, &func.args) {
                        return Ok(t);
                    }
                }
            }
        } else if let Expr::ScalarFunction(func) = &expr {
            if let Some(new_func) = unnest_json_calls(func) {
                return Ok(Transformed::yes(Expr::ScalarFunction(new_func)));
            }
        }
        Ok(Transformed::no(expr))
    }
}

// Replace nested JSON functions e.g. `json_get(json_get(col, 'foo'), 'bar')` with `json_get(col, 'foo', 'bar')`
fn unnest_json_calls(func: &ScalarFunction) -> Option<ScalarFunction> {
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
        Some(ScalarFunction {
            func: func.func.clone(),
            args,
        })
    } else {
        None
    }
}

fn switch_json_get(cast_data_type: &DataType, args: &[Expr]) -> Option<Transformed<Expr>> {
    let func = match cast_data_type {
        DataType::Boolean => crate::json_get_bool::json_get_bool_udf(),
        DataType::Float64 | DataType::Float32 => crate::json_get_float::json_get_float_udf(),
        DataType::Int64 | DataType::Int32 => crate::json_get_int::json_get_int_udf(),
        DataType::Utf8 => crate::json_get_str::json_get_str_udf(),
        _ => return None,
    };
    let f = ScalarFunction {
        func,
        args: args.to_vec(),
    };
    Some(Transformed::yes(Expr::ScalarFunction(f)))
}
