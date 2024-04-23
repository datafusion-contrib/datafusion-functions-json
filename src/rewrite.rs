use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{Expr, ScalarFunctionDefinition};

pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &str {
        "JsonFunctionRewriter"
    }

    fn rewrite(&self, expr: Expr, _schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        if let Expr::Cast(cast) = &expr {
            if let Expr::ScalarFunction(func) = &*cast.expr {
                if let ScalarFunctionDefinition::UDF(udf) = &func.func_def {
                    if udf.name() == "json_get" {
                        if let Some(t) = switch_json_get(&cast.data_type, &func.args) {
                            return Ok(t);
                        }
                    }
                }
            }
        }
        Ok(Transformed::no(expr))
    }
}

fn switch_json_get(cast_data_type: &DataType, args: &[Expr]) -> Option<Transformed<Expr>> {
    let udf = match cast_data_type {
        DataType::Utf8 => crate::json_get_str::json_get_str_udf(),
        DataType::Int64 | DataType::Int32 => crate::json_get_int::json_get_int_udf(),
        DataType::Float64 | DataType::Float32 => crate::json_get_float::json_get_float_udf(),
        _ => return None,
    };
    let f = ScalarFunction {
        func_def: ScalarFunctionDefinition::UDF(udf),
        args: args.to_vec(),
    };
    Some(Transformed::yes(Expr::ScalarFunction(f)))
}
