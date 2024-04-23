use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{Expr, ScalarFunctionDefinition};

use crate::json_get_str::json_get_str_udf;

pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &str {
        "JsonFunctionRewriter"
    }

    fn rewrite(&self, expr: Expr, _schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        match &expr {
            Expr::Cast(cast) => {
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
            _ => (),
        }
        Ok(Transformed::no(expr))
    }
}

fn switch_json_get(cast_data_type: &DataType, args: &[Expr]) -> Option<Transformed<Expr>> {
    if cast_data_type != &DataType::Utf8 {
        return None;
    }
    let f = ScalarFunction {
        func_def: ScalarFunctionDefinition::UDF(json_get_str_udf()),
        args: args.to_vec(),
    };
    Some(Transformed::yes(Expr::ScalarFunction(f)))
}
