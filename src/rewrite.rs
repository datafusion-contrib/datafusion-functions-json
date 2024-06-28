use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{CustomOperator, Expr, Operator, ParseCustomOperator, WrapCustomOperator};
use datafusion_sql::sqlparser::ast::BinaryOperator;

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
        } else if let Expr::BinaryExpr(bin_expr) = &expr {
            if let Operator::Custom(WrapCustomOperator(op)) = &bin_expr.op {
                if let Ok(json_op) = JsonOperator::try_from(op.name()) {
                    let func = match json_op {
                        JsonOperator::Arrow => crate::json_get::json_get_udf(),
                        JsonOperator::LongArrow => crate::json_get_str::json_get_str_udf(),
                        JsonOperator::Question => crate::json_contains::json_contains_udf(),
                    };
                    let f = ScalarFunction {
                        func,
                        args: vec![*bin_expr.left.clone(), *bin_expr.right.clone()],
                    };
                    return if let Some(new_func) = unnest_json_calls(&f) {
                        Ok(Transformed::yes(Expr::ScalarFunction(new_func)))
                    } else {
                        Ok(Transformed::yes(Expr::ScalarFunction(f)))
                    };
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

#[derive(Debug)]
enum JsonOperator {
    Question,
    Arrow,
    LongArrow,
}

impl std::fmt::Display for JsonOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonOperator::Question => write!(f, "?"),
            JsonOperator::Arrow => write!(f, "->"),
            JsonOperator::LongArrow => write!(f, "->>"),
        }
    }
}

impl CustomOperator for JsonOperator {
    fn binary_signature(&self, lhs: &DataType, rhs: &DataType) -> Result<(DataType, DataType, DataType)> {
        let args = vec![lhs.clone(), rhs.clone()];
        crate::common::check_args(&args, self.name())?;
        let return_type = match self {
            JsonOperator::Question => DataType::Boolean,
            JsonOperator::Arrow => crate::common_union::JsonUnion::data_type(),
            JsonOperator::LongArrow => DataType::Utf8,
        };
        Ok((lhs.clone(), rhs.clone(), return_type))
    }

    fn op_to_sql(&self) -> Result<BinaryOperator> {
        match self {
            JsonOperator::Question => Ok(BinaryOperator::Question),
            JsonOperator::Arrow => Ok(BinaryOperator::Arrow),
            JsonOperator::LongArrow => Ok(BinaryOperator::LongArrow),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            JsonOperator::Question => "Question",
            JsonOperator::Arrow => "Arrow",
            JsonOperator::LongArrow => "LongArrow",
        }
    }
}

impl TryFrom<&str> for JsonOperator {
    type Error = ();

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "Question" => Ok(JsonOperator::Question),
            "Arrow" => Ok(JsonOperator::Arrow),
            "LongArrow" => Ok(JsonOperator::LongArrow),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct JsonOperatorParser;

impl ParseCustomOperator for JsonOperatorParser {
    fn name(&self) -> &str {
        "JsonOperatorParser"
    }

    fn op_from_ast(&self, op: &BinaryOperator) -> Result<Option<Operator>> {
        match op {
            BinaryOperator::Question => Ok(Some(JsonOperator::Question.into())),
            BinaryOperator::Arrow => Ok(Some(JsonOperator::Arrow.into())),
            BinaryOperator::LongArrow => Ok(Some(JsonOperator::LongArrow.into())),
            _ => Ok(None),
        }
    }

    fn op_from_name(&self, raw_op: &str) -> Result<Option<Operator>> {
        Ok(JsonOperator::try_from(raw_op).ok().map(Into::into))
    }
}
