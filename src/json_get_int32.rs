use std::sync::{Arc, OnceLock};

use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

use crate::common::return_type_check;

static JSON_GET_INT32: OnceLock<Arc<ScalarUDF>> = OnceLock::new();

pub(super) fn json_get_int32_udf() -> Arc<ScalarUDF> {
    JSON_GET_INT32
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(JsonGetInt32::default())))
        .clone()
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonGetInt32 {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetInt32 {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_int32".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetInt32 {
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check(arg_types, self.name(), DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let json_get_int_args = ScalarFunctionArgs {
            return_field: Arc::new(Field::new("json_get_int", DataType::Int64, true)),
            ..args
        };
        match crate::json_get_int::json_get_int_udf().invoke_with_args(json_get_int_args)? {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_with_options(
                &array,
                &DataType::Int32,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?)),
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(value.cast_to(&DataType::Int32)?)),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn placement(
        &self,
        args: &[datafusion::logical_expr::ExpressionPlacement],
    ) -> datafusion::logical_expr::ExpressionPlacement {
        if args.len() >= 2
            && matches!(args[0], datafusion::logical_expr::ExpressionPlacement::Column)
            && args[1..]
                .iter()
                .all(|arg| matches!(arg, datafusion::logical_expr::ExpressionPlacement::Literal))
        {
            datafusion::logical_expr::ExpressionPlacement::MoveTowardsLeafNodes
        } else {
            datafusion::logical_expr::ExpressionPlacement::KeepInPlace
        }
    }
}
