use std::sync::{Arc, OnceLock};

use bigdecimal::BigDecimal;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::compute::{cast_with_options, CastOptions};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath};

static JSON_GET_DECIMAL: OnceLock<Arc<ScalarUDF>> = OnceLock::new();

pub(super) fn json_get_decimal_udf() -> Arc<ScalarUDF> {
    JSON_GET_DECIMAL
        .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(JsonGetDecimal::default())))
        .clone()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonGetDecimal {
    signature: Signature,
}

impl Default for JsonGetDecimal {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonGetDecimal {
    fn name(&self) -> &'static str {
        "json_get_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        exec_err!("json_get_decimal requires a literal decimal type")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
        if args.arg_fields.len() < 2 {
            return exec_err!("json_get_decimal requires a JSON value and a literal decimal type");
        }
        let target_type = args
            .scalar_arguments
            .last()
            .and_then(|value| *value)
            .and_then(ScalarValue::try_as_str)
            .flatten()
            .ok_or_else(|| {
                datafusion::common::exec_datafusion_err!("json_get_decimal requires a literal decimal type")
            })?
            .parse::<DataType>()?;
        if !matches!(target_type, DataType::Decimal128(_, _) | DataType::Decimal256(_, _)) {
            return exec_err!("json_get_decimal requires a decimal type, got {target_type}");
        }
        let json_arg_types = args.arg_fields[..args.arg_fields.len() - 1]
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        return_type_check(&json_arg_types, self.name(), target_type.clone())?;

        Ok(Arc::new(Field::new(self.name(), target_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let Some((type_arg, json_args)) = args.args.split_last() else {
            return exec_err!("json_get_decimal requires a literal decimal type");
        };
        let ColumnarValue::Scalar(type_arg) = type_arg else {
            return exec_err!("json_get_decimal requires a literal decimal type");
        };
        let target_type = type_arg
            .try_as_str()
            .flatten()
            .ok_or_else(|| {
                datafusion::common::exec_datafusion_err!("json_get_decimal requires a literal decimal type")
            })?
            .parse::<DataType>()?;

        match invoke::<StringArray>(json_args, jiter_json_get_number)? {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_with_options(
                &array,
                &target_type,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?)),
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(value.cast_to(&target_type)?)),
        }
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

fn jiter_json_get_number(json_data: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
    let Some((mut jiter, peek)) = jiter_json_find(json_data, path) else {
        return get_err!();
    };

    match peek {
        Peek::String => {
            let value = jiter.known_str()?;
            value.parse::<f64>().map_err(|_| GetError)?;
            Ok(value
                .parse::<BigDecimal>()
                .map_or_else(|_| value.to_owned(), |number| number.to_plain_string()))
        }
        Peek::Null | Peek::True | Peek::False | Peek::Infinity | Peek::NaN | Peek::Array | Peek::Object => get_err!(),
        _ => {
            let start = jiter.current_index();
            jiter.known_skip(peek)?;
            Ok(std::str::from_utf8(jiter.slice_to_current(start))?
                .parse::<BigDecimal>()
                .map_err(|_| GetError)?
                .to_plain_string())
        }
    }
}
