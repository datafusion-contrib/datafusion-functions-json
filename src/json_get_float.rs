use std::any::Any;
use std::sync::Arc;

use arrow::array::{as_string_array, Float64Array};
use arrow_schema::DataType;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::{NumberAny, Peek};

use crate::common_get::{check_args, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetFloat,
    json_get_float,
    json_data path, // arg name
    r#"Get a float value from a JSON object by it's "path""#
);

#[derive(Debug)]
pub(super) struct JsonGetFloat {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for JsonGetFloat {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["json_get_float".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetFloat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get_float"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        check_args(arg_types, self.name()).map(|_| DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let path = JsonPath::extract_args(args);

        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = as_string_array(array)
                    .iter()
                    .map(|opt_json| jiter_json_get_float(opt_json, &path).ok())
                    .collect::<Float64Array>();

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
                let v = jiter_json_get_float(s.as_ref().map(|s| s.as_str()), &path).ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(v)))
            }
            ColumnarValue::Scalar(_) => {
                exec_err!("unexpected first argument type, expected string")
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_float(json_data: Option<&str>, path: &[JsonPath]) -> Result<f64, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path) {
        match peek {
            // numbers are represented by everything else in peek, hence doing it this way
            Peek::Null
            | Peek::True
            | Peek::False
            | Peek::Minus
            | Peek::Infinity
            | Peek::NaN
            | Peek::String
            | Peek::Array
            | Peek::Object => Err(GetError),
            _ => match jiter.known_number(peek)? {
                NumberAny::Float(f) => Ok(f),
                NumberAny::Int(int) => Ok(int.into()),
            },
        }
    } else {
        Err(GetError)
    }
}
