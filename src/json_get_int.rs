use std::any::Any;
use std::sync::Arc;

use arrow::array::{as_string_array, Int64Array};
use arrow_schema::DataType;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::{NumberInt, Peek};

use crate::common_get::{check_args, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetInt,
    json_get_int,
    json_data path, // arg name
    r#"Get an integer value from a JSON object by it's "path""#
);

#[derive(Debug)]
pub(super) struct JsonGetInt {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for JsonGetInt {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["json_get_int".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetInt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get_int"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        check_args(arg_types, self.name()).map(|_| DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let path = JsonPath::extract_args(args);

        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = as_string_array(array)
                    .iter()
                    .map(|opt_json| jiter_json_get_int(opt_json, &path).ok())
                    .collect::<Int64Array>();

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
                let v = jiter_json_get_int(s.as_ref().map(|s| s.as_str()), &path).ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v)))
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

fn jiter_json_get_int(json_data: Option<&str>, path: &[JsonPath]) -> Result<i64, GetError> {
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
            _ => match jiter.known_int(peek)? {
                NumberInt::Int(i) => Ok(i),
                NumberInt::BigInt(_) => Err(GetError),
            },
        }
    } else {
        Err(GetError)
    }
}
