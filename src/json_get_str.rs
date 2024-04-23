use std::any::Any;
use std::sync::Arc;

use arrow::array::{as_string_array, StringArray};
use arrow_schema::DataType;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common_get::{check_args, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGet,
    json_get_str,
    json_data key, // arg name
    r#"Get a string value from a JSON object by it's "path""#
);

#[derive(Debug)]
pub(super) struct JsonGet {
    signature: Signature,
    aliases: Vec<String>,
}

impl JsonGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["json_get_str".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        check_args(arg_types, self.name()).map(|_| DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let path = JsonPath::extract_args(args);

        match &args[0] {
            ColumnarValue::Array(array) => {
                let array = as_string_array(array)
                    .iter()
                    .map(|opt_json| jiter_json_get_str(opt_json, &path).ok())
                    .collect::<StringArray>();

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
                let v = jiter_json_get_str(s.as_ref().map(|s| s.as_str()), &path).ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(v)))
            }
            ColumnarValue::Scalar(_) => {
                exec_err!("unexpected first argument type, expected string array")
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_str(json_data: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path) {
        match peek {
            Peek::String => Ok(jiter.known_str()?.to_owned()),
            _ => Err(GetError),
        }
    } else {
        Err(GetError)
    }
}
