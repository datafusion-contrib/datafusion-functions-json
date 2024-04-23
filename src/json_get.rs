use std::any::Any;
use std::sync::Arc;

use arrow::array::{as_string_array, Array, UnionArray};
use arrow_schema::DataType;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, NumberAny, NumberInt, Peek};

use crate::common_get::{check_args, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;
use crate::common_union::{JsonUnion, JsonUnionField};

make_udf_function!(
    JsonGet,
    json_get,
    json_data key, // arg name
    r#"Get a value from a JSON object by it's "path""#
);

#[derive(Debug)]
pub(super) struct JsonGet {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for JsonGet {
    fn default() -> Self {
        Self {
            signature: Signature::variadic(vec![DataType::Utf8, DataType::UInt64], Volatility::Immutable),
            aliases: vec!["json_get".to_string(), "json_get_union".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        check_args(arg_types, self.name()).map(|_| JsonUnion::data_type())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let path = JsonPath::extract_args(args);

        match &args[0] {
            ColumnarValue::Array(array) => {
                let json_array = as_string_array(array);
                let mut union = JsonUnion::new(json_array.len());
                for opt_json in as_string_array(array) {
                    if let Some(union_field) = jiter_json_get_union(opt_json, &path) {
                        union.push(union_field);
                    } else {
                        union.push_none();
                    }
                }
                let array: UnionArray = union.try_into()?;

                Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
                let v = jiter_json_get_union(s.as_ref().map(|s| s.as_str()), &path);
                Ok(JsonUnionField::column_scalar(v))
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

fn jiter_json_get_union(opt_json: Option<&str>, path: &[JsonPath]) -> Option<JsonUnionField> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, &path) {
        build_union(&mut jiter, peek).ok()
    } else {
        None
    }
}

fn build_union(jiter: &mut Jiter, peek: Peek) -> Result<JsonUnionField, GetError> {
    match peek {
        Peek::Null => {
            jiter.known_null()?;
            Ok(JsonUnionField::JsonNull)
        }
        Peek::True | Peek::False => {
            let value = jiter.known_bool(peek)?;
            Ok(JsonUnionField::Bool(value))
        }
        Peek::String => {
            let value = jiter.known_str()?;
            Ok(JsonUnionField::Str(value.to_owned()))
        }
        Peek::Array => {
            let start = jiter.current_index();
            jiter.known_skip(peek)?;
            let array_slice = jiter.slice_to_current(start);
            let array_string = std::str::from_utf8(array_slice)?;
            Ok(JsonUnionField::Array(array_string.to_owned()))
        }
        Peek::Object => {
            let start = jiter.current_index();
            jiter.known_skip(peek)?;
            let object_slice = jiter.slice_to_current(start);
            let object_string = std::str::from_utf8(object_slice)?;
            Ok(JsonUnionField::Object(object_string.to_owned()))
        }
        _ => match jiter.known_number(peek)? {
            NumberAny::Int(NumberInt::Int(value)) => Ok(JsonUnionField::Int(value)),
            NumberAny::Int(NumberInt::BigInt(_)) => todo!("BigInt not supported yet"),
            NumberAny::Float(value) => Ok(JsonUnionField::Float(value)),
        },
    }
}
