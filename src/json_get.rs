use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, UnionArray};
use arrow_schema::DataType;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::{plan_err, Result as DatafusionResult};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, NumberAny, NumberInt, Peek};

use crate::common_get::{jiter_json_get_peek, GetError, JsonPath};
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

impl JsonGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic(vec![DataType::Utf8, DataType::UInt64], Volatility::Immutable),
            aliases: vec!["json_get".to_string()],
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

    fn return_type(&self, arg_types: &[DataType]) -> DatafusionResult<DataType> {
        if arg_types.len() < 2 {
            return plan_err!("The `json_get` function requires two or more arguments.");
        }
        match arg_types[0] {
            DataType::Utf8 => Ok(JsonUnion::data_type()),
            _ => {
                plan_err!("The `json_get` function can only accept Utf8 or LargeUtf8.")
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DatafusionResult<ColumnarValue> {
        let (json_data, path) = JsonPath::extract_args(args)?;

        let mut union = JsonUnion::new(json_data.len());
        for opt_json in json_data {
            if let Some(union_field) = jiter_json_get(opt_json, &path) {
                union.push(union_field);
            } else {
                union.push_none();
            }
        }
        let array: UnionArray = union.try_into()?;

        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get(opt_json: Option<&str>, path: &[JsonPath]) -> Option<JsonUnionField> {
    if let Some(json_str) = opt_json {
        let mut jiter = Jiter::new(json_str.as_bytes(), false);
        if let Ok(peek) = jiter.peek() {
            return _jiter_json_get(&mut jiter, peek, path).ok();
        }
    }
    None
}

fn _jiter_json_get(jiter: &mut Jiter, peek: Peek, path: &[JsonPath]) -> Result<JsonUnionField, GetError> {
    let next_peek = jiter_json_get_peek(jiter, peek, path)?;
    match next_peek {
        Peek::Null => {
            jiter.known_null()?;
            Ok(JsonUnionField::JsonNull)
        }
        Peek::True | Peek::False => {
            let value = jiter.known_bool(next_peek)?;
            Ok(JsonUnionField::Bool(value))
        }
        Peek::String => {
            let value = jiter.known_str()?;
            Ok(JsonUnionField::String(value.to_owned()))
        }
        Peek::Array => {
            let start = jiter.current_index();
            jiter.known_skip(next_peek)?;
            let array_slice = jiter.slice_to_current(start);
            let array_string = std::str::from_utf8(array_slice)?;
            Ok(JsonUnionField::Array(array_string.to_owned()))
        }
        Peek::Object => {
            let start = jiter.current_index();
            jiter.known_skip(next_peek)?;
            let object_slice = jiter.slice_to_current(start);
            let object_string = std::str::from_utf8(object_slice)?;
            Ok(JsonUnionField::Object(object_string.to_owned()))
        }
        _ => match jiter.known_number(next_peek)? {
            NumberAny::Int(NumberInt::Int(value)) => Ok(JsonUnionField::Int(value)),
            NumberAny::Int(NumberInt::BigInt(_)) => todo!("BigInt not supported yet"),
            NumberAny::Float(value) => Ok(JsonUnionField::Float(value)),
        },
    }
}
