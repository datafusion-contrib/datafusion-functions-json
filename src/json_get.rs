use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, UnionArray};
use arrow_schema::DataType;
use datafusion_common::arrow::array::{as_string_array, ArrayRef};
use datafusion_common::{exec_err, plan_err, Result as DfResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, JiterError, NumberAny, NumberInt, Peek};

use crate::macros::make_udf_function;
use crate::union::{JsonUnion, JsonUnionField};

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

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
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

    fn invoke(&self, args: &[ColumnarValue]) -> DfResult<ColumnarValue> {
        let json_data = match &args[0] {
            ColumnarValue::Array(array) => as_string_array(array),
            ColumnarValue::Scalar(_) => {
                return exec_err!("json_get first argument: unexpected argument type, expected string array")
            }
        };

        let path = args[1..]
            .iter()
            .enumerate()
            .map(|(index, arg)| match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(JsonPath::Key(s)),
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(i))) => Ok(JsonPath::Index(*i as usize)),
                _ => exec_err!(
                    "json_get: unexpected argument type at {}, expected string or int",
                    index + 2
                ),
            })
            .collect::<DfResult<Vec<JsonPath>>>()?;

        let mut union = JsonUnion::new(json_data.len());
        for opt_json in json_data {
            if let Some(union_field) = jiter_json_get(opt_json, &path) {
                dbg!(&union_field);
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

enum JsonPath<'s> {
    Key(&'s str),
    Index(usize),
}

struct GetError;

impl From<JiterError> for GetError {
    fn from(_: JiterError) -> Self {
        GetError
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
    let (first, rest) = path.split_first().unwrap();
    let next_peek = match peek {
        Peek::Array => match first {
            JsonPath::Index(index) => jiter_array_get(jiter, *index),
            _ => Err(GetError),
        },
        Peek::Object => match first {
            JsonPath::Key(key) => jiter_object_get(jiter, key),
            _ => Err(GetError),
        },
        _ => Err(GetError),
    }?;

    if rest.is_empty() {
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
                let array_string = std::str::from_utf8(array_slice).map_err(|_| GetError)?;
                Ok(JsonUnionField::Array(array_string.to_owned()))
            }
            Peek::Object => {
                let start = jiter.current_index();
                jiter.known_skip(next_peek)?;
                let object_slice = jiter.slice_to_current(start);
                let object_string = std::str::from_utf8(object_slice).map_err(|_| GetError)?;
                Ok(JsonUnionField::Object(object_string.to_owned()))
            }
            _ => match jiter.known_number(next_peek)? {
                NumberAny::Int(NumberInt::Int(value)) => Ok(JsonUnionField::Int(value)),
                NumberAny::Int(NumberInt::BigInt(_)) => todo!("BigInt not supported yet"),
                NumberAny::Float(value) => Ok(JsonUnionField::Float(value)),
            },
        }
    } else {
        // we still have more of the path to traverse, recurse
        _jiter_json_get(jiter, next_peek, rest)
    }
}

fn jiter_array_get(jiter: &mut Jiter, find_key: usize) -> Result<Peek, GetError> {
    let mut peek_opt = jiter.known_array()?;

    let mut index: usize = 0;
    while let Some(peek) = peek_opt {
        if index == find_key {
            return Ok(peek);
        }
        index += 1;
        peek_opt = jiter.next_array()?;
    }
    Err(GetError)
}

fn jiter_object_get(jiter: &mut Jiter, find_key: &str) -> Result<Peek, GetError> {
    let mut opt_key = jiter.known_object()?;

    while let Some(key) = opt_key {
        if key == find_key {
            let value_peek = jiter.peek()?;
            return Ok(value_peek);
        }
        jiter.next_skip()?;
        opt_key = jiter.next_key()?;
    }
    Err(GetError)
}
