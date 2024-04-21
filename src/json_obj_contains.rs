use std::any::Any;
use std::sync::Arc;
use arrow_schema::DataType;
use arrow_schema::DataType::{LargeUtf8, Utf8};
use datafusion_expr::{Signature, Volatility, ScalarUDFImpl, ColumnarValue};
use datafusion_common::{plan_err, Result, exec_err, ScalarValue};
use datafusion_common::arrow::array::{ArrayRef, as_string_array, BooleanArray};
use jiter::Jiter;
use crate::macros::make_udf_function;

make_udf_function!(
    JsonObjContains,
    json_obj_contains,
    json_haystack needle, // arg name
    "Does the string exist as a top-level key within the JSON value?", // doc
    json_obj_contains_udf // internal function name
);

#[derive(Debug)]
pub(super) struct JsonObjContains {
    signature: Signature,
    aliases: Vec<String>,
}

impl JsonObjContains {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![Utf8, LargeUtf8],
                Volatility::Immutable,
            ),
            aliases: vec![
                "json_obj_contains".to_string(),
                "json_object_contains".to_string(),
            ],
        }
    }
}

impl ScalarUDFImpl for JsonObjContains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "json_obj_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            Utf8 | LargeUtf8 => Ok(DataType::Boolean),
            _ => {
                return plan_err!(
                    "The json_obj_contains function can only accept Utf8 or LargeUtf8."
                );
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let json_haystack = match &args[0] {
            ColumnarValue::Array(array) => as_string_array(array),
            _ => return exec_err!("json_obj_contains first argument: unexpected argument type, expected string array"),
        };

        let needle = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => return exec_err!("json_obj_contains second argument: unexpected argument type, expected string"),
        };

        let array = json_haystack
            .iter()
            .map(|opt_json| {
                match opt_json {
                    Some(json) => jiter_json_contains(json.as_bytes(), &needle),
                    None => None,
                }
            })
            .collect::<BooleanArray>();

        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_contains(json_data: &[u8], expected_key: &str) -> Option<bool> {
    let mut jiter = Jiter::new(json_data, false);
    let first_key = match jiter.next_object() {
        Ok(Some(key)) => key,
        _ => return None,
    };
    if first_key == expected_key {
        return Some(true);
    }
    if jiter.next_skip().is_err() {
        return None;
    }
    while let Ok(Some(key)) = jiter.next_key() {
        if key == expected_key {
            return Some(true);
        }
        if jiter.next_skip().is_err() {
            return None;
        }
    }
    Some(false)
}
