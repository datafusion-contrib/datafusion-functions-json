use crate::macros::make_udf_function;
use arrow_schema::DataType;
use arrow_schema::DataType::{LargeUtf8, Utf8};
use datafusion_common::arrow::array::{as_string_array, ArrayRef, BooleanArray};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, JiterResult};
use std::any::Any;
use std::sync::Arc;

make_udf_function!(
    JsonObjContains,
    json_obj_contains,
    json_data key, // arg name
    "Does the string exist as a top-level key within the JSON value?"
);

#[derive(Debug)]
pub(super) struct JsonObjContains {
    signature: Signature,
    aliases: Vec<String>,
}

impl JsonObjContains {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(2, vec![Utf8, LargeUtf8], Volatility::Immutable),
            aliases: vec!["json_obj_contains".to_string(), "json_object_contains".to_string()],
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
                plan_err!("The `json_obj_contains` function can only accept Utf8 or LargeUtf8.")
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let json_data = match &args[0] {
            ColumnarValue::Array(array) => as_string_array(array),
            ColumnarValue::Scalar(_) => {
                return exec_err!("json_obj_contains first argument: unexpected argument type, expected string array")
            }
        };

        let needle = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => return exec_err!("json_obj_contains second argument: unexpected argument type, expected string"),
        };

        let array = json_data
            .iter()
            .map(|opt_json| opt_json.map(|json| jiter_json_contains(json.as_bytes(), &needle).unwrap_or(false)))
            .collect::<BooleanArray>();

        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_contains(json_data: &[u8], expected_key: &str) -> JiterResult<bool> {
    let mut jiter = Jiter::new(json_data, false);
    let Some(first_key) = jiter.next_object()? else {
        return Ok(false);
    };

    if first_key == expected_key {
        return Ok(true);
    }
    jiter.next_skip()?;
    while let Ok(Some(key)) = jiter.next_key() {
        if key == expected_key {
            return Ok(true);
        }
        jiter.next_skip()?;
    }
    Ok(false)
}
