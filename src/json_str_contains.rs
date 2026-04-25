use std::any::Any;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonStrContains,
    json_str_contains,
    json_data path needle,
    r"Checks if a JSON string value at the specified path contains the given substring"
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonStrContains {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonStrContains {
    fn default() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            aliases: ["json_str_contains".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonStrContains {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() == 3 {
            return_type_check(arg_types, self.name(), DataType::Boolean).map(|_| DataType::Boolean)
        } else {
            plan_err!("'json_str_contains' function requires exactly three arguments: json_data, path, and needle.")
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke::<BooleanArray>(&args.args, jiter_json_str_contains)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_str_contains(json_data: Option<&str>, path: &[JsonPath]) -> Result<bool, GetError> {
    if path.len() != 2 {
        return get_err!();
    }

    let [JsonPath::Key(path_str), JsonPath::Key(needle_str)] = path else {
        return get_err!();
    };

    let parsed_path = path_str.split('.').map(|s| JsonPath::Key(s)).collect::<Vec<_>>();

    let Some((mut jiter, Peek::String)) = jiter_json_find(json_data, &parsed_path) else {
        return Ok(false);
    };

    let str_value = jiter.known_str()?;

    Ok(str_value.contains(needle_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_json_str_contains_simple() {
        let json_data = r#"{"name": "Norm Macdonald", "title": "Software Engineer"}"#;
        let json_array = Arc::new(StringArray::from(vec![json_data]));

        let args = vec![
            ColumnarValue::Array(json_array),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("name".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Norm".to_string()))),
        ];

        let result = invoke::<BooleanArray>(&args, jiter_json_str_contains).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(bool_arr.len(), 1);
                assert_eq!(bool_arr.value(0), true);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_json_str_contains_not_found() {
        let json_data = r#"{"name": "Norm Macdonald"}"#;
        let json_array = Arc::new(StringArray::from(vec![json_data]));

        let args = vec![
            ColumnarValue::Array(json_array),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("name".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Dave".to_string()))),
        ];

        let result = invoke::<BooleanArray>(&args, jiter_json_str_contains).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(bool_arr.len(), 1);
                assert_eq!(bool_arr.value(0), false);
            }
            _ => panic!("Expected array result"),
        }
    }

    #[test]
    fn test_json_str_contains_nested() {
        let json_data = r#"{"user": {"profile": {"name": "Norm Macdonald"}}}"#;
        let json_array = Arc::new(StringArray::from(vec![json_data]));

        let args = vec![
            ColumnarValue::Array(json_array),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("user.profile.name".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Macdonald".to_string()))),
        ];

        let result = invoke::<BooleanArray>(&args, jiter_json_str_contains).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(bool_arr.len(), 1);
                assert_eq!(bool_arr.value(0), true);
            }
            _ => panic!("Expected array result"),
        }
    }
}
