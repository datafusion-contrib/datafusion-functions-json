use std::any::Any;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath, Sortedness};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetStr,
    json_get_str,
    json_data path,
    r#"Get a string value from a JSON string by its "path""#,
    Sortedness::Unspecified
);

make_udf_function!(
    JsonGetStr,
    json_get_str_top_level_sorted,
    json_data path,
    r#"Get a string value from a JSON string by its "path"; assumes the JSON string's top level object's keys are sorted."#,
    Sortedness::TopLevel
);

make_udf_function!(
    JsonGetStr,
    json_get_str_recursive_sorted,
    json_data path,
    r#"Get a string value from a JSON string by its "path"; assumes all json object's keys are sorted."#,
    Sortedness::Recursive
);

#[derive(Debug)]
pub(super) struct JsonGetStr {
    signature: Signature,
    aliases: [String; 1],
    sorted: Sortedness,
}

impl JsonGetStr {
    pub fn new(sorted: Sortedness) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: [format!("json_get_str{}", sorted.function_name_suffix())],
            sorted,
        }
    }
}

impl ScalarUDFImpl for JsonGetStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check(arg_types, self.name(), DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<StringArray>(&args.args, |json, path| jiter_json_get_str(json, path, self.sorted))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_str(json_data: Option<&str>, path: &[JsonPath], sorted: Sortedness) -> Result<String, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path, sorted) {
        match peek {
            Peek::String => Ok(jiter.known_str()?.to_owned()),
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
