use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow_schema::DataType;
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::common_get::{check_args, get_err, get_invoke, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetJson,
    json_get_json,
    json_data path, // arg name
    r#"Get any value from a JSON object by it's "path", represented as a string"#
);

#[derive(Debug)]
pub(super) struct JsonGetJson {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for JsonGetJson {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["json_get_json".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetJson {
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
        check_args(arg_types, self.name()).map(|()| DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        get_invoke::<StringArray, String>(
            args,
            jiter_json_get_json,
            |c| Ok(Arc::new(c) as ArrayRef),
            ScalarValue::Utf8,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_json(opt_json: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
        let start = jiter.current_index();
        jiter.known_skip(peek)?;
        let object_slice = jiter.slice_to_current(start);
        let object_string = std::str::from_utf8(object_slice)?;
        Ok(object_string.to_owned())
    } else {
        get_err!()
    }
}
