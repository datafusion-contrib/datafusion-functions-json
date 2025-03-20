use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{
    get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath, Sortedness,
};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonAsText,
    json_as_text,
    json_data path,
    r#"Get any value from a JSON string by its "path", represented as a string"#,
    Sortedness::Unspecified
);

make_udf_function!(
    JsonAsText,
    json_as_text_top_level_sorted,
    json_data path,
    r#"Get any value from a JSON string by its "path", represented as a string; assumes the JSON string's top level object's keys are sorted."#,
    Sortedness::TopLevel
);

make_udf_function!(
    JsonAsText,
    json_as_text_recursive_sorted,
    json_data path,
    r#"Get any value from a JSON string by its "path", represented as a string; assumes all json object's keys are sorted."#,
    Sortedness::Recursive
);

#[derive(Debug)]
pub(super) struct JsonAsText {
    signature: Signature,
    aliases: [String; 1],
    sorted: Sortedness,
}

impl JsonAsText {
    pub fn new(sorted: Sortedness) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: [format!("json_as_text{}", sorted.function_name_suffix())],
            sorted,
        }
    }
}

impl ScalarUDFImpl for JsonAsText {
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
        invoke::<StringArray>(&args.args, |args, path| jiter_json_as_text(args, path, self.sorted))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl InvokeResult for StringArray {
    type Item = String;

    type Builder = StringBuilder;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        StringBuilder::with_capacity(capacity, 0)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Utf8(value)
    }
}

fn jiter_json_as_text(opt_json: Option<&str>, path: &[JsonPath], sorted: Sortedness) -> Result<String, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path, sorted) {
        match peek {
            Peek::Null => {
                jiter.known_null()?;
                get_err!()
            }
            Peek::String => Ok(jiter.known_str()?.to_owned()),
            _ => {
                let start = jiter.current_index();
                jiter.known_skip(peek)?;
                let object_slice = jiter.slice_to_current(start);
                let object_string = std::str::from_utf8(object_slice)?;
                Ok(object_string.to_owned())
            }
        }
    } else {
        get_err!()
    }
}
