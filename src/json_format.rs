use std::any::Any;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Utf8;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::common::{invoke, return_type_check};
use crate::common_macros::make_udf_function;
use crate::json_as_text::jiter_json_as_text;

make_udf_function!(
    JsonFormat,
    json_format,
    json_data,
    r#"Get any value from a JSON string by its "path", represented as a string"#
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonFormat {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![Utf8], Volatility::Immutable),
            aliases: ["json_format".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonFormat {
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
        return_type_check(arg_types, self.name(), Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let root = vec![];
        invoke::<StringArray>(&args.args, |json, _| jiter_json_as_text(json, &root))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

