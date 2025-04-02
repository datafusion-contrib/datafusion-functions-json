use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::Utf8;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::common::{invoke, return_type_check};
use crate::common_macros::make_udf_function;
use crate::common_union::JsonUnion;
use crate::json_get::jiter_json_get_union;

make_udf_function!(
    JsonParse,
    json_parse,
    json_data,
    r#"Parses the JSON string passed"#
);

#[derive(Debug)]
pub(super) struct JsonParse {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonParse {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![Utf8], Volatility::Immutable),
            aliases: ["json_parse".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonParse {
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
        return_type_check(arg_types, self.name(), JsonUnion::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let root = vec![];
        invoke::<JsonUnion>(&args.args, |json, _| jiter_json_get_union(json, &root))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

