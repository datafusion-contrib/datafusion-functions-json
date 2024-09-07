use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::arrow::array::{ArrayRef, BooleanArray};
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::common::{invoke, jiter_json_find, scalar_udf_return_type, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonContains,
    json_contains,
    json_data path,
    r#"Does the key/index exist within the JSON value as the specified "path"?"#
);

#[derive(Debug)]
pub(super) struct JsonContains {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonContains {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_contains".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonContains {
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
        if arg_types.len() < 2 {
            plan_err!("The 'json_contains' function requires two or more arguments.")
        } else {
            scalar_udf_return_type(arg_types, self.name(), DataType::Boolean)
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        invoke::<BooleanArray, bool>(
            args,
            jiter_json_contains,
            |c| Ok(Arc::new(c) as ArrayRef),
            ScalarValue::Boolean,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[allow(clippy::unnecessary_wraps)]
fn jiter_json_contains(json_data: Option<&str>, path: &[JsonPath]) -> Result<bool, GetError> {
    Ok(jiter_json_find(json_data, path).is_some())
}
