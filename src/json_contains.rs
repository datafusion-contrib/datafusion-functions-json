use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::arrow::array::{ArrayRef, BooleanArray};
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::common::{invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonContains,
    json_contains,
    json_data path,
    r#"Does the key/index exist within the JSON value as the specified "path"?"#,
    crate::common::Sortedness::Unspecified
);

make_udf_function!(
    JsonContains,
    json_contains_top_level_sorted,
    json_data path,
    r#"Does the key/index exist within the JSON value as the specified "path"; assumes the JSON string's top level object's keys are sorted?"#,
    crate::common::Sortedness::TopLevel
);

make_udf_function!(
    JsonContains,
    json_contains_recursive_sorted,
    json_data path,
    r#"Does the key/index exist within the JSON value as the specified "path"; assumes all json object's keys are sorted?"#,
    crate::common::Sortedness::Recursive
);

#[derive(Debug)]
pub(super) struct JsonContains {
    signature: Signature,
    aliases: [String; 1],
    sorted: crate::common::Sortedness,
}

impl JsonContains {
    pub fn new(sorted: crate::common::Sortedness) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: [format!("json_contains{}", sorted.function_name_suffix())],
            sorted,
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
            return_type_check(arg_types, self.name(), DataType::Boolean).map(|_| DataType::Boolean)
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        invoke::<BooleanArray>(args, |json, path| jiter_json_contains(json, path, self.sorted))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl InvokeResult for BooleanArray {
    type Item = bool;

    type Builder = BooleanBuilder;

    // Using boolean inside a dictionary is not an optimization!
    const ACCEPT_DICT_RETURN: bool = false;

    fn builder(capacity: usize) -> Self::Builder {
        BooleanBuilder::with_capacity(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> Result<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Boolean(value)
    }
}

#[allow(clippy::unnecessary_wraps)]
fn jiter_json_contains(
    json_data: Option<&str>,
    path: &[JsonPath],
    sorted: crate::common::Sortedness,
) -> Result<bool, GetError> {
    Ok(jiter_json_find(json_data, path, sorted).is_some())
}
