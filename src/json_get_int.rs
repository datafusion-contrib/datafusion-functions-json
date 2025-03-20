use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, Int64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{NumberInt, Peek};

use crate::common::{
    get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath, Sortedness,
};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetInt,
    json_get_int,
    json_data path,
    r#"Get an integer value from a JSON string by its "path""#,
    Sortedness::Unspecified
);

make_udf_function!(
    JsonGetInt,
    json_get_int_top_level_sorted,
    json_data path,
    r#"Get an integer value from a JSON string by its "path"; assumes the JSON string's top level object's keys are sorted."#,
    Sortedness::TopLevel
);

make_udf_function!(
    JsonGetInt,
    json_get_int_recursive_sorted,
    json_data path,
    r#"Get an integer value from a JSON string by its "path"; assumes all json object's keys are sorted."#,
    Sortedness::Recursive
);

#[derive(Debug)]
pub(super) struct JsonGetInt {
    signature: Signature,
    aliases: [String; 1],
    sorted: Sortedness,
}

impl JsonGetInt {
    pub fn new(sorted: Sortedness) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: [format!("json_get_int{}", sorted.function_name_suffix())],
            sorted,
        }
    }
}

impl ScalarUDFImpl for JsonGetInt {
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
        return_type_check(arg_types, self.name(), DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<Int64Array>(&args.args, |json, path| jiter_json_get_int(json, path, self.sorted))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl InvokeResult for Int64Array {
    type Item = i64;

    type Builder = Int64Builder;

    // Cheaper to return an int array rather than dict-encoded ints
    const ACCEPT_DICT_RETURN: bool = false;

    fn builder(capacity: usize) -> Self::Builder {
        Int64Builder::with_capacity(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Int64(value)
    }
}

fn jiter_json_get_int(json_data: Option<&str>, path: &[JsonPath], sorted: Sortedness) -> Result<i64, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path, sorted) {
        match peek {
            // numbers are represented by everything else in peek, hence doing it this way
            Peek::Null
            | Peek::True
            | Peek::False
            | Peek::Minus
            | Peek::Infinity
            | Peek::NaN
            | Peek::String
            | Peek::Array
            | Peek::Object => get_err!(),
            _ => match jiter.known_int(peek)? {
                NumberInt::Int(i) => Ok(i),
                NumberInt::BigInt(_) => get_err!(),
            },
        }
    } else {
        get_err!()
    }
}
