use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Float64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{NumberAny, Peek};

use crate::common::{
    get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath, Sortedness,
};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetFloat,
    json_get_float,
    json_data path,
    r#"Get a float value from a JSON string by its "path""#,
    Sortedness::Unspecified
);

make_udf_function!(
    JsonGetFloat,
    json_get_float_top_level_sorted,
    json_data path,
    r#"Get an float value from a JSON string by its "path"; assumes the JSON string's top level object's keys are sorted."#,
    Sortedness::TopLevel
);

make_udf_function!(
    JsonGetFloat,
    json_get_float_recursive_sorted,
    json_data path,
    r#"Get an float value from a JSON string by its "path"; assumes all object's keys are sorted."#,
    Sortedness::Recursive
);

#[derive(Debug)]
pub(super) struct JsonGetFloat {
    signature: Signature,
    aliases: [String; 1],
    sorted: Sortedness,
}

impl JsonGetFloat {
    pub fn new(sorted: Sortedness) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: [format!("json_get_float{}", sorted.function_name_suffix())],
            sorted,
        }
    }
}

impl ScalarUDFImpl for JsonGetFloat {
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
        return_type_check(arg_types, self.name(), DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<Float64Array>(&args.args, |json_data, path| {
            jiter_json_get_float(json_data, path, self.sorted)
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

impl InvokeResult for Float64Array {
    type Item = f64;

    type Builder = Float64Builder;

    // Cheaper to produce a float array rather than dict-encoded floats
    const ACCEPT_DICT_RETURN: bool = false;

    fn builder(capacity: usize) -> Self::Builder {
        Float64Builder::with_capacity(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Float64(value)
    }
}

fn jiter_json_get_float(json_data: Option<&str>, path: &[JsonPath], sorted: Sortedness) -> Result<f64, GetError> {
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
            _ => match jiter.known_number(peek)? {
                NumberAny::Float(f) => Ok(f),
                NumberAny::Int(int) => Ok(int.into()),
            },
        }
    } else {
        get_err!()
    }
}
