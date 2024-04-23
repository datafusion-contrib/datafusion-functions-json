use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::{NumberInt, Peek};

use crate::common::{check_args, get_err, invoke, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetInt,
    json_get_int,
    json_data path,
    r#"Get an integer value from a JSON object by its "path""#
);

#[derive(Debug)]
pub(super) struct JsonGetInt {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetInt {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_int".to_string()],
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
        check_args(arg_types, self.name()).map(|()| DataType::Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        invoke::<Int64Array, i64>(
            args,
            jiter_json_get_int,
            |c| Ok(Arc::new(c) as ArrayRef),
            ScalarValue::Int64,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_int(json_data: Option<&str>, path: &[JsonPath]) -> Result<i64, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path) {
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
