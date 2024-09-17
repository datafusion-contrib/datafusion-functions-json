use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonLength,
    json_length,
    json_data path,
    r#"Get the length of the array or object at the given path."#
);

#[derive(Debug)]
pub(super) struct JsonLength {
    signature: Signature,
    aliases: [String; 2],
}

impl Default for JsonLength {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_length".to_string(), "json_len".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonLength {
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
        return_type_check(arg_types, self.name(), DataType::UInt64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        invoke::<UInt64Array, u64>(
            args,
            jiter_json_length,
            |c| Ok(Arc::new(c) as ArrayRef),
            ScalarValue::UInt64,
            true,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_length(opt_json: Option<&str>, path: &[JsonPath]) -> Result<u64, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
        match peek {
            Peek::Array => {
                let mut peek_opt = jiter.known_array()?;
                let mut length: u64 = 0;
                while let Some(peek) = peek_opt {
                    jiter.known_skip(peek)?;
                    length += 1;
                    peek_opt = jiter.array_step()?;
                }
                Ok(length)
            }
            Peek::Object => {
                let mut opt_key = jiter.known_object()?;

                let mut length: u64 = 0;
                while opt_key.is_some() {
                    jiter.next_skip()?;
                    length += 1;
                    opt_key = jiter.next_key()?;
                }
                Ok(length)
            }
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
