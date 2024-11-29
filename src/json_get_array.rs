use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DatafusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use jiter::Peek;

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, JsonPath};
use crate::common_macros::make_udf_function;
use crate::common_union::{JsonArrayField, JsonUnion};

make_udf_function!(
    JsonGetArray,
    json_get_array,
    json_data path,
    r#"Get an arrow array value from a JSON string by its "path""#
);

#[derive(Debug)]
pub(super) struct JsonGetArray {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetArray {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_array".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DatafusionResult<DataType> {
        return_type_check(
            arg_types,
            self.name(),
            DataType::List(Field::new("item", DataType::Utf8, true).into()),
        )
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DatafusionResult<ColumnarValue> {
        let to_array = |c: JsonUnion| {
            let array: ListArray = c.try_into()?;
            Ok(Arc::new(array) as ArrayRef)
        };

        invoke::<JsonUnion, JsonArrayField>(
            args,
            jiter_json_get_array,
            to_array,
            |i| i.map_or_else(|| ScalarValue::Null, Into::into),
            true,
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_array(json_data: Option<&str>, path: &[JsonPath]) -> Result<JsonArrayField, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path) {
        match peek {
            Peek::Array => {
                let mut peek_opt = jiter.known_array()?;
                let mut elements = Vec::new();

                while let Some(peek) = peek_opt {
                    let start = jiter.current_index();
                    jiter.known_skip(peek)?;
                    let object_slice = jiter.slice_to_current(start);
                    let object_string = std::str::from_utf8(object_slice)?;

                    elements.push(object_string.to_owned());

                    peek_opt = jiter.array_step()?;
                }

                Ok(JsonArrayField(elements))
            }
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
