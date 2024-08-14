use std::any::Any;
use std::sync::Arc;

use arrow::array::{GenericListArray, ListBuilder, StringBuilder};
use arrow_schema::{DataType, Field};
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;

use crate::common::{check_args, get_err, invoke, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;

struct StrArrayColumn {
    rows: GenericListArray<i32>,
}

impl FromIterator<Option<Vec<String>>> for StrArrayColumn {
    fn from_iter<T: IntoIterator<Item = Option<Vec<String>>>>(iter: T) -> Self {
        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder);

        for row in iter {
            if let Some(row) = row {
                for elem in row {
                    list_builder.values().append_value(elem);
                }

                list_builder.append(true);
            } else {
                list_builder.append(false);
            }
        }

        Self {
            rows: list_builder.finish(),
        }
    }
}

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

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        check_args(arg_types, self.name()).map(|()| DataType::List(Field::new("item", DataType::Utf8, true).into()))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        invoke::<StrArrayColumn, Vec<String>>(
            args,
            jiter_json_get_array,
            |c| Ok(Arc::new(c.rows) as ArrayRef),
            |i| {
                let string_builder = StringBuilder::new();
                let mut list_builder = ListBuilder::new(string_builder);

                if let Some(row) = i {
                    for elem in row {
                        list_builder.values().append_value(elem);
                    }
                }

                ScalarValue::List(list_builder.finish().into())
            },
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn jiter_json_get_array(json_data: Option<&str>, path: &[JsonPath]) -> Result<Vec<String>, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path) {
        match peek {
            Peek::Array => {
                let mut peek_opt = jiter.known_array()?;
                let mut array_values = Vec::new();

                while let Some(peek) = peek_opt {
                    let start = jiter.current_index();
                    jiter.known_skip(peek)?;
                    let object_slice = jiter.slice_to_current(start);
                    let object_string = std::str::from_utf8(object_slice)?;

                    array_values.push(object_string.to_owned());

                    peek_opt = jiter.array_step()?;
                }

                Ok(array_values)
            }
            _ => get_err!(),
        }
    } else {
        get_err!()
    }
}
