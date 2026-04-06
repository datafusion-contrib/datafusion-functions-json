use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringViewBuilder, UnionArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::common_macros::make_udf_function;
use crate::common_union::{is_json_union, json_field_metadata, JsonUnionEncoder, JsonUnionValue, JSON_UNION_DATA_TYPE};

make_udf_function!(
    JsonUnionToText,
    json_union_to_text,
    json_union,
    "Flatten a JSON union value (produced by `json_get`) into its canonical JSON text"
);

/// Flattens the heterogeneous JSON union that `json_get` produces into a single
/// `Utf8View` column of canonical JSON text: scalars render as `true` / `42` /
/// `1.5`, strings are JSON-quoted and escaped, and array/object arms (already raw
/// JSON text) pass through. A JSON `null` arm becomes a SQL `NULL`.
///
/// Useful when a JSON-union column must be materialized somewhere that can't
/// represent an Arrow `Union` — e.g. the Parquet writer, which rejects unions
/// (`arrow_to_parquet_schema` panics with "See ARROW-8817.").
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonUnionToText {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonUnionToText {
    fn default() -> Self {
        Self {
            // Exactly the JSON union — any other argument type is a planning error.
            signature: Signature::exact(vec![JSON_UNION_DATA_TYPE.clone()], Volatility::Immutable),
            aliases: ["json_union_to_text".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonUnionToText {
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match arg_types {
            [t] if is_json_union(t) => Ok(DataType::Utf8View),
            _ => plan_err!("json_union_to_text expects a single JSON-union argument, got {arg_types:?}"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
        let arg_types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let return_type = self.return_type(&arg_types)?;
        Ok(Arc::new(
            Field::new(self.name(), return_type, true).with_metadata(json_field_metadata()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let Some(arg) = args.args.into_iter().next() else {
            return exec_err!("json_union_to_text expects one argument");
        };
        let array = arg.into_array(args.number_rows)?;
        Ok(ColumnarValue::Array(json_union_to_text_array(&array)?))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Encode a JSON-union array into a `Utf8View` array of canonical JSON text.
fn json_union_to_text_array(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    let Some(union) = array.as_any().downcast_ref::<UnionArray>() else {
        return exec_err!("json_union_to_text expects a UnionArray argument");
    };
    let Some(encoder) = JsonUnionEncoder::from_union(union.clone()) else {
        return exec_err!("json_union_to_text argument is not the JSON union type");
    };

    let mut builder = StringViewBuilder::with_capacity(encoder.len());
    // Scalar arms are JSON-encoded with serde_json (string escaping, float
    // formatting, …); the array/object arms already hold raw JSON text and pass
    // through verbatim.
    let mut scratch: Vec<u8> = Vec::new();
    for idx in 0..encoder.len() {
        scratch.clear();
        let write_result = match encoder.get_value(idx) {
            JsonUnionValue::JsonNull => {
                builder.append_null();
                continue;
            }
            JsonUnionValue::Bool(b) => serde_json::to_writer(&mut scratch, &b),
            JsonUnionValue::Int(i) => serde_json::to_writer(&mut scratch, &i),
            JsonUnionValue::Float(f) => serde_json::to_writer(&mut scratch, &f),
            JsonUnionValue::Str(s) => serde_json::to_writer(&mut scratch, s),
            JsonUnionValue::Array(s) | JsonUnionValue::Object(s) => {
                builder.append_value(s);
                continue;
            }
        };
        write_result.map_err(|e| exec_datafusion_err!("json_union_to_text: failed to encode JSON value: {e}"))?;
        // `serde_json` always emits valid UTF-8.
        let text = std::str::from_utf8(&scratch)
            .map_err(|e| exec_datafusion_err!("json_union_to_text: encoded value was not UTF-8: {e}"))?;
        builder.append_value(text);
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common_union::{JsonUnion, JsonUnionField};
    use datafusion::arrow::array::StringViewArray;

    #[test]
    fn flattens_each_arm_to_json_text() {
        let union = JsonUnion::from_iter(vec![
            Some(JsonUnionField::JsonNull),
            Some(JsonUnionField::Bool(true)),
            Some(JsonUnionField::Int(42)),
            Some(JsonUnionField::Float(1.5)),
            Some(JsonUnionField::Str("foo\"bar\n\u{1}".to_string())),
            Some(JsonUnionField::Array("[1,2]".to_string())),
            Some(JsonUnionField::Object(r#"{"a":1}"#.to_string())),
            None,
        ]);
        let array: ArrayRef = Arc::new(UnionArray::try_from(union).unwrap());

        let out = json_union_to_text_array(&array).unwrap();
        let strings = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        let got: Vec<Option<&str>> = (0..strings.len())
            .map(|i| (!strings.is_null(i)).then(|| strings.value(i)))
            .collect();
        assert_eq!(
            got,
            vec![
                None,                             // JsonNull
                Some("true"),                     // Bool
                Some("42"),                       // Int
                Some("1.5"),                      // Float
                Some("\"foo\\\"bar\\n\\u0001\""), // Str: JSON-quoted + escaped (quote, newline, control char)
                Some("[1,2]"),                    // Array (passthrough)
                Some(r#"{"a":1}"#),               // Object (passthrough)
                None,                             // None
            ]
        );
    }

    #[test]
    fn output_field_is_marked_as_json() {
        let udf = JsonUnionToText::default();
        let arg = Arc::new(Field::new("j", JSON_UNION_DATA_TYPE.clone(), true));
        let field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&arg),
                scalar_arguments: &[],
            })
            .unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8View);
        assert_eq!(
            field.metadata().get("ARROW:extension:name").map(String::as_str),
            Some("arrow.json")
        );
    }
}
