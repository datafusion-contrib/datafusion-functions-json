use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, Peek};

use crate::common::{
    get_err, invoke, invoke_array_scalars_direct, jiter_json_find, jiter_skip_str, return_type_check, GetError,
    InvokeResult, JsonPath,
};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonAsText,
    json_as_text,
    json_data path,
    r#"Get any value from a JSON string by its "path", represented as a string"#
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonAsText {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonAsText {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_as_text".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonAsText {
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check::<StringArray>(arg_types, self.name(), DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if let Some(result) = invoke_array_scalars_direct::<StringArray>(&args.args, append_json_as_text)? {
            return Ok(result);
        }
        invoke::<StringArray>(&args.args, jiter_json_as_text)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn placement(
        &self,
        args: &[datafusion::logical_expr::ExpressionPlacement],
    ) -> datafusion::logical_expr::ExpressionPlacement {
        // If the first argument is a column and the remaining arguments are literals (a path)
        // then we can push this UDF down to the leaf nodes.
        if args.len() >= 2
            && matches!(args[0], datafusion::logical_expr::ExpressionPlacement::Column)
            && args[1..]
                .iter()
                .all(|arg| matches!(arg, datafusion::logical_expr::ExpressionPlacement::Literal))
        {
            datafusion::logical_expr::ExpressionPlacement::MoveTowardsLeafNodes
        } else {
            datafusion::logical_expr::ExpressionPlacement::KeepInPlace
        }
    }
}

impl InvokeResult for StringArray {
    type Item = String;

    type Builder = StringBuilder;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        StringBuilder::with_capacity(capacity, 0)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Utf8(value)
    }
}

fn jiter_json_as_text(opt_json: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
    let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) else {
        return get_err!();
    };
    text_value(json, &mut jiter, peek).map(str::to_owned)
}

fn append_json_as_text(opt_json: Option<&str>, path: &[JsonPath], builder: &mut StringBuilder) {
    if let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) {
        builder.append_option(text_value(json, &mut jiter, peek).ok());
    } else {
        builder.append_null();
    }
}

/// A JSON string decoded, any other value as its raw JSON text. JSON `null` is an error, so
/// it becomes SQL null like a missing path.
fn text_value<'a, 'j: 'a>(json: &'j str, jiter: &'a mut Jiter<'j>, peek: Peek) -> Result<&'a str, GetError> {
    match peek {
        Peek::Null => get_err!(),
        Peek::String => Ok(jiter.known_str()?),
        _ => jiter_skip_str(json, jiter, peek),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_util::assert_direct_matches_owned;

    #[test]
    fn direct_builder_matches_owned_results() {
        let rows = [
            Some(r#"{"a":"escaped\nvalue","b":null,"c":[1,{"x":true}]}"#),
            Some(r#"{"a":42,"b":true,"c":[1,{"nested":"yes"}]}"#),
            Some(r#"{"a":null,"a":"second","c":"text"}"#),
            Some("invalid"),
            None,
        ];
        let paths = vec![
            vec![],
            vec![ScalarValue::Utf8(Some("a".to_owned()))],
            vec![ScalarValue::Utf8(Some("b".to_owned()))],
            vec![ScalarValue::Utf8(Some("missing".to_owned()))],
            vec![ScalarValue::Utf8(Some("c".to_owned())), ScalarValue::Int64(Some(1))],
            vec![
                ScalarValue::Utf8(Some("c".to_owned())),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("nested".to_owned())),
            ],
            vec![ScalarValue::Utf8(None)],
        ];
        assert_direct_matches_owned::<StringArray>(&rows, &paths, append_json_as_text, jiter_json_as_text);
    }
}
