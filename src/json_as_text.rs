use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, Peek};

use crate::common::{get_err, jiter_json_find, jiter_skip_str, GetError, JsonPath};
use crate::common_macros::make_udf_function;
use crate::common_string::{invoke_string, string_return_type, StringGetter, StringResultBuilder};

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
        string_return_type(arg_types, self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke_string::<Self>(&args.args)
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

impl StringGetter for JsonAsText {
    fn append<B: StringResultBuilder>(opt_json: Option<&str>, path: &[JsonPath], builder: &mut B) {
        if let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) {
            builder.append_str(text_value(json, &mut jiter, peek).ok());
        } else {
            builder.append_str(None);
        }
    }

    fn get(opt_json: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
        let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) else {
            return get_err!();
        };
        text_value(json, &mut jiter, peek).map(str::to_owned)
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
    use crate::common_string::test_util::assert_string_direct_matches_owned;
    use datafusion::common::ScalarValue;

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
        assert_string_direct_matches_owned::<JsonAsText>(&rows, &paths);
    }
}
