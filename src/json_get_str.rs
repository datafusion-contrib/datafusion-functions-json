use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{Jiter, Peek};

use crate::common::{get_err, jiter_json_find, GetError, JsonPath};
use crate::common_macros::make_udf_function;
use crate::common_string::{invoke_string, string_return_type, StringGetter, StringResultBuilder};

make_udf_function!(
    JsonGetStr,
    json_get_str,
    json_data path,
    r#"Get a string value from a JSON string by its "path""#
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonGetStr {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetStr {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_str".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetStr {
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

impl StringGetter for JsonGetStr {
    fn append<B: StringResultBuilder>(opt_json: Option<&str>, path: &[JsonPath], builder: &mut B) {
        if let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) {
            builder.append_str(str_value(&mut jiter, peek).ok());
        } else {
            builder.append_str(None);
        }
    }

    fn get(opt_json: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
        let Some((mut jiter, peek)) = jiter_json_find(opt_json, path) else {
            return get_err!();
        };
        str_value(&mut jiter, peek).map(str::to_owned)
    }
}

fn str_value<'a>(jiter: &'a mut Jiter<'_>, peek: Peek) -> Result<&'a str, GetError> {
    match peek {
        Peek::String => Ok(jiter.known_str()?),
        _ => get_err!(),
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
            Some(r#"{"a":"escaped\nvalue","b":null,"c":["x",{"d":"deep"}]}"#),
            Some(r#"{"a":42,"b":"plain","c":[]}"#),
            Some(r#"{"a":"","c":"text"}"#),
            Some("invalid"),
            None,
        ];
        let paths = vec![
            vec![],
            vec![ScalarValue::Utf8(Some("a".to_owned()))],
            vec![ScalarValue::Utf8(Some("b".to_owned()))],
            vec![ScalarValue::Utf8(Some("missing".to_owned()))],
            vec![ScalarValue::Utf8(Some("c".to_owned())), ScalarValue::Int64(Some(0))],
            vec![
                ScalarValue::Utf8(Some("c".to_owned())),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("d".to_owned())),
            ],
            vec![ScalarValue::Utf8(None)],
        ];
        assert_string_direct_matches_owned::<JsonGetStr>(&rows, &paths);
    }
}
