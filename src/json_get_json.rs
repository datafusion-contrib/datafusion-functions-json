use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::common::{get_err, jiter_json_find, jiter_skip_str, GetError, JsonPath};
use crate::common_macros::make_udf_function;
use crate::common_string::{invoke_string, string_return_type, StringGetter, StringResultBuilder};
use crate::common_union::json_field_metadata;

make_udf_function!(
    JsonGetJson,
    json_get_json,
    json_data path,
    r#"Get a nested raw JSON string from a JSON string by its "path""#
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonGetJson {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetJson {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_json".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetJson {
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        string_return_type(arg_types, self.name())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
        let arg_types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let return_type = self.return_type(&arg_types)?;
        Ok(Arc::new(
            Field::new(self.name(), return_type, true).with_metadata(json_field_metadata()),
        ))
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

impl StringGetter for JsonGetJson {
    fn append<B: StringResultBuilder>(opt_json: Option<&str>, path: &[JsonPath], builder: &mut B) {
        if let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) {
            builder.append_str(jiter_skip_str(json, &mut jiter, peek).ok());
        } else {
            builder.append_str(None);
        }
    }

    fn get(opt_json: Option<&str>, path: &[JsonPath]) -> Result<String, GetError> {
        let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) else {
            return get_err!();
        };
        jiter_skip_str(json, &mut jiter, peek).map(str::to_owned)
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
            Some(r#"{"a":42,"b":{"nested":[1,2,{"y":"z"}]},"c":"text"}"#),
            Some(r#"{"a":[],"b":{}}"#),
            Some("invalid"),
            None,
        ];
        let paths = vec![
            vec![],
            vec![ScalarValue::Utf8(Some("a".to_owned()))],
            vec![ScalarValue::Utf8(Some("b".to_owned()))],
            vec![ScalarValue::Utf8(Some("missing".to_owned()))],
            vec![ScalarValue::Utf8(Some("c".to_owned())), ScalarValue::Int64(Some(1))],
            vec![ScalarValue::Utf8(None)],
        ];
        assert_string_direct_matches_owned::<JsonGetJson>(&rows, &paths);
    }
}
