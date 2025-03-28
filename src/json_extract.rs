use std::any::Any;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, DataType::Utf8};
use datafusion::common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jsonpath_rust::parser::model::{Segment, Selector};
use jsonpath_rust::parser::parse_json_path;
use crate::common::{invoke, return_type_check, JsonPath};
use crate::common_macros::make_udf_function;
use crate::json_get_json::jiter_json_get_json;

make_udf_function!(
    JsonExtract,
    json_extract,
    json_data path,
    r#"Get a value from a JSON string by its "path" in JSONPath format"#
);

#[derive(Debug)]
pub(super) struct JsonExtract {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonExtract {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![Utf8, Utf8], // JSON data and JSONPath as strings
                Volatility::Immutable,
            ),
            aliases: ["json_extract".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonExtract {
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
        return_type_check(arg_types, self.name(), Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "'{}' expects exactly 2 arguments (JSON data, path), got {}",
                self.name(),
                args.args.len()
            );
        }

        let json_arg = &args.args[0];
        let path_arg = &args.args[1];

        let path_str = match path_arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            _ => return exec_err!("'{}' expects a valid JSONPath string (e.g., '$.key[0]') as second argument", self.name()),
        };

        let path = parse_jsonpath(path_str);

        invoke::<StringArray>(&[json_arg.clone()], |json, _| {
            jiter_json_get_json(json, &path)
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn parse_jsonpath(path: &str) -> Vec<JsonPath<'static>> {
    let segments = parse_json_path(path)
        .map(|it| it.segments)
        .unwrap_or(Vec::new());

    segments.into_iter().map(|segment| {
        match segment {
            Segment::Selector(s) => match s {
                Selector::Name(name) => JsonPath::Key(Box::leak(name.into_boxed_str())),
                Selector::Index(idx) => JsonPath::Index(idx as usize),
                _ => JsonPath::None,
            },
            _ => JsonPath::None,
        }
    }).collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use super::*;

    // Test cases for parse_jsonpath
    #[rstest]
    #[case("$.a.aa", vec![JsonPath::Key("a"), JsonPath::Key("aa")])]
    #[case("$.a.ab[0].ac", vec![JsonPath::Key("a"), JsonPath::Key("ab"), JsonPath::Index(0), JsonPath::Key("ac")])]
    #[case("$.a.ab[1].ad", vec![JsonPath::Key("a"), JsonPath::Key("ab"), JsonPath::Index(1), JsonPath::Key("ad")])]
    #[case(r#"$.a["a b"].ad"#, vec![JsonPath::Key("a"), JsonPath::Key("\"a b\""), JsonPath::Key("ad")])]
    #[tokio::test]
    async fn test_parse_jsonpath(
        #[case] path: &str,
        #[case] expected: Vec<JsonPath<'static>>,
    ) {
        let result = parse_jsonpath(path);
        assert_eq!(result, expected);
    }
}