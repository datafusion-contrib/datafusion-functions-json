use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::Peek;
use std::ops::Range;

use crate::common::{
    get_err, invoke, invoke_array_scalars_direct, jiter_json_find, jiter_skip_str, return_type_check, GetError,
    InvokeResult, JsonPath,
};
use crate::common_macros::make_udf_function;
use crate::common_union::json_field_metadata;

fn list_item_field() -> Field {
    Field::new("item", DataType::Utf8, true).with_metadata(json_field_metadata())
}

make_udf_function!(
    JsonGetArray,
    json_get_array,
    json_data path,
    r#"Get an arrow array from a JSON string by its "path""#
);

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check::<BuildArrayList>(arg_types, self.name(), DataType::List(Arc::new(list_item_field())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if let Some(result) = invoke_direct(&args.args)? {
            return Ok(result);
        }
        invoke::<BuildArrayList>(&args.args, jiter_json_get_array)
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

#[derive(Debug)]
struct BuildArrayList;

impl InvokeResult for BuildArrayList {
    type Item = Vec<String>;

    type Builder = ListBuilder<StringBuilder>;

    const ACCEPT_DICT_RETURN: bool = false;

    fn builder(capacity: usize) -> Self::Builder {
        let values_builder = StringBuilder::new();
        ListBuilder::with_capacity(values_builder, capacity).with_field(list_item_field())
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value.map(|v| v.into_iter().map(Some)));
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        let mut builder = ListBuilder::new(StringBuilder::new()).with_field(list_item_field());

        if let Some(array_items) = value {
            for item in array_items {
                builder.values().append_value(item);
            }

            builder.append(true);
        } else {
            builder.append(false);
        }
        let array = builder.finish();
        ScalarValue::List(Arc::new(array))
    }
}

fn jiter_json_get_array(opt_json: Option<&str>, path: &[JsonPath]) -> Result<Vec<String>, GetError> {
    let (Some(json), Some((mut jiter, Peek::Array))) = (opt_json, jiter_json_find(opt_json, path)) else {
        return get_err!();
    };
    let mut array_items = Vec::new();
    let mut peek_opt = jiter.known_array()?;
    while let Some(element_peek) = peek_opt {
        array_items.push(jiter_skip_str(json, &mut jiter, element_peek)?.to_owned());
        peek_opt = jiter.array_step()?;
    }
    Ok(array_items)
}

/// The direct path of `invoke_array_scalars_direct`, with one scratch buffer of element ranges
/// shared by every row of the batch.
fn invoke_direct(args: &[ColumnarValue]) -> DataFusionResult<Option<ColumnarValue>> {
    let mut scratch = Vec::new();
    invoke_array_scalars_direct::<BuildArrayList>(args, |opt_json, path, builder| {
        append_json_array(&mut scratch, opt_json, path, builder);
    })
}

/// Elements are buffered as byte ranges until the whole array parses, so a malformed
/// element leaves nothing in the values builder and the row is null.
fn append_json_array(
    scratch: &mut Vec<Range<usize>>,
    opt_json: Option<&str>,
    path: &[JsonPath],
    builder: &mut ListBuilder<StringBuilder>,
) {
    scratch.clear();
    let parsed = (|| {
        let Some((mut jiter, Peek::Array)) = jiter_json_find(opt_json, path) else {
            return get_err!();
        };
        let mut peek = jiter.known_array()?;
        while let Some(element) = peek {
            let start = jiter.current_index();
            jiter.known_skip(element)?;
            scratch.push(start..jiter.current_index());
            peek = jiter.array_step()?;
        }
        Ok::<_, GetError>(())
    })();
    match (parsed, opt_json) {
        // jiter stops on ASCII bytes, so every range boundary is a char boundary of `json`
        (Ok(()), Some(json)) => builder.append_value(scratch.iter().map(|range| Some(&json[range.clone()]))),
        _ => builder.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_util::assert_direct_matches_owned;
    use datafusion::arrow::array::AsArray;

    #[test]
    fn direct_builder_matches_owned_results() {
        let rows = [
            Some(r#"{"a":[null,true,42,"escaped\nvalue",{"x":[1,2]}],"b":[]}"#),
            Some(r#"{"a":[1,2,3,4,5,6,7,8,9],"b":null}"#),
            Some(r#"{"a":[[1,2],[3,4]],"b":["x"]}"#),
            Some(r#"{"a":false,"b":["x"]}"#),
            Some(r#"{"a":[1,truX]}"#),
            Some(r#"{"a":[9],"b":[]}"#),
            Some("invalid"),
            None,
        ];
        let paths = vec![
            vec![],
            vec![ScalarValue::Utf8(Some("a".to_owned()))],
            vec![ScalarValue::Utf8(Some("b".to_owned()))],
            vec![ScalarValue::Utf8(Some("missing".to_owned()))],
            vec![ScalarValue::Utf8(Some("a".to_owned())), ScalarValue::Int64(Some(0))],
            vec![ScalarValue::Utf8(None)],
        ];
        let mut scratch = Vec::new();
        assert_direct_matches_owned::<BuildArrayList>(
            &rows,
            &paths,
            |opt_json, path, builder| append_json_array(&mut scratch, opt_json, path, builder),
            jiter_json_get_array,
        );
    }

    #[test]
    fn malformed_late_element_does_not_append_partial_values() {
        let args = vec![
            ColumnarValue::Array(Arc::new(datafusion::arrow::array::StringArray::from(vec![
                r#"{"a":[1,truX]}"#,
                r#"{"a":[9]}"#,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_owned()))),
        ];
        let ColumnarValue::Array(result) = invoke_direct(&args).unwrap().unwrap() else {
            panic!("array input must produce array output");
        };
        assert!(result.is_null(0));
        let list = result.as_list::<i32>();
        assert_eq!(list.values().len(), 1);
        assert_eq!(list.value(1).as_string::<i32>().value(0), "9");
    }
}
