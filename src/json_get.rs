use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::UnionArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use jiter::{Jiter, NumberAny, NumberInt, Peek};

use crate::common::InvokeResult;
use crate::common::{
    get_err, invoke, invoke_array_scalars_direct, jiter_json_find, jiter_skip_str, return_type_check, GetError,
    JsonPath,
};
use crate::common_macros::make_udf_function;
use crate::common_union::{JsonUnion, JsonUnionField, JsonUnionValue};

make_udf_function!(
    JsonGet,
    json_get,
    json_data path,
    r#"Get a value from a JSON string by its "path""#
);

// build_typed_get!(JsonGet, "json_get", Union, Float64Array, jiter_json_get_float);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonGet {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGet {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGet {
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check::<JsonUnion>(arg_types, self.name(), JsonUnion::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if let Some(result) = invoke_array_scalars_direct::<JsonUnion>(&args.args, append_json_get_union)? {
            return Ok(result);
        }
        invoke::<JsonUnion>(&args.args, jiter_json_get_union)
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

impl InvokeResult for JsonUnion {
    type Item = JsonUnionField;

    type Builder = JsonUnion;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        JsonUnion::new(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        if let Some(value) = value {
            builder.push(&value);
        } else {
            builder.push_none();
        }
    }

    fn finish(builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        let array: UnionArray = builder.try_into()?;
        Ok(Arc::new(array) as ArrayRef)
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        JsonUnionField::scalar_value(value)
    }
}

fn jiter_json_get_union(opt_json: Option<&str>, path: &[JsonPath]) -> Result<JsonUnionField, GetError> {
    let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) else {
        return get_err!();
    };
    build_union(json, &mut jiter, peek).map(JsonUnionField::from)
}

fn append_json_get_union(opt_json: Option<&str>, path: &[JsonPath], builder: &mut JsonUnion) {
    if let (Some(json), Some((mut jiter, peek))) = (opt_json, jiter_json_find(opt_json, path)) {
        match build_union(json, &mut jiter, peek) {
            Ok(value) => builder.push_value(value),
            Err(GetError) => builder.push_none(),
        }
    } else {
        builder.push_none();
    }
}

fn build_union<'a, 'j: 'a>(
    json: &'j str,
    jiter: &'a mut Jiter<'j>,
    peek: Peek,
) -> Result<JsonUnionValue<'a>, GetError> {
    match peek {
        Peek::Null => {
            jiter.known_null()?;
            Ok(JsonUnionValue::JsonNull)
        }
        Peek::True | Peek::False => Ok(JsonUnionValue::Bool(jiter.known_bool(peek)?)),
        Peek::String => Ok(JsonUnionValue::Str(jiter.known_str()?)),
        Peek::Array => Ok(JsonUnionValue::Array(jiter_skip_str(json, jiter, peek)?)),
        Peek::Object => Ok(JsonUnionValue::Object(jiter_skip_str(json, jiter, peek)?)),
        _ => match jiter.known_number(peek)? {
            NumberAny::Int(NumberInt::Int(value)) => Ok(JsonUnionValue::Int(value)),
            // jiter returns `BigInt` for any integer its fast path couldn't decode, which includes
            // values that do fit in `i64`, hence the conversion attempt. Values genuinely outside
            // `i64` range have no representation in the union, so they're returned as null rather
            // than silently losing precision as a `Float`.
            NumberAny::Int(NumberInt::BigInt(value)) => match i64::try_from(value) {
                Ok(value) => Ok(JsonUnionValue::Int(value)),
                Err(_) => get_err!(),
            },
            NumberAny::Float(value) => Ok(JsonUnionValue::Float(value)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_util::assert_direct_matches_owned;

    #[test]
    fn direct_builder_matches_owned_results() {
        let rows = [
            Some(r#"{"a":"escaped\nvalue","b":null,"c":[1,{"x":true}],"d":{"k":1.5}}"#),
            Some(r#"{"a":42,"b":true,"c":[false,-7,18446744073709551615],"d":{}}"#),
            Some(r#"{"a":null,"a":"second","c":"text","d":[]}"#),
            Some(r#"{"a":1e400,"b":123456789012345678901234567890}"#),
            Some("invalid"),
            None,
        ];
        let paths = vec![
            vec![],
            vec![ScalarValue::Utf8(Some("a".to_owned()))],
            vec![ScalarValue::Utf8(Some("b".to_owned()))],
            vec![ScalarValue::Utf8(Some("d".to_owned()))],
            vec![ScalarValue::Utf8(Some("missing".to_owned()))],
            vec![ScalarValue::Utf8(Some("c".to_owned())), ScalarValue::Int64(Some(1))],
            vec![ScalarValue::Utf8(Some("c".to_owned())), ScalarValue::Int64(Some(2))],
            vec![ScalarValue::Utf8(None)],
        ];
        assert_direct_matches_owned::<JsonUnion>(&rows, &paths, append_json_get_union, jiter_json_get_union);
    }
}
