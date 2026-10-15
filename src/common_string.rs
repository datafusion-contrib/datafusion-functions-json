use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, GenericStringArray, GenericStringBuilder, LargeStringArray, OffsetSizeTrait, StringArray,
    StringViewArray, StringViewBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;

use crate::common::{invoke, invoke_array_scalars_direct, return_type_check, GetError, InvokeResult, JsonPath};

/// A JSON function that returns a string, such as `json_as_text`.
///
/// The result has the string type of the JSON argument (see [`string_result_type`]). Each value is
/// a slice of the JSON text or a decoded JSON string, and a decoded string is never longer than its
/// escaped form. Thus a result never needs more offset space than its argument. For example, a
/// `Utf8View` batch of more than 2 GiB does not overflow the `i32` offsets of a `Utf8` result.
pub(crate) trait StringGetter {
    /// Write the value at `path` in `json` into `builder`, or a null if there is none.
    fn append<B: StringResultBuilder>(json: Option<&str>, path: &[JsonPath], builder: &mut B);

    /// The value at `path` in `json` as an owned string, for the argument shapes that `invoke` serves.
    fn get(json: Option<&str>, path: &[JsonPath]) -> Result<String, GetError>;
}

/// A builder of one of the Arrow string types.
pub(crate) trait StringResultBuilder {
    fn append_str(&mut self, value: Option<&str>);
}

impl<O: OffsetSizeTrait> StringResultBuilder for GenericStringBuilder<O> {
    fn append_str(&mut self, value: Option<&str>) {
        self.append_option(value);
    }
}

impl StringResultBuilder for StringViewBuilder {
    fn append_str(&mut self, value: Option<&str>) {
        self.append_option(value);
    }
}

impl<O: OffsetSizeTrait> InvokeResult for GenericStringArray<O> {
    type Item = String;

    type Builder = GenericStringBuilder<O>;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        GenericStringBuilder::with_capacity(capacity, 0)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        if O::IS_LARGE {
            ScalarValue::LargeUtf8(value)
        } else {
            ScalarValue::Utf8(value)
        }
    }
}

impl InvokeResult for StringViewArray {
    type Item = String;

    type Builder = StringViewBuilder;

    const ACCEPT_DICT_RETURN: bool = true;

    fn builder(capacity: usize) -> Self::Builder {
        StringViewBuilder::with_capacity(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Utf8View(value)
    }
}

/// The string type of the result for a JSON argument of type `json_type`.
///
/// A string argument gives its own type, and a dictionary argument gives the type of its values.
/// A JSON union holds its strings as `Utf8`, so a union argument gives `Utf8`.
fn string_result_type(json_type: &DataType) -> DataType {
    match json_type {
        DataType::LargeUtf8 => DataType::LargeUtf8,
        DataType::Utf8View => DataType::Utf8View,
        DataType::Dictionary(_, value_type) => string_result_type(value_type),
        _ => DataType::Utf8,
    }
}

/// `ScalarUDFImpl::return_type` for a [`StringGetter`].
pub(crate) fn string_return_type(arg_types: &[DataType], fn_name: &str) -> DataFusionResult<DataType> {
    let value_type = arg_types.first().map_or(DataType::Utf8, string_result_type);
    return_type_check::<StringArray>(arg_types, fn_name, value_type)
}

/// `ScalarUDFImpl::invoke_with_args` for a [`StringGetter`].
pub(crate) fn invoke_string<G: StringGetter>(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    match args.first().map(|json| string_result_type(&json.data_type())) {
        Some(DataType::LargeUtf8) => invoke_string_as::<G, LargeStringArray>(args),
        Some(DataType::Utf8View) => invoke_string_as::<G, StringViewArray>(args),
        _ => invoke_string_as::<G, StringArray>(args),
    }
}

fn invoke_string_as<G: StringGetter, R>(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue>
where
    R: InvokeResult<Item = String>,
    R::Builder: StringResultBuilder,
{
    if let Some(result) = invoke_array_scalars_direct::<R>(args, G::append::<R::Builder>)? {
        return Ok(result);
    }
    invoke::<R>(args, G::get)
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use crate::common::test_util::assert_direct_matches_owned;

    /// Check the direct path and `invoke` build equal arrays for each string result type.
    pub(crate) fn assert_string_direct_matches_owned<G: StringGetter>(
        rows: &[Option<&str>],
        paths: &[Vec<ScalarValue>],
    ) {
        assert_direct_matches_owned::<StringArray>(rows, paths, G::append, G::get);
        assert_direct_matches_owned::<LargeStringArray>(rows, paths, G::append, G::get);
        assert_direct_matches_owned::<StringViewArray>(rows, paths, G::append, G::get);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_type_follows_json_type() {
        let dict = |value: DataType| DataType::Dictionary(Box::new(DataType::Int32), Box::new(value));
        let large_dict = |value: DataType| DataType::Dictionary(Box::new(DataType::Int64), Box::new(value));
        let union = crate::JSON_UNION_DATA_TYPE.clone();
        let cases = [
            (DataType::Utf8, DataType::Utf8),
            (DataType::LargeUtf8, DataType::LargeUtf8),
            (DataType::Utf8View, DataType::Utf8View),
            (union.clone(), DataType::Utf8),
            (dict(DataType::Utf8), large_dict(DataType::Utf8)),
            (dict(DataType::LargeUtf8), large_dict(DataType::LargeUtf8)),
            (dict(DataType::Utf8View), large_dict(DataType::Utf8View)),
            (dict(union), large_dict(DataType::Utf8)),
        ];
        for (json_type, want) in cases {
            let got = string_return_type(&[json_type.clone(), DataType::Utf8], "f").unwrap();
            assert_eq!(got, want, "{json_type}");
        }
    }
}
