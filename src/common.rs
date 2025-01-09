use std::str::Utf8Error;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayAccessor, ArrayRef, AsArray, DictionaryArray, Int64Array, LargeStringArray, PrimitiveArray,
    StringArray, StringViewArray, UInt64Array, UnionArray,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, ArrowPrimitiveType, DataType, Int64Type, UInt64Type,
};
use datafusion::arrow::downcast_dictionary_array;
use datafusion::common::{exec_err, plan_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use jiter::{Jiter, JiterError, Peek};

use crate::common_union::{is_json_union, json_from_union_scalar, nested_json_array, TYPE_ID_NULL};

/// General implementation of `ScalarUDFImpl::return_type`.
///
/// # Arguments
///
/// * `args` - The arguments to the function
/// * `fn_name` - The name of the function
/// * `value_type` - The general return type of the function, might be wrapped in a dictionary depending
///   on the first argument
pub fn return_type_check(args: &[DataType], fn_name: &str, value_type: DataType) -> DataFusionResult<DataType> {
    let Some(first) = args.first() else {
        return plan_err!("The '{fn_name}' function requires one or more arguments.");
    };
    let first_dict_key_type = dict_key_type(first);
    if !(is_str(first) || is_json_union(first) || first_dict_key_type.is_some()) {
        // if !matches!(first, DataType::Utf8 | DataType::LargeUtf8) {
        return plan_err!("Unexpected argument type to '{fn_name}' at position 1, expected a string, got {first:?}.");
    }
    args.iter().skip(1).enumerate().try_for_each(|(index, arg)| {
        if is_str(arg) || is_int(arg) || dict_key_type(arg).is_some() {
            Ok(())
        } else {
            plan_err!(
                "Unexpected argument type to '{fn_name}' at position {}, expected string or int, got {arg:?}.",
                index + 2
            )
        }
    })?;
    match first_dict_key_type {
        Some(t) => Ok(DataType::Dictionary(Box::new(t), Box::new(value_type))),
        None => Ok(value_type),
    }
}

fn is_str(d: &DataType) -> bool {
    matches!(d, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn is_int(d: &DataType) -> bool {
    // TODO we should support more types of int, but that's a longer task
    matches!(d, DataType::UInt64 | DataType::Int64)
}

fn dict_key_type(d: &DataType) -> Option<DataType> {
    if let DataType::Dictionary(key, value) = d {
        if is_str(value) || is_json_union(value) {
            return Some(*key.clone());
        }
    }
    None
}

#[derive(Debug)]
pub enum JsonPath<'s> {
    Key(&'s str),
    Index(usize),
    None,
}

impl<'a> From<&'a str> for JsonPath<'a> {
    fn from(key: &'a str) -> Self {
        JsonPath::Key(key)
    }
}

impl From<u64> for JsonPath<'_> {
    fn from(index: u64) -> Self {
        JsonPath::Index(usize::try_from(index).unwrap())
    }
}

impl From<i64> for JsonPath<'_> {
    fn from(index: i64) -> Self {
        match usize::try_from(index) {
            Ok(i) => Self::Index(i),
            Err(_) => Self::None,
        }
    }
}

enum JsonPathArgs<'a> {
    Array(&'a ArrayRef),
    Scalars(Vec<JsonPath<'a>>),
}

impl<'s> JsonPathArgs<'s> {
    fn extract_path(path_args: &'s [ColumnarValue]) -> DataFusionResult<Self> {
        // If there is a single argument as an array, we know how to handle it
        if let Some((ColumnarValue::Array(array), &[])) = path_args.split_first() {
            return Ok(Self::Array(array));
        }

        path_args
            .iter()
            .enumerate()
            .map(|(pos, arg)| match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
                    Ok(JsonPath::Key(s))
                }
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(i))) => Ok((*i).into()),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => Ok((*i).into()),
                ColumnarValue::Scalar(
                    ScalarValue::Null
                    | ScalarValue::Utf8(None)
                    | ScalarValue::LargeUtf8(None)
                    | ScalarValue::UInt64(None)
                    | ScalarValue::Int64(None),
                ) => Ok(JsonPath::None),
                ColumnarValue::Array(_) => {
                    // if there was a single arg, which is an array, handled above in the
                    // split_first case. So this is multiple args of which one is an array
                    exec_err!("More than 1 path element is not supported when querying JSON using an array.")
                }
                ColumnarValue::Scalar(arg) => exec_err!(
                    "Unexpected argument type at position {}, expected string or int, got {arg:?}.",
                    pos + 1
                ),
            })
            .collect::<DataFusionResult<_>>()
            .map(JsonPathArgs::Scalars)
    }
}

pub fn invoke<C: FromIterator<Option<I>> + 'static, I>(
    args: &[ColumnarValue],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    to_scalar: impl Fn(Option<I>) -> ScalarValue,
    return_dict: bool,
) -> DataFusionResult<ColumnarValue> {
    let Some((json_arg, path_args)) = args.split_first() else {
        return exec_err!("expected at least one argument");
    };

    let path = JsonPathArgs::extract_path(path_args)?;
    match (json_arg, path) {
        (ColumnarValue::Array(json_array), JsonPathArgs::Array(path_array)) => {
            invoke_array_array(json_array, path_array, to_array, jiter_find, return_dict).map(ColumnarValue::Array)
        }
        (ColumnarValue::Array(json_array), JsonPathArgs::Scalars(path)) => {
            invoke_array_scalars(json_array, &path, to_array, jiter_find, return_dict).map(ColumnarValue::Array)
        }
        (ColumnarValue::Scalar(s), JsonPathArgs::Array(path_array)) => {
            invoke_scalar_array(s, path_array, jiter_find, to_array)
        }
        (ColumnarValue::Scalar(s), JsonPathArgs::Scalars(path)) => {
            invoke_scalar_scalars(s, &path, jiter_find, to_scalar)
        }
    }
}

fn invoke_array_array<C: FromIterator<Option<I>> + 'static, I>(
    json_array: &ArrayRef,
    path_array: &ArrayRef,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    return_dict: bool,
) -> DataFusionResult<ArrayRef> {
    downcast_dictionary_array!(
        json_array => {
            let values = invoke_array_array(json_array.values(), path_array, to_array, jiter_find, return_dict)?;
            post_process_dict(json_array, values, return_dict)
        }
        DataType::Utf8 => zip_apply(json_array.as_string::<i32>().iter(), path_array, to_array, jiter_find),
        DataType::LargeUtf8 => zip_apply(json_array.as_string::<i64>().iter(), path_array, to_array, jiter_find),
        DataType::Utf8View => zip_apply(json_array.as_string_view().iter(), path_array, to_array, jiter_find),
        other => if let Some(string_array) = nested_json_array(json_array, is_object_lookup_array(path_array.data_type())) {
            zip_apply(string_array.iter(), path_array, to_array, jiter_find)
        } else {
            exec_err!("unexpected json array type {:?}", other)
        }
    )
}

fn invoke_array_scalars<C: FromIterator<Option<I>>, I>(
    json_array: &ArrayRef,
    path: &[JsonPath],
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    return_dict: bool,
) -> DataFusionResult<ArrayRef> {
    fn inner<'j, C: FromIterator<Option<I>>, I>(
        json_iter: impl IntoIterator<Item = Option<&'j str>>,
        path: &[JsonPath],
        jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    ) -> C {
        json_iter
            .into_iter()
            .map(|opt_json| jiter_find(opt_json, path).ok())
            .collect::<C>()
    }

    let c = downcast_dictionary_array!(
        json_array => {
            let values = invoke_array_scalars(json_array.values(), path, to_array, jiter_find, false)?;
            return post_process_dict(json_array, values, return_dict);
        }
        DataType::Utf8 => inner(json_array.as_string::<i32>(), path, jiter_find),
        DataType::LargeUtf8 => inner(json_array.as_string::<i64>(), path, jiter_find),
        DataType::Utf8View => inner(json_array.as_string_view(), path, jiter_find),
        other => if let Some(string_array) = nested_json_array(json_array, is_object_lookup(path)) {
            inner(string_array, path, jiter_find)
        } else {
            return exec_err!("unexpected json array type {:?}", other);
        }
    );
    to_array(c)
}

fn invoke_scalar_array<C: FromIterator<Option<I>> + 'static, I>(
    scalar: &ScalarValue,
    path_array: &ArrayRef,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
) -> DataFusionResult<ColumnarValue> {
    let s = extract_json_scalar(scalar)?;
    // TODO: possible optimization here if path_array is a dictionary; can apply against the
    // dictionary values directly for less work
    zip_apply(
        std::iter::repeat(s).take(path_array.len()),
        path_array,
        to_array,
        jiter_find,
    )
    .map(ColumnarValue::Array)
}

fn invoke_scalar_scalars<I>(
    scalar: &ScalarValue,
    path: &[JsonPath],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    to_scalar: impl Fn(Option<I>) -> ScalarValue,
) -> DataFusionResult<ColumnarValue> {
    let s = extract_json_scalar(scalar)?;
    let v = jiter_find(s, path).ok();
    Ok(ColumnarValue::Scalar(to_scalar(v)))
}

fn zip_apply<'a, C: FromIterator<Option<I>> + 'static, I>(
    json_array: impl IntoIterator<Item = Option<&'a str>>,
    path_array: &ArrayRef,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
) -> DataFusionResult<ArrayRef> {
    #[allow(clippy::needless_pass_by_value)] // ArrayAccessor is implemented on references
    fn inner<'a, 'j, P: Into<JsonPath<'a>>, C: FromIterator<Option<I>> + 'static, I>(
        json_iter: impl IntoIterator<Item = Option<&'j str>>,
        path_array: impl ArrayAccessor<Item = P>,
        jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    ) -> C {
        json_iter
            .into_iter()
            .enumerate()
            .map(|(i, opt_json)| {
                if path_array.is_null(i) {
                    None
                } else {
                    let path = path_array.value(i).into();
                    jiter_find(opt_json, &[path]).ok()
                }
            })
            .collect::<C>()
    }

    let c = downcast_dictionary_array!(
        path_array => match path_array.values().data_type() {
            DataType::Utf8 => inner(json_array, path_array.downcast_dict::<StringArray>().unwrap(), jiter_find),
            DataType::LargeUtf8 => inner(json_array, path_array.downcast_dict::<LargeStringArray>().unwrap(), jiter_find),
            DataType::Utf8View => inner(json_array, path_array.downcast_dict::<StringViewArray>().unwrap(), jiter_find),
            DataType::Int64 => inner(json_array, path_array.downcast_dict::<Int64Array>().unwrap(), jiter_find),
            DataType::UInt64 => inner(json_array, path_array.downcast_dict::<UInt64Array>().unwrap(), jiter_find),
            other => return exec_err!("unexpected second argument type, expected string or int array, got {:?}", other),
        },
        DataType::Utf8 => inner(json_array, path_array.as_string::<i32>(), jiter_find),
        DataType::LargeUtf8 => inner(json_array, path_array.as_string::<i64>(), jiter_find),
        DataType::Utf8View => inner(json_array, path_array.as_string_view(), jiter_find),
        DataType::Int64 => inner(json_array, path_array.as_primitive::<Int64Type>(), jiter_find),
        DataType::UInt64 => inner(json_array, path_array.as_primitive::<UInt64Type>(), jiter_find),
        other => return exec_err!("unexpected second argument type, expected string or int array, got {:?}", other)
    );

    to_array(c)
}

fn extract_json_scalar(scalar: &ScalarValue) -> DataFusionResult<Option<&str>> {
    match scalar {
        ScalarValue::Dictionary(_, b) => extract_json_scalar(b.as_ref()),
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => Ok(s.as_deref()),
        ScalarValue::Union(type_id_value, union_fields, _) => {
            Ok(json_from_union_scalar(type_id_value.as_ref(), union_fields))
        }
        _ => {
            exec_err!("unexpected first argument type, expected string or JSON union")
        }
    }
}

/// Take a dictionary array of JSON data and an array of result values and combine them.
fn post_process_dict<T: ArrowDictionaryKeyType>(
    dict_array: &DictionaryArray<T>,
    result_values: ArrayRef,
    return_dict: bool,
) -> DataFusionResult<ArrayRef> {
    if return_dict {
        if is_json_union(result_values.data_type()) {
            // JSON union: post-process the array to set keys to null where the union member is null
            let type_ids = result_values.as_any().downcast_ref::<UnionArray>().unwrap().type_ids();
            Ok(Arc::new(DictionaryArray::new(
                mask_dictionary_keys(dict_array.keys(), type_ids),
                result_values,
            )))
        } else {
            Ok(Arc::new(dict_array.with_values(result_values)))
        }
    } else {
        // this is what cast would do under the hood to unpack a dictionary into an array of its values
        Ok(take(&result_values, dict_array.keys(), None)?)
    }
}

fn is_object_lookup(path: &[JsonPath]) -> bool {
    if let Some(first) = path.first() {
        matches!(first, JsonPath::Key(_))
    } else {
        false
    }
}

fn is_object_lookup_array(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, value_type) => is_object_lookup_array(value_type),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        _ => false,
    }
}

pub fn jiter_json_find<'j>(opt_json: Option<&'j str>, path: &[JsonPath]) -> Option<(Jiter<'j>, Peek)> {
    let json_str = opt_json?;
    let mut jiter = Jiter::new(json_str.as_bytes());
    let mut peek = jiter.peek().ok()?;
    for element in path {
        match element {
            JsonPath::Key(key) if peek == Peek::Object => {
                let mut next_key = jiter.known_object().ok()??;

                while next_key != *key {
                    jiter.next_skip().ok()?;
                    next_key = jiter.next_key().ok()??;
                }

                peek = jiter.peek().ok()?;
            }
            JsonPath::Index(index) if peek == Peek::Array => {
                let mut array_item = jiter.known_array().ok()??;

                for _ in 0..*index {
                    jiter.known_skip(array_item).ok()?;
                    array_item = jiter.array_step().ok()??;
                }

                peek = array_item;
            }
            _ => {
                return None;
            }
        }
    }
    Some((jiter, peek))
}

macro_rules! get_err {
    () => {
        Err(GetError)
    };
}
pub(crate) use get_err;

pub struct GetError;

impl From<JiterError> for GetError {
    fn from(_: JiterError) -> Self {
        GetError
    }
}

impl From<Utf8Error> for GetError {
    fn from(_: Utf8Error) -> Self {
        GetError
    }
}

/// Set keys to null where the union member is null.
///
/// This is a workaround to <https://github.com/apache/arrow-rs/issues/6017#issuecomment-2352756753>
/// - i.e. that dictionary null is most reliably done if the keys are null.
///
/// That said, doing this might also be an optimization for cases like null-checking without needing
/// to check the value union array.
fn mask_dictionary_keys<K: ArrowPrimitiveType>(keys: &PrimitiveArray<K>, type_ids: &[i8]) -> PrimitiveArray<K> {
    let mut null_mask = vec![true; keys.len()];
    for (i, k) in keys.iter().enumerate() {
        match k {
            // if the key is non-null and value is non-null, don't mask it out
            Some(k) if type_ids[k.as_usize()] != TYPE_ID_NULL => {}
            // i.e. key is null or value is null here
            _ => null_mask[i] = false,
        }
    }
    PrimitiveArray::new(keys.values().clone(), Some(null_mask.into()))
}
