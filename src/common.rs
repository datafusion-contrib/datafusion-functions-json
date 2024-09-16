use std::str::Utf8Error;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, DictionaryArray, Int64Array, LargeStringArray,
    PrimitiveArray, StringArray, StringViewArray, UInt64Array, UnionArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion::arrow::datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType};
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
pub fn scalar_udf_return_type(args: &[DataType], fn_name: &str, value_type: DataType) -> DataFusionResult<DataType> {
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

impl<'s> JsonPath<'s> {
    pub fn extract_path(args: &'s [ColumnarValue]) -> Vec<Self> {
        args[1..]
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => Self::Key(s),
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(i))) => (*i).into(),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => (*i).into(),
                _ => Self::None,
            })
            .collect()
    }
}

pub fn invoke<C: FromIterator<Option<I>> + 'static, I>(
    args: &[ColumnarValue],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    to_scalar: impl Fn(Option<I>) -> ScalarValue,
) -> DataFusionResult<ColumnarValue> {
    let Some(first_arg) = args.first() else {
        // I think this can't happen, but I assumed the same about args[1] and I was wrong, so better to be safe
        return exec_err!("expected at least one argument");
    };
    match first_arg {
        ColumnarValue::Array(json_array) => {
            let array = match args.get(1) {
                Some(ColumnarValue::Array(a)) => {
                    if args.len() > 2 {
                        // TODO perhaps we could support this by zipping the arrays, but it's not trivial, #23
                        exec_err!("More than 1 path element is not supported when querying JSON using an array.")
                    } else {
                        invoke_array(json_array, a, to_array, jiter_find)
                    }
                }
                Some(ColumnarValue::Scalar(_)) => {
                    scalar_apply(json_array, &JsonPath::extract_path(args), to_array, jiter_find)
                }
                None => scalar_apply(json_array, &[], to_array, jiter_find),
            };
            array.map(ColumnarValue::from)
        }
        ColumnarValue::Scalar(s) => invoke_scalar(s, args, jiter_find, to_scalar),
    }
}

fn invoke_array<C: FromIterator<Option<I>> + 'static, I>(
    json_array: &ArrayRef,
    needle_array: &ArrayRef,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
) -> DataFusionResult<ArrayRef> {
    if let Some(d) = needle_array.as_any_dictionary_opt() {
        invoke_array(json_array, d.values(), to_array, jiter_find)
    } else if let Some(str_path_array) = needle_array.as_any().downcast_ref::<StringArray>() {
        let paths = str_path_array.iter().map(|opt_key| opt_key.map(JsonPath::Key));
        zip_apply(json_array, paths, to_array, jiter_find, true)
    } else if let Some(str_path_array) = needle_array.as_any().downcast_ref::<LargeStringArray>() {
        let paths = str_path_array.iter().map(|opt_key| opt_key.map(JsonPath::Key));
        zip_apply(json_array, paths, to_array, jiter_find, true)
    } else if let Some(str_path_array) = needle_array.as_any().downcast_ref::<StringViewArray>() {
        let paths = str_path_array.iter().map(|opt_key| opt_key.map(JsonPath::Key));
        zip_apply(json_array, paths, to_array, jiter_find, true)
    } else if let Some(int_path_array) = needle_array.as_any().downcast_ref::<Int64Array>() {
        let paths = int_path_array.iter().map(|opt_index| opt_index.map(Into::into));
        zip_apply(json_array, paths, to_array, jiter_find, false)
    } else if let Some(int_path_array) = needle_array.as_any().downcast_ref::<UInt64Array>() {
        let paths = int_path_array.iter().map(|opt_index| opt_index.map(Into::into));
        zip_apply(json_array, paths, to_array, jiter_find, false)
    } else {
        exec_err!("unexpected second argument type, expected string or int array")
    }
}

fn zip_apply<'a, P: Iterator<Item = Option<JsonPath<'a>>>, C: FromIterator<Option<I>> + 'static, I>(
    json_array: &ArrayRef,
    path_array: P,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    object_lookup: bool,
) -> DataFusionResult<ArrayRef> {
    // arrow_schema "use" is workaround for https://github.com/apache/arrow-rs/issues/6400#issue-2528388332
    use datafusion::arrow::datatypes as arrow_schema;

    let c = downcast_dictionary_array!(
        json_array => {
            let values = zip_apply(json_array.values(), path_array, to_array, jiter_find, object_lookup)?;
            if !is_json_union(values.data_type()) {
                return Ok(Arc::new(json_array.with_values(values)));
            }
            // JSON union: post-process the array to set keys to null where the union member is null
            let type_ids = values.as_any().downcast_ref::<UnionArray>().unwrap().type_ids();
            return Ok(Arc::new(DictionaryArray::new(
                mask_dictionary_keys(json_array.keys(), type_ids),
                values,
            )));
        }
        DataType::Utf8 => zip_apply_iter(json_array.as_string::<i32>().iter(), path_array, jiter_find),
        DataType::LargeUtf8 => zip_apply_iter(json_array.as_string::<i64>().iter(), path_array, jiter_find),
        DataType::Utf8View => zip_apply_iter(json_array.as_string_view().iter(), path_array, jiter_find),
        other => if let Some(string_array) = nested_json_array(json_array, object_lookup) {
            zip_apply_iter(string_array.iter(), path_array, jiter_find)
        } else {
            return exec_err!("unexpected json array type {:?}", other);
        }
    );

    to_array(c)
}

fn zip_apply_iter<'a, 'j, P: Iterator<Item = Option<JsonPath<'a>>>, C: FromIterator<Option<I>> + 'static, I>(
    json_iter: impl Iterator<Item = Option<&'j str>>,
    path_array: P,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
) -> C {
    json_iter
        .zip(path_array)
        .map(|(opt_json, opt_path)| {
            if let Some(path) = opt_path {
                jiter_find(opt_json, &[path]).ok()
            } else {
                None
            }
        })
        .collect::<C>()
}

fn invoke_scalar<I>(
    scalar: &ScalarValue,
    args: &[ColumnarValue],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    to_scalar: impl Fn(Option<I>) -> ScalarValue,
) -> DataFusionResult<ColumnarValue> {
    match scalar {
        ScalarValue::Dictionary(_, b) => invoke_scalar(b.as_ref(), args, jiter_find, to_scalar),
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => {
            let path = JsonPath::extract_path(args);
            let v = jiter_find(s.as_ref().map(String::as_str), &path).ok();
            Ok(ColumnarValue::Scalar(to_scalar(v)))
        }
        ScalarValue::Union(type_id_value, union_fields, _) => {
            let opt_json = json_from_union_scalar(type_id_value, union_fields);
            let v = jiter_find(opt_json, &JsonPath::extract_path(args)).ok();
            Ok(ColumnarValue::Scalar(to_scalar(v)))
        }
        _ => {
            exec_err!("unexpected first argument type, expected string or JSON union")
        }
    }
}

fn scalar_apply<C: FromIterator<Option<I>>, I>(
    json_array: &ArrayRef,
    path: &[JsonPath],
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
) -> DataFusionResult<ArrayRef> {
    // arrow_schema "use" is workaround for https://github.com/apache/arrow-rs/issues/6400#issue-2528388332
    use datafusion::arrow::datatypes as arrow_schema;

    let c = downcast_dictionary_array!(
        json_array => {
            let values = scalar_apply(json_array.values(), path, to_array, jiter_find)?;
            if !is_json_union(values.data_type()) {
                return Ok(Arc::new(json_array.with_values(values)));
            }
            // JSON union: post-process the array to set keys to null where the union member is null
            let type_ids = values.as_any().downcast_ref::<UnionArray>().unwrap().type_ids();
            return Ok(Arc::new(DictionaryArray::new(
                mask_dictionary_keys(json_array.keys(), type_ids),
                values,
            )));
        }
        DataType::Utf8 => scalar_apply_iter(json_array.as_string::<i32>().iter(), path, jiter_find),
        DataType::LargeUtf8 => scalar_apply_iter(json_array.as_string::<i64>().iter(), path, jiter_find),
        DataType::Utf8View => scalar_apply_iter(json_array.as_string_view().iter(), path, jiter_find),
        other => if let Some(string_array) = nested_json_array(json_array, is_object_lookup(path)) {
            scalar_apply_iter(string_array.iter(), path, jiter_find)
        } else {
            return exec_err!("unexpected json array type {:?}", other);
        }
    );

    to_array(c)
}

fn is_object_lookup(path: &[JsonPath]) -> bool {
    if let Some(first) = path.first() {
        matches!(first, JsonPath::Key(_))
    } else {
        false
    }
}

fn scalar_apply_iter<'j, C: FromIterator<Option<I>>, I>(
    json_iter: impl Iterator<Item = Option<&'j str>>,
    path: &[JsonPath],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
) -> C {
    json_iter.map(|opt_json| jiter_find(opt_json, path).ok()).collect::<C>()
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
/// This is a workaround to https://github.com/apache/arrow-rs/issues/6017#issuecomment-2352756753
/// - i.e. that dictionary null is most reliably done if the keys are null.
fn mask_dictionary_keys<K: ArrowPrimitiveType>(keys: &PrimitiveArray<K>, type_ids: &[i8]) -> PrimitiveArray<K> {
    PrimitiveArray::new(
        keys.values().clone(),
        Some(
            keys.iter()
                .map(|k| match k {
                    Some(k) => type_ids[k.as_usize()] != TYPE_ID_NULL,
                    None => false,
                })
                .collect(),
        ),
    )
}
