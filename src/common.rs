use datafusion::arrow::array::{Array, ArrayRef, Int64Array, LargeStringArray, StringArray, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use jiter::{Jiter, JiterError, Peek};
use std::str::Utf8Error;

use crate::common_union::{is_json_union, json_from_union_scalar, nested_json_array};

pub fn check_args(args: &[DataType], fn_name: &str) -> DataFusionResult<()> {
    let Some(first) = args.first() else {
        return plan_err!("The '{fn_name}' function requires one or more arguments.");
    };
    if !(matches!(first, DataType::Utf8 | DataType::LargeUtf8) || is_json_union(first)) {
        // if !matches!(first, DataType::Utf8 | DataType::LargeUtf8) {
        return plan_err!("Unexpected argument type to '{fn_name}' at position 1, expected a string, got {first:?}.");
    }
    args[1..].iter().enumerate().try_for_each(|(index, arg)| match arg {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::UInt64 | DataType::Int64 => Ok(()),
        t => plan_err!(
            "Unexpected argument type to '{fn_name}' at position {}, expected string or int, got {t:?}.",
            index + 2
        ),
    })
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
            let result_collect = match args.get(1) {
                Some(ColumnarValue::Array(a)) => {
                    if args.len() > 2 {
                        // TODO perhaps we could support this by zipping the arrays, but it's not trivial, #23
                        exec_err!("More than 1 path element is not supported when querying JSON using an array.")
                    } else if let Some(str_path_array) = a.as_any().downcast_ref::<StringArray>() {
                        let paths = str_path_array.iter().map(|opt_key| opt_key.map(JsonPath::Key));
                        zip_apply(json_array, paths, jiter_find, true)
                    } else if let Some(str_path_array) = a.as_any().downcast_ref::<LargeStringArray>() {
                        let paths = str_path_array.iter().map(|opt_key| opt_key.map(JsonPath::Key));
                        zip_apply(json_array, paths, jiter_find, true)
                    } else if let Some(int_path_array) = a.as_any().downcast_ref::<Int64Array>() {
                        let paths = int_path_array.iter().map(|opt_index| opt_index.map(Into::into));
                        zip_apply(json_array, paths, jiter_find, false)
                    } else if let Some(int_path_array) = a.as_any().downcast_ref::<UInt64Array>() {
                        let paths = int_path_array.iter().map(|opt_index| opt_index.map(Into::into));
                        zip_apply(json_array, paths, jiter_find, false)
                    } else {
                        exec_err!("unexpected second argument type, expected string or int array")
                    }
                }
                Some(ColumnarValue::Scalar(_)) => scalar_apply(json_array, &JsonPath::extract_path(args), jiter_find),
                None => scalar_apply(json_array, &[], jiter_find),
            };
            to_array(result_collect?).map(ColumnarValue::from)
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s)) => {
            let path = JsonPath::extract_path(args);
            let v = jiter_find(s.as_ref().map(String::as_str), &path).ok();
            Ok(ColumnarValue::Scalar(to_scalar(v)))
        }
        ColumnarValue::Scalar(ScalarValue::Union(type_id_value, union_fields, _)) => {
            let opt_json = json_from_union_scalar(type_id_value, union_fields);
            let v = jiter_find(opt_json, &JsonPath::extract_path(args)).ok();
            Ok(ColumnarValue::Scalar(to_scalar(v)))
        }
        ColumnarValue::Scalar(_) => {
            exec_err!("unexpected first argument type, expected string or JSON union")
        }
    }
}

fn zip_apply<'a, P: Iterator<Item = Option<JsonPath<'a>>>, C: FromIterator<Option<I>> + 'static, I>(
    json_array: &ArrayRef,
    path_array: P,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    object_lookup: bool,
) -> DataFusionResult<C> {
    if let Some(string_array) = json_array.as_any().downcast_ref::<StringArray>() {
        Ok(zip_apply_iter(string_array.iter(), path_array, jiter_find))
    } else if let Some(large_string_array) = json_array.as_any().downcast_ref::<LargeStringArray>() {
        Ok(zip_apply_iter(large_string_array.iter(), path_array, jiter_find))
    } else if let Some(string_array) = nested_json_array(json_array, object_lookup) {
        Ok(zip_apply_iter(string_array.iter(), path_array, jiter_find))
    } else {
        exec_err!("unexpected json array type {:?}", json_array.data_type())
    }
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

fn scalar_apply<C: FromIterator<Option<I>>, I>(
    json_array: &ArrayRef,
    path: &[JsonPath],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
) -> DataFusionResult<C> {
    if let Some(string_array) = json_array.as_any().downcast_ref::<StringArray>() {
        Ok(scalar_apply_iter(string_array.iter(), path, jiter_find))
    } else if let Some(large_string_array) = json_array.as_any().downcast_ref::<LargeStringArray>() {
        Ok(scalar_apply_iter(large_string_array.iter(), path, jiter_find))
    } else if let Some(string_array) = nested_json_array(json_array, is_object_lookup(path)) {
        Ok(scalar_apply_iter(string_array.iter(), path, jiter_find))
    } else {
        exec_err!("unexpected json array type {:?}", json_array.data_type())
    }
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
