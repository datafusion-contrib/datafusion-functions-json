use std::str::Utf8Error;

use arrow::array::{as_string_array, Array, ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use datafusion_common::{exec_err, plan_err, Result as DataFusionResult, ScalarValue};
use datafusion_expr::ColumnarValue;
use jiter::{Jiter, JiterError, Peek};

pub fn check_args(args: &[DataType], fn_name: &str) -> DataFusionResult<()> {
    if args.len() < 2 {
        return plan_err!("The `{fn_name}` function requires two or more arguments.");
    }
    if !matches!(&args[0], DataType::Utf8) {
        return plan_err!("Unexpected argument type to `{fn_name}` at position 1, expected a string.");
    }
    args[1..].iter().enumerate().try_for_each(|(index, arg)| match arg {
        DataType::Utf8 | DataType::UInt64 | DataType::Int64 => Ok(()),
        _ => plan_err!(
            "Unexpected argument type to `{fn_name}` at position {}, expected string or int.",
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
        JsonPath::Index(index as usize)
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
    pub fn extract_args(args: &'s [ColumnarValue]) -> Vec<Self> {
        args[1..]
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Self::Key(s),
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(i))) => (*i).into(),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => (*i).into(),
                _ => Self::None,
            })
            .collect()
    }
}

pub fn get_invoke<C: FromIterator<Option<I>> + 'static, I>(
    args: &[ColumnarValue],
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
    to_array: impl Fn(C) -> DataFusionResult<ArrayRef>,
    to_scalar: impl Fn(Option<I>) -> ScalarValue,
) -> DataFusionResult<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(json_array) => {
            let result_collect = match &args[1] {
                ColumnarValue::Array(a) => {
                    if let Some(str_path_array) = a.as_any().downcast_ref::<StringArray>() {
                        let paths = str_path_array.iter().map(|opt_key| opt_key.map(JsonPath::Key));
                        zip_apply(json_array, paths, jiter_find)
                    } else if let Some(int_path_array) = a.as_any().downcast_ref::<Int64Array>() {
                        let paths = int_path_array.iter().map(|opt_index| opt_index.map(Into::into));
                        zip_apply(json_array, paths, jiter_find)
                    } else {
                        return exec_err!("unexpected second argument type, expected string or int array");
                    }
                }
                ColumnarValue::Scalar(_) => {
                    let path = JsonPath::extract_args(args);
                    as_string_array(json_array)
                        .iter()
                        .map(|opt_json| jiter_find(opt_json, &path).ok())
                        .collect::<C>()
                }
            };
            to_array(result_collect).map(ColumnarValue::from)
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
            let path = JsonPath::extract_args(args);
            let v = jiter_find(s.as_ref().map(String::as_str), &path).ok();
            Ok(ColumnarValue::Scalar(to_scalar(v)))
        }
        ColumnarValue::Scalar(_) => {
            exec_err!("unexpected first argument type, expected string")
        }
    }
}

fn zip_apply<'a, P: Iterator<Item = Option<JsonPath<'a>>>, C: FromIterator<Option<I>> + 'static, I>(
    json_array: &ArrayRef,
    paths: P,
    jiter_find: impl Fn(Option<&str>, &[JsonPath]) -> Result<I, GetError>,
) -> C {
    as_string_array(json_array)
        .iter()
        .zip(paths)
        .map(|(opt_json, opt_path)| {
            if let Some(path) = opt_path {
                jiter_find(opt_json, &[path]).ok()
            } else {
                None
            }
        })
        .collect::<C>()
}

pub fn jiter_json_find<'j>(opt_json: Option<&'j str>, path: &[JsonPath]) -> Option<(Jiter<'j>, Peek)> {
    if let Some(json_str) = opt_json {
        let mut jiter = Jiter::new(json_str.as_bytes(), false);
        if let Ok(peek) = jiter.peek() {
            if let Ok(peek_found) = jiter_json_find_step(&mut jiter, peek, path) {
                return Some((jiter, peek_found));
            }
        }
    }
    None
}

fn jiter_json_find_step(jiter: &mut Jiter, peek: Peek, path: &[JsonPath]) -> Result<Peek, GetError> {
    let (first, rest) = path.split_first().unwrap();
    let next_peek = match peek {
        Peek::Array => match first {
            JsonPath::Index(index) => jiter_array_get(jiter, *index),
            _ => Err(GetError),
        },
        Peek::Object => match first {
            JsonPath::Key(key) => jiter_object_get(jiter, key),
            _ => Err(GetError),
        },
        _ => Err(GetError),
    }?;

    if rest.is_empty() {
        Ok(next_peek)
    } else {
        // we still have more of the path to traverse, recurse
        jiter_json_find_step(jiter, next_peek, rest)
    }
}

fn jiter_array_get(jiter: &mut Jiter, find_key: usize) -> Result<Peek, GetError> {
    let mut peek_opt = jiter.known_array()?;

    let mut index: usize = 0;
    while let Some(peek) = peek_opt {
        if index == find_key {
            return Ok(peek);
        }
        jiter.next_skip()?;
        index += 1;
        peek_opt = jiter.array_step()?;
    }
    Err(GetError)
}

fn jiter_object_get(jiter: &mut Jiter, find_key: &str) -> Result<Peek, GetError> {
    let mut opt_key = jiter.known_object()?;

    while let Some(key) = opt_key {
        if key == find_key {
            let value_peek = jiter.peek()?;
            return Ok(value_peek);
        }
        jiter.next_skip()?;
        opt_key = jiter.next_key()?;
    }
    Err(GetError)
}

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
