use std::str::Utf8Error;

use datafusion_common::{exec_err, Result as DatafusionResult, ScalarValue};
use datafusion_expr::ColumnarValue;
use jiter::{Jiter, JiterError, Peek};

#[derive(Debug)]
pub enum JsonPath<'s> {
    Key(&'s str),
    Index(usize),
}

impl<'s> JsonPath<'s> {
    pub fn extract_args(args: &'s [ColumnarValue], fn_name: &str) -> DatafusionResult<Vec<Self>> {
        args[1..]
            .iter()
            .enumerate()
            .map(|(index, arg)| match arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(Self::Key(s)),
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(i))) => Ok(Self::Index(*i as usize)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => Ok(Self::Index(*i as usize)),
                _ => exec_err!(
                    "`{fn_name}`: unexpected argument type at {}, expected string or int",
                    index + 2
                ),
            })
            .collect()
    }
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
            JsonPath::Key(_) => Err(GetError),
        },
        Peek::Object => match first {
            JsonPath::Key(key) => jiter_object_get(jiter, key),
            JsonPath::Index(_) => Err(GetError),
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
