use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, Int64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use jiter::{NumberAny, NumberInt, Peek};

use crate::common::{get_err, invoke, jiter_json_find, return_type_check, GetError, InvokeResult, JsonPath};
use crate::common_macros::make_udf_function;

make_udf_function!(
    JsonGetInt,
    json_get_int,
    json_data path,
    r#"Get an integer value from a JSON string by its "path""#
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonGetInt {
    signature: Signature,
    aliases: [String; 1],
}

impl Default for JsonGetInt {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: ["json_get_int".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonGetInt {
    fn name(&self) -> &str {
        self.aliases[0].as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        return_type_check::<Int64Array>(arg_types, self.name(), DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        invoke::<Int64Array>(&args.args, jiter_json_get_int)
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

impl InvokeResult for Int64Array {
    type Item = i64;

    type Builder = Int64Builder;

    // Cheaper to return an int array rather than dict-encoded ints
    const ACCEPT_DICT_RETURN: bool = false;

    fn builder(capacity: usize) -> Self::Builder {
        Int64Builder::with_capacity(capacity)
    }

    fn append_value(builder: &mut Self::Builder, value: Option<Self::Item>) {
        builder.append_option(value);
    }

    fn finish(mut builder: Self::Builder) -> DataFusionResult<ArrayRef> {
        Ok(Arc::new(builder.finish()))
    }

    fn scalar(value: Option<Self::Item>) -> ScalarValue {
        ScalarValue::Int64(value)
    }
}

fn jiter_json_get_int(json_data: Option<&str>, path: &[JsonPath]) -> Result<i64, GetError> {
    if let Some((mut jiter, peek)) = jiter_json_find(json_data, path) {
        match peek {
            Peek::String => {
                let s = jiter.known_str()?;
                s.parse::<i64>().map_err(|_| GetError)
            }
            // Valid JSON numbers are represented by all other `Peek` variants (including `Minus`),
            // so we only need to explicitly reject non-numeric values (and non-standard `Infinity`/`NaN`).
            Peek::Null | Peek::True | Peek::False | Peek::Infinity | Peek::NaN | Peek::Array | Peek::Object => {
                get_err!()
            }
            _ => match jiter.known_number(peek)? {
                NumberAny::Int(NumberInt::Int(i)) => Ok(i),
                // jiter returns `BigInt` for any integer its fast path couldn't decode, which
                // includes values that do fit in `i64`, hence the conversion attempt
                NumberAny::Int(NumberInt::BigInt(b)) => i64::try_from(b).map_err(|_| GetError),
                NumberAny::Float(f) => float_to_int(f),
            },
        }
    } else {
        get_err!()
    }
}

/// A float with an integral value, such as `1.0` or `2e3`, as that integer. A fractional value is
/// not rounded, and a value outside the `i64` range is not saturated: both are an error.
fn float_to_int(f: f64) -> Result<i64, GetError> {
    // -2^63 and 2^63 are exact as f64, so the range check is exact; a NaN or infinite value has a
    // NaN fractional part and fails the first check
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    if f.fract() == 0.0 && f >= i64::MIN as f64 && f < -(i64::MIN as f64) {
        Ok(f as i64)
    } else {
        get_err!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_to_int_edges() {
        assert_eq!(float_to_int(1.0).ok(), Some(1));
        assert_eq!(float_to_int(-0.0).ok(), Some(0));
        assert_eq!(float_to_int(2e3).ok(), Some(2000));
        // the largest f64 below 2^63, and -2^63 itself, both fit
        assert_eq!(
            float_to_int(9_223_372_036_854_774_784.0).ok(),
            Some(9_223_372_036_854_774_784)
        );
        assert_eq!(float_to_int(-9_223_372_036_854_775_808.0).ok(), Some(i64::MIN));
        // 2^63 is one past i64::MAX, and the next f64 below -2^63 is out of range too
        assert!(float_to_int(9_223_372_036_854_775_808.0).is_err());
        assert!(float_to_int(-9_223_372_036_854_777_856.0).is_err());
        assert!(float_to_int(1.5).is_err());
        assert!(float_to_int(-0.5).is_err());
        assert!(float_to_int(1e300).is_err());
        assert!(float_to_int(f64::NAN).is_err());
        assert!(float_to_int(f64::INFINITY).is_err());
        assert!(float_to_int(f64::NEG_INFINITY).is_err());
    }
}
