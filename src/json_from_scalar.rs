use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, UnionArray};
use datafusion::arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion::common::{exec_datafusion_err, exec_err, plan_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::common_macros::make_udf_function;
use crate::common_union::{JsonUnion, JsonUnionField};

make_udf_function!(
    JsonFromScalar,
    json_from_scalar,
    value,
    r"Convert a scalar value (null, bool, integer, float, or string) to a JSON union type"
);

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct JsonFromScalar {
    signature: Signature,
    aliases: [String; 2],
}

impl Default for JsonFromScalar {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: ["json_from_scalar".to_string(), "scalar_to_json".to_string()],
        }
    }
}

impl ScalarUDFImpl for JsonFromScalar {
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
        if arg_types.len() != 1 {
            return plan_err!(
                "The '{}' function requires exactly one argument, got {}",
                self.name(),
                arg_types.len()
            );
        }
        // Check that the input type is a scalar type that we can convert to JSON
        match arg_types[0] {
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View => {}
            _ => {
                return plan_err!("Unsupported type for json_from_scalar: {:?}", arg_types[0]);
            }
        }
        Ok(JsonUnion::data_type())
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!(
                "The '{}' function requires exactly one argument, got {}",
                self.name(),
                args.args.len()
            );
        }

        match args.args.pop().expect("Expected exactly one argument") {
            ColumnarValue::Scalar(scalar) => {
                let field = scalar_to_json_union_field(scalar)?;
                Ok(ColumnarValue::Scalar(JsonUnionField::scalar_value(Some(field))))
            }
            ColumnarValue::Array(array) => {
                let union = array_to_json_union(&array)?;
                let union_array: UnionArray = union.try_into()?;
                Ok(ColumnarValue::Array(Arc::new(union_array) as ArrayRef))
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn scalar_to_json_union_field(scalar: ScalarValue) -> DataFusionResult<JsonUnionField> {
    match scalar {
        // Null type / values
        ScalarValue::Null
        | ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None) => Ok(JsonUnionField::JsonNull),
        // Boolean type
        ScalarValue::Boolean(Some(b)) => Ok(JsonUnionField::Bool(b)),
        // Integer types - coerce to i64
        ScalarValue::Int8(Some(v)) => Ok(JsonUnionField::Int(i64::from(v))),
        ScalarValue::Int16(Some(v)) => Ok(JsonUnionField::Int(i64::from(v))),
        ScalarValue::Int32(Some(v)) => Ok(JsonUnionField::Int(i64::from(v))),
        ScalarValue::Int64(Some(v)) => Ok(JsonUnionField::Int(v)),
        ScalarValue::UInt8(Some(v)) => Ok(JsonUnionField::Int(i64::from(v))),
        ScalarValue::UInt16(Some(v)) => Ok(JsonUnionField::Int(i64::from(v))),
        ScalarValue::UInt32(Some(v)) => Ok(JsonUnionField::Int(i64::from(v))),
        ScalarValue::UInt64(Some(v)) => {
            Ok(JsonUnionField::Int(i64::try_from(v).map_err(|_| {
                exec_datafusion_err!("UInt64 value {} is out of range for i64", v)
            })?))
        }
        // Float types - coerce to f64
        ScalarValue::Float32(Some(v)) => Ok(JsonUnionField::Float(f64::from(v))),
        ScalarValue::Float64(Some(v)) => Ok(JsonUnionField::Float(v)),
        // String types
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            Ok(JsonUnionField::Str(s))
        }
        _ => exec_err!("Unsupported type for json_from_scalar: {:?}", scalar.data_type()),
    }
}

#[expect(clippy::too_many_lines)]
fn array_to_json_union(array: &ArrayRef) -> DataFusionResult<JsonUnion> {
    let mut union = JsonUnion::new(array.len());

    match array.data_type() {
        DataType::Null => {
            for _ in 0..array.len() {
                union.push(JsonUnionField::JsonNull);
            }
        }

        DataType::Boolean => {
            let arr = array.as_boolean();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Bool(arr.value(i)));
                }
            }
        }

        // Integer types - coerce to i64
        DataType::Int8 => {
            let arr = array.as_primitive::<Int8Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(i64::from(arr.value(i))));
                }
            }
        }
        DataType::Int16 => {
            let arr = array.as_primitive::<Int16Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(i64::from(arr.value(i))));
                }
            }
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<Int32Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(i64::from(arr.value(i))));
                }
            }
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<Int64Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(arr.value(i)));
                }
            }
        }
        DataType::UInt8 => {
            let arr = array.as_primitive::<UInt8Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(i64::from(arr.value(i))));
                }
            }
        }
        DataType::UInt16 => {
            let arr = array.as_primitive::<UInt16Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(i64::from(arr.value(i))));
                }
            }
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<UInt32Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(i64::from(arr.value(i))));
                }
            }
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<UInt64Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Int(i64::try_from(arr.value(i)).map_err(|_| {
                        exec_datafusion_err!("UInt64 value {} is out of range for i64", arr.value(i))
                    })?));
                }
            }
        }

        // Float types - coerce to f64
        DataType::Float32 => {
            let arr = array.as_primitive::<Float32Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Float(f64::from(arr.value(i))));
                }
            }
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<Float64Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Float(arr.value(i)));
                }
            }
        }

        // String types
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Str(arr.value(i).to_string()));
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Str(arr.value(i).to_string()));
                }
            }
        }
        DataType::Utf8View => {
            let arr = array.as_string_view();
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    union.push_none();
                } else {
                    union.push(JsonUnionField::Str(arr.value(i).to_string()));
                }
            }
        }

        dt => {
            return exec_err!("Unsupported array type for json_from_scalar: {:?}", dt);
        }
    }

    Ok(union)
}
