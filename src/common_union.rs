use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UnionArray};
use arrow::buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use datafusion_common::ScalarValue;

#[derive(Debug)]
pub(crate) struct JsonUnion {
    nulls: Vec<Option<bool>>,
    bools: Vec<Option<bool>>,
    ints: Vec<Option<i64>>,
    floats: Vec<Option<f64>>,
    strings: Vec<Option<String>>,
    arrays: Vec<Option<String>>,
    objects: Vec<Option<String>>,
    type_ids: Vec<i8>,
    index: usize,
    capacity: usize,
}

impl JsonUnion {
    fn new(capacity: usize) -> Self {
        Self {
            nulls: vec![None; capacity],
            bools: vec![None; capacity],
            ints: vec![None; capacity],
            floats: vec![None; capacity],
            strings: vec![None; capacity],
            arrays: vec![None; capacity],
            objects: vec![None; capacity],
            type_ids: vec![0; capacity],
            index: 0,
            capacity,
        }
    }

    pub fn data_type() -> DataType {
        DataType::Union(
            UnionFields::new(TYPE_IDS.to_vec(), union_fields().to_vec()),
            UnionMode::Sparse,
        )
    }

    fn push(&mut self, field: JsonUnionField) {
        self.type_ids[self.index] = field.type_id();
        match field {
            JsonUnionField::JsonNull => self.nulls[self.index] = Some(true),
            JsonUnionField::Bool(value) => self.bools[self.index] = Some(value),
            JsonUnionField::Int(value) => self.ints[self.index] = Some(value),
            JsonUnionField::Float(value) => self.floats[self.index] = Some(value),
            JsonUnionField::Str(value) => self.strings[self.index] = Some(value),
            JsonUnionField::Array(value) => self.arrays[self.index] = Some(value),
            JsonUnionField::Object(value) => self.objects[self.index] = Some(value),
        }
        self.index += 1;
        debug_assert!(self.index <= self.capacity);
    }

    fn push_none(&mut self) {
        self.type_ids[self.index] = TYPE_IDS[0];
        self.index += 1;
        debug_assert!(self.index <= self.capacity);
    }
}

/// So we can do `collect::<JsonUnion>()`
impl FromIterator<Option<JsonUnionField>> for JsonUnion {
    fn from_iter<I: IntoIterator<Item = Option<JsonUnionField>>>(iter: I) -> Self {
        let inner = iter.into_iter();
        let (lower, upper) = inner.size_hint();
        let mut union = Self::new(upper.unwrap_or(lower));

        for opt_field in inner {
            if let Some(union_field) = opt_field {
                union.push(union_field);
            } else {
                union.push_none();
            }
        }
        union
    }
}

impl TryFrom<JsonUnion> for UnionArray {
    type Error = arrow::error::ArrowError;

    fn try_from(value: JsonUnion) -> Result<Self, Self::Error> {
        UnionArray::try_new(
            UnionFields::new(TYPE_IDS.iter().cloned(), union_fields()),
            ScalarBuffer::from_iter(TYPE_IDS.iter().cloned()),
            None,
            vec![
                Arc::new(BooleanArray::from(value.nulls)),
                Arc::new(BooleanArray::from(value.bools)),
                Arc::new(Int64Array::from(value.ints)),
                Arc::new(Float64Array::from(value.floats)),
                Arc::new(StringArray::from(value.strings)),
                Arc::new(StringArray::from(value.arrays)),
                Arc::new(StringArray::from(value.objects)),
            ],
        )
    }
}

#[derive(Debug)]
pub(crate) enum JsonUnionField {
    JsonNull,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(String),
    Object(String),
}

const TYPE_IDS: &[i8] = &[0, 1, 2, 3, 4, 5, 6];

fn union_fields() -> [Field; 7] {
    [
        Field::new("null", DataType::Boolean, true),
        Field::new("bool", DataType::Boolean, false),
        Field::new("int", DataType::Int64, false),
        Field::new("float", DataType::Float64, false),
        Field::new("str", DataType::Utf8, false),
        Field::new("array", DataType::Utf8, false),
        Field::new("object", DataType::Utf8, false),
    ]
}

impl JsonUnionField {
    fn type_id(&self) -> i8 {
        match self {
            Self::JsonNull => 0,
            Self::Bool(_) => 1,
            Self::Int(_) => 2,
            Self::Float(_) => 3,
            Self::Str(_) => 4,
            Self::Array(_) => 5,
            Self::Object(_) => 6,
        }
    }

    pub fn scalar_value(f: Option<Self>) -> ScalarValue {
        ScalarValue::Union(
            f.map(|f| (f.type_id(), Box::new(f.into()))),
            UnionFields::new(TYPE_IDS.to_vec(), union_fields().to_vec()),
            UnionMode::Sparse,
        )
    }
}

impl From<JsonUnionField> for ScalarValue {
    fn from(value: JsonUnionField) -> Self {
        match value {
            JsonUnionField::JsonNull => Self::Null,
            JsonUnionField::Bool(b) => Self::Boolean(Some(b)),
            JsonUnionField::Int(i) => Self::Int64(Some(i)),
            JsonUnionField::Float(f) => Self::Float64(Some(f)),
            JsonUnionField::Str(s) | JsonUnionField::Array(s) | JsonUnionField::Object(s) => Self::Utf8(Some(s)),
        }
    }
}
