use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UnionArray};
use arrow::buffer::Buffer;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use std::sync::Arc;

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
    pub fn new(capacity: usize) -> Self {
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

    pub fn push(&mut self, field: JsonUnionField) {
        self.type_ids[self.index] = field.type_id();
        match field {
            JsonUnionField::JsonNull => self.nulls[self.index] = Some(true),
            JsonUnionField::Bool(value) => self.bools[self.index] = Some(value),
            JsonUnionField::Int(value) => self.ints[self.index] = Some(value),
            JsonUnionField::Float(value) => self.floats[self.index] = Some(value),
            JsonUnionField::String(value) => self.strings[self.index] = Some(value),
            JsonUnionField::Array(value) => self.arrays[self.index] = Some(value),
            JsonUnionField::Object(value) => self.objects[self.index] = Some(value),
        }
        self.index += 1;
        debug_assert!(self.index <= self.capacity);
    }

    pub fn push_none(&mut self) {
        self.type_ids[self.index] = TYPE_IDS[0];
        self.index += 1;
        debug_assert!(self.index <= self.capacity);
    }
}

impl TryFrom<JsonUnion> for UnionArray {
    type Error = arrow::error::ArrowError;

    fn try_from(value: JsonUnion) -> Result<Self, Self::Error> {
        let [f0, f1, f2, f3, f4, f5, f6] = union_fields();
        let children: Vec<(Field, Arc<dyn Array>)> = vec![
            (f0, Arc::new(BooleanArray::from(value.nulls))),
            (f1, Arc::new(BooleanArray::from(value.bools))),
            (f2, Arc::new(Int64Array::from(value.ints))),
            (f3, Arc::new(Float64Array::from(value.floats))),
            (f4, Arc::new(StringArray::from(value.strings))),
            (f5, Arc::new(StringArray::from(value.arrays))),
            (f6, Arc::new(StringArray::from(value.objects))),
        ];
        UnionArray::try_new(TYPE_IDS, Buffer::from_slice_ref(&value.type_ids), None, children)
    }
}

#[derive(Debug)]
pub(crate) enum JsonUnionField {
    JsonNull,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
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
        Field::new("string", DataType::Utf8, false),
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
            Self::String(_) => 4,
            Self::Array(_) => 5,
            Self::Object(_) => 6,
        }
    }
}
