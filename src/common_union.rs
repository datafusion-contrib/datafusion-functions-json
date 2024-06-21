use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UnionArray};
use arrow::buffer::Buffer;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};
use datafusion_common::ScalarValue;

pub(crate) fn is_json_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(fields, UnionMode::Sparse) => fields == &union_fields(),
        _ => false,
    }
}

/// Extract nested JSON from a `JsonUnion` `UnionArray`
///
/// # Arguments
/// * `array` - The `UnionArray` to extract the nested JSON from
/// * `object_lookup` - If `true`, extract from the "object" member of the union,
///   otherwise extract from the "array" member
pub(crate) fn nested_json_array(array: &ArrayRef, object_lookup: bool) -> Option<&StringArray> {
    let union_array: &UnionArray = array.as_any().downcast_ref::<UnionArray>()?;
    let type_id = if object_lookup { TYPE_ID_OBJECT } else { TYPE_ID_ARRAY };
    union_array.child(type_id).as_any().downcast_ref()
}

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
        DataType::Union(union_fields(), UnionMode::Sparse)
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
        self.type_ids[self.index] = TYPE_ID_NULL;
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
        let children: Vec<Arc<dyn Array>> = vec![
            Arc::new(BooleanArray::from(value.nulls)),
            Arc::new(BooleanArray::from(value.bools)),
            Arc::new(Int64Array::from(value.ints)),
            Arc::new(Float64Array::from(value.floats)),
            Arc::new(StringArray::from(value.strings)),
            Arc::new(StringArray::from(value.arrays)),
            Arc::new(StringArray::from(value.objects)),
        ];
        UnionArray::try_new(union_fields(), Buffer::from_vec(value.type_ids).into(), None, children)
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

const TYPE_ID_NULL: i8 = 0;
const TYPE_ID_BOOL: i8 = 1;
const TYPE_ID_INT: i8 = 2;
const TYPE_ID_FLOAT: i8 = 3;
const TYPE_ID_STR: i8 = 4;
const TYPE_ID_ARRAY: i8 = 5;
const TYPE_ID_OBJECT: i8 = 6;

fn union_fields() -> UnionFields {
    static FIELDS: OnceLock<UnionFields> = OnceLock::new();
    FIELDS
        .get_or_init(|| {
            UnionFields::from_iter([
                (TYPE_ID_NULL, Arc::new(Field::new("null", DataType::Boolean, true))),
                (TYPE_ID_BOOL, Arc::new(Field::new("bool", DataType::Boolean, false))),
                (TYPE_ID_INT, Arc::new(Field::new("int", DataType::Int64, false))),
                (TYPE_ID_FLOAT, Arc::new(Field::new("float", DataType::Float64, false))),
                (TYPE_ID_STR, Arc::new(Field::new("str", DataType::Utf8, false))),
                (TYPE_ID_ARRAY, Arc::new(Field::new("array", DataType::Utf8, false))),
                (TYPE_ID_OBJECT, Arc::new(Field::new("object", DataType::Utf8, false))),
            ])
        })
        .clone()
}

impl JsonUnionField {
    fn type_id(&self) -> i8 {
        match self {
            Self::JsonNull => TYPE_ID_NULL,
            Self::Bool(_) => TYPE_ID_BOOL,
            Self::Int(_) => TYPE_ID_INT,
            Self::Float(_) => TYPE_ID_FLOAT,
            Self::Str(_) => TYPE_ID_STR,
            Self::Array(_) => TYPE_ID_ARRAY,
            Self::Object(_) => TYPE_ID_OBJECT,
        }
    }

    pub fn scalar_value(f: Option<Self>) -> ScalarValue {
        ScalarValue::Union(
            f.map(|f| (f.type_id(), Box::new(f.into()))),
            union_fields(),
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
