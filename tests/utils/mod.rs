#![allow(dead_code)]
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow::{array::StringArray, record_batch::RecordBatch};
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion_functions_json::register_all;

async fn create_test_table() -> Result<SessionContext> {
    let mut ctx = SessionContext::new();
    register_all(&mut ctx)?;

    let test_data = [
        ("object_foo", r#" {"foo": "abc"} "#),
        ("object_foo_array", r#" {"foo": [1]} "#),
        ("object_foo_obj", r#" {"foo": {}} "#),
        ("object_foo_null", r#" {"foo": null} "#),
        ("object_bar", r#" {"bar": true} "#),
        ("list_foo", r#" ["foo"] "#),
        ("invalid_json", "is not json"),
    ];
    let test_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("json_data", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                test_data.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                test_data.iter().map(|(_, json)| *json).collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.register_batch("test", test_batch)?;

    let other_data = [
        (r#" {"foo": 42} "#, "foo", 0),
        (r#" {"foo": 42} "#, "bar", 1),
        (r" [42] ", "foo", 0),
        (r" [42] ", "bar", 1),
    ];
    let other_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("json_data", DataType::Utf8, false),
            Field::new("str_key", DataType::Utf8, false),
            Field::new("int_key", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                other_data.iter().map(|(json, _, _)| *json).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                other_data.iter().map(|(_, str_key, _)| *str_key).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                other_data.iter().map(|(_, _, int_key)| *int_key).collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.register_batch("other", other_batch)?;

    Ok(ctx)
}

pub async fn run_query(sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table().await?;
    let df = ctx.sql(sql).await?;
    df.collect().await
}

pub async fn display_val(batch: Vec<RecordBatch>) -> (DataType, String) {
    assert_eq!(batch.len(), 1);
    let batch = batch.first().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let schema = batch.schema();
    let schema_col = schema.field(0);
    let c = batch.column(0);
    let options = FormatOptions::default().with_display_error(true);
    let f = ArrayFormatter::try_new(c.as_ref(), &options).unwrap();
    let repr = f.value(0).try_to_string().unwrap();
    (schema_col.data_type().clone(), repr)
}
