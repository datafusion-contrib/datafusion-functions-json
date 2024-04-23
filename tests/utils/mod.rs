#![allow(dead_code)]
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow::{array::StringArray, record_batch::RecordBatch};
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion_functions_json::register_all;

async fn create_test_table() -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("json_data", DataType::Utf8, false),
    ]));

    let data = [
        ("object_foo", r#" {"foo": "abc"} "#),
        ("object_foo_array", r#" {"foo": [1]} "#),
        ("object_foo_obj", r#" {"foo": {}} "#),
        ("object_foo_null", r#" {"foo": null} "#),
        ("object_bar", r#" {"bar": true} "#),
        ("list_foo", r#" ["foo"] "#),
        ("invalid_json", "is not json"),
    ];

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                data.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                data.iter().map(|(_, json)| *json).collect::<Vec<_>>(),
            )),
        ],
    )?;

    let mut ctx = SessionContext::new();
    register_all(&mut ctx)?;
    ctx.register_batch("test", batch)?;
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
