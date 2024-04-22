use arrow::datatypes::{DataType, Field, Schema};
use arrow::{array::StringArray, record_batch::RecordBatch};
use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion_functions_json::register_all;

async fn create_test_table() -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("json_data", DataType::Utf8, false),
    ]));

    let data = [
        ("object_foo", r#" {"foo": 123} "#),
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

/// Executes an expression on the test dataframe as a select.
/// Compares formatted output of a record batch with an expected
/// vector of strings, using the `assert_batch_eq`! macro
macro_rules! query {
    ($sql:expr, $expected: expr) => {
        let ctx = create_test_table().await?;
        let df = ctx.sql($sql).await?;
        let batches = df.collect().await?;

        assert_batches_eq!($expected, &batches);
    };
}

#[tokio::test]
async fn test_json_obj_contains() -> Result<()> {
    let expected = [
        "+--------------+-----------------------------------------------+",
        "| name         | json_obj_contains(test.json_data,Utf8(\"foo\")) |",
        "+--------------+-----------------------------------------------+",
        "| object_foo   | true                                          |",
        "| object_bar   | false                                         |",
        "| list_foo     | false                                         |",
        "| invalid_json | false                                         |",
        "+--------------+-----------------------------------------------+",
    ];

    query!("select name, json_obj_contains(json_data, 'foo') from test", expected);
    Ok(())
}
