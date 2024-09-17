#![allow(dead_code)]
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, Int64Array, StringViewArray, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema, UInt32Type, UInt8Type};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::arrow::{array::LargeStringArray, array::StringArray, record_batch::RecordBatch};
use datafusion::common::ParamValues;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_functions_json::register_all;

pub async fn create_context() -> Result<SessionContext> {
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let mut ctx = SessionContext::new_with_config(config);
    register_all(&mut ctx)?;
    Ok(ctx)
}

async fn create_test_table(large_utf8: bool, dict_encoded: bool) -> Result<SessionContext> {
    let ctx = create_context().await?;

    let test_data = [
        ("object_foo", r#" {"foo": "abc"} "#),
        ("object_foo_array", r#" {"foo": [1]} "#),
        ("object_foo_obj", r#" {"foo": {}} "#),
        ("object_foo_null", r#" {"foo": null} "#),
        ("object_bar", r#" {"bar": true} "#),
        ("list_foo", r#" ["foo"] "#),
        ("invalid_json", "is not json"),
    ];
    let json_values = test_data.iter().map(|(_, json)| *json).collect::<Vec<_>>();
    let (mut json_data_type, mut json_array): (DataType, ArrayRef) = if large_utf8 {
        (DataType::LargeUtf8, Arc::new(LargeStringArray::from(json_values)))
    } else {
        (DataType::Utf8, Arc::new(StringArray::from(json_values)))
    };

    if dict_encoded {
        json_data_type = DataType::Dictionary(DataType::Int32.into(), json_data_type.into());
        json_array = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from_iter_values(0..(json_array.len() as i32)),
            json_array,
        ));
    }

    let test_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("json_data", json_data_type, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                test_data.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            )),
            json_array,
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

    let more_nested = [
        (r#" {"foo": {"bar": [0]}} "#, "foo", "bar", 0),
        (r#" {"foo": {"bar": [1]}} "#, "foo", "spam", 0),
        (r#" {"foo": {"bar": null}} "#, "foo", "bar", 0),
    ];
    let more_nested_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("json_data", DataType::Utf8, false),
            Field::new("str_key1", DataType::Utf8, false),
            Field::new("str_key2", DataType::Utf8, false),
            Field::new("int_key", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                more_nested.iter().map(|(json, _, _, _)| *json).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                more_nested
                    .iter()
                    .map(|(_, str_key1, _, _)| *str_key1)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                more_nested
                    .iter()
                    .map(|(_, _, str_key2, _)| *str_key2)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                more_nested
                    .iter()
                    .map(|(_, _, _, int_key)| *int_key)
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.register_batch("more_nested", more_nested_batch)?;

    let dict_data = [
        (r#" {"foo": {"bar": [0]}} "#, "foo", "bar", 0),
        (r#" {"bar": "snap"} "#, "foo", "spam", 0),
        (r#" {"spam": 1, "snap": 2} "#, "foo", "spam", 0),
        (r#" {"spam": 1, "snap": 2} "#, "foo", "snap", 0),
    ];
    let dict_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "json_data",
                DataType::Dictionary(DataType::UInt32.into(), DataType::Utf8.into()),
                false,
            ),
            Field::new(
                "str_key1",
                DataType::Dictionary(DataType::UInt8.into(), DataType::LargeUtf8.into()),
                false,
            ),
            Field::new(
                "str_key2",
                DataType::Dictionary(DataType::UInt8.into(), DataType::Utf8View.into()),
                false,
            ),
            Field::new(
                "int_key",
                DataType::Dictionary(DataType::Int64.into(), DataType::UInt64.into()),
                false,
            ),
        ])),
        vec![
            Arc::new(DictionaryArray::<UInt32Type>::new(
                UInt32Array::from_iter_values(dict_data.iter().enumerate().map(|(id, _)| id as u32)),
                Arc::new(StringArray::from(
                    dict_data.iter().map(|(json, _, _, _)| *json).collect::<Vec<_>>(),
                )),
            )),
            Arc::new(DictionaryArray::<UInt8Type>::new(
                UInt8Array::from_iter_values(dict_data.iter().enumerate().map(|(id, _)| id as u8)),
                Arc::new(LargeStringArray::from(
                    dict_data
                        .iter()
                        .map(|(_, str_key1, _, _)| *str_key1)
                        .collect::<Vec<_>>(),
                )),
            )),
            Arc::new(DictionaryArray::<UInt8Type>::new(
                UInt8Array::from_iter_values(dict_data.iter().enumerate().map(|(id, _)| id as u8)),
                Arc::new(StringViewArray::from(
                    dict_data
                        .iter()
                        .map(|(_, _, str_key2, _)| *str_key2)
                        .collect::<Vec<_>>(),
                )),
            )),
            Arc::new(DictionaryArray::<Int64Type>::new(
                Int64Array::from_iter_values(dict_data.iter().enumerate().map(|(id, _)| id as i64)),
                Arc::new(UInt64Array::from_iter_values(
                    dict_data.iter().map(|(_, _, _, int_key)| *int_key as u64),
                )),
            )),
        ],
    )?;
    ctx.register_batch("dicts", dict_batch)?;

    Ok(ctx)
}

pub async fn run_query(sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table(false, false).await?;
    ctx.sql(sql).await?.collect().await
}

pub async fn run_query_large(sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table(true, false).await?;
    ctx.sql(sql).await?.collect().await
}

pub async fn run_query_dict(sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table(false, true).await?;
    ctx.sql(sql).await?.collect().await
}

pub async fn run_query_params(
    sql: &str,
    large_utf8: bool,
    query_values: impl Into<ParamValues>,
) -> Result<Vec<RecordBatch>> {
    let ctx = create_test_table(large_utf8, false).await?;
    ctx.sql(sql).await?.with_param_values(query_values)?.collect().await
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

pub async fn logical_plan(sql: &str) -> Vec<String> {
    let batches = run_query(sql).await.unwrap();
    let plan_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let logical_plan = plan_col.value(0);
    logical_plan.split('\n').map(ToString::to_string).collect()
}
