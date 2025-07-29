use std::sync::Arc;

use codspeed_criterion_compat::{criterion_group, criterion_main, Bencher, Criterion};

use datafusion::arrow::array::{StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion::{common::ScalarValue, logical_expr::ScalarFunctionArgs};
use datafusion_functions_json::udfs::{json_contains_udf, json_get_str_udf};

fn bench_json_contains(b: &mut Bencher) {
    let json_contains = json_contains_udf();
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    let arg_fields = vec![
        Arc::new(Field::new("arg0", DataType::Utf8, false)),
        Arc::new(Field::new("arg1", DataType::Utf8, false)),
        Arc::new(Field::new("arg2", DataType::Utf8, false)),
    ];

    let return_field = Arc::new(Field::new("json_contains", DataType::Boolean, false));

    b.iter(|| {
        json_contains
            .invoke_with_args(ScalarFunctionArgs {
                args: args.clone(),
                number_rows: 1,
                arg_fields: arg_fields.clone(),
                return_field: return_field.clone(),
            })
            .unwrap()
    });
}

fn bench_json_get_str_scalar(b: &mut Bencher) {
    let json_get_str = json_get_str_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    let arg_fields = vec![
        Arc::new(Field::new("arg0", DataType::Utf8, false)),
        Arc::new(Field::new("arg1", DataType::Utf8, false)),
        Arc::new(Field::new("arg2", DataType::Utf8, false)),
    ];

    let return_field = Arc::new(Field::new("json_get_str", DataType::Utf8, false));

    b.iter(|| {
        json_get_str
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                arg_fields: arg_fields.clone(),
                number_rows: 1,
                return_field: return_field.clone(),
            })
            .unwrap();
    });
}

fn bench_json_get_str_array(b: &mut Bencher) {
    let json_get_str = json_get_str_udf();
    let args = &[
        ColumnarValue::Array(Arc::new(StringArray::from_iter_values(vec![
            r#"{"a": {"aa": "x", "ab": "y"}, "b": []}"#.to_string(),
            r#"{"a": {"aa": "x2", "ab": "y2"}, "b": []}"#.to_string(),
        ]))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    let arg_fields = vec![
        Arc::new(Field::new("arg0", DataType::Utf8, false)),
        Arc::new(Field::new("arg1", DataType::Utf8, false)),
        Arc::new(Field::new("arg2", DataType::Utf8, false)),
    ];

    let return_field = Arc::new(Field::new("json_get_str", DataType::Utf8, false));

    b.iter(|| {
        json_get_str
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                arg_fields: arg_fields.clone(),
                number_rows: 1,
                return_field: return_field.clone(),
            })
            .unwrap();
    });
}

fn bench_json_get_str_view_array(b: &mut Bencher) {
    let json_get_str = json_get_str_udf();
    let args = &[
        ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(vec![
            r#"{"a": {"aa": "x", "ab": "y"}, "b": []}"#.to_string(),
            r#"{"a": {"aa": "x2", "ab": "y2"}, "b": []}"#.to_string(),
        ]))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    let arg_fields = vec![
        Arc::new(Field::new("arg0", DataType::Utf8View, false)),
        Arc::new(Field::new("arg1", DataType::Utf8, false)),
        Arc::new(Field::new("arg2", DataType::Utf8, false)),
    ];

    let return_field = Arc::new(Field::new("json_get_str", DataType::Utf8, false));

    b.iter(|| {
        json_get_str
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                arg_fields: arg_fields.clone(),
                number_rows: 1,
                return_field: return_field.clone(),
            })
            .unwrap();
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("json_contains", bench_json_contains);
    c.bench_function("json_get_str_scalar", bench_json_get_str_scalar);
    c.bench_function("json_get_str_array", bench_json_get_str_array);
    c.bench_function("json_get_str_view_array", bench_json_get_str_view_array);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
