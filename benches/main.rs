use std::sync::Arc;

use codspeed_criterion_compat::{criterion_group, criterion_main, Bencher, Criterion};

use datafusion::arrow::array::{StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion::{common::ScalarValue, logical_expr::ScalarFunctionArgs};
use datafusion_functions_json::udfs::{json_contains_udf, json_get_str_udf, json_length_udf};

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
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
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
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
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
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
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
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();
    });
}

/// A document with a 64 element array, so that stepping through it is measurable.
fn array_doc() -> String {
    let items: Vec<String> = (0..64).map(|i| format!(r#""v{i}""#)).collect();
    format!(r#"{{"a": [{}]}}"#, items.join(", "))
}

fn bench_array_path(b: &mut Bencher, udf: &datafusion::logical_expr::ScalarUDF, index: &ScalarValue) {
    let args = &[
        ColumnarValue::Array(Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            array_doc(),
            1024,
        )))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(index.clone()),
    ];

    let arg_fields = vec![
        Arc::new(Field::new("arg0", DataType::Utf8, false)),
        Arc::new(Field::new("arg1", DataType::Utf8, false)),
        Arc::new(Field::new("arg2", index.data_type(), false)),
    ];

    let return_field = Arc::new(Field::new(
        "out",
        udf.return_type(&[DataType::Utf8, DataType::Utf8, index.data_type()])
            .unwrap(),
        false,
    ));

    b.iter(|| {
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: arg_fields.clone(),
            number_rows: 1024,
            return_field: return_field.clone(),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })
        .unwrap();
    });
}

fn bench_json_get_str_index(b: &mut Bencher) {
    bench_array_path(b, &json_get_str_udf(), &ScalarValue::Int64(Some(32)));
}

fn bench_json_get_str_index_last(b: &mut Bencher) {
    bench_array_path(b, &json_get_str_udf(), &ScalarValue::Int64(Some(63)));
}

fn bench_json_get_str_negative_index(b: &mut Bencher) {
    bench_array_path(b, &json_get_str_udf(), &ScalarValue::Int64(Some(-32)));
}

fn bench_json_get_str_text_index(b: &mut Bencher) {
    bench_array_path(b, &json_get_str_udf(), &ScalarValue::Utf8(Some("32".to_string())));
}

fn bench_json_length_array(b: &mut Bencher) {
    let args = &[
        ColumnarValue::Array(Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            array_doc(),
            1024,
        )))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
    ];
    let arg_fields = vec![
        Arc::new(Field::new("arg0", DataType::Utf8, false)),
        Arc::new(Field::new("arg1", DataType::Utf8, false)),
    ];
    let return_field = Arc::new(Field::new("json_length", DataType::UInt64, false));
    let udf = json_length_udf();
    b.iter(|| {
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: arg_fields.clone(),
            number_rows: 1024,
            return_field: return_field.clone(),
            config_options: Arc::new(datafusion::config::ConfigOptions::default()),
        })
        .unwrap();
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("json_get_str_index", bench_json_get_str_index);
    c.bench_function("json_get_str_index_last", bench_json_get_str_index_last);
    c.bench_function("json_get_str_negative_index", bench_json_get_str_negative_index);
    c.bench_function("json_get_str_text_index", bench_json_get_str_text_index);
    c.bench_function("json_length_array", bench_json_length_array);
    c.bench_function("json_contains", bench_json_contains);
    c.bench_function("json_get_str_scalar", bench_json_get_str_scalar);
    c.bench_function("json_get_str_array", bench_json_get_str_array);
    c.bench_function("json_get_str_view_array", bench_json_get_str_view_array);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
