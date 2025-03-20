use codspeed_criterion_compat::{criterion_group, criterion_main, Bencher, Criterion};

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::{common::ScalarValue, logical_expr::ScalarFunctionArgs};
use datafusion_functions_json::udfs::{json_contains_udf, json_get_str_top_level_sorted_udf, json_get_str_udf};

fn bench_json_contains(b: &mut Bencher) {
    let json_contains = json_contains_udf();
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    b.iter(|| {
        json_contains
            .invoke_with_args(ScalarFunctionArgs {
                args: args.clone(),
                number_rows: 1,
                return_type: &DataType::Boolean,
            })
            .unwrap()
    });
}

fn bench_json_get_str(b: &mut Bencher) {
    let json_get_str = json_get_str_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    b.iter(|| {
        json_get_str
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                number_rows: 1,
                return_type: &DataType::Utf8,
            })
            .unwrap()
    });
}

fn make_json_negative_testcase() -> String {
    // build a json with keys "b1", "b2" ... "b100", each with a large value ("a" repeated 1024 times)
    let kvs = (0..100)
        .map(|i| format!(r#""b{}": "{}""#, i, "a".repeat(1024)))
        .collect::<Vec<_>>()
        .join(",");
    format!(r"{{ {kvs} }}")
}

fn bench_json_get_str_negative(b: &mut Bencher) {
    let json_get_str = json_get_str_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(make_json_negative_testcase()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))), // lexicographically less than "b1"
    ];

    b.iter(|| {
        json_get_str
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                number_rows: 1,
                return_type: &DataType::Utf8,
            })
            .unwrap()
    });
}

fn bench_json_get_str_sorted(b: &mut Bencher) {
    let json_get_str = json_get_str_top_level_sorted_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    b.iter(|| {
        json_get_str
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                number_rows: 1,
                return_type: &DataType::Utf8,
            })
            .unwrap()
    });
}

fn bench_json_get_str_sorted_negative(b: &mut Bencher) {
    let json_get_str = json_get_str_top_level_sorted_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(make_json_negative_testcase()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))), // lexicographically less than "b1"
    ];

    b.iter(|| {
        json_get_str
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                number_rows: 1,
                return_type: &DataType::Utf8,
            })
            .unwrap()
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("json_contains", bench_json_contains);
    c.bench_function("json_get_str", bench_json_get_str);
    c.bench_function("json_get_str_negative", bench_json_get_str_negative);
    c.bench_function("json_get_str_sorted", bench_json_get_str_sorted);
    c.bench_function("json_get_str_sorted_negative", bench_json_get_str_sorted_negative);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
