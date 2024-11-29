use codspeed_criterion_compat::{criterion_group, criterion_main, Bencher, Criterion};

use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion_functions_json::udfs::{json_contains_udf, json_get_str_udf};

fn bench_json_contains(b: &mut Bencher) {
    let json_contains = json_contains_udf();
    let args = &[
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"aa": "x", "ab: "y"}, "b": []}"#.to_string(),
        ))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("aa".to_string()))),
    ];

    b.iter(|| json_contains.invoke_batch(args, 1).unwrap());
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

    b.iter(|| json_get_str.invoke_batch(args, 1).unwrap());
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("json_contains", bench_json_contains);
    c.bench_function("json_get_str", bench_json_get_str);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
