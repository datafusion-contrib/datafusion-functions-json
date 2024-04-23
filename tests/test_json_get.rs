use datafusion::assert_batches_eq;

mod utils;
use utils::run_query;

#[tokio::test]
async fn test_json_get() {
    let expected = [
        "+------------------+--------------------------------------+",
        "| name             | json_get(test.json_data,Utf8(\"foo\")) |",
        "+------------------+--------------------------------------+",
        "| object_foo       | {int=123}                            |",
        "| object_foo_array | {array=[1]}                          |",
        "| object_foo_obj   | {object={}}                          |",
        "| object_foo_null  | {null=true}                          |",
        "| object_bar       | {null=}                              |",
        "| list_foo         | {null=}                              |",
        "| invalid_json     | {null=}                              |",
        "+------------------+--------------------------------------+",
    ];

    let batches = run_query("select name, json_get(json_data, 'foo') from test")
        .await
        .unwrap();
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_equals() {
    let e = run_query("select name, json_get(json_data, 'foo')=123 from test")
        .await
        .unwrap_err();

    // see https://github.com/apache/datafusion/issues/10180
    assert!(e
        .to_string()
        .starts_with("Error during planning: Cannot infer common argument type for comparison operation Union"));
}
