use datafusion::assert_batches_eq;

mod utils;
use utils::run_query;

#[tokio::test]
async fn test_json_obj_contains() {
    let expected = [
        "+------------------+-----------------------------------------------+",
        "| name             | json_obj_contains(test.json_data,Utf8(\"foo\")) |",
        "+------------------+-----------------------------------------------+",
        "| object_foo       | true                                          |",
        "| object_foo_array | true                                          |",
        "| object_foo_obj   | true                                          |",
        "| object_foo_null  | true                                          |",
        "| object_bar       | false                                         |",
        "| list_foo         | false                                         |",
        "| invalid_json     | false                                         |",
        "+------------------+-----------------------------------------------+",
    ];

    let batches = run_query("select name, json_obj_contains(json_data, 'foo') from test")
        .await
        .unwrap();
    assert_batches_eq!(expected, &batches);
}
