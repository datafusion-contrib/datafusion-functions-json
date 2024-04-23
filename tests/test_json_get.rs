use arrow_schema::DataType;
use datafusion::assert_batches_eq;

mod utils;
use utils::{display_val, run_query};

#[tokio::test]
async fn test_json_get_union() {
    let batches = run_query("select name, json_get(json_data, 'foo') from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+--------------------------------------+",
        "| name             | json_get(test.json_data,Utf8(\"foo\")) |",
        "+------------------+--------------------------------------+",
        "| object_foo       | {str=abc}                            |",
        "| object_foo_array | {array=[1]}                          |",
        "| object_foo_obj   | {object={}}                          |",
        "| object_foo_null  | {null=true}                          |",
        "| object_bar       | {null=}                              |",
        "| list_foo         | {null=}                              |",
        "| invalid_json     | {null=}                              |",
        "+------------------+--------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_equals() {
    let e = run_query(r#"select name, json_get(json_data, 'foo')='abc' from test"#)
        .await
        .unwrap_err();

    // see https://github.com/apache/datafusion/issues/10180
    assert!(e
        .to_string()
        .starts_with("Error during planning: Cannot infer common argument type for comparison operation Union"));
}

#[tokio::test]
async fn test_json_get_cast_equals() {
    let batches = run_query(r#"select name, json_get(json_data, 'foo')::string='abc' from test"#)
        .await
        .unwrap();

    let expected = [
        "+------------------+----------------------------------------------------+",
        "| name             | json_get(test.json_data,Utf8(\"foo\")) = Utf8(\"abc\") |",
        "+------------------+----------------------------------------------------+",
        "| object_foo       | true                                               |",
        "| object_foo_array |                                                    |",
        "| object_foo_obj   |                                                    |",
        "| object_foo_null  |                                                    |",
        "| object_bar       |                                                    |",
        "| list_foo         |                                                    |",
        "| invalid_json     |                                                    |",
        "+------------------+----------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_str() {
    let batches = run_query("select name, json_get_str(json_data, 'foo') from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+------------------------------------------+",
        "| name             | json_get_str(test.json_data,Utf8(\"foo\")) |",
        "+------------------+------------------------------------------+",
        "| object_foo       | abc                                      |",
        "| object_foo_array |                                          |",
        "| object_foo_obj   |                                          |",
        "| object_foo_null  |                                          |",
        "| object_bar       |                                          |",
        "| list_foo         |                                          |",
        "| invalid_json     |                                          |",
        "+------------------+------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_str_equals() {
    let batches = run_query("select name, json_get_str(json_data, 'foo')='abc' from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+--------------------------------------------------------+",
        "| name             | json_get_str(test.json_data,Utf8(\"foo\")) = Utf8(\"abc\") |",
        "+------------------+--------------------------------------------------------+",
        "| object_foo       | true                                                   |",
        "| object_foo_array |                                                        |",
        "| object_foo_obj   |                                                        |",
        "| object_foo_null  |                                                        |",
        "| object_bar       |                                                        |",
        "| list_foo         |                                                        |",
        "| invalid_json     |                                                        |",
        "+------------------+--------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_str_int() {
    let batches = run_query(r#"select json_get_str('["a", "b", "c"]', 1) as v"#)
        .await
        .unwrap();

    let expected = ["+---+", "| v |", "+---+", "| b |", "+---+"];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_str_path() {
    let batches = run_query(r#"select json_get_str('{"a": {"aa": "x", "ab: "y"}, "b": []}', 'a', 'aa') as v"#)
        .await
        .unwrap();

    assert_eq!(
        display_val(batches).await,
        ("v".to_string(), DataType::Utf8, "x".to_string())
    );
}

#[tokio::test]
async fn test_json_get_str_null() {
    let e = run_query(r#"select json_get_str('{}', null)"#).await.unwrap_err();

    assert_eq!(
        e.to_string(),
        "Error during planning: Unexpected argument type to `json_get_str` at position 2, expected string or int."
    );
}

#[tokio::test]
async fn test_json_get_one_arg() {
    let e = run_query(r#"select json_get('{}')"#).await.unwrap_err();

    assert_eq!(
        e.to_string(),
        "Error during planning: The `json_get` function requires two or more arguments."
    );
}
