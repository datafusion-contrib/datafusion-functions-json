use arrow_schema::DataType;
use datafusion::assert_batches_eq;

mod utils;
use utils::{display_val, run_query};

#[tokio::test]
async fn test_json_contains() {
    let expected = [
        "+------------------+-------------------------------------------+",
        "| name             | json_contains(test.json_data,Utf8(\"foo\")) |",
        "+------------------+-------------------------------------------+",
        "| object_foo       | true                                      |",
        "| object_foo_array | true                                      |",
        "| object_foo_obj   | true                                      |",
        "| object_foo_null  | true                                      |",
        "| object_bar       | false                                     |",
        "| list_foo         | false                                     |",
        "| invalid_json     | false                                     |",
        "+------------------+-------------------------------------------+",
    ];

    let batches = run_query("select name, json_contains(json_data, 'foo') from test")
        .await
        .unwrap();
    assert_batches_eq!(expected, &batches);
}
#[tokio::test]
async fn test_json_contains_array() {
    let sql = "select json_contains('[1, 2, 3]', 2)";
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));
}

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
async fn test_json_get_array() {
    let sql = "select json_get('[1, 2, 3]', 2)";
    let batches = run_query(sql).await.unwrap();
    let (value_type, value_repr) = display_val(batches).await;
    assert!(matches!(value_type, DataType::Union(_, _)));
    assert_eq!(value_repr, "{int=3}");
}

#[tokio::test]
async fn test_json_get_equals() {
    let e = run_query(r"select name, json_get(json_data, 'foo')='abc' from test")
        .await
        .unwrap_err();

    // see https://github.com/apache/datafusion/issues/10180
    assert!(e
        .to_string()
        .starts_with("Error during planning: Cannot infer common argument type for comparison operation Union"));
}

#[tokio::test]
async fn test_json_get_cast_equals() {
    let batches = run_query(r"select name, json_get(json_data, 'foo')::string='abc' from test")
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
    let sql = "select name, json_get_str(json_data, 'foo')='abc' from test";
    let batches = run_query(sql).await.unwrap();

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
    let sql = r#"select json_get_str('["a", "b", "c"]', 1)"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "b".to_string()));

    let sql = r#"select json_get_str('["a", "b", "c"]', 3)"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, String::new()));
}

#[tokio::test]
async fn test_json_get_str_path() {
    let sql = r#"select json_get_str('{"a": {"aa": "x", "ab: "y"}, "b": []}', 'a', 'aa')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "x".to_string()));
}

#[tokio::test]
async fn test_json_get_str_null() {
    let e = run_query(r"select json_get_str('{}', null)").await.unwrap_err();

    assert_eq!(
        e.to_string(),
        "Error during planning: Unexpected argument type to `json_get_str` at position 2, expected string or int."
    );
}

#[tokio::test]
async fn test_json_get_no_path() {
    let batches = run_query(r#"select json_get('"foo"')::string"#).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "foo".to_string()));

    let batches = run_query(r#"select json_get('123')::int"#).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "123".to_string()));

    let batches = run_query(r#"select json_get('true')::int"#).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "".to_string()));
}

#[tokio::test]
async fn test_json_get_int() {
    let batches = run_query(r"select json_get_int('[1, 2, 3]', 1)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "2".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_int() {
    let sql = r#"select json_get('{"foo": 42}', 'foo')::int"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "42".to_string()));

    // floats not allowed
    let sql = r#"select json_get('{"foo": 4.2}', 'foo')::int"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, String::new()));
}

#[tokio::test]
async fn test_json_get_cast_int_path() {
    let sql = r#"select json_get('{"foo": [null, {"x": false, "bar": 73}}', 'foo', 1, 'bar')::int"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Int64, "73".to_string()));
}

#[tokio::test]
async fn test_json_get_int_lookup() {
    let sql = "select str_key, json_data from other where json_get_int(json_data, str_key) is not null";
    let batches = run_query(sql).await.unwrap();
    let expected = [
        "+---------+---------------+",
        "| str_key | json_data     |",
        "+---------+---------------+",
        "| foo     |  {\"foo\": 42}  |",
        "+---------+---------------+",
    ];
    assert_batches_eq!(expected, &batches);

    // lookup by int
    let sql = "select int_key, json_data from other where json_get_int(json_data, int_key) is not null";
    let batches = run_query(sql).await.unwrap();
    let expected = [
        "+---------+-----------+",
        "| int_key | json_data |",
        "+---------+-----------+",
        "| 0       |  [42]     |",
        "+---------+-----------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_float() {
    let batches = run_query("select json_get_float('[1.5]', 0)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "1.5".to_string()));

    // coerce int to float
    let batches = run_query("select json_get_float('[1]', 0)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "1.0".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_float() {
    let sql = r#"select json_get('{"foo": 4.2e2}', 'foo')::float"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Float64, "420.0".to_string()));
}

#[tokio::test]
async fn test_json_get_bool() {
    let batches = run_query("select json_get_bool('[true]', 0)").await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));

    let batches = run_query(r#"select json_get_bool('{"": false}', '')"#).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "false".to_string()));
}

#[tokio::test]
async fn test_json_get_cast_bool() {
    let sql = r#"select json_get('{"foo": true}', 'foo')::bool"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Boolean, "true".to_string()));
}

#[tokio::test]
async fn test_json_get_json() {
    let batches = run_query("select name, json_get_json(json_data, 'foo') from test")
        .await
        .unwrap();

    let expected = [
        "+------------------+-------------------------------------------+",
        "| name             | json_get_json(test.json_data,Utf8(\"foo\")) |",
        "+------------------+-------------------------------------------+",
        "| object_foo       | \"abc\"                                     |",
        "| object_foo_array | [1]                                       |",
        "| object_foo_obj   | {}                                        |",
        "| object_foo_null  | null                                      |",
        "| object_bar       |                                           |",
        "| list_foo         |                                           |",
        "| invalid_json     |                                           |",
        "+------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_json_get_json_float() {
    let sql = r#"select json_get_json('{"x": 4.2e-1}', 'x')"#;
    let batches = run_query(sql).await.unwrap();
    assert_eq!(display_val(batches).await, (DataType::Utf8, "4.2e-1".to_string()));
}
