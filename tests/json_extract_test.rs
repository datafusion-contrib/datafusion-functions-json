use crate::utils::{display_val, run_query};
use rstest::{fixture, rstest};

mod utils;

#[fixture]
fn json_data() -> String {
    let json = r#"{"a": {"a a": "My Collection","ab": [{"ac": "Dune", "ca": "Frank Herbert"},{"ad": "Foundation", "da": "Isaac Asimov"}]}}"#;
    json.to_string()
}

#[rstest]
#[case(
    "$.a.ab",
    "{array=[{\"ac\": \"Dune\", \"ca\": \"Frank Herbert\"},{\"ad\": \"Foundation\", \"da\": \"Isaac Asimov\"}]}"
)]
#[tokio::test]
async fn test_json_paths(json_data: String, #[case] path: &str, #[case] expected: &str) {
    let result = json_extract(&json_data, path).await;
    assert_eq!(result, expected.to_string());
}

#[rstest]
#[tokio::test]
#[ignore]
async fn test_invalid_json_path(json_data: String) {
    let result = json_extract(&json_data, "store.invalid.path").await;
    assert_eq!(result, "".to_string());
}

async fn json_extract(json: &str, path: &str) -> String {
    let sql = format!("select json_extract('{}', '{}')", json, path);
    let batches = run_query(sql.as_str()).await.unwrap();
    display_val(batches).await.1
}
