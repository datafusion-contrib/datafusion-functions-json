use crate::utils::{display_val, run_query};
use rstest::rstest;

mod utils;


#[rstest]
#[case(
    "[{\"ac\": \"Dune\", \"ca\": \"Frank Herbert\"},{\"ad\": \"Foundation\", \"da\": \"Isaac Asimov\"}]",
    "{array=[{\"ac\": \"Dune\", \"ca\": \"Frank Herbert\"},{\"ad\": \"Foundation\", \"da\": \"Isaac Asimov\"}]}"
)]
#[tokio::test]
async fn test_json_parse(#[case] json_data: String, #[case] expected: &str) {
    let result = json_parse(&json_data).await;
    assert_eq!(result, expected.to_string());
}


async fn json_parse(json: &str) -> String {
    let sql = format!("select json_parse('{}')", json);
    let batches = run_query(sql.as_str()).await.unwrap();
    display_val(batches).await.1
}
