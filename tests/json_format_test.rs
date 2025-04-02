use crate::utils::{display_val, run_query};
use rstest::rstest;

mod utils;


#[rstest]
#[case(
    "[1, 2, 3]",
    "[1, 2, 3]"
)]
#[tokio::test]
async fn test_json_format(#[case] json_data: String, #[case] expected: &str) {
    let result = json_format(&json_data).await;
    assert_eq!(result, expected.to_string());
}


async fn json_format(json: &str) -> String {
    let sql = format!("select json_format('{}')", json);
    let batches = run_query(sql.as_str()).await.unwrap();
    display_val(batches).await.1
}
