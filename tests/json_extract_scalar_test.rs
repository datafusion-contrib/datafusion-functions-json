use crate::utils::{display_val, run_query};
use rstest::{fixture, rstest};

mod utils;

#[fixture]
fn json_data() -> String {
    let json = r#"
        {
          "store": {
            "book name": "My Favorite Books",
            "book": [
              {"title": "1984", "author": "George Orwell"},
              {"title": "Pride and Prejudice", "author": "Jane Austen"}
            ]
          }
        }
    "#;
    json.to_string()
}

#[rstest]
#[case("$.store.book[0].author", "{str=George Orwell}")]
#[tokio::test]
async fn test_json_extract_scalar(json_data: String, #[case] path: &str, #[case] expected: &str) {
    let result = json_extract_scalar(&json_data, path).await;
    assert_eq!(result, expected.to_string());
}

#[rstest]
#[case("[1, 2, 3]", "$[2]", "{int=3}")]
#[case("[1, 2, 3]", "$[3]", "{null=}")]
#[tokio::test]
async fn test_json_extract_scalar_simple(#[case] json: String, #[case] path: &str, #[case] expected: &str) {
    let result = json_extract_scalar(&json, path).await;
    assert_eq!(result, expected.to_string());
}

async fn json_extract_scalar(json: &str, path: &str) -> String {
    let sql = format!("select json_extract_scalar('{}', '{}')", json, path);
    let batches = run_query(sql.as_str()).await.unwrap();
    display_val(batches).await.1
}
