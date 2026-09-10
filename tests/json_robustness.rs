//! Properties that hold for every JSON UDF, checked over generated input.
//!
//! These are deliberately cheap and broad rather than targeted: they cover the accessors as a
//! group, so a change to any one of them — or to the shared scanning code in `common.rs` —
//! has to keep them true.
//!
//! * [`prop_no_panic_on_arbitrary_json`] — malformed input yields NULL, never a panic and
//!   never an error. This is the bug shape that produced the `todo!("BigInt not supported
//!   yet")` panic fixed in #124: a JSON integer too wide for jiter's fast path.
//! * [`prop_scalar_and_array_paths_agree`] — a literal argument is const-evaluated through
//!   the `ScalarValue` path while a column goes through the array path. They are separate
//!   code paths in every UDF here, and they have to agree.
//! * [`prop_input_encoding_does_not_change_result`] — `Utf8`, `LargeUtf8`, `Utf8View` and
//!   dictionary-encoded input are four more separate paths that have to agree.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_functions_json::register_all;
use proptest::prelude::*;

/// Every UDF that takes a JSON document and a lookup path.
const PATH_FUNCS: &[&str] = &[
    "json_get",
    "json_get_bool",
    "json_get_float",
    "json_get_int",
    "json_get_json",
    "json_get_str",
    "json_get_array",
    "json_as_text",
    "json_contains",
    "json_length",
    "json_object_keys",
];

const JSON_TABLE: &str = "t";
const JSON_COLUMN: &str = "j";

fn create_context() -> Result<SessionContext> {
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let mut ctx = SessionContext::new_with_config(config);
    register_all(&mut ctx)?;
    Ok(ctx)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

/// Each case here runs a query per UDF, so the defaults are kept low enough that the suite
/// stays fast. `PROPTEST_CASES` still wins when it is set, which is how these get run as a
/// fuzzer: `PROPTEST_CASES=100000 cargo test --release --test json_robustness`.
fn config(cases: u32) -> ProptestConfig {
    let mut config = ProptestConfig::default();
    if std::env::var_os("PROPTEST_CASES").is_none() {
        config.cases = cases;
    }
    config
}

/// Build the single-row table `t` holding `json` in the given encoding.
fn set_doc(ctx: &SessionContext, json: &str, encoding: &DataType) {
    let array: ArrayRef = match encoding {
        DataType::Utf8 => Arc::new(StringArray::from(vec![json])),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![json])),
        DataType::Utf8View => Arc::new(StringViewArray::from(vec![json])),
        DataType::Dictionary(_, _) => Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0]),
            Arc::new(StringArray::from(vec![json])),
        )),
        other => panic!("unsupported JSON encoding {other}"),
    };
    let schema = Schema::new(vec![Field::new(JSON_COLUMN, encoding.clone(), false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();
    let _ = ctx.deregister_table(JSON_TABLE);
    ctx.register_batch(JSON_TABLE, batch).unwrap();
}

const ENCODINGS: &[DataType] = &[DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];

fn dict_encoding() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// Run a single-row query, formatting the result. `Err` carries only the fact that the query
/// failed, so the properties are not brittle about error text.
async fn outcome(ctx: &SessionContext, sql: &str) -> std::result::Result<String, String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    let batch = &batches[0];
    let column = batch.column(0);
    let options = FormatOptions::default().with_display_error(true);
    let formatter = ArrayFormatter::try_new(column.as_ref(), &options).map_err(|e| e.to_string())?;
    Ok(format!(
        "{}={}",
        logical_type(batch.schema().field(0).data_type()),
        formatter.value(0).try_to_string().map_err(|e| e.to_string())?
    ))
}

/// Dictionary encoding of the input carries through to the output, so compare the type it
/// wraps rather than the wrapper.
fn logical_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value,
        other => other,
    }
}

// ---------------------------------------------------------------------------------------
// generators
// ---------------------------------------------------------------------------------------

/// Well-formed JSON, then mangled: truncated, or with a byte dropped. Most of the interesting
/// malformed input is "nearly valid", which random text never produces.
fn json_text() -> impl Strategy<Value = String> {
    prop_oneof![
        6 => valid_json(),
        2 => (valid_json(), 0usize..40).prop_map(|(j, n)| {
            let cut = j.char_indices().nth(n).map_or(j.len(), |(i, _)| i);
            j[..cut].to_string()
        }),
        2 => (valid_json(), 0usize..40).prop_map(|(j, n)| {
            let mut chars: Vec<char> = j.chars().collect();
            if !chars.is_empty() {
                chars.remove(n % chars.len());
            }
            chars.into_iter().collect()
        }),
        1 => ".{0,24}".prop_map(|s| s),
    ]
}

fn valid_json() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        Just("null".to_string()),
        any::<bool>().prop_map(|b| b.to_string()),
        any::<i64>().prop_map(|i| i.to_string()),
        any::<f64>()
            .prop_filter("JSON has no inf or nan", |f| f.is_finite())
            .prop_map(|f| format!("{f:?}")),
        "[a-zA-Z0-9 ._+-]{0,6}".prop_map(|s| format!("\"{s}\"")),
        // integers and floats outside the fast paths
        Just("123456789012345678901234567890".to_string()),
        Just("-99999999999999999999999999999999".to_string()),
        Just("1e309".to_string()),
        Just("-1e-309".to_string()),
    ];
    leaf.prop_recursive(3, 12, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(|items| format!("[{}]", items.join(", "))),
            prop::collection::vec(("[a-c]{1,2}", inner), 0..3).prop_map(|entries| {
                let body = entries
                    .into_iter()
                    .map(|(k, v)| format!("\"{k}\": {v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{body}}}")
            }),
        ]
    })
}

/// A lookup path: string keys and array indices, rendered as SQL arguments.
fn path() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        prop_oneof![
            "[a-c]{1,2}".prop_map(|k| format!("'{k}'")),
            (0i64..4).prop_map(|i| i.to_string()),
        ],
        0..3,
    )
}

fn call(func: &str, target: &str, path: &[String]) -> String {
    let args = std::iter::once(target.to_string())
        .chain(path.iter().cloned())
        .collect::<Vec<_>>();
    format!("{func}({})", args.join(", "))
}

/// `json_object_keys` and `json_get_array` return lists, and `json_length` takes no path
/// beyond the document, but every function still accepts the same argument shape.
fn callable(func: &str, path: &[String]) -> bool {
    // json_contains needs at least one path element to mean anything
    !(func == "json_contains" && path.is_empty())
}

// ---------------------------------------------------------------------------------------
// properties
// ---------------------------------------------------------------------------------------

/// No input, however malformed, may panic or raise an error. The accessors report "not
/// found" as NULL, and invalid JSON is just another way of not finding anything.
#[test]
fn prop_no_panic_on_arbitrary_json() {
    let rt = runtime();
    let ctx = create_context().unwrap();

    proptest!(config(96), |(json in json_text(), path in path())| {
        set_doc(&ctx, &json, &DataType::Utf8View);
        for func in PATH_FUNCS {
            if !callable(func, &path) {
                continue;
            }
            let sql = format!("select {} as v from {JSON_TABLE}", call(func, JSON_COLUMN, &path));
            let result = rt.block_on(outcome(&ctx, &sql));
            prop_assert!(result.is_ok(), "{sql}\n  json: {json:?}\n  {result:?}");
        }
    });
}

/// A literal document is const-evaluated through each UDF's `ScalarValue` path; a column goes
/// through its array path. The two must not disagree.
#[test]
fn prop_scalar_and_array_paths_agree() {
    let rt = runtime();
    let ctx = create_context().unwrap();

    proptest!(config(96), |(json in json_text(), path in path())| {
        set_doc(&ctx, &json, &DataType::Utf8View);
        let literal = format!("'{}'", json.replace('\'', "''"));
        for func in PATH_FUNCS {
            if !callable(func, &path) {
                continue;
            }
            let from_column = format!("select {} as v from {JSON_TABLE}", call(func, JSON_COLUMN, &path));
            let from_literal = format!("select {} as v", call(func, &literal, &path));
            let column = rt.block_on(outcome(&ctx, &from_column));
            let scalar = rt.block_on(outcome(&ctx, &from_literal));
            prop_assert_eq!(&column, &scalar,
                "\n  column:  {} => {:?}\n  literal: {} => {:?}\n  json: {:?}\n",
                from_column, column, from_literal, scalar, json);
        }
    });
}

/// The four string encodings a JSON column can arrive in are four separate code paths, and
/// they must all produce the same answer.
///
/// Dictionary-encoded input keeps its encoding in the output, so the dictionary wrapper is
/// stripped before comparing types. `json_get` is left out: it returns the JSON union, which
/// has no meaningful text form to compare — `json_as_text` and `json_get_json` cover the same
/// scanning code and do return text. `json_get_array` is left out too, but for a different
/// reason — see [`json_get_array_errors_on_dictionary_input`].
#[test]
fn prop_input_encoding_does_not_change_result() {
    let rt = runtime();
    let ctx = create_context().unwrap();

    proptest!(config(48), |(json in json_text(), path in path())| {
        for func in PATH_FUNCS {
            if !callable(func, &path) || matches!(*func, "json_get" | "json_get_array") {
                continue;
            }
            let sql = format!("select {} as v from {JSON_TABLE}", call(func, JSON_COLUMN, &path));

            set_doc(&ctx, &json, &DataType::Utf8View);
            let baseline = rt.block_on(outcome(&ctx, &sql));

            for encoding in ENCODINGS.iter().cloned().chain(std::iter::once(dict_encoding())) {
                set_doc(&ctx, &json, &encoding);
                let got = rt.block_on(outcome(&ctx, &sql));
                prop_assert_eq!(&got, &baseline,
                    "\n  {} in {} => {:?}\n  in Utf8View => {:?}\n  json: {:?}\n",
                    sql, encoding, got, baseline, json);
            }
        }
    });
}

/// `json_get_array` is unusable on a dictionary-encoded JSON column: every call fails with an
/// internal error, whatever the document and whatever the path.
///
/// `return_type_check` wraps the declared return type in `Dictionary(Int64, ..)` whenever the
/// first argument is a dictionary and the value type is not primitive. `json_get_array` is the
/// only UDF here whose value type is not primitive (`List(Utf8)`) *and* whose `InvokeResult`
/// sets `ACCEPT_DICT_RETURN = false`, so `invoke_array_array` hands back a bare `List` while
/// `return_type` promised a dictionary, and `DataFusion` rejects the mismatch.
///
/// The fix is to stop promising the wrapper for this one function, i.e. teach
/// `return_type_check` about `ACCEPT_DICT_RETURN` (or unwrap the dictionary in
/// `JsonGetArray::return_type`). **Delete this test once that lands**, and drop
/// `json_get_array` from the exclusion in [`prop_input_encoding_does_not_change_result`],
/// which then covers it.
#[test]
fn json_get_array_errors_on_dictionary_input() {
    let rt = runtime();
    let ctx = create_context().unwrap();

    for json in [r"[1, 2, 3]", r#"{"a": [1, 2]}"#, "null", "not json"] {
        set_doc(&ctx, json, &dict_encoding());
        let err = rt
            .block_on(outcome(
                &ctx,
                &format!("select json_get_array({JSON_COLUMN}) as v from {JSON_TABLE}"),
            ))
            .expect_err("json_get_array over a dictionary column should still be failing");
        // debug builds trip DataFusion's own assertion, release builds reach Arrow's schema
        // check; both name the dictionary type that `return_type` promised and did not deliver
        assert!(
            err.contains("Dictionary(Int64, List("),
            "unexpected error for {json:?}: {err}"
        );

        // the same document in a plain string column works
        set_doc(&ctx, json, &DataType::Utf8View);
        rt.block_on(outcome(
            &ctx,
            &format!("select json_get_array({JSON_COLUMN}) as v from {JSON_TABLE}"),
        ))
        .expect("json_get_array over a string column works");
    }
}
