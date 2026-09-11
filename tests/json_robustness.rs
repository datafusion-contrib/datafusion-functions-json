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
//!   code paths in every UDF here, and they have to agree — for plain and dictionary-encoded
//!   documents alike, including on whether the result is a dictionary.
//! * [`prop_input_encoding_does_not_change_result`] — `Utf8`, `LargeUtf8`, `Utf8View` and
//!   their dictionary-encoded forms are more separate paths that have to agree, with the
//!   lookup path written as literals and again with it held in a column.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Int32Type};
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
/// A lookup path step held in a column of [`JSON_TABLE`] rather than written as a literal.
const PATH_COLUMN: &str = "p";

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
    set_table(ctx, vec![(JSON_COLUMN, string_array(json, encoding))]);
}

/// Replace the table `t` with one holding `columns`.
fn set_table(ctx: &SessionContext, columns: Vec<(&str, ArrayRef)>) {
    let batch = RecordBatch::try_from_iter_with_nullable(columns.into_iter().map(|(name, array)| (name, array, false)))
        .unwrap();
    let _ = ctx.deregister_table(JSON_TABLE);
    ctx.register_batch(JSON_TABLE, batch).unwrap();
}

/// A single-row array holding `text` in the given encoding.
fn string_array(text: &str, encoding: &DataType) -> ArrayRef {
    match encoding {
        DataType::Utf8 => Arc::new(StringArray::from(vec![text])),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![text])),
        DataType::Utf8View => Arc::new(StringViewArray::from(vec![text])),
        DataType::Dictionary(key, value) if **key == DataType::Int32 => Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0]),
            string_array(text, value),
        )),
        other => panic!("unsupported string encoding {other}"),
    }
}

/// Every string encoding a JSON document, or a key in a path column, can arrive in.
fn string_encodings() -> Vec<DataType> {
    let plain = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
    let dicts = plain
        .clone()
        .map(|value| DataType::Dictionary(Box::new(DataType::Int32), Box::new(value)));
    plain.into_iter().chain(dicts).collect()
}

fn dict_encoding() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// Run a single-row query, formatting the result with its exact type. `Err` carries only the
/// fact that the query failed, so the properties are not brittle about error text.
async fn outcome(ctx: &SessionContext, sql: &str) -> std::result::Result<String, String> {
    let (data_type, value) = run_single(ctx, sql).await?;
    Ok(format!("{data_type}={value}"))
}

/// Like [`outcome`], but with any dictionary wrapper stripped from the type.
async fn logical_outcome(ctx: &SessionContext, sql: &str) -> std::result::Result<String, String> {
    let (data_type, value) = run_single(ctx, sql).await?;
    Ok(format!("{}={value}", logical_type(&data_type)))
}

async fn run_single(ctx: &SessionContext, sql: &str) -> std::result::Result<(DataType, String), String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    let batch = &batches[0];
    let column = batch.column(0);
    let options = FormatOptions::default().with_display_error(true);
    let formatter = ArrayFormatter::try_new(column.as_ref(), &options).map_err(|e| e.to_string())?;
    let value = formatter.value(0).try_to_string().map_err(|e| e.to_string())?;
    Ok((batch.schema().field(0).data_type().clone(), value))
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

/// One step of a lookup path.
#[derive(Debug)]
enum Step {
    Key(String),
    Index(i64),
}

impl Step {
    /// The step as a SQL literal.
    fn literal(&self) -> String {
        match self {
            Step::Key(key) => format!("'{key}'"),
            Step::Index(index) => index.to_string(),
        }
    }
}

/// A string key or an array index.
fn step() -> impl Strategy<Value = Step> {
    prop_oneof!["[a-c]{1,2}".prop_map(Step::Key), (0i64..4).prop_map(Step::Index)]
}

/// A lookup path: string keys and array indices, rendered as SQL arguments.
fn path() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(step().prop_map(|step| step.literal()), 0..3)
}

/// A single lookup step held in a single-row column: a key in any string encoding, or an index
/// as either integer type the UDFs accept, plain or dictionary-encoded.
fn column_step() -> impl Strategy<Value = (Step, ArrayRef)> {
    (step(), prop::sample::select(string_encodings()), any::<(bool, bool)>()).prop_map(
        |(step, key_encoding, (unsigned, dictionary))| {
            let column: ArrayRef = match &step {
                Step::Key(key) => string_array(key, &key_encoding),
                Step::Index(index) => {
                    let values: ArrayRef = if unsigned {
                        Arc::new(UInt64Array::from(vec![index.unsigned_abs()]))
                    } else {
                        Arc::new(Int64Array::from(vec![*index]))
                    };
                    if dictionary {
                        Arc::new(DictionaryArray::<Int32Type>::new(Int32Array::from(vec![0]), values))
                    } else {
                        values
                    }
                }
            };
            (step, column)
        },
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
/// through its array path. The two must not disagree, down to the exact result type.
///
/// Both are checked as plain strings and dictionary-encoded, since a dictionary argument may make
/// the result a dictionary too, and each path has to decide that the same way `return_type` did.
#[test]
fn prop_scalar_and_array_paths_agree() {
    let rt = runtime();
    let ctx = create_context().unwrap();

    proptest!(config(96), |(json in json_text(), path in path())| {
        let literal = format!("'{}'", json.replace('\'', "''"));
        let dict_literal = format!("arrow_cast({literal}, '{}')", dict_encoding());
        for (encoding, literal) in [(DataType::Utf8View, literal), (dict_encoding(), dict_literal)] {
            set_doc(&ctx, &json, &encoding);
            for func in PATH_FUNCS {
                if !callable(func, &path) {
                    continue;
                }
                let from_column = format!("select {} as v from {JSON_TABLE}", call(func, JSON_COLUMN, &path));
                let from_literal = format!("select {} as v", call(func, &literal, &path));
                let column = rt.block_on(outcome(&ctx, &from_column));
                let scalar = rt.block_on(outcome(&ctx, &from_literal));
                prop_assert_eq!(&column, &scalar,
                    "\n  column in {}: {} => {:?}\n  literal: {} => {:?}\n  json: {:?}\n",
                    encoding, from_column, column, from_literal, scalar, json);
            }
        }
    });
}

/// The string encodings a JSON column can arrive in are separate code paths, and they must all
/// produce the same answer.
///
/// A lookup path held in a column rather than written as literals takes separate code again
/// (`invoke_array_array` rather than `invoke_array_scalars`, with a branch per dictionary value
/// type), so every encoding is also checked with a single step in a column, against the same
/// step written as a literal.
///
/// Dictionary-encoded input may keep its encoding in the output, so the dictionary wrapper is
/// stripped before comparing types. `json_get` is left out: it returns the JSON union, which
/// has no meaningful text form to compare — `json_as_text` and `json_get_json` cover the same
/// scanning code and do return text.
#[test]
fn prop_input_encoding_does_not_change_result() {
    let rt = runtime();
    let ctx = create_context().unwrap();

    proptest!(config(48), |(json in json_text(), path in path(), (step, step_column) in column_step())| {
        // the baselines are a `Utf8View` document with the path written as literals
        set_doc(&ctx, &json, &DataType::Utf8View);
        let mut cases = Vec::new();
        for func in PATH_FUNCS.iter().filter(|func| **func != "json_get") {
            let sql = callable(func, &path)
                .then(|| format!("select {} as v from {JSON_TABLE}", call(func, JSON_COLUMN, &path)));
            let baseline = sql.as_ref().map(|sql| rt.block_on(logical_outcome(&ctx, sql)));
            let from_column = format!("select {} as v from {JSON_TABLE}", call(func, JSON_COLUMN, &[PATH_COLUMN.to_string()]));
            let from_literal = format!("select {} as v from {JSON_TABLE}", call(func, JSON_COLUMN, &[step.literal()]));
            let step_baseline = rt.block_on(logical_outcome(&ctx, &from_literal));
            cases.push((sql.zip(baseline), from_column, from_literal, step_baseline));
        }

        for encoding in string_encodings() {
            set_table(&ctx, vec![
                (JSON_COLUMN, string_array(&json, &encoding)),
                (PATH_COLUMN, step_column.clone()),
            ]);
            for (literal_path, from_column, from_literal, step_baseline) in &cases {
                if let Some((sql, baseline)) = literal_path {
                    let got = rt.block_on(logical_outcome(&ctx, sql));
                    prop_assert_eq!(&got, baseline,
                        "\n  {} in {} => {:?}\n  in Utf8View => {:?}\n  json: {:?}\n",
                        sql, encoding, got, baseline, json);
                }
                let got = rt.block_on(logical_outcome(&ctx, from_column));
                prop_assert_eq!(&got, step_baseline,
                    "\n  {} in {} with {} = {:?} as {} => {:?}\n  {} in Utf8View => {:?}\n  json: {:?}\n",
                    from_column, encoding, PATH_COLUMN, step, step_column.data_type(), got,
                    from_literal, step_baseline, json);
            }
        }
    });
}
