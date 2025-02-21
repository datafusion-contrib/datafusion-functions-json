use log::debug;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::ScalarUDF;

mod common;
mod common_macros;
mod common_union;
mod json_as_text;
mod json_contains;
mod json_get;
mod json_get_bool;
mod json_get_float;
mod json_get_int;
mod json_get_json;
mod json_get_str;
mod json_length;
mod json_object_keys;
mod rewrite;

pub use common_union::{JsonUnionEncoder, JsonUnionValue};

pub mod functions {
    pub use crate::json_as_text::{json_as_text, json_as_text_recursive_sorted, json_as_text_top_level_sorted};
    pub use crate::json_contains::{json_contains, json_contains_recursive_sorted, json_contains_top_level_sorted};
    pub use crate::json_get::{json_get, json_get_recursive_sorted, json_get_top_level_sorted};
    pub use crate::json_get_bool::{json_get_bool, json_get_bool_recursive_sorted, json_get_bool_top_level_sorted};
    pub use crate::json_get_float::{json_get_float, json_get_float_recursive_sorted, json_get_float_top_level_sorted};
    pub use crate::json_get_int::{json_get_int, json_get_int_recursive_sorted, json_get_int_top_level_sorted};
    pub use crate::json_get_json::{json_get_json, json_get_json_recursive_sorted, json_get_json_top_level_sorted};
    pub use crate::json_get_str::{json_get_str, json_get_str_recursive_sorted, json_get_str_top_level_sorted};
    pub use crate::json_length::{json_length, json_length_recursive_sorted, json_length_top_level_sorted};
    pub use crate::json_object_keys::{json_keys_recursive_sorted, json_keys_sorted, json_object_keys};
}

pub mod udfs {
    pub use crate::json_as_text::{
        json_as_text_recursive_sorted_udf, json_as_text_top_level_sorted_udf, json_as_text_udf,
    };
    pub use crate::json_contains::{
        json_contains_recursive_sorted_udf, json_contains_top_level_sorted_udf, json_contains_udf,
    };
    pub use crate::json_get::{json_get_recursive_sorted_udf, json_get_top_level_sorted_udf, json_get_udf};
    pub use crate::json_get_bool::{
        json_get_bool_recursive_sorted_udf, json_get_bool_top_level_sorted_udf, json_get_bool_udf,
    };
    pub use crate::json_get_float::{
        json_get_float_recursive_sorted_udf, json_get_float_top_level_sorted_udf, json_get_float_udf,
    };
    pub use crate::json_get_int::{
        json_get_int_recursive_sorted_udf, json_get_int_top_level_sorted_udf, json_get_int_udf,
    };
    pub use crate::json_get_json::{
        json_get_json_recursive_sorted_udf, json_get_json_top_level_sorted_udf, json_get_json_udf,
    };
    pub use crate::json_get_str::{
        json_get_str_recursive_sorted_udf, json_get_str_top_level_sorted_udf, json_get_str_udf,
    };
    pub use crate::json_length::{json_length_recursive_sorted_udf, json_length_top_level_sorted_udf, json_length_udf};
    pub use crate::json_object_keys::{json_keys_recursive_sorted_udf, json_keys_sorted_udf, json_object_keys_udf};
}

/// Register all JSON UDFs, and [`rewrite::JsonFunctionRewriter`] with the provided [`FunctionRegistry`].
///
/// # Arguments
///
/// * `registry`: `FunctionRegistry` to register the UDFs
///
/// # Errors
///
/// Returns an error if the UDFs cannot be registered or if the rewriter cannot be registered.
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        json_get::json_get_udf(),
        json_get::json_get_top_level_sorted_udf(),
        json_get::json_get_recursive_sorted_udf(),
        json_get_bool::json_get_bool_udf(),
        json_get_bool::json_get_bool_top_level_sorted_udf(),
        json_get_bool::json_get_bool_recursive_sorted_udf(),
        json_get_float::json_get_float_udf(),
        json_get_float::json_get_float_top_level_sorted_udf(),
        json_get_float::json_get_float_recursive_sorted_udf(),
        json_get_int::json_get_int_udf(),
        json_get_int::json_get_int_top_level_sorted_udf(),
        json_get_int::json_get_int_recursive_sorted_udf(),
        json_get_json::json_get_json_udf(),
        json_get_json::json_get_json_top_level_sorted_udf(),
        json_get_json::json_get_json_recursive_sorted_udf(),
        json_as_text::json_as_text_udf(),
        json_as_text::json_as_text_top_level_sorted_udf(),
        json_as_text::json_as_text_recursive_sorted_udf(),
        json_get_str::json_get_str_udf(),
        json_get_str::json_get_str_top_level_sorted_udf(),
        json_get_str::json_get_str_recursive_sorted_udf(),
        json_contains::json_contains_udf(),
        json_contains::json_contains_top_level_sorted_udf(),
        json_contains::json_contains_recursive_sorted_udf(),
        json_length::json_length_udf(),
        json_length::json_length_top_level_sorted_udf(),
        json_length::json_length_recursive_sorted_udf(),
        json_object_keys::json_object_keys_udf(),
        json_object_keys::json_keys_sorted_udf(),
        json_object_keys::json_keys_recursive_sorted_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    registry.register_function_rewrite(Arc::new(rewrite::JsonFunctionRewriter))?;
    registry.register_expr_planner(Arc::new(rewrite::JsonExprPlanner))?;

    Ok(())
}
