use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

mod common;
mod common_macros;
mod common_union;
mod json_contains;
mod json_get;
mod json_get_bool;
mod json_get_float;
mod json_get_int;
mod json_get_json;
mod json_get_str;
mod json_length;
mod rewrite;

pub mod functions {
    pub use crate::json_contains::json_contains;
    pub use crate::json_get::json_get;
    pub use crate::json_get_bool::json_get_bool;
    pub use crate::json_get_float::json_get_float;
    pub use crate::json_get_int::json_get_int;
    pub use crate::json_get_json::json_get_json;
    pub use crate::json_get_str::json_get_str;
    pub use crate::json_length::json_length;
}

pub mod udfs {
    pub use crate::json_contains::json_contains_udf;
    pub use crate::json_get::json_get_udf;
    pub use crate::json_get_bool::json_get_bool_udf;
    pub use crate::json_get_float::json_get_float_udf;
    pub use crate::json_get_int::json_get_int_udf;
    pub use crate::json_get_json::json_get_json_udf;
    pub use crate::json_get_str::json_get_str_udf;
    pub use crate::json_length::json_length_udf;
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
        json_get_bool::json_get_bool_udf(),
        json_get_float::json_get_float_udf(),
        json_get_int::json_get_int_udf(),
        json_get_json::json_get_json_udf(),
        json_get_str::json_get_str_udf(),
        json_contains::json_contains_udf(),
        json_length::json_length_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    registry.register_function_rewrite(Arc::new(rewrite::JsonFunctionRewriter))?;
    registry.register_parse_custom_operator(Arc::new(rewrite::JsonOperatorParser))?;

    Ok(())
}
