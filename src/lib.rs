use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;
use std::sync::Arc;

mod common_get;
mod common_macros;
mod common_union;
mod json_get;
mod json_get_bool;
mod json_get_float;
mod json_get_int;
mod json_get_str;
mod json_obj_contains;
mod rewrite;

pub mod functions {
    pub use crate::json_get::json_get;
    pub use crate::json_get_bool::json_get_bool;
    pub use crate::json_get_float::json_get_float;
    pub use crate::json_get_int::json_get_int;
    pub use crate::json_get_str::json_get_str;
    pub use crate::json_obj_contains::json_obj_contains;
}

/// Register all JSON UDFs
pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        json_get::json_get_udf(),
        json_get_bool::json_get_bool_udf(),
        json_get_float::json_get_float_udf(),
        json_get_int::json_get_int_udf(),
        json_get_str::json_get_str_udf(),
        json_obj_contains::json_obj_contains_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    registry.register_function_rewrite(Arc::new(rewrite::JsonFunctionRewriter))?;

    Ok(())
}
