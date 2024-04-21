use std::sync::Arc;
use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::ScalarUDF;
use log::debug;

mod json_obj_contains;
mod macros;

pub mod functions {
    pub use crate::json_obj_contains::json_obj_contains;
}

pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        json_obj_contains::json_obj_contains_udf(),
    ];
    functions.into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;

    Ok(())
}
