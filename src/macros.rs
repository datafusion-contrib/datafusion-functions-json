#[allow(clippy::doc_markdown)]
/// Currently copied verbatim, can hopefully be replaced or simplified
/// https://github.com/apache/datafusion/blob/19356b26f515149f96f9b6296975a77ac7260149/datafusion/functions-array/src/macros.rs
///
/// Creates external API functions for an array UDF. Specifically, creates
///
/// 1. Single `ScalarUDF` instance
///
/// Creates a singleton `ScalarUDF` of the `$UDF` function named `$GNAME` and a
/// function named `$NAME` which returns that function named $NAME.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
///
/// # 2. `expr_fn` style function
///
/// These are functions that create an `Expr` that invokes the UDF, used
/// primarily to programmatically create expressions.
///
/// For example:
/// ```text
/// pub fn array_to_string(delimiter: Expr) -> Expr {
/// ...
/// }
/// ```
/// # Arguments
/// * `UDF`: name of the [`ScalarUDFImpl`]
/// * `EXPR_FN`: name of the `expr_fn` function to be created
/// * `arg`: 0 or more named arguments for the function
/// * `DOC`: documentation string for the function
/// * `SCALAR_UDF_FUNC`: name of the function to create (just) the `ScalarUDF`
/// * `GNAME`: name for the single static instance of the `ScalarUDF`
///
/// [`ScalarUDFImpl`]: datafusion_expr::ScalarUDFImpl
macro_rules! make_udf_function {
    ($UDF:ty, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr , $SCALAR_UDF_FN:ident) => {
        paste::paste! {
            // "fluent expr_fn" style function
            #[doc = $DOC]
            #[must_use] pub fn $EXPR_FN($($arg: datafusion_expr::Expr),*) -> datafusion_expr::Expr {
                datafusion_expr::Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                    $SCALAR_UDF_FN(),
                    vec![$($arg),*],
                ))
            }

            /// Singleton instance of [`$UDF`], ensures the UDF is only created once
            /// named STATIC_$(UDF). For example `STATIC_ArrayToString`
            #[allow(non_upper_case_globals)]
            static [< STATIC_ $UDF >]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::ScalarUDF>> =
                std::sync::OnceLock::new();

            /// ScalarFunction that returns a [`ScalarUDF`] for [`$UDF`]
            ///
            /// [`ScalarUDF`]: datafusion_expr::ScalarUDF
            pub fn $SCALAR_UDF_FN() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
                [< STATIC_ $UDF >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
                            <$UDF>::new(),
                        ))
                    })
                    .clone()
            }
        }
    };
    ($UDF:ty, $EXPR_FN:ident, $DOC:expr , $SCALAR_UDF_FN:ident) => {
        paste::paste! {
            // "fluent expr_fn" style function
            #[doc = $DOC]
            pub fn $EXPR_FN(arg: Vec<Expr>) -> datafusion_expr::Expr {
                datafusion_expr::Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
                    $SCALAR_UDF_FN(),
                    arg,
                ))
            }

            /// Singleton instance of [`$UDF`], ensures the UDF is only created once
            /// named STATIC_$(UDF). For example `STATIC_ArrayToString`
            #[allow(non_upper_case_globals)]
            static [< STATIC_ $UDF >]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::ScalarUDF>> =
                std::sync::OnceLock::new();
            /// ScalarFunction that returns a [`ScalarUDF`] for [`$UDF`]
            ///
            /// [`ScalarUDF`]: datafusion_expr::ScalarUDF
            pub fn $SCALAR_UDF_FN() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
                [< STATIC_ $UDF >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
                            <$UDF>::new(),
                        ))
                    })
                    .clone()
            }
        }
    };
}

pub(crate) use make_udf_function;
