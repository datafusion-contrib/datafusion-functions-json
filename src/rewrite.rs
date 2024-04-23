use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::Expr;

pub(crate) struct JsonFunctionRewriter;

impl FunctionRewrite for JsonFunctionRewriter {
    fn name(&self) -> &str {
        "JsonFunctionRewriter"
    }

    fn rewrite(&self, expr: Expr, _schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        // TODO: Implement the rewrite logic
        Ok(Transformed::no(expr))
    }
}
