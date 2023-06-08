/// Conversion between dolomite plan and datafusion plan.
mod logical;

use anyhow::bail;
use datafusion::common::Column;
use datafusion::prelude::Expr;
use dolomite::error::DolomiteResult;
pub use logical::*;
mod physical;
pub use physical::*;

use datafusion::prelude::Expr::Column as ExprColumn;
use datafusion_expr::{BinaryExpr, Operator as DFOperator};
pub(in crate::conversion) fn expr_to_df_join_condition(
    expr: &Expr,
) -> DolomiteResult<Vec<(Expr, Expr)>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })  if matches!(op, DFOperator::Eq) => {
            match (&**left, &**right) {
        (ExprColumn(left_col), ExprColumn(right_col)) => Ok(vec![(ExprColumn(left_col.clone()), ExprColumn(right_col.clone())
        )]),
        _ => bail!("Unsupported join condition to convert to datafusion join condition: {:?}",
          expr)
      }
        }
        _ => bail!(
            "Unsupported join condition to convert to datafusion join condition: {:?}",
            expr
        ),
    }
}
