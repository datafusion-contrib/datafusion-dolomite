/// Conversion between dolomite plan and datafusion plan.
mod logical;

use anyhow::bail;
use datafusion::common::Column;
use datafusion::prelude::Expr;
use dolomite::error::DolomiteResult;
pub use logical::*;
mod physical;
pub use physical::*;

use datafusion::logical_plan::Operator as DFOperator;
use datafusion::prelude::Expr::Column as ExprColumn;
pub(in crate::conversion) fn expr_to_df_join_condition(
    expr: &Expr,
) -> DolomiteResult<Vec<(Column, Column)>> {
    match expr {
        Expr::BinaryExpr { left, op, right } if matches!(op, DFOperator::Eq) => {
            match (&**left, &**right) {
        (ExprColumn(left_col), ExprColumn(right_col)) => Ok(vec![(left_col.clone(), right_col
            .clone()
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
