use crate::error::DolomiteResult;
use crate::plan::{Plan, PlanNodeRef};
use datafusion::logical_plan::Subquery;
use datafusion::prelude::Expr;

pub(in crate::rules::subquery) struct Decorrelator {}

enum SubqueryKind {
    Exists { negate: bool },
    In { negate: bool },
}

struct SubqueryInfo {
    kind: SubqueryKind,
    subquery: PlanNodeRef,
    correlated: bool,
}

impl Decorrelator {
    pub fn plan_subqueries(
        &self,
        _exprs: &[Expr],
        _root: PlanNodeRef,
    ) -> DolomiteResult<PlanNodeRef> {
        todo!()
    }

    fn plan_subquery(
        &self,
        _subquery_info: SubqueryInfo,
        _root_plan: PlanNodeRef,
    ) -> DolomiteResult<(Expr, PlanNodeRef)> {
        todo!()
    }
}
