use crate::error::DolomiteResult;
use crate::plan::PlanNodeRef;

pub trait Visitor {
    /// Context
    type C;
    type R;
    fn visit(&self, context: Self::C, node: PlanNodeRef) -> DolomiteResult<Self::R>;
}

pub fn visit<V>(visitor: V, context: V::C, root: PlanNodeRef) -> DolomiteResult<V::R>
where
    V: Visitor,
{
    visitor.visit(context, root)
}
