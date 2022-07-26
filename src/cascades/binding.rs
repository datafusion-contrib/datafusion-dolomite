use std::rc::Rc;
use std::vec::IntoIter;

use itertools::Itertools;

use crate::cascades::memo::Memo;
use crate::cascades::{CascadesOptimizer, GroupExprId};

use crate::rules::{OptExpression, Pattern};

type OptExpr = OptExpression<CascadesOptimizer>;

#[derive(Clone)]
pub(super) struct Binding<'a, 'b> {
    group_expr_ids: Rc<Vec<GroupExprId>>,
    memo: &'a Memo,
    pattern: &'b Pattern,
}

pub(super) struct BindingIterator<'a, 'b, I> {
    binding: Binding<'a, 'b>,
    iter: I,
}

impl<'a, 'b> Clone for BindingIterator<'a, 'b, IntoIter<OptExpr>> {
    fn clone(&self) -> Self {
        let iter = self.binding.clone().bind();
        Self {
            binding: self.binding.clone(),
            iter,
        }
    }
}

impl<'a, 'b, I> Iterator for BindingIterator<'a, 'b, I>
where
    I: Iterator<Item = OptExpr>,
{
    type Item = OptExpr;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, 'b> IntoIterator for Binding<'a, 'b> {
    type IntoIter = BindingIterator<'a, 'b, IntoIter<OptExpr>>;
    type Item = OptExpr;

    fn into_iter(self) -> Self::IntoIter {
        let iter = self.clone().bind();
        BindingIterator {
            binding: self,
            iter,
        }
    }
}

impl<'a, 'b> Binding<'a, 'b> {
    pub(super) fn new(group_expr_id: GroupExprId, pattern: &'b Pattern, memo: &'a Memo) -> Self {
        Self {
            group_expr_ids: Rc::new(vec![group_expr_id]),
            memo,
            pattern,
        }
    }

    fn bind(self) -> IntoIter<OptExpr> {
        let matched_group_ids: Vec<GroupExprId> = {
            self.group_expr_ids
                .iter()
                .filter(|group_expr_id| {
                    self.memo[**group_expr_id].matches_without_children(&self.pattern)
                })
                .map(|g| *g)
                .collect()
        };

        if let Some(children_patterns) = &self.pattern.children {
            matched_group_ids
                .into_iter()
                .flat_map(move |group_expr_id| {
                    let logical_group_expr = &self.memo[group_expr_id];
                    let children_bindings = children_patterns
                        .iter()
                        .zip(logical_group_expr.input_group_ids())
                        .map(|(pattern, group_id)| Binding {
                            group_expr_ids: Rc::new(self.memo[group_id].logical_group_expr_ids()),
                            pattern,
                            memo: self.memo,
                        })
                        .multi_cartesian_product();

                    children_bindings
                        .into_iter()
                        .map(move |inputs| OptExpression::with_expr_handle(group_expr_id, inputs))
                })
                .collect::<Vec<OptExpr>>()
                .into_iter()
        } else {
            matched_group_ids
                .into_iter()
                .map(|group_expr_id| {
                    OptExpression::with_expr_handle(
                        group_expr_id,
                        self.memo[group_expr_id]
                            .inputs()
                            .iter()
                            .map(|group_id| OptExpr::with_group_handle(*group_id))
                            .collect::<Vec<OptExpr>>(),
                    )
                })
                .collect::<Vec<OptExpr>>()
                .into_iter()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cascades::binding::Binding;
    use crate::cascades::memo::Memo;
    use crate::cascades::CascadesOptimizer;
    use crate::operator::LogicalOperator::{
        LogicalJoin, LogicalLimit, LogicalProjection, LogicalScan,
    };
    use crate::operator::Operator::Logical;
    use crate::operator::{Join, Limit, Projection, TableScan};
    use crate::optimizer::OptimizerContext;
    use crate::plan::{LogicalPlanBuilder, Plan};
    use crate::properties::PhysicalPropertySet;
    use crate::rules::OptExprNode::GroupHandleNode;
    use crate::rules::{any, pattern, OptExpression, PatterBuilder};
    use datafusion::logical_expr::Operator::Eq;
    use datafusion::logical_plan::binary_expr;
    use datafusion::prelude::{col, JoinType};

    fn create_optimizer(plan: Plan) -> CascadesOptimizer {
        CascadesOptimizer {
            required_prop: PhysicalPropertySet::default(),
            rules: vec![],
            memo: Memo::from(plan),
            context: OptimizerContext::default(),
        }
    }

    #[test]
    fn test_bind_one() {
        let plan = LogicalPlanBuilder::new()
            .scan(None, "t1")
            .projection(vec![col("c1")])
            .limit(10)
            .build();

        let optimizer = create_optimizer(plan);

        let table_scan_pattern = pattern(|op| matches!(op, Logical(LogicalLimit(_))))
            .leaf(any)
            .finish();

        let root_group_expr_id =
            optimizer.memo[optimizer.memo.root_group_id()].logical_group_expr_ids()[0];

        let mut bindings =
            Binding::new(root_group_expr_id, &table_scan_pattern, &optimizer.memo).into_iter();

        // First binding
        {
            let opt_expr = bindings.next().unwrap();
            assert_eq!(
                &Logical(LogicalLimit(Limit::new(10))),
                opt_expr.get_operator(&optimizer).unwrap(),
            );

            assert_eq!(1, opt_expr.inputs().len());
            assert_eq!(
                &Logical(LogicalProjection(Projection::new(vec![col("c1")]))),
                opt_expr[0].get_operator(&optimizer).unwrap()
            );

            assert_eq!(1, opt_expr[0].inputs().len());
            assert!(matches!(opt_expr[0][0].node(), GroupHandleNode(_)));
        }

        // No second binding
        {
            assert!(bindings.next().is_none());
        }
    }

    #[test]
    fn test_bind_multi() {
        let plan = {
            let mut builder = LogicalPlanBuilder::new();
            let right = builder
                .scan(None, "t2")
                .projection(vec![col("c2")])
                .build()
                .root();

            builder
                .scan(None, "t1")
                .projection(vec![col("c1")] )
                .limit(10)
                .join(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2")),
                    right,
                )
                .limit(7)
                .build()
        };

        let mut optimizer = create_optimizer(plan);

        // Currently root group has only one group expr
        let root_group_expr_id =
            optimizer.memo[optimizer.memo.root_group_id()].logical_group_expr_ids()[0];

        // insert equal plan
        {
            let left_child = OptExpression::<CascadesOptimizer>::from(Logical(LogicalScan(
                TableScan::new("t2"),
            )));
            let right_child = OptExpression::<CascadesOptimizer>::from(Logical(LogicalScan(
                TableScan::new("t1"),
            )));
            let opt_expr = OptExpression::with_operator(
                Logical(LogicalJoin(Join::new(
                    JoinType::Inner,
                    binary_expr(col("t2.c2"), Eq, col("t1.c1")),
                ))),
                vec![left_child, right_child],
            );

            let join_group_id = optimizer.memo[root_group_expr_id].inputs()[0];

            // Insert alternative plan to join
            optimizer
                .memo
                .insert_opt_expression(&opt_expr, Some(join_group_id));
        }

        let table_scan_pattern = pattern(|op| matches!(op, Logical(LogicalLimit(_))))
            .leaf(|op| matches!(op, Logical(LogicalJoin(_))))
            .finish();

        let mut bindings =
            Binding::new(root_group_expr_id, &table_scan_pattern, &optimizer.memo).into_iter();

        // First binding
        {
            let opt_expr = bindings.next().unwrap();
            assert_eq!(
                &Logical(LogicalLimit(Limit::new(7))),
                opt_expr.get_operator(&optimizer).unwrap()
            );

            assert_eq!(1, opt_expr.inputs().len());

            assert_eq!(
                &Logical(LogicalJoin(Join::new(
                    JoinType::Inner,
                    binary_expr(col("t1.c1"), Eq, col("t2.c2"))
                ))),
                opt_expr[0].get_operator(&optimizer).unwrap()
            );

            assert_eq!(2, opt_expr[0].inputs().len());
            assert!(opt_expr[0]
                .inputs()
                .iter()
                .all(|expr| matches!(expr.node(), GroupHandleNode(_))));
        }

        // Second binding
        {
            let opt_expr = bindings.next().unwrap();
            assert_eq!(
                &Logical(LogicalLimit(Limit::new(7))),
                opt_expr.get_operator(&optimizer).unwrap()
            );

            assert_eq!(1, opt_expr.inputs().len());

            assert_eq!(
                &Logical(LogicalJoin(Join::new(
                    JoinType::Inner,
                    binary_expr(col("t2.c2"), Eq, col("t1.c1"))
                ))),
                opt_expr[0].get_operator(&optimizer).unwrap()
            );
            assert_eq!(2, opt_expr[0].inputs().len());
            assert!(opt_expr[0]
                .inputs()
                .iter()
                .all(|expr| matches!(expr.node(), GroupHandleNode(_))));
        }

        // No second binding
        {
            assert!(bindings.next().is_none());
        }
    }
}
