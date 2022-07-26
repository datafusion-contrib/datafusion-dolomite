use crate::heuristic::{HepNodeId, HepOptimizer};
use crate::optimizer::{OptExpr, Optimizer};
use crate::rules::{OptExpression, Pattern};

pub(super) struct Binding<'a, 'b> {
    expr_handle: HepNodeId,
    pattern: &'a Pattern,
    optimizer: &'b HepOptimizer,
}

impl<'a, 'b> Binding<'a, 'b> {
    pub(super) fn new(
        expr_handle: HepNodeId,
        pattern: &'a Pattern,
        optimizer: &'b HepOptimizer,
    ) -> Self {
        Self {
            expr_handle,
            pattern,
            optimizer,
        }
    }

    pub(super) fn next(self) -> Option<OptExpression<HepOptimizer>> {
        let expr = self.optimizer.expr_at(self.expr_handle);
        if !(self.pattern.predict)(expr.operator()) {
            return None;
        }

        if let Some(children) = &self.pattern.children {
            if expr.inputs_len(self.optimizer) != children.len() {
                return None;
            }

            let mut inputs = Vec::with_capacity(children.len());
            for idx in 0..expr.inputs_len(self.optimizer) {
                if let Some(opt_input) = Binding::new(
                    expr.input_at(idx, self.optimizer),
                    &children[idx],
                    self.optimizer,
                )
                .next()
                {
                    inputs.push(opt_input);
                } else {
                    return None;
                }
            }

            Some(OptExpression::with_expr_handle(
                self.expr_handle.clone(),
                inputs,
            ))
        } else {
            // Collect leaf node's inputs
            let current_node = self.optimizer.expr_at(self.expr_handle);
            let inputs = (0..current_node.inputs_len(self.optimizer))
                .map(|input_idx| current_node.input_at(input_idx, self.optimizer))
                .map(|group_id| OptExpression::<HepOptimizer>::with_group_handle(group_id))
                .collect::<Vec<OptExpression<HepOptimizer>>>();
            Some(OptExpression::with_expr_handle(
                self.expr_handle.clone(),
                inputs,
            ))
        }
    }
}
