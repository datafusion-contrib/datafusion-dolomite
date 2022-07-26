use anyhow::bail;
use datafusion::prelude::JoinType;

use crate::error::OptResult;
use crate::operator::LogicalOperator::LogicalJoin;
use crate::operator::Operator;
use crate::operator::Operator::{Logical, Physical};
use crate::operator::PhysicalOperator::PhysicalHashJoin;
use crate::optimizer::Optimizer;
use crate::rules::RuleId::Join2HashJoin;
use crate::rules::RulePromise::High;
use crate::rules::{
    pattern, OptExpression, PatterBuilder, Pattern, Rule, RuleId, RulePromise, RuleResult,
};

#[rustfmt::skip::macros(lazy_static)]
lazy_static! {
    static ref COMMUTATE_JOIN_RULE_PATTERN: Pattern = {
        pattern(CommutateJoinRule::matches)
        .finish()
    };
    static ref JOIN_TO_HASH_JOIN_RULE_PATTERN: Pattern = {
        pattern(Join2HashJoinRule::matches)
        .finish()
    };
}

/// Commutate inner join inputs.
#[derive(Clone)]
pub struct CommutateJoinRule {}

impl CommutateJoinRule {
    pub fn new() -> Self {
        Self {}
    }

    fn matches(op: &Operator) -> bool {
        match op {
            Logical(LogicalJoin(join)) => matches!(join.join_type(), JoinType::Inner),
            _ => false,
        }
    }
}

impl Rule for CommutateJoinRule {
    fn apply<O: Optimizer>(
        &self,
        input: OptExpression<O>,
        _ctx: &O,
        result: &mut RuleResult<O>,
    ) -> OptResult<()> {
        let op = input.get_operator(&_ctx)?.clone();
        let ret = OptExpression::with_operator(op, vec![input[1].clone(), input[0].clone()]);
        result.add(ret);
        Ok(())
    }

    fn pattern(&self) -> &Pattern {
        &COMMUTATE_JOIN_RULE_PATTERN
    }

    fn rule_id(&self) -> RuleId {
        RuleId::CommutateJoin
    }

    fn rule_promise(&self) -> RulePromise {
        High
    }
}

/// Transforms equi join to hash join.
#[derive(Clone)]
pub struct Join2HashJoinRule {}

impl Join2HashJoinRule {
    pub fn new() -> Self {
        Self {}
    }

    fn matches(op: &Operator) -> bool {
        match op {
            Logical(LogicalJoin(_)) => true,
            _ => false,
        }
    }
}

impl Rule for Join2HashJoinRule {
    fn apply<O: Optimizer>(
        &self,
        input: OptExpression<O>,
        _ctx: &O,
        result: &mut RuleResult<O>,
    ) -> OptResult<()> {
        if let Logical(LogicalJoin(join)) = input.get_operator(&_ctx)? {
            let hash_join_op = Physical(PhysicalHashJoin(join.clone()));
            let ret = input.clone_with_inputs(hash_join_op);

            result.add(ret);
            Ok(())
        } else {
            bail!("Miss matched pattern")
        }
    }

    fn pattern(&self) -> &Pattern {
        &JOIN_TO_HASH_JOIN_RULE_PATTERN
    }

    fn rule_id(&self) -> RuleId {
        Join2HashJoin
    }

    fn rule_promise(&self) -> RulePromise {
        High
    }
}
