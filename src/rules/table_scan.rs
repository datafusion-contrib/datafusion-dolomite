use anyhow::bail;

use crate::error::OptResult;
use crate::operator::LogicalOperator::LogicalScan;
use crate::operator::Operator::{Logical, Physical};
use crate::operator::PhysicalOperator::PhysicalTableScan as PTableScan;
use crate::optimizer::Optimizer;
use crate::rules::RuleId::Scan2TableScan;
use crate::rules::{OptExpression, Pattern, Rule, RuleId, RulePromise, RuleResult};

#[rustfmt::skip::macros(lazy_static)]
lazy_static! {
    static ref SCAN_TO_TABLESCAN_RULE_PATTERN: Pattern = {
        Pattern::new_leaf(|op| {
            matches!(op, Logical(LogicalScan(_)))
        })
    };
}

/// Table scan implementation rule.
#[derive(Clone)]
pub struct Scan2TableScanRule {}

impl Scan2TableScanRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Rule for Scan2TableScanRule {
    fn apply<O: Optimizer>(
        &self,
        input: OptExpression<O>,
        _ctx: &O,
        result: &mut RuleResult<O>,
    ) -> OptResult<()> {
        if let Logical(LogicalScan(t)) = input.get_operator(&_ctx)? {
            let new_op = Physical(PTableScan(t.clone()));
            result.add(OptExpression::from(new_op));
            Ok(())
        } else {
            bail!("Pattern mismatch")
        }
    }

    fn pattern(&self) -> &Pattern {
        &SCAN_TO_TABLESCAN_RULE_PATTERN
    }

    fn rule_id(&self) -> RuleId {
        Scan2TableScan
    }

    fn rule_promise(&self) -> RulePromise {
        RulePromise::High
    }
}
