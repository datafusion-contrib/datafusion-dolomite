use crate::utils::OptimizerFactory;
use dolomite::heuristic::{HepOptimizer, MatchOrder};
use dolomite::optimizer::OptimizerContext;
use dolomite::plan::Plan;
use dolomite::rules::RuleImpl;

pub struct HepOptimizerFactory {
    match_order: MatchOrder,
    /// Max number of iteration
    max_iter_times: usize,
    rules: Vec<RuleImpl>,
    context: OptimizerContext,
}

impl OptimizerFactory for HepOptimizerFactory {
    type O = HepOptimizer;

    fn create(&self, plan: Plan) -> Self::O {
        HepOptimizer::new(
            self.match_order,
            self.max_iter_times,
            self.rules.clone(),
            plan,
            self.context.clone(),
        )
        .unwrap()
    }
}

pub struct HepOptimizerFactoryBuilder {
    match_order: MatchOrder,
    /// Max number of iteration
    max_iter_times: usize,
    rules: Vec<RuleImpl>,
    context: OptimizerContext,
}

impl HepOptimizerFactoryBuilder {
    pub fn with_optimizer_context(context: OptimizerContext) -> Self {
        Self {
            match_order: MatchOrder::TopDown,
            max_iter_times: usize::MAX,
            rules: vec![],
            context,
        }
    }

    pub fn add_rules(mut self, rules: impl IntoIterator<Item = RuleImpl>) -> Self {
        self.rules.extend(rules);
        self
    }

    pub fn build(self) -> HepOptimizerFactory {
        HepOptimizerFactory {
            match_order: self.match_order,
            max_iter_times: self.max_iter_times,
            rules: self.rules,
            context: self.context,
        }
    }
}
