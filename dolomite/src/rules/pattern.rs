use crate::operator::Operator;
use crate::utils::RootBuilder;

pub type OperatorMatcher = fn(&Operator) -> bool;

/// A pattern defines how to match a sub tree of a plan.
///
/// If we want to match `Join(Filter, Scan)` pattern, the pattern tree should be defined like:
/// ```
/// use dolomite::operator::LogicalOperator::LogicalJoin;
/// use dolomite::operator::LogicalOperator::LogicalLimit;
/// use dolomite::operator::LogicalOperator::LogicalScan;
/// use dolomite::operator::Operator::Logical;
/// use dolomite::rules::{any, Pattern};
/// use dolomite::utils::TreeBuilder;
///
/// Pattern::new_builder(|op| matches!(op, Logical(LogicalJoin(_))))
///   .begin_node(|op| matches!(op, Logical(LogicalLimit(_))))
///     .leaf_node(any)
///   .end()
///   .leaf_node(|op| matches!(op, Logical(LogicalScan(_))))
/// .end();
/// ```
///
/// The root node in pattern tree matches `Join` operator, the first child node matches
/// `Filter` operator, and the last matches `Scan`.
pub struct Pattern {
    /// Matches against an operator.
    pub predict: OperatorMatcher,
    /// `None` for leaf node.
    pub children: Option<Vec<Pattern>>,
}

impl From<(OperatorMatcher, Vec<Pattern>)> for Pattern {
    fn from(t: (OperatorMatcher, Vec<Pattern>)) -> Self {
        let children = if t.1.is_empty() { None } else { Some(t.1) };

        Self {
            predict: t.0,
            children,
        }
    }
}

impl Pattern {
    pub fn new_leaf(matcher: OperatorMatcher) -> Pattern {
        Pattern {
            predict: matcher,
            // This is a current limitation of our pattern matching algorithm, we will eliminate
            // later
            children: None,
        }
    }

    pub fn new<I: IntoIterator<Item = Pattern>>(
        matcher: OperatorMatcher,
        children: I,
    ) -> Pattern {
        let children = children.into_iter().collect::<Vec<Pattern>>();
        let children_pattern = if !children.is_empty() {
            Some(children)
        } else {
            None
        };

        Pattern {
            predict: matcher,
            children: children_pattern,
        }
    }

    pub fn new_builder(
        predict: OperatorMatcher,
    ) -> RootBuilder<Pattern, OperatorMatcher> {
        RootBuilder::new(predict)
    }
}

pub fn any(_: &Operator) -> bool {
    true
}
