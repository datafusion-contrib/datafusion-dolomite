use crate::operator::Operator;

pub type OperatorMatcher = fn(&Operator) -> bool;

/// A pattern defines how to match a sub tree of a plan.
///
/// If we want to match `Join(Filter, Scan)` pattern, the pattern tree should be defined like:
/// ```
/// use dolomite::operator::LogicalOperator::LogicalJoin;
/// use dolomite::operator::LogicalOperator::LogicalLimit;
/// use dolomite::operator::LogicalOperator::LogicalScan;
/// use dolomite::operator::Operator::Logical;
/// use dolomite::rules::{any, pattern, PatterBuilder};
///
/// pattern(|op| matches!(op, Logical(LogicalJoin(_))))
///   .pattern(|op| matches!(op, Logical(LogicalLimit(_))))
///     .leaf(any)
///   .finish()
///   .leaf(|op| matches!(op, Logical(LogicalScan(_))))
/// .finish();
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
}

pub fn any(_: &Operator) -> bool {
    true
}

pub fn pattern(matcher: OperatorMatcher) -> RootPatternBuilder {
    RootPatternBuilder {
        matcher,
        inputs: vec![],
    }
}

pub trait PatterBuilder {
    type Child;
    type Output;
    fn pattern(self, matcher: OperatorMatcher) -> Self::Child;
    fn leaf(self, matcher: OperatorMatcher) -> Self;
    fn finish(self) -> Self::Output;
}

pub struct RootPatternBuilder {
    matcher: OperatorMatcher,
    inputs: Vec<Pattern>,
}

trait AddChild {
    fn add_child(&mut self, pattern: Pattern);
}

pub struct NonRootPatternBuilder<P> {
    parent_builder: P,
    matcher: OperatorMatcher,
    inputs: Vec<Pattern>,
}

impl<P: PatterBuilder + AddChild> PatterBuilder for NonRootPatternBuilder<P> {
    type Child = NonRootPatternBuilder<Self>;
    type Output = P;

    fn pattern(self, matcher: OperatorMatcher) -> NonRootPatternBuilder<Self> {
        NonRootPatternBuilder {
            parent_builder: self,
            matcher,
            inputs: vec![],
        }
    }

    fn leaf(mut self, matcher: OperatorMatcher) -> Self {
        let input = Pattern::new_leaf(matcher);
        self.inputs.push(input);
        self
    }

    fn finish(mut self) -> Self::Output {
        let pattern = Pattern::new(self.matcher, self.inputs);
        self.parent_builder.add_child(pattern);
        self.parent_builder
    }
}

impl<P> AddChild for NonRootPatternBuilder<P> {
    fn add_child(&mut self, pattern: Pattern) {
        self.inputs.push(pattern)
    }
}

impl PatterBuilder for RootPatternBuilder {
    type Child = NonRootPatternBuilder<Self>;
    type Output = Pattern;

    fn pattern(self, matcher: OperatorMatcher) -> Self::Child {
        NonRootPatternBuilder {
            parent_builder: self,
            matcher,
            inputs: vec![],
        }
    }

    fn leaf(mut self, matcher: OperatorMatcher) -> Self {
        let input = Pattern::new_leaf(matcher);
        self.inputs.push(input);
        self
    }

    fn finish(self) -> Self::Output {
        Pattern::new(self.matcher, self.inputs)
    }
}

impl AddChild for RootPatternBuilder {
    fn add_child(&mut self, pattern: Pattern) {
        self.inputs.push(pattern)
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::LogicalOperator::LogicalJoin;
    use crate::operator::LogicalOperator::LogicalLimit;
    use crate::operator::LogicalOperator::LogicalScan;
    use crate::rules::Operator::Logical;
    use crate::rules::{any, pattern, PatterBuilder};

    #[test]
    fn test() {
        pattern(|op| matches!(op, Logical(LogicalJoin(_))))
            .pattern(|op| matches!(op, Logical(LogicalLimit(_))))
            .leaf(any)
            .finish()
            .leaf(|op| matches!(op, Logical(LogicalScan(_))))
            .finish();
    }
}
