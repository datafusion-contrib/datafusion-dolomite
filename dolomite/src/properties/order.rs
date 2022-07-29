use datafusion::prelude::Column;

use crate::properties::PhysicalProp;

/// Ordering of one column.
#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub struct Ordering {
    column: Column,
    /// Ascending or descending.
    asc: bool,
    /// Should null be treated first.
    null_first: bool,
}

/// Ordering property specification.
#[derive(Hash, Debug, Clone, Eq, PartialEq, Default)]
pub struct OrderSpec {
    orders: Vec<Ordering>,
}

impl PhysicalProp for OrderSpec {
    fn satisfies(&self, _other: &Self) -> bool {
        true
    }
}
