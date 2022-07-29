use crate::properties::DistributionSpec::Random;
use datafusion::prelude::Column;

use crate::properties::PhysicalProp;

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub enum DistributionSpec {
    /// The data set is not partitioned and has only one partition.
    Singleton,
    /// The data set is partitioned according to hash values of columns.
    Hashed(Vec<Column>),
    /// The data set has several partitions, but the partitioning doesn't following any rule.
    Random,
}

impl PhysicalProp for DistributionSpec {
    fn satisfies(&self, _other: &Self) -> bool {
        true
    }
}

impl Default for DistributionSpec {
    fn default() -> Self {
        Random
    }
}
