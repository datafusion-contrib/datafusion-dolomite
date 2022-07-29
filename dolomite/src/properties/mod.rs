//! Properties of relation operators.
//!
//! Currently we have two kinds of properties: [`LogicalProperty`] and [`PhysicalPropertySet`].
//! Logical property are things shared by logically equivalent plans, such as schema, key.
//! Physical properties are concerned with sorting, distribution, etc.

mod distribution;

use std::fmt::Debug;
use std::hash::Hash;

pub use distribution::*;
mod order;
pub use order::*;
mod logical;
pub use logical::*;
mod physical;
pub use physical::*;

pub trait PhysicalProp: Debug + Hash {
    /// Tests whether satisfies self.
    fn satisfies(&self, other: &Self) -> bool;
}
