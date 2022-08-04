//! Contains code to integrate with datafusion.
pub mod plan;
pub mod planner;
pub mod rule;

pub type DFResult<T> = datafusion::common::Result<T>;
