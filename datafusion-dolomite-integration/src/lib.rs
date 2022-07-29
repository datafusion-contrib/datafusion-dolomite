//! Contains code to integrate with datafusion.
mod plan;
mod planner;
mod rule;

pub type DFResult<T> = datafusion::common::Result<T>;
