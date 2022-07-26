//! Contains code to integrate with datafusion.
mod rule;
mod plan;
mod planner;

pub type DFResult<T> = datafusion::common::Result<T>;
