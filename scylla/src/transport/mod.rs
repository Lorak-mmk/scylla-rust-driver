pub mod downgrading_consistency_retry_policy;
pub mod execution_profile;
pub mod iterator;
pub mod load_balancing;
pub(crate) mod metrics;
pub mod partitioner;
pub mod query_result;
pub mod retry_policy;
pub mod speculative_execution;

pub use crate::frame::{Authenticator, Compression};
pub use execution_profile::ExecutionProfile;
pub use scylla_cql::errors;

#[cfg(test)]
mod authenticate_test;
#[cfg(test)]
mod cql_collections_test;

#[cfg(test)]
mod cql_types_test;
#[cfg(test)]
mod cql_value_test;
