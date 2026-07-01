//! Unified request-execution core shared by all request paths.
//!
//! Historically, request execution (load balancing, retries, speculative
//! execution, history, metrics) was implemented independently in several
//! places: [`Session`](crate::client::session::Session) non-paged methods, the
//! [`QueryPager`](crate::client::pager::QueryPager) worker and the control
//! connection. This module collapses all of that into a single set of layered
//! functions:
//!
//! - [`run_request_no_side_effects`] (Layer 2) - takes no `Session` and does
//!   not handle side effects. It applies the client-side timeout and dispatches
//!   speculative-execution fibers. It is generic over the *source* of
//!   connections (see [`AttemptTarget`]) so that it works both with a
//!   load-balancing plan of nodes and with a single fixed connection (as used
//!   by the control connection).
//! - [`run_request_speculative_fiber`] (Layer 3) - a single speculative fiber:
//!   iterates the execution plan, picks connections, runs the per-attempt
//!   closure and applies the retry policy.
//!
//! The outermost layer (`run_any_request`), which additionally handles
//! side effects coming from `USE <keyspace>` and schema-changing statements,
//! lives on [`Session`](crate::client::session::Session) because it needs
//! access to session state.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use tracing::Instrument;
use tracing::trace;
use tracing::trace_span;

use crate::cluster::NodeRef;
use crate::errors::ConnectionPoolError;
use crate::errors::RequestAttemptError;
use crate::errors::RequestError;
use crate::frame::types::Consistency;
use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::{self, HistoryListener};
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing::{LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use crate::policies::speculative_execution::{self, SpeculativeExecutionPolicy};
use crate::response::Coordinator;
use crate::response::NonErrorQueryResponse;
use crate::routing::Shard;

/// Result of running a request, before side effects are handled.
pub(crate) enum RunRequestResult<ResT> {
    IgnoredWriteError,
    Completed(ResT),
}

/// Distinguishes paged from non-paged requests, only for the purpose of
/// bumping the right metrics counters.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RequestKind {
    NonPaged,
    Paged,
}

/// All resolved per-request configuration needed to execute a request,
/// independent of any `Session`.
///
/// The two-level fallback between per-statement config and the execution
/// profile is resolved *before* constructing this struct.
pub(crate) struct RequestExecutionParams<'a> {
    /// Whether the request is idempotent (gates speculative execution and is
    /// passed to the retry policy).
    pub(crate) is_idempotent: bool,
    /// Consistency explicitly set on the statement, if any. When `None`,
    /// [`Self::default_consistency`] is used.
    pub(crate) consistency_set_on_statement: Option<Consistency>,
    /// Consistency to use when the statement does not set one explicitly
    /// (i.e. the execution profile's consistency).
    pub(crate) default_consistency: Consistency,
    /// Retry policy used to start a fresh retry session per fiber.
    pub(crate) retry_policy: &'a dyn RetryPolicy,
    /// Speculative execution policy, if any. Only fires for idempotent requests.
    pub(crate) speculative_policy: Option<&'a dyn SpeculativeExecutionPolicy>,
    /// Client-side request timeout, if any.
    pub(crate) request_timeout: Option<Duration>,
    /// History listener, if any.
    pub(crate) history_listener: Option<&'a dyn HistoryListener>,
    /// Metrics sink, if any. The control connection has no metrics.
    pub(crate) metrics: Option<&'a Arc<Metrics>>,
    /// Paged vs non-paged, for metrics.
    pub(crate) request_kind: RequestKind,
}

impl RequestExecutionParams<'_> {
    fn inc_total_queries(&self) {
        let Some(metrics) = self.metrics else {
            return;
        };
        match self.request_kind {
            RequestKind::NonPaged => metrics.inc_total_nonpaged_queries(),
            RequestKind::Paged => metrics.inc_total_paged_queries(),
        }
    }

    fn inc_failed_queries(&self) {
        let Some(metrics) = self.metrics else {
            return;
        };
        match self.request_kind {
            RequestKind::NonPaged => metrics.inc_failed_nonpaged_queries(),
            RequestKind::Paged => metrics.inc_failed_paged_queries(),
        }
    }

    fn inc_retries_num(&self) {
        if let Some(metrics) = self.metrics {
            metrics.inc_retries_num();
        }
    }

    fn inc_request_timeouts(&self) {
        if let Some(metrics) = self.metrics {
            metrics.inc_request_timeouts();
        }
    }

    fn log_query_latency(&self, latency_ms: u64) {
        if let Some(metrics) = self.metrics {
            let _ = metrics.log_query_latency(latency_ms);
        }
    }
}

/// Abstracts a single target that a request attempt can be sent to.
///
/// A request plan is an iterator of `AttemptTarget`s. There are two
/// implementations:
/// - [`NodeAttemptTarget`] - a `(node, shard)` pair produced by a load
///   balancing plan. It produces a real [`Coordinator`] and forwards
///   success/failure feedback to the load balancing policy.
/// - [`SingleConnectionTarget`] - a single fixed connection (used by the
///   control connection, which intentionally has no [`Node`](crate::cluster::Node)).
///   It produces no coordinator (`()`) and has no load balancing feedback.
pub(crate) trait AttemptTarget {
    /// Coordinator descriptor reported back to the caller.
    type Coordinator;

    /// Acquires a connection to send the attempt on.
    ///
    /// On `Err`, the fiber skips this target and proceeds to the next one in
    /// the plan (without counting it as a failed request in metrics).
    async fn get_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError>;

    /// Builds the coordinator descriptor once a connection has been chosen.
    fn coordinator(&self, connection: &Arc<Connection>) -> Self::Coordinator;

    /// Load balancing feedback after a successful attempt. No-op for targets
    /// that are not driven by load balancing.
    fn on_attempt_success(&self, elapsed: Duration);

    /// Load balancing feedback after a failed attempt. No-op for targets that
    /// are not driven by load balancing.
    fn on_attempt_failure(&self, elapsed: Duration, error: &RequestAttemptError);
}

/// A load-balancing-driven target: a `(node, shard)` pair.
pub(crate) struct NodeAttemptTarget<'a> {
    node: NodeRef<'a>,
    shard: Shard,
    /// If set, this connection is used instead of asking the node's pool.
    /// Used for "coordinator stability" across pages, where we want to reuse
    /// the exact connection (and thus shard) that served the previous page.
    preselected_connection: Option<Arc<Connection>>,
    load_balancing_policy: &'a dyn LoadBalancingPolicy,
    routing_info: &'a RoutingInfo<'a>,
}

impl<'a> NodeAttemptTarget<'a> {
    pub(crate) fn new(
        node: NodeRef<'a>,
        shard: Shard,
        load_balancing_policy: &'a dyn LoadBalancingPolicy,
        routing_info: &'a RoutingInfo<'a>,
    ) -> Self {
        Self {
            node,
            shard,
            preselected_connection: None,
            load_balancing_policy,
            routing_info,
        }
    }

    /// Like [`Self::new`], but reuses an already-chosen connection rather than
    /// asking the node's pool for one.
    pub(crate) fn with_preselected_connection(
        node: NodeRef<'a>,
        shard: Shard,
        connection: Arc<Connection>,
        load_balancing_policy: &'a dyn LoadBalancingPolicy,
        routing_info: &'a RoutingInfo<'a>,
    ) -> Self {
        Self {
            node,
            shard,
            preselected_connection: Some(connection),
            load_balancing_policy,
            routing_info,
        }
    }
}

impl AttemptTarget for NodeAttemptTarget<'_> {
    type Coordinator = Coordinator;

    async fn get_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError> {
        match &self.preselected_connection {
            Some(conn) => Ok(Arc::clone(conn)),
            None => self.node.connection_for_shard(self.shard).await,
        }
    }

    fn coordinator(&self, connection: &Arc<Connection>) -> Coordinator {
        Coordinator::new(
            self.node,
            self.node.sharder().is_some().then_some(self.shard),
            connection,
        )
    }

    fn on_attempt_success(&self, elapsed: Duration) {
        self.load_balancing_policy
            .on_request_success(self.routing_info, elapsed, self.node);
    }

    fn on_attempt_failure(&self, elapsed: Duration, error: &RequestAttemptError) {
        self.load_balancing_policy
            .on_request_failure(self.routing_info, elapsed, self.node, error);
    }
}

/// A target that always uses one specific connection. Used by the control
/// connection, which has no [`Node`](crate::cluster::Node) and no load
/// balancing.
pub(crate) struct SingleConnectionTarget {
    connection: Arc<Connection>,
}

impl SingleConnectionTarget {
    pub(crate) fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }
}

impl AttemptTarget for SingleConnectionTarget {
    type Coordinator = ();

    async fn get_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError> {
        Ok(Arc::clone(&self.connection))
    }

    fn coordinator(&self, _connection: &Arc<Connection>) {}

    fn on_attempt_success(&self, _elapsed: Duration) {}

    fn on_attempt_failure(&self, _elapsed: Duration, _error: &RequestAttemptError) {}
}

/// Outcome of [`run_request_no_side_effects`]/[`run_request_speculative_fiber`].
pub(crate) struct RequestExecutionOutcome<C> {
    /// The successful (or ignored-write) result.
    pub(crate) result: RunRequestResult<NonErrorQueryResponse>,
    /// The coordinator that served the request (target-dependent type).
    pub(crate) coordinator: C,
    /// The connection on which the request succeeded. Useful for coordinator
    /// stability across pages.
    pub(crate) connection: Arc<Connection>,
}

/// History data threaded through a single fiber.
struct HistoryData<'a> {
    listener: &'a dyn HistoryListener,
    request_id: history::RequestId,
    speculative_id: Option<history::SpeculativeId>,
}

/// Per-fiber execution context: retry session, history and the request span.
struct ExecuteRequestContext<'a> {
    retry_session: Box<dyn RetrySession>,
    history_data: Option<HistoryData<'a>>,
    request_span: &'a RequestSpan,
}

impl ExecuteRequestContext<'_> {
    fn log_attempt_start(&self, node_addr: std::net::SocketAddr) -> Option<history::AttemptId> {
        self.history_data.as_ref().map(|hd| {
            hd.listener
                .log_attempt_start(hd.request_id, hd.speculative_id, node_addr)
        })
    }

    fn log_attempt_success(&self, attempt_id_opt: &Option<history::AttemptId>) {
        let (Some(attempt_id), Some(history_data)) = (attempt_id_opt, &self.history_data) else {
            return;
        };
        history_data.listener.log_attempt_success(*attempt_id);
    }

    fn log_attempt_error(
        &self,
        attempt_id_opt: &Option<history::AttemptId>,
        error: &RequestAttemptError,
        retry_decision: &RetryDecision,
    ) {
        let (Some(attempt_id), Some(history_data)) = (attempt_id_opt, &self.history_data) else {
            return;
        };
        history_data
            .listener
            .log_attempt_error(*attempt_id, error, retry_decision);
    }
}

/// Wraps a request plan in a mutex so that it can be shared between speculative
/// fibers running concurrently.
struct SharedPlan<I> {
    iter: Mutex<I>,
}

impl<Target, I> Iterator for &SharedPlan<I>
where
    I: Iterator<Item = Target>,
{
    type Item = Target;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.lock().unwrap().next()
    }
}

/// Layer 2: executes a request without handling side effects and without
/// needing a `Session`.
///
/// Applies the client-side timeout and, for idempotent requests with a
/// speculative execution policy, runs multiple [`run_request_speculative_fiber`]
/// fibers; otherwise runs a single fiber.
///
/// `request_plan` is an iterator of [`AttemptTarget`]s. `run_request_once`
/// performs a single attempt against a chosen connection and consistency.
pub(crate) async fn run_request_no_side_effects<'a, Target, QueryFut>(
    request_plan: impl Iterator<Item = Target>,
    run_request_once: impl Fn(Arc<Connection>, Consistency) -> QueryFut,
    params: &RequestExecutionParams<'a>,
    request_span: &'a RequestSpan,
) -> Result<RequestExecutionOutcome<Target::Coordinator>, RequestError>
where
    Target: AttemptTarget,
    QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
{
    let history_listener_and_id: Option<(&dyn HistoryListener, history::RequestId)> = params
        .history_listener
        .map(|hl| (hl, hl.log_request_start()));

    let runner = async {
        match params.speculative_policy {
            Some(speculative) if params.is_idempotent => {
                let shared_request_plan = SharedPlan {
                    iter: Mutex::new(request_plan),
                };

                let request_runner_generator = |is_speculative: bool| {
                    let history_data: Option<HistoryData> =
                        history_listener_and_id.map(|(listener, request_id)| {
                            let speculative_id: Option<history::SpeculativeId> = is_speculative
                                .then(|| listener.log_new_speculative_fiber(request_id));
                            HistoryData {
                                listener,
                                request_id,
                                speculative_id,
                            }
                        });

                    if is_speculative {
                        request_span.inc_speculative_executions();
                    }

                    run_request_speculative_fiber(
                        &shared_request_plan,
                        &run_request_once,
                        params,
                        ExecuteRequestContext {
                            retry_session: params.retry_policy.new_session(),
                            history_data,
                            request_span,
                        },
                    )
                };

                let context = speculative_execution::Context {
                    #[cfg(feature = "metrics")]
                    metrics: Arc::clone(
                        params
                            .metrics
                            .expect("BUG: speculative execution enabled without a metrics sink"),
                    ),
                };

                speculative_execution::execute(speculative, &context, request_runner_generator)
                    .await
            }
            _ => {
                let history_data: Option<HistoryData> =
                    history_listener_and_id.map(|(listener, request_id)| HistoryData {
                        listener,
                        request_id,
                        speculative_id: None,
                    });
                run_request_speculative_fiber(
                    request_plan,
                    &run_request_once,
                    params,
                    ExecuteRequestContext {
                        retry_session: params.retry_policy.new_session(),
                        history_data,
                        request_span,
                    },
                )
                .await
                .unwrap_or(Err(RequestError::EmptyPlan))
            }
        }
    };

    let result = match params.request_timeout {
        Some(timeout) => tokio::time::timeout(timeout, runner).await.unwrap_or_else(
            |_: tokio::time::error::Elapsed| {
                params.inc_request_timeouts();
                let timeout_error = RequestError::RequestTimeout(timeout);
                trace!(
                    parent: request_span.span(),
                    error = %timeout_error,
                    "Request timed out"
                );
                Err(timeout_error)
            },
        ),
        None => runner.await,
    };

    if let Some((history_listener, request_id)) = history_listener_and_id {
        match &result {
            Ok(_) => history_listener.log_request_success(request_id),
            Err(e) => history_listener.log_request_error(request_id, e),
        }
    }

    result
}

/// Layer 3: a single speculative fiber.
///
/// Iterates the execution plan, picking a connection for each target, running
/// `run_request_once` and consulting the retry policy on failure. The chosen
/// connection is reused across same-target retries (preserving e.g. shard
/// paging caches).
///
/// Returns `None` only if the plan was empty.
async fn run_request_speculative_fiber<'a, Target, QueryFut>(
    request_plan: impl Iterator<Item = Target>,
    run_request_once: impl Fn(Arc<Connection>, Consistency) -> QueryFut,
    params: &RequestExecutionParams<'a>,
    mut context: ExecuteRequestContext<'a>,
) -> Option<Result<RequestExecutionOutcome<Target::Coordinator>, RequestError>>
where
    Target: AttemptTarget,
    QueryFut: Future<Output = Result<NonErrorQueryResponse, RequestAttemptError>>,
{
    let mut last_error: Option<RequestError> = None;
    let mut current_consistency: Consistency = params
        .consistency_set_on_statement
        .unwrap_or(params.default_consistency);

    'nodes_in_plan: for target in request_plan {
        let span = trace_span!("Executing request");
        // Choose a connection for this target. It is reused across same-target
        // retries to preserve any per-connection (per-shard) paging state.
        let connection = match target.get_connection().await {
            Ok(connection) => connection,
            Err(e) => {
                trace!(
                    parent: &span,
                    error = %e,
                    "Choosing connection failed"
                );
                last_error = Some(e.into());
                // Broken connection doesn't count as a failed request, don't log in metrics
                continue 'nodes_in_plan;
            }
        };
        context.request_span.record_shard_id(&connection);
        let connect_address = connection.get_connect_address();
        let coordinator = target.coordinator(&connection);

        'same_target_retries: loop {
            trace!(parent: &span, "Execution started");
            params.inc_total_queries();
            let request_start = Instant::now();

            let attempt_id: Option<history::AttemptId> = context.log_attempt_start(connect_address);

            let request_result: Result<NonErrorQueryResponse, RequestAttemptError> =
                run_request_once(Arc::clone(&connection), current_consistency)
                    .instrument(span.clone())
                    .await;

            let elapsed = request_start.elapsed();
            let request_error: RequestAttemptError = match request_result {
                Ok(response) => {
                    trace!(parent: &span, "Request succeeded");
                    params.log_query_latency(elapsed.as_millis() as u64);
                    context.log_attempt_success(&attempt_id);
                    target.on_attempt_success(elapsed);
                    return Some(Ok(RequestExecutionOutcome {
                        result: RunRequestResult::Completed(response),
                        coordinator,
                        connection,
                    }));
                }
                Err(e) => {
                    trace!(
                        parent: &span,
                        last_error = %e,
                        "Request failed"
                    );
                    params.inc_failed_queries();
                    target.on_attempt_failure(elapsed, &e);
                    e
                }
            };

            // Use retry policy to decide what to do next.
            let request_info = RequestInfo {
                error: &request_error,
                is_idempotent: params.is_idempotent,
                consistency: current_consistency,
            };

            let retry_decision = context.retry_session.decide_should_retry(request_info);
            trace!(
                parent: &span,
                retry_decision = ?retry_decision
            );

            context.log_attempt_error(&attempt_id, &request_error, &retry_decision);

            last_error = Some(request_error.into());

            match retry_decision {
                RetryDecision::RetrySameTarget(new_cl) => {
                    params.inc_retries_num();
                    current_consistency = new_cl.unwrap_or(current_consistency);
                    continue 'same_target_retries;
                }
                RetryDecision::RetryNextTarget(new_cl) => {
                    params.inc_retries_num();
                    current_consistency = new_cl.unwrap_or(current_consistency);
                    continue 'nodes_in_plan;
                }
                RetryDecision::DontRetry => break 'nodes_in_plan,
                RetryDecision::IgnoreWriteError => {
                    return Some(Ok(RequestExecutionOutcome {
                        result: RunRequestResult::IgnoredWriteError,
                        coordinator,
                        connection,
                    }));
                }
            };
        }
    }

    last_error.map(Result::Err)
}
