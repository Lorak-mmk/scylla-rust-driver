//! Entities that provide automated transparent paging of a query.
//! They enable consuming result of a paged query as a stream over rows,
//! which abstracts over page boundaries.

use std::future::Future;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::deserialize::result::RawRowLendingIterator;
use crate::deserialize::row::{ColumnIterator, DeserializeRow};
use crate::deserialize::{DeserializationError, TypeCheckError};
use crate::frame::frame_errors::ResultMetadataAndRowsCountParseError;
use crate::frame::request::query::{PagingState, PagingStateResponse};
use crate::frame::response::NonErrorResponseWithDeserializedMetadataV2 as NonErrorResponseWithDeserializedMetadata;
use crate::frame::response::result::{DeserializedMetadataAndRawRows, SchemaChange, SetKeyspace};
use crate::frame::types::{Consistency, SerialConsistency};
use crate::serialize::row::SerializedValues;
use futures::Stream;
use std::result::Result;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::client::execution::{
    NodeAttemptTarget, RequestExecutionOutcome, RequestExecutionParams, RequestKind,
    RunRequestResult, SingleConnectionTarget, run_request_no_side_effects,
};
use crate::client::execution_profile::ExecutionProfileInner;
use crate::client::session::Session;
use crate::cluster::ClusterState;
use crate::deserialize::DeserializeOwnedRow;
use crate::errors::{PagerExecutionError, RequestAttemptError, RequestError};
use crate::frame::response::result;
use crate::network::Connection;
use crate::observability::driver_tracing::RequestSpan;
use crate::observability::history::HistoryListener;
use crate::observability::metrics::Metrics;
use crate::policies::load_balancing::{self, LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{FallthroughRetryPolicy, RetryPolicy};
use crate::policies::speculative_execution::SpeculativeExecutionPolicy;
use crate::response::query_result::ColumnSpecs;
use crate::response::{Coordinator, NonErrorQueryResponse, QueryResponse};
use crate::routing::NodeLocationPreference;
use crate::statement::prepared::{PartitionKey, PartitionKeyError, PreparedStatement};
use crate::statement::unprepared::Statement;
use tracing::{Instrument, warn};
use uuid::Uuid;

// Like std::task::ready!, but handles the whole stack of Poll<Option<Result<>>>.
// If it matches Poll::Ready(Some(Ok(_))), then it returns the innermost value,
// otherwise it returns from the surrounding function.
macro_rules! ready_some_ok {
    ($e:expr) => {
        match $e {
            Poll::Ready(Some(Ok(x))) => x,
            Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }
    };
}

struct NextReceivedPage {
    rows: DeserializedMetadataAndRawRows,
    tracing_id: Option<Uuid>,
    request_coordinator: Option<Coordinator>,
}

/*
 * The first page is special in a number of ways:
 * - It is delivered synchronously (not meaning non-`async`, but the code
 *   does not progress until the first page is there) when QueryPager is
 *   constructed. All subsequent pages are delivered asynchronously via
 *   a channel to QueryPager.
 * - The first page may be non-Rows Result and still correct. For example,
 *   `USE <keyspace>` statements return Result:SetKeyspace, and DDLs often
 *   return Result:SchemaChange response. In order to handle those special
 *   results, we need access to Session:
 *   - SetKeyspace requires issuing `USE <keyspace>` statement on
 *     connections to all nodes;
 *   - SchemaChange should be followed with awaiting schema agreement.
 *   Session is available when constructing the QueryPager (except for
 *   `Connection::execute_iter()` API, which we handle separately by
 *   treating all non-Rows results as erroneous).
 *   However, Session is not available once we have the QueryPager
 *   constructed, because it does not borrow from Session. Combining this
 *   with the fact that non-Rows result should never appear as a non-first
 *   page in the sequence of pages, this is another argument for having
 *   clear distinction between the first page case and the remaining pages.
 */

enum FirstPageContent {
    Rows {
        rows: DeserializedMetadataAndRawRows,
    },
    SetKeyspace {
        set_keyspace: SetKeyspace,
    },
    SchemaChange {
        schema_change: SchemaChange,
    },
}

struct FirstReceivedPage {
    content: FirstPageContent,
    tracing_id: Option<Uuid>,
    request_coordinator: Coordinator,
}

type ResultNextPage = Result<NextReceivedPage, NextPageError>;

enum ShouldFetchMorePages {
    NoMorePages,
    MorePages {
        first_page_coordinator: Coordinator,
        first_page_connection: Arc<Connection>,
        paging_state: PagingState,
    },
}

/// Drives paged execution by repeatedly invoking the unified request-execution
/// core ([`run_request_no_side_effects`]) - once per page.
///
/// Each page is one logical request: it goes through the full load balancing,
/// retry, speculative-execution, history and metrics machinery, exactly like a
/// non-paged request. The only paging-specific logic that remains here is:
/// - injecting the current [`PagingState`] into each attempt,
/// - "coordinator stability" (preferring the node/connection that served the
///   previous page for the next one),
/// - turning each successful page into a [`NextReceivedPage`] sent over the
///   channel, and stopping once the server reports no more pages.
struct PagingExecutor<SpanCreator> {
    load_balancing_policy: Arc<dyn LoadBalancingPolicy>,
    retry_policy: Arc<dyn RetryPolicy>,
    speculative_policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
    request_timeout: Option<Duration>,
    is_idempotent: bool,
    consistency_set_on_statement: Option<Consistency>,
    default_consistency: Consistency,
    metrics: Arc<Metrics>,
    history_listener: Option<Arc<dyn HistoryListener>>,
    span_creator: SpanCreator,
}

impl<SpanCreator> PagingExecutor<SpanCreator>
where
    SpanCreator: Fn(&Option<PartitionKey<'_>>) -> RequestSpan,
{
    /// Fetches exactly one page by delegating to [`run_request_no_side_effects`].
    ///
    /// `paging_state` is injected into each attempt. If `stability` is set, the
    /// given coordinator/connection is tried first (and filtered out of the
    /// fresh load balancing plan so it is not attempted twice).
    async fn fetch_one_page<PageQuery, QueryFut>(
        &self,
        cluster_state: &ClusterState,
        routing_info: &RoutingInfo<'_>,
        paging_state: &PagingState,
        stability: Option<(&Coordinator, &Arc<Connection>)>,
        page_query: &PageQuery,
        request_span: &RequestSpan,
    ) -> Result<RequestExecutionOutcome<Coordinator>, RequestError>
    where
        PageQuery: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    {
        let params = RequestExecutionParams {
            is_idempotent: self.is_idempotent,
            consistency_set_on_statement: self.consistency_set_on_statement,
            default_consistency: self.default_consistency,
            retry_policy: self.retry_policy.as_ref(),
            speculative_policy: self.speculative_policy.as_deref(),
            request_timeout: self.request_timeout,
            history_listener: self.history_listener.as_deref(),
            metrics: Some(&self.metrics),
            request_kind: RequestKind::Paged,
        };

        // Adapt the paging-aware `page_query` to the two-argument closure
        // expected by the execution core, injecting the current paging state
        // and normalizing error responses.
        let run_request_once = move |connection: Arc<Connection>, consistency: Consistency| {
            let paging_state = paging_state.clone();
            async move {
                page_query(connection, consistency, paging_state)
                    .await
                    .and_then(QueryResponse::into_non_error_query_response)
            }
        };

        let load_balancing_policy = self.load_balancing_policy.as_ref();
        let base_plan =
            load_balancing::Plan::new(load_balancing_policy, routing_info, cluster_state);

        // Coordinator stability: try the previous page's connection first.
        let stability_target = stability.map(|(coordinator, connection)| {
            NodeAttemptTarget::with_preselected_connection(
                coordinator.node(),
                coordinator.shard().unwrap_or(0),
                Arc::clone(connection),
                load_balancing_policy,
                routing_info,
            )
        });

        let plan = stability_target.into_iter().chain(
            base_plan
                .filter(move |&(node, shard)| match stability {
                    // Filter out the preselected coordinator - it is tried first.
                    // If the previous attempt targeted an unsharded node (such as
                    // Cassandra), filter out all targets to that node regardless
                    // of shard.
                    Some((coordinator, _)) => {
                        !(Arc::ptr_eq(node, coordinator.node())
                            && coordinator
                                .shard()
                                .is_none_or(|last_shard| last_shard == shard))
                    }
                    None => true,
                })
                .map(move |(node, shard)| {
                    NodeAttemptTarget::new(node, shard, load_balancing_policy, routing_info)
                }),
        );

        run_request_no_side_effects(plan, run_request_once, &params, request_span).await
    }

    /// Interprets the outcome of the first page fetch.
    ///
    /// The first page is special: it may legitimately be a non-Rows response
    /// (`SetKeyspace`, `SchemaChange` or a modification statement's empty
    /// result), which the caller turns into the appropriate session side
    /// effects.
    fn build_first_page(
        &self,
        outcome: RequestExecutionOutcome<Coordinator>,
        request_span: &RequestSpan,
    ) -> Result<(FirstReceivedPage, ShouldFetchMorePages), NextPageError> {
        let coordinator = outcome.coordinator;
        let connection = outcome.connection;

        let response = match outcome.result {
            RunRequestResult::IgnoredWriteError => {
                warn!("Ignoring error during fetching pages; stopping fetching.");
                return Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::Rows {
                            rows: DeserializedMetadataAndRawRows::mock_empty(),
                        },
                        tracing_id: None,
                        request_coordinator: coordinator,
                    },
                    ShouldFetchMorePages::NoMorePages,
                ));
            }
            RunRequestResult::Completed(response) => response,
        };

        let tracing_id = response.tracing_id;
        match response.response {
            NonErrorResponseWithDeserializedMetadata::Result(
                result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
            ) => {
                request_span.record_raw_rows_fields(&rows);
                let should_fetch_more_pages = match paging_state_response {
                    PagingStateResponse::HasMorePages { state } => {
                        ShouldFetchMorePages::MorePages {
                            first_page_coordinator: coordinator.clone(),
                            first_page_connection: connection,
                            paging_state: state,
                        }
                    }
                    PagingStateResponse::NoMorePages => ShouldFetchMorePages::NoMorePages,
                };
                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::Rows { rows },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    should_fetch_more_pages,
                ))
            }
            NonErrorResponseWithDeserializedMetadata::Result(
                result::ResultWithDeserializedMetadata::SetKeyspace(set_keyspace),
            ) => Ok((
                FirstReceivedPage {
                    content: FirstPageContent::SetKeyspace { set_keyspace },
                    tracing_id,
                    request_coordinator: coordinator,
                },
                ShouldFetchMorePages::NoMorePages,
            )),
            NonErrorResponseWithDeserializedMetadata::Result(
                result::ResultWithDeserializedMetadata::SchemaChange(schema_change),
            ) => Ok((
                FirstReceivedPage {
                    content: FirstPageContent::SchemaChange { schema_change },
                    tracing_id,
                    request_coordinator: coordinator,
                },
                ShouldFetchMorePages::NoMorePages,
            )),
            NonErrorResponseWithDeserializedMetadata::Result(_) => {
                // We have most probably sent a modification statement (e.g. INSERT
                // or UPDATE), so let's return an empty stream as suggested in #631.
                Ok((
                    FirstReceivedPage {
                        content: FirstPageContent::Rows {
                            rows: DeserializedMetadataAndRawRows::mock_empty(),
                        },
                        tracing_id,
                        request_coordinator: coordinator,
                    },
                    ShouldFetchMorePages::NoMorePages,
                ))
            }
            // A non-Result response. The request "succeeded" at the protocol
            // level, but we cannot interpret it as a page.
            other => Err(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::UnexpectedResponse(
                    other.to_response_kind(),
                )),
            )),
        }
    }

    /// Fetches pages 2.. in a background task, sending each over `sender`.
    ///
    /// A fresh plan is built for every page (preventing plan exhaustion on
    /// long-running queries), with the previous page's coordinator preferred.
    #[expect(clippy::too_many_arguments)]
    async fn fetch_remaining_pages<PageQuery, QueryFut>(
        self,
        cluster_state: Arc<ClusterState>,
        routing_info: RoutingInfo<'_>,
        partition_key: Option<PartitionKey<'_>>,
        mut paging_state: PagingState,
        mut last_coordinator: Coordinator,
        mut last_connection: Arc<Connection>,
        page_query: PageQuery,
        sender: mpsc::Sender<ResultNextPage>,
    ) where
        PageQuery: Fn(Arc<Connection>, Consistency, PagingState) -> QueryFut,
        QueryFut: Future<Output = Result<QueryResponse, RequestAttemptError>>,
    {
        loop {
            let request_span = (self.span_creator)(&partition_key);
            let outcome = self
                .fetch_one_page(
                    &cluster_state,
                    &routing_info,
                    &paging_state,
                    Some((&last_coordinator, &last_connection)),
                    &page_query,
                    &request_span,
                )
                .instrument(request_span.span().clone())
                .await;

            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = sender.send(Err(NextPageError::RequestFailure(error))).await;
                    return;
                }
            };

            let coordinator = outcome.coordinator;
            let connection = outcome.connection;

            let response = match outcome.result {
                RunRequestResult::IgnoredWriteError => {
                    warn!("Ignoring error during fetching pages; stopping fetching.");
                    return;
                }
                RunRequestResult::Completed(response) => response,
            };

            let tracing_id = response.tracing_id;
            match response.response {
                NonErrorResponseWithDeserializedMetadata::Result(
                    result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
                ) => {
                    request_span.record_raw_rows_fields(&rows);

                    let received_page = NextReceivedPage {
                        rows,
                        tracing_id,
                        request_coordinator: Some(coordinator.clone()),
                    };

                    if sender.send(Ok(received_page)).await.is_err() {
                        // Channel was closed, QueryPager was dropped - should shutdown.
                        return;
                    }

                    match paging_state_response.into_paging_control_flow() {
                        ControlFlow::Continue(new_paging_state) => {
                            paging_state = new_paging_state;
                            // Prioritize this coordinator for the next page fetch.
                            last_coordinator = coordinator;
                            last_connection = connection;
                        }
                        ControlFlow::Break(()) => {
                            // Reached the last page, shutdown.
                            return;
                        }
                    }
                }
                // This catches all other kinds of responses that are not rows.
                // As this is not the first page, this is certainly an error.
                other => {
                    let error = RequestAttemptError::UnexpectedResponse(other.to_response_kind());
                    let _ = sender
                        .send(Err(NextPageError::RequestFailure(
                            RequestError::LastAttemptError(error),
                        )))
                        .await;
                    return;
                }
            }
        }
    }
}

/// Drives paged execution over a single fixed connection, used by
/// [`Connection::execute_iter`](crate::network::Connection::execute_iter).
///
/// Unlike [`PagingExecutor`], it has no load balancing, retries, speculative
/// execution, metrics or history: it simply sends each page on the given
/// connection through the unified execution core (via
/// [`SingleConnectionTarget`]), applying the client-side timeout.
///
/// NOTE: This executor only supports executing SELECT statements. More
/// specifically, it expects that each response is of Rows kind. Other kinds of
/// responses will result in an error.
struct ConnectionPagingExecutor {
    connection: Arc<Connection>,
    prepared: PreparedStatement,
    values: SerializedValues,
    page_size: crate::statement::PageSize,
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,
    request_timeout: Option<Duration>,
}

impl ConnectionPagingExecutor {
    /// Fetches exactly one page from the fixed connection by delegating to
    /// [`run_request_no_side_effects`] with a single-connection plan.
    async fn fetch_one_page(
        &self,
        paging_state: &PagingState,
        request_span: &RequestSpan,
    ) -> Result<RequestExecutionOutcome<()>, RequestError> {
        // The control-connection-style path never retries: a fallthrough retry
        // policy makes every attempt terminal.
        let retry_policy = FallthroughRetryPolicy::new();
        let params = RequestExecutionParams {
            is_idempotent: false,
            consistency_set_on_statement: None,
            default_consistency: self.consistency,
            retry_policy: &retry_policy,
            speculative_policy: None,
            request_timeout: self.request_timeout,
            history_listener: None,
            metrics: None,
            request_kind: RequestKind::Paged,
        };

        let run_request_once = |connection: Arc<Connection>, consistency: Consistency| {
            let paging_state = paging_state.clone();
            async move {
                connection
                    .execute_raw_with_consistency(
                        &self.prepared,
                        &self.values,
                        consistency,
                        self.serial_consistency,
                        Some(self.page_size),
                        paging_state,
                    )
                    .await
                    .and_then(QueryResponse::into_non_error_query_response)
            }
        };

        let plan = std::iter::once(SingleConnectionTarget::new(Arc::clone(&self.connection)));

        run_request_no_side_effects(plan, run_request_once, &params, request_span).await
    }

    /// Interprets a page outcome. Only Rows responses are valid here.
    fn page_from_outcome(
        outcome: RequestExecutionOutcome<()>,
    ) -> Result<(NextReceivedPage, PagingStateResponse), NextPageError> {
        let response = match outcome.result {
            // A fallthrough retry policy never ignores write errors, so this
            // arm is effectively unreachable; end the stream gracefully.
            RunRequestResult::IgnoredWriteError => {
                return Ok((
                    NextReceivedPage {
                        rows: DeserializedMetadataAndRawRows::mock_empty(),
                        tracing_id: None,
                        request_coordinator: None,
                    },
                    PagingStateResponse::NoMorePages,
                ));
            }
            RunRequestResult::Completed(response) => response,
        };

        let tracing_id = response.tracing_id;
        match response.response {
            NonErrorResponseWithDeserializedMetadata::Result(
                result::ResultWithDeserializedMetadata::Rows((rows, paging_state_response)),
            ) => Ok((
                NextReceivedPage {
                    rows,
                    tracing_id,
                    request_coordinator: None,
                },
                paging_state_response,
            )),
            other => Err(NextPageError::RequestFailure(
                RequestError::LastAttemptError(RequestAttemptError::UnexpectedResponse(
                    other.to_response_kind(),
                )),
            )),
        }
    }

    fn make_span(&self) -> RequestSpan {
        let span = RequestSpan::new_query(self.prepared.get_statement());
        span.record_request_size(0);
        span
    }

    /// Fetches pages 2.. in a background task, sending each over `sender`.
    async fn fetch_remaining_pages(
        self,
        mut paging_state: PagingState,
        sender: mpsc::Sender<ResultNextPage>,
    ) {
        loop {
            let request_span = self.make_span();
            let outcome = self
                .fetch_one_page(&paging_state, &request_span)
                .instrument(request_span.span().clone())
                .await;

            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    let _ = sender.send(Err(NextPageError::RequestFailure(error))).await;
                    return;
                }
            };

            let (page, paging_state_response) = match Self::page_from_outcome(outcome) {
                Ok(page) => page,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };

            if sender.send(Ok(page)).await.is_err() {
                // Channel was closed, QueryPager was dropped - should shutdown.
                return;
            }

            match paging_state_response.into_paging_control_flow() {
                ControlFlow::Continue(new_paging_state) => paging_state = new_paging_state,
                ControlFlow::Break(()) => return,
            }
        }
    }
}

pub(crate) struct PreparedPagerConfig {
    pub(crate) prepared: PreparedStatement,
    pub(crate) values: SerializedValues,
    pub(crate) execution_profile: Arc<ExecutionProfileInner>,
    pub(crate) cluster_state: Arc<ClusterState>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) location_preference: Arc<NodeLocationPreference>,
}

/// An intermediate object that allows to construct a stream over a query
/// that is asynchronously paged in the background.
///
/// Before the results can be processed in a convenient way, the QueryPager
/// needs to be cast into a typed stream. This is done by use of `rows_stream()` method.
/// As the method is generic over the target type, the turbofish syntax
/// can come in handy there, e.g. `query_pager.rows_stream::<(i32, String, Uuid)>()`.
#[derive(Debug)]
pub struct QueryPager {
    current_page: RawRowLendingIterator,
    page_receiver: mpsc::Receiver<Result<NextReceivedPage, NextPageError>>,
    tracing_ids: Vec<Uuid>,
    request_coordinators: Vec<Coordinator>,
}

// QueryPager is not an iterator or a stream! However, it implements
// a `next()` method that returns a [ColumnIterator], which can be used
// to manually deserialize a row.
// The `ColumnIterator` borrows from the `QueryPager`, and the [futures::Stream] trait
// does not allow for such a pattern. Lending streams are not a thing yet.
impl QueryPager {
    /// Returns the next item (`ColumnIterator`) from the stream.
    ///
    /// Because pages may have different result metadata, each one needs to be type-checked before deserialization.
    /// The bool returned in second element of the tuple indicates whether the page was fresh or not.
    /// This allows user to then perform the type check for fresh pages.
    ///
    /// This is not a part of the `Stream` interface because the returned iterator
    /// borrows from self.
    ///
    /// This is cancel-safe.
    async fn next(&mut self) -> Option<Result<(ColumnIterator<'_, '_>, bool), NextRowError>> {
        let res = std::future::poll_fn(|cx| Pin::new(&mut *self).poll_fill_page(cx)).await;
        let fresh_page = match res {
            Some(Ok(f)) => f,
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };

        Some(
            self.current_page
                .next()
                .unwrap()
                .map_err(NextRowError::RowDeserializationError)
                .map(|x| (x, fresh_page)),
        )
    }

    /// Tries to acquire a non-empty page, if current page is exhausted.
    /// Boolean value in `Some(Ok(r))` is true if a new page was fetched.
    fn poll_fill_page(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<bool, NextRowError>>> {
        if !self.is_current_page_exhausted() {
            return Poll::Ready(Some(Ok(false)));
        }
        ready_some_ok!(self.as_mut().poll_next_page(cx));
        if self.is_current_page_exhausted() {
            // We most likely got a zero-sized page.
            // Try again later.
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Some(Ok(true)))
        }
    }

    /// Makes an attempt to acquire the next page (which may be empty).
    ///
    /// On success, returns Some(Ok()).
    /// On failure, returns Some(Err()).
    /// If there are no more pages, returns None.
    fn poll_next_page(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), NextRowError>>> {
        let mut s = self.as_mut();

        let received_page = ready_some_ok!(Pin::new(&mut s.page_receiver).poll_recv(cx));

        s.current_page = RawRowLendingIterator::new(received_page.rows);

        if let Some(tracing_id) = received_page.tracing_id {
            s.tracing_ids.push(tracing_id);
        }

        s.request_coordinators
            .extend(received_page.request_coordinator);

        Poll::Ready(Some(Ok(())))
    }

    /// Type-checks the iterator against given type.
    ///
    /// This is automatically called upon transforming [QueryPager] into [TypedRowStream].
    // Can be used with `next()` for manual deserialization.
    #[inline]
    #[deprecated(
        since = "1.4.0",
        note = "Type check should be performed for each page, which is not possible with public API.
Also, the only thing user can do (rows_stream) will take care of type check anyway.
If you are using this API, you are probably doing something wrong."
    )]
    pub fn type_check<'frame, 'metadata, RowT: DeserializeRow<'frame, 'metadata>>(
        &self,
    ) -> Result<(), TypeCheckError> {
        RowT::type_check(self.column_specs().as_slice())
    }

    /// Casts the pager's stream to a given row type, enabling [Stream]'ed operations
    /// on rows, which deserialize them on-the-fly to that given type.
    /// It only allows deserializing owned types, because [Stream] is not lending.
    /// Begins with performing type check.
    #[inline]
    pub fn rows_stream<RowT: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>>(
        self,
    ) -> Result<TypedRowStream<RowT>, TypeCheckError> {
        TypedRowStream::<RowT>::new(self)
    }

    pub(crate) async fn new_for_query(
        session: &Session,
        statement: Statement,
        execution_profile: Arc<ExecutionProfileInner>,
        cluster_state: Arc<ClusterState>,
        metrics: Arc<Metrics>,
        node_location_preference: Arc<NodeLocationPreference>,
    ) -> Result<Self, PagerExecutionError> {
        let consistency = statement
            .config
            .consistency
            .unwrap_or(execution_profile.consistency);
        let serial_consistency = statement
            .config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);

        let request_timeout = statement
            .get_request_timeout()
            .or(execution_profile.request_timeout);

        let page_size = statement.get_validated_page_size();

        let load_balancing_policy = Arc::clone(
            statement
                .get_load_balancing_policy()
                .unwrap_or(&execution_profile.load_balancing_policy),
        );

        let retry_policy = Arc::clone(
            statement
                .get_retry_policy()
                .unwrap_or(&execution_profile.retry_policy),
        );

        let speculative_policy = execution_profile.speculative_execution_policy.clone();

        let routing_info = RoutingInfo {
            consistency,
            serial_consistency,
            token: None,
            table: None,
            is_confirmed_lwt: false,
            node_location_preference: &node_location_preference,
        };

        let statement_contents = statement.contents.clone();
        let span_creator = move |_partition_key: &Option<PartitionKey<'_>>| {
            let span = RequestSpan::new_query(&statement_contents);
            span.record_request_size(0);
            span
        };

        let executor = PagingExecutor {
            load_balancing_policy,
            retry_policy,
            speculative_policy,
            request_timeout,
            is_idempotent: statement.config.is_idempotent,
            consistency_set_on_statement: statement.config.consistency,
            default_consistency: execution_profile.consistency,
            metrics,
            history_listener: statement.config.history_listener.as_ref().map(Arc::clone),
            span_creator,
        };

        // Fetch the first page on the caller task (no spawning). The borrow of
        // `statement` ends with this block, so it can be moved into the task.
        let (first_page, should_fetch_more_pages) = {
            let statement_ref = &statement;
            let page_query = |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: PagingState| async move {
                connection
                    .query_raw_with_consistency(
                        statement_ref,
                        consistency,
                        serial_consistency,
                        Some(page_size),
                        paging_state,
                    )
                    .await
            };

            let request_span = (executor.span_creator)(&None);
            let outcome = executor
                .fetch_one_page(
                    &cluster_state,
                    &routing_info,
                    &PagingState::start(),
                    None,
                    &page_query,
                    &request_span,
                )
                .instrument(request_span.span().clone())
                .await
                .map_err(NextPageError::RequestFailure)?;
            executor.build_first_page(outcome, &request_span)?
        };

        /* PROCESS FIRST PAGE */
        let (sender, receiver) = mpsc::channel::<ResultNextPage>(1);
        match should_fetch_more_pages {
            ShouldFetchMorePages::NoMorePages => {
                // No more pages - we are done, return the first page and an empty receiver.
                std::mem::drop(sender);
            }
            ShouldFetchMorePages::MorePages {
                first_page_coordinator,
                first_page_connection,
                paging_state,
            } => {
                /* REMAINING PAGES */
                let parent_span = tracing::Span::current();
                let worker_task = async move {
                    let routing_info = RoutingInfo {
                        consistency,
                        serial_consistency,
                        token: None,
                        table: None,
                        is_confirmed_lwt: false,
                        node_location_preference: &node_location_preference,
                    };

                    let statement_ref = &statement;
                    let page_query =
                        |connection: Arc<Connection>,
                         consistency: Consistency,
                         paging_state: PagingState| async move {
                            connection
                                .query_raw_with_consistency(
                                    statement_ref,
                                    consistency,
                                    serial_consistency,
                                    Some(page_size),
                                    paging_state,
                                )
                                .await
                        };

                    executor
                        .fetch_remaining_pages(
                            cluster_state,
                            routing_info,
                            None,
                            paging_state,
                            first_page_coordinator,
                            first_page_connection,
                            page_query,
                            sender,
                        )
                        .await;
                }
                .instrument(parent_span);
                let _worker_handle = tokio::task::spawn(worker_task);
            }
        }

        Self::new_from_first_page(first_page, receiver, session).await
    }

    pub(crate) async fn new_for_prepared_statement(
        session: &Session,
        config: PreparedPagerConfig,
    ) -> Result<Self, PagerExecutionError> {
        let consistency = config
            .prepared
            .config
            .consistency
            .unwrap_or(config.execution_profile.consistency);
        let serial_consistency = config
            .prepared
            .config
            .serial_consistency
            .unwrap_or(config.execution_profile.serial_consistency);

        let request_timeout = config
            .prepared
            .get_request_timeout()
            .or(config.execution_profile.request_timeout);

        let page_size = config.prepared.get_validated_page_size();

        let load_balancing_policy = Arc::clone(
            config
                .prepared
                .get_load_balancing_policy()
                .unwrap_or(&config.execution_profile.load_balancing_policy),
        );

        let retry_policy = Arc::clone(
            config
                .prepared
                .get_retry_policy()
                .unwrap_or(&config.execution_profile.retry_policy),
        );

        let speculative_policy = config
            .execution_profile
            .speculative_execution_policy
            .clone();

        let (partition_key, token) =
            match config.prepared.extract_partition_key_and_calculate_token(
                config.prepared.get_partitioner_name(),
                &config.values,
            ) {
                Ok(res) => res.unzip(),
                Err(err) => {
                    return Err(PagerExecutionError::NextPageError(
                        NextPageError::PartitionKeyError(err),
                    ));
                }
            };

        let table_spec = config.prepared.get_table_spec();
        let routing_info = RoutingInfo {
            consistency,
            serial_consistency,
            token,
            table: table_spec,
            is_confirmed_lwt: config.prepared.is_confirmed_lwt(),
            node_location_preference: &config.location_preference,
        };

        let serialized_values_size = config.values.buffer_size();

        let replicas: Option<smallvec::SmallVec<[_; 8]>> =
            if let (Some(table_spec), Some(token)) = (routing_info.table, routing_info.token) {
                Some(
                    config
                        .cluster_state
                        .get_token_endpoints_iter(table_spec, token)
                        .map(|(node, shard)| (node.clone(), shard))
                        .collect(),
                )
            } else {
                None
            };

        let span_creator = move |partition_key: &Option<PartitionKey<'_>>| {
            let span = RequestSpan::new_prepared(
                partition_key.as_ref().map(|pk| pk.iter()),
                token,
                serialized_values_size,
            );
            if let Some(replicas) = replicas.as_ref() {
                span.record_replicas(replicas.iter().map(|(node, shard)| (node, *shard)));
            }
            span
        };

        let executor = PagingExecutor {
            load_balancing_policy,
            retry_policy,
            speculative_policy,
            request_timeout,
            is_idempotent: config.prepared.config.is_idempotent,
            consistency_set_on_statement: config.prepared.config.consistency,
            default_consistency: config.execution_profile.consistency,
            metrics: config.metrics,
            history_listener: config
                .prepared
                .config
                .history_listener
                .as_ref()
                .map(Arc::clone),
            span_creator,
        };

        // Fetch the first page on the caller task (no spawning). The borrows of
        // `config.prepared`/`config.values`/`partition_key` end with this block.
        let (first_page, should_fetch_more_pages) = {
            let prepared_ref = &config.prepared;
            let values_ref = &config.values;
            let page_query = |connection: Arc<Connection>,
                              consistency: Consistency,
                              paging_state: PagingState| async move {
                connection
                    .execute_raw_with_consistency(
                        prepared_ref,
                        values_ref,
                        consistency,
                        serial_consistency,
                        Some(page_size),
                        paging_state,
                    )
                    .await
            };

            let request_span = (executor.span_creator)(&partition_key);
            let outcome = executor
                .fetch_one_page(
                    &config.cluster_state,
                    &routing_info,
                    &PagingState::start(),
                    None,
                    &page_query,
                    &request_span,
                )
                .instrument(request_span.span().clone())
                .await
                .map_err(NextPageError::RequestFailure)?;
            executor.build_first_page(outcome, &request_span)?
        };

        // Required to end the borrow of `partition_key`, so `config` can be moved into the worker task.
        std::mem::drop(partition_key);

        /* PROCESS FIRST PAGE */
        let (sender, receiver) = mpsc::channel::<ResultNextPage>(1);
        match should_fetch_more_pages {
            ShouldFetchMorePages::NoMorePages => {
                // No more pages - we are done, return the first page and an empty receiver.
                std::mem::drop(sender);
            }
            ShouldFetchMorePages::MorePages {
                first_page_coordinator,
                first_page_connection,
                paging_state,
            } => {
                /* REMAINING PAGES */
                let parent_span = tracing::Span::current();
                let worker_task = async move {
                    let partition_key = if config.prepared.is_token_aware() {
                        match config.prepared.extract_partition_key(&config.values) {
                            Ok(res) => Some(res),
                            Err(err) => {
                                let _ = sender
                                    .send(Err(NextPageError::PartitionKeyError(
                                        PartitionKeyError::PartitionKeyExtraction(err),
                                    )))
                                    .await;
                                return;
                            }
                        }
                    } else {
                        None
                    };

                    let table_spec = config.prepared.get_table_spec();
                    let routing_info = RoutingInfo {
                        consistency,
                        serial_consistency,
                        token,
                        table: table_spec,
                        is_confirmed_lwt: config.prepared.is_confirmed_lwt(),
                        node_location_preference: &config.location_preference,
                    };

                    let prepared = &config.prepared;
                    let values_ref = &config.values;
                    let page_query =
                        |connection: Arc<Connection>,
                         consistency: Consistency,
                         paging_state: PagingState| async move {
                            connection
                                .execute_raw_with_consistency(
                                    prepared,
                                    values_ref,
                                    consistency,
                                    serial_consistency,
                                    Some(page_size),
                                    paging_state,
                                )
                                .await
                        };

                    executor
                        .fetch_remaining_pages(
                            config.cluster_state,
                            routing_info,
                            partition_key,
                            paging_state,
                            first_page_coordinator,
                            first_page_connection,
                            page_query,
                            sender,
                        )
                        .await;
                }
                .instrument(parent_span);
                let _worker_handle = tokio::task::spawn(worker_task);
            }
        };

        Self::new_from_first_page(first_page, receiver, session).await
    }

    pub(crate) async fn new_for_connection_execute_iter(
        prepared: PreparedStatement,
        values: SerializedValues,
        connection: Arc<Connection>,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<Self, NextPageError> {
        let page_size = prepared.get_validated_page_size();
        let request_timeout = prepared.get_request_timeout().or_else(|| {
            prepared
                .get_execution_profile_handle()?
                .access()
                .request_timeout
        });

        let executor = ConnectionPagingExecutor {
            connection,
            prepared,
            values,
            page_size,
            consistency,
            serial_consistency,
            request_timeout,
        };

        // Fetch the first page on the caller task (no spawning).
        let request_span = executor.make_span();
        let outcome = executor
            .fetch_one_page(&PagingState::start(), &request_span)
            .instrument(request_span.span().clone())
            .await
            .map_err(NextPageError::RequestFailure)?;
        let (first_page, paging_state_response) =
            ConnectionPagingExecutor::page_from_outcome(outcome)?;

        /* PROCESS FIRST PAGE */
        let (sender, receiver) = mpsc::channel::<ResultNextPage>(1);
        match paging_state_response {
            PagingStateResponse::NoMorePages => {
                // No more pages - we are done, return the first page and an empty receiver.
                std::mem::drop(sender);
            }
            PagingStateResponse::HasMorePages { state } => {
                /* REMAINING PAGES */
                let parent_span = tracing::Span::current();
                let worker_task =
                    async move { executor.fetch_remaining_pages(state, sender).await }
                        .instrument(parent_span);
                let _worker_handle = tokio::task::spawn(worker_task);
            }
        }

        let NextReceivedPage {
            rows,
            tracing_id,
            request_coordinator,
        } = first_page;

        Ok(Self {
            current_page: RawRowLendingIterator::new(rows),
            page_receiver: receiver,
            tracing_ids: Vec::from_iter(tracing_id),
            request_coordinators: Vec::from_iter(request_coordinator),
        })
    }

    async fn new_from_first_page(
        first_page: FirstReceivedPage,
        receiver: mpsc::Receiver<ResultNextPage>,
        session: &Session,
    ) -> Result<Self, PagerExecutionError> {
        let tracing_ids = Vec::from_iter(first_page.tracing_id);
        let coordinator_id = first_page.request_coordinator.node().host_id;
        let request_coordinators = vec![first_page.request_coordinator];

        let first_page = match first_page.content {
            FirstPageContent::Rows { rows } => RawRowLendingIterator::new(rows),
            FirstPageContent::SetKeyspace { set_keyspace } => {
                // If we are here, this means that we received a SET_KEYSPACE response as a first page.
                // This can happen when the user executes a "USE <keyspace>" statement.
                // Although it makes little sense to page over such a statement,
                // we must handle it gracefully. Especially that there may be users who execute
                // all statements in a paged manner (e.g., C# RS Driver).
                //
                // Let's set the keyspace on the session.
                let response = NonErrorQueryResponse {
                    response: NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SetKeyspace(set_keyspace),
                    ),
                    tracing_id: None,
                    warnings: Vec::new(),
                };
                session.handle_set_keyspace_response(&response).await?;
                // The stream will be empty.
                RawRowLendingIterator::new(DeserializedMetadataAndRawRows::mock_empty())
            }
            FirstPageContent::SchemaChange { schema_change } => {
                // If we are here, this means that we received a SCHEMA_CHANGE response as a first page.
                // This can happen when the user executes a DDL statement.
                // Although it makes little sense to page over such a statement,
                // we must handle it gracefully. Especially that there may be users who execute
                // all statements in a paged manner (e.g., C#-RS Driver).
                //
                // Let's await schema agreement, if Session is configured to do so.
                let response = NonErrorQueryResponse {
                    response: NonErrorResponseWithDeserializedMetadata::Result(
                        result::ResultWithDeserializedMetadata::SchemaChange(schema_change),
                    ),
                    tracing_id: None,
                    warnings: Vec::new(),
                };
                session
                    .handle_auto_await_schema_agreement(&response, coordinator_id)
                    .await?;
                // The stream will be empty.
                RawRowLendingIterator::new(DeserializedMetadataAndRawRows::mock_empty())
            }
        };

        Ok(Self {
            current_page: first_page,
            page_receiver: receiver,
            tracing_ids,
            request_coordinators,
        })
    }

    /// If tracing was enabled, returns tracing ids of all finished page queries.
    #[inline]
    pub fn tracing_ids(&self) -> &[Uuid] {
        &self.tracing_ids
    }

    /// Returns the targets that served finished page queries, in query order.
    #[inline]
    pub fn request_coordinators(&self) -> impl Iterator<Item = &Coordinator> {
        self.request_coordinators.iter()
    }

    /// Returns specification of row columns
    #[inline]
    pub fn column_specs(&self) -> ColumnSpecs<'_, '_> {
        ColumnSpecs::new(self.current_page.metadata().col_specs())
    }

    fn is_current_page_exhausted(&self) -> bool {
        self.current_page.rows_remaining() == 0
    }
}

/// Returned by [QueryPager::rows_stream].
///
/// Implements [Stream], but only permits deserialization of owned types.
/// To use [Stream] API (only accessible for owned types), use [QueryPager::rows_stream].
pub struct TypedRowStream<RowT> {
    raw_row_lending_stream: QueryPager,
    current_page_typechecked: bool,
    _phantom: std::marker::PhantomData<RowT>,
}

// Manual implementation not to depend on RowT implementing Debug.
// Explanation: automatic derive of Debug would impose the RowT: Debug
// constaint for the Debug impl.
impl<T> std::fmt::Debug for TypedRowStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedRowStream")
            .field("raw_row_lending_stream", &self.raw_row_lending_stream)
            .finish()
    }
}

impl<RowT> Unpin for TypedRowStream<RowT> {}

impl<RowT> TypedRowStream<RowT>
where
    RowT: for<'frame, 'metadata> DeserializeRow<'frame, 'metadata>,
{
    fn new(raw_stream: QueryPager) -> Result<Self, TypeCheckError> {
        #[allow(deprecated)] // In TypedRowStream we take care to type check each page.
        raw_stream.type_check::<RowT>()?;

        Ok(Self {
            raw_row_lending_stream: raw_stream,
            current_page_typechecked: true,
            _phantom: Default::default(),
        })
    }
}

impl<RowT> TypedRowStream<RowT> {
    /// If tracing was enabled, returns tracing ids of all finished page queries.
    #[inline]
    pub fn tracing_ids(&self) -> &[Uuid] {
        self.raw_row_lending_stream.tracing_ids()
    }

    /// Returns the targets that served finished page queries, in query order.
    #[inline]
    pub fn request_coordinators(&self) -> impl Iterator<Item = &Coordinator> {
        self.raw_row_lending_stream.request_coordinators()
    }

    /// Returns specification of row columns
    #[inline]
    pub fn column_specs(&self) -> ColumnSpecs<'_, '_> {
        self.raw_row_lending_stream.column_specs()
    }
}

/// Stream implementation for TypedRowStream.
///
/// It only works with owned types! For example, &str is not supported.
impl<RowT> Stream for TypedRowStream<RowT>
where
    RowT: DeserializeOwnedRow,
{
    type Item = Result<RowT, NextRowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next_fut = async {
            let real_self: &mut Self = &mut self; // Self is Unpin, and this lets us perform partial borrows.
            real_self.raw_row_lending_stream.next().await.map(|res| {
                res.and_then(|(column_iterator, fresh_page)| {
                    if fresh_page {
                        real_self.current_page_typechecked = false;
                    }
                    if !real_self.current_page_typechecked {
                        column_iterator.type_check::<RowT>().map_err(|e| {
                            NextRowError::NextPageError(NextPageError::TypeCheckError(e))
                        })?;
                        real_self.current_page_typechecked = true;
                    }
                    <RowT as DeserializeRow>::deserialize(column_iterator)
                        .map_err(NextRowError::RowDeserializationError)
                })
            })
        };

        futures::pin_mut!(next_fut);
        let value = ready_some_ok!(next_fut.poll(cx));
        Poll::Ready(Some(Ok(value)))
    }
}

/// An error returned that occurred during next page fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum NextPageError {
    /// PK extraction and/or token calculation error. Applies only for prepared statements.
    #[error("Failed to extract PK and compute token required for routing: {0}")]
    PartitionKeyError(#[from] PartitionKeyError),

    /// Failed to run a request responsible for fetching new page.
    #[error(transparent)]
    RequestFailure(#[from] RequestError),

    /// Failed to deserialize result metadata associated with next page response.
    #[error("Failed to deserialize result metadata associated with next page response: {0}")]
    ResultMetadataParseError(#[from] ResultMetadataAndRowsCountParseError),

    /// Failed to type check a received page.
    #[error("Failed to type check a received page: {0}")]
    TypeCheckError(#[from] TypeCheckError),
}

/// An error returned by async pager API.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum NextRowError {
    /// Failed to fetch next page of result.
    #[error("Failed to fetch next page of result: {0}")]
    NextPageError(#[from] NextPageError),

    /// An error occurred during row deserialization.
    #[error("Row deserialization error: {0}")]
    RowDeserializationError(#[from] DeserializationError),
}
