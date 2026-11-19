use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use clashmap::{ClashMap, Entry};
use safekeeper_api::models::PullTimelineRequest;
use safekeeper_client::mgmt_api;
use tokio::sync::{
    Semaphore,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{
    id::{NodeId, TenantId, TimelineId},
    logging::SecretString,
};

use crate::{
    metrics::{METRICS_REGISTRY, SafekeeperReconcilerLabelGroup},
    persistence::{SafekeeperTimelineOpKind, TimelinePendingOpPersistence, TimelinePersistence},
    safekeeper::Safekeeper,
    safekeeper_client::SafekeeperClient,
};

use super::Service;

pub(crate) struct SafekeeperReconcilers {
    cancel: CancellationToken,
    reconcilers: HashMap<NodeId, ReconcilerHandle>,
}

impl SafekeeperReconcilers {
    pub fn new(cancel: CancellationToken) -> Self {
        SafekeeperReconcilers {
            cancel,
            reconcilers: HashMap::new(),
        }
    }
    /// Adds a safekeeper-specific reconciler.
    /// Can be called multiple times, but it needs to be called at least once
    /// for every new safekeeper added.
    pub(crate) fn start_reconciler(&mut self, node_id: NodeId, service: &Arc<Service>) {
        self.reconcilers.entry(node_id).or_insert_with(|| {
            SafekeeperReconciler::spawn(self.cancel.child_token(), service.clone())
        });
    }
    /// Stop a safekeeper-specific reconciler.
    /// Stops the reconciler, cancelling all ongoing tasks.
    pub(crate) fn stop_reconciler(&mut self, node_id: NodeId) {
        if let Some(handle) = self.reconcilers.remove(&node_id) {
            handle.cancel.cancel();
        }
    }
    pub(crate) fn schedule_request_vec(&self, reqs: Vec<ScheduleRequest>) {
        tracing::info!(
            "Scheduling {} pending safekeeper ops loaded from db",
            reqs.len()
        );
        for req in reqs {
            self.schedule_request(req);
        }
    }
    pub(crate) fn schedule_request(&self, req: ScheduleRequest) {
        let node_id = req.safekeeper.get_id();
        let Some(reconciler_handle) = self.reconcilers.get(&node_id) else {
            // A safekeeper that is Paused, Decomissioned or still Activating has no reconciler
            // (`stop_reconciler` removes it when the scheduling policy changes). Moving a timeline
            // off such a safekeeper (a membership change) still asks for an Exclude on it, which
            // used to panic here and take the whole storage controller down.
            //
            // Nothing is lost by skipping the request: the operation is already persisted. It is
            // loaded again (`load_schedule_requests`) when the safekeeper becomes Active
            // (`set_safekeeper_scheduling_policy`) and at the next start of the controller.
            tracing::warn!(
                tenant_id = %req.tenant_id,
                timeline_id = ?req.timeline_id,
                kind = ?req.kind,
                "not scheduling a safekeeper operation: safekeeper {node_id} has no reconciler \
                (it is paused, decomissioned or not active); it stays pending in the database until it is active again"
            );
            return;
        };
        reconciler_handle.schedule_reconcile(req);
    }
    /// Cancel ongoing reconciles for the given timeline
    ///
    /// Specifying `None` here only removes reconciles for the tenant-global reconciliation,
    /// instead of doing this for all timelines of the tenant.
    ///
    /// Callers must remove the reconciles from the db manually
    pub(crate) fn cancel_reconciles_for_timeline(
        &mut self,
        node_id: NodeId,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
    ) {
        if let Some(handle) = self.reconcilers.get(&node_id) {
            handle.cancel_reconciliation(tenant_id, timeline_id);
        }
    }
}

/// Whether a pending `Exclude` of `sk_id` from a timeline is stale: the safekeeper is a member of the
/// timeline's current set (`sk_set`) or of the set it is moving to (`new_sk_set`). An exclusion is run as a
/// delete of the timeline on that safekeeper, and the safekeeper's delete carries no generation, so an
/// exclusion queued while the safekeeper was out of the timeline must never run once it is back in.
pub(crate) fn exclude_is_stale(timeline: &TimelinePersistence, sk_id: NodeId) -> bool {
    let id = sk_id.0 as i64;
    timeline.sk_set.contains(&id)
        || timeline
            .new_sk_set
            .as_ref()
            .is_some_and(|set| set.contains(&id))
}

/// What to do with the pending operations read from the db.
pub(crate) struct PlannedOps {
    /// Requests to schedule.
    pub(crate) requests: Vec<ScheduleRequest>,
    /// Stale `Exclude`s: to be dropped, row included, and not run.
    pub(crate) stale: Vec<TimelinePendingOpPersistence>,
}

/// Turns pending operations (joined with their timelines) into requests, dropping the stale `Exclude`s.
/// `only` limits it to one safekeeper's operations. Pure: it reads and writes nothing.
pub(crate) fn plan_schedule_requests(
    pending_ops_timelines: Vec<(TimelinePendingOpPersistence, Option<TimelinePersistence>)>,
    safekeepers: &HashMap<NodeId, Safekeeper>,
    only: Option<NodeId>,
) -> anyhow::Result<PlannedOps> {
    let mut res = Vec::with_capacity(pending_ops_timelines.len());
    let mut stale = Vec::new();
    for (op_persist, timeline_persist) in pending_ops_timelines {
        let node_id = NodeId(op_persist.sk_id as u64);
        if only.is_some_and(|only| only != node_id) {
            continue;
        }
        let Some(sk) = safekeepers.get(&node_id) else {
            // This shouldn't happen, at least the safekeeper should exist as decomissioned.
            tracing::warn!(
                tenant_id = op_persist.tenant_id,
                timeline_id = op_persist.timeline_id,
                "couldn't find safekeeper with pending op id {node_id} in list of stored safekeepers"
            );
            continue;
        };
        if op_persist.op_kind == SafekeeperTimelineOpKind::Exclude
            && timeline_persist
                .as_ref()
                .is_some_and(|timeline| exclude_is_stale(timeline, node_id))
        {
            tracing::info!(
                tenant_id = op_persist.tenant_id,
                timeline_id = op_persist.timeline_id,
                "dropping a pending exclusion of safekeeper {node_id}: it is a member of the timeline again"
            );
            stale.push(op_persist);
            continue;
        }
        let sk = Box::new(sk.clone());
        let tenant_id = TenantId::from_str(&op_persist.tenant_id)?;
        let timeline_id = if !op_persist.timeline_id.is_empty() {
            Some(TimelineId::from_str(&op_persist.timeline_id)?)
        } else {
            None
        };
        let host_list = match op_persist.op_kind {
            SafekeeperTimelineOpKind::Delete => Vec::new(),
            SafekeeperTimelineOpKind::Exclude => Vec::new(),
            SafekeeperTimelineOpKind::Pull => {
                if timeline_id.is_none() {
                    // We only do this extra check (outside of timeline_persist check) to give better error msgs
                    anyhow::bail!(
                        "timeline_id is empty for `pull` schedule request for {tenant_id}"
                    );
                };
                let Some(timeline_persist) = timeline_persist else {
                    // This shouldn't happen, the timeline should still exist
                    tracing::warn!(
                        tenant_id = op_persist.tenant_id,
                        timeline_id = op_persist.timeline_id,
                        "couldn't find timeline for corresponding pull op"
                    );
                    continue;
                };
                timeline_persist
                    .sk_set
                    .iter()
                    .filter_map(|sk_id| {
                        let other_node_id = NodeId(*sk_id as u64);
                        if node_id == other_node_id {
                            // We obviously don't want to pull from ourselves
                            return None;
                        }
                        let Some(sk) = safekeepers.get(&other_node_id) else {
                            tracing::warn!(
                                "couldn't find safekeeper with pending op id {other_node_id}, not pulling from it"
                            );
                            return None;
                        };
                        Some((other_node_id, sk.base_url()))
                    })
                    .collect::<Vec<_>>()
            }
        };
        let req = ScheduleRequest {
            safekeeper: sk,
            host_list,
            tenant_id,
            timeline_id,
            generation: op_persist.generation as u32,
            kind: op_persist.op_kind,
        };
        res.push(req);
    }
    Ok(PlannedOps {
        requests: res,
        stale,
    })
}

/// Load the pending operations from the db (at the start of the controller, or for one safekeeper that
/// is active again: `only`), drop the stale `Exclude`s with their rows, and return the requests.
pub(crate) async fn load_schedule_requests(
    service: &Arc<Service>,
    safekeepers: &HashMap<NodeId, Safekeeper>,
    only: Option<NodeId>,
) -> anyhow::Result<Vec<ScheduleRequest>> {
    let pending_ops_timelines = service
        .persistence
        .list_pending_ops_with_timelines()
        .await?;
    let planned = plan_schedule_requests(pending_ops_timelines, safekeepers, only)?;
    for op in planned.stale {
        let tenant_id = TenantId::from_str(&op.tenant_id)?;
        let timeline_id = TimelineId::from_str(&op.timeline_id)?;
        service
            .persistence
            .remove_pending_op(
                tenant_id,
                Some(timeline_id),
                NodeId(op.sk_id as u64),
                op.generation as u32,
            )
            .await?;
    }
    Ok(planned.requests)
}

pub(crate) struct ScheduleRequest {
    pub(crate) safekeeper: Box<Safekeeper>,
    pub(crate) host_list: Vec<(NodeId, String)>,
    pub(crate) tenant_id: TenantId,
    pub(crate) timeline_id: Option<TimelineId>,
    pub(crate) generation: u32,
    pub(crate) kind: SafekeeperTimelineOpKind,
}

/// A way to keep ongoing/queued reconcile requests apart
#[derive(Copy, Clone, PartialEq, Eq)]
struct TokenId(u64);

type OngoingTokens = ClashMap<(TenantId, Option<TimelineId>), (CancellationToken, TokenId)>;

/// Handle to per safekeeper reconciler.
struct ReconcilerHandle {
    tx: UnboundedSender<(ScheduleRequest, CancellationToken, TokenId)>,
    ongoing_tokens: Arc<OngoingTokens>,
    token_id_counter: AtomicU64,
    cancel: CancellationToken,
}

impl ReconcilerHandle {
    /// Obtain a new token slot, cancelling any existing reconciliations for
    /// that timeline. It is not useful to have >1 operation per <tenant_id,
    /// timeline_id, safekeeper>, hence scheduling op cancels current one if it
    /// exists.
    fn new_token_slot(
        &self,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
    ) -> (CancellationToken, TokenId) {
        let token_id = self
            .token_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let token_id = TokenId(token_id);
        let entry = self.ongoing_tokens.entry((tenant_id, timeline_id));
        if let Entry::Occupied(entry) = &entry {
            let (cancel, _) = entry.get();
            cancel.cancel();
        }
        entry.insert((self.cancel.child_token(), token_id)).clone()
    }
    /// Cancel an ongoing reconciliation
    fn cancel_reconciliation(&self, tenant_id: TenantId, timeline_id: Option<TimelineId>) {
        if let Some((_, (cancel, _id))) = self.ongoing_tokens.remove(&(tenant_id, timeline_id)) {
            cancel.cancel();
        }
    }
    fn schedule_reconcile(&self, req: ScheduleRequest) {
        let (cancel, token_id) = self.new_token_slot(req.tenant_id, req.timeline_id);
        let hostname = req.safekeeper.skp.host.clone();
        let sk_az = req.safekeeper.skp.availability_zone_id.clone();
        let sk_node_id = req.safekeeper.get_id().to_string();

        // We don't have direct access to the queue depth here, so increase it blindly by 1.
        // We know that putting into the queue increases the queue depth. The receiver will
        // update with the correct value once it processes the next item. To avoid races where we
        // reduce before we increase, leaving the gauge with a 1 value for a long time, we
        // increase it before putting into the queue.
        let queued_gauge = &METRICS_REGISTRY
            .metrics_group
            .storage_controller_safekeeper_reconciles_queued;
        let label_group = SafekeeperReconcilerLabelGroup {
            sk_az: &sk_az,
            sk_node_id: &sk_node_id,
            sk_hostname: &hostname,
        };
        queued_gauge.inc(label_group.clone());

        if let Err(err) = self.tx.send((req, cancel, token_id)) {
            queued_gauge.set(label_group, 0);
            tracing::info!("scheduling request onto {hostname} returned error: {err}");
        }
    }
}

pub(crate) struct SafekeeperReconciler {
    inner: SafekeeperReconcilerInner,
    concurrency_limiter: Arc<Semaphore>,
    rx: UnboundedReceiver<(ScheduleRequest, CancellationToken, TokenId)>,
    cancel: CancellationToken,
}

/// Thin wrapper over `Service` to not clutter its inherent functions
#[derive(Clone)]
struct SafekeeperReconcilerInner {
    ongoing_tokens: Arc<OngoingTokens>,
    service: Arc<Service>,
}

impl SafekeeperReconciler {
    fn spawn(cancel: CancellationToken, service: Arc<Service>) -> ReconcilerHandle {
        // We hold the ServiceInner lock so we don't want to make sending to the reconciler channel to be blocking.
        let (tx, rx) = mpsc::unbounded_channel();
        let concurrency = service.config.safekeeper_reconciler_concurrency;
        let ongoing_tokens = Arc::new(ClashMap::new());
        let mut reconciler = SafekeeperReconciler {
            inner: SafekeeperReconcilerInner {
                service,
                ongoing_tokens: ongoing_tokens.clone(),
            },
            rx,
            concurrency_limiter: Arc::new(Semaphore::new(concurrency)),
            cancel: cancel.clone(),
        };
        let handle = ReconcilerHandle {
            tx,
            ongoing_tokens,
            token_id_counter: AtomicU64::new(0),
            cancel,
        };
        tokio::spawn(async move { reconciler.run().await });
        handle
    }
    async fn run(&mut self) {
        loop {
            let req = tokio::select! {
                req = self.rx.recv() => req,
                _ = self.cancel.cancelled() => break,
            };
            let Some((req, req_cancel, req_token_id)) = req else {
                break;
            };

            let permit_res = tokio::select! {
                req = self.concurrency_limiter.clone().acquire_owned() => req,
                _ = self.cancel.cancelled() => break,
            };
            let Ok(_permit) = permit_res else { return };

            let inner = self.inner.clone();
            if req_cancel.is_cancelled() {
                continue;
            }

            let queued_gauge = &METRICS_REGISTRY
                .metrics_group
                .storage_controller_safekeeper_reconciles_queued;
            queued_gauge.set(
                SafekeeperReconcilerLabelGroup {
                    sk_az: &req.safekeeper.skp.availability_zone_id,
                    sk_node_id: &req.safekeeper.get_id().to_string(),
                    sk_hostname: &req.safekeeper.skp.host,
                },
                self.rx.len() as i64,
            );

            tokio::task::spawn(async move {
                let kind = req.kind;
                let tenant_id = req.tenant_id;
                let timeline_id = req.timeline_id;
                let node_id = req.safekeeper.skp.id;
                inner
                    .reconcile_one(req, req_cancel, req_token_id)
                    .instrument(tracing::info_span!(
                        "reconcile_one",
                        ?kind,
                        %tenant_id,
                        ?timeline_id,
                        %node_id,
                    ))
                    .await;
            });
        }
    }
}

impl SafekeeperReconcilerInner {
    async fn reconcile_one(
        &self,
        req: ScheduleRequest,
        req_cancel: CancellationToken,
        req_token_id: TokenId,
    ) {
        let req_host = req.safekeeper.skp.host.clone();
        let success;
        match req.kind {
            SafekeeperTimelineOpKind::Pull => {
                let Some(timeline_id) = req.timeline_id else {
                    tracing::warn!(
                        "ignoring invalid schedule request: timeline_id is empty for `pull`"
                    );
                    return;
                };
                let our_id = req.safekeeper.get_id();
                let http_hosts = req
                    .host_list
                    .iter()
                    .filter(|(node_id, _hostname)| *node_id != our_id)
                    .map(|(_, hostname)| hostname.clone())
                    .collect::<Vec<_>>();
                let pull_req = PullTimelineRequest {
                    http_hosts,
                    tenant_id: req.tenant_id,
                    timeline_id,
                    // TODO(diko): get mconf from "timelines" table and pass it here.
                    // Now we use pull_timeline reconciliation only for the timeline creation,
                    // so it's not critical right now.
                    // It could be fixed together with other reconciliation issues:
                    // https://github.com/neondatabase/neon/issues/12189
                    mconf: None,
                };
                success = self
                    .reconcile_inner(
                        &req,
                        async |client| client.pull_timeline(&pull_req).await,
                        |resp| {
                            if let Some(host) = resp.safekeeper_host {
                                tracing::info!("pulled timeline from {host} onto {req_host}");
                            } else {
                                tracing::info!(
                                    "timeline already present on safekeeper on {req_host}"
                                );
                            }
                        },
                        req_cancel,
                    )
                    .await;
            }
            SafekeeperTimelineOpKind::Exclude => {
                // TODO actually exclude instead of delete here
                let tenant_id = req.tenant_id;
                let Some(timeline_id) = req.timeline_id else {
                    tracing::warn!(
                        "ignoring invalid schedule request: timeline_id is empty for `exclude`"
                    );
                    return;
                };
                // Check again just before the delete: the exclusion may have been queued long ago, and
                // the safekeeper may have rejoined the timeline since (see `exclude_is_stale`).
                match self
                    .service
                    .persistence
                    .get_timeline(tenant_id, timeline_id)
                    .await
                {
                    Ok(Some(timeline)) if exclude_is_stale(&timeline, req.safekeeper.get_id()) => {
                        tracing::info!(
                            "not deleting the timeline from {req_host}: it is a member of the timeline again; dropping the exclusion"
                        );
                        if let Err(err) = self
                            .service
                            .persistence
                            .remove_pending_op(
                                tenant_id,
                                Some(timeline_id),
                                req.safekeeper.get_id(),
                                req.generation,
                            )
                            .await
                        {
                            tracing::warn!("couldn't remove the stale pending op: {err}");
                        }
                        return;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        // Do not delete on a guess: the op stays pending and is loaded again later.
                        tracing::warn!(
                            "couldn't read the timeline before an exclusion, not running it: {err}"
                        );
                        return;
                    }
                }
                success = self
                    .reconcile_inner(
                        &req,
                        async |client| client.delete_timeline(tenant_id, timeline_id).await,
                        |_resp| {
                            tracing::info!("deleted timeline from {req_host}");
                        },
                        req_cancel,
                    )
                    .await;
            }
            SafekeeperTimelineOpKind::Delete => {
                let tenant_id = req.tenant_id;
                if let Some(timeline_id) = req.timeline_id {
                    success = self
                        .reconcile_inner(
                            &req,
                            async |client| client.delete_timeline(tenant_id, timeline_id).await,
                            |_resp| {
                                tracing::info!("deleted timeline from {req_host}");
                            },
                            req_cancel,
                        )
                        .await;
                    if success {
                        self.delete_timeline_from_db(tenant_id, timeline_id).await;
                    }
                } else {
                    success = self
                        .reconcile_inner(
                            &req,
                            async |client| client.delete_tenant(tenant_id).await,
                            |_resp| {
                                tracing::info!(%tenant_id, "deleted tenant from {req_host}");
                            },
                            req_cancel,
                        )
                        .await;
                    if success {
                        self.delete_tenant_timelines_from_db(tenant_id).await;
                    }
                }
            }
        }
        if success {
            self.ongoing_tokens.remove_if(
                &(req.tenant_id, req.timeline_id),
                |_ttid, (_cancel, token_id)| {
                    // Ensure that this request is indeed the request we just finished and not a new one
                    req_token_id == *token_id
                },
            );
        }
    }
    async fn delete_timeline_from_db(&self, tenant_id: TenantId, timeline_id: TimelineId) {
        match self
            .service
            .persistence
            .list_pending_ops_for_timeline(tenant_id, timeline_id)
            .await
        {
            Ok(list) => {
                if !list.is_empty() {
                    // duplicate the timeline_id here because it might be None in the reconcile context
                    tracing::info!(%timeline_id, "not deleting timeline from db as there is {} open reconciles", list.len());
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(%timeline_id, "couldn't query pending ops: {e}");
                return;
            }
        }
        tracing::info!(%tenant_id, %timeline_id, "deleting timeline from db after all reconciles succeeded");
        // In theory we could crash right after deleting the op from the db and right before reaching this,
        // but then we'll boot up with a timeline that has deleted_at set, so hopefully we'll issue deletion ops for it again.
        if let Err(err) = self
            .service
            .persistence
            .delete_timeline(tenant_id, timeline_id)
            .await
        {
            tracing::warn!(%tenant_id, %timeline_id, "couldn't delete timeline from db: {err}");
        }
    }
    async fn delete_tenant_timelines_from_db(&self, tenant_id: TenantId) {
        let timeline_list = match self
            .service
            .persistence
            .list_timelines_for_tenant(tenant_id)
            .await
        {
            Ok(timeline_list) => timeline_list,
            Err(e) => {
                tracing::warn!(%tenant_id, "couldn't query timelines: {e}");
                return;
            }
        };
        for timeline in timeline_list {
            let Ok(timeline_id) = TimelineId::from_str(&timeline.timeline_id) else {
                tracing::warn!("Invalid timeline ID in database {}", timeline.timeline_id);
                continue;
            };
            self.delete_timeline_from_db(tenant_id, timeline_id).await;
        }
    }
    /// Returns whether the reconciliation happened successfully (or we got cancelled)
    async fn reconcile_inner<T, F, U>(
        &self,
        req: &ScheduleRequest,
        closure: impl Fn(SafekeeperClient) -> F,
        log_success: impl FnOnce(T) -> U,
        req_cancel: CancellationToken,
    ) -> bool
    where
        F: Future<Output = Result<T, safekeeper_client::mgmt_api::Error>>,
    {
        let jwt = self
            .service
            .config
            .safekeeper_jwt_token
            .clone()
            .map(SecretString::from);
        loop {
            let res = req
                .safekeeper
                .with_client_retries(
                    |client| {
                        let closure = &closure;
                        async move { closure(client).await }
                    },
                    self.service.get_http_client(),
                    &jwt,
                    3,
                    10,
                    Duration::from_secs(10),
                    &req_cancel,
                )
                .await;
            match res {
                Ok(resp) => {
                    log_success(resp);
                    let res = self
                        .service
                        .persistence
                        .remove_pending_op(
                            req.tenant_id,
                            req.timeline_id,
                            req.safekeeper.get_id(),
                            req.generation,
                        )
                        .await;

                    let complete_counter = &METRICS_REGISTRY
                        .metrics_group
                        .storage_controller_safekeeper_reconciles_complete;
                    complete_counter.inc(SafekeeperReconcilerLabelGroup {
                        sk_az: &req.safekeeper.skp.availability_zone_id,
                        sk_node_id: &req.safekeeper.get_id().to_string(),
                        sk_hostname: &req.safekeeper.skp.host,
                    });

                    if let Err(err) = res {
                        tracing::info!(
                            "couldn't remove reconciliation request onto {} from persistence: {err:?}",
                            req.safekeeper.skp.host
                        );
                    }
                    return true;
                }
                Err(mgmt_api::Error::Cancelled) => {
                    // On cancellation, the code that issued it will take care of removing db entries (if needed)
                    return false;
                }
                Err(e) => {
                    tracing::info!(
                        "Reconcile attempt for safekeeper {} failed, retrying after sleep: {e:?}",
                        req.safekeeper.skp.host
                    );
                    const SLEEP_TIME: Duration = Duration::from_secs(1);
                    tokio::time::sleep(SLEEP_TIME).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pageserver_api::controller_api::SkSchedulingPolicy;
    use utils::lsn::Lsn;

    use super::*;
    use crate::persistence::LsnWrapper;

    fn test_safekeeper(id: i64, policy: SkSchedulingPolicy) -> Safekeeper {
        Safekeeper::from_persistence(
            crate::persistence::SafekeeperPersistence {
                id,
                region_id: "test".to_string(),
                version: 1,
                host: format!("safekeeper-{id}.test"),
                port: 5454,
                http_port: 7676,
                availability_zone_id: format!("sk-{id}"),
                scheduling_policy: policy.into(),
                https_port: None,
            },
            CancellationToken::new(),
            false,
        )
        .expect("a test safekeeper loads")
    }

    fn test_timeline(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        sk_set: &[i64],
        new_sk_set: Option<&[i64]>,
    ) -> TimelinePersistence {
        TimelinePersistence {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            start_lsn: LsnWrapper(Lsn(0)),
            generation: 1,
            sk_set: sk_set.to_vec(),
            new_sk_set: new_sk_set.map(|s| s.to_vec()),
            cplane_notified_generation: 0,
            deleted_at: None,
            sk_set_notified_generation: 0,
        }
    }

    fn test_op(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        sk_id: i64,
        kind: SafekeeperTimelineOpKind,
    ) -> TimelinePendingOpPersistence {
        TimelinePendingOpPersistence {
            sk_id,
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            generation: 3,
            op_kind: kind,
        }
    }

    fn safekeepers(ids: &[i64]) -> HashMap<NodeId, Safekeeper> {
        ids.iter()
            .map(|id| {
                (
                    NodeId(*id as u64),
                    test_safekeeper(*id, SkSchedulingPolicy::Pause),
                )
            })
            .collect()
    }

    /// Scheduling an operation for a safekeeper that has no reconciler (it is Paused or Decomissioned)
    /// must not panic: it happens when a timeline is moved off such a safekeeper.
    #[test]
    fn schedule_request_for_a_safekeeper_without_reconciler_does_not_panic() {
        let reconcilers = SafekeeperReconcilers::new(CancellationToken::new());
        reconcilers.schedule_request(ScheduleRequest {
            safekeeper: Box::new(test_safekeeper(3, SkSchedulingPolicy::Pause)),
            host_list: Vec::new(),
            tenant_id: TenantId::generate(),
            timeline_id: Some(TimelineId::generate()),
            generation: 3,
            kind: SafekeeperTimelineOpKind::Exclude,
        });
    }

    /// A skipped request leaves the persisted operation as it was: planning again from the same rows
    /// (which is what happens when the safekeeper becomes Active, or at the next start) yields it.
    #[test]
    fn a_skipped_exclude_is_planned_again_from_the_database() {
        let (tenant, timeline) = (TenantId::generate(), TimelineId::generate());
        let rows = || {
            vec![(
                test_op(tenant, timeline, 3, SafekeeperTimelineOpKind::Exclude),
                Some(test_timeline(tenant, timeline, &[1, 2, 4], None)),
            )]
        };
        let sks = safekeepers(&[1, 2, 3, 4]);
        // the skip: no reconciler, nothing happens to the rows
        SafekeeperReconcilers::new(CancellationToken::new()).schedule_request(
            plan_schedule_requests(rows(), &sks, None)
                .unwrap()
                .requests
                .pop()
                .unwrap(),
        );
        // later: the same rows give the same request, and nothing is dropped
        let planned = plan_schedule_requests(rows(), &sks, Some(NodeId(3))).unwrap();
        assert_eq!(planned.requests.len(), 1);
        assert_eq!(planned.requests[0].kind, SafekeeperTimelineOpKind::Exclude);
        assert_eq!(planned.requests[0].safekeeper.get_id(), NodeId(3));
        assert!(planned.stale.is_empty());
    }

    /// The exclusion of a safekeeper that is a member of the timeline again (in its current set, or in the
    /// set it is moving to) is dropped and no request is made, so nothing is deleted on that safekeeper.
    #[test]
    fn a_stale_exclude_for_a_current_member_is_dropped_and_deletes_nothing() {
        let (tenant, timeline) = (TenantId::generate(), TimelineId::generate());
        let sks = safekeepers(&[1, 2, 3, 4]);
        for (sk_set, new_sk_set) in [
            (vec![1, 2, 3], None),                // moved back
            (vec![1, 2, 4], Some(vec![1, 2, 3])), // moving back
        ] {
            let rows = vec![(
                test_op(tenant, timeline, 3, SafekeeperTimelineOpKind::Exclude),
                Some(test_timeline(
                    tenant,
                    timeline,
                    &sk_set,
                    new_sk_set.as_deref(),
                )),
            )];
            let planned = plan_schedule_requests(rows, &sks, None).unwrap();
            assert!(
                planned.requests.is_empty(),
                "no request: {sk_set:?} {new_sk_set:?}"
            );
            assert_eq!(planned.stale.len(), 1);
            assert_eq!(planned.stale[0].sk_id, 3);
        }
    }

    /// Only an `Exclude` is checked against the sets, and only a safekeeper's own operations are planned
    /// when one safekeeper becomes Active.
    #[test]
    fn only_excludes_are_dropped_and_only_the_named_safekeeper_is_planned() {
        let (tenant, timeline) = (TenantId::generate(), TimelineId::generate());
        let sks = safekeepers(&[1, 2, 3, 4]);
        let tl = || Some(test_timeline(tenant, timeline, &[1, 2, 3], None));
        let rows = vec![
            // a Delete of a member's copy is not an exclusion: it is planned
            (
                test_op(tenant, timeline, 3, SafekeeperTimelineOpKind::Delete),
                tl(),
            ),
            // an exclusion of a safekeeper that is not a member: planned
            (
                test_op(tenant, timeline, 4, SafekeeperTimelineOpKind::Exclude),
                tl(),
            ),
            // an exclusion of a member: dropped
            (
                test_op(tenant, timeline, 2, SafekeeperTimelineOpKind::Exclude),
                tl(),
            ),
        ];
        let all = plan_schedule_requests(rows, &sks, None).unwrap();
        assert_eq!(all.requests.len(), 2);
        assert_eq!(all.stale.len(), 1);
        assert_eq!(all.stale[0].sk_id, 2);

        let rows = vec![
            (
                test_op(tenant, timeline, 4, SafekeeperTimelineOpKind::Exclude),
                tl(),
            ),
            (
                test_op(tenant, timeline, 3, SafekeeperTimelineOpKind::Delete),
                tl(),
            ),
        ];
        let only4 = plan_schedule_requests(rows, &sks, Some(NodeId(4))).unwrap();
        assert_eq!(only4.requests.len(), 1);
        assert_eq!(only4.requests[0].safekeeper.get_id(), NodeId(4));
    }
}
