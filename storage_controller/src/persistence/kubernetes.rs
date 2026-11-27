//! The storage controller's records in Kubernetes objects (`storage_controller/kubernetes/`).
//!
//! Every operation of [`super::Persistence`] is done against the API server, never a cache: a
//! read is a quorum read, and every write is a compare-and-swap of one object on its
//! `resourceVersion` (a create fails if the object exists, a delete carries the version it read),
//! retried on a conflict as a serialization failure is retried on Postgres. The records are
//! grouped one object per tenant, holding all its shards, so every multi-row transaction Neon's
//! controller relies on for generations (an increment, a split's begin, completion and abort,
//! one tenant's part of a re-attach) is one write of one object.

pub(crate) mod objects;

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use kube::api::{Api, DeleteParams, ListParams, PostParams, Preconditions};
use pageserver_api::controller_api::{
    AvailabilityZone, NodeLifecycle, NodeSchedulingPolicy, PlacementPolicy, ShardSchedulingPolicy,
    SkSchedulingPolicy,
};
use pageserver_api::models::{ShardImportStatus, TenantConfig};
use pageserver_api::shard::{ShardCount, ShardNumber, TenantShardId};
use safekeeper_api::membership::SafekeeperGeneration;
use utils::generation::Generation;
use utils::id::{NodeId, TenantId, TimelineId};

use self::objects::{
    HealthRecord, ImportRecord, LEADER_OBJECT_NAME, LeaderSpec, Membership, MetadataHealthSpec,
    Object, PageserverSpec, PendingOp, SafekeeperSpec, ShardRecord, Spec, TenantSpec, TimelineSpec,
    generation_label, pageserver_object_name, safekeeper_object_name, timeline_object_name,
};
use super::split_state::SplitState;
use super::{
    AbortShardSplitStatus, ControllerPersistence, DatabaseError, DatabaseOperation, DatabaseResult,
    MAX_RETRIES, MetadataHealthPersistence, NodePersistence, SafekeeperPersistence,
    SafekeeperUpsert, ShardGenerationState, TenantFilter, TenantShardPersistence,
    TimelineImportPersistence, TimelinePendingOpPersistence, TimelinePersistence, TimelineUpdate,
};
use crate::metrics::{
    DatabaseQueryErrorLabelGroup, DatabaseQueryLatencyLabelGroup, METRICS_REGISTRY,
};
use crate::node::Node;
use crate::timeline_import::{
    TimelineImport, TimelineImportUpdateError, TimelineImportUpdateFollowUp,
};

/// How many tenants of one pageserver's re-attach are written at once.
const RE_ATTACH_CONCURRENCY: usize = 32;

/// The page size of a list.
const LIST_PAGE: u32 = 500;

pub(crate) struct KubernetesStore {
    client: kube::Client,
    namespace: String,
    guard: Guard,
    /// The page size of a list (smaller in tests, to read across pages).
    list_page: u32,
    /// One write at a time per object within this controller (`storage_controller/kubernetes/
    /// README.md`): a tenant with many shards reconciling at once queues here instead of
    /// spending its retries on conflicts with itself. The compare-and-swap still guards against
    /// a second controller.
    locks: Locks,
}

type Locks = std::sync::Mutex<HashMap<(&'static str, String), Arc<tokio::sync::Mutex<()>>>>;

/// What this controller has seen of each object, so a read that finds one older than that (the
/// cluster's store taken back under it) is caught before anything is written or answered from it.
///
/// A read is compared only with what was seen before it was sent: a write this controller makes
/// while a read is in flight is not a reason to refuse the read's older answer. Every note of an
/// object takes a sequence number, and a read takes the number current when it is sent.
struct Guard {
    seen: std::sync::Mutex<HashMap<(&'static str, String), Seen>>,
    /// The sequence of notes.
    sequence: std::sync::atomic::AtomicU64,
    /// Exit the process when the records went back (the controller); never in tests, which take
    /// the error instead.
    exit: bool,
}

/// How many notes of one object are kept: the changes one read can overlap (a read at an
/// unchanged revision adds none). A read that overlapped more is not compared for that object;
/// the next read is.
const SEEN_HISTORY: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Noted {
    /// Read or written at the revision.
    Live,
    /// Deleted by this controller, at the revision.
    Deleted,
    /// A delete of it at the revision was sent and its outcome is not known (found already
    /// gone, or no definite answer: a timeout, a lost connection, a server error): it may be
    /// there still, or gone.
    InDoubt,
}

#[derive(Clone, Copy)]
struct Note {
    sequence: u64,
    revision: u64,
    noted: Noted,
}

#[derive(Default)]
struct Seen {
    /// The latest notes, oldest first; their revisions never go down.
    notes: std::collections::VecDeque<Note>,
}

impl Seen {
    fn latest(&self) -> Option<Note> {
        self.notes.back().copied()
    }

    /// What was seen when a read sent at `sent` went out: the latest note no later than it.
    fn as_of(&self, sent: u64) -> Option<Note> {
        self.notes
            .iter()
            .rev()
            .find(|n| n.sequence <= sent)
            .copied()
    }

    /// A delete (done, or in doubt) noted at or after `from`.
    fn delete_since(&self, from: u64) -> bool {
        self.notes
            .iter()
            .any(|n| n.sequence >= from && n.noted != Noted::Live)
    }
}

impl Guard {
    fn new() -> Self {
        Guard {
            seen: Default::default(),
            sequence: Default::default(),
            exit: !cfg!(test),
        }
    }

    /// The sequence number a read is sent at.
    fn before_read(&self) -> u64 {
        self.sequence.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn went_back(&self, what: String) -> DatabaseError {
        tracing::error!(
            "The records went back under this controller ({what}): the cluster's store was \
             taken back, or lost objects. Writing nothing more and exiting; the next start \
             checks every record against the copy."
        );
        if self.exit {
            std::process::exit(1);
        }
        DatabaseError::Logical(format!("the records went back: {what}"))
    }

    /// Note an object at a revision. Not noted: a revision below the latest note (a read never
    /// lowers what was seen), the same as the latest note (so reads alone do not fill the
    /// history), and a live read at the revision of a delete, done or in doubt (the read may
    /// have been answered from before the delete; only a new revision shows the name live again).
    fn note(&self, kind: &'static str, name: &str, revision: u64, noted: Noted) {
        let mut seen = self.seen.lock().unwrap();
        let s = seen.entry((kind, name.to_string())).or_default();
        if let Some(l) = s.latest() {
            if revision < l.revision
                || (revision == l.revision && (noted == l.noted || noted == Noted::Live))
            {
                return;
            }
        }
        let sequence = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        s.notes.push_back(Note {
            sequence,
            revision,
            noted,
        });
        while s.notes.len() > SEEN_HISTORY {
            s.notes.pop_front();
        }
    }

    /// Check an object returned by a read sent at `sent` against what was seen before it.
    fn observe<S: Spec>(&self, obj: &Object<S>, sent: u64) -> DatabaseResult<()> {
        let revision = obj.revision()?;
        let before = self
            .seen
            .lock()
            .unwrap()
            .get(&(S::KIND, obj.name().to_string()))
            .and_then(|s| s.as_of(sent));
        if let Some(b) = before {
            let deleted_here = b.noted == Noted::Deleted && revision == b.revision;
            if revision < b.revision || deleted_here {
                return Err(self.went_back(format!(
                    "{} {} at revision {revision}, this controller saw {}{}",
                    S::KIND,
                    obj.name(),
                    b.revision,
                    if b.noted == Noted::Deleted {
                        " and deleted it"
                    } else {
                        ""
                    }
                )));
            }
        }
        self.note(S::KIND, obj.name(), revision, Noted::Live);
        Ok(())
    }

    /// Check that an object a read sent at `sent` found absent was not one this controller saw
    /// live before it, and has not deleted (or tried to) since: any delete noted from the read's
    /// baseline on makes the absence possible (deleted, then perhaps made again).
    fn observe_absent<S: Spec>(&self, name: &str, sent: u64) -> DatabaseResult<()> {
        let gone = {
            let seen = self.seen.lock().unwrap();
            match seen.get(&(S::KIND, name.to_string())) {
                Some(s) => match s.as_of(sent) {
                    Some(b) if b.noted == Noted::Live && !s.delete_since(b.sequence) => {
                        Some(b.revision)
                    }
                    _ => None,
                },
                None => None,
            }
        };
        match gone {
            Some(revision) => Err(self.went_back(format!(
                "{} {name} is gone, this controller saw it at revision {revision} and did not delete it",
                S::KIND
            ))),
            None => Ok(()),
        }
    }

    fn wrote<S: Spec>(&self, name: &str, revision: u64, deleted: bool) {
        let noted = if deleted { Noted::Deleted } else { Noted::Live };
        self.note(S::KIND, name, revision, noted);
    }

    /// A delete at `revision` whose outcome is not known (found already gone, or no definite
    /// answer).
    fn delete_in_doubt<S: Spec>(&self, name: &str, revision: u64) {
        self.note(S::KIND, name, revision, Noted::InDoubt);
    }

    /// The revision a new object of this name starts at: above any this controller has seen.
    fn next_revision<S: Spec>(&self, name: &str) -> u64 {
        self.seen
            .lock()
            .unwrap()
            .get(&(S::KIND, name.to_string()))
            .and_then(|s| s.latest())
            .map_or(1, |n| n.revision + 1)
    }

    /// Names of one kind this controller saw and has not deleted.
    fn live<S: Spec>(&self) -> Vec<String> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|((kind, _), s)| {
                *kind == S::KIND && s.latest().is_some_and(|n| n.noted != Noted::Deleted)
            })
            .map(|((_, name), _)| name.clone())
            .collect()
    }
}

/// What a read-modify-write does with the object it read.
enum Write<S> {
    /// Leave it as it is (or absent).
    Keep,
    /// Create it, or replace it, with this spec.
    Put(S),
    /// Delete it.
    Delete,
}

impl KubernetesStore {
    pub(crate) fn new(client: kube::Client, namespace: String) -> Self {
        KubernetesStore {
            client,
            namespace,
            guard: Guard::new(),
            list_page: LIST_PAGE,
            locks: Default::default(),
        }
    }

    /// Read lists `page` objects at a time (tests).
    #[cfg(test)]
    pub(crate) fn list_page(&mut self, page: u32) {
        self.list_page = page;
    }

    /// Another store on the same records, as a second controller would have.
    #[cfg(test)]
    pub(crate) fn second_client(&self) -> Self {
        KubernetesStore {
            client: self.client.clone(),
            namespace: self.namespace.clone(),
            guard: Guard::new(),
            list_page: LIST_PAGE,
            locks: Default::default(),
        }
    }

    fn api<S: Spec>(&self) -> Api<Object<S>> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    async fn measured<R>(
        &self,
        op: DatabaseOperation,
        fut: impl Future<Output = DatabaseResult<R>>,
    ) -> DatabaseResult<R> {
        let latency = &METRICS_REGISTRY
            .metrics_group
            .storage_controller_database_query_latency;
        let _timer = latency.start_timer(DatabaseQueryLatencyLabelGroup { operation: op });
        let res = fut.await;
        if let Err(err) = &res {
            METRICS_REGISTRY
                .metrics_group
                .storage_controller_database_query_error
                .inc(DatabaseQueryErrorLabelGroup {
                    error_type: err.error_label(),
                    operation: op,
                });
        }
        res
    }

    async fn get<S: Spec>(&self, name: &str) -> DatabaseResult<Option<Object<S>>> {
        let sent = self.guard.before_read();
        let obj = self.api::<S>().get_opt(name).await?;
        match &obj {
            Some(o) => self.guard.observe(o, sent)?,
            None => self.guard.observe_absent::<S>(name, sent)?,
        }
        Ok(obj)
    }

    /// Every object of a kind (matching `labels`, when given), read page by page.
    async fn list<S: Spec>(&self, labels: Option<&str>) -> DatabaseResult<Vec<Object<S>>> {
        let api = self.api::<S>();
        let mut out = Vec::new();
        let sent = self.guard.before_read();
        let mut params = ListParams::default().limit(self.list_page);
        if let Some(labels) = labels {
            params = params.labels(labels);
        }
        loop {
            let page = api.list(&params).await?;
            out.extend(page.items);
            match page.metadata.continue_ {
                Some(token) if !token.is_empty() => params = params.continue_token(&token),
                _ => break,
            }
        }
        for o in &out {
            self.guard.observe(o, sent)?;
        }
        if labels.is_none() {
            // A list of every object also shows the ones that are gone (of those seen before it
            // was sent, and not deleted since).
            let listed: std::collections::HashSet<&str> = out.iter().map(|o| o.name()).collect();
            for name in self.guard.live::<S>() {
                if !listed.contains(name.as_str()) {
                    self.guard.observe_absent::<S>(&name, sent)?;
                }
            }
        }
        Ok(out)
    }

    /// A create or replace asking the API server for strict field validation, so a field the
    /// installed kinds do not know is refused rather than dropped.
    async fn write_strict<S: Spec>(
        &self,
        name: Option<&str>,
        obj: &Object<S>,
    ) -> Result<Object<S>, kube::Error> {
        let request = kube::core::Request::new(<Object<S> as kube::Resource>::url_path(
            &(),
            Some(&self.namespace),
        ));
        let body = serde_json::to_vec(obj).map_err(kube::Error::SerdeError)?;
        let pp = PostParams::default();
        let mut req = match name {
            None => request.create(&pp, body),
            Some(name) => request.replace(name, &pp, body),
        }
        .map_err(kube::Error::BuildRequest)?;
        let uri = req.uri().to_string();
        let sep = if uri.contains('?') { '&' } else { '?' };
        *req.uri_mut() = format!("{uri}{sep}fieldValidation=Strict")
            .parse()
            .expect("a valid URI");
        self.client.request::<Object<S>>(req).await
    }

    fn lock<S: Spec>(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap()
            .entry((S::KIND, name.to_string()))
            .or_default()
            .clone()
    }

    /// The one way an object is written: read it, decide, and write what was decided as a
    /// compare-and-swap on the version read; on a conflict read again and decide again.
    /// `decide` may run several times and must depend only on what it is given.
    async fn modify<S: Spec, R>(
        &self,
        name: &str,
        mut decide: impl FnMut(Option<&S>) -> DatabaseResult<(Write<S>, R)>,
    ) -> DatabaseResult<R> {
        let api = self.api::<S>();
        let lock = self.lock::<S>(name);
        let held = lock.clone().lock_owned().await;
        let result = self.modify_locked(&api, name, &mut decide).await;
        drop(held);
        // Forget the lock once no one waits on it, so the map does not keep every name.
        let mut locks = self.locks.lock().unwrap();
        if Arc::strong_count(&lock) == 2 {
            locks.remove(&(S::KIND, name.to_string()));
        }
        result
    }

    async fn modify_locked<S: Spec, R>(
        &self,
        api: &Api<Object<S>>,
        name: &str,
        decide: &mut impl FnMut(Option<&S>) -> DatabaseResult<(Write<S>, R)>,
    ) -> DatabaseResult<R> {
        for _ in 0..MAX_RETRIES {
            let current = self.get::<S>(name).await?;
            let (write, result) = decide(current.as_ref().map(|o| &o.spec))?;
            let written = match (write, current) {
                (Write::Keep, _) | (Write::Delete, None) => return Ok(result),
                (Write::Put(spec), None) => {
                    let obj = Object::new(name, spec, self.guard.next_revision::<S>(name));
                    match self.write_strict(None, &obj).await {
                        Ok(stored) => self.stored(name, &obj, &stored),
                        Err(e) if is_conflict(&e) => continue,
                        Err(e) => Err(e.into()),
                    }
                }
                (Write::Put(spec), Some(current)) => {
                    let obj = current.with_spec(spec)?;
                    match self.write_strict(Some(name), &obj).await {
                        Ok(stored) => self.stored(name, &obj, &stored),
                        Err(e) if is_conflict(&e) || is_not_found(&e) => continue,
                        Err(e) => Err(e.into()),
                    }
                }
                (Write::Delete, Some(current)) => {
                    let params = DeleteParams {
                        preconditions: Some(Preconditions {
                            resource_version: current.metadata.resource_version.clone(),
                            uid: current.metadata.uid.clone(),
                        }),
                        ..Default::default()
                    };
                    match api.delete(name, &params).await {
                        Ok(_) => {
                            self.guard.wrote::<S>(name, current.revision()?, true);
                            Ok(())
                        }
                        Err(e) if is_conflict(&e) => continue,
                        Err(e) => {
                            // Found gone (404), or no answer known (a transport error, a server
                            // error): it may be deleted, so its absence from now on is not the
                            // store going back. A definite refusal (403, 422) deleted nothing.
                            let refused = matches!(&e, kube::Error::Api(s)
                                if s.code < 500 && s.code != 404);
                            if !refused {
                                self.guard.delete_in_doubt::<S>(name, current.revision()?);
                            }
                            if is_not_found(&e) {
                                continue;
                            }
                            Err(e.into())
                        }
                    }
                }
            };
            return written.map(|()| result);
        }
        Err(DatabaseError::Logical(format!(
            "{} {name}: still conflicting after {MAX_RETRIES} attempts",
            S::KIND
        )))
    }

    /// What the API server stored, checked against what was sent, and noted as seen.
    fn stored<S: Spec>(
        &self,
        name: &str,
        sent: &Object<S>,
        stored: &Object<S>,
    ) -> DatabaseResult<()> {
        check_stored(name, &sent.spec, &stored.spec)?;
        let revision = stored.revision()?;
        if revision != sent.revision()? {
            return Err(DatabaseError::Logical(format!(
                "{} {name} was stored at revision {revision}, not the {} written",
                S::KIND,
                sent.revision()?
            )));
        }
        self.guard.wrote::<S>(name, revision, false);
        Ok(())
    }

    /// The controller's check at start, in place of the Postgres store's migrations: every
    /// kind is installed and readable in the records' namespace.
    pub(crate) async fn migration_run(&self) -> DatabaseResult<()> {
        async fn readable<S: Spec>(store: &KubernetesStore) -> DatabaseResult<()> {
            match store.api::<S>().list(&ListParams::default().limit(1)).await {
                Ok(_) => Ok(()),
                Err(e) if is_not_found(&e) => Err(DatabaseError::Logical(format!(
                    "the kind {}.{} is not installed in this cluster (apply \
                     storage_controller/kubernetes/crds.yaml)",
                    S::PLURAL,
                    objects::GROUP
                ))),
                Err(e) => Err(e.into()),
            }
        }
        readable::<TenantSpec>(self).await?;
        readable::<MetadataHealthSpec>(self).await?;
        readable::<TimelineSpec>(self).await?;
        readable::<PageserverSpec>(self).await?;
        readable::<SafekeeperSpec>(self).await?;
        readable::<LeaderSpec>(self).await?;
        self.sweep_orphaned_health().await?;
        tracing::info!(
            "Records are kept in Kubernetes objects in namespace {}",
            self.namespace
        );
        Ok(())
    }

    // -----------------------------------------------------------------------------------------
    // Pageservers

    pub(crate) async fn insert_node(&self, node: &Node) -> DatabaseResult<()> {
        let spec = PageserverSpec::from_row(&node.to_persistent());
        let name = pageserver_object_name(spec.node_id);
        self.measured(
            DatabaseOperation::InsertNode,
            self.modify::<PageserverSpec, ()>(&name, |current| match current {
                Some(_) => Err(DatabaseError::Exists(format!(
                    "node {} already exists",
                    spec.node_id
                ))),
                None => Ok((Write::Put(spec.clone()), ())),
            }),
        )
        .await
    }

    async fn nodes_where(&self, deleted: bool) -> DatabaseResult<Vec<NodePersistence>> {
        let deleted_label = String::from(NodeLifecycle::Deleted);
        Ok(self
            .list::<PageserverSpec>(None)
            .await?
            .into_iter()
            .filter(|o| (o.spec.lifecycle == deleted_label) == deleted)
            .map(|o| o.spec.to_row())
            .collect())
    }

    pub(crate) async fn list_nodes(&self) -> DatabaseResult<Vec<NodePersistence>> {
        let result = self
            .measured(DatabaseOperation::ListNodes, self.nodes_where(false))
            .await?;
        tracing::info!("list_nodes: loaded {} nodes", result.len());
        Ok(result)
    }

    pub(crate) async fn list_tombstones(&self) -> DatabaseResult<Vec<NodePersistence>> {
        let result = self
            .measured(DatabaseOperation::ListTombstones, self.nodes_where(true))
            .await?;
        tracing::info!("list_tombstones: loaded {} nodes", result.len());
        Ok(result)
    }

    /// Neon's `update_node`: change a node that exists and is not deleted.
    async fn update_node(
        &self,
        node_id: NodeId,
        change: impl Fn(&mut PageserverSpec),
    ) -> DatabaseResult<()> {
        let deleted = String::from(NodeLifecycle::Deleted);
        self.measured(
            DatabaseOperation::UpdateNode,
            self.modify::<PageserverSpec, ()>(&pageserver_object_name(node_id.0 as i64), |cur| {
                match cur {
                    Some(spec) if spec.lifecycle != deleted => {
                        let mut spec = spec.clone();
                        change(&mut spec);
                        Ok((Write::Put(spec), ()))
                    }
                    _ => Err(DatabaseError::Logical(format!(
                        "Node {node_id:?} not found for update",
                    ))),
                }
            }),
        )
        .await
    }

    pub(crate) async fn update_node_scheduling_policy(
        &self,
        input_node_id: NodeId,
        input_scheduling: NodeSchedulingPolicy,
    ) -> DatabaseResult<()> {
        self.update_node(input_node_id, |spec| {
            spec.scheduling_policy = String::from(input_scheduling)
        })
        .await
    }

    pub(crate) async fn update_node_on_registration(
        &self,
        input_node_id: NodeId,
        input_https_port: Option<u16>,
        input_grpc_addr: Option<String>,
        input_grpc_port: Option<u16>,
    ) -> DatabaseResult<()> {
        self.update_node(input_node_id, |spec| {
            spec.listen_https_port = input_https_port.map(|x| x as i32);
            spec.listen_grpc_addr = input_grpc_addr.clone();
            spec.listen_grpc_port = input_grpc_port.map(|x| x as i32);
        })
        .await
    }

    pub(crate) async fn set_tombstone(&self, del_node_id: NodeId) -> DatabaseResult<()> {
        self.update_node(del_node_id, |spec| {
            spec.lifecycle = String::from(NodeLifecycle::Deleted)
        })
        .await
    }

    pub(crate) async fn delete_node(&self, del_node_id: NodeId) -> DatabaseResult<()> {
        self.measured(
            DatabaseOperation::DeleteNode,
            self.modify::<PageserverSpec, ()>(
                &pageserver_object_name(del_node_id.0 as i64),
                |cur| {
                    // A node can be hard deleted only once it has a tombstone.
                    let Some(spec) = cur else {
                        return Ok((Write::Keep, ()));
                    };
                    let lc = NodeLifecycle::from_str(&spec.lifecycle).map_err(|e| {
                        DatabaseError::Logical(format!(
                            "Node {del_node_id} has invalid lifecycle: {e}"
                        ))
                    })?;
                    if lc != NodeLifecycle::Deleted {
                        return Err(DatabaseError::Logical(format!(
                            "Node {del_node_id} was not soft deleted before, cannot hard delete it"
                        )));
                    }
                    Ok((Write::Delete, ()))
                },
            ),
        )
        .await
    }

    // -----------------------------------------------------------------------------------------
    // Tenants

    async fn all_shards(&self) -> DatabaseResult<Vec<TenantShardPersistence>> {
        Ok(self
            .list::<TenantSpec>(None)
            .await?
            .into_iter()
            .flat_map(|o| o.spec.rows().collect::<Vec<_>>())
            .collect())
    }

    pub(crate) async fn load_active_tenant_shards(
        &self,
    ) -> DatabaseResult<Vec<TenantShardPersistence>> {
        let detached = serde_json::to_string(&PlacementPolicy::Detached).unwrap();
        self.measured(DatabaseOperation::ListTenantShards, async {
            Ok(self
                .all_shards()
                .await?
                .into_iter()
                .filter(|s| s.placement_policy != detached)
                .collect())
        })
        .await
    }

    pub(crate) async fn load_tenant(
        &self,
        filter_tenant_id: TenantId,
    ) -> DatabaseResult<Vec<TenantShardPersistence>> {
        self.measured(DatabaseOperation::LoadTenant, async {
            Ok(self
                .get::<TenantSpec>(&filter_tenant_id.to_string())
                .await?
                .map(|o| o.spec.rows().collect())
                .unwrap_or_default())
        })
        .await
    }

    pub(crate) async fn insert_tenant_shards(
        &self,
        shards: Vec<TenantShardPersistence>,
    ) -> DatabaseResult<()> {
        let now = chrono::Utc::now();
        self.measured(DatabaseOperation::InsertTenantShards, async {
            let Some(tenant_id) = shards.first().map(|s| s.tenant_id.clone()) else {
                return Ok(());
            };
            // On Postgres the rows are one transaction; here they are one object, so they must
            // be one tenant's (as every caller's are).
            if shards.iter().any(|s| s.tenant_id != tenant_id) {
                return Err(DatabaseError::Logical(
                    "insert_tenant_shards: shards of more than one tenant".to_string(),
                ));
            }
            self.modify::<TenantSpec, ()>(&tenant_id, |cur| {
                let mut spec = cur.cloned().unwrap_or_else(|| TenantSpec {
                    tenant_id: tenant_id.clone(),
                    shards: Vec::new(),
                });
                for row in &shards {
                    let new = ShardRecord::from_row(row);
                    if spec.shards.iter().any(|s| s.key() == new.key()) {
                        return Err(DatabaseError::Exists(format!(
                            "tenant shard {tenant_id} {:?} already exists",
                            new.key()
                        )));
                    }
                    spec.shards.push(new);
                }
                Ok((Write::Put(spec), ()))
            })
            .await?;
            // Each new shard starts healthy, as on Postgres. Written after the shards, so a
            // health record never exists without its shard.
            self.modify::<MetadataHealthSpec, ()>(&tenant_id, |cur| {
                let mut spec = cur.cloned().unwrap_or_else(|| MetadataHealthSpec {
                    tenant_id: tenant_id.clone(),
                    shards: Vec::new(),
                });
                for row in &shards {
                    let rec = HealthRecord::from_row(&MetadataHealthPersistence {
                        tenant_id: tenant_id.clone(),
                        shard_number: row.shard_number,
                        shard_count: row.shard_count,
                        healthy: true,
                        last_scrubbed_at: now,
                    });
                    spec.shards.retain(|s| s.key() != rec.key());
                    spec.shards.push(rec);
                }
                Ok((Write::Put(spec), ()))
            })
            .await
        })
        .await
    }

    pub(crate) async fn delete_tenant(&self, del_tenant_id: TenantId) -> DatabaseResult<()> {
        let name = del_tenant_id.to_string();
        self.measured(DatabaseOperation::DeleteTenant, async {
            // The Tenant, then its health entries in a second write (Postgres removes them with
            // the shards by a cascade); until that lands, readers drop entries whose shard is gone.
            self.modify::<TenantSpec, ()>(&name, |_| Ok((Write::Delete, ())))
                .await?;
            self.modify::<MetadataHealthSpec, ()>(&name, |_| Ok((Write::Delete, ())))
                .await
        })
        .await
    }

    #[tracing::instrument(skip_all, fields(node_id))]
    pub(crate) async fn re_attach(
        &self,
        input_node_id: NodeId,
    ) -> DatabaseResult<HashMap<TenantShardId, Generation>> {
        let node = input_node_id.0 as i64;
        self.measured(DatabaseOperation::ReAttach, async {
            let node_spec = self
                .get::<PageserverSpec>(&pageserver_object_name(node))
                .await?
                .map(|o| o.spec);
            if let Some(spec) = &node_spec {
                if matches!(
                    NodeLifecycle::from_str(&spec.lifecycle),
                    Ok(NodeLifecycle::Deleted)
                ) {
                    return Err(DatabaseError::Logical(format!(
                        "Node {input_node_id} is marked as deleted, re-attach is not allowed"
                    )));
                }
            }

            // One compare-and-swap per tenant with a shard on this node. Each is safe to repeat:
            // a controller that stops halfway is finished by the pageserver's retry, and a
            // generation may then skip a number, never repeat one.
            let tenants = self
                .list::<TenantSpec>(Some(&generation_label(node)))
                .await?;
            let incremented: Vec<Vec<TenantShardPersistence>> =
                futures::stream::iter(tenants.into_iter().map(|t| {
                    let name = t.metadata.name.clone().unwrap_or_default();
                    async move {
                        self.modify::<TenantSpec, Vec<TenantShardPersistence>>(&name, |cur| {
                            let Some(cur) = cur else {
                                return Ok((Write::Keep, Vec::new()));
                            };
                            let mut spec = cur.clone();
                            let mut rows = Vec::new();
                            for shard in spec
                                .shards
                                .iter_mut()
                                .filter(|s| s.generation_pageserver == Some(node))
                            {
                                let Some(g) = shard.generation else {
                                    return Err(DatabaseError::Logical(
                                        "Generation should always be set after incrementing"
                                            .to_string(),
                                    ));
                                };
                                shard.generation = Some(g + 1);
                                rows.push(shard.to_row(&spec.tenant_id));
                            }
                            if rows.is_empty() {
                                Ok((Write::Keep, rows))
                            } else {
                                Ok((Write::Put(spec), rows))
                            }
                        })
                        .await
                    }
                }))
                .buffer_unordered(RE_ATTACH_CONCURRENCY)
                .try_collect()
                .await?;
            let incremented: Vec<TenantShardPersistence> =
                incremented.into_iter().flatten().collect();
            tracing::info!("Incremented {} tenants' generations", incremented.len());

            if node_spec.is_some() {
                self.modify::<PageserverSpec, ()>(&pageserver_object_name(node), |cur| {
                    let Some(cur) = cur else {
                        return Ok((Write::Keep, ()));
                    };
                    let old =
                        NodeSchedulingPolicy::from_str(&cur.scheduling_policy).map_err(|e| {
                            DatabaseError::Logical(format!(
                                "Pageserver {node}: scheduling policy {:?}: {e}",
                                cur.scheduling_policy
                            ))
                        })?;
                    let new = match old {
                        NodeSchedulingPolicy::Active => NodeSchedulingPolicy::Active,
                        NodeSchedulingPolicy::PauseForRestart => NodeSchedulingPolicy::Active,
                        NodeSchedulingPolicy::Draining => NodeSchedulingPolicy::Active,
                        NodeSchedulingPolicy::Filling => NodeSchedulingPolicy::Active,
                        NodeSchedulingPolicy::Pause => NodeSchedulingPolicy::Pause,
                        NodeSchedulingPolicy::Deleting => NodeSchedulingPolicy::Pause,
                    };
                    let mut spec = cur.clone();
                    spec.scheduling_policy = String::from(new);
                    Ok((Write::Put(spec), ()))
                })
                .await?;
            }

            let mut result = HashMap::new();
            for tsp in incremented {
                let tenant_shard_id = TenantShardId {
                    tenant_id: TenantId::from_str(tsp.tenant_id.as_str())
                        .map_err(|e| DatabaseError::Logical(format!("Malformed tenant id: {e}")))?,
                    shard_number: ShardNumber(tsp.shard_number as u8),
                    shard_count: ShardCount::new(tsp.shard_count as u8),
                };
                let Some(g) = tsp.generation else {
                    return Err(DatabaseError::Logical(
                        "Generation should always be set after incrementing".to_string(),
                    ));
                };
                result.insert(tenant_shard_id, Generation::new(g as u32));
            }
            Ok(result)
        })
        .await
    }

    pub(crate) async fn increment_generation(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<Generation> {
        let key = shard_key(&tenant_shard_id);
        let generation = self
            .measured(
                DatabaseOperation::IncrementGeneration,
                self.modify::<TenantSpec, Option<i32>>(
                    &tenant_shard_id.tenant_id.to_string(),
                    |cur| {
                        let mut spec = cur
                            .cloned()
                            .ok_or_else(|| shard_not_found(&tenant_shard_id))?;
                        let shard = spec
                            .shards
                            .iter_mut()
                            .find(|s| s.key() == key)
                            .ok_or_else(|| shard_not_found(&tenant_shard_id))?;
                        shard.generation = shard.generation.map(|g| g + 1);
                        shard.generation_pageserver = Some(node_id.0 as i64);
                        let g = shard.generation;
                        Ok((Write::Put(spec), g))
                    },
                ),
            )
            .await?;
        let Some(g) = generation else {
            return Err(DatabaseError::Logical(
                "Generation should always be set after incrementing".to_string(),
            )
            .into());
        };
        Ok(Generation::new(g as u32))
    }

    pub(crate) async fn tenant_generations(
        &self,
        filter_tenant_id: TenantId,
    ) -> Result<Vec<ShardGenerationState>, DatabaseError> {
        let mut rows = self
            .measured(DatabaseOperation::TenantGenerations, async {
                Ok(self
                    .get::<TenantSpec>(&filter_tenant_id.to_string())
                    .await?
                    .map(|o| o.spec.rows().collect::<Vec<_>>())
                    .unwrap_or_default())
            })
            .await?;
        rows.sort_by_key(|r| r.shard_number);
        rows.into_iter()
            .map(|p| {
                // The kinds declare free strings: an object edited by hand is an error, not a
                // panic.
                let tenant_shard_id = p.get_tenant_shard_id().map_err(|e| {
                    DatabaseError::Logical(format!(
                        "Tenant {}: a shard that is not a tenant shard id: {e}",
                        p.tenant_id
                    ))
                })?;
                Ok(ShardGenerationState {
                    tenant_shard_id,
                    generation: p.generation.map(|g| Generation::new(g as u32)),
                    generation_pageserver: p.generation_pageserver.map(|n| NodeId(n as u64)),
                })
            })
            .collect()
    }

    pub(crate) async fn shard_generations(
        &self,
        tenant_shard_ids: impl Iterator<Item = &TenantShardId>,
    ) -> Result<Vec<(TenantShardId, Option<Generation>)>, DatabaseError> {
        self.measured(
            DatabaseOperation::ShardGenerations,
            self.read_shard_generations(tenant_shard_ids),
        )
        .await
    }

    /// Shards' generations, read one tenant object at a time.
    async fn read_shard_generations(
        &self,
        tenant_shard_ids: impl Iterator<Item = &TenantShardId>,
    ) -> Result<Vec<(TenantShardId, Option<Generation>)>, DatabaseError> {
        let mut by_tenant: BTreeMap<TenantId, Vec<TenantShardId>> = BTreeMap::new();
        for id in tenant_shard_ids {
            by_tenant.entry(id.tenant_id).or_default().push(*id);
        }
        let mut out = Vec::new();
        for (tenant_id, ids) in by_tenant {
            let Some(obj) = self.get::<TenantSpec>(&tenant_id.to_string()).await? else {
                continue;
            };
            for id in ids {
                if let Some(s) = obj.spec.shards.iter().find(|s| s.key() == shard_key(&id)) {
                    out.push((id, s.generation.map(|g| Generation::new(g as u32))));
                }
            }
        }
        Ok(out)
    }

    /// A pageserver's generation validation: its shards' generations, read from one list of
    /// the tenants labelled with `node_id` (the pageserver the controller expects). A shard the
    /// list does not cover is read on its own, so a guess that is wrong (a live migration's
    /// warm-up, when the controller's intent already names the new pageserver and the old one
    /// still validates) costs a read, never an answer.
    pub(crate) async fn shard_generations_on(
        &self,
        node_id: NodeId,
        tenant_shard_ids: impl Iterator<Item = &TenantShardId>,
    ) -> Result<Vec<(TenantShardId, Option<Generation>)>, DatabaseError> {
        let wanted: Vec<TenantShardId> = tenant_shard_ids.copied().collect();
        self.measured(DatabaseOperation::ShardGenerations, async {
            let tenants: HashMap<String, TenantSpec> = self
                .list::<TenantSpec>(Some(&generation_label(node_id.0 as i64)))
                .await?
                .into_iter()
                .map(|o| (o.spec.tenant_id.clone(), o.spec))
                .collect();
            let mut out = Vec::new();
            let mut uncovered = Vec::new();
            for id in wanted {
                let found = tenants
                    .get(&id.tenant_id.to_string())
                    .and_then(|t| t.shards.iter().find(|s| s.key() == shard_key(&id)));
                match found {
                    Some(s) => out.push((id, s.generation.map(|g| Generation::new(g as u32)))),
                    None => uncovered.push(id),
                }
            }
            if !uncovered.is_empty() {
                out.extend(self.read_shard_generations(uncovered.iter()).await?);
            }
            Ok(out)
        })
        .await
    }

    pub(crate) async fn update_tenant_shard(
        &self,
        tenant: TenantFilter,
        input_placement_policy: Option<PlacementPolicy>,
        input_config: Option<TenantConfig>,
        input_generation: Option<Generation>,
        input_scheduling_policy: Option<ShardSchedulingPolicy>,
    ) -> DatabaseResult<()> {
        let (tenant_id, only) = match &tenant {
            TenantFilter::Shard(id) => (id.tenant_id, Some(shard_key(id))),
            TenantFilter::Tenant(id) => (*id, None),
        };
        // Clear generation_pageserver if we are moving into a state where we won't have
        // any attached pageservers.
        let clear_pageserver = matches!(
            input_placement_policy,
            Some(PlacementPolicy::Detached | PlacementPolicy::Secondary)
        );
        let placement = input_placement_policy
            .as_ref()
            .map(|p| serde_json::to_string(&p).unwrap());
        let config = input_config
            .as_ref()
            .map(|c| serde_json::to_string(&c).unwrap());
        let scheduling = input_scheduling_policy.map(|p| serde_json::to_string(&p).unwrap());
        let generation = input_generation.map(|g| g.into().unwrap() as i32);
        self.measured(
            DatabaseOperation::UpdateTenantShard,
            self.modify::<TenantSpec, ()>(&tenant_id.to_string(), |cur| {
                let Some(cur) = cur else {
                    return Ok((Write::Keep, ()));
                };
                let mut spec = cur.clone();
                let mut changed = false;
                for s in spec
                    .shards
                    .iter_mut()
                    .filter(|s| only.is_none_or(|k| s.key() == k))
                {
                    changed = true;
                    if let Some(g) = generation {
                        s.generation = Some(g);
                    }
                    if let Some(p) = &placement {
                        s.placement_policy = p.clone();
                    }
                    if let Some(c) = &config {
                        s.config = c.clone();
                    }
                    if let Some(p) = &scheduling {
                        s.scheduling_policy = p.clone();
                    }
                    if clear_pageserver {
                        s.generation_pageserver = None;
                    }
                }
                Ok((
                    if changed {
                        Write::Put(spec)
                    } else {
                        Write::Keep
                    },
                    (),
                ))
            }),
        )
        .await
    }

    pub(crate) async fn set_tenant_shard_preferred_azs(
        &self,
        preferred_azs: Vec<(TenantShardId, Option<AvailabilityZone>)>,
    ) -> DatabaseResult<Vec<(TenantShardId, Option<AvailabilityZone>)>> {
        let mut by_tenant: BTreeMap<TenantId, Vec<(TenantShardId, Option<AvailabilityZone>)>> =
            BTreeMap::new();
        for (id, az) in preferred_azs {
            by_tenant.entry(id.tenant_id).or_default().push((id, az));
        }
        self.measured(DatabaseOperation::SetPreferredAzs, async {
            // One write per tenant: a preference is not a generation, and Postgres's single
            // transaction across tenants is not needed for it.
            let mut updated = Vec::new();
            for (tenant_id, wanted) in by_tenant {
                let done = self
                    .modify::<TenantSpec, Vec<(TenantShardId, Option<AvailabilityZone>)>>(
                        &tenant_id.to_string(),
                        |cur| {
                            let Some(cur) = cur else {
                                return Ok((Write::Keep, Vec::new()));
                            };
                            let mut spec = cur.clone();
                            let mut done = Vec::new();
                            for (id, az) in &wanted {
                                if let Some(s) =
                                    spec.shards.iter_mut().find(|s| s.key() == shard_key(id))
                                {
                                    s.preferred_az_id = az.as_ref().map(|az| az.0.clone());
                                    done.push((*id, az.clone()));
                                }
                            }
                            if done.is_empty() {
                                Ok((Write::Keep, done))
                            } else {
                                Ok((Write::Put(spec), done))
                            }
                        },
                    )
                    .await?;
                updated.extend(done);
            }
            Ok(updated)
        })
        .await
    }

    pub(crate) async fn detach(&self, tenant_shard_id: TenantShardId) -> anyhow::Result<()> {
        let key = shard_key(&tenant_shard_id);
        let detached = serde_json::to_string(&PlacementPolicy::Detached).unwrap();
        self.measured(
            DatabaseOperation::Detach,
            self.modify::<TenantSpec, ()>(&tenant_shard_id.tenant_id.to_string(), |cur| {
                let Some(cur) = cur else {
                    return Ok((Write::Keep, ()));
                };
                let mut spec = cur.clone();
                let Some(s) = spec.shards.iter_mut().find(|s| s.key() == key) else {
                    return Ok((Write::Keep, ()));
                };
                s.generation_pageserver = None;
                s.placement_policy = detached.clone();
                Ok((Write::Put(spec), ()))
            }),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn begin_shard_split(
        &self,
        old_shard_count: ShardCount,
        split_tenant_id: TenantId,
        parent_to_children: Vec<(TenantShardId, Vec<TenantShardPersistence>)>,
    ) -> DatabaseResult<()> {
        let old = old_shard_count.literal() as i32;
        self.measured(
            DatabaseOperation::BeginShardSplit,
            self.modify::<TenantSpec, ()>(&split_tenant_id.to_string(), |cur| {
                let mut spec = cur.cloned().unwrap_or_else(|| TenantSpec {
                    tenant_id: split_tenant_id.to_string(),
                    shards: Vec::new(),
                });
                // Mark parent shards as splitting
                let mut marked = 0usize;
                for s in spec.shards.iter_mut().filter(|s| s.shard_count == old) {
                    s.splitting = SplitState::Splitting;
                    marked += 1;
                }
                if u8::try_from(marked).map_err(|_| {
                    DatabaseError::Logical(format!(
                        "Overflow existing shard count {marked} while splitting"
                    ))
                })? != old_shard_count.count()
                {
                    // Perhaps a deletion or another split raced with this attempt to split,
                    // mutating the parent shards that we intend to split. In this case the
                    // split request should fail.
                    return Err(DatabaseError::Logical(format!(
                        "Unexpected existing shard count {marked} when preparing tenant for split (expected {})",
                        old_shard_count.count()
                    )));
                }
                // Insert child shards, each with its parent's generation
                for (parent_shard_id, children) in &parent_to_children {
                    let parent = spec
                        .shards
                        .iter()
                        .find(|s| s.key() == shard_key(parent_shard_id))
                        .cloned()
                        .ok_or_else(|| {
                            DatabaseError::Logical(format!(
                                "Parent shard {parent_shard_id} not found"
                            ))
                        })?;
                    for child in children {
                        let mut child = ShardRecord::from_row(child);
                        child.generation = parent.generation;
                        debug_assert!(child.splitting == SplitState::Splitting);
                        if spec.shards.iter().any(|s| s.key() == child.key()) {
                            return Err(DatabaseError::Exists(format!(
                                "child shard {split_tenant_id} {:?} already exists",
                                child.key()
                            )));
                        }
                        spec.shards.push(child);
                    }
                }
                Ok((Write::Put(spec), ()))
            }),
        )
        .await
    }

    pub(crate) async fn complete_shard_split(
        &self,
        split_tenant_id: TenantId,
        old_shard_count: ShardCount,
        new_shard_count: ShardCount,
    ) -> DatabaseResult<()> {
        let old = old_shard_count.literal() as i32;
        let new = new_shard_count.literal() as i32;
        let name = split_tenant_id.to_string();
        self.measured(DatabaseOperation::CompleteShardSplit, async {
            self.modify::<TenantSpec, ()>(&name, |cur| {
                let mut spec = cur.cloned().unwrap_or_else(|| TenantSpec {
                    tenant_id: name.clone(),
                    shards: Vec::new(),
                });
                // Sanity: child shards must still exist, as we're deleting parent shards
                let children = spec.shards.iter().filter(|s| s.shard_count == new).count();
                if children != new_shard_count.count() as usize {
                    return Err(DatabaseError::Logical(format!(
                        "Unexpected child shard count {children} while completing split to \
                            count {new_shard_count:?} on tenant {split_tenant_id}"
                    )));
                }
                // Drop parent shards, clear the splitting flag on the children
                spec.shards.retain(|s| s.shard_count != old);
                for s in spec.shards.iter_mut().filter(|s| s.shard_count == new) {
                    s.splitting = SplitState::Idle;
                }
                Ok((Write::Put(spec), ()))
            })
            .await?;
            // The parents' health records go with them, as Postgres cascades the delete.
            self.drop_health(&name, |s| s.shard_count == old).await
        })
        .await
    }

    pub(crate) async fn abort_shard_split(
        &self,
        split_tenant_id: TenantId,
        new_shard_count: ShardCount,
    ) -> DatabaseResult<AbortShardSplitStatus> {
        let new = new_shard_count.literal() as i32;
        let name = split_tenant_id.to_string();
        self.measured(DatabaseOperation::AbortShardSplit, async {
            let status = self
                .modify::<TenantSpec, AbortShardSplitStatus>(&name, |cur| {
                    let Some(cur) = cur else {
                        // Parent shards are already gone: we cannot abort.
                        return Ok((Write::Keep, AbortShardSplitStatus::Complete));
                    };
                    let mut spec = cur.clone();
                    // Clear the splitting state on parent shards
                    let mut parents = 0usize;
                    for s in spec.shards.iter_mut().filter(|s| s.shard_count != new) {
                        s.splitting = SplitState::Idle;
                        parents += 1;
                    }
                    if parents == 0 {
                        return Ok((Write::Keep, AbortShardSplitStatus::Complete));
                    }
                    // Sanity check: if parent shards were present, their cardinality should
                    // be less than the number of child shards.
                    if parents >= new_shard_count.count() as usize {
                        return Err(DatabaseError::Logical(format!(
                            "Unexpected parent shard count {parents} while aborting split to \
                                count {new_shard_count:?} on tenant {split_tenant_id}"
                        )));
                    }
                    // Erase child shards
                    spec.shards.retain(|s| s.shard_count != new);
                    Ok((Write::Put(spec), AbortShardSplitStatus::Aborted))
                })
                .await?;
            // The children's health entries go with them, in a second write, as Postgres
            // cascades the delete.
            if matches!(status, AbortShardSplitStatus::Aborted) {
                self.drop_health(&name, |s| s.shard_count == new).await?;
            }
            Ok(status)
        })
        .await
    }

    /// Drop a tenant's health entries that `gone` picks, deleting the object once it is empty.
    async fn drop_health(
        &self,
        name: &str,
        gone: impl Fn(&HealthRecord) -> bool,
    ) -> DatabaseResult<()> {
        self.modify::<MetadataHealthSpec, ()>(name, |cur| {
            let Some(cur) = cur else {
                return Ok((Write::Keep, ()));
            };
            let mut spec = cur.clone();
            spec.shards.retain(|s| !gone(s));
            Ok((
                if spec == *cur {
                    Write::Keep
                } else if spec.shards.is_empty() {
                    Write::Delete
                } else {
                    Write::Put(spec)
                },
                (),
            ))
        })
        .await
    }

    // -----------------------------------------------------------------------------------------
    // The scrubber's health records

    pub(crate) async fn update_metadata_health_records(
        &self,
        healthy_records: Vec<MetadataHealthPersistence>,
        unhealthy_records: Vec<MetadataHealthPersistence>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> DatabaseResult<()> {
        let mut by_tenant: BTreeMap<String, Vec<(i32, i32, bool)>> = BTreeMap::new();
        for (records, healthy) in [(healthy_records, true), (unhealthy_records, false)] {
            for r in records {
                by_tenant.entry(r.tenant_id).or_default().push((
                    r.shard_number,
                    r.shard_count,
                    healthy,
                ));
            }
        }
        self.measured(DatabaseOperation::UpdateMetadataHealth, async {
            // A health record for a shard that does not exist is refused, as Postgres's foreign
            // key refuses it; checked for every tenant named before anything is written.
            for (tenant_id, updates) in &by_tenant {
                let have: Vec<(i32, i32)> = self
                    .get::<TenantSpec>(tenant_id)
                    .await?
                    .map(|o| o.spec.shards.iter().map(|s| s.key()).collect())
                    .unwrap_or_default();
                if let Some((n, c, _)) = updates.iter().find(|(n, c, _)| !have.contains(&(*n, *c)))
                {
                    return Err(DatabaseError::Logical(format!(
                        "a health record for tenant shard {tenant_id} {n}/{c}, which does not exist"
                    )));
                }
            }
            for (tenant_id, updates) in by_tenant {
                self.modify::<MetadataHealthSpec, ()>(&tenant_id, |cur| {
                    let mut spec = cur.cloned().unwrap_or_else(|| MetadataHealthSpec {
                        tenant_id: tenant_id.clone(),
                        shards: Vec::new(),
                    });
                    for (shard_number, shard_count, healthy) in &updates {
                        spec.shards
                            .retain(|s| s.key() != (*shard_number, *shard_count));
                        spec.shards.push(HealthRecord {
                            shard_number: *shard_number,
                            shard_count: *shard_count,
                            healthy: *healthy,
                            last_scrubbed_at: objects::truncate_to_micros(now),
                        });
                    }
                    Ok((Write::Put(spec), ()))
                })
                .await?;
            }
            Ok(())
        })
        .await
    }

    /// The shards that exist, by tenant: a health entry whose shard is gone (a removal whose
    /// second write has not landed) is not a record.
    async fn existing_shards(&self) -> DatabaseResult<HashMap<String, Vec<(i32, i32)>>> {
        Ok(self
            .list::<TenantSpec>(None)
            .await?
            .into_iter()
            .map(|o| {
                let keys = o.spec.shards.iter().map(|s| s.key()).collect();
                (o.spec.tenant_id, keys)
            })
            .collect())
    }

    async fn health_where(
        &self,
        keep: impl Fn(&HealthRecord) -> bool,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        let shards = self.existing_shards().await?;
        Ok(self
            .list::<MetadataHealthSpec>(None)
            .await?
            .into_iter()
            .flat_map(|o| {
                let have = shards.get(&o.spec.tenant_id).cloned().unwrap_or_default();
                o.spec
                    .shards
                    .iter()
                    .filter(|s| have.contains(&s.key()) && keep(s))
                    .map(|s| s.to_row(&o.spec.tenant_id))
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    /// Remove the health entries whose shard is gone, left by a removal interrupted between its
    /// two writes.
    async fn sweep_orphaned_health(&self) -> DatabaseResult<()> {
        let shards = self.existing_shards().await?;
        for o in self.list::<MetadataHealthSpec>(None).await? {
            let have = shards.get(&o.spec.tenant_id).cloned().unwrap_or_default();
            if o.spec.shards.iter().all(|s| have.contains(&s.key())) {
                continue;
            }
            self.modify::<MetadataHealthSpec, ()>(o.name(), |cur| {
                let Some(cur) = cur else {
                    return Ok((Write::Keep, ()));
                };
                let mut spec = cur.clone();
                spec.shards.retain(|s| have.contains(&s.key()));
                Ok((
                    if spec.shards.is_empty() {
                        Write::Delete
                    } else {
                        Write::Put(spec)
                    },
                    (),
                ))
            })
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn list_metadata_health_records(
        &self,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        self.measured(
            DatabaseOperation::ListMetadataHealth,
            self.health_where(|_| true),
        )
        .await
    }

    pub(crate) async fn list_unhealthy_metadata_health_records(
        &self,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        self.measured(
            DatabaseOperation::ListMetadataHealthUnhealthy,
            self.health_where(|s| !s.healthy),
        )
        .await
    }

    pub(crate) async fn list_outdated_metadata_health_records(
        &self,
        earlier: chrono::DateTime<chrono::Utc>,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        self.measured(
            DatabaseOperation::ListMetadataHealthOutdated,
            self.health_where(|s| s.last_scrubbed_at < objects::truncate_to_micros(earlier)),
        )
        .await
    }

    // -----------------------------------------------------------------------------------------
    // The leader

    pub(crate) async fn get_leader(&self) -> DatabaseResult<Option<ControllerPersistence>> {
        self.measured(DatabaseOperation::GetLeader, async {
            Ok(self
                .get::<LeaderSpec>(LEADER_OBJECT_NAME)
                .await?
                .map(|o| o.spec.to_row()))
        })
        .await
    }

    pub(crate) async fn update_leader(
        &self,
        prev: Option<ControllerPersistence>,
        new: ControllerPersistence,
    ) -> DatabaseResult<()> {
        let prev = prev.as_ref().map(LeaderSpec::from_row);
        let new = LeaderSpec::from_row(&new);
        let updated = self
            .measured(
                DatabaseOperation::UpdateLeader,
                self.modify::<LeaderSpec, bool>(LEADER_OBJECT_NAME, |cur| {
                    Ok(match (&prev, cur) {
                        (Some(prev), Some(cur)) if prev == cur => (Write::Put(new.clone()), true),
                        (None, None) => (Write::Put(new.clone()), true),
                        _ => (Write::Keep, false),
                    })
                }),
            )
            .await?;
        if !updated {
            return Err(DatabaseError::Logical(
                "Leader table update failed".to_string(),
            ));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------------------------
    // Safekeepers

    pub(crate) async fn list_safekeepers(&self) -> DatabaseResult<Vec<SafekeeperPersistence>> {
        let safekeepers = self
            .measured(DatabaseOperation::ListNodes, async {
                self.list::<SafekeeperSpec>(None)
                    .await?
                    .into_iter()
                    .map(|o| o.spec.to_row())
                    .collect::<DatabaseResult<Vec<_>>>()
            })
            .await?;
        tracing::info!("list_safekeepers: loaded {} nodes", safekeepers.len());
        Ok(safekeepers)
    }

    pub(crate) async fn safekeeper_upsert(
        &self,
        record: SafekeeperUpsert,
    ) -> Result<(), DatabaseError> {
        if record.version < 0 {
            return Err(DatabaseError::Logical(format!(
                "negative version: {}",
                record.version
            )));
        }
        self.modify::<SafekeeperSpec, ()>(&safekeeper_object_name(record.id), |cur| {
            // An upsert never changes the scheduling policy; a new safekeeper takes Postgres's
            // column default.
            let scheduling_policy = cur
                .map(|c| c.scheduling_policy.clone())
                .unwrap_or_else(|| String::from(SkSchedulingPolicy::Activating));
            Ok((
                Write::Put(SafekeeperSpec {
                    id: record.id,
                    region_id: record.region_id.clone(),
                    version: record.version,
                    host: record.host.clone(),
                    port: record.port,
                    http_port: record.http_port,
                    https_port: record.https_port,
                    availability_zone_id: record.availability_zone_id.clone(),
                    scheduling_policy,
                }),
                (),
            ))
        })
        .await
    }

    pub(crate) async fn set_safekeeper_scheduling_policy(
        &self,
        id_: i64,
        scheduling_policy_: SkSchedulingPolicy,
    ) -> Result<(), DatabaseError> {
        self.modify::<SafekeeperSpec, ()>(&safekeeper_object_name(id_), |cur| {
            let Some(cur) = cur else {
                return Err(DatabaseError::Logical(
                    "unexpected number of rows (0)".to_string(),
                ));
            };
            let mut spec = cur.clone();
            spec.scheduling_policy = String::from(scheduling_policy_);
            Ok((Write::Put(spec), ()))
        })
        .await
    }

    pub(crate) async fn activate_safekeeper(&self, id_: i64) -> Result<Option<()>, DatabaseError> {
        let active = String::from(SkSchedulingPolicy::Active);
        let activating = String::from(SkSchedulingPolicy::Activating);
        self.modify::<SafekeeperSpec, Option<()>>(&safekeeper_object_name(id_), |cur| {
            match cur {
                Some(cur)
                    if cur.scheduling_policy == activating || cur.scheduling_policy == active =>
                {
                    let mut spec = cur.clone();
                    spec.scheduling_policy = active.clone();
                    Ok((Write::Put(spec), Some(())))
                }
                // As on Postgres, the answer is the same whether or not a row changed.
                _ => Ok((Write::Keep, Some(()))),
            }
        })
        .await
    }

    // -----------------------------------------------------------------------------------------
    // Timelines: membership, pending safekeeper operations, imports

    /// Change one Timeline object; one emptied of all three parts is deleted.
    async fn modify_timeline<R>(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        mut decide: impl FnMut(&mut TimelineSpec) -> DatabaseResult<R>,
    ) -> DatabaseResult<R> {
        self.modify::<TimelineSpec, R>(&timeline_object_name(tenant_id, timeline_id), |cur| {
            let mut spec = cur
                .cloned()
                .unwrap_or_else(|| TimelineSpec::empty(tenant_id, timeline_id));
            let result = decide(&mut spec)?;
            let write = match cur {
                Some(cur) if *cur == spec => Write::Keep,
                None if spec.is_empty() => Write::Keep,
                _ if spec.is_empty() => Write::Delete,
                _ => Write::Put(spec),
            };
            Ok((write, result))
        })
        .await
    }

    async fn timeline(
        &self,
        tenant_id: &str,
        timeline_id: &str,
    ) -> DatabaseResult<Option<TimelineSpec>> {
        Ok(self
            .get::<TimelineSpec>(&timeline_object_name(tenant_id, timeline_id))
            .await?
            .map(|o| o.spec))
    }

    async fn timelines_of(&self, tenant_id: &str) -> DatabaseResult<Vec<TimelineSpec>> {
        Ok(self
            .list::<TimelineSpec>(Some(&format!("{}={tenant_id}", objects::TENANT_LABEL)))
            .await?
            .into_iter()
            .map(|o| o.spec)
            .collect())
    }

    pub(crate) async fn insert_timeline(&self, entry: TimelinePersistence) -> DatabaseResult<bool> {
        self.measured(
            DatabaseOperation::InsertTimeline,
            self.modify_timeline(&entry.tenant_id, &entry.timeline_id, |spec| {
                if spec.membership.is_some() {
                    return Ok(false);
                }
                spec.membership = Some(Membership::from_row(&entry));
                Ok(true)
            }),
        )
        .await
    }

    pub(crate) async fn update_timeline_unsafe(
        &self,
        entry: TimelineUpdate,
    ) -> DatabaseResult<bool> {
        self.measured(
            DatabaseOperation::UpdateTimeline,
            self.modify_timeline(&entry.tenant_id, &entry.timeline_id, |spec| {
                let Some(m) = spec.membership.as_mut() else {
                    return Ok(false);
                };
                m.start_lsn = entry.start_lsn.0.to_string();
                m.sk_set = entry.sk_set.clone();
                m.new_sk_set = entry.new_sk_set.clone();
                Ok(true)
            }),
        )
        .await
    }

    pub(crate) async fn update_timeline_membership(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        new_generation: SafekeeperGeneration,
        sk_set: &[NodeId],
        new_sk_set: Option<&[NodeId]>,
        reconcile_requests: &[TimelinePendingOpPersistence],
    ) -> DatabaseResult<()> {
        let prev_generation = new_generation.previous().unwrap().into_inner() as i32;
        let (tenant, timeline) = (tenant_id.to_string(), timeline_id.to_string());
        // The membership and the operations it schedules are one write, as they are one
        // transaction on Postgres; so they must all be this timeline's.
        if reconcile_requests
            .iter()
            .any(|r| r.tenant_id != tenant || r.timeline_id != timeline)
        {
            return Err(DatabaseError::Logical(format!(
                "update_timeline_membership {tenant}/{timeline}: an operation for another timeline"
            )));
        }
        self.measured(
            DatabaseOperation::UpdateTimelineMembership,
            self.modify_timeline(&tenant, &timeline, |spec| {
                match spec.membership.as_mut() {
                    Some(m) if m.generation == prev_generation => {
                        m.generation = new_generation.into_inner() as i32;
                        m.sk_set = sk_set.iter().map(|id| id.0 as i64).collect();
                        m.new_sk_set =
                            new_sk_set.map(|set| set.iter().map(|id| id.0 as i64).collect());
                    }
                    _ => {
                        return Err(DatabaseError::Cas(
                            "Failed to update membership configuration".to_string(),
                        ));
                    }
                }
                for req in reconcile_requests {
                    upsert_pending_op(spec, req);
                }
                Ok(())
            }),
        )
        .await
    }

    async fn update_notified_generation(
        &self,
        op: DatabaseOperation,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        generation: SafekeeperGeneration,
        field: fn(&mut Membership) -> &mut i32,
        what: &str,
    ) -> DatabaseResult<()> {
        let g = generation.into_inner() as i32;
        self.measured(
            op,
            self.modify_timeline(
                &tenant_id.to_string(),
                &timeline_id.to_string(),
                |spec| match spec.membership.as_mut().map(field) {
                    Some(current) if *current <= g => {
                        *current = g;
                        Ok(())
                    }
                    _ => Err(DatabaseError::Cas(format!("Failed to update {what}"))),
                },
            ),
        )
        .await
    }

    pub(crate) async fn update_cplane_notified_generation(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        generation: SafekeeperGeneration,
    ) -> DatabaseResult<()> {
        self.update_notified_generation(
            DatabaseOperation::UpdateCplaneNotifiedGeneration,
            tenant_id,
            timeline_id,
            generation,
            |m| &mut m.cplane_notified_generation,
            "cplane notified generation",
        )
        .await
    }

    pub(crate) async fn update_sk_set_notified_generation(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        generation: SafekeeperGeneration,
    ) -> DatabaseResult<()> {
        self.update_notified_generation(
            DatabaseOperation::UpdateSkSetNotifiedGeneration,
            tenant_id,
            timeline_id,
            generation,
            |m| &mut m.sk_set_notified_generation,
            "sk set notified generation",
        )
        .await
    }

    pub(crate) async fn get_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<Option<TimelinePersistence>> {
        self.measured(DatabaseOperation::GetTimeline, async {
            match self
                .timeline(&tenant_id.to_string(), &timeline_id.to_string())
                .await?
            {
                Some(spec) => spec.timeline_row(),
                None => Ok(None),
            }
        })
        .await
    }

    pub(crate) async fn timeline_set_deleted_at(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<()> {
        let deletion_time = objects::truncate_to_micros(chrono::Local::now().to_utc());
        self.measured(
            DatabaseOperation::InsertTimeline,
            self.modify_timeline(&tenant_id.to_string(), &timeline_id.to_string(), |spec| {
                if let Some(m) = spec.membership.as_mut() {
                    m.deleted_at = Some(deletion_time);
                }
                Ok(())
            }),
        )
        .await
    }

    pub(crate) async fn delete_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<()> {
        self.measured(
            DatabaseOperation::GetTimeline,
            self.modify_timeline(&tenant_id.to_string(), &timeline_id.to_string(), |spec| {
                // Only a timeline whose deleted_at is set
                if spec
                    .membership
                    .as_ref()
                    .is_some_and(|m| m.deleted_at.is_some())
                {
                    spec.membership = None;
                }
                Ok(())
            }),
        )
        .await
    }

    pub(crate) async fn list_timelines_for_tenant(
        &self,
        tenant_id: TenantId,
    ) -> DatabaseResult<Vec<TimelinePersistence>> {
        self.measured(DatabaseOperation::GetTimeline, async {
            let mut out = Vec::new();
            for spec in self.timelines_of(&tenant_id.to_string()).await? {
                out.extend(spec.timeline_row()?);
            }
            Ok(out)
        })
        .await
    }

    pub(crate) async fn insert_pending_op(
        &self,
        entry: TimelinePendingOpPersistence,
    ) -> DatabaseResult<bool> {
        self.measured(
            DatabaseOperation::InsertTimelineReconcile,
            self.modify_timeline(&entry.tenant_id, &entry.timeline_id, |spec| {
                Ok(upsert_pending_op(spec, &entry))
            }),
        )
        .await
    }

    pub(crate) async fn remove_pending_op(
        &self,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
        sk_id: NodeId,
        generation: u32,
    ) -> DatabaseResult<()> {
        let timeline = timeline_id.map(|t| t.to_string()).unwrap_or_default();
        self.measured(
            DatabaseOperation::RemoveTimelineReconcile,
            self.modify_timeline(&tenant_id.to_string(), &timeline, |spec| {
                spec.pending_ops.retain(|op| {
                    !(op.safekeeper_id == sk_id.0 as i64 && op.generation == generation as i32)
                });
                Ok(())
            }),
        )
        .await
    }

    pub(crate) async fn list_pending_ops_with_timelines(
        &self,
    ) -> DatabaseResult<Vec<(TimelinePendingOpPersistence, Option<TimelinePersistence>)>> {
        self.measured(DatabaseOperation::ListTimelineReconcileStartup, async {
            let mut out = Vec::new();
            for obj in self.list::<TimelineSpec>(None).await? {
                let timeline = obj.spec.timeline_row()?;
                for op in obj.spec.pending_op_rows() {
                    out.push((op, timeline.clone()));
                }
            }
            Ok(out)
        })
        .await
    }

    pub(crate) async fn list_pending_ops_for_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<Vec<TimelinePendingOpPersistence>> {
        let tenant = tenant_id.to_string();
        self.measured(DatabaseOperation::ListTimelineReconcile, async {
            let mut out = Vec::new();
            for timeline in [timeline_id.to_string(), String::new()] {
                if let Some(spec) = self.timeline(&tenant, &timeline).await? {
                    out.extend(spec.pending_op_rows());
                }
            }
            Ok(out)
        })
        .await
    }

    pub(crate) async fn remove_pending_ops_for_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
    ) -> DatabaseResult<()> {
        let timeline = timeline_id.map(|t| t.to_string()).unwrap_or_default();
        self.measured(
            DatabaseOperation::RemoveTimelineReconcile,
            self.modify_timeline(&tenant_id.to_string(), &timeline, |spec| {
                spec.pending_ops.clear();
                Ok(())
            }),
        )
        .await
    }

    pub(crate) async fn insert_timeline_import(
        &self,
        import: TimelineImportPersistence,
    ) -> DatabaseResult<bool> {
        self.measured(
            DatabaseOperation::InsertTimelineImport,
            self.modify_timeline(&import.tenant_id, &import.timeline_id, |spec| {
                if spec.import.is_some() {
                    return Err(DatabaseError::Exists(format!(
                        "timeline import {}/{} already exists",
                        import.tenant_id, import.timeline_id
                    )));
                }
                spec.import = Some(ImportRecord::from_row(&import));
                Ok(true)
            }),
        )
        .await
    }

    pub(crate) async fn list_timeline_imports(&self) -> DatabaseResult<Vec<TimelineImport>> {
        let persistent = self
            .measured(DatabaseOperation::ListTimelineImports, async {
                let mut out = Vec::new();
                for obj in self.list::<TimelineSpec>(None).await? {
                    out.extend(obj.spec.import_row()?);
                }
                Ok(out)
            })
            .await?;
        persistent
            .into_iter()
            .map(TimelineImport::from_persistent)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| DatabaseError::Logical(format!("failed to deserialize import: {err}")))
    }

    pub(crate) async fn get_timeline_import(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<Option<TimelineImport>> {
        let persistent = self
            .measured(DatabaseOperation::ListTimelineImports, async {
                match self
                    .timeline(&tenant_id.to_string(), &timeline_id.to_string())
                    .await?
                {
                    Some(spec) => spec.import_row(),
                    None => Ok(None),
                }
            })
            .await?;
        persistent
            .map(TimelineImport::from_persistent)
            .transpose()
            .map_err(|err| DatabaseError::Logical(format!("failed to deserialize import: {err}")))
    }

    pub(crate) async fn delete_timeline_import(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<()> {
        self.measured(
            DatabaseOperation::DeleteTimelineImport,
            self.modify_timeline(&tenant_id.to_string(), &timeline_id.to_string(), |spec| {
                spec.import = None;
                Ok(())
            }),
        )
        .await
    }

    pub(crate) async fn update_timeline_import(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        shard_status: ShardImportStatus,
    ) -> DatabaseResult<Result<Option<TimelineImport>, TimelineImportUpdateError>> {
        let tenant = tenant_shard_id.tenant_id.to_string();
        let timeline = timeline_id.to_string();
        self.measured(
            DatabaseOperation::UpdateTimelineImport,
            self.modify_timeline(&tenant, &timeline, |spec| {
                let not_found = TimelineImportUpdateError::ImportNotFound {
                    tenant_id: tenant_shard_id.tenant_id,
                    timeline_id,
                };
                let Some(row) = spec.import_row()? else {
                    return Ok(Err(not_found));
                };
                let mut status = TimelineImport::from_persistent(row).map_err(|e| {
                    DatabaseError::Logical(format!("failed to deserialize import: {e}"))
                })?;
                let follow_up =
                    match status.update(tenant_shard_id.to_index(), shard_status.clone()) {
                        Ok(ok) => ok,
                        Err(err) => return Ok(Err(err)),
                    };
                match follow_up {
                    TimelineImportUpdateFollowUp::Persist => {
                        spec.import = Some(ImportRecord::from_row(&status.to_persistent()));
                        Ok(Ok(Some(status)))
                    }
                    TimelineImportUpdateFollowUp::None => Ok(Ok(None)),
                }
            }),
        )
        .await
    }

    pub(crate) async fn is_tenant_importing_timeline(
        &self,
        tenant_id: TenantId,
    ) -> DatabaseResult<bool> {
        self.measured(DatabaseOperation::IsTenantImportingTimeline, async {
            Ok(self
                .timelines_of(&tenant_id.to_string())
                .await?
                .iter()
                .any(|t| t.import.is_some()))
        })
        .await
    }
}

/// Neon's upsert of a pending operation: keep at most one per safekeeper, and replace it only
/// with one of a higher generation. Whether anything was written.
fn upsert_pending_op(spec: &mut TimelineSpec, entry: &TimelinePendingOpPersistence) -> bool {
    let new = PendingOp::from_row(entry);
    match spec
        .pending_ops
        .iter_mut()
        .find(|op| op.safekeeper_id == new.safekeeper_id)
    {
        Some(op) if op.generation < new.generation => {
            *op = new;
            true
        }
        Some(_) => false,
        None => {
            spec.pending_ops.push(new);
            true
        }
    }
}

fn shard_key(id: &TenantShardId) -> (i32, i32) {
    (id.shard_number.0 as i32, id.shard_count.literal() as i32)
}

fn shard_not_found(id: &TenantShardId) -> DatabaseError {
    DatabaseError::Logical(format!("tenant shard {id} not found"))
}

/// The API server stored the object without a field this controller wrote: the installed kinds
/// are older than the controller. Stop rather than run with records that drop data.
fn check_stored<S: Spec>(name: &str, sent: &S, stored: &S) -> DatabaseResult<()> {
    if sent == stored {
        Ok(())
    } else {
        Err(DatabaseError::Logical(format!(
            "{} {name} was stored without some of what was written (are the installed kinds \
             older than this controller?): wrote {sent:?}, stored {stored:?}",
            S::KIND
        )))
    }
}

fn is_conflict(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(s) if s.code == 409)
}

fn is_not_found(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(s) if s.code == 404)
}

#[cfg(test)]
mod guard_tests {
    use super::*;

    const L: &str = LEADER_OBJECT_NAME;

    fn leader(rev: u64) -> Object<LeaderSpec> {
        Object::new(
            L,
            LeaderSpec {
                address: "http://controller.example.com:1234".to_string(),
                started_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            },
            rev,
        )
    }

    fn absent(g: &Guard, sent: u64) -> bool {
        g.observe_absent::<LeaderSpec>(L, sent).is_ok()
    }

    fn read(g: &Guard, rev: u64, sent: u64) -> bool {
        g.observe(&leader(rev), sent).is_ok()
    }

    /// Every interleaving of a read with this controller's writes, deletes and remakes of one
    /// name: refused only when the store went back.
    #[test]
    fn a_read_is_refused_only_when_the_store_went_back() {
        // Seen at 5; a read sent now, a write of 6 finished while it was in flight: its answer
        // at 5 is older than the write, not the store going back, and lowers nothing
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        let sent = g.before_read();
        g.wrote::<LeaderSpec>(L, 6, false);
        assert!(read(&g, 5, sent));
        assert_eq!(g.next_revision::<LeaderSpec>(L), 7);
        // Sent after the write, answered at 5: went back
        assert!(!read(&g, 5, g.before_read()));

        // Seen at 5 and never deleted, answered absent: went back
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        assert!(!absent(&g, g.before_read()));

        // Deleted by this controller at 5, then answered at 5: went back; at 6 (made again): fine
        let g = Guard::new();
        g.wrote::<LeaderSpec>(L, 5, true);
        assert!(!read(&g, 5, g.before_read()));
        let g = Guard::new();
        g.wrote::<LeaderSpec>(L, 5, true);
        assert!(read(&g, 6, g.before_read()));

        // A read sent before a delete, answered present at the deleted revision: fine, and the
        // delete stays noted
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        let sent = g.before_read();
        g.wrote::<LeaderSpec>(L, 5, true);
        assert!(read(&g, 5, sent));
        assert!(!read(&g, 5, g.before_read()));
    }

    #[test]
    fn an_absence_is_accepted_after_any_delete_since_the_read_was_sent() {
        // A name first seen after the read was sent (made while a list was in flight)
        let g = Guard::new();
        let sent = g.before_read();
        g.wrote::<LeaderSpec>(L, 1, false);
        assert!(absent(&g, sent));

        // Deleted while the read was in flight
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        let sent = g.before_read();
        g.wrote::<LeaderSpec>(L, 5, true);
        assert!(absent(&g, sent));

        // Deleted and made again while a list was in flight, the list's snapshot between the two
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        let sent = g.before_read();
        g.wrote::<LeaderSpec>(L, 5, true);
        g.wrote::<LeaderSpec>(L, 6, false);
        assert!(absent(&g, sent));
        // But a read sent after the remake that finds it gone: went back
        assert!(!absent(&g, g.before_read()));

        // A delete whose answer was lost, then a get, then the remake, the get answered absent
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        g.delete_in_doubt::<LeaderSpec>(L, 5);
        let sent = g.before_read();
        g.wrote::<LeaderSpec>(L, 6, false);
        assert!(absent(&g, sent));

        // A get sent before a delete whose answer was lost, answered present from before the
        // delete; the delete did happen, and a later get finds it gone: not the store going back
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        let sent = g.before_read();
        g.delete_in_doubt::<LeaderSpec>(L, 5);
        assert!(read(&g, 5, sent));
        assert!(absent(&g, g.before_read()));

        // A delete in doubt that did not happen: the object read at its revision is fine; only a
        // new revision shows it live again, after which its absence is the store going back
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        g.delete_in_doubt::<LeaderSpec>(L, 5);
        assert!(read(&g, 5, g.before_read()));
        assert!(absent(&g, g.before_read()));
        g.wrote::<LeaderSpec>(L, 6, false);
        assert!(!absent(&g, g.before_read()));
        assert!(!read(&g, 5, g.before_read()));
    }

    #[test]
    fn reads_at_an_unchanged_revision_do_not_fill_the_history() {
        let g = Guard::new();
        assert!(read(&g, 5, g.before_read()));
        let slow = g.before_read();
        for _ in 0..100 {
            assert!(read(&g, 5, g.before_read()));
        }
        g.wrote::<LeaderSpec>(L, 6, false);
        // The slow read's baseline (5) is still kept, so its answer is compared: 4 is refused
        assert!(!read(&g, 4, slow));
    }
}
