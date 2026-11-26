//! The storage controller's records in Kubernetes objects (`storage_controller/kubernetes/`).
//!
//! Every operation of [`super::Persistence`] is done against the API server, never a cache: a
//! read is a quorum read, and every write is a compare-and-swap of one object on its
//! `resourceVersion` (a create fails if the object exists, a delete carries the version it read),
//! retried on a conflict as a serialization failure is retried on Postgres. The records are
//! grouped one object per tenant, holding all its shards, so every multi-row transaction Neon's
//! controller relies on for generations (an increment, a split's begin, completion and abort,
//! one tenant's part of a re-attach) is one write of one object.

pub(crate) mod copy;
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
use tokio_util::sync::CancellationToken;
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
    /// The copy in the bucket, when one is configured; written from the start check on.
    copy: Option<Arc<copy::BucketCopy>>,
    /// Stops the copy's writing.
    copy_cancel: CancellationToken,
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
    pub(crate) fn new(
        client: kube::Client,
        namespace: String,
        copy: Option<Arc<copy::BucketCopy>>,
    ) -> Self {
        KubernetesStore {
            client,
            namespace,
            copy,
            copy_cancel: CancellationToken::new(),
        }
    }

    /// Another store on the same records, as a second controller would have.
    #[cfg(test)]
    pub(crate) fn second_client(&self) -> Self {
        KubernetesStore {
            client: self.client.clone(),
            namespace: self.namespace.clone(),
            copy: None,
            copy_cancel: CancellationToken::new(),
        }
    }

    /// Stop writing the copy, as a controller lost with its cluster stops.
    #[cfg(test)]
    pub(crate) fn stop_copy(&self) {
        self.copy_cancel.cancel();
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
        Ok(self.api::<S>().get_opt(name).await?)
    }

    /// Every object of a kind (matching `labels`, when given), read page by page.
    async fn list<S: Spec>(&self, labels: Option<&str>) -> DatabaseResult<Vec<Object<S>>> {
        let api = self.api::<S>();
        let mut out = Vec::new();
        let mut params = ListParams::default().limit(LIST_PAGE);
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
        Ok(out)
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
        for _ in 0..MAX_RETRIES {
            let current = api.get_opt(name).await?;
            let (write, result) = decide(current.as_ref().map(|o| &o.spec))?;
            let written = match (write, current) {
                (Write::Keep, _) | (Write::Delete, None) => return Ok(result),
                (Write::Put(spec), None) => {
                    let obj = Object::new(name, spec);
                    match api.create(&PostParams::default(), &obj).await {
                        Ok(stored) => check_stored(name, &obj.spec, &stored.spec),
                        Err(e) if is_conflict(&e) => continue,
                        Err(e) => Err(e.into()),
                    }
                }
                (Write::Put(spec), Some(current)) => {
                    let obj = current.with_spec(spec);
                    match api.replace(name, &PostParams::default(), &obj).await {
                        Ok(stored) => check_stored(name, &obj.spec, &stored.spec),
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
                        Ok(_) => Ok(()),
                        Err(e) if is_conflict(&e) || is_not_found(&e) => continue,
                        Err(e) => Err(e.into()),
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
        tracing::info!(
            "Records are kept in Kubernetes objects in namespace {}",
            self.namespace
        );

        if let Some(copy) = &self.copy {
            // Records lost with their cluster would otherwise start again from nothing: every
            // pageserver told to drop its tenants, and generations the store has seen handed
            // out again. Only a restore from the copy goes on from here.
            let empty = self.list::<TenantSpec>(None).await?.is_empty()
                && self.list::<PageserverSpec>(None).await?.is_empty()
                && self.list::<SafekeeperSpec>(None).await?.is_empty();
            if empty {
                let held = copy.read().await.map_err(|e| {
                    DatabaseError::Logical(format!(
                        "the records are empty, and the copy in the bucket cannot be read to \
                         check that it is too: {e:#}"
                    ))
                })?;
                if let Some(doc) = held.filter(|d| !d.records.is_empty()) {
                    return Err(DatabaseError::Logical(format!(
                        "the records in namespace {} are empty but the copy in the bucket \
                         (written {}) holds {:?}: restore them with `storcon_records restore` \
                         before starting the controller",
                        self.namespace,
                        doc.written_at,
                        doc.records.counts()
                    )));
                }
            }
            copy::spawn(
                self.client.clone(),
                self.namespace.clone(),
                copy.clone(),
                self.copy_cancel.child_token(),
            );
            tracing::info!("Records are copied to the bucket within a second of each change");
        }
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
            // The health records go first (Postgres removes them with the shards), so none is
            // left without its shard.
            self.modify::<MetadataHealthSpec, ()>(&name, |_| Ok((Write::Delete, ())))
                .await?;
            self.modify::<TenantSpec, ()>(&name, |_| Ok((Write::Delete, ())))
                .await
        })
        .await
    }

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
                    let old = NodeSchedulingPolicy::from_str(&cur.scheduling_policy).unwrap();
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
        Ok(rows
            .into_iter()
            .map(|p| ShardGenerationState {
                tenant_shard_id: p
                    .get_tenant_shard_id()
                    .expect("Corrupt tenant shard id in database"),
                generation: p.generation.map(|g| Generation::new(g as u32)),
                generation_pageserver: p.generation_pageserver.map(|n| NodeId(n as u64)),
            })
            .collect())
    }

    pub(crate) async fn shard_generations(
        &self,
        tenant_shard_ids: impl Iterator<Item = &TenantShardId>,
    ) -> Result<Vec<(TenantShardId, Option<Generation>)>, DatabaseError> {
        let mut by_tenant: BTreeMap<TenantId, Vec<TenantShardId>> = BTreeMap::new();
        for id in tenant_shard_ids {
            by_tenant.entry(id.tenant_id).or_default().push(*id);
        }
        self.measured(DatabaseOperation::ShardGenerations, async {
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
            self.modify::<MetadataHealthSpec, ()>(&name, |cur| {
                let Some(cur) = cur else {
                    return Ok((Write::Keep, ()));
                };
                let mut spec = cur.clone();
                spec.shards.retain(|s| s.shard_count != old);
                Ok((
                    if spec == *cur {
                        Write::Keep
                    } else {
                        Write::Put(spec)
                    },
                    (),
                ))
            })
            .await
        })
        .await
    }

    pub(crate) async fn abort_shard_split(
        &self,
        split_tenant_id: TenantId,
        new_shard_count: ShardCount,
    ) -> DatabaseResult<AbortShardSplitStatus> {
        let new = new_shard_count.literal() as i32;
        self.measured(
            DatabaseOperation::AbortShardSplit,
            self.modify::<TenantSpec, AbortShardSplitStatus>(&split_tenant_id.to_string(), |cur| {
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
            }),
        )
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
                            last_scrubbed_at: objects::round_to_micros(now),
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

    async fn health_where(
        &self,
        keep: impl Fn(&HealthRecord) -> bool,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        Ok(self
            .list::<MetadataHealthSpec>(None)
            .await?
            .into_iter()
            .flat_map(|o| {
                o.spec
                    .shards
                    .iter()
                    .filter(|s| keep(s))
                    .map(|s| s.to_row(&o.spec.tenant_id))
                    .collect::<Vec<_>>()
            })
            .collect())
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
            self.health_where(|s| s.last_scrubbed_at < earlier),
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
        let deletion_time = objects::round_to_micros(chrono::Local::now().to_utc());
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

/// Every record at once, for the copy tool and the copy in the bucket.
impl KubernetesStore {
    async fn all<S: Spec>(&self) -> DatabaseResult<BTreeMap<String, S>> {
        Ok(self
            .list::<S>(None)
            .await?
            .into_iter()
            .map(|o| (o.metadata.name.unwrap_or_default(), o.spec))
            .collect())
    }

    pub(crate) async fn read_all(&self) -> DatabaseResult<super::records::Records> {
        let mut r = super::records::Records::empty();
        r.tenants = self.all().await?;
        r.metadata_health = self.all().await?;
        r.timelines = self.all().await?;
        r.pageservers = self.all().await?;
        r.safekeepers = self.all().await?;
        r.leader = self.all::<LeaderSpec>().await?.remove(LEADER_OBJECT_NAME);
        r.sort();
        Ok(r)
    }

    /// Write every record as a new object, first deleting every record object there is when
    /// `replace`.
    pub(crate) async fn write_all(
        &self,
        records: &super::records::Records,
        replace: bool,
    ) -> DatabaseResult<()> {
        async fn put<S: Spec>(
            store: &KubernetesStore,
            objects: impl Iterator<Item = (String, S)>,
            replace: bool,
        ) -> DatabaseResult<()> {
            let api = store.api::<S>();
            if replace {
                for name in store.all::<S>().await?.into_keys() {
                    match api.delete(&name, &DeleteParams::default()).await {
                        Ok(_) => {}
                        Err(e) if is_not_found(&e) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            futures::stream::iter(objects.map(|(name, spec)| {
                let api = api.clone();
                async move {
                    let obj = Object::new(&name, spec);
                    let stored = api.create(&PostParams::default(), &obj).await?;
                    check_stored(&name, &obj.spec, &stored.spec)
                }
            }))
            .buffer_unordered(RE_ATTACH_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;
            Ok(())
        }
        put(self, records.tenants.clone().into_iter(), replace).await?;
        put(self, records.metadata_health.clone().into_iter(), replace).await?;
        put(self, records.timelines.clone().into_iter(), replace).await?;
        put(self, records.pageservers.clone().into_iter(), replace).await?;
        put(self, records.safekeepers.clone().into_iter(), replace).await?;
        put(
            self,
            records
                .leader
                .clone()
                .map(|l| (LEADER_OBJECT_NAME.to_string(), l))
                .into_iter(),
            replace,
        )
        .await?;
        Ok(())
    }
}

/// A proxy from the environment (`HTTPS_PROXY`) is not used for an API server on this machine
/// or one that `NO_PROXY` names, as other clients treat it; this client has no proxy support, so
/// it would otherwise refuse to start.
pub(crate) fn skip_proxy_where_excluded(config: &mut kube::Config) {
    if config.proxy_url.is_none() {
        return;
    }
    let host = config.cluster_url.host().unwrap_or_default().to_string();
    let loopback = host == "localhost"
        || host
            .trim_matches(|c| c == '[' || c == ']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    let excluded = ["NO_PROXY", "no_proxy"]
        .iter()
        .filter_map(|v| std::env::var(v).ok())
        .flat_map(|list| {
            list.split(',')
                .map(|s| s.trim().trim_start_matches('.').to_string())
                .collect::<Vec<_>>()
        })
        .any(|entry| {
            !entry.is_empty()
                && (entry == "*" || host == entry || host.ends_with(&format!(".{entry}")))
        });
    if loopback || excluded {
        config.proxy_url = None;
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
