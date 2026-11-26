pub(crate) mod kubernetes;
mod postgres;
pub(crate) mod records;
pub(crate) mod split_state;
#[cfg(test)]
pub(crate) mod tests;
use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;
use std::time::Duration;

use diesel::deserialize::{FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::serialize::{IsNull, ToSql};
use pageserver_api::controller_api::{
    AvailabilityZone, MetadataHealthRecord, NodeSchedulingPolicy, PlacementPolicy,
    SafekeeperDescribeResponse, ShardSchedulingPolicy, SkSchedulingPolicy,
};
use pageserver_api::models::{ShardImportStatus, TenantConfig};
use pageserver_api::shard::{
    ShardConfigError, ShardCount, ShardIdentity, ShardNumber, ShardStripeSize, TenantShardId,
};
use safekeeper_api::membership::SafekeeperGeneration;
use serde::{Deserialize, Serialize};
use utils::generation::Generation;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::lsn::Lsn;

use self::split_state::SplitState;
use crate::node::Node;
use crate::timeline_import::{TimelineImport, TimelineImportUpdateError};

/// ## What do we store?
///
/// The storage controller service does not store most of its state durably.
///
/// The essential things to store durably are:
/// - generation numbers, as these must always advance monotonically to ensure data safety.
/// - Tenant's PlacementPolicy and TenantConfig, as the source of truth for these is something external.
/// - Node's scheduling policies, as the source of truth for these is something external.
///
/// Other things we store durably as an implementation detail:
/// - Node's host/port: this could be avoided it we made nodes emit a self-registering heartbeat,
///   but it is operationally simpler to make this service the authority for which nodes
///   it talks to.
///
/// ## Performance/efficiency
///
/// The storage controller service does not go via the database for most things: there are
/// a couple of places where we must, and where efficiency matters:
/// - Incrementing generation numbers: the Reconciler has to wait for this to complete
///   before it can attach a tenant, so this acts as a bound on how fast things like
///   failover can happen.
/// - Pageserver re-attach: we will increment many shards' generations when this happens,
///   so it is important to avoid e.g. issuing O(N) queries.
///
/// Database calls relating to nodes have low performance requirements, as they are very rarely
/// updated, and reads of nodes are always from memory, not the database.  We only require that
/// we can UPDATE a node's scheduling mode reasonably quickly to mark a bad node offline.
///
/// ## Where the records are kept
///
/// This type is the one interface the rest of the controller uses; each operation is passed to
/// the store the records are kept in: Postgres ([`postgres::PostgresStore`], Neon's code
/// unchanged, and the default) or Kubernetes objects ([`kubernetes::KubernetesStore`], described
/// in `storage_controller/kubernetes/README.md`).
pub struct Persistence {
    store: Store,
}

/// Where the records are kept.
enum Store {
    Postgres(postgres::PostgresStore),
    Kubernetes(kubernetes::KubernetesStore),
}

/// Legacy format, for use in JSON compat objects in test environment
#[derive(Serialize, Deserialize)]
struct JsonPersistence {
    tenants: HashMap<TenantShardId, TenantShardPersistence>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum DatabaseError {
    #[error(transparent)]
    Query(#[from] diesel::result::Error),
    #[error(transparent)]
    Connection(#[from] diesel::result::ConnectionError),
    #[error(transparent)]
    ConnectionPool(#[from] diesel_async::pooled_connection::bb8::RunError),
    #[error("Logical error: {0}")]
    Logical(String),
    #[error("Migration error: {0}")]
    Migration(String),
    #[error("CAS error: {0}")]
    Cas(String),
    /// A request to the Kubernetes API server failed (the Kubernetes store only).
    #[error(transparent)]
    Kubernetes(#[from] kube::Error),
    /// A record that must be new already exists: the Kubernetes store's counterpart of a unique
    /// violation on Postgres.
    #[error("Already exists: {0}")]
    Exists(String),
}

impl DatabaseError {
    /// The record being inserted already exists: a unique violation on Postgres, or
    /// [`DatabaseError::Exists`] from the Kubernetes store.
    pub(crate) fn is_unique_violation(&self) -> bool {
        matches!(
            self,
            DatabaseError::Query(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UniqueViolation,
                _,
            )) | DatabaseError::Exists(_)
        )
    }
}

#[derive(measured::FixedCardinalityLabel, Copy, Clone)]
pub(crate) enum DatabaseOperation {
    InsertNode,
    UpdateNode,
    DeleteNode,
    ListNodes,
    ListTombstones,
    BeginShardSplit,
    CompleteShardSplit,
    AbortShardSplit,
    Detach,
    ReAttach,
    IncrementGeneration,
    TenantGenerations,
    ShardGenerations,
    ListTenantShards,
    LoadTenant,
    InsertTenantShards,
    UpdateTenantShard,
    DeleteTenant,
    UpdateTenantConfig,
    UpdateMetadataHealth,
    ListMetadataHealth,
    ListMetadataHealthUnhealthy,
    ListMetadataHealthOutdated,
    ListSafekeepers,
    GetLeader,
    UpdateLeader,
    SetPreferredAzs,
    InsertTimeline,
    UpdateTimeline,
    UpdateTimelineMembership,
    UpdateCplaneNotifiedGeneration,
    UpdateSkSetNotifiedGeneration,
    GetTimeline,
    InsertTimelineReconcile,
    RemoveTimelineReconcile,
    ListTimelineReconcile,
    ListTimelineReconcileStartup,
    InsertTimelineImport,
    UpdateTimelineImport,
    DeleteTimelineImport,
    ListTimelineImports,
    IsTenantImportingTimeline,
}

#[must_use]
pub(crate) enum AbortShardSplitStatus {
    /// We aborted the split in the database by reverting to the parent shards
    Aborted,
    /// The split had already been persisted.
    Complete,
}

pub(crate) type DatabaseResult<T> = Result<T, DatabaseError>;

/// Some methods can operate on either a whole tenant or a single shard
#[derive(Clone)]
pub(crate) enum TenantFilter {
    Tenant(TenantId),
    Shard(TenantShardId),
}

/// Represents the results of looking up generation+pageserver for the shards of a tenant
pub(crate) struct ShardGenerationState {
    pub(crate) tenant_shard_id: TenantShardId,
    pub(crate) generation: Option<Generation>,
    pub(crate) generation_pageserver: Option<NodeId>,
}

// A generous allowance for how many times we may retry serializable transactions
// before giving up.  This is not expected to be hit: it is a defensive measure in case we
// somehow engineer a situation where duelling transactions might otherwise live-lock.
const MAX_RETRIES: usize = 128;

impl Persistence {
    // The default postgres connection limit is 100.  We use up to 99, to leave one free for a human admin under
    // normal circumstances.  This assumes we have exclusive use of the database cluster to which we connect.
    pub const MAX_CONNECTIONS: u32 = postgres::PostgresStore::MAX_CONNECTIONS;

    /// Keep the records in the Postgres database at `database_url`.
    pub async fn new(database_url: String) -> Self {
        Self {
            store: Store::Postgres(postgres::PostgresStore::new(database_url).await),
        }
    }

    /// Keep the records in Kubernetes objects in `namespace`, or, when none is given, in the
    /// namespace of the pod's service account (outside a pod, the kubeconfig's current one);
    /// and, when `copy` names a bucket, a copy of them there. The API server is reached through
    /// `kubeconfig` when given, and otherwise as any client reaches it (in a pod, its service
    /// account; outside one, `KUBECONFIG` or `~/.kube/config`).
    pub async fn new_kubernetes(
        kubeconfig: Option<&std::path::Path>,
        namespace: Option<String>,
        copy: Option<remote_storage::RemoteStorageConfig>,
    ) -> anyhow::Result<Self> {
        let mut config = match kubeconfig {
            Some(path) => {
                kube::Config::from_custom_kubeconfig(
                    kube::config::Kubeconfig::read_from(path)?,
                    &Default::default(),
                )
                .await?
            }
            None => kube::Config::infer().await?,
        };
        kubernetes::skip_proxy_where_excluded(&mut config);
        let namespace = namespace.unwrap_or_else(|| config.default_namespace.clone());
        let client = kube::Client::try_from(config)?;
        let copy = match copy {
            Some(config) => Some(std::sync::Arc::new(
                kubernetes::copy::BucketCopy::new(&config).await?,
            )),
            None => None,
        };
        Ok(Self {
            store: Store::Kubernetes(kubernetes::KubernetesStore::new(client, namespace, copy)),
        })
    }

    /// Stop writing the copy in the bucket, as a controller lost with its cluster stops.
    #[cfg(test)]
    pub(crate) fn stop_copy(&self) {
        if let Store::Kubernetes(store) = &self.store {
            store.stop_copy();
        }
    }

    /// Every record at once, in either store (for the copy tool, with the controller stopped).
    pub(crate) async fn read_all_records(&self) -> DatabaseResult<records::Records> {
        match &self.store {
            Store::Postgres(store) => records::Records::from_rows(store.read_all().await?),
            Store::Kubernetes(store) => store.read_all().await,
        }
    }

    /// Write every record, first removing what the store holds when `replace` (for the copy
    /// tool, with the controller stopped).
    pub(crate) async fn write_all_records(
        &self,
        records: &records::Records,
        replace: bool,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.write_all(&records.to_rows()?, replace).await,
            Store::Kubernetes(store) => store.write_all(records, replace).await,
        }
    }

    /// A helper for use during startup, where we would like to tolerate concurrent restarts of the
    /// database and the storage controller, therefore the database might not be available right away
    pub async fn await_connection(
        database_url: &str,
        timeout: Duration,
    ) -> Result<(), diesel::ConnectionError> {
        postgres::PostgresStore::await_connection(database_url, timeout).await
    }

    /// Execute the diesel migrations that are built into this binary
    pub(crate) async fn migration_run(&self) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.migration_run().await,
            Store::Kubernetes(store) => store.migration_run().await,
        }
    }

    /// When a node is first registered, persist it before using it for anything
    /// If the provided node_id already exists, it will be error.
    /// The common case is when a node marked for deletion wants to register.
    pub(crate) async fn insert_node(&self, node: &Node) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.insert_node(node).await,
            Store::Kubernetes(store) => store.insert_node(node).await,
        }
    }

    /// At startup, populate the list of nodes which our shards may be placed on
    pub(crate) async fn list_nodes(&self) -> DatabaseResult<Vec<NodePersistence>> {
        match &self.store {
            Store::Postgres(store) => store.list_nodes().await,
            Store::Kubernetes(store) => store.list_nodes().await,
        }
    }

    pub(crate) async fn list_tombstones(&self) -> DatabaseResult<Vec<NodePersistence>> {
        match &self.store {
            Store::Postgres(store) => store.list_tombstones().await,
            Store::Kubernetes(store) => store.list_tombstones().await,
        }
    }

    pub(crate) async fn update_node_scheduling_policy(
        &self,
        input_node_id: NodeId,
        input_scheduling: NodeSchedulingPolicy,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_node_scheduling_policy(input_node_id, input_scheduling)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_node_scheduling_policy(input_node_id, input_scheduling)
                    .await
            }
        }
    }

    pub(crate) async fn update_node_on_registration(
        &self,
        input_node_id: NodeId,
        input_https_port: Option<u16>,
        input_grpc_addr: Option<String>,
        input_grpc_port: Option<u16>,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_node_on_registration(
                        input_node_id,
                        input_https_port,
                        input_grpc_addr,
                        input_grpc_port,
                    )
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_node_on_registration(
                        input_node_id,
                        input_https_port,
                        input_grpc_addr,
                        input_grpc_port,
                    )
                    .await
            }
        }
    }

    /// Tombstone is a special state where the node is not deleted from the database,
    /// but it is not available for usage.
    /// The main reason for it is to prevent the flaky node to register.
    pub(crate) async fn set_tombstone(&self, del_node_id: NodeId) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.set_tombstone(del_node_id).await,
            Store::Kubernetes(store) => store.set_tombstone(del_node_id).await,
        }
    }

    pub(crate) async fn delete_node(&self, del_node_id: NodeId) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.delete_node(del_node_id).await,
            Store::Kubernetes(store) => store.delete_node(del_node_id).await,
        }
    }

    /// At startup, load the high level state for shards, such as their config + policy.  This will
    /// be enriched at runtime with state discovered on pageservers.
    ///
    /// We exclude shards configured to be detached.  During startup, if we see any attached locations
    /// for such shards, they will automatically be detached as 'orphans'.
    pub(crate) async fn load_active_tenant_shards(
        &self,
    ) -> DatabaseResult<Vec<TenantShardPersistence>> {
        match &self.store {
            Store::Postgres(store) => store.load_active_tenant_shards().await,
            Store::Kubernetes(store) => store.load_active_tenant_shards().await,
        }
    }

    /// When restoring a previously detached tenant into memory, load it from the database
    pub(crate) async fn load_tenant(
        &self,
        filter_tenant_id: TenantId,
    ) -> DatabaseResult<Vec<TenantShardPersistence>> {
        match &self.store {
            Store::Postgres(store) => store.load_tenant(filter_tenant_id).await,
            Store::Kubernetes(store) => store.load_tenant(filter_tenant_id).await,
        }
    }

    /// Tenants must be persisted before we schedule them for the first time.  This enables us
    /// to correctly retain generation monotonicity, and the externally provided placement policy & config.
    pub(crate) async fn insert_tenant_shards(
        &self,
        shards: Vec<TenantShardPersistence>,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.insert_tenant_shards(shards).await,
            Store::Kubernetes(store) => store.insert_tenant_shards(shards).await,
        }
    }

    /// Ordering: call this _after_ deleting the tenant on pageservers, but _before_ dropping state for
    /// the tenant from memory on this server.
    pub(crate) async fn delete_tenant(&self, del_tenant_id: TenantId) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.delete_tenant(del_tenant_id).await,
            Store::Kubernetes(store) => store.delete_tenant(del_tenant_id).await,
        }
    }

    /// When a tenant invokes the /re-attach API, this function is responsible for doing an efficient
    /// batched increment of the generations of all tenants whose generation_pageserver is equal to
    /// the node that called /re-attach.
    #[tracing::instrument(skip_all, fields(node_id))]
    pub(crate) async fn re_attach(
        &self,
        input_node_id: NodeId,
    ) -> DatabaseResult<HashMap<TenantShardId, Generation>> {
        match &self.store {
            Store::Postgres(store) => store.re_attach(input_node_id).await,
            Store::Kubernetes(store) => store.re_attach(input_node_id).await,
        }
    }

    /// Reconciler calls this immediately before attaching to a new pageserver, to acquire a unique, monotonically
    /// advancing generation number.  We also store the NodeId for which the generation was issued, so that in
    /// [`Self::re_attach`] we can do a bulk UPDATE on the generations for that node.
    pub(crate) async fn increment_generation(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<Generation> {
        match &self.store {
            Store::Postgres(store) => store.increment_generation(tenant_shard_id, node_id).await,
            Store::Kubernetes(store) => store.increment_generation(tenant_shard_id, node_id).await,
        }
    }

    /// When we want to call out to the running shards for a tenant, e.g. during timeline CRUD operations,
    /// we need to know where the shard is attached, _and_ the generation, so that we can re-check the generation
    /// afterwards to confirm that our timeline CRUD operation is truly persistent (it must have happened in the
    /// latest generation)
    ///
    /// If the tenant doesn't exist, an empty vector is returned.
    ///
    /// Output is sorted by shard number
    pub(crate) async fn tenant_generations(
        &self,
        filter_tenant_id: TenantId,
    ) -> Result<Vec<ShardGenerationState>, DatabaseError> {
        match &self.store {
            Store::Postgres(store) => store.tenant_generations(filter_tenant_id).await,
            Store::Kubernetes(store) => store.tenant_generations(filter_tenant_id).await,
        }
    }

    /// Read the generation number of specific tenant shards
    ///
    /// Output is unsorted.  Output may not include values for all inputs, if they are missing in the database.
    pub(crate) async fn shard_generations(
        &self,
        tenant_shard_ids: impl Iterator<Item = &TenantShardId>,
    ) -> Result<Vec<(TenantShardId, Option<Generation>)>, DatabaseError> {
        match &self.store {
            Store::Postgres(store) => store.shard_generations(tenant_shard_ids).await,
            Store::Kubernetes(store) => store.shard_generations(tenant_shard_ids).await,
        }
    }

    #[allow(non_local_definitions)]
    /// For use when updating a persistent property of a tenant, such as its config or placement_policy.
    ///
    /// Do not use this for settting generation, unless in the special onboarding code path (/location_config)
    /// API: use [`Self::increment_generation`] instead.  Setting the generation via this route is a one-time thing
    /// that we only do the first time a tenant is set to an attached policy via /location_config.
    pub(crate) async fn update_tenant_shard(
        &self,
        tenant: TenantFilter,
        input_placement_policy: Option<PlacementPolicy>,
        input_config: Option<TenantConfig>,
        input_generation: Option<Generation>,
        input_scheduling_policy: Option<ShardSchedulingPolicy>,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_tenant_shard(
                        tenant,
                        input_placement_policy,
                        input_config,
                        input_generation,
                        input_scheduling_policy,
                    )
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_tenant_shard(
                        tenant,
                        input_placement_policy,
                        input_config,
                        input_generation,
                        input_scheduling_policy,
                    )
                    .await
            }
        }
    }

    /// Note that passing None for a shard clears the preferred AZ (rather than leaving it unmodified)
    pub(crate) async fn set_tenant_shard_preferred_azs(
        &self,
        preferred_azs: Vec<(TenantShardId, Option<AvailabilityZone>)>,
    ) -> DatabaseResult<Vec<(TenantShardId, Option<AvailabilityZone>)>> {
        match &self.store {
            Store::Postgres(store) => store.set_tenant_shard_preferred_azs(preferred_azs).await,
            Store::Kubernetes(store) => store.set_tenant_shard_preferred_azs(preferred_azs).await,
        }
    }

    pub(crate) async fn detach(&self, tenant_shard_id: TenantShardId) -> anyhow::Result<()> {
        match &self.store {
            Store::Postgres(store) => store.detach(tenant_shard_id).await,
            Store::Kubernetes(store) => store.detach(tenant_shard_id).await,
        }
    }

    pub(crate) async fn begin_shard_split(
        &self,
        old_shard_count: ShardCount,
        split_tenant_id: TenantId,
        parent_to_children: Vec<(TenantShardId, Vec<TenantShardPersistence>)>,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .begin_shard_split(old_shard_count, split_tenant_id, parent_to_children)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .begin_shard_split(old_shard_count, split_tenant_id, parent_to_children)
                    .await
            }
        }
    }

    pub(crate) async fn complete_shard_split(
        &self,
        split_tenant_id: TenantId,
        old_shard_count: ShardCount,
        new_shard_count: ShardCount,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .complete_shard_split(split_tenant_id, old_shard_count, new_shard_count)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .complete_shard_split(split_tenant_id, old_shard_count, new_shard_count)
                    .await
            }
        }
    }

    /// Used when the remote part of a shard split failed: we will revert the database state to have only
    /// the parent shards, with SplitState::Idle.
    pub(crate) async fn abort_shard_split(
        &self,
        split_tenant_id: TenantId,
        new_shard_count: ShardCount,
    ) -> DatabaseResult<AbortShardSplitStatus> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .abort_shard_split(split_tenant_id, new_shard_count)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .abort_shard_split(split_tenant_id, new_shard_count)
                    .await
            }
        }
    }

    /// Stores all the latest metadata health updates durably. Updates existing entry on conflict.
    ///
    /// **Correctness:** `metadata_health_updates` should all belong the tenant shards managed by the storage controller.
    #[allow(dead_code)]
    pub(crate) async fn update_metadata_health_records(
        &self,
        healthy_records: Vec<MetadataHealthPersistence>,
        unhealthy_records: Vec<MetadataHealthPersistence>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_metadata_health_records(healthy_records, unhealthy_records, now)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_metadata_health_records(healthy_records, unhealthy_records, now)
                    .await
            }
        }
    }

    /// Lists all the metadata health records.
    #[allow(dead_code)]
    pub(crate) async fn list_metadata_health_records(
        &self,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        match &self.store {
            Store::Postgres(store) => store.list_metadata_health_records().await,
            Store::Kubernetes(store) => store.list_metadata_health_records().await,
        }
    }

    /// Lists all the metadata health records that is unhealthy.
    #[allow(dead_code)]
    pub(crate) async fn list_unhealthy_metadata_health_records(
        &self,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        match &self.store {
            Store::Postgres(store) => store.list_unhealthy_metadata_health_records().await,
            Store::Kubernetes(store) => store.list_unhealthy_metadata_health_records().await,
        }
    }

    /// Lists all the metadata health records that have not been updated since an `earlier` time.
    #[allow(dead_code)]
    pub(crate) async fn list_outdated_metadata_health_records(
        &self,
        earlier: chrono::DateTime<chrono::Utc>,
    ) -> DatabaseResult<Vec<MetadataHealthPersistence>> {
        match &self.store {
            Store::Postgres(store) => store.list_outdated_metadata_health_records(earlier).await,
            Store::Kubernetes(store) => store.list_outdated_metadata_health_records(earlier).await,
        }
    }

    /// Get the current entry from the `leader` table if one exists.
    /// It is an error for the table to contain more than one entry.
    pub(crate) async fn get_leader(&self) -> DatabaseResult<Option<ControllerPersistence>> {
        match &self.store {
            Store::Postgres(store) => store.get_leader().await,
            Store::Kubernetes(store) => store.get_leader().await,
        }
    }

    /// Update the new leader with compare-exchange semantics. If `prev` does not
    /// match the current leader entry, then the update is treated as a failure.
    /// When `prev` is not specified, the update is forced.
    pub(crate) async fn update_leader(
        &self,
        prev: Option<ControllerPersistence>,
        new: ControllerPersistence,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.update_leader(prev, new).await,
            Store::Kubernetes(store) => store.update_leader(prev, new).await,
        }
    }

    /// At startup, populate the list of nodes which our shards may be placed on
    pub(crate) async fn list_safekeepers(&self) -> DatabaseResult<Vec<SafekeeperPersistence>> {
        match &self.store {
            Store::Postgres(store) => store.list_safekeepers().await,
            Store::Kubernetes(store) => store.list_safekeepers().await,
        }
    }

    pub(crate) async fn safekeeper_upsert(
        &self,
        record: SafekeeperUpsert,
    ) -> Result<(), DatabaseError> {
        match &self.store {
            Store::Postgres(store) => store.safekeeper_upsert(record).await,
            Store::Kubernetes(store) => store.safekeeper_upsert(record).await,
        }
    }

    pub(crate) async fn set_safekeeper_scheduling_policy(
        &self,
        id_: i64,
        scheduling_policy_: SkSchedulingPolicy,
    ) -> Result<(), DatabaseError> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .set_safekeeper_scheduling_policy(id_, scheduling_policy_)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .set_safekeeper_scheduling_policy(id_, scheduling_policy_)
                    .await
            }
        }
    }

    /// Activate the given safekeeper, ensuring that there is no TOCTOU.
    /// Returns `Some` if the safekeeper has indeed been activating (or already active). Other states return `None`.
    pub(crate) async fn activate_safekeeper(&self, id_: i64) -> Result<Option<()>, DatabaseError> {
        match &self.store {
            Store::Postgres(store) => store.activate_safekeeper(id_).await,
            Store::Kubernetes(store) => store.activate_safekeeper(id_).await,
        }
    }

    /// Persist timeline. Returns if the timeline was newly inserted. If it wasn't, we haven't done any writes.
    pub(crate) async fn insert_timeline(&self, entry: TimelinePersistence) -> DatabaseResult<bool> {
        match &self.store {
            Store::Postgres(store) => store.insert_timeline(entry).await,
            Store::Kubernetes(store) => store.insert_timeline(entry).await,
        }
    }

    /// Update an already present timeline.
    /// VERY UNSAFE FUNCTION: this overrides in-progress migrations. Don't use this unless neccessary.
    pub(crate) async fn update_timeline_unsafe(
        &self,
        entry: TimelineUpdate,
    ) -> DatabaseResult<bool> {
        match &self.store {
            Store::Postgres(store) => store.update_timeline_unsafe(entry).await,
            Store::Kubernetes(store) => store.update_timeline_unsafe(entry).await,
        }
    }

    /// Update timeline membership configuration in the database.
    /// Perform a compare-and-swap (CAS) operation on the timeline's generation.
    /// The `new_generation` must be the next (+1) generation after the one in the database.
    /// Also inserts reconcile_requests to safekeeper_timeline_pending_ops table in the same
    /// transaction.
    pub(crate) async fn update_timeline_membership(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        new_generation: SafekeeperGeneration,
        sk_set: &[NodeId],
        new_sk_set: Option<&[NodeId]>,
        reconcile_requests: &[TimelinePendingOpPersistence],
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_timeline_membership(
                        tenant_id,
                        timeline_id,
                        new_generation,
                        sk_set,
                        new_sk_set,
                        reconcile_requests,
                    )
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_timeline_membership(
                        tenant_id,
                        timeline_id,
                        new_generation,
                        sk_set,
                        new_sk_set,
                        reconcile_requests,
                    )
                    .await
            }
        }
    }

    /// Update the cplane notified generation for a timeline.
    /// Perform a compare-and-swap (CAS) operation on the timeline's cplane notified generation.
    /// The update will fail if the specified generation is less than the cplane notified generation
    /// in the database.
    pub(crate) async fn update_cplane_notified_generation(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        generation: SafekeeperGeneration,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_cplane_notified_generation(tenant_id, timeline_id, generation)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_cplane_notified_generation(tenant_id, timeline_id, generation)
                    .await
            }
        }
    }

    /// Update the sk set notified generation for a timeline.
    /// Perform a compare-and-swap (CAS) operation on the timeline's sk set notified generation.
    /// The update will fail if the specified generation is less than the sk set notified generation
    /// in the database.
    pub(crate) async fn update_sk_set_notified_generation(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        generation: SafekeeperGeneration,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_sk_set_notified_generation(tenant_id, timeline_id, generation)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_sk_set_notified_generation(tenant_id, timeline_id, generation)
                    .await
            }
        }
    }

    /// Load timeline from db. Returns `None` if not present.
    pub(crate) async fn get_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<Option<TimelinePersistence>> {
        match &self.store {
            Store::Postgres(store) => store.get_timeline(tenant_id, timeline_id).await,
            Store::Kubernetes(store) => store.get_timeline(tenant_id, timeline_id).await,
        }
    }

    /// Set `delete_at` for the given timeline
    pub(crate) async fn timeline_set_deleted_at(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.timeline_set_deleted_at(tenant_id, timeline_id).await,
            Store::Kubernetes(store) => store.timeline_set_deleted_at(tenant_id, timeline_id).await,
        }
    }

    /// Load timeline from db. Returns `None` if not present.
    ///
    /// Only works if `deleted_at` is set, so you should call [`Self::timeline_set_deleted_at`] before.
    pub(crate) async fn delete_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.delete_timeline(tenant_id, timeline_id).await,
            Store::Kubernetes(store) => store.delete_timeline(tenant_id, timeline_id).await,
        }
    }

    /// Loads a list of all timelines from database.
    pub(crate) async fn list_timelines_for_tenant(
        &self,
        tenant_id: TenantId,
    ) -> DatabaseResult<Vec<TimelinePersistence>> {
        match &self.store {
            Store::Postgres(store) => store.list_timelines_for_tenant(tenant_id).await,
            Store::Kubernetes(store) => store.list_timelines_for_tenant(tenant_id).await,
        }
    }

    /// Persist pending op. Returns if it was newly inserted. If it wasn't, we haven't done any writes.
    pub(crate) async fn insert_pending_op(
        &self,
        entry: TimelinePendingOpPersistence,
    ) -> DatabaseResult<bool> {
        match &self.store {
            Store::Postgres(store) => store.insert_pending_op(entry).await,
            Store::Kubernetes(store) => store.insert_pending_op(entry).await,
        }
    }

    /// Remove persisted pending op.
    pub(crate) async fn remove_pending_op(
        &self,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
        sk_id: NodeId,
        generation: u32,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .remove_pending_op(tenant_id, timeline_id, sk_id, generation)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .remove_pending_op(tenant_id, timeline_id, sk_id, generation)
                    .await
            }
        }
    }

    /// Load pending operations from db, joined together with timeline data.
    pub(crate) async fn list_pending_ops_with_timelines(
        &self,
    ) -> DatabaseResult<Vec<(TimelinePendingOpPersistence, Option<TimelinePersistence>)>> {
        match &self.store {
            Store::Postgres(store) => store.list_pending_ops_with_timelines().await,
            Store::Kubernetes(store) => store.list_pending_ops_with_timelines().await,
        }
    }

    /// List pending operations for a given timeline (including tenant-global ones)
    pub(crate) async fn list_pending_ops_for_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<Vec<TimelinePendingOpPersistence>> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .list_pending_ops_for_timeline(tenant_id, timeline_id)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .list_pending_ops_for_timeline(tenant_id, timeline_id)
                    .await
            }
        }
    }

    /// Delete all pending ops for the given timeline.
    ///
    /// Use this only at timeline deletion, otherwise use generation based APIs
    pub(crate) async fn remove_pending_ops_for_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .remove_pending_ops_for_timeline(tenant_id, timeline_id)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .remove_pending_ops_for_timeline(tenant_id, timeline_id)
                    .await
            }
        }
    }

    pub(crate) async fn insert_timeline_import(
        &self,
        import: TimelineImportPersistence,
    ) -> DatabaseResult<bool> {
        match &self.store {
            Store::Postgres(store) => store.insert_timeline_import(import).await,
            Store::Kubernetes(store) => store.insert_timeline_import(import).await,
        }
    }

    pub(crate) async fn list_timeline_imports(&self) -> DatabaseResult<Vec<TimelineImport>> {
        match &self.store {
            Store::Postgres(store) => store.list_timeline_imports().await,
            Store::Kubernetes(store) => store.list_timeline_imports().await,
        }
    }

    pub(crate) async fn get_timeline_import(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<Option<TimelineImport>> {
        match &self.store {
            Store::Postgres(store) => store.get_timeline_import(tenant_id, timeline_id).await,
            Store::Kubernetes(store) => store.get_timeline_import(tenant_id, timeline_id).await,
        }
    }

    pub(crate) async fn delete_timeline_import(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> DatabaseResult<()> {
        match &self.store {
            Store::Postgres(store) => store.delete_timeline_import(tenant_id, timeline_id).await,
            Store::Kubernetes(store) => store.delete_timeline_import(tenant_id, timeline_id).await,
        }
    }

    /// Idempotently update the status of one shard for an ongoing timeline import
    ///
    /// If the update was persisted to the database, then the current state of the
    /// import is returned to the caller. In case of logical errors a bespoke
    /// [`TimelineImportUpdateError`] instance is returned. Other database errors
    /// are covered by the outer [`DatabaseError`].
    pub(crate) async fn update_timeline_import(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        shard_status: ShardImportStatus,
    ) -> DatabaseResult<Result<Option<TimelineImport>, TimelineImportUpdateError>> {
        match &self.store {
            Store::Postgres(store) => {
                store
                    .update_timeline_import(tenant_shard_id, timeline_id, shard_status)
                    .await
            }
            Store::Kubernetes(store) => {
                store
                    .update_timeline_import(tenant_shard_id, timeline_id, shard_status)
                    .await
            }
        }
    }

    pub(crate) async fn is_tenant_importing_timeline(
        &self,
        tenant_id: TenantId,
    ) -> DatabaseResult<bool> {
        match &self.store {
            Store::Postgres(store) => store.is_tenant_importing_timeline(tenant_id).await,
            Store::Kubernetes(store) => store.is_tenant_importing_timeline(tenant_id).await,
        }
    }
}

/// Parts of [`crate::tenant_shard::TenantShard`] that are stored durably
#[derive(
    QueryableByName, Queryable, Selectable, Insertable, Serialize, Deserialize, Clone, Eq, PartialEq,
)]
#[diesel(table_name = crate::schema::tenant_shards)]
pub(crate) struct TenantShardPersistence {
    #[serde(default)]
    pub(crate) tenant_id: String,
    #[serde(default)]
    pub(crate) shard_number: i32,
    #[serde(default)]
    pub(crate) shard_count: i32,
    #[serde(default)]
    pub(crate) shard_stripe_size: i32,

    // Latest generation number: next time we attach, increment this
    // and use the incremented number when attaching.
    //
    // Generation is only None when first onboarding a tenant, where it may
    // be in PlacementPolicy::Secondary and therefore have no valid generation state.
    pub(crate) generation: Option<i32>,

    // Currently attached pageserver
    #[serde(rename = "pageserver")]
    pub(crate) generation_pageserver: Option<i64>,

    #[serde(default)]
    pub(crate) placement_policy: String,
    #[serde(default)]
    pub(crate) splitting: SplitState,
    #[serde(default)]
    pub(crate) config: String,
    #[serde(default)]
    pub(crate) scheduling_policy: String,

    // Hint that we should attempt to schedule this tenant shard the given
    // availability zone in order to minimise the chances of cross-AZ communication
    // with compute.
    pub(crate) preferred_az_id: Option<String>,
}

impl TenantShardPersistence {
    fn get_shard_count(&self) -> Result<ShardCount, ShardConfigError> {
        self.shard_count
            .try_into()
            .map(ShardCount)
            .map_err(|_| ShardConfigError::InvalidCount)
    }

    fn get_shard_number(&self) -> Result<ShardNumber, ShardConfigError> {
        self.shard_number
            .try_into()
            .map(ShardNumber)
            .map_err(|_| ShardConfigError::InvalidNumber)
    }

    fn get_stripe_size(&self) -> Result<ShardStripeSize, ShardConfigError> {
        self.shard_stripe_size
            .try_into()
            .map(ShardStripeSize)
            .map_err(|_| ShardConfigError::InvalidStripeSize)
    }

    pub(crate) fn get_shard_identity(&self) -> Result<ShardIdentity, ShardConfigError> {
        if self.shard_count == 0 {
            // NB: carry over the stripe size from the persisted record, to avoid consistency check
            // failures if the persisted value differs from the default stripe size. The stripe size
            // doesn't really matter for unsharded tenants anyway.
            Ok(ShardIdentity::unsharded_with_stripe_size(
                self.get_stripe_size()?,
            ))
        } else {
            Ok(ShardIdentity::new(
                self.get_shard_number()?,
                self.get_shard_count()?,
                self.get_stripe_size()?,
            )?)
        }
    }

    pub(crate) fn get_tenant_shard_id(&self) -> anyhow::Result<TenantShardId> {
        Ok(TenantShardId {
            tenant_id: TenantId::from_str(self.tenant_id.as_str())?,
            shard_number: self.get_shard_number()?,
            shard_count: self.get_shard_count()?,
        })
    }
}

/// Parts of [`crate::node::Node`] that are stored durably
#[derive(Serialize, Deserialize, Queryable, Selectable, Insertable, Eq, PartialEq)]
#[diesel(table_name = crate::schema::nodes)]
pub(crate) struct NodePersistence {
    pub(crate) node_id: i64,
    pub(crate) scheduling_policy: String,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: i32,
    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: i32,
    pub(crate) availability_zone_id: String,
    pub(crate) listen_https_port: Option<i32>,
    pub(crate) lifecycle: String,
    pub(crate) listen_grpc_addr: Option<String>,
    pub(crate) listen_grpc_port: Option<i32>,
}

/// Tenant metadata health status that are stored durably.
#[derive(Queryable, Selectable, Insertable, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[diesel(table_name = crate::schema::metadata_health)]
pub(crate) struct MetadataHealthPersistence {
    #[serde(default)]
    pub(crate) tenant_id: String,
    #[serde(default)]
    pub(crate) shard_number: i32,
    #[serde(default)]
    pub(crate) shard_count: i32,

    pub(crate) healthy: bool,
    pub(crate) last_scrubbed_at: chrono::DateTime<chrono::Utc>,
}

impl MetadataHealthPersistence {
    pub fn new(
        tenant_shard_id: TenantShardId,
        healthy: bool,
        last_scrubbed_at: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        let tenant_id = tenant_shard_id.tenant_id.to_string();
        let shard_number = tenant_shard_id.shard_number.0 as i32;
        let shard_count = tenant_shard_id.shard_count.literal() as i32;

        MetadataHealthPersistence {
            tenant_id,
            shard_number,
            shard_count,
            healthy,
            last_scrubbed_at,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_tenant_shard_id(&self) -> Result<TenantShardId, hex::FromHexError> {
        Ok(TenantShardId {
            tenant_id: TenantId::from_str(self.tenant_id.as_str())?,
            shard_number: ShardNumber(self.shard_number as u8),
            shard_count: ShardCount::new(self.shard_count as u8),
        })
    }
}

impl From<MetadataHealthPersistence> for MetadataHealthRecord {
    fn from(value: MetadataHealthPersistence) -> Self {
        MetadataHealthRecord {
            tenant_shard_id: value
                .get_tenant_shard_id()
                .expect("stored tenant id should be valid"),
            healthy: value.healthy,
            last_scrubbed_at: value.last_scrubbed_at,
        }
    }
}

#[derive(
    Serialize, Deserialize, Queryable, Selectable, Insertable, Eq, PartialEq, Debug, Clone,
)]
#[diesel(table_name = crate::schema::controllers)]
pub(crate) struct ControllerPersistence {
    pub(crate) address: String,
    pub(crate) started_at: chrono::DateTime<chrono::Utc>,
}

// What we store in the database
#[derive(Serialize, Deserialize, Queryable, Selectable, Eq, PartialEq, Debug, Clone)]
#[diesel(table_name = crate::schema::safekeepers)]
pub(crate) struct SafekeeperPersistence {
    pub(crate) id: i64,
    pub(crate) region_id: String,
    /// 1 is special, it means just created (not currently posted to storcon).
    /// Zero or negative is not really expected.
    /// Otherwise the number from `release-$(number_of_commits_on_branch)` tag.
    pub(crate) version: i64,
    pub(crate) host: String,
    pub(crate) port: i32,
    pub(crate) http_port: i32,
    pub(crate) availability_zone_id: String,
    pub(crate) scheduling_policy: SkSchedulingPolicyFromSql,
    pub(crate) https_port: Option<i32>,
}

/// Wrapper struct around [`SkSchedulingPolicy`] because both it and [`FromSql`] are from foreign crates,
/// and we don't want to make [`safekeeper_api`] depend on [`diesel`].
#[derive(Serialize, Deserialize, FromSqlRow, Eq, PartialEq, Debug, Copy, Clone)]
pub(crate) struct SkSchedulingPolicyFromSql(pub(crate) SkSchedulingPolicy);

impl From<SkSchedulingPolicy> for SkSchedulingPolicyFromSql {
    fn from(value: SkSchedulingPolicy) -> Self {
        SkSchedulingPolicyFromSql(value)
    }
}

impl FromSql<diesel::sql_types::VarChar, Pg> for SkSchedulingPolicyFromSql {
    fn from_sql(
        bytes: <Pg as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let bytes = bytes.as_bytes();
        match core::str::from_utf8(bytes) {
            Ok(s) => match SkSchedulingPolicy::from_str(s) {
                Ok(policy) => Ok(SkSchedulingPolicyFromSql(policy)),
                Err(e) => Err(format!("can't parse: {e}").into()),
            },
            Err(e) => Err(format!("invalid UTF-8 for scheduling policy: {e}").into()),
        }
    }
}

impl SafekeeperPersistence {
    pub(crate) fn from_upsert(
        upsert: SafekeeperUpsert,
        scheduling_policy: SkSchedulingPolicy,
    ) -> Self {
        crate::persistence::SafekeeperPersistence {
            id: upsert.id,
            region_id: upsert.region_id,
            version: upsert.version,
            host: upsert.host,
            port: upsert.port,
            http_port: upsert.http_port,
            https_port: upsert.https_port,
            availability_zone_id: upsert.availability_zone_id,
            scheduling_policy: SkSchedulingPolicyFromSql(scheduling_policy),
        }
    }
    pub(crate) fn as_describe_response(&self) -> Result<SafekeeperDescribeResponse, DatabaseError> {
        Ok(SafekeeperDescribeResponse {
            id: NodeId(self.id as u64),
            region_id: self.region_id.clone(),
            version: self.version,
            host: self.host.clone(),
            port: self.port,
            http_port: self.http_port,
            https_port: self.https_port,
            availability_zone_id: self.availability_zone_id.clone(),
            scheduling_policy: self.scheduling_policy.0,
        })
    }
}

/// What we expect from the upsert http api
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub(crate) struct SafekeeperUpsert {
    pub(crate) id: i64,
    pub(crate) region_id: String,
    /// 1 is special, it means just created (not currently posted to storcon).
    /// Zero or negative is not really expected.
    /// Otherwise the number from `release-$(number_of_commits_on_branch)` tag.
    pub(crate) version: i64,
    pub(crate) host: String,
    pub(crate) port: i32,
    /// The active flag will not be stored in the database and will be ignored.
    pub(crate) active: Option<bool>,
    pub(crate) http_port: i32,
    pub(crate) https_port: Option<i32>,
    pub(crate) availability_zone_id: String,
}

impl SafekeeperUpsert {
    fn as_insert_or_update(&self) -> anyhow::Result<InsertUpdateSafekeeper<'_>> {
        if self.version < 0 {
            anyhow::bail!("negative version: {}", self.version);
        }
        Ok(InsertUpdateSafekeeper {
            id: self.id,
            region_id: &self.region_id,
            version: self.version,
            host: &self.host,
            port: self.port,
            http_port: self.http_port,
            https_port: self.https_port,
            availability_zone_id: &self.availability_zone_id,
            // None means a wish to not update this column. We expose abilities to update it via other means.
            scheduling_policy: None,
        })
    }
}

#[derive(Insertable, AsChangeset)]
#[diesel(table_name = crate::schema::safekeepers)]
struct InsertUpdateSafekeeper<'a> {
    id: i64,
    region_id: &'a str,
    version: i64,
    host: &'a str,
    port: i32,
    http_port: i32,
    https_port: Option<i32>,
    availability_zone_id: &'a str,
    scheduling_policy: Option<&'a str>,
}

#[derive(Serialize, Deserialize, FromSqlRow, AsExpression, Eq, PartialEq, Debug, Copy, Clone)]
#[diesel(sql_type = crate::schema::sql_types::PgLsn)]
pub(crate) struct LsnWrapper(pub(crate) Lsn);

impl From<Lsn> for LsnWrapper {
    fn from(value: Lsn) -> Self {
        LsnWrapper(value)
    }
}

impl FromSql<crate::schema::sql_types::PgLsn, Pg> for LsnWrapper {
    fn from_sql(
        bytes: <Pg as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let byte_arr: diesel::deserialize::Result<[u8; 8]> = bytes
            .as_bytes()
            .try_into()
            .map_err(|_| "Can't obtain lsn from sql".into());
        Ok(LsnWrapper(Lsn(u64::from_be_bytes(byte_arr?))))
    }
}

impl ToSql<crate::schema::sql_types::PgLsn, Pg> for LsnWrapper {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, Pg>,
    ) -> diesel::serialize::Result {
        out.write_all(&u64::to_be_bytes(self.0.0))
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

#[derive(Insertable, AsChangeset, Clone)]
#[diesel(table_name = crate::schema::timelines)]
pub(crate) struct TimelinePersistence {
    pub(crate) tenant_id: String,
    pub(crate) timeline_id: String,
    pub(crate) start_lsn: LsnWrapper,
    pub(crate) generation: i32,
    pub(crate) sk_set: Vec<i64>,
    pub(crate) new_sk_set: Option<Vec<i64>>,
    pub(crate) cplane_notified_generation: i32,
    pub(crate) deleted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) sk_set_notified_generation: i32,
}

/// This is separate from [TimelinePersistence] only because postgres allows NULLs
/// in arrays and there is no way to forbid that at schema level. Hence diesel
/// wants `sk_set` to be `Vec<Option<i64>>` instead of `Vec<i64>` for
/// Queryable/Selectable. It does however allow insertions without redundant
/// Option(s), so [TimelinePersistence] doesn't have them.
#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::timelines)]
pub(crate) struct TimelineFromDb {
    pub(crate) tenant_id: String,
    pub(crate) timeline_id: String,
    pub(crate) start_lsn: LsnWrapper,
    pub(crate) generation: i32,
    pub(crate) sk_set: Vec<Option<i64>>,
    pub(crate) new_sk_set: Option<Vec<Option<i64>>>,
    pub(crate) cplane_notified_generation: i32,
    pub(crate) deleted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) sk_set_notified_generation: i32,
}

impl TimelineFromDb {
    fn into_persistence(self) -> TimelinePersistence {
        // We should never encounter null entries in the sets, but we need to filter them out.
        // There is no way to forbid this in the schema that diesel recognizes (to our knowledge).
        let sk_set = self.sk_set.into_iter().flatten().collect::<Vec<_>>();
        let new_sk_set = self
            .new_sk_set
            .map(|s| s.into_iter().flatten().collect::<Vec<_>>());
        TimelinePersistence {
            tenant_id: self.tenant_id,
            timeline_id: self.timeline_id,
            start_lsn: self.start_lsn,
            generation: self.generation,
            sk_set,
            new_sk_set,
            cplane_notified_generation: self.cplane_notified_generation,
            deleted_at: self.deleted_at,
            sk_set_notified_generation: self.sk_set_notified_generation,
        }
    }
}

// This is separate from TimelinePersistence because we don't want to touch generation and deleted_at values for the update.
#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::timelines)]
#[diesel(treat_none_as_null = true)]
pub(crate) struct TimelineUpdate {
    pub(crate) tenant_id: String,
    pub(crate) timeline_id: String,
    pub(crate) start_lsn: LsnWrapper,
    pub(crate) sk_set: Vec<i64>,
    pub(crate) new_sk_set: Option<Vec<i64>>,
}

#[derive(Insertable, AsChangeset, Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::safekeeper_timeline_pending_ops)]
pub(crate) struct TimelinePendingOpPersistence {
    pub(crate) sk_id: i64,
    pub(crate) tenant_id: String,
    pub(crate) timeline_id: String,
    pub(crate) generation: i32,
    pub(crate) op_kind: SafekeeperTimelineOpKind,
}

#[derive(Serialize, Deserialize, FromSqlRow, AsExpression, Eq, PartialEq, Debug, Copy, Clone)]
#[diesel(sql_type = diesel::sql_types::VarChar)]
pub(crate) enum SafekeeperTimelineOpKind {
    Pull,
    Exclude,
    Delete,
}

impl FromSql<diesel::sql_types::VarChar, Pg> for SafekeeperTimelineOpKind {
    fn from_sql(
        bytes: <Pg as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let bytes = bytes.as_bytes();
        match core::str::from_utf8(bytes) {
            Ok(s) => match s {
                "pull" => Ok(SafekeeperTimelineOpKind::Pull),
                "exclude" => Ok(SafekeeperTimelineOpKind::Exclude),
                "delete" => Ok(SafekeeperTimelineOpKind::Delete),
                _ => Err(format!("can't parse: {s}").into()),
            },
            Err(e) => Err(format!("invalid UTF-8 for op_kind: {e}").into()),
        }
    }
}

impl ToSql<diesel::sql_types::VarChar, Pg> for SafekeeperTimelineOpKind {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, Pg>,
    ) -> diesel::serialize::Result {
        let kind_str = match self {
            SafekeeperTimelineOpKind::Pull => "pull",
            SafekeeperTimelineOpKind::Exclude => "exclude",
            SafekeeperTimelineOpKind::Delete => "delete",
        };
        out.write_all(kind_str.as_bytes())
            .map(|_| IsNull::No)
            .map_err(Into::into)
    }
}

#[derive(Serialize, Deserialize, Queryable, Selectable, Insertable, Eq, PartialEq, Clone)]
#[diesel(table_name = crate::schema::timeline_imports)]
pub(crate) struct TimelineImportPersistence {
    pub(crate) tenant_id: String,
    pub(crate) timeline_id: String,
    pub(crate) shard_statuses: serde_json::Value,
}
