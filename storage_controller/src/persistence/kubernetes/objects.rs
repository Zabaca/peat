//! The record kinds of `storage_controller/kubernetes/crds.yaml`, and their conversions to and
//! from the rows the rest of the controller uses. `storage_controller/kubernetes/README.md` maps
//! every field to Neon's column.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::str::FromStr;

use k8s_openapi::NamespaceResourceScope;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Resource;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use utils::lsn::Lsn;

use crate::persistence::split_state::SplitState;
use crate::persistence::{
    ControllerPersistence, DatabaseError, DatabaseResult, LsnWrapper, MetadataHealthPersistence,
    NodePersistence, SafekeeperPersistence, SafekeeperTimelineOpKind, SkSchedulingPolicyFromSql,
    TenantShardPersistence, TimelineImportPersistence, TimelinePendingOpPersistence,
    TimelinePersistence,
};

pub(crate) const GROUP: &str = "storagecontroller.peat.zabaca.com";
pub(crate) const VERSION: &str = "v1alpha1";

/// The label on a Timeline naming its tenant.
pub(crate) const TENANT_LABEL: &str = "storagecontroller.peat.zabaca.com/tenant";
/// The prefix of the label on a Tenant naming a pageserver one of its shards' generations was
/// issued to: `generation.storagecontroller.peat.zabaca.com/<node id>`.
pub(crate) const GENERATION_LABEL_PREFIX: &str = "generation.storagecontroller.peat.zabaca.com/";

/// What every record kind's `spec` provides.
pub(crate) trait Spec:
    Serialize + DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static
{
    const KIND: &'static str;
    const PLURAL: &'static str;

    /// The labels the controller keeps on the object, recomputed from the spec on every write.
    fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    /// Whether a label key is one this kind's [`Spec::labels`] owns (and so is rewritten).
    fn owns_label(_key: &str) -> bool {
        false
    }
}

/// One record object: a custom resource of one of the six kinds.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Object<S> {
    #[serde(default)]
    pub(crate) api_version: String,
    #[serde(default)]
    pub(crate) kind: String,
    pub(crate) metadata: ObjectMeta,
    pub(crate) spec: S,
}

impl<S: Spec> Object<S> {
    pub(crate) fn new(name: &str, spec: S) -> Self {
        let labels = spec.labels();
        Object {
            api_version: format!("{GROUP}/{VERSION}"),
            kind: S::KIND.to_string(),
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: (!labels.is_empty()).then_some(labels),
                ..Default::default()
            },
            spec,
        }
    }

    /// The same object holding `spec`, with the labels the controller keeps recomputed and any
    /// other label left as it was.
    pub(crate) fn with_spec(&self, spec: S) -> Self {
        let mut labels: BTreeMap<String, String> = self
            .metadata
            .labels
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter(|(k, _)| !S::owns_label(k))
            .collect();
        labels.extend(spec.labels());
        let mut metadata = self.metadata.clone();
        metadata.labels = (!labels.is_empty()).then_some(labels);
        Object {
            api_version: format!("{GROUP}/{VERSION}"),
            kind: S::KIND.to_string(),
            metadata,
            spec,
        }
    }
}

impl<S: Spec> Resource for Object<S> {
    type DynamicType = ();
    type Scope = NamespaceResourceScope;

    fn kind(_: &()) -> Cow<'_, str> {
        Cow::Borrowed(S::KIND)
    }
    fn group(_: &()) -> Cow<'_, str> {
        Cow::Borrowed(GROUP)
    }
    fn version(_: &()) -> Cow<'_, str> {
        Cow::Borrowed(VERSION)
    }
    fn plural(_: &()) -> Cow<'_, str> {
        Cow::Borrowed(S::PLURAL)
    }
    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }
    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// Times are kept as Postgres keeps them: UTC, rounded to the microsecond.
mod micros {
    use chrono::{DateTime, SecondsFormat, SubsecRound, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn round(t: DateTime<Utc>) -> DateTime<Utc> {
        t.round_subsecs(6)
    }

    pub(crate) fn serialize<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&round(*t).to_rfc3339_opts(SecondsFormat::Micros, true))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|t| t.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }

    pub(crate) mod option {
        use chrono::{DateTime, Utc};
        use serde::{Deserialize, Deserializer, Serializer};

        pub(crate) fn serialize<S: Serializer>(
            t: &Option<DateTime<Utc>>,
            s: S,
        ) -> Result<S::Ok, S::Error> {
            match t {
                Some(t) => super::serialize(t, s),
                None => s.serialize_none(),
            }
        }

        pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
            d: D,
        ) -> Result<Option<DateTime<Utc>>, D::Error> {
            #[derive(Deserialize)]
            struct Wrap(#[serde(with = "super")] DateTime<Utc>);
            Ok(Option::<Wrap>::deserialize(d)?.map(|Wrap(t)| t))
        }
    }
}

pub(crate) use micros::round as round_to_micros;

// ---------------------------------------------------------------------------------------------
// Tenant

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TenantSpec {
    pub(crate) tenant_id: String,
    pub(crate) shards: Vec<ShardRecord>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShardRecord {
    pub(crate) shard_number: i32,
    pub(crate) shard_count: i32,
    pub(crate) shard_stripe_size: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) generation: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) generation_pageserver: Option<i64>,
    pub(crate) placement_policy: String,
    pub(crate) splitting: SplitState,
    pub(crate) config: String,
    pub(crate) scheduling_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) preferred_az_id: Option<String>,
}

impl Spec for TenantSpec {
    const KIND: &'static str = "Tenant";
    const PLURAL: &'static str = "tenants";

    fn labels(&self) -> BTreeMap<String, String> {
        self.shards
            .iter()
            .filter_map(|s| s.generation_pageserver)
            .map(|node| (generation_label(node), "true".to_string()))
            .collect()
    }

    fn owns_label(key: &str) -> bool {
        key.starts_with(GENERATION_LABEL_PREFIX)
    }
}

pub(crate) fn generation_label(node_id: i64) -> String {
    format!("{GENERATION_LABEL_PREFIX}{node_id}")
}

impl ShardRecord {
    pub(crate) fn from_row(row: &TenantShardPersistence) -> Self {
        ShardRecord {
            shard_number: row.shard_number,
            shard_count: row.shard_count,
            shard_stripe_size: row.shard_stripe_size,
            generation: row.generation,
            generation_pageserver: row.generation_pageserver,
            placement_policy: row.placement_policy.clone(),
            splitting: row.splitting,
            config: row.config.clone(),
            scheduling_policy: row.scheduling_policy.clone(),
            preferred_az_id: row.preferred_az_id.clone(),
        }
    }

    pub(crate) fn to_row(&self, tenant_id: &str) -> TenantShardPersistence {
        TenantShardPersistence {
            tenant_id: tenant_id.to_string(),
            shard_number: self.shard_number,
            shard_count: self.shard_count,
            shard_stripe_size: self.shard_stripe_size,
            generation: self.generation,
            generation_pageserver: self.generation_pageserver,
            placement_policy: self.placement_policy.clone(),
            splitting: self.splitting,
            config: self.config.clone(),
            scheduling_policy: self.scheduling_policy.clone(),
            preferred_az_id: self.preferred_az_id.clone(),
        }
    }

    /// The shard's key in Neon's table, beside its tenant: (shard number, shard count).
    pub(crate) fn key(&self) -> (i32, i32) {
        (self.shard_number, self.shard_count)
    }
}

impl TenantSpec {
    pub(crate) fn rows(&self) -> impl Iterator<Item = TenantShardPersistence> + '_ {
        self.shards.iter().map(|s| s.to_row(&self.tenant_id))
    }
}

// ---------------------------------------------------------------------------------------------
// MetadataHealth

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MetadataHealthSpec {
    pub(crate) tenant_id: String,
    pub(crate) shards: Vec<HealthRecord>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HealthRecord {
    pub(crate) shard_number: i32,
    pub(crate) shard_count: i32,
    pub(crate) healthy: bool,
    #[serde(with = "micros")]
    pub(crate) last_scrubbed_at: chrono::DateTime<chrono::Utc>,
}

impl Spec for MetadataHealthSpec {
    const KIND: &'static str = "MetadataHealth";
    const PLURAL: &'static str = "metadatahealths";
}

impl HealthRecord {
    pub(crate) fn from_row(row: &MetadataHealthPersistence) -> Self {
        HealthRecord {
            shard_number: row.shard_number,
            shard_count: row.shard_count,
            healthy: row.healthy,
            last_scrubbed_at: round_to_micros(row.last_scrubbed_at),
        }
    }

    pub(crate) fn to_row(&self, tenant_id: &str) -> MetadataHealthPersistence {
        MetadataHealthPersistence {
            tenant_id: tenant_id.to_string(),
            shard_number: self.shard_number,
            shard_count: self.shard_count,
            healthy: self.healthy,
            last_scrubbed_at: self.last_scrubbed_at,
        }
    }

    pub(crate) fn key(&self) -> (i32, i32) {
        (self.shard_number, self.shard_count)
    }
}

// ---------------------------------------------------------------------------------------------
// Timeline

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TimelineSpec {
    pub(crate) tenant_id: String,
    /// Empty for the object holding a tenant's tenant-wide pending operations.
    pub(crate) timeline_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) membership: Option<Membership>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) pending_ops: Vec<PendingOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) import: Option<ImportRecord>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Membership {
    pub(crate) start_lsn: String,
    pub(crate) generation: i32,
    pub(crate) sk_set: Vec<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) new_sk_set: Option<Vec<i64>>,
    pub(crate) cplane_notified_generation: i32,
    pub(crate) sk_set_notified_generation: i32,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "micros::option"
    )]
    pub(crate) deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PendingOp {
    pub(crate) safekeeper_id: i64,
    pub(crate) generation: i32,
    pub(crate) kind: OpKind,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OpKind {
    Pull,
    Exclude,
    Delete,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportRecord {
    /// The JSON text of Neon's `shard_statuses` column.
    pub(crate) shard_statuses: String,
}

impl Spec for TimelineSpec {
    const KIND: &'static str = "Timeline";
    const PLURAL: &'static str = "timelines";

    fn labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(TENANT_LABEL.to_string(), self.tenant_id.clone())])
    }

    fn owns_label(key: &str) -> bool {
        key == TENANT_LABEL
    }
}

/// The name of the Timeline object of a timeline, or of a tenant's tenant-wide operations.
pub(crate) fn timeline_object_name(tenant_id: &str, timeline_id: &str) -> String {
    if timeline_id.is_empty() {
        tenant_id.to_string()
    } else {
        format!("{tenant_id}-{timeline_id}")
    }
}

impl TimelineSpec {
    pub(crate) fn empty(tenant_id: &str, timeline_id: &str) -> Self {
        TimelineSpec {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            membership: None,
            pending_ops: Vec::new(),
            import: None,
        }
    }

    /// An object none of whose three parts exists is deleted.
    pub(crate) fn is_empty(&self) -> bool {
        self.membership.is_none() && self.pending_ops.is_empty() && self.import.is_none()
    }

    pub(crate) fn timeline_row(&self) -> DatabaseResult<Option<TimelinePersistence>> {
        self.membership
            .as_ref()
            .map(|m| m.to_row(&self.tenant_id, &self.timeline_id))
            .transpose()
    }

    pub(crate) fn pending_op_rows(
        &self,
    ) -> impl Iterator<Item = TimelinePendingOpPersistence> + '_ {
        self.pending_ops
            .iter()
            .map(|op| TimelinePendingOpPersistence {
                sk_id: op.safekeeper_id,
                tenant_id: self.tenant_id.clone(),
                timeline_id: self.timeline_id.clone(),
                generation: op.generation,
                op_kind: op.kind.into(),
            })
    }

    pub(crate) fn import_row(&self) -> DatabaseResult<Option<TimelineImportPersistence>> {
        let Some(import) = &self.import else {
            return Ok(None);
        };
        let shard_statuses = serde_json::from_str(&import.shard_statuses).map_err(|e| {
            DatabaseError::Logical(format!(
                "timeline {}/{}: shard statuses are not JSON: {e}",
                self.tenant_id, self.timeline_id
            ))
        })?;
        Ok(Some(TimelineImportPersistence {
            tenant_id: self.tenant_id.clone(),
            timeline_id: self.timeline_id.clone(),
            shard_statuses,
        }))
    }
}

impl Membership {
    pub(crate) fn from_row(row: &TimelinePersistence) -> Self {
        Membership {
            start_lsn: row.start_lsn.0.to_string(),
            generation: row.generation,
            sk_set: row.sk_set.clone(),
            new_sk_set: row.new_sk_set.clone(),
            cplane_notified_generation: row.cplane_notified_generation,
            sk_set_notified_generation: row.sk_set_notified_generation,
            deleted_at: row.deleted_at.map(round_to_micros),
        }
    }

    fn to_row(&self, tenant_id: &str, timeline_id: &str) -> DatabaseResult<TimelinePersistence> {
        let start_lsn = Lsn::from_str(&self.start_lsn).map_err(|e| {
            DatabaseError::Logical(format!(
                "timeline {tenant_id}/{timeline_id}: start LSN {:?}: {e}",
                self.start_lsn
            ))
        })?;
        Ok(TimelinePersistence {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            start_lsn: LsnWrapper(start_lsn),
            generation: self.generation,
            sk_set: self.sk_set.clone(),
            new_sk_set: self.new_sk_set.clone(),
            cplane_notified_generation: self.cplane_notified_generation,
            deleted_at: self.deleted_at,
            sk_set_notified_generation: self.sk_set_notified_generation,
        })
    }
}

impl From<OpKind> for SafekeeperTimelineOpKind {
    fn from(k: OpKind) -> Self {
        match k {
            OpKind::Pull => SafekeeperTimelineOpKind::Pull,
            OpKind::Exclude => SafekeeperTimelineOpKind::Exclude,
            OpKind::Delete => SafekeeperTimelineOpKind::Delete,
        }
    }
}

impl From<SafekeeperTimelineOpKind> for OpKind {
    fn from(k: SafekeeperTimelineOpKind) -> Self {
        match k {
            SafekeeperTimelineOpKind::Pull => OpKind::Pull,
            SafekeeperTimelineOpKind::Exclude => OpKind::Exclude,
            SafekeeperTimelineOpKind::Delete => OpKind::Delete,
        }
    }
}

impl PendingOp {
    pub(crate) fn from_row(row: &TimelinePendingOpPersistence) -> Self {
        PendingOp {
            safekeeper_id: row.sk_id,
            generation: row.generation,
            kind: row.op_kind.into(),
        }
    }
}

impl ImportRecord {
    pub(crate) fn from_row(row: &TimelineImportPersistence) -> Self {
        ImportRecord {
            shard_statuses: row.shard_statuses.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Pageserver

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PageserverSpec {
    pub(crate) node_id: i64,
    pub(crate) scheduling_policy: String,
    pub(crate) lifecycle: String,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) listen_https_port: Option<i32>,
    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) listen_grpc_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) listen_grpc_port: Option<i32>,
    pub(crate) availability_zone_id: String,
}

impl Spec for PageserverSpec {
    const KIND: &'static str = "Pageserver";
    const PLURAL: &'static str = "pageservers";
}

pub(crate) fn pageserver_object_name(node_id: i64) -> String {
    format!("pageserver-{node_id}")
}

impl PageserverSpec {
    pub(crate) fn from_row(row: &NodePersistence) -> Self {
        PageserverSpec {
            node_id: row.node_id,
            scheduling_policy: row.scheduling_policy.clone(),
            lifecycle: row.lifecycle.clone(),
            listen_http_addr: row.listen_http_addr.clone(),
            listen_http_port: row.listen_http_port,
            listen_https_port: row.listen_https_port,
            listen_pg_addr: row.listen_pg_addr.clone(),
            listen_pg_port: row.listen_pg_port,
            listen_grpc_addr: row.listen_grpc_addr.clone(),
            listen_grpc_port: row.listen_grpc_port,
            availability_zone_id: row.availability_zone_id.clone(),
        }
    }

    pub(crate) fn to_row(&self) -> NodePersistence {
        NodePersistence {
            node_id: self.node_id,
            scheduling_policy: self.scheduling_policy.clone(),
            listen_http_addr: self.listen_http_addr.clone(),
            listen_http_port: self.listen_http_port,
            listen_pg_addr: self.listen_pg_addr.clone(),
            listen_pg_port: self.listen_pg_port,
            availability_zone_id: self.availability_zone_id.clone(),
            listen_https_port: self.listen_https_port,
            lifecycle: self.lifecycle.clone(),
            listen_grpc_addr: self.listen_grpc_addr.clone(),
            listen_grpc_port: self.listen_grpc_port,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Safekeeper

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SafekeeperSpec {
    pub(crate) id: i64,
    pub(crate) region_id: String,
    pub(crate) version: i64,
    pub(crate) host: String,
    pub(crate) port: i32,
    pub(crate) http_port: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) https_port: Option<i32>,
    pub(crate) availability_zone_id: String,
    pub(crate) scheduling_policy: String,
}

impl Spec for SafekeeperSpec {
    const KIND: &'static str = "Safekeeper";
    const PLURAL: &'static str = "safekeepers";
}

pub(crate) fn safekeeper_object_name(id: i64) -> String {
    format!("safekeeper-{id}")
}

impl SafekeeperSpec {
    pub(crate) fn from_row(row: &SafekeeperPersistence) -> Self {
        SafekeeperSpec {
            id: row.id,
            region_id: row.region_id.clone(),
            version: row.version,
            host: row.host.clone(),
            port: row.port,
            http_port: row.http_port,
            https_port: row.https_port,
            availability_zone_id: row.availability_zone_id.clone(),
            scheduling_policy: String::from(row.scheduling_policy.0),
        }
    }

    pub(crate) fn to_row(&self) -> DatabaseResult<SafekeeperPersistence> {
        let policy =
            pageserver_api::controller_api::SkSchedulingPolicy::from_str(&self.scheduling_policy)
                .map_err(|e| {
                DatabaseError::Logical(format!(
                    "safekeeper {}: scheduling policy {:?}: {e}",
                    self.id, self.scheduling_policy
                ))
            })?;
        Ok(SafekeeperPersistence {
            id: self.id,
            region_id: self.region_id.clone(),
            version: self.version,
            host: self.host.clone(),
            port: self.port,
            http_port: self.http_port,
            availability_zone_id: self.availability_zone_id.clone(),
            scheduling_policy: SkSchedulingPolicyFromSql(policy),
            https_port: self.https_port,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Leader

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LeaderSpec {
    pub(crate) address: String,
    #[serde(with = "micros")]
    pub(crate) started_at: chrono::DateTime<chrono::Utc>,
}

impl Spec for LeaderSpec {
    const KIND: &'static str = "Leader";
    const PLURAL: &'static str = "leaders";
}

pub(crate) const LEADER_OBJECT_NAME: &str = "leader";

impl LeaderSpec {
    pub(crate) fn from_row(row: &ControllerPersistence) -> Self {
        LeaderSpec {
            address: row.address.clone(),
            started_at: round_to_micros(row.started_at),
        }
    }

    pub(crate) fn to_row(&self) -> ControllerPersistence {
        ControllerPersistence {
            address: self.address.clone(),
            started_at: self.started_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_round_to_microseconds_as_postgres_keeps_them() {
        let t = chrono::DateTime::parse_from_rfc3339("2026-11-26T12:00:00.1234567Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let spec = LeaderSpec {
            address: "http://controller.example.com:1234".to_string(),
            started_at: t,
        };
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["startedAt"], "2026-11-26T12:00:00.123457Z");
        let back: LeaderSpec = serde_json::from_value(json).unwrap();
        assert_eq!(back.started_at, round_to_micros(t));
    }

    #[test]
    fn a_shard_reads_back_as_the_row_it_was_written_from() {
        let row = TenantShardPersistence {
            tenant_id: "1f359dd625e519a1a4e8d7509690f6fc".to_string(),
            shard_number: 1,
            shard_count: 4,
            shard_stripe_size: 2048,
            generation: Some(7),
            generation_pageserver: Some(3),
            placement_policy: r#"{"Attached":1}"#.to_string(),
            splitting: SplitState::Splitting,
            config: "{}".to_string(),
            scheduling_policy: r#""Active""#.to_string(),
            preferred_az_id: None,
        };
        let shard = ShardRecord::from_row(&row);
        let json = serde_json::to_value(&shard).unwrap();
        assert_eq!(json["splitting"], "Splitting");
        assert!(json.get("preferredAzId").is_none());
        let back: ShardRecord = serde_json::from_value(json).unwrap();
        assert!(back.to_row(&row.tenant_id) == row);
    }

    #[test]
    fn a_tenant_is_labelled_with_each_pageserver_holding_a_generation() {
        let shard = |n: i32, node: Option<i64>| ShardRecord {
            shard_number: n,
            shard_count: 2,
            shard_stripe_size: 2048,
            generation: Some(1),
            generation_pageserver: node,
            placement_policy: r#"{"Attached":1}"#.to_string(),
            splitting: SplitState::Idle,
            config: "{}".to_string(),
            scheduling_policy: r#""Active""#.to_string(),
            preferred_az_id: None,
        };
        let spec = TenantSpec {
            tenant_id: "1f359dd625e519a1a4e8d7509690f6fc".to_string(),
            shards: vec![shard(0, Some(5)), shard(1, None)],
        };
        let mut obj = Object::new(&spec.tenant_id.clone(), spec.clone());
        assert_eq!(
            obj.metadata
                .labels
                .clone()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec!["generation.storagecontroller.peat.zabaca.com/5"]
        );
        obj.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("someone.example.com/else".to_string(), "x".to_string());
        let moved = TenantSpec {
            shards: vec![shard(0, Some(6)), shard(1, Some(6))],
            ..spec
        };
        let obj = obj.with_spec(moved);
        assert_eq!(
            obj.metadata.labels.unwrap().keys().collect::<Vec<_>>(),
            vec![
                "generation.storagecontroller.peat.zabaca.com/6",
                "someone.example.com/else"
            ]
        );
    }
}
