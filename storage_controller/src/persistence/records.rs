//! Every record of the controller as one document: the copy in the bucket, what the copy tool
//! dumps, and what it compares. The same shape from either store, in the record kinds' own
//! fields (`storage_controller/kubernetes/README.md`), each list in one order, so two stores
//! holding the same records give equal documents.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::kubernetes::objects::{
    HealthRecord, ImportRecord, LeaderSpec, Membership, MetadataHealthSpec, PageserverSpec,
    PendingOp, SafekeeperSpec, ShardRecord, TenantSpec, TimelineSpec, pageserver_object_name,
    safekeeper_object_name, timeline_object_name,
};
use super::{
    ControllerPersistence, DatabaseError, DatabaseResult, MetadataHealthPersistence,
    NodePersistence, SafekeeperPersistence, TenantShardPersistence, TimelineImportPersistence,
    TimelinePendingOpPersistence, TimelinePersistence,
};

/// The version of the document's shape.
pub(crate) const FORMAT: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Records {
    pub(crate) format: u32,
    /// Each map is keyed by the object's name in the Kubernetes store.
    pub(crate) tenants: BTreeMap<String, TenantSpec>,
    pub(crate) metadata_health: BTreeMap<String, MetadataHealthSpec>,
    pub(crate) timelines: BTreeMap<String, TimelineSpec>,
    pub(crate) pageservers: BTreeMap<String, PageserverSpec>,
    pub(crate) safekeepers: BTreeMap<String, SafekeeperSpec>,
    pub(crate) leader: Option<LeaderSpec>,
}

/// Every row of the Postgres store, read in one transaction.
pub(crate) struct Rows {
    pub(crate) tenant_shards: Vec<TenantShardPersistence>,
    pub(crate) metadata_health: Vec<MetadataHealthPersistence>,
    pub(crate) timelines: Vec<TimelinePersistence>,
    pub(crate) pending_ops: Vec<TimelinePendingOpPersistence>,
    pub(crate) imports: Vec<TimelineImportPersistence>,
    pub(crate) nodes: Vec<NodePersistence>,
    pub(crate) safekeepers: Vec<SafekeeperPersistence>,
    pub(crate) controllers: Vec<ControllerPersistence>,
}

impl Records {
    pub(crate) fn empty() -> Self {
        Records {
            format: FORMAT,
            ..Default::default()
        }
    }

    /// How many objects of each kind, for a person and for the copy tool's report.
    pub(crate) fn counts(&self) -> BTreeMap<&'static str, usize> {
        BTreeMap::from([
            ("tenants", self.tenants.len()),
            (
                "shards",
                self.tenants.values().map(|t| t.shards.len()).sum::<usize>(),
            ),
            ("metadataHealth", self.metadata_health.len()),
            ("timelines", self.timelines.len()),
            (
                "pendingOps",
                self.timelines
                    .values()
                    .map(|t| t.pending_ops.len())
                    .sum::<usize>(),
            ),
            ("pageservers", self.pageservers.len()),
            ("safekeepers", self.safekeepers.len()),
            ("leader", usize::from(self.leader.is_some())),
        ])
    }

    /// Whether the store holds nothing a controller could have issued: no tenant, pageserver or
    /// safekeeper.
    pub(crate) fn is_empty(&self) -> bool {
        self.tenants.is_empty() && self.pageservers.is_empty() && self.safekeepers.is_empty()
    }

    /// Drop the health entries whose shard is gone (a removal whose second write has not
    /// landed): they are not records, and Postgres, which removes them with their shard, would
    /// refuse them.
    pub(crate) fn drop_orphaned_health(&mut self) {
        let tenants = &self.tenants;
        for (name, h) in self.metadata_health.iter_mut() {
            let keys: Vec<(i32, i32)> = tenants
                .get(name)
                .map(|t| t.shards.iter().map(|s| s.key()).collect())
                .unwrap_or_default();
            h.shards.retain(|s| keys.contains(&s.key()));
        }
        self.metadata_health.retain(|_, h| !h.shards.is_empty());
    }

    /// Put every list in one order (the order a store returns rows or shards in means nothing).
    pub(crate) fn sort(&mut self) {
        for t in self.tenants.values_mut() {
            t.shards.sort_by_key(|s| (s.shard_count, s.shard_number));
        }
        for h in self.metadata_health.values_mut() {
            h.shards.sort_by_key(|s| (s.shard_count, s.shard_number));
        }
        for t in self.timelines.values_mut() {
            t.pending_ops.sort_by_key(|o| o.safekeeper_id);
        }
    }

    /// The same records with every list in one order and each JSON text in one form (Postgres's
    /// jsonb, for one, gives an import's keys in an order of its own), for comparing. The stores
    /// keep the text exactly as written; only comparisons read it this way.
    pub(crate) fn normalized(&self) -> DatabaseResult<Self> {
        let mut r = self.clone();
        r.sort();
        for t in r.tenants.values_mut() {
            for s in &mut t.shards {
                s.config = normal_json(&s.config)?;
                s.placement_policy = normal_json(&s.placement_policy)?;
                s.scheduling_policy = normal_json(&s.scheduling_policy)?;
            }
        }
        for t in r.timelines.values_mut() {
            if let Some(i) = &mut t.import {
                i.shard_statuses = normal_json(&i.shard_statuses)?;
            }
        }
        Ok(r)
    }

    pub(crate) fn from_rows(rows: Rows) -> DatabaseResult<Self> {
        let mut r = Records::empty();
        for row in rows.tenant_shards {
            r.tenants
                .entry(row.tenant_id.clone())
                .or_insert_with(|| TenantSpec {
                    tenant_id: row.tenant_id.clone(),
                    shards: Vec::new(),
                })
                .shards
                .push(ShardRecord::from_row(&row));
        }
        for row in rows.metadata_health {
            r.metadata_health
                .entry(row.tenant_id.clone())
                .or_insert_with(|| MetadataHealthSpec {
                    tenant_id: row.tenant_id.clone(),
                    shards: Vec::new(),
                })
                .shards
                .push(HealthRecord::from_row(&row));
        }
        let timeline = |r: &mut Records, tenant: &str, tl: &str| -> String {
            let name = timeline_object_name(tenant, tl);
            r.timelines
                .entry(name.clone())
                .or_insert_with(|| TimelineSpec::empty(tenant, tl));
            name
        };
        for row in rows.timelines {
            let name = timeline(&mut r, &row.tenant_id, &row.timeline_id);
            r.timelines.get_mut(&name).unwrap().membership = Some(Membership::from_row(&row));
        }
        for row in rows.pending_ops {
            let name = timeline(&mut r, &row.tenant_id, &row.timeline_id);
            r.timelines
                .get_mut(&name)
                .unwrap()
                .pending_ops
                .push(PendingOp::from_row(&row));
        }
        for row in rows.imports {
            let name = timeline(&mut r, &row.tenant_id, &row.timeline_id);
            r.timelines.get_mut(&name).unwrap().import = Some(ImportRecord::from_row(&row));
        }
        for row in rows.nodes {
            r.pageservers.insert(
                pageserver_object_name(row.node_id),
                PageserverSpec::from_row(&row),
            );
        }
        for row in rows.safekeepers {
            r.safekeepers.insert(
                safekeeper_object_name(row.id),
                SafekeeperSpec::from_row(&row),
            );
        }
        if rows.controllers.len() > 1 {
            return Err(DatabaseError::Logical(format!(
                "More than one entry present in the leader table: {:?}",
                rows.controllers
            )));
        }
        r.leader = rows.controllers.first().map(LeaderSpec::from_row);
        r.sort();
        Ok(r)
    }

    pub(crate) fn to_rows(&self) -> DatabaseResult<Rows> {
        let mut rows = Rows {
            tenant_shards: Vec::new(),
            metadata_health: Vec::new(),
            timelines: Vec::new(),
            pending_ops: Vec::new(),
            imports: Vec::new(),
            nodes: Vec::new(),
            safekeepers: Vec::new(),
            controllers: Vec::new(),
        };
        for t in self.tenants.values() {
            rows.tenant_shards.extend(t.rows());
        }
        for h in self.metadata_health.values() {
            rows.metadata_health
                .extend(h.shards.iter().map(|s| s.to_row(&h.tenant_id)));
        }
        for t in self.timelines.values() {
            rows.timelines.extend(t.timeline_row()?);
            rows.pending_ops.extend(t.pending_op_rows());
            rows.imports.extend(t.import_row()?);
        }
        rows.nodes
            .extend(self.pageservers.values().map(|p| p.to_row()));
        for s in self.safekeepers.values() {
            rows.safekeepers.push(s.to_row()?);
        }
        rows.controllers
            .extend(self.leader.as_ref().map(|l| l.to_row()));
        Ok(rows)
    }

    /// Every difference between two sets of records, one line each, field by field (empty when
    /// they are equal), each read as [`Records::normalized`] reads it.
    pub(crate) fn diff(&self, other: &Records) -> DatabaseResult<Vec<String>> {
        let a = serde_json::to_value(self.normalized()?).expect("records serialize");
        let b = serde_json::to_value(other.normalized()?).expect("records serialize");
        let mut out = Vec::new();
        diff_values("", &a, &b, &mut out);
        Ok(out)
    }
}

fn normal_json(text: &str) -> DatabaseResult<String> {
    let v: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| DatabaseError::Logical(format!("not JSON: {text:?}: {e}")))?;
    Ok(sorted(v).to_string())
}

/// The same value with every object's keys in order, whatever map serde_json was built with.
fn sorted(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            let ordered: BTreeMap<String, serde_json::Value> =
                m.into_iter().map(|(k, v)| (k, sorted(v))).collect();
            serde_json::Value::Object(ordered.into_iter().collect())
        }
        serde_json::Value::Array(a) => {
            serde_json::Value::Array(a.into_iter().map(sorted).collect())
        }
        v => v,
    }
}

fn diff_values(path: &str, a: &serde_json::Value, b: &serde_json::Value, out: &mut Vec<String>) {
    use serde_json::Value::{Array, Object};
    match (a, b) {
        (Object(x), Object(y)) => {
            let keys: std::collections::BTreeSet<&String> = x.keys().chain(y.keys()).collect();
            for k in keys {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                match (x.get(k), y.get(k)) {
                    (Some(va), Some(vb)) => diff_values(&p, va, vb, out),
                    (Some(va), None) => out.push(format!("{p}: {va} on the first side only")),
                    (None, Some(vb)) => out.push(format!("{p}: {vb} on the second side only")),
                    (None, None) => {}
                }
            }
        }
        (Array(x), Array(y)) if x.len() == y.len() => {
            for (i, (va, vb)) in x.iter().zip(y).enumerate() {
                diff_values(&format!("{path}[{i}]"), va, vb, out);
            }
        }
        _ if a != b => out.push(format!("{path}: {a} != {b}")),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::split_state::SplitState;

    fn shard(n: i32, generation: i32) -> ShardRecord {
        ShardRecord {
            shard_number: n,
            shard_count: 2,
            shard_stripe_size: 2048,
            generation: Some(generation),
            generation_pageserver: Some(1),
            placement_policy: r#"{"Attached":1}"#.to_string(),
            splitting: SplitState::Idle,
            config: r#"{"b":1,"a":2}"#.to_string(),
            scheduling_policy: r#""Active""#.to_string(),
            preferred_az_id: None,
        }
    }

    #[test]
    fn records_in_another_order_are_equal_after_normalizing() {
        let tenant = "1f359dd625e519a1a4e8d7509690f6fc".to_string();
        let mut a = Records::empty();
        a.tenants.insert(
            tenant.clone(),
            TenantSpec {
                tenant_id: tenant.clone(),
                shards: vec![shard(1, 4), shard(0, 3)],
            },
        );
        let mut b = Records::empty();
        b.tenants.insert(
            tenant.clone(),
            TenantSpec {
                tenant_id: tenant.clone(),
                shards: vec![shard(0, 3), shard(1, 4)],
            },
        );
        b.tenants.get_mut(&tenant).unwrap().shards[0].config = r#"{"a":2,"b":1}"#.to_string();
        assert_eq!(a.diff(&b).unwrap(), Vec::<String>::new());
        assert_eq!(a.normalized().unwrap(), b.normalized().unwrap());
        // The text is kept as written
        assert_eq!(b.tenants[&tenant].shards[0].config, r#"{"a":2,"b":1}"#);
    }

    #[test]
    fn a_difference_names_its_field() {
        let tenant = "1f359dd625e519a1a4e8d7509690f6fc".to_string();
        let mut a = Records::empty();
        a.tenants.insert(
            tenant.clone(),
            TenantSpec {
                tenant_id: tenant.clone(),
                shards: vec![shard(0, 3)],
            },
        );
        let mut b = a.clone();
        b.tenants.get_mut(&tenant).unwrap().shards[0].generation = Some(4);
        b.leader = Some(LeaderSpec {
            address: "http://controller.example.com:1234".to_string(),
            started_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        });
        assert_eq!(
            a.diff(&b).unwrap(),
            vec![
                "leader: null != {\"address\":\"http://controller.example.com:1234\",\"startedAt\":\"1970-01-01T00:00:00.000000Z\"}".to_string(),
                format!("tenants.{tenant}.shards[0].generation: 3 != 4"),
            ]
        );
    }
}
