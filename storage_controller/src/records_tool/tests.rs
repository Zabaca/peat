//! The copy in the bucket, the start's checks against it, the went-back record, and the copy
//! tool, against a real API server and a real Postgres (`storage_controller/kubernetes/
//! test-stores.sh`); each test passes without running when the environment has no store.

use std::sync::Arc;

use kube::api::{Api, PostParams};
use pageserver_api::shard::ShardCount;
use safekeeper_api::membership::SafekeeperGeneration;
use utils::id::NodeId;

use super::*;
use crate::persistence::kubernetes::objects::{Object, PageserverSpec, ShardRecord};
use crate::persistence::split_state::SplitState;
use crate::persistence::tests::{
    TENANT, TENANT_B, TIMELINE, node_row, op, safekeeper_upsert, shard_id, shard_row, tenant,
    test_database, test_namespace_and_client, timeline, timeline_row,
};
use crate::persistence::{
    ControllerPersistence, NodePersistence, SafekeeperTimelineOpKind, TenantFilter,
    TenantShardPersistence, TimelineImportPersistence,
};
use crate::records_tool::CopyOptions;

/// The error of what was to be refused.
trait Refused {
    fn refused(self) -> String;
}

impl<T, E: std::fmt::Display> Refused for Result<T, E> {
    fn refused(self) -> String {
        match self {
            Ok(_) => panic!("not refused"),
            Err(e) => e.to_string(),
        }
    }
}

fn local_storage(dir: &camino::Utf8Path) -> String {
    format!("local_path = '{dir}'")
}

fn node(id: i64) -> crate::node::Node {
    crate::node::Node::from_persistent(node_row(id), false).unwrap()
}

/// Records of every kind, written through the controller's own operations.
async fn fill(p: &Persistence) {
    p.insert_node(&node(1)).await.unwrap();
    p.insert_node(&node(2)).await.unwrap();
    p.set_tombstone(NodeId(2)).await.unwrap();
    p.insert_tenant_shards(vec![shard_row(TENANT, 0, 0)])
        .await
        .unwrap();
    p.insert_tenant_shards(vec![shard_row(TENANT_B, 0, 2), shard_row(TENANT_B, 1, 2)])
        .await
        .unwrap();
    for _ in 0..3 {
        p.increment_generation(shard_id(TENANT, 0, 0), NodeId(1))
            .await
            .unwrap();
    }
    p.increment_generation(shard_id(TENANT_B, 1, 2), NodeId(1))
        .await
        .unwrap();
    let children = (0..2)
        .map(|n| TenantShardPersistence {
            splitting: SplitState::Splitting,
            ..shard_row(TENANT, n, 2)
        })
        .collect();
    p.begin_shard_split(
        ShardCount::new(0),
        tenant(TENANT),
        vec![(shard_id(TENANT, 0, 0), children)],
    )
    .await
    .unwrap();
    p.safekeeper_upsert(safekeeper_upsert(1, 1)).await.unwrap();
    p.safekeeper_upsert(safekeeper_upsert(2, 1)).await.unwrap();
    p.insert_timeline(timeline_row(TENANT, TIMELINE))
        .await
        .unwrap();
    p.update_timeline_membership(
        tenant(TENANT),
        timeline(TIMELINE),
        SafekeeperGeneration::new(2),
        &[NodeId(1), NodeId(2)],
        None,
        &[op(
            TENANT,
            TIMELINE,
            3,
            2,
            SafekeeperTimelineOpKind::Exclude,
        )],
    )
    .await
    .unwrap();
    p.insert_pending_op(op(
        TENANT_B,
        "",
        1,
        i32::MAX,
        SafekeeperTimelineOpKind::Delete,
    ))
    .await
    .unwrap();
    p.insert_timeline_import(TimelineImportPersistence {
        tenant_id: TENANT_B.to_string(),
        timeline_id: TIMELINE.to_string(),
        shard_statuses: serde_json::json!({"0002": {"InProgress": null}, "0102": "Done"}),
    })
    .await
    .unwrap();
    p.update_leader(
        None,
        ControllerPersistence {
            address: "http://controller-0.example.com:1234".to_string(),
            started_at: chrono::Utc::now(),
        },
    )
    .await
    .unwrap();
}

/// A controller's store on its own namespace and bucket, started as a controller starts it.
struct Controller {
    p: Arc<Persistence>,
    kubeconfig: std::path::PathBuf,
    namespace: String,
    client: kube::Client,
    bucket: camino_tempfile::Utf8TempDir,
}

impl Controller {
    fn copy_storage(&self) -> String {
        local_storage(self.bucket.path())
    }

    async fn reader(&self) -> BucketCopy {
        BucketCopy::new(&RemoteStorageConfig::from_toml_str(&self.copy_storage()).unwrap())
            .await
            .unwrap()
    }

    /// Another controller on the same records and bucket.
    async fn again(&self, records: KubernetesRecords) -> anyhow::Result<Persistence> {
        let p = Persistence::new_kubernetes(
            Some(&self.kubeconfig),
            Some(self.namespace.clone()),
            KubernetesRecords {
                copy_storage: Some(RemoteStorageConfig::from_toml_str(&self.copy_storage())?),
                ..records
            },
        )
        .await?;
        p.start_checks().await?;
        p.migration_run().await?;
        Ok(p)
    }

    fn stores(&self) -> Stores {
        Stores {
            kubeconfig: Some(self.kubeconfig.clone()),
            records_namespace: Some(self.namespace.clone()),
            records_copy_storage: Some(self.copy_storage()),
            ..Default::default()
        }
    }

    fn raw<S: crate::persistence::kubernetes::objects::Spec>(&self) -> Api<Object<S>> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }
}

async fn controller(records: KubernetesRecords) -> Option<Controller> {
    let (kubeconfig, namespace, client) = test_namespace_and_client().await?;
    let bucket = camino_tempfile::tempdir().unwrap();
    let p = Persistence::new_kubernetes(
        Some(&kubeconfig),
        Some(namespace.clone()),
        KubernetesRecords {
            copy_storage: Some(
                RemoteStorageConfig::from_toml_str(&local_storage(bucket.path())).unwrap(),
            ),
            ..records
        },
    )
    .await
    .unwrap();
    p.start_checks().await.unwrap();
    p.migration_run().await.unwrap();
    Some(Controller {
        p: Arc::new(p),
        kubeconfig,
        namespace,
        client,
        bucket,
    })
}

async fn wait_for_copy(
    copy: &BucketCopy,
    done: impl Fn(&CopyDocument) -> bool,
) -> std::time::Duration {
    let start = std::time::Instant::now();
    loop {
        if let Some(doc) = copy.read().await.unwrap() {
            if done(&doc) {
                return start.elapsed();
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the copy did not follow"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Put an object back as `old` was read (its revision too), as an etcd restore would.
async fn put_back<S: crate::persistence::kubernetes::objects::Spec>(
    api: &Api<Object<S>>,
    old: &Object<S>,
) {
    let now = api.get(old.name()).await.unwrap();
    let mut back = old.clone();
    back.metadata.resource_version = now.metadata.resource_version;
    api.replace(old.name(), &PostParams::default(), &back)
        .await
        .unwrap();
}

#[test]
fn a_key_names_the_generation_it_was_written_with() {
    let t = "1f359dd625e519a1a4e8d7509690f6fc";
    let shard = |s: &str| TenantShardId::from_str(s).unwrap();
    assert_eq!(
        key_generation(&format!(
            "tenants/{t}-0102/timelines/de200bd42b49cc1814412c7e592dd6e9/index_part.json-0000000a"
        )),
        Some((shard(&format!("{t}-0102")), 10))
    );
    assert_eq!(
        key_generation(&format!("tenants/{t}/tenant-manifest-0000002f.json")),
        Some((shard(t), 47))
    );
    assert_eq!(
        key_generation(&format!("tenants/{t}/heatmap-v1.json")),
        None
    );
    assert_eq!(
        key_generation("tenants/not-a-tenant/index_part.json-00000001"),
        None
    );
    assert_eq!(
        key_timeline(&format!(
            "tenants/{t}-0102/timelines/de200bd42b49cc1814412c7e592dd6e9/index_part.json-0000000a"
        )),
        Some((
            TenantId::from_str(t).unwrap(),
            TimelineId::from_str("de200bd42b49cc1814412c7e592dd6e9").unwrap()
        ))
    );
}

#[test]
fn a_restore_sets_every_shard_above_the_copy_and_the_store() {
    let shard = |n: i32, generation: Option<i32>| ShardRecord {
        shard_number: n,
        shard_count: 2,
        shard_stripe_size: 2048,
        generation,
        generation_pageserver: Some(1),
        placement_policy: r#"{"Attached":1}"#.to_string(),
        splitting: SplitState::Idle,
        config: "{}".to_string(),
        scheduling_policy: r#""Active""#.to_string(),
        preferred_az_id: None,
    };
    let (a, b, c) = (
        "1f359dd625e519a1a4e8d7509690f6fc",
        "2c4f2a6b8f1e4d0c9a7b6e5d4c3b2a19",
        "3d5a3b7c9f2e5d1cab8c7f6e5d4c3b2a",
    );
    let mut r = Records::empty();
    for (t, shards) in [
        (a, vec![shard(0, Some(7)), shard(1, Some(9))]),
        (b, vec![shard(0, Some(4))]),
        (c, vec![shard(0, None)]),
    ] {
        r.tenants.insert(
            t.to_string(),
            TenantSpec {
                tenant_id: t.to_string(),
                shards,
            },
        );
    }
    let seen = BTreeMap::from([
        (TenantId::from_str(a).unwrap(), 12),
        (TenantId::from_str(b).unwrap(), 2),
    ]);
    assert_eq!(raise_generations(&mut r, &seen, 1000), 3);
    let g = |t: &str| {
        r.tenants[t]
            .shards
            .iter()
            .map(|s| s.generation)
            .collect::<Vec<_>>()
    };
    assert_eq!(g(a), vec![Some(1012), Some(1012)]);
    assert_eq!(g(b), vec![Some(1004)]);
    assert_eq!(g(c), vec![None]);
}

/// The copy follows the records within a second, with every revision and the highest-ever
/// values; a delete writes its mark first; the highest-ever values outlive a deleted tenant; a
/// copy holding records is not overwritten when every object is deleted under the controller;
/// and a controller on empty records with a non-empty copy refuses to start.
#[tokio::test]
async fn the_copy_follows_the_records_and_never_goes_back() {
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    let copy = c.reader().await;
    fill(&c.p).await;
    let expected = c.p.read_all_records().await.unwrap();
    let took = wait_for_copy(&copy, |d| d.records.diff(&expected).unwrap().is_empty()).await;
    eprintln!("the copy held every record {took:?} after the last write");
    let doc = copy.read().await.unwrap().unwrap();
    assert!(doc.revisions.contains_key(&format!("Tenant/{TENANT}")));
    assert_eq!(
        doc.highest_memberships.get(&format!("{TENANT}-{TIMELINE}")),
        Some(&2)
    );

    c.p.increment_generation(shard_id(TENANT_B, 1, 2), NodeId(1))
        .await
        .unwrap();
    let took = wait_for_copy(&copy, |d| {
        d.records.tenants[TENANT_B].shards[1].generation == Some(2)
    })
    .await;
    assert!(took < Duration::from_secs(1), "{took:?}");

    c.p.delete_tenant(tenant(TENANT_B)).await.unwrap();
    let marks = copy.read_marks().await.unwrap();
    assert!(
        marks.contains_key(&format!("Tenant/{TENANT_B}")),
        "the mark is written before the delete"
    );
    let took = wait_for_copy(&copy, |d| !d.records.tenants.contains_key(TENANT_B)).await;
    assert!(took < Duration::from_secs(1), "{took:?}");
    let doc = copy.read().await.unwrap().unwrap();
    assert_eq!(doc.highest_generations.get(TENANT_B), Some(&2));

    // Every object deleted under the running controller, behind its back: the copy keeps them
    for name in c
        .raw::<TenantSpec>()
        .list(&Default::default())
        .await
        .unwrap()
        .items
    {
        c.raw::<TenantSpec>()
            .delete(name.name(), &Default::default())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let doc = copy.read().await.unwrap().unwrap();
    assert!(doc.records.tenants.contains_key(TENANT));

    // A controller on another, empty namespace with this copy refuses to start
    let (kubeconfig, empty, _) = test_namespace_and_client().await.unwrap();
    let fresh = Persistence::new_kubernetes(
        Some(&kubeconfig),
        Some(empty),
        KubernetesRecords {
            copy_storage: Some(RemoteStorageConfig::from_toml_str(&c.copy_storage()).unwrap()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let err = fresh.start_checks().await.refused();
    assert!(err.contains("storcon_records restore"), "{err}");
}

/// An etcd restore while the controller is stopped: an object below the copy's revision, and a
/// record gone with no mark, each refuse the next start, listing them; a record deleted without
/// a mark can be accepted, nothing that went back can.
#[tokio::test]
async fn a_start_refuses_records_that_went_back_or_went_missing() {
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    let copy = c.reader().await;
    fill(&c.p).await;
    let snapshot = c.raw::<TenantSpec>().get(TENANT_B).await.unwrap();
    c.p.increment_generation(shard_id(TENANT_B, 1, 2), NodeId(1))
        .await
        .unwrap();
    let expected = c.p.read_all_records().await.unwrap();
    wait_for_copy(&copy, |d| d.records.diff(&expected).unwrap().is_empty()).await;
    c.p.stop_copy();

    put_back(&c.raw::<TenantSpec>(), &snapshot).await;
    let err = c.again(KubernetesRecords::default()).await.refused();
    assert!(err.contains("went back [\"Tenant/2c4f"), "{err}");
    // Accepting what went back is refused too
    let err = c
        .again(KubernetesRecords {
            accept_deleted: vec![format!("Tenant/{TENANT_B}")],
            ..Default::default()
        })
        .await
        .refused();
    assert!(err.contains("not missing [\"Tenant/2c4f"), "{err}");

    // Put it forward again; now delete a Safekeeper behind the store's back, with no mark
    let ahead = c.raw::<TenantSpec>().get(TENANT_B).await.unwrap();
    let mut forward = ahead.clone();
    forward.spec = expected.tenants[TENANT_B].clone();
    forward.metadata.annotations.as_mut().unwrap().insert(
        crate::persistence::kubernetes::objects::REVISION_ANNOTATION.to_string(),
        (snapshot.revision().unwrap() + 5).to_string(),
    );
    c.raw::<TenantSpec>()
        .replace(TENANT_B, &PostParams::default(), &forward)
        .await
        .unwrap();
    c.raw::<crate::persistence::kubernetes::objects::SafekeeperSpec>()
        .delete("safekeeper-2", &Default::default())
        .await
        .unwrap();
    let err = c.again(KubernetesRecords::default()).await.refused();
    assert!(
        err.contains("missing [\"Safekeeper/safekeeper-2\"]"),
        "{err}"
    );
    let accepted = c
        .again(KubernetesRecords {
            accept_deleted: vec!["Safekeeper/safekeeper-2".to_string()],
            ..Default::default()
        })
        .await
        .unwrap();
    let marks = copy.read_marks().await.unwrap();
    assert!(marks["Safekeeper/safekeeper-2"].accepted);
    accepted.stop_copy();
    // And once accepted, the next start passes without it, and with it still set (a pod
    // restarted before the setting is taken out)
    c.again(KubernetesRecords::default())
        .await
        .unwrap()
        .stop_copy();
    c.again(KubernetesRecords {
        accept_deleted: vec!["Safekeeper/safekeeper-2".to_string()],
        ..Default::default()
    })
    .await
    .unwrap()
    .stop_copy();
}

/// A crash between a mark and its delete: the object is at its mark's revision; the start
/// passes, and the controller finishes the delete once it leads. While it runs, such an object
/// is never listed into a re-attach.
#[tokio::test]
async fn an_unfinished_delete_is_finished_not_refused() {
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    let copy = c.reader().await;
    c.p.insert_node(&node(1)).await.unwrap();
    c.p.insert_tenant_shards(vec![shard_row(TENANT, 0, 0)])
        .await
        .unwrap();
    c.p.increment_generation(shard_id(TENANT, 0, 0), NodeId(1))
        .await
        .unwrap();
    let obj = c.raw::<TenantSpec>().get(TENANT).await.unwrap();
    wait_for_copy(&copy, |d| {
        d.revisions.get(&format!("Tenant/{TENANT}")) == Some(&obj.revision().unwrap())
    })
    .await;
    // The mark the delete wrote, and the crash before the delete
    copy.write_mark(
        &format!("Tenant/{TENANT}"),
        Mark {
            revision: obj.revision().unwrap(),
            accepted: false,
        },
    )
    .await
    .unwrap();
    c.p.stop_copy();
    // A running controller holding that mark does not re-attach the tenant
    let running = c.again(KubernetesRecords::default());
    let p2 = running.await.unwrap();
    assert!(
        c.raw::<TenantSpec>()
            .get_opt(TENANT)
            .await
            .unwrap()
            .is_none(),
        "the delete was finished"
    );
    assert!(p2.re_attach(NodeId(1)).await.unwrap().is_empty());
    p2.stop_copy();
}

/// The cluster's store taken back under the running controller: it refuses the read, records
/// what it had seen in the bucket, and every later start refuses until a restore.
#[tokio::test]
async fn a_controller_that_sees_its_records_go_back_records_it_for_the_next_start() {
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    let copy = c.reader().await;
    c.p.insert_node(&node(1)).await.unwrap();
    c.p.insert_node(&node(2)).await.unwrap();
    c.p.insert_tenant_shards(vec![shard_row(TENANT, 0, 0)])
        .await
        .unwrap();
    let id = shard_id(TENANT, 0, 0);
    c.p.increment_generation(id, NodeId(1)).await.unwrap();
    let snapshot = c.raw::<TenantSpec>().get(TENANT).await.unwrap();
    c.p.increment_generation(id, NodeId(2)).await.unwrap();
    put_back(&c.raw::<TenantSpec>(), &snapshot).await;
    assert!(c.p.re_attach(NodeId(1)).await.is_err());
    // Halted: it answers nothing more
    assert!(c.p.list_nodes().await.is_err());
    let start = std::time::Instant::now();
    loop {
        if copy.read_went_back().await.unwrap().is_some() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "no went-back record"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let w = copy.read_went_back().await.unwrap().unwrap();
    assert!(w.seen[&format!("Tenant/{TENANT}")] > snapshot.revision().unwrap());
    c.p.stop_copy();
    let err = c.again(KubernetesRecords::default()).await.refused();
    assert!(err.contains("found the records went back"), "{err}");
}

/// While the copy is further behind than the limit (the bucket cannot be written), no generation
/// is raised; nothing else stops.
#[tokio::test]
async fn no_generation_is_raised_while_the_copy_is_far_behind() {
    let Some(c) = controller(KubernetesRecords {
        copy_max_lag: Some(Duration::from_secs(1)),
        ..Default::default()
    })
    .await
    else {
        return;
    };
    let copy = c.reader().await;
    c.p.insert_node(&node(1)).await.unwrap();
    c.p.insert_tenant_shards(vec![shard_row(TENANT, 0, 0)])
        .await
        .unwrap();
    wait_for_copy(&copy, |d| d.records.tenants.contains_key(TENANT)).await;
    // The bucket cannot be written
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(c.bucket.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
    c.p.update_node_scheduling_policy(
        NodeId(1),
        pageserver_api::controller_api::NodeSchedulingPolicy::Pause,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let err =
        c.p.increment_generation(shard_id(TENANT, 0, 0), NodeId(1))
            .await
            .unwrap_err()
            .to_string();
    std::fs::set_permissions(c.bucket.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(err.contains("behind"), "{err}");
    // Once the copy catches up, it is raised again
    wait_for_copy(&copy, |d| {
        d.records.pageservers["pageserver-1"].scheduling_policy == "pause"
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    c.p.increment_generation(shard_id(TENANT, 0, 0), NodeId(1))
        .await
        .unwrap();
    c.p.stop_copy();
}

/// The copy is written only while the Leader record names this controller.
#[tokio::test]
async fn the_copy_is_not_written_while_another_controller_leads() {
    let Some(c) = controller(KubernetesRecords {
        own_address: Some("http://controller-0.example.com:1234".to_string()),
        ..Default::default()
    })
    .await
    else {
        return;
    };
    let copy = c.reader().await;
    fill(&c.p).await;
    wait_for_copy(&copy, |d| d.records.leader.is_some()).await;
    let leader = c.p.get_leader().await.unwrap().unwrap();
    c.p.update_leader(
        Some(leader),
        ControllerPersistence {
            address: "http://controller-1.example.com:1234".to_string(),
            started_at: chrono::Utc::now(),
        },
    )
    .await
    .unwrap();
    c.p.increment_generation(shard_id(TENANT_B, 1, 2), NodeId(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let doc = copy.read().await.unwrap().unwrap();
    assert_eq!(
        doc.records.tenants[TENANT_B].shards[1].generation,
        Some(1),
        "not written"
    );
}

/// A start whose Leader record names another controller (the one it takes over from, dead or
/// alive, or one the tool copied in) writes the copy once it becomes leader, and stops, showing
/// behind, when another is named after it.
#[tokio::test]
async fn the_copy_is_written_from_when_this_controller_becomes_leader() {
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    let copy = c.reader().await;
    // The Leader record names controller-0
    fill(&c.p).await;
    let expected = c.p.read_all_records().await.unwrap();
    wait_for_copy(&copy, |d| d.records.diff(&expected).unwrap().is_empty()).await;
    c.p.stop_copy();

    let own = "http://controller-1.example.com:1234".to_string();
    let p = c
        .again(KubernetesRecords {
            own_address: Some(own.clone()),
            copy_max_lag: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .await
        .unwrap();
    // Before it leads: not written
    p.update_node_scheduling_policy(
        NodeId(1),
        pageserver_api::controller_api::NodeSchedulingPolicy::Pause,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let doc = copy.read().await.unwrap().unwrap();
    assert_eq!(
        doc.records.pageservers["pageserver-1"].scheduling_policy,
        "active"
    );
    // It becomes leader: written
    let leader = p.get_leader().await.unwrap();
    p.update_leader(
        leader,
        ControllerPersistence {
            address: own,
            started_at: chrono::Utc::now(),
        },
    )
    .await
    .unwrap();
    wait_for_copy(&copy, |d| {
        d.records.pageservers["pageserver-1"].scheduling_policy == "pause"
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    p.increment_generation(shard_id(TENANT, 0, 0), NodeId(1))
        .await
        .unwrap();
    // Another is named after it: no more writes, and the copy shows behind
    let leader = p.get_leader().await.unwrap();
    p.update_leader(
        leader,
        ControllerPersistence {
            address: "http://controller-2.example.com:1234".to_string(),
            started_at: chrono::Utc::now(),
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let err = p
        .increment_generation(shard_id(TENANT, 0, 0), NodeId(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("behind"), "{err}");
    let doc = copy.read().await.unwrap().unwrap();
    assert_eq!(
        doc.records.leader.as_ref().map(|l| l.address.as_str()),
        Some("http://controller-1.example.com:1234")
    );
    p.stop_copy();
}

/// Under --records-start-without-copy nothing is made until the copy and its marks are read.
#[tokio::test]
async fn nothing_is_made_before_the_copy_is_read() {
    let Some((kubeconfig, namespace, _)) = test_namespace_and_client().await else {
        return;
    };
    // A bucket that cannot be read
    use std::os::unix::fs::PermissionsExt;
    let dir = camino_tempfile::tempdir().unwrap();
    let bucket = dir.path().join("bucket");
    std::fs::create_dir(&bucket).unwrap();
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o000)).unwrap();
    let p = Persistence::new_kubernetes(
        Some(&kubeconfig),
        Some(namespace),
        KubernetesRecords {
            copy_storage: Some(
                RemoteStorageConfig::from_toml_str(&local_storage(&bucket)).unwrap(),
            ),
            start_without_copy: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    p.start_checks().await.unwrap();
    p.migration_run().await.unwrap();
    let err = p.insert_node(&node(1)).await.refused();
    assert!(err.contains("not made before the copy"), "{err}");
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o700)).unwrap();
}

fn both(c: &Controller, url: &str) -> Stores {
    Stores {
        database_url: Some(url.to_string()),
        ..c.stores()
    }
}

/// The tool, both ways: Postgres to Kubernetes (with the pageservers up, and a new copy), back
/// into another database (refused while a pageserver answers), compared field by field; its
/// refusals when it cannot tell whether the controller is stopped; the marks it writes for names
/// it removes; and a stale source refused against the copy's highest-ever values.
#[tokio::test]
async fn the_tool_copies_both_ways_and_refuses_what_it_must() {
    let Some(first_db) = test_database().await else {
        return;
    };
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    c.p.stop_copy();
    let stores = both(&c, &first_db);
    let pg = stores.open(Store::Postgres, true).await.unwrap();
    fill(&pg).await;
    let stopped = Running::default();
    // Pageserver 1 on a port of this machine nothing listens on: a stopped pageserver
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let (sql, conn) = tokio_postgres::connect(&first_db, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    sql.batch_execute(&format!(
        "UPDATE nodes SET listen_http_addr = '127.0.0.1', listen_http_port = {closed} WHERE node_id = 1"
    ))
    .await
    .unwrap();

    // A pageserver answering does not stop the move into Kubernetes
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    pg.insert_node(
        &crate::node::Node::from_persistent(
            NodePersistence {
                listen_http_addr: "127.0.0.1".to_string(),
                listen_http_port: port as i32,
                ..node_row(7)
            },
            false,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    // The leader's address does not resolve: refused, unless checked by hand
    match copy(
        &stores,
        Store::Postgres,
        Store::Kubernetes,
        false,
        &stopped,
        &BTreeSet::new(),
    )
    .await
    .unwrap()
    {
        Outcome::Refused(r) => assert!(r.contains("cannot tell"), "{r}"),
        o => panic!("{o:?}"),
    }
    let refused_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let controller_stopped = Running {
        controller_address: Some(format!("http://127.0.0.1:{refused_port}")),
        assume_stopped: false,
    };
    assert_eq!(
        copy(
            &stores,
            Store::Postgres,
            Store::Kubernetes,
            false,
            &controller_stopped,
            &BTreeSet::new()
        )
        .await
        .unwrap(),
        Outcome::Done
    );
    let (cmp, counts) = compare(&stores, Store::Postgres, Store::Kubernetes)
        .await
        .unwrap();
    assert_eq!(cmp, Outcome::Done);
    assert_eq!(counts["shards"], (5, 5));
    let doc = c.reader().await.read().await.unwrap().unwrap();
    assert_eq!(
        doc.records
            .diff(&pg.read_all_records().await.unwrap())
            .unwrap(),
        Vec::<String>::new()
    );

    // The way back, into another database: refused while a pageserver answers
    let second = Stores {
        database_url: Some(test_database().await.unwrap()),
        ..stores.clone()
    };
    match copy(
        &second,
        Store::Kubernetes,
        Store::Postgres,
        false,
        &controller_stopped,
        &BTreeSet::new(),
    )
    .await
    .unwrap()
    {
        Outcome::Refused(r) => assert!(r.contains("pageserver 7"), "{r}"),
        o => panic!("{o:?}"),
    }
    drop(listener);
    assert_eq!(
        copy(
            &second,
            Store::Kubernetes,
            Store::Postgres,
            false,
            &controller_stopped,
            &BTreeSet::new()
        )
        .await
        .unwrap(),
        Outcome::Done
    );
    let (a, _) = stores.read(Store::Postgres).await.unwrap();
    let (b, _) = second.read(Store::Postgres).await.unwrap();
    assert_eq!(a.diff(&b).unwrap(), Vec::<String>::new());

    // A name the source no longer holds is removed with a mark at or above its revision
    let removed = c.raw::<PageserverSpec>().get("pageserver-7").await.unwrap();
    pg.set_tombstone(NodeId(7)).await.unwrap();
    pg.delete_node(NodeId(7)).await.unwrap();
    // A source that went back (its generation below what the copy has seen) is refused
    pg.update_tenant_shard(
        TenantFilter::Shard(shard_id(TENANT_B, 1, 2)),
        None,
        None,
        Some(Generation::new(0)),
        None,
    )
    .await
    .unwrap();
    match copy(
        &stores,
        Store::Postgres,
        Store::Kubernetes,
        true,
        &controller_stopped,
        &BTreeSet::new(),
    )
    .await
    .unwrap()
    {
        Outcome::Refused(r) => assert!(r.contains(&format!("Tenant/{TENANT_B}")), "{r}"),
        o => panic!("{o:?}"),
    }
    let remade = BTreeSet::from([format!("Tenant/{TENANT_B}")]);
    assert_eq!(
        copy(
            &stores,
            Store::Postgres,
            Store::Kubernetes,
            true,
            &controller_stopped,
            &remade
        )
        .await
        .unwrap(),
        Outcome::Done
    );
    let marks = c.reader().await.read_marks().await.unwrap();
    assert!(marks["Pageserver/pageserver-7"].revision >= removed.revision().unwrap());
    // And a controller starts on what the tool wrote
    c.again(KubernetesRecords::default())
        .await
        .unwrap()
        .stop_copy();
}

/// A restore after every object was lost: refused while a pageserver of the copy answers, for a
/// scan of the wrong storage or older than the copy, for a tenant the store holds and the copy
/// lacks (unless left out, which marks it), and for a silent safekeeper (unless accepted, which
/// raises its membership); then every generation above the copy's, the highest-ever and the
/// store's, the went-back record cleared, and the controller starts.
#[tokio::test]
async fn a_restore_raises_every_generation_and_refuses_what_it_must() {
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    let copy_reader = c.reader().await;
    fill(&c.p).await;
    let before = c.p.read_all_records().await.unwrap();
    wait_for_copy(&copy_reader, |d| {
        d.records.diff(&before).unwrap().is_empty()
    })
    .await;
    c.p.stop_copy();

    // The pageservers' store: a newer generation of TENANT_B, and a tenant the copy lacks
    let store = camino_tempfile::tempdir().unwrap();
    let other = "4e6b4c8d0a3f6e2dbc9d8a7f6e5d4c3b";
    for key in [
        format!("tenants/{TENANT_B}-0102/timelines/{TIMELINE}/index_part.json-00000009"),
        format!("tenants/{TENANT}-0002/timelines/{TIMELINE}/index_part.json-00000003"),
        format!("tenants/{other}/timelines/{TIMELINE}/index_part.json-00000001"),
    ] {
        let path = store.path().join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{}").unwrap();
    }
    // The cluster loses every object
    for name in before.tenants.keys() {
        c.raw::<TenantSpec>()
            .delete(name, &Default::default())
            .await
            .unwrap();
    }
    let stores = c.stores();
    let stopped = Running {
        assume_stopped: true,
        ..Default::default()
    };
    let scan = scan_generations(&local_storage(store.path()))
        .await
        .unwrap();
    assert_eq!(scan.shards[&format!("{TENANT_B}-0102")], 9);
    let options = RestoreOptions {
        margin: 1000,
        replace: true,
        ..Default::default()
    };

    // A scan older than the copy
    let old_scan = Scan {
        scanned_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        ..scan.clone()
    };
    let (o, _) = restore(&stores, Store::Kubernetes, &old_scan, &options, &stopped)
        .await
        .unwrap();
    assert!(
        matches!(o, Outcome::Refused(ref r) if r.contains("before the copy's last write")),
        "{o:?}"
    );
    // A scan of the wrong storage
    let wrong = Scan {
        shards: BTreeMap::from([(other.to_string(), 1)]),
        ..scan.clone()
    };
    let (o, _) = restore(&stores, Store::Kubernetes, &wrong, &options, &stopped)
        .await
        .unwrap();
    assert!(
        matches!(o, Outcome::Refused(ref r) if r.contains("wrong bucket")),
        "{o:?}"
    );
    // A tenant the store holds and the copy lacks
    let (o, _) = restore(&stores, Store::Kubernetes, &scan, &options, &stopped)
        .await
        .unwrap();
    assert!(
        matches!(o, Outcome::Refused(ref r) if r.contains(other)),
        "{o:?}"
    );
    // A safekeeper that does not answer
    let leave = RestoreOptions {
        leave_out: BTreeSet::from([format!("Tenant/{other}")]),
        ..options.clone()
    };
    let (o, _) = restore(&stores, Store::Kubernetes, &scan, &leave, &stopped)
        .await
        .unwrap();
    assert!(
        matches!(o, Outcome::Refused(ref r) if r.contains("did not answer")),
        "{o:?}"
    );
    // A pageserver of the copy still answering
    let running = Running {
        controller_address: Some("http://127.0.0.1:1".to_string()),
        assume_stopped: false,
    };
    let (o, _) = restore(&stores, Store::Kubernetes, &scan, &leave, &running)
        .await
        .unwrap();
    assert!(matches!(o, Outcome::Refused(_)), "{o:?}");

    copy_reader
        .write_went_back(&crate::persistence::kubernetes::copy::WentBack {
            at: chrono::Utc::now(),
            what: "a test".to_string(),
            seen: BTreeMap::new(),
        })
        .await
        .unwrap();
    let accept = RestoreOptions {
        accept_silent_safekeepers: true,
        ..leave.clone()
    };
    let (o, report) = restore(&stores, Store::Kubernetes, &scan, &accept, &stopped)
        .await
        .unwrap();
    assert_eq!(o, Outcome::Done);
    assert_eq!(report.shards_raised, 5);
    assert_eq!(report.left_out, vec![format!("Tenant/{other}")]);
    let after = c.p.read_all_records().await.unwrap();
    let g = |r: &Records, t: &str| {
        r.tenants[t]
            .shards
            .iter()
            .map(|s| s.generation)
            .collect::<Vec<_>>()
    };
    assert_eq!(g(&after, TENANT), vec![Some(1003), Some(1003), Some(1003)]);
    assert_eq!(g(&after, TENANT_B), vec![Some(1009), Some(1009)]);
    let m = after.timelines[&format!("{TENANT}-{TIMELINE}")]
        .membership
        .as_ref()
        .unwrap();
    assert_eq!(
        m.generation, 1002,
        "a silent member's timeline raised by the margin"
    );
    assert!(copy_reader.read_went_back().await.unwrap().is_none());
    assert!(
        copy_reader
            .read_marks()
            .await
            .unwrap()
            .contains_key(&format!("Tenant/{other}"))
    );

    // The controller starts on what the restore made
    c.again(KubernetesRecords::default())
        .await
        .unwrap()
        .stop_copy();
}

/// A copy with --replace refuses, in both directions, to empty the destination from a source
/// with no pageserver, and to lower or drop what the destination holds (a shard at a higher
/// generation, or missing from the source), unless told that is meant.
#[tokio::test]
async fn a_replacing_copy_refuses_to_empty_or_lower_the_destination() {
    const TENANT_C: &str = "3d5a1b7c9e2f4a6b8c0d1e2f3a4b5c6d";
    let Some(db) = test_database().await else {
        return;
    };
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    let copy_reader = c.reader().await;
    fill(&c.p).await;
    let filled = c.p.read_all_records().await.unwrap();
    wait_for_copy(&copy_reader, |d| {
        d.records.diff(&filled).unwrap().is_empty()
    })
    .await;
    c.p.stop_copy();
    let stores = both(&c, &db);
    let stopped = Running {
        assume_stopped: true,
        ..Default::default()
    };
    let none = BTreeSet::new();
    let refused = |o: Outcome| match o {
        Outcome::Refused(r) => r,
        o => panic!("not refused: {o:?}"),
    };
    let accepting = |names: &[String]| CopyOptions {
        replace: true,
        accept_older: names.iter().cloned().collect(),
        ..Default::default()
    };
    let emptying = CopyOptions {
        replace: true,
        empty_destination: true,
        ..Default::default()
    };
    assert_eq!(
        copy(
            &stores,
            Store::Kubernetes,
            Store::Postgres,
            false,
            &stopped,
            &none
        )
        .await
        .unwrap(),
        Outcome::Done
    );

    // Back into Postgres: a shard the source holds at a lower generation than the destination,
    // and one it lacks
    let pg = stores.open(Store::Postgres, true).await.unwrap();
    pg.increment_generation(shard_id(TENANT_B, 1, 2), NodeId(1))
        .await
        .unwrap();
    pg.insert_tenant_shards(vec![shard_row(TENANT_C, 0, 0)])
        .await
        .unwrap();
    let r = refused(
        copy(
            &stores,
            Store::Kubernetes,
            Store::Postgres,
            true,
            &stopped,
            &none,
        )
        .await
        .unwrap(),
    );
    assert!(
        r.contains(&format!("Tenant/{TENANT_B} shard 1/2: generation")),
        "{r}"
    );
    assert!(
        r.contains(&format!("Tenant/{TENANT_C} shard 0/0: not in the source")),
        "{r}"
    );
    // Each loss is accepted by name: one named, the other still refused
    let b = format!("Tenant/{TENANT_B}");
    let c3 = format!("Tenant/{TENANT_C}");
    let r = refused(
        copy(
            &stores,
            Store::Kubernetes,
            Store::Postgres,
            accepting(&[b.clone()]),
            &stopped,
            &none,
        )
        .await
        .unwrap(),
    );
    assert!(!r.contains(&b), "{r}");
    assert!(r.contains(&c3), "{r}");
    assert_eq!(
        copy(
            &stores,
            Store::Kubernetes,
            Store::Postgres,
            accepting(&[b.clone(), c3]),
            &stopped,
            &none
        )
        .await
        .unwrap(),
        Outcome::Done
    );
    // Without the bucket: refused, unless asked to go without its check
    let unchecked = Stores {
        records_copy_storage: None,
        ..stores.clone()
    };
    let r = refused(
        copy(
            &unchecked,
            Store::Kubernetes,
            Store::Postgres,
            false,
            &stopped,
            &none,
        )
        .await
        .unwrap(),
    );
    assert!(r.contains("needs --records-copy-storage"), "{r}");
    let without = CopyOptions {
        replace: true,
        no_bucket_check: true,
        ..Default::default()
    };
    assert_eq!(
        copy(
            &unchecked,
            Store::Kubernetes,
            Store::Postgres,
            without,
            &stopped,
            &none
        )
        .await
        .unwrap(),
        Outcome::Done
    );

    // Into Kubernetes: a shard the destination holds and the source lacks
    pg.delete_tenant(tenant(TENANT)).await.unwrap();
    let r = refused(
        copy(
            &stores,
            Store::Postgres,
            Store::Kubernetes,
            true,
            &stopped,
            &none,
        )
        .await
        .unwrap(),
    );
    assert!(r.contains(&format!("Tenant/{TENANT} shard")), "{r}");
    assert!(r.contains("not in the source"), "{r}");

    // Back into Postgres, a source below what the bucket's copy has seen: refused even with
    // --accept-older, which speaks only for the destination
    let mut lowered = c.raw::<TenantSpec>().get(TENANT_B).await.unwrap();
    for shard in &mut lowered.spec.shards {
        shard.generation = Some(0);
    }
    c.raw::<TenantSpec>()
        .replace(TENANT_B, &PostParams::default(), &lowered)
        .await
        .unwrap();
    let r = refused(
        copy(
            &stores,
            Store::Kubernetes,
            Store::Postgres,
            accepting(&[b.clone()]),
            &stopped,
            &none,
        )
        .await
        .unwrap(),
    );
    assert!(
        r.contains("below what the copy in the bucket has seen"),
        "{r}"
    );
    assert!(r.contains(&format!("Tenant/{TENANT_B}")), "{r}");

    // An empty source, both ways: refused, unless the destination is meant to be emptied
    let empty = Stores {
        database_url: Some(test_database().await.unwrap()),
        ..stores.clone()
    };
    Persistence::new(empty.database_url.clone().unwrap())
        .await
        .migration_run()
        .await
        .unwrap();
    let r = refused(
        copy(
            &empty,
            Store::Postgres,
            Store::Kubernetes,
            true,
            &stopped,
            &none,
        )
        .await
        .unwrap(),
    );
    assert!(r.contains("holds no pageserver"), "{r}");
    let Some(d) = controller(KubernetesRecords::default()).await else {
        return;
    };
    d.p.stop_copy();
    let r = refused(
        copy(
            &both(&d, &db),
            Store::Kubernetes,
            Store::Postgres,
            true,
            &stopped,
            &none,
        )
        .await
        .unwrap(),
    );
    assert!(r.contains("holds no pageserver"), "{r}");
    // Meant: emptied, with nothing of what it loses to name
    assert_eq!(
        copy(
            &both(&d, &db),
            Store::Kubernetes,
            Store::Postgres,
            emptying,
            &stopped,
            &none
        )
        .await
        .unwrap(),
        Outcome::Done
    );
    assert!(pg.load_active_tenant_shards().await.unwrap().is_empty());
}

/// Going back after a tenant was split on the Kubernetes records: the destination's parent shard
/// is held split by the source, so it is not taken for a loss; children below its generation are.
#[tokio::test]
async fn a_split_after_the_switch_goes_back() {
    let Some(db) = test_database().await else {
        return;
    };
    let Some(c) = controller(KubernetesRecords::default()).await else {
        return;
    };
    c.p.insert_node(&node(1)).await.unwrap();
    c.p.insert_tenant_shards(vec![shard_row(TENANT, 0, 0)])
        .await
        .unwrap();
    for _ in 0..2 {
        c.p.increment_generation(shard_id(TENANT, 0, 0), NodeId(1))
            .await
            .unwrap();
    }
    let stores = both(&c, &db);
    let stopped = Running {
        assume_stopped: true,
        ..Default::default()
    };
    let none = BTreeSet::new();
    let copy_reader = c.reader().await;
    let wait = |records: Records| {
        let copy_reader = &copy_reader;
        async move {
            wait_for_copy(copy_reader, |d| {
                d.records.diff(&records).unwrap().is_empty()
            })
            .await;
        }
    };
    wait(c.p.read_all_records().await.unwrap()).await;
    // With --replace into a new database: migrated before what it holds is read
    assert_eq!(
        copy(
            &stores,
            Store::Kubernetes,
            Store::Postgres,
            true,
            &stopped,
            &none
        )
        .await
        .unwrap(),
        Outcome::Done
    );
    // Split on Kubernetes after the switch: the children at the parent's generation (2)
    let children = |generation| -> Vec<TenantShardPersistence> {
        (0..2)
            .map(|n| TenantShardPersistence {
                splitting: SplitState::Splitting,
                generation: Some(generation),
                generation_pageserver: Some(1),
                ..shard_row(TENANT, n, 2)
            })
            .collect()
    };
    c.p.begin_shard_split(
        ShardCount::new(0),
        tenant(TENANT),
        vec![(shard_id(TENANT, 0, 0), children(2))],
    )
    .await
    .unwrap();
    c.p.complete_shard_split(tenant(TENANT), ShardCount::new(0), ShardCount::new(2))
        .await
        .unwrap();
    wait(c.p.read_all_records().await.unwrap()).await;
    // Back: the parent the destination holds is covered by the children
    assert_eq!(
        copy(
            &stores,
            Store::Kubernetes,
            Store::Postgres,
            true,
            &stopped,
            &none
        )
        .await
        .unwrap(),
        Outcome::Done
    );

    // A child below the parent's generation is a loss: the destination's parent is not covered
    let pg = stores.open(Store::Postgres, true).await.unwrap();
    pg.delete_tenant(tenant(TENANT)).await.unwrap();
    let mut parent = shard_row(TENANT, 0, 0);
    parent.generation = Some(3);
    pg.insert_tenant_shards(vec![parent]).await.unwrap();
    match copy(
        &stores,
        Store::Kubernetes,
        Store::Postgres,
        true,
        &stopped,
        &none,
    )
    .await
    .unwrap()
    {
        Outcome::Refused(r) => {
            assert!(
                r.contains(&format!("Tenant/{TENANT} shard 0/0: not in the source")),
                "{r}"
            )
        }
        o => panic!("not refused: {o:?}"),
    }
    c.p.stop_copy();
}
