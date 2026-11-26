//! The records' operations, run the same way on every store a test environment provides, through
//! [`Persistence`], the seam the controller uses. Each scenario asserts what Neon's controller
//! relies on, and returns a trace of what it saw; the traces of the two stores must be equal, so
//! the Kubernetes store is held to the Postgres store's behaviour.
//!
//! A store takes part when its environment is set:
//! - `STORCON_TEST_KUBECONFIG`: a kubeconfig for an API server with
//!   `storage_controller/kubernetes/crds.yaml` applied; each test makes a namespace of its own.
//! - `STORCON_TEST_DATABASE_URL`: a Postgres server; each test makes a database of its own.
//!
//! `storage_controller/kubernetes/test-stores.sh` starts both and runs a command with them set.
//! With neither set the tests pass without running, unless `STORCON_TEST_STORES_REQUIRED` is set
//! (as CI sets it), when a missing store fails them.

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, PostParams};
use pageserver_api::controller_api::{
    AvailabilityZone, NodeSchedulingPolicy, PlacementPolicy, ShardSchedulingPolicy,
    SkSchedulingPolicy,
};
use pageserver_api::models::ShardImportStatus;
use pageserver_api::shard::{ShardCount, ShardIndex, ShardNumber, TenantShardId};
use safekeeper_api::membership::SafekeeperGeneration;
use utils::id::{NodeId, TenantId, TimelineId};
use utils::lsn::Lsn;

use super::split_state::SplitState;
use super::*;
use crate::timeline_import::ShardImportStatuses;

const KUBECONFIG_ENV: &str = "STORCON_TEST_KUBECONFIG";
const DATABASE_URL_ENV: &str = "STORCON_TEST_DATABASE_URL";
const REQUIRED_ENV: &str = "STORCON_TEST_STORES_REQUIRED";

async fn kubernetes_store() -> Option<Persistence> {
    let path = std::env::var(KUBECONFIG_ENV).ok()?;
    let kubeconfig = kube::config::Kubeconfig::read_from(&path).expect("read the test kubeconfig");
    let mut config = kube::Config::from_custom_kubeconfig(kubeconfig, &Default::default())
        .await
        .expect("a client config from the test kubeconfig");
    // The test API server is on this machine: never through a proxy the environment names.
    config.proxy_url = None;
    let client = kube::Client::try_from(config).expect("a Kubernetes client");
    let namespace = format!("records-test-{}", uuid::Uuid::new_v4().simple());
    let ns = Namespace {
        metadata: kube::api::ObjectMeta {
            name: Some(namespace.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    Api::<Namespace>::all(client.clone())
        .create(&PostParams::default(), &ns)
        .await
        .expect("make the test's namespace");
    let p = Persistence::kubernetes_with_client(client, namespace);
    p.migration_run().await.expect("the kinds are installed");
    Some(p)
}

async fn postgres_store() -> Option<Persistence> {
    let url = std::env::var(DATABASE_URL_ENV).ok()?;
    let name = format!("storcon_test_{}", uuid::Uuid::new_v4().simple());
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect to the test Postgres");
    tokio::spawn(conn);
    client
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .expect("make the test's database");
    let mut config: tokio_postgres::Config = url.parse().unwrap();
    config.dbname(&name);
    let url = format!(
        "postgresql://{}@{}:{}/{name}",
        config.get_user().unwrap_or("postgres"),
        match &config.get_hosts()[0] {
            tokio_postgres::config::Host::Tcp(h) => h.clone(),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(p) => p.to_string_lossy().to_string(),
        },
        config.get_ports().first().copied().unwrap_or(5432),
    );
    let p = Persistence::new(url).await;
    p.migration_run().await.expect("run the migrations");
    Some(p)
}

/// Every store this environment provides, each empty.
async fn stores() -> Vec<(&'static str, Arc<Persistence>)> {
    let mut out = Vec::new();
    if let Some(p) = postgres_store().await {
        out.push(("postgres", Arc::new(p)));
    }
    if let Some(p) = kubernetes_store().await {
        out.push(("kubernetes", Arc::new(p)));
    }
    if out.len() < 2 && std::env::var(REQUIRED_ENV).is_ok() {
        panic!("{REQUIRED_ENV} is set but {KUBECONFIG_ENV} or {DATABASE_URL_ENV} is not");
    }
    if out.is_empty() {
        eprintln!("no store to test: set {KUBECONFIG_ENV} and {DATABASE_URL_ENV}");
    }
    out
}

/// Run a scenario on every store and require the same trace from each.
async fn on_every_store<F, Fut>(scenario: F)
where
    F: Fn(Arc<Persistence>) -> Fut,
    Fut: std::future::Future<Output = Vec<String>>,
{
    let mut traces: Vec<(&str, Vec<String>)> = Vec::new();
    for (name, p) in stores().await {
        traces.push((name, scenario(p).await));
    }
    if let Some((first, expected)) = traces.first() {
        for (name, trace) in &traces[1..] {
            assert_eq!(
                trace, expected,
                "the {name} store's trace differs from the {first} store's"
            );
        }
    }
}

const TENANT: &str = "1f359dd625e519a1a4e8d7509690f6fc";
const TENANT_B: &str = "2c4f2a6b8f1e4d0c9a7b6e5d4c3b2a19";
const TIMELINE: &str = "de200bd42b49cc1814412c7e592dd6e9";

fn tenant(s: &str) -> TenantId {
    TenantId::from_str(s).unwrap()
}

fn timeline(s: &str) -> TimelineId {
    TimelineId::from_str(s).unwrap()
}

fn shard_id(t: &str, number: u8, count: u8) -> TenantShardId {
    TenantShardId {
        tenant_id: tenant(t),
        shard_number: ShardNumber(number),
        shard_count: ShardCount::new(count),
    }
}

fn shard_row(t: &str, number: i32, count: i32) -> TenantShardPersistence {
    TenantShardPersistence {
        tenant_id: t.to_string(),
        shard_number: number,
        shard_count: count,
        shard_stripe_size: pageserver_api::shard::DEFAULT_STRIPE_SIZE.0 as i32,
        generation: Some(0),
        generation_pageserver: None,
        placement_policy: serde_json::to_string(&PlacementPolicy::Attached(1)).unwrap(),
        splitting: SplitState::default(),
        config: serde_json::to_string(&pageserver_api::models::TenantConfig::default()).unwrap(),
        scheduling_policy: serde_json::to_string(&ShardSchedulingPolicy::default()).unwrap(),
        preferred_az_id: None,
    }
}

fn node_row(id: i64) -> NodePersistence {
    NodePersistence {
        node_id: id,
        scheduling_policy: String::from(NodeSchedulingPolicy::Active),
        listen_http_addr: format!("pageserver-{id}.example.com"),
        listen_http_port: 9898,
        listen_pg_addr: format!("pageserver-{id}.example.com"),
        listen_pg_port: 6400,
        availability_zone_id: "zone-a".to_string(),
        listen_https_port: None,
        lifecycle: String::from(pageserver_api::controller_api::NodeLifecycle::Active),
        listen_grpc_addr: None,
        listen_grpc_port: None,
    }
}

fn node(id: i64) -> crate::node::Node {
    crate::node::Node::from_persistent(node_row(id), false).unwrap()
}

/// A shard row as a trace line, with what the stores must agree on.
fn show_shard(s: &TenantShardPersistence) -> String {
    format!(
        "{} {}/{} gen={:?} ps={:?} placement={} split={:?} sched={} az={:?}",
        s.tenant_id,
        s.shard_number,
        s.shard_count,
        s.generation,
        s.generation_pageserver,
        s.placement_policy,
        s.splitting,
        s.scheduling_policy,
        s.preferred_az_id
    )
}

fn show_shards(mut rows: Vec<TenantShardPersistence>) -> Vec<String> {
    rows.sort_by(|a, b| {
        (&a.tenant_id, a.shard_count, a.shard_number).cmp(&(
            &b.tenant_id,
            b.shard_count,
            b.shard_number,
        ))
    });
    rows.iter().map(show_shard).collect()
}

fn show_node(n: &NodePersistence) -> String {
    format!(
        "node {} sched={} lifecycle={} https={:?} grpc={:?}:{:?}",
        n.node_id,
        n.scheduling_policy,
        n.lifecycle,
        n.listen_https_port,
        n.listen_grpc_addr,
        n.listen_grpc_port
    )
}

fn show_nodes(mut v: Vec<NodePersistence>) -> Vec<String> {
    v.sort_by_key(|n| n.node_id);
    v.iter().map(show_node).collect()
}

fn show_err<T>(r: &DatabaseResult<T>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(e) if e.is_unique_violation() => "exists".to_string(),
        Err(DatabaseError::Cas(_)) => "cas".to_string(),
        Err(DatabaseError::Logical(_)) => "logical".to_string(),
        Err(e) => format!("other: {e}"),
    }
}

#[tokio::test]
async fn pageservers() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        trace.push(show_err(&p.insert_node(&node(1)).await));
        trace.push(show_err(&p.insert_node(&node(2)).await));
        // Registering the same id again is refused
        trace.push(show_err(&p.insert_node(&node(1)).await));
        p.update_node_scheduling_policy(NodeId(1), NodeSchedulingPolicy::Pause)
            .await
            .unwrap();
        p.update_node_on_registration(
            NodeId(1),
            Some(443),
            Some("grpc.example.com".into()),
            Some(51051),
        )
        .await
        .unwrap();
        // A node that is still active cannot be hard deleted
        trace.push(show_err(&p.delete_node(NodeId(2)).await));
        p.set_tombstone(NodeId(2)).await.unwrap();
        // A tombstoned node is not updated
        trace.push(show_err(
            &p.update_node_scheduling_policy(NodeId(2), NodeSchedulingPolicy::Active)
                .await,
        ));
        trace.extend(show_nodes(p.list_nodes().await.unwrap()));
        trace.extend(show_nodes(p.list_tombstones().await.unwrap()));
        trace.push(show_err(&p.delete_node(NodeId(2)).await));
        // Deleting a node that is not there is not an error
        trace.push(show_err(&p.delete_node(NodeId(2)).await));
        trace.push(format!(
            "tombstones {}",
            p.list_tombstones().await.unwrap().len()
        ));
        trace
    })
    .await;
}

#[tokio::test]
async fn generations() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        p.insert_node(&node(1)).await.unwrap();
        p.insert_node(&node(2)).await.unwrap();
        p.insert_tenant_shards(vec![shard_row(TENANT, 0, 2), shard_row(TENANT, 1, 2)])
            .await
            .unwrap();
        p.insert_tenant_shards(vec![shard_row(TENANT_B, 0, 0)])
            .await
            .unwrap();
        // The same shard twice is refused, as creation's retry relies on
        trace.push(show_err(
            &p.insert_tenant_shards(vec![shard_row(TENANT, 0, 2)]).await,
        ));

        // Every attach takes a new, higher generation
        let a = shard_id(TENANT, 0, 2);
        let b = shard_id(TENANT, 1, 2);
        let c = shard_id(TENANT_B, 0, 0);
        let g1 = p.increment_generation(a, NodeId(1)).await.unwrap();
        let g2 = p.increment_generation(a, NodeId(1)).await.unwrap();
        assert!(g2 > g1);
        p.increment_generation(b, NodeId(2)).await.unwrap();
        p.increment_generation(c, NodeId(1)).await.unwrap();
        trace.push(format!("{g1:?} {g2:?}"));
        trace.push(format!(
            "{:?}",
            p.increment_generation(shard_id(TENANT, 3, 4), NodeId(1))
                .await
                .is_err()
        ));

        for s in p.tenant_generations(tenant(TENANT)).await.unwrap() {
            trace.push(format!(
                "{} {:?} {:?}",
                s.tenant_shard_id, s.generation, s.generation_pageserver
            ));
        }
        let mut gens = p
            .shard_generations([a, c, shard_id(TENANT, 5, 8)].iter())
            .await
            .unwrap();
        gens.sort_by_key(|(id, _)| *id);
        trace.push(format!("{gens:?}"));

        // A re-attach takes a new generation for every shard on that pageserver, and only those
        p.update_node_scheduling_policy(NodeId(1), NodeSchedulingPolicy::PauseForRestart)
            .await
            .unwrap();
        let before = p.tenant_generations(tenant(TENANT)).await.unwrap();
        let mut reattached: Vec<_> = p.re_attach(NodeId(1)).await.unwrap().into_iter().collect();
        reattached.sort_by_key(|(id, _)| *id);
        trace.push(format!("re-attach {reattached:?}"));
        let after = p.tenant_generations(tenant(TENANT)).await.unwrap();
        assert!(after[0].generation > before[0].generation);
        assert_eq!(after[1].generation, before[1].generation);
        trace.extend(show_nodes(p.list_nodes().await.unwrap()));
        // A deleted pageserver may not re-attach
        p.set_tombstone(NodeId(2)).await.unwrap();
        trace.push(show_err(&p.re_attach(NodeId(2)).await));
        // A pageserver the controller does not know re-attaches to nothing
        trace.push(format!("{:?}", p.re_attach(NodeId(9)).await.unwrap()));

        // Detaching keeps the generation and forgets the pageserver
        p.update_tenant_shard(
            TenantFilter::Shard(b),
            Some(PlacementPolicy::Secondary),
            None,
            None,
            Some(ShardSchedulingPolicy::Pause),
        )
        .await
        .unwrap();
        p.detach(c).await.unwrap();
        p.update_tenant_shard(
            TenantFilter::Tenant(tenant(TENANT)),
            None,
            None,
            Some(Generation::new(40)),
            None,
        )
        .await
        .unwrap();
        // An update of a shard that is not there does nothing
        p.update_tenant_shard(
            TenantFilter::Shard(shard_id(TENANT, 7, 8)),
            Some(PlacementPolicy::Detached),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let azs = p
            .set_tenant_shard_preferred_azs(vec![
                (a, Some(AvailabilityZone("zone-b".to_string()))),
                (
                    shard_id(TENANT, 9, 16),
                    Some(AvailabilityZone("zone-c".to_string())),
                ),
            ])
            .await
            .unwrap();
        trace.push(format!("{azs:?}"));
        trace.extend(show_shards(p.load_active_tenant_shards().await.unwrap()));
        trace.extend(show_shards(p.load_tenant(tenant(TENANT_B)).await.unwrap()));

        p.delete_tenant(tenant(TENANT_B)).await.unwrap();
        trace.push(format!(
            "after delete {}",
            p.load_tenant(tenant(TENANT_B)).await.unwrap().len()
        ));
        trace.push(format!(
            "{}",
            p.tenant_generations(tenant(TENANT_B)).await.unwrap().len()
        ));
        trace
    })
    .await;
}

#[tokio::test]
async fn shard_splits() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        p.insert_node(&node(1)).await.unwrap();
        p.insert_tenant_shards(vec![shard_row(TENANT, 0, 0)])
            .await
            .unwrap();
        let parent = shard_id(TENANT, 0, 0);
        p.increment_generation(parent, NodeId(1)).await.unwrap();
        p.increment_generation(parent, NodeId(1)).await.unwrap();

        let children = |count: i32| {
            (0..count)
                .map(|n| TenantShardPersistence {
                    splitting: SplitState::Splitting,
                    generation: None,
                    ..shard_row(TENANT, n, count)
                })
                .collect::<Vec<_>>()
        };
        let old = ShardCount::new(0);
        let new = ShardCount::new(2);

        p.begin_shard_split(old, tenant(TENANT), vec![(parent, children(2))])
            .await
            .unwrap();
        // The children carry the parent's generation
        trace.extend(show_shards(p.load_tenant(tenant(TENANT)).await.unwrap()));
        // A second split of the same parents is refused
        trace.push(show_err(
            &p.begin_shard_split(old, tenant(TENANT), vec![(parent, children(2))])
                .await,
        ));
        // A child may take a generation while the split is under way
        p.increment_generation(shard_id(TENANT, 1, 2), NodeId(1))
            .await
            .unwrap();

        let aborted = p.abort_shard_split(tenant(TENANT), new).await.unwrap();
        trace.push(format!(
            "{}",
            matches!(aborted, AbortShardSplitStatus::Aborted)
        ));
        trace.extend(show_shards(p.load_tenant(tenant(TENANT)).await.unwrap()));

        p.begin_shard_split(old, tenant(TENANT), vec![(parent, children(2))])
            .await
            .unwrap();
        p.complete_shard_split(tenant(TENANT), old, new)
            .await
            .unwrap();
        trace.extend(show_shards(p.load_tenant(tenant(TENANT)).await.unwrap()));
        let aborted = p.abort_shard_split(tenant(TENANT), new).await.unwrap();
        trace.push(format!(
            "{}",
            matches!(aborted, AbortShardSplitStatus::Complete)
        ));
        // Completing again finds no children of that count to complete
        trace.push(show_err(
            &p.complete_shard_split(tenant(TENANT), new, ShardCount::new(4))
                .await,
        ));
        // The parent's health record went with it
        let mut health: Vec<_> = p
            .list_metadata_health_records()
            .await
            .unwrap()
            .into_iter()
            .map(|h| (h.tenant_id, h.shard_number, h.shard_count, h.healthy))
            .collect();
        health.sort();
        trace.push(format!("{health:?}"));
        trace
    })
    .await;
}

#[tokio::test]
async fn metadata_health() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        p.insert_tenant_shards(vec![shard_row(TENANT, 0, 2), shard_row(TENANT, 1, 2)])
            .await
            .unwrap();
        let start = chrono::Utc::now();
        let later = start + chrono::Duration::hours(1);
        p.update_metadata_health_records(
            vec![MetadataHealthPersistence::new(
                shard_id(TENANT, 0, 2),
                true,
                later,
            )],
            vec![MetadataHealthPersistence::new(
                shard_id(TENANT, 1, 2),
                false,
                later,
            )],
            later,
        )
        .await
        .unwrap();
        let show = |v: Vec<MetadataHealthPersistence>| {
            let mut v: Vec<_> = v
                .into_iter()
                .map(|h| {
                    format!(
                        "{}/{}/{} {}",
                        h.tenant_id, h.shard_number, h.shard_count, h.healthy
                    )
                })
                .collect();
            v.sort();
            v
        };
        trace.extend(show(p.list_metadata_health_records().await.unwrap()));
        trace.extend(show(
            p.list_unhealthy_metadata_health_records().await.unwrap(),
        ));
        trace.extend(show(
            p.list_outdated_metadata_health_records(start)
                .await
                .unwrap(),
        ));
        trace.push(format!(
            "{}",
            p.list_outdated_metadata_health_records(later + chrono::Duration::seconds(1))
                .await
                .unwrap()
                .len()
        ));
        p.delete_tenant(tenant(TENANT)).await.unwrap();
        trace.push(format!(
            "{}",
            p.list_metadata_health_records().await.unwrap().len()
        ));
        trace
    })
    .await;
}

#[tokio::test]
async fn leader() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        trace.push(format!("{:?}", p.get_leader().await.unwrap()));
        let first = ControllerPersistence {
            address: "http://controller-0.example.com:1234".to_string(),
            started_at: chrono::Utc::now(),
        };
        p.update_leader(None, first.clone()).await.unwrap();
        let stored = p.get_leader().await.unwrap().unwrap();
        trace.push(stored.address.clone());
        // Postgres keeps the time to the microsecond, and so does the Kubernetes store
        assert_eq!(
            stored.started_at,
            kubernetes::objects::round_to_micros(first.started_at)
        );
        let second = ControllerPersistence {
            address: "http://controller-1.example.com:1234".to_string(),
            started_at: chrono::Utc::now(),
        };
        // Taking over from a leader that is not the current one is refused
        let wrong = ControllerPersistence {
            address: "http://controller-9.example.com:1234".to_string(),
            ..stored.clone()
        };
        trace.push(show_err(
            &p.update_leader(Some(wrong), second.clone()).await,
        ));
        p.update_leader(Some(stored), second.clone()).await.unwrap();
        trace.push(p.get_leader().await.unwrap().unwrap().address);
        trace
    })
    .await;
}

fn safekeeper_upsert(id: i64, version: i64) -> SafekeeperUpsert {
    SafekeeperUpsert {
        id,
        region_id: "region-1".to_string(),
        version,
        host: format!("safekeeper-{id}.example.com"),
        port: 5454,
        active: None,
        http_port: 7676,
        https_port: None,
        availability_zone_id: "zone-a".to_string(),
    }
}

#[tokio::test]
async fn safekeepers() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        let show = |v: Vec<SafekeeperPersistence>| {
            let mut v: Vec<_> = v
                .into_iter()
                .map(|s| {
                    format!(
                        "{} v{} {} {}",
                        s.id,
                        s.version,
                        s.host,
                        String::from(s.scheduling_policy.0)
                    )
                })
                .collect();
            v.sort();
            v
        };
        p.safekeeper_upsert(safekeeper_upsert(1, 1)).await.unwrap();
        p.safekeeper_upsert(safekeeper_upsert(2, 1)).await.unwrap();
        trace.push(show_err(
            &p.safekeeper_upsert(safekeeper_upsert(3, -1)).await,
        ));
        trace.extend(show(p.list_safekeepers().await.unwrap()));
        p.set_safekeeper_scheduling_policy(1, SkSchedulingPolicy::Pause)
            .await
            .unwrap();
        // An upsert does not change the scheduling policy
        p.safekeeper_upsert(safekeeper_upsert(1, 7)).await.unwrap();
        trace.push(format!("{:?}", p.activate_safekeeper(1).await.unwrap()));
        trace.push(format!("{:?}", p.activate_safekeeper(2).await.unwrap()));
        trace.push(format!("{:?}", p.activate_safekeeper(5).await.unwrap()));
        trace.push(show_err(
            &p.set_safekeeper_scheduling_policy(5, SkSchedulingPolicy::Active)
                .await,
        ));
        trace.extend(show(p.list_safekeepers().await.unwrap()));
        trace
    })
    .await;
}

fn timeline_row(t: &str, tl: &str) -> TimelinePersistence {
    TimelinePersistence {
        tenant_id: t.to_string(),
        timeline_id: tl.to_string(),
        start_lsn: LsnWrapper(Lsn(0x16B3748)),
        generation: 1,
        sk_set: vec![1, 2, 3],
        new_sk_set: None,
        cplane_notified_generation: 1,
        deleted_at: None,
        sk_set_notified_generation: 1,
    }
}

fn op(
    t: &str,
    tl: &str,
    sk: i64,
    generation: i32,
    kind: SafekeeperTimelineOpKind,
) -> TimelinePendingOpPersistence {
    TimelinePendingOpPersistence {
        sk_id: sk,
        tenant_id: t.to_string(),
        timeline_id: tl.to_string(),
        generation,
        op_kind: kind,
    }
}

fn show_timeline(t: &TimelinePersistence) -> String {
    format!(
        "{}/{} lsn={} gen={} set={:?} new={:?} cplane={} skset={} deleted={}",
        t.tenant_id,
        t.timeline_id,
        t.start_lsn.0,
        t.generation,
        t.sk_set,
        t.new_sk_set,
        t.cplane_notified_generation,
        t.sk_set_notified_generation,
        t.deleted_at.is_some()
    )
}

fn show_ops(v: Vec<TimelinePendingOpPersistence>) -> Vec<String> {
    let mut v: Vec<_> = v
        .into_iter()
        .map(|o| {
            format!(
                "op {}/{:?} sk={} gen={} {:?}",
                o.tenant_id, o.timeline_id, o.sk_id, o.generation, o.op_kind
            )
        })
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn timelines_on_safekeepers() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        let (t, tl) = (tenant(TENANT), timeline(TIMELINE));
        trace.push(format!(
            "{:?}",
            p.insert_timeline(timeline_row(TENANT, TIMELINE))
                .await
                .unwrap()
        ));
        // A second insert writes nothing
        trace.push(format!(
            "{:?}",
            p.insert_timeline(timeline_row(TENANT, TIMELINE))
                .await
                .unwrap()
        ));
        trace.push(format!(
            "{:?}",
            p.update_timeline_unsafe(TimelineUpdate {
                tenant_id: TENANT.to_string(),
                timeline_id: TIMELINE.to_string(),
                start_lsn: LsnWrapper(Lsn(0x2000000)),
                sk_set: vec![1, 2, 4],
                new_sk_set: Some(vec![4]),
            })
            .await
            .unwrap()
        ));

        // A membership change is a compare-and-swap on the generation
        let g2 = SafekeeperGeneration::new(2);
        let g3 = SafekeeperGeneration::new(3);
        trace.push(show_err(
            &p.update_timeline_membership(t, tl, g3, &[NodeId(1)], None, &[])
                .await,
        ));
        p.update_timeline_membership(
            t,
            tl,
            g2,
            &[NodeId(1), NodeId(2), NodeId(4)],
            None,
            &[
                op(TENANT, TIMELINE, 3, 2, SafekeeperTimelineOpKind::Exclude),
                op(TENANT, TIMELINE, 4, 2, SafekeeperTimelineOpKind::Pull),
            ],
        )
        .await
        .unwrap();
        // An operation is replaced only by one of a higher generation
        trace.push(format!(
            "{:?}",
            p.insert_pending_op(op(TENANT, TIMELINE, 4, 1, SafekeeperTimelineOpKind::Delete))
                .await
                .unwrap()
        ));
        trace.push(format!(
            "{:?}",
            p.insert_pending_op(op(TENANT, TIMELINE, 4, 5, SafekeeperTimelineOpKind::Delete))
                .await
                .unwrap()
        ));
        // A tenant-wide operation
        p.insert_pending_op(op(
            TENANT,
            "",
            1,
            i32::MAX,
            SafekeeperTimelineOpKind::Delete,
        ))
        .await
        .unwrap();
        trace.extend(show_ops(
            p.list_pending_ops_for_timeline(t, tl).await.unwrap(),
        ));

        // The notified generations only go forward
        trace.push(show_err(
            &p.update_cplane_notified_generation(t, tl, g2).await,
        ));
        trace.push(show_err(
            &p.update_cplane_notified_generation(t, tl, SafekeeperGeneration::new(1))
                .await,
        ));
        trace.push(show_err(
            &p.update_sk_set_notified_generation(t, tl, g2).await,
        ));
        trace.push(show_err(
            &p.update_cplane_notified_generation(
                t,
                timeline("aa200bd42b49cc1814412c7e592dd6e9"),
                g2,
            )
            .await,
        ));
        trace.push(show_timeline(
            &p.get_timeline(t, tl).await.unwrap().unwrap(),
        ));

        // A timeline is deleted only once marked deleted; its pending operations outlive it
        p.delete_timeline(t, tl).await.unwrap();
        trace.push(format!(
            "{}",
            p.get_timeline(t, tl).await.unwrap().is_some()
        ));
        p.timeline_set_deleted_at(t, tl).await.unwrap();
        trace.extend(
            p.list_timelines_for_tenant(t)
                .await
                .unwrap()
                .iter()
                .map(show_timeline),
        );
        p.delete_timeline(t, tl).await.unwrap();
        trace.push(format!(
            "{}",
            p.get_timeline(t, tl).await.unwrap().is_some()
        ));
        let mut with: Vec<_> = p
            .list_pending_ops_with_timelines()
            .await
            .unwrap()
            .into_iter()
            .map(|(o, t)| format!("{:?} {}", show_ops(vec![o]), t.is_some()))
            .collect();
        with.sort();
        trace.extend(with);

        p.remove_pending_op(t, Some(tl), NodeId(4), 4)
            .await
            .unwrap();
        p.remove_pending_op(t, Some(tl), NodeId(4), 5)
            .await
            .unwrap();
        trace.extend(show_ops(
            p.list_pending_ops_for_timeline(t, tl).await.unwrap(),
        ));
        p.remove_pending_ops_for_timeline(t, Some(tl))
            .await
            .unwrap();
        p.remove_pending_ops_for_timeline(t, None).await.unwrap();
        trace.push(format!(
            "{}",
            p.list_pending_ops_with_timelines().await.unwrap().len()
        ));
        trace.push(format!(
            "{}",
            p.list_timelines_for_tenant(t).await.unwrap().len()
        ));
        trace
    })
    .await;
}

#[tokio::test]
async fn timeline_imports() {
    on_every_store(|p| async move {
        let mut trace = Vec::new();
        let (t, tl) = (tenant(TENANT), timeline(TIMELINE));
        let shards = vec![
            ShardIndex::new(ShardNumber(0), ShardCount::new(2)),
            ShardIndex::new(ShardNumber(1), ShardCount::new(2)),
        ];
        let import = TimelineImportPersistence {
            tenant_id: TENANT.to_string(),
            timeline_id: TIMELINE.to_string(),
            shard_statuses: serde_json::to_value(ShardImportStatuses::new(shards)).unwrap(),
        };
        trace.push(format!(
            "{}",
            p.is_tenant_importing_timeline(t).await.unwrap()
        ));
        trace.push(format!(
            "{:?}",
            p.insert_timeline_import(import.clone()).await.unwrap()
        ));
        trace.push(show_err(&p.insert_timeline_import(import.clone()).await));
        trace.push(format!(
            "{}",
            p.is_tenant_importing_timeline(t).await.unwrap()
        ));
        let status = |r: Result<
            Option<TimelineImport>,
            crate::timeline_import::TimelineImportUpdateError,
        >| match r {
            Ok(Some(i)) => format!("persisted complete={}", i.is_complete()),
            Ok(None) => "unchanged".to_string(),
            Err(_) => "refused".to_string(),
        };
        let s0 = shard_id(TENANT, 0, 2);
        let s1 = shard_id(TENANT, 1, 2);
        trace.push(status(
            p.update_timeline_import(s0, tl, ShardImportStatus::Done)
                .await
                .unwrap(),
        ));
        trace.push(status(
            p.update_timeline_import(s0, tl, ShardImportStatus::Done)
                .await
                .unwrap(),
        ));
        trace.push(status(
            p.update_timeline_import(s0, tl, ShardImportStatus::Error("late".into()))
                .await
                .unwrap(),
        ));
        trace.push(status(
            p.update_timeline_import(shard_id(TENANT, 3, 4), tl, ShardImportStatus::Done)
                .await
                .unwrap(),
        ));
        trace.push(status(
            p.update_timeline_import(s1, tl, ShardImportStatus::Done)
                .await
                .unwrap(),
        ));
        trace.push(status(
            p.update_timeline_import(
                s1,
                timeline("aa200bd42b49cc1814412c7e592dd6e9"),
                ShardImportStatus::Done,
            )
            .await
            .unwrap(),
        ));
        let got = p.get_timeline_import(t, tl).await.unwrap().unwrap();
        trace.push(format!("complete={}", got.is_complete()));
        trace.push(format!(
            "{}",
            p.list_timeline_imports().await.unwrap().len()
        ));
        p.delete_timeline_import(t, tl).await.unwrap();
        trace.push(format!(
            "{}",
            p.get_timeline_import(t, tl).await.unwrap().is_some()
        ));
        trace.push(format!(
            "{}",
            p.is_tenant_importing_timeline(t).await.unwrap()
        ));
        trace
    })
    .await;
}

/// Many attaches of one shard at once, through one controller's store and through two (two
/// controllers on the same records, as during a handover): every generation handed out is
/// different, and the last is the count of attaches.
#[tokio::test]
async fn concurrent_attaches_never_share_a_generation() {
    for (name, p) in stores().await {
        let other = match name {
            "kubernetes" => {
                let Store::Kubernetes(k) = &p.store else {
                    unreachable!()
                };
                Arc::new(Persistence {
                    store: Store::Kubernetes(k.second_client()),
                })
            }
            _ => p.clone(),
        };
        p.insert_tenant_shards(vec![shard_row(TENANT, 0, 0)])
            .await
            .unwrap();
        let id = shard_id(TENANT, 0, 0);
        let attaches = 40;
        let tasks: Vec<_> = (0..attaches)
            .map(|i| {
                let p = if i % 2 == 0 { p.clone() } else { other.clone() };
                tokio::spawn(
                    async move { p.increment_generation(id, NodeId(1 + i % 3)).await.unwrap() },
                )
            })
            .collect();
        let mut seen = BTreeMap::new();
        for t in tasks {
            let g = t.await.unwrap();
            assert!(
                seen.insert(g, ()).is_none(),
                "{name}: generation {g:?} handed out twice"
            );
        }
        let last = p.tenant_generations(tenant(TENANT)).await.unwrap()[0].generation;
        assert_eq!(last, Some(Generation::new(attaches as u32)), "{name}");
        assert_eq!(seen.len(), attaches as usize, "{name}");
    }
}

/// A re-attach of a pageserver holding many tenants takes a new generation for every one, while
/// attaches of some of them to other pageservers run at the same time: a shard moved away before
/// the re-attach read it keeps its new pageserver, and no generation repeats.
#[tokio::test]
async fn re_attach_races_attaches() {
    for (name, p) in stores().await {
        p.insert_node(&node(1)).await.unwrap();
        let tenants: Vec<String> = (0..60)
            .map(|i| format!("{i:08x}{}", &TENANT[8..]))
            .collect();
        for t in &tenants {
            p.insert_tenant_shards(vec![shard_row(t, 0, 0)])
                .await
                .unwrap();
            p.increment_generation(shard_id(t, 0, 0), NodeId(1))
                .await
                .unwrap();
        }
        let moving: Vec<_> = tenants.iter().step_by(5).cloned().collect();
        let mover = {
            let p = p.clone();
            let moving = moving.clone();
            tokio::spawn(async move {
                let mut out = Vec::new();
                for t in moving {
                    out.push((
                        t.clone(),
                        p.increment_generation(shard_id(&t, 0, 0), NodeId(2))
                            .await
                            .unwrap(),
                    ));
                }
                out
            })
        };
        let reattached = p.re_attach(NodeId(1)).await.unwrap();
        let moved = mover.await.unwrap();
        for t in &tenants {
            let now = &p.tenant_generations(tenant(t)).await.unwrap()[0];
            let id = shard_id(t, 0, 0);
            match (reattached.get(&id), moved.iter().find(|(m, _)| m == t)) {
                (Some(r), Some((_, m))) => {
                    assert_ne!(r, m, "{name}: {t} has one generation for two pageservers");
                    assert_eq!(now.generation, Some(*r.max(m)), "{name}: {t}");
                }
                (Some(r), None) => assert_eq!(now.generation, Some(*r), "{name}: {t}"),
                (None, Some((_, m))) => {
                    assert_eq!(now.generation, Some(*m), "{name}: {t}");
                    assert_eq!(now.generation_pageserver, Some(NodeId(2)), "{name}: {t}");
                }
                (None, None) => panic!("{name}: {t} was neither re-attached nor moved"),
            }
        }
    }
}
