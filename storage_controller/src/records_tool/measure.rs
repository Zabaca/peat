//! The scale measure of `storage_controller/kubernetes/measure.md`: a storage server's restart
//! with many databases on it, timed on the Kubernetes records (and on Postgres, given one).

use std::time::{Duration, Instant};

use anyhow::Context;
use futures::StreamExt;
use pageserver_api::controller_api::{NodeLifecycle, NodeSchedulingPolicy, PlacementPolicy};
use pageserver_api::shard::{ShardCount, ShardNumber, TenantShardId};
use remote_storage::RemoteStorageConfig;
use utils::id::{NodeId, TenantId};

use crate::persistence::kubernetes::copy::BucketCopy;
use crate::persistence::split_state::SplitState;
use crate::persistence::{KubernetesRecords, NodePersistence, Persistence, TenantShardPersistence};

/// What to measure.
pub struct Measure {
    /// Databases (tenants of one shard) on the storage server.
    pub tenants: usize,
    /// The API server's kubeconfig; by default the pod's account, or `KUBECONFIG`.
    pub kubeconfig: Option<std::path::PathBuf>,
    /// An empty Postgres database to measure the same on, for comparison.
    pub database_url: Option<String>,
    /// Leave the measure's namespace and its records in place.
    pub keep: bool,
}

/// How many of the fill's operations run at once.
const FILL_AT_ONCE: usize = 32;

/// What one store took.
struct Timed {
    fill: Duration,
    re_attach: Duration,
    /// When the re-attach ended.
    ended: Instant,
}

/// Measure, and say what was measured: one line per store.
pub async fn measure_restart(m: &Measure) -> anyhow::Result<Vec<String>> {
    let mut lines = Vec::new();
    let mut config = match &m.kubeconfig {
        Some(path) => {
            kube::Config::from_custom_kubeconfig(
                kube::config::Kubeconfig::read_from(path)?,
                &Default::default(),
            )
            .await?
        }
        None => kube::Config::infer().await?,
    };
    crate::persistence::kubernetes::skip_proxy_where_excluded(&mut config);
    let client = kube::Client::try_from(crate::persistence::kubernetes::client_config(config))?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    let namespace = format!("storcon-measure-{}", &id[..12]);
    let namespaces = kube::Api::<k8s_openapi::api::core::v1::Namespace>::all(client.clone());
    namespaces
        .create(
            &Default::default(),
            &k8s_openapi::api::core::v1::Namespace {
                metadata: kube::api::ObjectMeta {
                    name: Some(namespace.clone()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("making the namespace {namespace}"))?;
    let bucket = std::env::temp_dir().join(format!("storcon-measure-{id}"));
    std::fs::create_dir_all(&bucket)?;
    let copy_storage =
        RemoteStorageConfig::from_toml_str(&format!("local_path = '{}'", bucket.display()))?;

    let measured = async {
        let p = Persistence::new_kubernetes(
            m.kubeconfig.as_deref(),
            Some(namespace.clone()),
            KubernetesRecords {
                copy_storage: Some(copy_storage.clone()),
                ..Default::default()
            },
        )
        .await?;
        p.start_checks().await?;
        p.migration_run().await?;
        let t = timed_restart(&p, m.tenants).await?;
        // The copy: how long after the re-attach it held every new generation, its size, and
        // how long one write of it takes
        let copy = BucketCopy::new(&copy_storage).await?;
        let after = p.read_all_records().await?;
        let behind = loop {
            if let Some(doc) = copy.read().await? {
                if doc.records.diff(&after)?.is_empty() {
                    break t.ended.elapsed();
                }
            }
            anyhow::ensure!(
                t.ended.elapsed() < Duration::from_secs(600),
                "the copy did not catch up within ten minutes of the re-attach"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        p.step_down().await;
        let doc = copy.read().await?.context("the copy is gone")?;
        let size = serde_json::to_vec(&doc)?.len();
        let started = Instant::now();
        copy.write(&doc).await?;
        let write = started.elapsed();
        anyhow::Ok(format!(
            "kubernetes, {} tenants: fill {:.1} s; re-attach {:.2} s ({:.0} tenants/s); copy \
             {size} bytes, one write {} ms, caught up {:.1} s after the re-attach",
            m.tenants,
            t.fill.as_secs_f64(),
            t.re_attach.as_secs_f64(),
            m.tenants as f64 / t.re_attach.as_secs_f64(),
            write.as_millis(),
            behind.as_secs_f64(),
        ))
    }
    .await;
    if !m.keep {
        namespaces
            .delete(&namespace, &Default::default())
            .await
            .with_context(|| format!("deleting the namespace {namespace}"))?;
        std::fs::remove_dir_all(&bucket).ok();
    }
    lines.push(measured?);

    if let Some(url) = &m.database_url {
        let p = Persistence::new(url.clone()).await;
        p.migration_run().await?;
        anyhow::ensure!(
            p.load_active_tenant_shards().await?.is_empty() && p.list_nodes().await?.is_empty(),
            "the Postgres database to measure on is not empty"
        );
        let t = timed_restart(&p, m.tenants).await?;
        lines.push(format!(
            "postgres, {} tenants: fill {:.1} s; re-attach {:.2} s ({:.0} tenants/s)",
            m.tenants,
            t.fill.as_secs_f64(),
            t.re_attach.as_secs_f64(),
            m.tenants as f64 / t.re_attach.as_secs_f64(),
        ));
    }
    Ok(lines)
}

/// `tenants` tenants of one shard each, attached to pageserver 1 through the controller's own
/// operations (the fill), then pageserver 1's re-attach, timed.
async fn timed_restart(p: &Persistence, tenants: usize) -> anyhow::Result<Timed> {
    let node = NodePersistence {
        node_id: 1,
        scheduling_policy: String::from(NodeSchedulingPolicy::Active),
        listen_http_addr: "pageserver-1.example.com".to_string(),
        listen_http_port: 9898,
        listen_pg_addr: "pageserver-1.example.com".to_string(),
        listen_pg_port: 6400,
        availability_zone_id: "zone-a".to_string(),
        listen_https_port: None,
        lifecycle: String::from(NodeLifecycle::Active),
        listen_grpc_addr: None,
        listen_grpc_port: None,
    };
    p.insert_node(&crate::node::Node::from_persistent(node, false)?)
        .await?;
    let started = Instant::now();
    let results: Vec<anyhow::Result<()>> = futures::stream::iter((0..tenants).map(|_| async {
        let tenant_id = TenantId::generate();
        p.insert_tenant_shards(vec![TenantShardPersistence {
            tenant_id: tenant_id.to_string(),
            shard_number: 0,
            shard_count: 0,
            shard_stripe_size: pageserver_api::shard::DEFAULT_STRIPE_SIZE.0 as i32,
            generation: Some(0),
            generation_pageserver: None,
            placement_policy: serde_json::to_string(&PlacementPolicy::Attached(0))?,
            splitting: SplitState::default(),
            config: serde_json::to_string(&pageserver_api::models::TenantConfig::default())?,
            scheduling_policy: serde_json::to_string(
                &pageserver_api::controller_api::ShardSchedulingPolicy::default(),
            )?,
            preferred_az_id: None,
        }])
        .await?;
        let id = TenantShardId {
            tenant_id,
            shard_number: ShardNumber(0),
            shard_count: ShardCount::new(0),
        };
        p.increment_generation(id, NodeId(1)).await?;
        Ok(())
    }))
    .buffer_unordered(FILL_AT_ONCE)
    .collect()
    .await;
    results.into_iter().collect::<anyhow::Result<Vec<()>>>()?;
    let fill = started.elapsed();
    let started = Instant::now();
    let given = p.re_attach(NodeId(1)).await?;
    let re_attach = started.elapsed();
    anyhow::ensure!(
        given.len() == tenants,
        "{} of {tenants} tenants re-attached",
        given.len()
    );
    Ok(Timed {
        fill,
        re_attach,
        ended: Instant::now(),
    })
}
