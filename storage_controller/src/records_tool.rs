//! `storcon_records`, the copy tool of `storage_controller/kubernetes/README.md`: dump, compare
//! and copy the controller's records between its two stores, and rebuild them from the copy in
//! the bucket, always with the controller stopped.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use pageserver_api::shard::TenantShardId;
use remote_storage::{GenericRemoteStorage, ListingMode, RemotePath, RemoteStorageConfig};
use tokio_util::sync::CancellationToken;
use utils::generation::Generation;
use utils::id::{TenantId, TimelineId};
use utils::logging::SecretString;

use crate::persistence::Persistence;
use crate::persistence::kubernetes::copy::BucketCopy;
use crate::persistence::records::Records;

/// One of the places the records can be read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Store {
    Postgres,
    Kubernetes,
    Bucket,
}

/// Where each store is.
#[derive(Clone, Debug, Default)]
pub struct Stores {
    /// A kubeconfig for the Kubernetes store; by default found as any client finds one.
    pub kubeconfig: Option<std::path::PathBuf>,
    pub database_url: Option<String>,
    pub records_namespace: Option<String>,
    pub records_copy_storage: Option<String>,
}

/// How a command ended, as the exit codes of the README.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Done; for `compare`, the stores are equal.
    Done,
    /// Refused before anything was written, with the reason.
    Refused(String),
    /// The stores differ, with every difference.
    Differ(Vec<String>),
}

impl Outcome {
    pub fn exit_code(&self) -> i32 {
        match self {
            Outcome::Done => 0,
            Outcome::Refused(_) => 3,
            Outcome::Differ(_) => 4,
        }
    }
}

impl Stores {
    async fn open(&self, store: Store, writing: bool) -> anyhow::Result<Persistence> {
        match store {
            Store::Postgres => {
                let url = self
                    .database_url
                    .clone()
                    .context("the postgres store needs --database-url or DATABASE_URL")?;
                Persistence::await_connection(&url, Duration::from_secs(10)).await?;
                let p = Persistence::new(url).await;
                if writing {
                    p.migration_run().await?;
                }
                Ok(p)
            }
            Store::Kubernetes => {
                let p = Persistence::new_kubernetes(
                    self.kubeconfig.as_deref(),
                    self.records_namespace.clone(),
                    None,
                )
                .await?;
                p.migration_run().await?;
                Ok(p)
            }
            Store::Bucket => anyhow::bail!("the bucket is read with read_bucket"),
        }
    }

    async fn bucket(&self) -> anyhow::Result<BucketCopy> {
        let config = self
            .records_copy_storage
            .as_deref()
            .context("the bucket needs --records-copy-storage")?;
        BucketCopy::new(&RemoteStorageConfig::from_toml_str(config)?).await
    }

    /// The records a store holds, and, for the bucket, when its copy was written.
    pub(crate) async fn read(
        &self,
        store: Store,
    ) -> anyhow::Result<(Records, Option<chrono::DateTime<chrono::Utc>>)> {
        match store {
            Store::Bucket => {
                let doc = self.read_copy().await?;
                Ok((doc.records, Some(doc.written_at)))
            }
            _ => Ok((
                self.open(store, false).await?.read_all_records().await?,
                None,
            )),
        }
    }

    async fn read_copy(
        &self,
    ) -> anyhow::Result<crate::persistence::kubernetes::copy::CopyDocument> {
        self.bucket()
            .await?
            .read()
            .await?
            .context("the bucket holds no copy of the records")
    }
}

/// Every record of a store as one JSON document.
pub async fn dump(stores: &Stores, from: Store) -> anyhow::Result<String> {
    let (records, written_at) = stores.read(from).await?;
    let mut doc = serde_json::to_value(records.normalized()?)?;
    if let Some(t) = written_at {
        doc["copyWrittenAt"] = serde_json::Value::String(t.to_rfc3339());
    }
    Ok(serde_json::to_string_pretty(&doc)?)
}

/// Every difference between two stores' records, and each side's counts.
pub async fn compare(
    stores: &Stores,
    a: Store,
    b: Store,
) -> anyhow::Result<(Outcome, BTreeMap<&'static str, (usize, usize)>)> {
    let (ra, _) = stores.read(a).await?;
    let (rb, _) = stores.read(b).await?;
    let ca = ra.counts();
    let cb = rb.counts();
    let counts = ca.iter().map(|(k, v)| (*k, (*v, cb[k]))).collect();
    let diff = ra.diff(&rb)?;
    Ok((
        if diff.is_empty() {
            Outcome::Done
        } else {
            Outcome::Differ(diff)
        },
        counts,
    ))
}

/// Copy every record of one store into the other, then compare them.
pub async fn copy(
    stores: &Stores,
    from: Store,
    to: Store,
    replace: bool,
) -> anyhow::Result<Outcome> {
    if from == Store::Bucket || to == Store::Bucket {
        return Ok(Outcome::Refused(
            "copy moves records between postgres and kubernetes; the bucket is read by restore"
                .to_string(),
        ));
    }
    if from == to {
        return Ok(Outcome::Refused(
            "copy needs two different stores".to_string(),
        ));
    }
    let source = stores.open(from, false).await?;
    let records = source.read_all_records().await?;
    if let Some(reason) = something_runs(&records).await {
        return Ok(Outcome::Refused(reason));
    }
    let dest = stores.open(to, true).await?;
    write_checked(&dest, &records, replace).await
}

/// Write `records` into `dest` and read them back.
async fn write_checked(
    dest: &Persistence,
    records: &Records,
    replace: bool,
) -> anyhow::Result<Outcome> {
    if !replace {
        let held = dest.read_all_records().await?;
        if held != Records::empty() {
            return Ok(Outcome::Refused(format!(
                "the destination is not empty ({:?}): give --replace to replace what it holds",
                held.counts()
            )));
        }
    }
    dest.write_all_records(records, replace).await?;
    let written = dest.read_all_records().await?;
    let diff = records.diff(&written)?;
    Ok(if diff.is_empty() {
        Outcome::Done
    } else {
        Outcome::Differ(diff)
    })
}

/// What a restore changed, for its report.
#[derive(Debug, Default)]
pub struct Restored {
    pub copy_written_at: Option<chrono::DateTime<chrono::Utc>>,
    pub shards_raised: usize,
    pub timelines_read_from_safekeepers: usize,
    pub timelines_no_safekeeper_answered: Vec<String>,
    pub tenants_in_store_not_in_copy: Vec<String>,
}

/// How far above the highest generation seen a restore sets each shard's.
pub const DEFAULT_GENERATION_MARGIN: u32 = 1000;

/// Rebuild the records from the copy in the bucket, raising every shard's generation above
/// anything the copy or the pageservers' store shows, and reading each timeline's safekeeper
/// membership back from its safekeepers.
pub async fn restore(
    stores: &Stores,
    to: Store,
    pageserver_storage: &str,
    safekeeper_jwt: Option<String>,
    replace: bool,
    margin: u32,
) -> anyhow::Result<(Outcome, Restored)> {
    let mut report = Restored::default();
    if to == Store::Bucket {
        return Ok((
            Outcome::Refused("restore writes into postgres or kubernetes".to_string()),
            report,
        ));
    }
    let doc = stores.read_copy().await?;
    let mut records = doc.records;
    report.copy_written_at = Some(doc.written_at);
    if let Some(reason) = something_runs(&records).await {
        return Ok((Outcome::Refused(reason), report));
    }

    // Generations: above the highest the copy holds or has ever seen, and the highest any
    // object in the pageservers' store was written with, for every shard of the tenant.
    let in_store = generations_in_store(pageserver_storage).await?;
    let mut seen = in_store.clone();
    for (tenant, g) in &doc.highest_generations {
        if let Ok(t) = TenantId::from_str(tenant) {
            let e = seen.entry(t).or_default();
            *e = (*e).max(*g as u32);
        }
    }
    for (tenant, g) in &in_store {
        if !records.tenants.contains_key(&tenant.to_string()) {
            report.tenants_in_store_not_in_copy.push(format!(
                "{tenant} (generation {g} in the store, {} the highest ever seen)",
                seen[tenant]
            ));
        }
    }
    report.shards_raised = raise_generations(&mut records, &seen, margin);

    // Safekeeper membership: the highest configuration any of the timeline's safekeepers holds.
    let (read, silent) = read_membership(&mut records, safekeeper_jwt).await;
    report.timelines_read_from_safekeepers = read;
    report.timelines_no_safekeeper_answered = silent;

    let dest = stores.open(to, true).await?;
    Ok((write_checked(&dest, &records, replace).await?, report))
}

/// Set every shard's generation above `margin` past the highest of the copy's and the store's,
/// for that shard's tenant; a shard without a generation gets one only if the store shows one.
/// How many shards were raised.
pub(crate) fn raise_generations(
    records: &mut Records,
    in_store: &BTreeMap<TenantId, u32>,
    margin: u32,
) -> usize {
    let mut raised = 0;
    for t in records.tenants.values_mut() {
        let Ok(tenant) = TenantId::from_str(&t.tenant_id) else {
            continue;
        };
        let stored = in_store.get(&tenant).copied();
        let highest_copy = t.shards.iter().filter_map(|s| s.generation).max();
        let highest = match (highest_copy, stored) {
            (None, None) => continue,
            (a, b) => a.map(|g| g as u32).max(b).unwrap_or(0),
        };
        let new = highest.saturating_add(margin) as i32;
        for s in &mut t.shards {
            if s.generation.is_some() || stored.is_some() {
                s.generation = Some(new);
                raised += 1;
            }
        }
    }
    raised
}

/// The highest generation any object under `tenants/` in the pageservers' store was written
/// with, per tenant (every key a pageserver writes carries its generation as a suffix).
pub(crate) async fn generations_in_store(config: &str) -> anyhow::Result<BTreeMap<TenantId, u32>> {
    let storage =
        GenericRemoteStorage::from_config(&RemoteStorageConfig::from_toml_str(config)?).await?;
    let cancel = CancellationToken::new();
    let prefix = RemotePath::from_string("tenants/")?;
    let mut out: BTreeMap<TenantId, u32> = BTreeMap::new();
    let mut listing = std::pin::pin!(storage.list_streaming(
        Some(&prefix),
        ListingMode::NoDelimiter,
        None,
        &cancel
    ));
    while let Some(page) = listing.next().await {
        for obj in page?.keys {
            if let Some((tenant, generation)) = key_generation(obj.key.get_path().as_str()) {
                let e = out.entry(tenant).or_default();
                *e = (*e).max(generation);
            }
        }
    }
    Ok(out)
}

/// `tenants/<tenant shard>/.../<name>-<generation, 8 hex digits>[.json]`.
pub(crate) fn key_generation(key: &str) -> Option<(TenantId, u32)> {
    let mut parts = key.split('/');
    if parts.next()? != "tenants" {
        return None;
    }
    let shard = TenantShardId::from_str(parts.next()?).ok()?;
    let name = key.rsplit('/').next()?;
    let name = name.strip_suffix(".json").unwrap_or(name);
    let (_, suffix) = name.rsplit_once('-')?;
    if suffix.len() != 8 {
        return None;
    }
    match Generation::parse_suffix(suffix)? {
        Generation::Valid(g) => Some((shard.tenant_id, g)),
        Generation::None => None,
    }
}

/// For each timeline with a membership, ask its safekeepers and keep the highest configuration
/// any of them holds. Returns how many timelines were read, and those no safekeeper answered for.
async fn read_membership(records: &mut Records, jwt: Option<String>) -> (usize, Vec<String>) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("an http client");
    let addresses: BTreeMap<i64, String> = records
        .safekeepers
        .values()
        .map(|s| (s.id, format!("http://{}:{}", s.host, s.http_port)))
        .collect();
    let mut read = 0;
    let mut silent = Vec::new();
    for t in records.timelines.values_mut() {
        let Some(m) = t.membership.as_mut() else {
            continue;
        };
        let (Ok(tenant), Ok(timeline)) = (
            TenantId::from_str(&t.tenant_id),
            TimelineId::from_str(&t.timeline_id),
        ) else {
            continue;
        };
        let mut best: Option<safekeeper_api::membership::Configuration> = None;
        let ids = m.sk_set.iter().chain(m.new_sk_set.iter().flatten());
        for id in ids {
            let Some(addr) = addresses.get(id) else {
                continue;
            };
            let client = safekeeper_client::mgmt_api::Client::new(
                http.clone(),
                addr.clone(),
                jwt.clone().map(SecretString::from),
            );
            let Ok(resp) = client.timeline_status(tenant, timeline).await else {
                continue;
            };
            let Ok(status) = resp.json::<safekeeper_api::models::TimelineStatus>().await else {
                continue;
            };
            if best
                .as_ref()
                .is_none_or(|b| status.mconf.generation > b.generation)
            {
                best = Some(status.mconf);
            }
        }
        match best {
            Some(conf) => {
                read += 1;
                let g = conf.generation.into_inner() as i32;
                if g > m.generation {
                    m.generation = g;
                    m.sk_set = conf.members.m.iter().map(|s| s.id.0 as i64).collect();
                    m.new_sk_set = conf
                        .new_members
                        .map(|n| n.m.iter().map(|s| s.id.0 as i64).collect());
                }
            }
            None => silent.push(format!("{}/{}", t.tenant_id, t.timeline_id)),
        }
    }
    (read, silent)
}

/// The controller (at the leader's address) or a pageserver (at its HTTP port) that accepts a
/// connection, named; `None` when nothing does.
async fn something_runs(records: &Records) -> Option<String> {
    let mut targets = Vec::new();
    if let Some(l) = &records.leader {
        if let Ok(url) = reqwest::Url::parse(&l.address) {
            if let (Some(host), Some(port)) = (url.host_str(), url.port_or_known_default()) {
                targets.push((
                    format!("the storage controller at {}", l.address),
                    host.to_string(),
                    port,
                ));
            }
        }
    }
    for p in records.pageservers.values() {
        if p.lifecycle == "deleted" {
            continue;
        }
        targets.push((
            format!(
                "pageserver {} at {}:{}",
                p.node_id, p.listen_http_addr, p.listen_http_port
            ),
            p.listen_http_addr.clone(),
            p.listen_http_port as u16,
        ));
    }
    let mut answered = Vec::new();
    for (what, host, port) in targets {
        let connect = tokio::net::TcpStream::connect((host.as_str(), port));
        if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), connect).await {
            answered.push(what);
        }
    }
    (!answered.is_empty()).then(|| format!("still running, stop it first: {}", answered.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kubernetes::objects::{ShardRecord, TenantSpec};
    use crate::persistence::split_state::SplitState;

    use std::sync::Arc;

    use pageserver_api::shard::ShardCount;
    use safekeeper_api::membership::SafekeeperGeneration;
    use utils::id::NodeId;

    use crate::persistence::tests::{
        TENANT, TENANT_B, TIMELINE, node_row, op, safekeeper_upsert, shard_id, shard_row, tenant,
        test_database, test_namespace, timeline, timeline_row,
    };
    use crate::persistence::{
        ControllerPersistence, SafekeeperTimelineOpKind, TenantShardPersistence,
        TimelineImportPersistence,
    };

    fn local_storage(dir: &camino::Utf8Path) -> String {
        format!("local_path = '{dir}'")
    }

    /// Records of every kind, written through the controller's own operations.
    async fn fill(p: &Persistence) {
        let node = |id| crate::node::Node::from_persistent(node_row(id), false).unwrap();
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
                splitting: crate::persistence::split_state::SplitState::Splitting,
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

    async fn wait_for_copy(
        copy: &BucketCopy,
        done: impl Fn(&Records) -> bool,
    ) -> std::time::Duration {
        let start = std::time::Instant::now();
        loop {
            if let Some(doc) = copy.read().await.unwrap() {
                if done(&doc.records) {
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

    /// Both stores, or neither (the tests then pass without running, as persistence's do).
    async fn both() -> Option<(Stores, String)> {
        let (kubeconfig, namespace) = test_namespace().await?;
        let url = test_database().await?;
        Some((
            Stores {
                kubeconfig: Some(kubeconfig),
                database_url: Some(url.clone()),
                records_namespace: Some(namespace),
                records_copy_storage: None,
            },
            url,
        ))
    }

    #[tokio::test]
    async fn the_copy_follows_the_records_within_a_second() {
        let Some((kubeconfig, namespace)) = test_namespace().await else {
            return;
        };
        let dir = camino_tempfile::tempdir().unwrap();
        let config = RemoteStorageConfig::from_toml_str(&local_storage(dir.path())).unwrap();
        let p =
            Persistence::new_kubernetes(Some(&kubeconfig), Some(namespace), Some(config.clone()))
                .await
                .unwrap();
        p.migration_run().await.unwrap();
        let copy = BucketCopy::new(&config).await.unwrap();

        fill(&p).await;
        let expected = p.read_all_records().await.unwrap();
        let took = wait_for_copy(&copy, |r| r.diff(&expected).unwrap().is_empty()).await;
        eprintln!("the copy held every record {took:?} after the last write");

        p.increment_generation(shard_id(TENANT_B, 1, 2), NodeId(1))
            .await
            .unwrap();
        let took = wait_for_copy(&copy, |r| {
            r.tenants[TENANT_B].shards[1].generation == Some(2)
        })
        .await;
        assert!(took < Duration::from_secs(1), "{took:?}");
        p.delete_tenant(tenant(TENANT_B)).await.unwrap();
        let took = wait_for_copy(&copy, |r| !r.tenants.contains_key(TENANT_B)).await;
        assert!(took < Duration::from_secs(1), "{took:?}");
        // The copy keeps the highest generation it saw of a tenant that is gone
        let doc = copy.read().await.unwrap().unwrap();
        assert_eq!(doc.highest_generations.get(TENANT_B), Some(&2));
        assert_eq!(doc.highest_generations.get(TENANT), Some(&3));

        // Every object deleted under a running controller: the copy is not overwritten with
        // nothing
        p.write_all_records(&Records::empty(), true).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let doc = copy.read().await.unwrap().unwrap();
        assert!(!doc.records.is_empty());

        // A controller whose records are empty while the copy is not refuses to start.
        let (kubeconfig, empty) = test_namespace().await.unwrap();
        let fresh = Persistence::new_kubernetes(Some(&kubeconfig), Some(empty), Some(config))
            .await
            .unwrap();
        let err = fresh.migration_run().await.unwrap_err();
        assert!(err.to_string().contains("storcon_records restore"), "{err}");
    }

    #[tokio::test]
    async fn the_tool_copies_both_ways_and_refuses_what_it_must() {
        let Some((stores, first_db)) = both().await else {
            return;
        };
        let pg = stores.open(Store::Postgres, true).await.unwrap();
        fill(&pg).await;

        assert_eq!(
            copy(&stores, Store::Postgres, Store::Kubernetes, false)
                .await
                .unwrap(),
            Outcome::Done
        );
        let (cmp, counts) = compare(&stores, Store::Postgres, Store::Kubernetes)
            .await
            .unwrap();
        assert_eq!(cmp, Outcome::Done);
        assert_eq!(counts["shards"], (5, 5));
        assert_eq!(counts["pendingOps"], (2, 2));
        // A destination that holds records is not written without --replace
        assert!(matches!(
            copy(&stores, Store::Postgres, Store::Kubernetes, false)
                .await
                .unwrap(),
            Outcome::Refused(_)
        ));
        assert_eq!(
            copy(&stores, Store::Postgres, Store::Kubernetes, true)
                .await
                .unwrap(),
            Outcome::Done
        );

        // And back, into another database
        let second = Stores {
            database_url: Some(test_database().await.unwrap()),
            ..stores.clone()
        };
        assert_eq!(
            copy(&second, Store::Kubernetes, Store::Postgres, false)
                .await
                .unwrap(),
            Outcome::Done
        );
        let first = Stores {
            database_url: Some(first_db),
            ..stores.clone()
        };
        let (a, _) = first.read(Store::Postgres).await.unwrap();
        let (b, _) = second.read(Store::Postgres).await.unwrap();
        assert_eq!(a.diff(&b).unwrap(), Vec::<String>::new());

        // A difference is found and named
        pg.increment_generation(shard_id(TENANT_B, 1, 2), NodeId(1))
            .await
            .unwrap();
        match compare(&stores, Store::Postgres, Store::Kubernetes)
            .await
            .unwrap()
            .0
        {
            Outcome::Differ(d) => assert_eq!(
                d,
                vec![format!("tenants.{TENANT_B}.shards[1].generation: 2 != 1")]
            ),
            o => panic!("{o:?}"),
        }

        // Nothing is written while a pageserver of the records answers
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let k8s = stores.open(Store::Kubernetes, true).await.unwrap();
        k8s.insert_node(
            &crate::node::Node::from_persistent(
                crate::persistence::NodePersistence {
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
        match copy(&second, Store::Kubernetes, Store::Postgres, true)
            .await
            .unwrap()
        {
            Outcome::Refused(reason) => assert!(reason.contains("pageserver 7"), "{reason}"),
            o => panic!("{o:?}"),
        }
        drop(listener);
        assert_eq!(
            copy(&second, Store::Kubernetes, Store::Postgres, true)
                .await
                .unwrap(),
            Outcome::Done
        );
    }

    #[tokio::test]
    async fn a_restore_raises_every_generation_and_is_refused_while_a_pageserver_runs() {
        let Some((kubeconfig, namespace)) = test_namespace().await else {
            return;
        };
        let bucket = camino_tempfile::tempdir().unwrap();
        let copy_config = local_storage(bucket.path());
        let config = RemoteStorageConfig::from_toml_str(&copy_config).unwrap();
        let p = Arc::new(
            Persistence::new_kubernetes(
                Some(&kubeconfig),
                Some(namespace.clone()),
                Some(config.clone()),
            )
            .await
            .unwrap(),
        );
        p.migration_run().await.unwrap();
        fill(&p).await;
        let before = p.read_all_records().await.unwrap();
        let copy_reader = BucketCopy::new(&config).await.unwrap();
        wait_for_copy(&copy_reader, |r| r.diff(&before).unwrap().is_empty()).await;

        // The pageservers' store saw a newer generation of TENANT_B than the copy holds
        let store = camino_tempfile::tempdir().unwrap();
        let index = store.path().join(format!(
            "tenants/{TENANT_B}-0102/timelines/{TIMELINE}/index_part.json-00000009"
        ));
        std::fs::create_dir_all(index.parent().unwrap()).unwrap();
        std::fs::write(&index, b"{}").unwrap();

        // The cluster loses every object, and its controller with it
        p.stop_copy();
        p.write_all_records(&Records::empty(), true).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let kept = copy_reader.read().await.unwrap().unwrap().records;
        assert_eq!(kept.diff(&before).unwrap(), Vec::<String>::new());
        let stores = Stores {
            kubeconfig: Some(kubeconfig.clone()),
            records_namespace: Some(namespace.clone()),
            records_copy_storage: Some(copy_config.clone()),
            ..Default::default()
        };

        // Refused while a pageserver of the copy answers
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut with_running = before.clone();
        let ps = with_running.pageservers.get_mut("pageserver-1").unwrap();
        ps.listen_http_addr = "127.0.0.1".to_string();
        ps.listen_http_port = listener.local_addr().unwrap().port() as i32;
        let running_bucket = camino_tempfile::tempdir().unwrap();
        let running_config = local_storage(running_bucket.path());
        BucketCopy::new(&RemoteStorageConfig::from_toml_str(&running_config).unwrap())
            .await
            .unwrap()
            .write(&with_running, &BTreeMap::new())
            .await
            .unwrap();
        let running = Stores {
            records_copy_storage: Some(running_config),
            ..stores.clone()
        };
        let (outcome, _) = restore(
            &running,
            Store::Kubernetes,
            &local_storage(store.path()),
            None,
            false,
            1000,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, Outcome::Refused(ref r) if r.contains("pageserver 1")),
            "{outcome:?}"
        );
        drop(listener);

        let (outcome, report) = restore(
            &stores,
            Store::Kubernetes,
            &local_storage(store.path()),
            None,
            false,
            1000,
        )
        .await
        .unwrap();
        assert_eq!(outcome, Outcome::Done);
        assert_eq!(report.shards_raised, 5);
        // No safekeeper of these records runs, so each timeline keeps the copy's membership
        assert_eq!(report.timelines_no_safekeeper_answered.len(), 1);
        let after = p.read_all_records().await.unwrap();
        let g = |r: &Records, t: &str| {
            r.tenants[t]
                .shards
                .iter()
                .map(|s| s.generation)
                .collect::<Vec<_>>()
        };
        // TENANT: highest in the copy 3 (the parent and its children carry it); TENANT_B: the store's 9
        assert_eq!(g(&before, TENANT), vec![Some(3), Some(3), Some(3)]);
        assert_eq!(g(&after, TENANT), vec![Some(1003), Some(1003), Some(1003)]);
        assert_eq!(g(&after, TENANT_B), vec![Some(1009), Some(1009)]);
        // Everything but the generations is as the copy holds it
        let mut same = after.clone();
        for (name, t) in &mut same.tenants {
            for (s, b) in t.shards.iter_mut().zip(&before.tenants[name].shards) {
                s.generation = b.generation;
            }
        }
        assert_eq!(same.diff(&before).unwrap(), Vec::<String>::new());

        // The controller starts on the restored records
        let restarted =
            Persistence::new_kubernetes(Some(&kubeconfig), Some(namespace), Some(config))
                .await
                .unwrap();
        restarted.migration_run().await.unwrap();
    }

    #[test]
    fn a_key_names_the_generation_it_was_written_with() {
        let t = "1f359dd625e519a1a4e8d7509690f6fc";
        let tenant = TenantId::from_str(t).unwrap();
        assert_eq!(
            key_generation(&format!(
                "tenants/{t}-0102/timelines/de200bd42b49cc1814412c7e592dd6e9/index_part.json-0000000a"
            )),
            Some((tenant, 10))
        );
        assert_eq!(
            key_generation(&format!("tenants/{t}/tenant-manifest-0000002f.json")),
            Some((tenant, 47))
        );
        assert_eq!(
            key_generation(&format!(
                "tenants/{t}/timelines/de200bd42b49cc1814412c7e592dd6e9/000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9-v1-00000003"
            )),
            Some((tenant, 3))
        );
        assert_eq!(
            key_generation(&format!("tenants/{t}/heatmap-v1.json")),
            None
        );
        assert_eq!(
            key_generation("tenants/not-a-tenant/index_part.json-00000001"),
            None
        );
        assert_eq!(key_generation(&format!("other/{t}/x-00000001")), None);
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
        let a = "1f359dd625e519a1a4e8d7509690f6fc";
        let b = "2c4f2a6b8f1e4d0c9a7b6e5d4c3b2a19";
        let c = "3d5a3b7c9f2e5d1cab8c7f6e5d4c3b2a";
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
        // The store saw a newer generation of a than the copy, and an older one of b.
        let in_store = BTreeMap::from([
            (TenantId::from_str(a).unwrap(), 12),
            (TenantId::from_str(b).unwrap(), 2),
        ]);
        assert_eq!(raise_generations(&mut r, &in_store, 1000), 3);
        let g = |t: &str| {
            r.tenants[t]
                .shards
                .iter()
                .map(|s| s.generation)
                .collect::<Vec<_>>()
        };
        assert_eq!(g(a), vec![Some(1012), Some(1012)]);
        assert_eq!(g(b), vec![Some(1004)]);
        // A shard never attached, with nothing in the store, stays without a generation.
        assert_eq!(g(c), vec![None]);
    }
}
