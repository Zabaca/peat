//! The copy of the records in a bucket (`storage_controller/kubernetes/README.md`, "The copy in
//! the bucket"): one document holding every record, rewritten within a second of each change,
//! so a platform whose cluster lost its objects can be rebuilt from the bucket alone.
//!
//! The copy follows the API server, not this controller's own writes: every kind is watched, and
//! what is written is what the API server holds. So any controller running on the records
//! (a leader, or a candidate during a handover) writes the same copy, and a controller that has
//! stepped down never writes back an older one. The one change it does not follow is to no
//! records at all over a copy that holds some.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use kube::api::{Api, ListParams, WatchEvent, WatchParams};
use remote_storage::{
    DownloadError, DownloadOpts, GenericRemoteStorage, RemotePath, RemoteStorageConfig,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::objects::{
    LEADER_OBJECT_NAME, LeaderSpec, MetadataHealthSpec, Object, PageserverSpec, SafekeeperSpec,
    Spec, TenantSpec, TimelineSpec,
};
use crate::metrics::METRICS_REGISTRY;
use crate::persistence::records::Records;

/// The copy's one object, under the configured prefix.
const COPY_KEY: &str = "records.json";

/// The shortest time between two writes of the copy.
const MIN_INTERVAL: Duration = Duration::from_millis(250);

/// What is kept in the bucket.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CopyDocument {
    /// When it was written, by the clock of the controller that wrote it.
    pub(crate) written_at: chrono::DateTime<chrono::Utc>,
    pub(crate) records: Records,
    /// The highest generation the copy has ever seen for any shard of each tenant, kept even
    /// after the tenant's record is gone: what a restore must stay above, whatever happened to
    /// the objects (lost one by one, or taken back to an older state).
    #[serde(default)]
    pub(crate) highest_generations: BTreeMap<String, i32>,
}

pub(crate) struct BucketCopy {
    storage: GenericRemoteStorage,
}

impl BucketCopy {
    pub(crate) async fn new(config: &RemoteStorageConfig) -> anyhow::Result<Self> {
        Ok(BucketCopy {
            storage: GenericRemoteStorage::from_config(config).await?,
        })
    }

    fn key() -> RemotePath {
        RemotePath::from_string(COPY_KEY).expect("a valid key")
    }

    /// The copy, or `None` when the bucket holds none yet.
    pub(crate) async fn read(&self) -> anyhow::Result<Option<CopyDocument>> {
        let cancel = CancellationToken::new();
        let download = match self
            .storage
            .download(&Self::key(), &DownloadOpts::default(), &cancel)
            .await
        {
            Ok(d) => d,
            Err(DownloadError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut body = Vec::new();
        let mut stream = download.download_stream;
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk?);
        }
        let doc: CopyDocument = serde_json::from_slice(&body)?;
        if doc.records.format != crate::persistence::records::FORMAT {
            anyhow::bail!(
                "the copy is in format {}, this program reads format {}",
                doc.records.format,
                crate::persistence::records::FORMAT
            );
        }
        Ok(Some(doc))
    }

    pub(crate) async fn write(
        &self,
        records: &Records,
        highest_generations: &BTreeMap<String, i32>,
    ) -> anyhow::Result<()> {
        let doc = CopyDocument {
            written_at: chrono::Utc::now(),
            records: records.clone(),
            highest_generations: highest_generations.clone(),
        };
        let body = Bytes::from(serde_json::to_vec(&doc)?);
        let len = body.len();
        let cancel = CancellationToken::new();
        self.storage
            .upload(
                futures::stream::once(futures::future::ready(Ok(body))),
                len,
                &Self::key(),
                None,
                &cancel,
            )
            .await
    }
}

/// The records as the API server holds them, kept by watching every kind.
#[derive(Default)]
struct Mirror {
    tenants: BTreeMap<String, TenantSpec>,
    metadata_health: BTreeMap<String, MetadataHealthSpec>,
    timelines: BTreeMap<String, TimelineSpec>,
    pageservers: BTreeMap<String, PageserverSpec>,
    safekeepers: BTreeMap<String, SafekeeperSpec>,
    leaders: BTreeMap<String, LeaderSpec>,
    /// Kinds listed at least once, and the copy found at start read (one more); the copy is
    /// written only once all seven are done.
    listed: usize,
    /// When the oldest change not yet in the copy was seen.
    behind_since: Option<Instant>,
    /// Counts changes, so a write knows whether one came in while it ran.
    changes: u64,
    /// The highest generation seen per tenant, from the copy found at start on; never lowered.
    highest: BTreeMap<String, i32>,
}

impl Mirror {
    fn records(&self) -> Records {
        let mut r = Records::empty();
        r.tenants = self.tenants.clone();
        r.metadata_health = self.metadata_health.clone();
        r.timelines = self.timelines.clone();
        r.pageservers = self.pageservers.clone();
        r.safekeepers = self.safekeepers.clone();
        r.leader = self.leaders.get(LEADER_OBJECT_NAME).cloned();
        r
    }

    fn changed(&mut self) {
        self.changes += 1;
        self.behind_since.get_or_insert_with(Instant::now);
    }
}

/// A kind the mirror follows.
trait Mirrored: Spec {
    fn slot(m: &mut Mirror) -> &mut BTreeMap<String, Self>;

    /// Take note of what a record shows that must outlive it.
    fn observe(&self, _highest: &mut BTreeMap<String, i32>) {}
}

impl Mirrored for TenantSpec {
    fn slot(m: &mut Mirror) -> &mut BTreeMap<String, Self> {
        &mut m.tenants
    }

    fn observe(&self, highest: &mut BTreeMap<String, i32>) {
        if let Some(g) = self.shards.iter().filter_map(|s| s.generation).max() {
            let h = highest.entry(self.tenant_id.clone()).or_insert(g);
            *h = (*h).max(g);
        }
    }
}

macro_rules! mirrored {
    ($spec:ty, $field:ident) => {
        impl Mirrored for $spec {
            fn slot(m: &mut Mirror) -> &mut BTreeMap<String, Self> {
                &mut m.$field
            }
        }
    };
}
mirrored!(MetadataHealthSpec, metadata_health);
mirrored!(TimelineSpec, timelines);
mirrored!(PageserverSpec, pageservers);
mirrored!(SafekeeperSpec, safekeepers);
mirrored!(LeaderSpec, leaders);

/// Keep the copy in the bucket: watch every kind in `namespace`, and write the copy within a
/// second of each change, until `cancel`.
pub(crate) fn spawn(
    client: kube::Client,
    namespace: String,
    copy: Arc<BucketCopy>,
    cancel: CancellationToken,
) {
    let mirror = Arc::new(Mutex::new(Mirror::default()));
    let changed = Arc::new(tokio::sync::Notify::new());
    macro_rules! follow {
        ($spec:ty) => {
            tokio::spawn(follow::<$spec>(
                Api::namespaced(client.clone(), &namespace),
                mirror.clone(),
                changed.clone(),
                cancel.clone(),
            ));
        };
    }
    follow!(TenantSpec);
    follow!(MetadataHealthSpec);
    follow!(TimelineSpec);
    follow!(PageserverSpec);
    follow!(SafekeeperSpec);
    follow!(LeaderSpec);
    // The highest generations carry over from the copy already in the bucket.
    tokio::spawn({
        let (copy, mirror, cancel) = (copy.clone(), mirror.clone(), cancel.clone());
        async move {
            loop {
                match copy.read().await {
                    Ok(doc) => {
                        let mut m = mirror.lock().unwrap();
                        for (t, g) in doc.map(|d| d.highest_generations).unwrap_or_default() {
                            let h = m.highest.entry(t).or_insert(g);
                            *h = (*h).max(g);
                        }
                        m.listed += 1;
                        return;
                    }
                    Err(e) => tracing::warn!("records copy: reading the copy failed: {e:#}"),
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = cancel.cancelled() => return,
                }
            }
        }
    });
    tokio::spawn(write_copies(copy, mirror, changed, cancel));
}

async fn follow<S: Mirrored>(
    api: Api<Object<S>>,
    mirror: Arc<Mutex<Mirror>>,
    changed: Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
) {
    let mut first = true;
    while !cancel.is_cancelled() {
        // List, then watch from the list's version until the watch can no longer go on.
        let listed = async {
            let mut items = BTreeMap::new();
            let mut params = ListParams::default().limit(500);
            let version = loop {
                let page = api.list(&params).await?;
                for o in page.items {
                    items.insert(o.metadata.name.clone().unwrap_or_default(), o.spec);
                }
                match page.metadata.continue_ {
                    Some(t) if !t.is_empty() => params = params.continue_token(&t),
                    _ => break page.metadata.resource_version.unwrap_or_default(),
                }
            };
            Ok::<_, kube::Error>((items, version))
        }
        .await;
        let mut version = match listed {
            Ok((items, version)) => {
                let mut m = mirror.lock().unwrap();
                for spec in items.values() {
                    spec.observe(&mut m.highest);
                }
                if *S::slot(&mut m) != items {
                    *S::slot(&mut m) = items;
                    m.changed();
                }
                if first {
                    m.listed += 1;
                    m.changed();
                    first = false;
                }
                drop(m);
                changed.notify_one();
                version
            }
            Err(e) => {
                tracing::warn!("records copy: listing {} failed: {e}", S::PLURAL);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                    _ = cancel.cancelled() => return,
                }
            }
        };
        'watch: loop {
            let params = WatchParams::default().timeout(290);
            let stream = match api.watch(&params, &version).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("records copy: watching {} failed: {e}", S::PLURAL);
                    break 'watch;
                }
            };
            let mut stream = std::pin::pin!(stream);
            loop {
                let event = tokio::select! {
                    e = stream.try_next() => e,
                    _ = cancel.cancelled() => return,
                };
                let event = match event {
                    Ok(Some(e)) => e,
                    // The watch's time ran out: watch again from where it was.
                    Ok(None) => continue 'watch,
                    Err(e) => {
                        tracing::warn!("records copy: watch of {} ended: {e}", S::PLURAL);
                        break 'watch;
                    }
                };
                let mut m = mirror.lock().unwrap();
                match event {
                    WatchEvent::Added(o) | WatchEvent::Modified(o) => {
                        version = o.metadata.resource_version.clone().unwrap_or(version);
                        o.spec.observe(&mut m.highest);
                        S::slot(&mut m).insert(o.metadata.name.unwrap_or_default(), o.spec);
                        m.changed();
                    }
                    WatchEvent::Deleted(o) => {
                        version = o.metadata.resource_version.clone().unwrap_or(version);
                        S::slot(&mut m).remove(&o.metadata.name.unwrap_or_default());
                        m.changed();
                    }
                    WatchEvent::Bookmark(b) => version = b.metadata.resource_version,
                    WatchEvent::Error(e) => {
                        // 410: the version is too old to watch from; list again.
                        tracing::info!("records copy: watch of {}: {e}", S::PLURAL);
                        break 'watch;
                    }
                }
                drop(m);
                changed.notify_one();
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            _ = cancel.cancelled() => return,
        }
    }
}

async fn write_copies(
    copy: Arc<BucketCopy>,
    mirror: Arc<Mutex<Mirror>>,
    changed: Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
) {
    let behind = &METRICS_REGISTRY
        .metrics_group
        .storage_controller_records_copy_behind_seconds;
    loop {
        tokio::select! {
            _ = changed.notified() => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = cancel.cancelled() => return,
        }
        let (records, highest, changes) = {
            let m = mirror.lock().unwrap();
            behind.set(behind_seconds(m.behind_since));
            // Six kinds listed, and the copy found at start read
            if m.listed < 7 || m.behind_since.is_none() {
                continue;
            }
            (m.records(), m.highest.clone(), m.changes)
        };
        // Records that are all gone while the copy still holds some are what a lost cluster
        // looks like from here (its objects deleted under a running controller): the copy keeps
        // what it holds, as the way back, and the metric shows it falling behind.
        if records.is_empty() {
            match copy.read().await {
                Ok(Some(doc)) if !doc.records.is_empty() => {
                    tracing::warn!(
                        "records copy: every record is gone but the copy holds {:?}; not \
                         overwriting it (restore with storcon_records, or delete the copy)",
                        doc.records.counts()
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(10)) => continue,
                        _ = cancel.cancelled() => return,
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("records copy: reading the copy failed: {e:#}");
                    continue;
                }
            }
        }
        match copy.write(&records, &highest).await {
            Ok(()) => {
                let mut m = mirror.lock().unwrap();
                if m.changes == changes {
                    m.behind_since = None;
                }
                behind.set(behind_seconds(m.behind_since));
            }
            Err(e) => tracing::warn!("records copy: writing the copy failed: {e:#}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(MIN_INTERVAL) => {}
            _ = cancel.cancelled() => return,
        }
    }
}

fn behind_seconds(since: Option<Instant>) -> i64 {
    since.map_or(0, |t| t.elapsed().as_secs_f64().ceil() as i64)
}
