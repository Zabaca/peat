//! The copy of the records in a bucket, its deletion marks and its went-back record
//! (`storage_controller/kubernetes/README.md`): one document holding every record with its
//! revision and the highest-ever values, rewritten by the leader within a second of each change;
//! a mark per deleted object, written before the delete; and, when a running controller finds
//! its records went back, a record of what it had seen, which every later start refuses on.
//!
//! The document follows the API server, not this controller's own writes: every kind is watched,
//! and what is written is what the API server holds.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use kube::api::{Api, ListParams, WatchEvent, WatchParams};
use remote_storage::{
    DownloadError, DownloadOpts, GenericRemoteStorage, ListingMode, RemotePath, RemoteStorageConfig,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::Guard;
use super::objects::{
    LEADER_OBJECT_NAME, LeaderSpec, MetadataHealthSpec, Object, PageserverSpec, SafekeeperSpec,
    Spec, TenantSpec, TimelineSpec,
};
use crate::metrics::METRICS_REGISTRY;
use crate::persistence::records::Records;

/// The copy's document, under the configured prefix.
const COPY_KEY: &str = "records.json";
/// The went-back record.
const WENT_BACK_KEY: &str = "went-back.json";
/// The deletion marks: `deleted/<kind>/<name>`.
const MARKS_PREFIX: &str = "deleted/";

/// The shortest time between two writes of the copy.
const MIN_INTERVAL: Duration = Duration::from_millis(250);
/// The longest: the leader writes the copy again at least this often, changed or not, so a
/// document another controller left behind does not stay.
const MAX_INTERVAL: Duration = Duration::from_secs(60);

/// The key of a record, in the document and in marks: `<Kind>/<name>`.
pub(crate) fn record_key(kind: &str, name: &str) -> String {
    format!("{kind}/{name}")
}

/// What is kept in the bucket.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CopyDocument {
    /// When it was written, by the clock of the controller that wrote it.
    pub(crate) written_at: chrono::DateTime<chrono::Utc>,
    pub(crate) records: Records,
    /// Each record's revision, by [`record_key`].
    #[serde(default)]
    pub(crate) revisions: BTreeMap<String, u64>,
    /// The highest generation ever seen for any shard of each tenant, kept for good.
    #[serde(default)]
    pub(crate) highest_generations: BTreeMap<String, i32>,
    /// The highest safekeeper membership generation ever seen for each timeline, by its
    /// Timeline object's name, kept for good.
    #[serde(default)]
    pub(crate) highest_memberships: BTreeMap<String, i32>,
}

impl CopyDocument {
    pub(crate) fn new(records: Records) -> Self {
        CopyDocument {
            written_at: chrono::Utc::now(),
            records,
            revisions: BTreeMap::new(),
            highest_generations: BTreeMap::new(),
            highest_memberships: BTreeMap::new(),
        }
    }

    /// Raise the highest-ever values to what `records` holds, and to `other`'s.
    pub(crate) fn merge_highest(&mut self, other: Option<&CopyDocument>) {
        let records = self.records.clone();
        for t in records.tenants.values() {
            if let Some(g) = t.shards.iter().filter_map(|s| s.generation).max() {
                raise(&mut self.highest_generations, &t.tenant_id, g);
            }
        }
        for (name, t) in &records.timelines {
            if let Some(m) = &t.membership {
                raise(&mut self.highest_memberships, name, m.generation);
            }
        }
        if let Some(o) = other {
            for (k, g) in &o.highest_generations {
                raise(&mut self.highest_generations, k, *g);
            }
            for (k, g) in &o.highest_memberships {
                raise(&mut self.highest_memberships, k, *g);
            }
        }
    }
}

fn raise(map: &mut BTreeMap<String, i32>, key: &str, value: i32) {
    let e = map.entry(key.to_string()).or_insert(value);
    *e = (*e).max(value);
}

/// A deletion mark: written before an object is deleted, holding the revision deleted.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Mark {
    pub(crate) revision: u64,
    /// Written by `--records-accept-deleted` for a record deleted without a mark; a restore does
    /// not count it as covering data it finds in the pageservers' store.
    #[serde(default)]
    pub(crate) accepted: bool,
}

/// What a running controller had seen when it found its records went back.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WentBack {
    pub(crate) at: chrono::DateTime<chrono::Utc>,
    pub(crate) what: String,
    /// The highest revision it had seen of each record, by [`record_key`].
    pub(crate) seen: BTreeMap<String, u64>,
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

    async fn get<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<T>> {
        let cancel = CancellationToken::new();
        let download = match self
            .storage
            .download(
                &RemotePath::from_string(key)?,
                &DownloadOpts::default(),
                &cancel,
            )
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
        Ok(Some(serde_json::from_slice(&body)?))
    }

    async fn put<T: Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        let body = Bytes::from(serde_json::to_vec(value)?);
        let len = body.len();
        let cancel = CancellationToken::new();
        self.storage
            .upload(
                futures::stream::once(futures::future::ready(Ok(body))),
                len,
                &RemotePath::from_string(key)?,
                None,
                &cancel,
            )
            .await
    }

    /// The copy, or `None` when the bucket holds none yet.
    pub(crate) async fn read(&self) -> anyhow::Result<Option<CopyDocument>> {
        let mut doc: Option<CopyDocument> = self.get(COPY_KEY).await?;
        if let Some(doc) = &mut doc {
            // Health entries left without their shard (a removal, or a split's abort, between
            // its two writes) are not records, and Postgres would refuse them.
            doc.records.drop_orphaned_health();
            if doc.records.format != crate::persistence::records::FORMAT {
                anyhow::bail!(
                    "the copy is in format {}, this program reads format {}",
                    doc.records.format,
                    crate::persistence::records::FORMAT
                );
            }
        }
        Ok(doc)
    }

    pub(crate) async fn write(&self, doc: &CopyDocument) -> anyhow::Result<()> {
        let mut doc = doc.clone();
        doc.written_at = chrono::Utc::now();
        self.put(COPY_KEY, &doc).await
    }

    /// Every deletion mark, by [`record_key`].
    pub(crate) async fn read_marks(&self) -> anyhow::Result<BTreeMap<String, Mark>> {
        let cancel = CancellationToken::new();
        let prefix = RemotePath::from_string(MARKS_PREFIX)?;
        let mut keys = Vec::new();
        let mut listing = std::pin::pin!(self.storage.list_streaming(
            Some(&prefix),
            ListingMode::NoDelimiter,
            None,
            &cancel
        ));
        while let Some(page) = listing.next().await {
            keys.extend(page?.keys.into_iter().map(|o| o.key));
        }
        let mut marks = BTreeMap::new();
        for key in keys {
            let path = key.get_path().as_str().to_string();
            let Some(record) = path.split_once(MARKS_PREFIX).map(|(_, r)| r.to_string()) else {
                continue;
            };
            if let Some(mark) = self.get::<Mark>(&path).await? {
                marks.insert(record, mark);
            }
        }
        Ok(marks)
    }

    /// Write a mark, never lowering one already there: a mark at or above `mark`'s revision is
    /// left as it is.
    pub(crate) async fn write_mark(&self, record: &str, mark: Mark) -> anyhow::Result<()> {
        let key = format!("{MARKS_PREFIX}{record}");
        if let Some(held) = self.get::<Mark>(&key).await? {
            if held.revision >= mark.revision {
                return Ok(());
            }
        }
        self.put(&key, &mark).await
    }

    pub(crate) async fn read_went_back(&self) -> anyhow::Result<Option<WentBack>> {
        self.get(WENT_BACK_KEY).await
    }

    pub(crate) async fn write_went_back(&self, record: &WentBack) -> anyhow::Result<()> {
        self.put(WENT_BACK_KEY, record).await
    }

    pub(crate) async fn clear_went_back(&self) -> anyhow::Result<()> {
        let cancel = CancellationToken::new();
        self.storage
            .delete(&RemotePath::from_string(WENT_BACK_KEY)?, &cancel)
            .await
    }
}

/// What the store and the copy's writer share.
pub(crate) struct CopyState {
    /// The deletion marks, as read at start and on taking over, and as written since.
    pub(crate) marks: Mutex<BTreeMap<String, Mark>>,
    /// The document last read or written; a write never goes back from it.
    pub(crate) last: Mutex<Option<CopyDocument>>,
    /// When the oldest change not yet in the copy was seen.
    pub(crate) behind_since: Mutex<Option<Instant>>,
    /// The copy and its marks have been read (not so only under `--records-start-without-copy`).
    pub(crate) read: std::sync::atomic::AtomicBool,
}

impl CopyState {
    pub(crate) fn new() -> Self {
        CopyState {
            marks: Mutex::new(BTreeMap::new()),
            last: Mutex::new(None),
            behind_since: Mutex::new(None),
            read: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn behind(&self) -> Duration {
        self.behind_since
            .lock()
            .unwrap()
            .map_or(Duration::ZERO, |t| t.elapsed())
    }

    pub(crate) fn mark(&self, kind: &str, name: &str) -> Option<Mark> {
        self.marks
            .lock()
            .unwrap()
            .get(&record_key(kind, name))
            .copied()
    }
}

/// The records as the API server holds them, with their revisions, kept by watching every kind.
#[derive(Default)]
struct Mirror {
    tenants: BTreeMap<String, (TenantSpec, u64)>,
    metadata_health: BTreeMap<String, (MetadataHealthSpec, u64)>,
    timelines: BTreeMap<String, (TimelineSpec, u64)>,
    pageservers: BTreeMap<String, (PageserverSpec, u64)>,
    safekeepers: BTreeMap<String, (SafekeeperSpec, u64)>,
    leaders: BTreeMap<String, (LeaderSpec, u64)>,
    /// Kinds listed at least once; the copy is written only once all six are.
    listed: usize,
    /// Counts changes, so a write knows whether one came in while it ran.
    changes: u64,
}

impl Mirror {
    fn document(&self) -> CopyDocument {
        fn take<S: Clone>(
            m: &BTreeMap<String, (S, u64)>,
            kind: &str,
            revisions: &mut BTreeMap<String, u64>,
        ) -> BTreeMap<String, S> {
            m.iter()
                .map(|(name, (spec, rev))| {
                    revisions.insert(record_key(kind, name), *rev);
                    (name.clone(), spec.clone())
                })
                .collect()
        }
        let mut revisions = BTreeMap::new();
        let mut r = Records::empty();
        r.tenants = take(&self.tenants, TenantSpec::KIND, &mut revisions);
        r.metadata_health = take(
            &self.metadata_health,
            MetadataHealthSpec::KIND,
            &mut revisions,
        );
        r.timelines = take(&self.timelines, TimelineSpec::KIND, &mut revisions);
        r.pageservers = take(&self.pageservers, PageserverSpec::KIND, &mut revisions);
        r.safekeepers = take(&self.safekeepers, SafekeeperSpec::KIND, &mut revisions);
        let leaders = take(&self.leaders, LeaderSpec::KIND, &mut revisions);
        r.leader = leaders.get(LEADER_OBJECT_NAME).cloned();
        r.drop_orphaned_health();
        let mut doc = CopyDocument::new(r);
        doc.revisions = revisions;
        doc
    }
}

/// A kind the mirror follows.
trait Mirrored: Spec {
    fn slot(m: &mut Mirror) -> &mut BTreeMap<String, (Self, u64)>;
}

macro_rules! mirrored {
    ($spec:ty, $field:ident) => {
        impl Mirrored for $spec {
            fn slot(m: &mut Mirror) -> &mut BTreeMap<String, (Self, u64)> {
                &mut m.$field
            }
        }
    };
}
mirrored!(TenantSpec, tenants);
mirrored!(MetadataHealthSpec, metadata_health);
mirrored!(TimelineSpec, timelines);
mirrored!(PageserverSpec, pageservers);
mirrored!(SafekeeperSpec, safekeepers);
mirrored!(LeaderSpec, leaders);

/// Who writes the copy, and where.
pub(crate) struct Writer {
    pub(crate) client: kube::Client,
    pub(crate) namespace: String,
    pub(crate) copy: Arc<BucketCopy>,
    pub(crate) state: Arc<CopyState>,
    pub(crate) guard: Arc<Guard>,
    /// This controller's `--address-for-peers`: before each write the Leader object is read, and
    /// the copy is not written while it names another controller.
    pub(crate) own_address: Option<String>,
}

/// Keep the copy in the bucket while this controller leads, until `cancel`; the returned task
/// ends after any write under way has finished.
pub(crate) fn spawn(writer: Writer, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    let writer = Arc::new(writer);
    let mirror = Arc::new(Mutex::new(Mirror::default()));
    let changed = Arc::new(tokio::sync::Notify::new());
    macro_rules! follow {
        ($spec:ty) => {
            tokio::spawn(follow::<$spec>(
                Api::namespaced(writer.client.clone(), &writer.namespace),
                writer.guard.clone(),
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
    tokio::spawn(write_copies(writer, mirror, changed, cancel))
}

async fn follow<S: Mirrored>(
    api: Api<Object<S>>,
    guard: Arc<Guard>,
    mirror: Arc<Mutex<Mirror>>,
    changed: Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
) {
    let mut first = true;
    while !cancel.is_cancelled() {
        // List, then watch from the list's version until the watch can no longer go on. A list
        // is compared with everything this controller has seen.
        // Compared with what was seen before the list was sent.
        let sent = guard.before_read();
        let listed = async {
            let mut items = BTreeMap::new();
            let mut params = ListParams::default().limit(500);
            let version = loop {
                let page = api.list(&params).await?;
                for o in page.items {
                    guard.observe(&o, sent)?;
                    let rev = o.revision()?;
                    items.insert(o.name().to_string(), (o.spec, rev));
                }
                match page.metadata.continue_ {
                    Some(t) if !t.is_empty() => params = params.continue_token(&t),
                    _ => break page.metadata.resource_version.unwrap_or_default(),
                }
            };
            Ok::<_, crate::persistence::DatabaseError>((items, version))
        }
        .await;
        let mut version = match listed {
            Ok((items, version)) => {
                let mut m = mirror.lock().unwrap();
                for name in S::slot(&mut m).keys() {
                    if !items.contains_key(name) {
                        // A list missing what the watch held: compared with everything seen.
                        if let Err(e) = guard.observe_absent::<S>(name, sent) {
                            tracing::error!("records copy: {e}");
                            return;
                        }
                    }
                }
                *S::slot(&mut m) = items;
                m.changes += 1;
                if first {
                    m.listed += 1;
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
        // Within one stream, an event is compared only with the stream's earlier events for the
        // same name: a late event (revision 10 after 11 written in the same millisecond) is the
        // API server's order, not the store going back.
        let mut in_stream: BTreeMap<String, u64> = BTreeMap::new();
        'watch: loop {
            let params = WatchParams::default().timeout(290);
            let stream = match api.watch(&params, &version).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::info!("records copy: watching {} failed: {e}", S::PLURAL);
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
                        tracing::info!("records copy: watch of {} ended: {e}", S::PLURAL);
                        break 'watch;
                    }
                };
                let (o, deleted) = match event {
                    WatchEvent::Added(o) | WatchEvent::Modified(o) => (o, false),
                    WatchEvent::Deleted(o) => (o, true),
                    WatchEvent::Bookmark(b) => {
                        version = b.metadata.resource_version;
                        continue;
                    }
                    WatchEvent::Error(e) => {
                        // 410 (too old to watch from) and 504 with "Too large resource version"
                        // (ahead of an API server whose store was taken back): list again,
                        // which compares with everything seen.
                        tracing::info!("records copy: watch of {}: {e}", S::PLURAL);
                        break 'watch;
                    }
                };
                version = o.metadata.resource_version.clone().unwrap_or(version);
                let Ok(rev) = o.revision() else {
                    tracing::error!("records copy: {} {} has no revision", S::KIND, o.name());
                    break 'watch;
                };
                let name = o.name().to_string();
                if let Some(prev) = in_stream.get(&name) {
                    if rev < *prev {
                        let _ = guard.went_back_now(format!(
                            "the watch of {} gave {name} at revision {rev} after {prev}",
                            S::KIND
                        ));
                        return;
                    }
                }
                in_stream.insert(name.clone(), rev);
                guard.saw::<S>(&name, rev, deleted);
                let mut m = mirror.lock().unwrap();
                if deleted {
                    S::slot(&mut m).remove(&name);
                } else {
                    S::slot(&mut m).insert(name, (o.spec, rev));
                }
                m.changes += 1;
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

/// Why a document is not written over the last one read or written.
fn goes_back(
    next: &CopyDocument,
    last: &CopyDocument,
    marks: &BTreeMap<String, Mark>,
) -> Option<String> {
    if next.records.is_empty() && !last.records.is_empty() {
        return Some("every record is gone, and the copy holds some".to_string());
    }
    for (key, rev) in &last.revisions {
        match next.revisions.get(key) {
            Some(n) if n < rev => {
                return Some(format!("{key} at revision {n}, the copy has {rev}"));
            }
            Some(_) => {}
            None => {
                let covered = marks.get(key).is_some_and(|m| m.revision >= *rev);
                if !covered {
                    return Some(format!(
                        "{key} is gone with no mark covering it (the copy has revision {rev})"
                    ));
                }
            }
        }
    }
    None
}

async fn write_copies(
    writer: Arc<Writer>,
    mirror: Arc<Mutex<Mirror>>,
    changed: Arc<tokio::sync::Notify>,
    cancel: CancellationToken,
) {
    let behind = &METRICS_REGISTRY
        .metrics_group
        .storage_controller_records_copy_behind_seconds;
    let mut last_write = Instant::now();
    let mut written_changes = 0u64;
    loop {
        tokio::select! {
            _ = changed.notified() => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = cancel.cancelled() => return,
        }
        // Nothing is written before the copy and its marks are read (they are, except under
        // --records-start-without-copy until the bucket answers).
        if !writer.state.read.load(std::sync::atomic::Ordering::SeqCst) {
            continue;
        }
        let (mut doc, changes) = {
            let m = mirror.lock().unwrap();
            if m.listed < 6 {
                continue;
            }
            if m.changes != written_changes {
                writer
                    .state
                    .behind_since
                    .lock()
                    .unwrap()
                    .get_or_insert_with(Instant::now);
            }
            (m.document(), m.changes)
        };
        behind.set(writer.state.behind().as_secs_f64().ceil() as i64);
        if changes == written_changes && last_write.elapsed() < MAX_INTERVAL {
            continue;
        }
        // Only while this controller leads: the Leader object names it (when it names anyone).
        if let Some(own) = &writer.own_address {
            let leaders: Api<Object<LeaderSpec>> =
                Api::namespaced(writer.client.clone(), &writer.namespace);
            match leaders.get_opt(LEADER_OBJECT_NAME).await {
                Ok(Some(l)) if l.spec.address != *own => {
                    tracing::warn!(
                        "records copy: the Leader record names {}, not this controller; not \
                         writing the copy",
                        l.spec.address
                    );
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("records copy: reading the Leader record failed: {e}");
                    continue;
                }
            }
        }
        let refused = {
            let last = writer.state.last.lock().unwrap();
            let marks = writer.state.marks.lock().unwrap();
            if let Some(last) = last.as_ref() {
                doc.merge_highest(Some(last));
                goes_back(&doc, last, &marks)
            } else {
                doc.merge_highest(None);
                None
            }
        };
        if let Some(why) = refused {
            tracing::warn!("records copy: not writing a copy that goes back: {why}");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => continue,
                _ = cancel.cancelled() => return,
            }
        }
        match writer.copy.write(&doc).await {
            Ok(()) => {
                last_write = Instant::now();
                written_changes = changes;
                *writer.state.last.lock().unwrap() = Some(doc);
                let m = mirror.lock().unwrap();
                if m.changes == changes {
                    *writer.state.behind_since.lock().unwrap() = None;
                }
                behind.set(writer.state.behind().as_secs_f64().ceil() as i64);
            }
            Err(e) => tracing::warn!("records copy: writing the copy failed: {e:#}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(MIN_INTERVAL) => {}
            _ = cancel.cancelled() => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(revisions: &[(&str, u64)]) -> CopyDocument {
        let mut d = CopyDocument::new(Records::empty());
        d.revisions = revisions.iter().map(|(k, r)| (k.to_string(), *r)).collect();
        d
    }

    #[test]
    fn a_copy_that_goes_back_is_not_written() {
        let last = doc(&[("Tenant/a", 5), ("Tenant/b", 3)]);
        let none = BTreeMap::new();
        // Forward, and a new record: written
        assert_eq!(
            goes_back(
                &doc(&[("Tenant/a", 6), ("Tenant/b", 3), ("Tenant/c", 1)]),
                &last,
                &none
            ),
            None
        );
        // A revision lower than the copy's
        assert!(goes_back(&doc(&[("Tenant/a", 4), ("Tenant/b", 3)]), &last, &none).is_some());
        // A record gone with no mark, with a stale mark, and with a covering one
        let gone = doc(&[("Tenant/a", 5)]);
        assert!(goes_back(&gone, &last, &none).is_some());
        let stale = BTreeMap::from([(
            "Tenant/b".to_string(),
            Mark {
                revision: 2,
                accepted: false,
            },
        )]);
        assert!(goes_back(&gone, &last, &stale).is_some());
        let covering = BTreeMap::from([(
            "Tenant/b".to_string(),
            Mark {
                revision: 3,
                accepted: false,
            },
        )]);
        assert_eq!(goes_back(&gone, &last, &covering), None);
    }
}
