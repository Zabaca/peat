//! `storcon_records`, the copy tool of `storage_controller/kubernetes/README.md`: dump, compare
//! and copy the controller's records between its two stores, scan the pageservers' store for
//! the generations it holds, and rebuild the records from the copy in the bucket, always with
//! the controller stopped.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use pageserver_api::shard::TenantShardId;
use remote_storage::{GenericRemoteStorage, ListingMode, RemotePath, RemoteStorageConfig};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use utils::generation::Generation;
use utils::id::{TenantId, TimelineId};
use utils::logging::SecretString;

use crate::persistence::kubernetes::copy::{BucketCopy, CopyDocument, Mark, record_key};
use crate::persistence::kubernetes::objects::{
    Spec, TenantSpec, TimelineSpec, timeline_object_name,
};
use crate::persistence::records::Records;
use crate::persistence::{KubernetesRecords, Persistence};

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

/// The check that nothing still runs, as the command line sets it.
#[derive(Clone, Debug, Default)]
pub struct Running {
    /// `--controller-address`: where the controller is, when the records name no leader.
    pub controller_address: Option<String>,
    /// `--assume-stopped`: skip the check (the person checked that nothing runs).
    pub assume_stopped: bool,
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
                let copy_storage = self
                    .records_copy_storage
                    .as_deref()
                    .map(RemoteStorageConfig::from_toml_str)
                    .transpose()?;
                let p = Persistence::new_kubernetes(
                    self.kubeconfig.as_deref(),
                    self.records_namespace.clone(),
                    KubernetesRecords {
                        copy_storage,
                        ..Default::default()
                    },
                )
                .await?;
                p.check_kinds().await?;
                Ok(p)
            }
            Store::Bucket => anyhow::bail!("the bucket is read with read_copy"),
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

    async fn read_copy(&self) -> anyhow::Result<CopyDocument> {
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
    running: &Running,
    accept_remade: &BTreeSet<String>,
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
    // The way back (into Postgres) runs with every pageserver stopped; the move into Kubernetes
    // runs with them up, which is safe with the controller stopped.
    if let Some(reason) = something_may_run(&records, running, to == Store::Postgres).await {
        return Ok(Outcome::Refused(reason));
    }
    if to == Store::Postgres {
        let dest = stores.open(to, true).await?;
        return write_checked(&dest, &records, replace, &BTreeMap::new()).await;
    }

    // Into Kubernetes: above the copy's revisions and the marks', checked against the copy's
    // highest-ever values, and a new copy written of what was written.
    if stores.records_copy_storage.is_none() {
        return Ok(Outcome::Refused(
            "copy --to kubernetes needs --records-copy-storage".to_string(),
        ));
    }
    let bucket = stores.bucket().await?;
    if let Some(w) = bucket.read_went_back().await? {
        return Ok(Outcome::Refused(format!(
            "a controller found the records went back at {} ({}): restore them, not copy over",
            w.at, w.what
        )));
    }
    let old = bucket.read().await?;
    let marks = bucket.read_marks().await?;
    if let Some(old) = &old {
        let stale = below_highest(&records, old, accept_remade);
        if !stale.is_empty() {
            return Ok(Outcome::Refused(format!(
                "the source holds generations below what the copy in the bucket has seen (a \
                 source that went back, or a tenant remade under its id): {stale:?}; give \
                 --accept-remade with those that were remade"
            )));
        }
    }
    let dest = stores.open(to, true).await?;
    let floors = floors(old.as_ref(), &marks);
    let (outcome, revisions) = write_checked_revisions(&dest, &records, replace, &floors).await?;
    if outcome == Outcome::Done {
        write_new_copy(&bucket, &records, revisions, old.as_ref()).await?;
    }
    Ok(outcome)
}

/// The tenants and timelines whose generations are below the copy's highest-ever values.
fn below_highest(
    records: &Records,
    copy: &CopyDocument,
    accept_remade: &BTreeSet<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    for (name, t) in &records.tenants {
        let key = record_key(TenantSpec::KIND, name);
        let highest = copy.highest_generations.get(&t.tenant_id).copied();
        let here = t.shards.iter().filter_map(|s| s.generation).max();
        if let (Some(h), Some(g)) = (highest, here) {
            if g < h && !accept_remade.contains(&key) {
                out.push(format!("{key}: generation {g}, the copy has seen {h}"));
            }
        }
    }
    for (name, t) in &records.timelines {
        let key = record_key(TimelineSpec::KIND, name);
        let highest = copy.highest_memberships.get(name).copied();
        if let (Some(h), Some(m)) = (highest, &t.membership) {
            if m.generation < h && !accept_remade.contains(&key) {
                out.push(format!(
                    "{key}: membership generation {}, the copy has seen {h}",
                    m.generation
                ));
            }
        }
    }
    out
}

/// Each name's revision floor: the copy's revision and the mark's.
fn floors(copy: Option<&CopyDocument>, marks: &BTreeMap<String, Mark>) -> BTreeMap<String, u64> {
    let mut out: BTreeMap<String, u64> = copy.map(|c| c.revisions.clone()).unwrap_or_default();
    for (k, m) in marks {
        let e = out.entry(k.clone()).or_insert(m.revision);
        *e = (*e).max(m.revision);
    }
    out
}

async fn write_new_copy(
    bucket: &BucketCopy,
    records: &Records,
    revisions: BTreeMap<String, u64>,
    old: Option<&CopyDocument>,
) -> anyhow::Result<()> {
    let mut doc = CopyDocument::new(records.clone());
    doc.revisions = revisions;
    doc.merge_highest(old);
    bucket.write(&doc).await
}

/// Write `records` into `dest` and read them back.
async fn write_checked(
    dest: &Persistence,
    records: &Records,
    replace: bool,
    floors: &BTreeMap<String, u64>,
) -> anyhow::Result<Outcome> {
    Ok(write_checked_revisions(dest, records, replace, floors)
        .await?
        .0)
}

async fn write_checked_revisions(
    dest: &Persistence,
    records: &Records,
    replace: bool,
    floors: &BTreeMap<String, u64>,
) -> anyhow::Result<(Outcome, BTreeMap<String, u64>)> {
    if !replace {
        let held = dest.read_all_records().await?;
        if held != Records::empty() {
            return Ok((
                Outcome::Refused(format!(
                    "the destination is not empty ({:?}): give --replace to replace what it holds",
                    held.counts()
                )),
                BTreeMap::new(),
            ));
        }
    }
    let revisions = dest.write_all_records(records, replace, floors).await?;
    let written = dest.read_all_records().await?;
    let diff = records.diff(&written)?;
    Ok((
        if diff.is_empty() {
            Outcome::Done
        } else {
            Outcome::Differ(diff)
        },
        revisions,
    ))
}

/// What `scan-generations` found in the pageservers' store.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Scan {
    /// The `remote_storage` setting scanned (it holds no secret).
    pub storage: String,
    pub scanned_at: chrono::DateTime<chrono::Utc>,
    /// The highest generation any object under each tenant shard's prefix was written with.
    pub shards: BTreeMap<String, u32>,
    /// The timelines found under each tenant.
    pub timelines: BTreeMap<String, BTreeSet<String>>,
}

impl Scan {
    /// The highest generation of any shard of each tenant.
    fn by_tenant(&self) -> BTreeMap<TenantId, u32> {
        let mut out: BTreeMap<TenantId, u32> = BTreeMap::new();
        for (shard, g) in &self.shards {
            if let Ok(id) = TenantShardId::from_str(shard) {
                let e = out.entry(id.tenant_id).or_default();
                *e = (*e).max(*g);
            }
        }
        out
    }
}

/// Read the pageservers' store: every object under `tenants/`, its generation suffix and its
/// timeline. Run with the pageservers' credentials, after every pageserver is stopped.
pub async fn scan_generations(config: &str) -> anyhow::Result<Scan> {
    let storage =
        GenericRemoteStorage::from_config(&RemoteStorageConfig::from_toml_str(config)?).await?;
    let cancel = CancellationToken::new();
    let prefix = RemotePath::from_string("tenants/")?;
    let mut scan = Scan {
        storage: config.to_string(),
        scanned_at: chrono::Utc::now(),
        ..Default::default()
    };
    let mut listing = std::pin::pin!(storage.list_streaming(
        Some(&prefix),
        ListingMode::NoDelimiter,
        None,
        &cancel
    ));
    while let Some(page) = listing.next().await {
        for obj in page?.keys {
            let key = obj.key.get_path().as_str().to_string();
            if let Some((shard, generation)) = key_generation(&key) {
                let e = scan.shards.entry(shard.to_string()).or_default();
                *e = (*e).max(generation);
            }
            if let Some((tenant, timeline)) = key_timeline(&key) {
                scan.timelines
                    .entry(tenant.to_string())
                    .or_default()
                    .insert(timeline.to_string());
            }
        }
    }
    Ok(scan)
}

/// `tenants/<tenant shard>/.../<name>-<generation, 8 hex digits>[.json]`.
pub(crate) fn key_generation(key: &str) -> Option<(TenantShardId, u32)> {
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
        Generation::Valid(g) => Some((shard, g)),
        Generation::None => None,
    }
}

/// `tenants/<tenant shard>/timelines/<timeline>/...`.
fn key_timeline(key: &str) -> Option<(TenantId, TimelineId)> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() < 4 || parts[0] != "tenants" || parts[2] != "timelines" {
        return None;
    }
    Some((
        TenantShardId::from_str(parts[1]).ok()?.tenant_id,
        TimelineId::from_str(parts[3]).ok()?,
    ))
}

/// What a restore changed, for its report.
#[derive(Debug, Default)]
pub struct Restored {
    pub copy_written_at: Option<chrono::DateTime<chrono::Utc>>,
    pub scan_storage: String,
    pub scanned_at: Option<chrono::DateTime<chrono::Utc>>,
    pub shards_raised: usize,
    pub timelines_read_from_safekeepers: usize,
    pub timelines_kept_silent: Vec<String>,
    pub left_out: Vec<String>,
}

/// How far above the highest generation seen a restore sets each shard's.
pub const DEFAULT_GENERATION_MARGIN: u32 = 1000;

/// What a restore may go on without.
#[derive(Clone, Debug, Default)]
pub struct RestoreOptions {
    pub replace: bool,
    pub margin: u32,
    /// `--leave-out`: records the store shows and the copy lacks, by record key.
    pub leave_out: BTreeSet<String>,
    /// `--accept-silent-safekeepers`.
    pub accept_silent_safekeepers: bool,
    pub safekeeper_jwt: Option<String>,
}

/// Rebuild the records from the copy in the bucket (`storage_controller/kubernetes/README.md`,
/// "restore").
pub async fn restore(
    stores: &Stores,
    to: Store,
    scan: &Scan,
    options: &RestoreOptions,
    running: &Running,
) -> anyhow::Result<(Outcome, Restored)> {
    let mut report = Restored {
        scan_storage: scan.storage.clone(),
        scanned_at: Some(scan.scanned_at),
        ..Default::default()
    };
    if to == Store::Bucket {
        return Ok((
            Outcome::Refused("restore writes into postgres or kubernetes".to_string()),
            report,
        ));
    }
    let bucket = stores.bucket().await?;
    let doc = bucket
        .read()
        .await?
        .context("the bucket holds no copy of the records")?;
    let marks = bucket.read_marks().await?;
    report.copy_written_at = Some(doc.written_at);
    let mut records = doc.records.clone();
    if let Some(reason) = something_may_run(&records, running, true).await {
        return Ok((Outcome::Refused(reason), report));
    }

    // The generations file: of the right storage, and made after the copy's last write.
    if scan.scanned_at < doc.written_at {
        return Ok((
            Outcome::Refused(format!(
                "the generations file was made at {}, before the copy's last write at {}: scan \
                 again, with every pageserver stopped",
                scan.scanned_at, doc.written_at
            )),
            report,
        ));
    }
    let with_generation: Vec<String> = records
        .tenants
        .values()
        .flat_map(|t| {
            t.shards
                .iter()
                .filter(|s| s.generation.is_some())
                .map(|s| s.to_row(&t.tenant_id))
                .filter_map(|r| r.get_tenant_shard_id().ok())
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    if !with_generation.is_empty() && !with_generation.iter().any(|s| scan.shards.contains_key(s)) {
        return Ok((
            Outcome::Refused(format!(
                "none of the copy's {} shards with a generation is in the generations file of \
                 {}: the wrong bucket or prefix?",
                with_generation.len(),
                scan.storage
            )),
            report,
        ));
    }

    // What the store shows and the copy lacks, no mark covering it (an accepted mark does not).
    let covered = |key: &str| marks.get(key).is_some_and(|m| !m.accepted);
    let mut lacking = Vec::new();
    let in_store = scan.by_tenant();
    for tenant in in_store.keys() {
        let key = record_key(TenantSpec::KIND, &tenant.to_string());
        match records.tenants.get(&tenant.to_string()) {
            None if !covered(&key) => lacking.push(key),
            Some(t) => {
                let copy_count = t.shards.iter().map(|s| s.shard_count).max().unwrap_or(0);
                let store_count = scan
                    .shards
                    .keys()
                    .filter_map(|s| TenantShardId::from_str(s).ok())
                    .filter(|id| id.tenant_id == *tenant)
                    .map(|id| id.shard_count.literal() as i32)
                    .max()
                    .unwrap_or(0);
                if store_count > copy_count {
                    lacking.push(format!(
                        "{key} (split to {store_count} shards in the store)"
                    ));
                }
            }
            None => {}
        }
    }
    let tracked: BTreeSet<&str> = records
        .timelines
        .values()
        .filter(|t| t.membership.is_some())
        .map(|t| t.tenant_id.as_str())
        .collect();
    for (tenant, timelines) in &scan.timelines {
        if !tracked.contains(tenant.as_str()) {
            continue;
        }
        for tl in timelines {
            let name = timeline_object_name(tenant, tl);
            let key = record_key(TimelineSpec::KIND, &name);
            if !records.timelines.contains_key(&name) && !covered(&key) {
                lacking.push(key);
            }
        }
    }
    let unaccounted: Vec<String> = lacking
        .iter()
        .filter(|k| {
            let bare = k.split(" (").next().unwrap_or(k);
            !options.leave_out.contains(bare)
        })
        .cloned()
        .collect();
    if !unaccounted.is_empty() {
        return Ok((
            Outcome::Refused(format!(
                "the pageservers' store holds what the copy lacks (a copy behind while the \
                 bucket was unreachable): {unaccounted:?}; give --leave-out with those to go on \
                 without them"
            )),
            report,
        ));
    }
    report.left_out = lacking;

    // Generations: above the highest the copy holds or has seen, and the store's.
    let mut seen = in_store;
    for (tenant, g) in &doc.highest_generations {
        if let Ok(t) = TenantId::from_str(tenant) {
            let e = seen.entry(t).or_default();
            *e = (*e).max(*g as u32);
        }
    }
    report.shards_raised = raise_generations(&mut records, &seen, options.margin);

    // Safekeeper membership: the highest configuration a member holds; every member answers.
    let (read, silent) = read_membership(&mut records, &doc, options).await;
    report.timelines_read_from_safekeepers = read;
    if !silent.is_empty() && !options.accept_silent_safekeepers {
        return Ok((
            Outcome::Refused(format!(
                "a safekeeper of these timelines did not answer, and may hold a higher \
                 membership than the copy: {silent:?}; start it, or give \
                 --accept-silent-safekeepers"
            )),
            report,
        ));
    }
    report.timelines_kept_silent = silent;

    let dest = stores.open(to, true).await?;
    let floors = floors(Some(&doc), &marks);
    let (outcome, revisions) =
        write_checked_revisions(&dest, &records, options.replace, &floors).await?;
    if outcome == Outcome::Done {
        for key in &report.left_out {
            let bare = key.split(" (").next().unwrap_or(key).to_string();
            let revision = floors.get(&bare).copied().unwrap_or(0);
            bucket
                .write_mark(
                    &bare,
                    Mark {
                        revision,
                        accepted: false,
                    },
                )
                .await?;
        }
        if to == Store::Kubernetes {
            write_new_copy(&bucket, &records, revisions, Some(&doc)).await?;
        }
        bucket.clear_went_back().await?;
    }
    Ok((outcome, report))
}

/// Set every shard of a tenant to `margin` above the highest of the copy's generations for it
/// and `seen` (the store's, and the copy's highest-ever); a shard without a generation gets one
/// only if `seen` has one. How many shards were raised.
pub(crate) fn raise_generations(
    records: &mut Records,
    seen: &BTreeMap<TenantId, u32>,
    margin: u32,
) -> usize {
    let mut raised = 0;
    for t in records.tenants.values_mut() {
        let Ok(tenant) = TenantId::from_str(&t.tenant_id) else {
            continue;
        };
        let stored = seen.get(&tenant).copied();
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

/// For each timeline with a membership, ask every member and keep the highest configuration
/// any of them holds (at least the copy's highest-ever). Returns how many timelines were read,
/// and those a member did not answer for; with `--accept-silent-safekeepers` those keep the
/// copy's set with their generation raised by the margin.
async fn read_membership(
    records: &mut Records,
    doc: &CopyDocument,
    options: &RestoreOptions,
) -> (usize, Vec<String>) {
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
    for (name, t) in records.timelines.iter_mut() {
        let Some(m) = t.membership.as_mut() else {
            continue;
        };
        if let Some(h) = doc.highest_memberships.get(name) {
            m.generation = m.generation.max(*h);
        }
        let (Ok(tenant), Ok(timeline)) = (
            TenantId::from_str(&t.tenant_id),
            TimelineId::from_str(&t.timeline_id),
        ) else {
            continue;
        };
        let mut best: Option<safekeeper_api::membership::Configuration> = None;
        let mut all_answered = true;
        let members: BTreeSet<i64> = m
            .sk_set
            .iter()
            .chain(m.new_sk_set.iter().flatten())
            .copied()
            .collect();
        for id in members {
            let answer = async {
                let addr = addresses.get(&id)?;
                let client = safekeeper_client::mgmt_api::Client::new(
                    http.clone(),
                    addr.clone(),
                    options.safekeeper_jwt.clone().map(SecretString::from),
                );
                let resp = client.timeline_status(tenant, timeline).await.ok()?;
                resp.json::<safekeeper_api::models::TimelineStatus>()
                    .await
                    .ok()
            }
            .await;
            match answer {
                Some(status) => {
                    if best
                        .as_ref()
                        .is_none_or(|b| status.mconf.generation > b.generation)
                    {
                        best = Some(status.mconf);
                    }
                }
                None => all_answered = false,
            }
        }
        if let Some(conf) = best {
            let g = conf.generation.into_inner() as i32;
            if g > m.generation {
                m.generation = g;
                m.sk_set = conf.members.m.iter().map(|s| s.id.0 as i64).collect();
                m.new_sk_set = conf
                    .new_members
                    .map(|n| n.m.iter().map(|s| s.id.0 as i64).collect());
            }
        }
        if all_answered {
            read += 1;
        } else {
            silent.push(name.clone());
            if options.accept_silent_safekeepers {
                m.generation = m.generation.saturating_add(options.margin as i32);
            }
        }
    }
    (read, silent)
}

/// The controller (at the leader's address, or `--controller-address`) or, when
/// `pageservers_too`, a pageserver (at its HTTP port) that runs or may run, named; `None` when
/// everything checked is stopped. An address that accepts a connection runs; one that refuses it
/// is stopped; one that does not resolve or answer within 5 seconds may run, and refuses.
async fn something_may_run(
    records: &Records,
    running: &Running,
    pageservers_too: bool,
) -> Option<String> {
    if running.assume_stopped {
        eprintln!("--assume-stopped: not checking that the controller and pageservers are stopped");
        return None;
    }
    let mut targets = Vec::new();
    let controller = running
        .controller_address
        .clone()
        .or_else(|| records.leader.as_ref().map(|l| l.address.clone()));
    match controller {
        Some(address) => match reqwest::Url::parse(&address) {
            Ok(url) => match (url.host_str(), url.port_or_known_default()) {
                (Some(host), Some(port)) => targets.push((
                    format!("the storage controller at {address}"),
                    host.to_string(),
                    port,
                )),
                _ => return Some(format!("the controller's address {address} names no port")),
            },
            Err(e) => return Some(format!("the controller's address {address}: {e}")),
        },
        None => {
            return Some(
                "the records name no leader (the controller runs without --address-for-peers): \
                 give --controller-address, or --assume-stopped once it is stopped"
                    .to_string(),
            );
        }
    }
    if pageservers_too {
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
    }
    let mut runs = Vec::new();
    let mut unknown = Vec::new();
    for (what, host, port) in targets {
        let connect = tokio::net::TcpStream::connect((host.as_str(), port));
        match tokio::time::timeout(Duration::from_secs(5), connect).await {
            Ok(Ok(_)) => runs.push(what),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => {}
            Ok(Err(e)) => unknown.push(format!("{what} ({e})")),
            Err(_) => unknown.push(format!("{what} (no answer within 5 s)")),
        }
    }
    if !runs.is_empty() {
        return Some(format!("still running, stop it first: {}", runs.join(", ")));
    }
    if !unknown.is_empty() {
        return Some(format!(
            "cannot tell whether these are stopped (run the tool inside the cluster's network, \
             or check by hand and give --assume-stopped): {}",
            unknown.join(", ")
        ));
    }
    None
}

pub mod measure;

#[cfg(test)]
mod tests;
