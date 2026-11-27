//! How each record reads at start, compared with the copy and the marks: the table in
//! `storage_controller/kubernetes/README.md`, "Records that went back, or went missing". A pure
//! function of the three, so every row is tested here.

use std::collections::{BTreeMap, BTreeSet};

use super::copy::Mark;

/// What the start found, by record key (`<Kind>/<name>`).
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Verdict {
    /// Below the copy's revision for it, or below its mark's: refused.
    pub(crate) went_back: Vec<String>,
    /// In the copy, no object, no mark covering it: refused unless accepted.
    pub(crate) missing: Vec<String>,
    /// Exactly at a mark at or above the copy's revision: a delete decided and not done, to finish.
    pub(crate) unfinished: Vec<String>,
    /// Missing, and named by `--records-accept-deleted`: an accepted mark is written.
    pub(crate) accepted: Vec<String>,
    /// Named by `--records-accept-deleted` but not missing, nor accepted by an earlier start:
    /// refused, so the list is exact.
    pub(crate) wrongly_accepted: Vec<String>,
}

impl Verdict {
    pub(crate) fn refuses(&self) -> bool {
        !self.went_back.is_empty() || !self.missing.is_empty() || !self.wrongly_accepted.is_empty()
    }
}

/// `objects` and `copy` give each record's revision; `marks` the deletion marks.
pub(crate) fn classify(
    objects: &BTreeMap<String, u64>,
    copy: &BTreeMap<String, u64>,
    marks: &BTreeMap<String, Mark>,
    accept_deleted: &BTreeSet<String>,
) -> Verdict {
    let mut v = Verdict::default();
    for (key, &r) in objects {
        let c = copy.get(key).copied();
        let m = marks.get(key);
        if c.is_some_and(|c| r < c) || m.is_some_and(|m| r < m.revision) {
            v.went_back.push(key.clone());
        } else if let Some(m) = m {
            // At its mark: a delete not done, if the mark is the last word on the record (at or
            // above the copy's revision); above it, the mark is stale and the object lives.
            if r == m.revision && m.revision >= c.unwrap_or(0) {
                v.unfinished.push(key.clone());
            }
        }
    }
    for (key, &c) in copy {
        if objects.contains_key(key) {
            continue;
        }
        let covered = marks.get(key).is_some_and(|m| m.revision >= c);
        if covered {
            continue;
        }
        // A name accepted before and written again since (its accepted mark below the copy's
        // revision) is not accepted again: lost again, it is missing.
        if accept_deleted.contains(key) && !marks.get(key).is_some_and(|m| m.accepted) {
            v.accepted.push(key.clone());
        } else {
            v.missing.push(key.clone());
        }
    }
    // A name an earlier start accepted (its mark says so) is satisfied, so a restart with the
    // setting still in place starts.
    v.wrongly_accepted = accept_deleted
        .iter()
        .filter(|k| !v.accepted.contains(k) && !marks.get(*k).is_some_and(|m| m.accepted))
        .cloned()
        .collect();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(entries: &[(&str, u64)]) -> BTreeMap<String, u64> {
        entries.iter().map(|(k, r)| (k.to_string(), *r)).collect()
    }

    fn marks(entries: &[(&str, u64)]) -> BTreeMap<String, Mark> {
        entries
            .iter()
            .map(|(k, r)| {
                (
                    k.to_string(),
                    Mark {
                        revision: *r,
                        accepted: false,
                    },
                )
            })
            .collect()
    }

    fn classify_(
        objects: &[(&str, u64)],
        copy: &[(&str, u64)],
        m: &[(&str, u64)],
        accept: &[&str],
    ) -> Verdict {
        classify(
            &map(objects),
            &map(copy),
            &marks(m),
            &accept.iter().map(|s| s.to_string()).collect(),
        )
    }

    #[test]
    fn an_object_at_or_ahead_of_the_copy_is_fine() {
        let v = classify_(
            &[("Tenant/a", 5), ("Tenant/b", 7)],
            &[("Tenant/a", 5), ("Tenant/b", 6)],
            &[],
            &[],
        );
        assert_eq!(v, Verdict::default());
    }

    #[test]
    fn below_the_copy_or_below_its_mark_went_back() {
        let v = classify_(&[("Tenant/a", 4)], &[("Tenant/a", 5)], &[], &[]);
        assert_eq!(v.went_back, vec!["Tenant/a"]);
        // An object brought back below the revision it was deleted at
        let v = classify_(&[("Tenant/a", 3)], &[], &[("Tenant/a", 4)], &[]);
        assert_eq!(v.went_back, vec!["Tenant/a"]);
    }

    #[test]
    fn exactly_at_its_mark_is_a_delete_to_finish_only_when_the_mark_is_the_last_word() {
        // A crash between the mark and the delete
        let v = classify_(
            &[("Tenant/a", 4)],
            &[("Tenant/a", 4)],
            &[("Tenant/a", 4)],
            &[],
        );
        assert_eq!(v.unfinished, vec!["Tenant/a"]);
        assert!(!v.refuses());
        // At a mark the copy has gone past: a restore to that revision after the object went on;
        // it went back, and is never deleted
        let v = classify_(
            &[("Tenant/a", 4)],
            &[("Tenant/a", 5)],
            &[("Tenant/a", 4)],
            &[],
        );
        assert_eq!(v.went_back, vec!["Tenant/a"]);
        assert!(v.unfinished.is_empty());
    }

    #[test]
    fn above_its_mark_the_mark_is_stale_and_the_object_lives() {
        // A delete that lost its compare-and-swap to a later write
        let v = classify_(
            &[("Timeline/t", 6)],
            &[("Timeline/t", 5)],
            &[("Timeline/t", 5)],
            &[],
        );
        assert_eq!(v, Verdict::default());
    }

    #[test]
    fn a_record_with_no_object_is_deleted_only_when_a_mark_covers_it() {
        let v = classify_(&[], &[("Tenant/a", 5)], &[("Tenant/a", 5)], &[]);
        assert_eq!(v, Verdict::default());
        // No mark: went missing (a tenant made after the etcd snapshot)
        let v = classify_(&[], &[("Tenant/a", 5)], &[], &[]);
        assert_eq!(v.missing, vec!["Tenant/a"]);
        // A stale mark (the record written again after it) covers nothing
        let v = classify_(&[], &[("Timeline/t", 6)], &[("Timeline/t", 5)], &[]);
        assert_eq!(v.missing, vec!["Timeline/t"]);
    }

    #[test]
    fn only_what_is_missing_can_be_accepted_and_nothing_that_went_back() {
        let v = classify_(&[], &[("Tenant/a", 5)], &[], &["Tenant/a"]);
        assert_eq!(v.accepted, vec!["Tenant/a"]);
        assert!(!v.refuses());
        let v = classify_(&[("Tenant/a", 4)], &[("Tenant/a", 5)], &[], &["Tenant/a"]);
        assert_eq!(v.went_back, vec!["Tenant/a"]);
        assert_eq!(v.wrongly_accepted, vec!["Tenant/a"]);
        assert!(v.refuses());
    }

    #[test]
    fn a_name_accepted_before_is_satisfied_on_a_restart_with_the_setting_still_there() {
        let accepted = |r| {
            BTreeMap::from([(
                "Safekeeper/s".to_string(),
                Mark {
                    revision: r,
                    accepted: true,
                },
            )])
        };
        let accept = BTreeSet::from(["Safekeeper/s".to_string()]);
        // The copy still holds the record, covered by the accepted mark
        let v = classify(
            &map(&[]),
            &map(&[("Safekeeper/s", 5)]),
            &accepted(5),
            &accept,
        );
        assert!(!v.refuses(), "{v:?}");
        // The copy written since, without it
        let v = classify(&map(&[]), &map(&[]), &accepted(5), &accept);
        assert!(!v.refuses(), "{v:?}");
        // A plain mark (a delete of this controller's) is no acceptance
        let v = classify_(&[], &[], &[("Safekeeper/s", 5)], &["Safekeeper/s"]);
        assert_eq!(v.wrongly_accepted, vec!["Safekeeper/s"]);
    }

    #[test]
    fn a_name_accepted_then_made_again_and_lost_again_is_missing() {
        for key in [
            "Tenant/t",
            "Pageserver/pageserver-3",
            "Safekeeper/safekeeper-3",
            "Timeline/t.l",
            "MetadataHealth/t",
            "Leader/leader",
        ] {
            let accepted = BTreeMap::from([(
                key.to_string(),
                Mark {
                    revision: 5,
                    accepted: true,
                },
            )]);
            let accept = BTreeSet::from([key.to_string()]);
            // Made again above the accepted mark, then lost with the store: the copy has it at 7
            let v = classify(&map(&[]), &map(&[(key, 7)]), &accepted, &accept);
            assert_eq!(v.missing, vec![key], "{key}");
            assert!(v.accepted.is_empty(), "{key}");
            assert!(v.refuses(), "{key}");
            // Still there: fine
            let v = classify(&map(&[(key, 7)]), &map(&[(key, 7)]), &accepted, &accept);
            assert!(!v.refuses(), "{key}: {v:?}");
        }
    }

    #[test]
    fn a_name_made_again_above_its_mark_is_fine() {
        let v = classify_(
            &[("Pageserver/pageserver-5", 4)],
            &[("Pageserver/pageserver-5", 4)],
            &[("Pageserver/pageserver-5", 3)],
            &[],
        );
        assert_eq!(v, Verdict::default());
    }
}
