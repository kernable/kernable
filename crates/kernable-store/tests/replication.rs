//! The record log (design/replication.md): a standby that applies a primary's records, or
//! takes its image and then follows, holds the same store, for every pair of backends; records
//! that do not continue its log are refused without a trace.

use std::{fs, path::PathBuf};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        AggregateIndex, Attribute, Command, Grant, ListenerRecord, Membership, MembershipRecord,
        Middleware, NewEvent, ParkedEvent, Partition, PartitionMap, Permission, Rejected,
        ReplicatedRecord, RequestContext, RevocationRecord, Signature, StoredKey, TenantPartition,
        When, WorkerRecord, log_record::Record,
    },
    signing,
};
use kernable_store::{
    Durability, Error, FileStore, Head, SqliteStore, Store,
    invariants::Log,
    replication::{self, chained},
};

#[derive(Clone, Copy, Debug)]
enum Kind {
    File,
    Sqlite,
}

use Kind::{File, Sqlite};

const PAIRS: [(Kind, Kind); 4] = [
    (File, File),
    (File, Sqlite),
    (Sqlite, File),
    (Sqlite, Sqlite),
];

/// A directory of stores, removed afterwards.
struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "kernable-replication-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Dir(path)
    }

    /// Opens store `name`, without its record log (SQLite).
    fn plain(&self, kind: Kind, name: &str) -> Box<dyn Store> {
        match kind {
            File => Box::new(
                FileStore::open_with(self.0.join(format!("{name}.log")), Durability::Normal)
                    .unwrap(),
            ),
            Sqlite => Box::new(
                SqliteStore::open_with(self.0.join(format!("{name}.db")), Durability::Normal)
                    .unwrap(),
            ),
        }
    }

    /// Opens store `name` with its record log on.
    fn open(&self, kind: Kind, name: &str) -> Box<dyn Store> {
        let store = self.plain(kind, name);
        store.keep_record_log(true).unwrap();
        store
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn kernel() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

fn kernel_ids() -> Vec<String> {
    vec![signing::key_id(&kernel().verifying_key())]
}

fn cmd(id: &str, aggregate_id: &str) -> Command {
    Command {
        command_id: id.into(),
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: aggregate_id.into(),
        command_type: "Place".into(),
        payload: format!(r#"{{"id":"{id}"}}"#).into_bytes(),
        client_sig: Some(Signature {
            key_id: "client".into(),
            sig: vec![9; 64],
        }),
        context: Some(RequestContext {
            sender_identity: "ada".into(),
            correlation_id: format!("corr-{id}"),
            expected_version: Some(0),
            attributes: vec![Attribute {
                key: "channel".into(),
                value: "web".into(),
            }],
            ..Default::default()
        }),
        vault_refs: Vec::new(),
    }
}

fn ev(event_type: &str) -> NewEvent {
    NewEvent {
        event_type: event_type.into(),
        payload: format!(r#"{{"type":"{event_type}"}}"#).into_bytes(),
        schema: 1,
        vault_refs: Vec::new(),
        erases: String::new(),
    }
}

fn sig() -> Signature {
    Signature {
        key_id: "worker".into(),
        sig: vec![7; 64],
    }
}

fn holds(n: u8) -> AggregateIndex {
    AggregateIndex {
        keys: vec![StoredKey {
            name: "number".into(),
            global: false,
            id: vec![n; 32],
            vault_ref: String::new(),
        }],
        facts: vec!["open".into()],
    }
}

fn listener(name: &str, position: u64, at: i64) -> ListenerRecord {
    ListenerRecord {
        name: name.into(),
        key_id: "app".into(),
        order: 0,
        event_types: vec!["orders.*".into()],
        position,
        middlewares: vec![Middleware {
            order: 1,
            types: vec!["orders.*".into()],
            when: When::Before.into(),
        }],
        last_seen: at,
        ..Default::default()
    }
}

/// One round of writes, of every kind the record log carries.
fn writes(s: &dyn Store, round: u8) {
    let at = i64::from(round) * 100;
    let id = |x: &str| format!("{x}-{round}");
    let (a, b) = (id("a"), id("b"));
    for (i, (c, agg)) in [("c1", &a), ("c2", &b), ("c3", &a), ("c4", &b), ("c5", &a)]
        .into_iter()
        .enumerate()
    {
        s.enqueue(&cmd(&id(c), agg), at + i as i64).unwrap();
    }
    let n = round * 10;
    s.commit(
        &id("c1"),
        0,
        &[ev("Opened"), ev("Named")],
        &sig(),
        Some(&holds(n + 1)),
        at + 5,
    )
    .unwrap();
    s.commit(&id("c2"), 0, &[ev("Opened")], &sig(), None, at + 6)
        .unwrap();
    s.reject(
        &id("c3"),
        Rejected {
            code: "closed".into(),
            message: "the order is closed".into(),
            ..Default::default()
        },
    )
    .unwrap();
    // Keys and facts only, no events; then a commit that frees a's key.
    s.commit(&id("c4"), 1, &[], &sig(), Some(&holds(n + 2)), at + 7)
        .unwrap();
    s.commit(
        &id("c5"),
        2,
        &[ev("Closed")],
        &sig(),
        Some(&AggregateIndex::default()),
        at + 8,
    )
    .unwrap();
    s.enqueue(&cmd(&id("c6"), &b), at + 9).unwrap(); // stays pending
    let mut reactor = listener("reactor", u64::from(round), at);
    reactor.parked = vec![ParkedEvent {
        position: 1,
        event_type: "orders.Opened".into(),
        code: "handler_error".into(),
        message: "boom".into(),
        attempts: 3,
        parked_at: at,
        retry: round % 2 == 1,
    }];
    reactor.parked_changed = true;
    let mut gone = listener("gone", 0, at);
    gone.removed = round > 0;
    s.save_listeners(&[listener("shop", 3, at), reactor, gone])
        .unwrap();
    s.save_listeners(&[listener("shop", 4, at + 1)]).unwrap();
    s.save_workers(&[WorkerRecord {
        aggregate_type: "orders.Order".into(),
        key_id: "worker".into(),
        command_types: vec!["orders.Place".into()],
        service: "shop".into(),
        last_seen: at,
        removed: false,
    }])
    .unwrap();
    s.save_membership(&MembershipRecord {
        membership: Some(Membership {
            service: "shop".into(),
            workers: vec!["orders.Order".into()],
            listeners: vec!["shop".into()],
            middlewares: Vec::new(),
            metadata: vec![Attribute {
                key: "version".into(),
                value: round.to_string(),
            }],
            issued_at: at,
        }),
        sig: Some(Signature {
            key_id: "manager".into(),
            sig: vec![1; 64],
        }),
        applied_at: at,
    })
    .unwrap();
    let grant = |issued_at| Grant {
        key_id: "app".into(),
        permissions: vec![Permission {
            action: "submit".into(),
            types: vec!["orders.*".into()],
        }],
        succeeds: String::new(),
        issued_at,
        root_sig: Some(Signature {
            key_id: "root".into(),
            sig: vec![2; 64],
        }),
    };
    s.save_grant(&grant(at + 10)).unwrap();
    s.save_grant(&grant(at)).unwrap(); // older: kept out
    let revocation = RevocationRecord {
        key_id: id("stolen"),
        since: at,
        created_at: at,
        kernel_sig: Some(signing::sign(&kernel(), b"revocation")),
    };
    s.revoke(&revocation).unwrap();
    s.revoke(&revocation).unwrap(); // revoked already
    s.checkpoint(&kernel(), at + 11).unwrap();
    s.data_changed("t1", "orders", &id("invoice")).unwrap();
    let map = |epoch: u64| PartitionMap {
        epoch,
        partitions: vec![Partition {
            name: "p1".into(),
            addresses: vec!["unix:/p1.sock".into()],
        }],
        tenants: vec![TenantPartition {
            tenant: id("t"),
            partition: "p1".into(),
        }],
        default: "p1".into(),
        control: "p1".into(),
        issued_at: at,
        ..Default::default()
    };
    s.save_partition_map(&map(u64::from(round) + 1)).unwrap();
    s.save_partition_map(&map(0)).unwrap(); // older: kept out
}

/// Applies the primary's records to the standby, `batch` at a time, until it has them all.
fn follow(primary: &dyn Store, standby: &dyn Store, batch: usize) {
    loop {
        let head = standby.record_head().unwrap();
        assert_eq!(
            primary.record_hash_at(head.seq).unwrap(),
            Some(head.hash.clone()),
            "the primary holds the standby's head"
        );
        let records = primary
            .records_after(head.seq, batch)
            .unwrap()
            .expect("the primary's log reaches back to the standby's head");
        if records.is_empty() {
            return;
        }
        standby.apply(&records).unwrap();
    }
}

/// Fills an empty standby from the primary's image.
fn take_image(primary: &dyn Store, standby: &dyn Store) -> Head {
    let mut records = Vec::new();
    let head = primary
        .image(&mut |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
    for chunk in records.chunks(4) {
        standby.apply_image(chunk).unwrap();
    }
    standby.start_record_log(&head).unwrap();
    head
}

/// Everything a kernel reads from a store.
fn same(primary: &dyn Store, standby: &dyn Store, what: &str) {
    let ids = kernel_ids();
    let (p, s) = (
        Log::read(primary, &ids).unwrap(),
        Log::read(standby, &ids).unwrap(),
    );
    assert_eq!(p.events, s.events, "{what}: events");
    assert_eq!(p.commands, s.commands, "{what}: commands");
    assert_eq!(p.results, s.results, "{what}: results");
    assert_eq!(p.pending, s.pending, "{what}: pending");
    assert_eq!(p.listeners, s.listeners, "{what}: listeners");
    assert_eq!(p.index, s.index, "{what}: keys and facts");
    assert_eq!(p.head, s.head, "{what}: head");
    // The test signs with dummy keys, so S1 flags the same signatures on both; every other
    // invariant holds.
    assert_eq!(
        format!("{:?}", p.check()),
        format!("{:?}", s.check()),
        "{what}: invariants"
    );
    let broken: Vec<_> = s
        .check()
        .into_iter()
        .filter(|v| v.invariant != "S1 integrity")
        .collect();
    assert!(broken.is_empty(), "{what}: {broken:?}");
    let pending = |st: &dyn Store| -> Vec<String> {
        let pending = st.pending().unwrap();
        pending.into_iter().map(|p| p.command.command_id).collect()
    };
    assert_eq!(pending(primary), pending(standby), "{what}: queue order");
    assert_eq!(
        primary.workers().unwrap(),
        standby.workers().unwrap(),
        "{what}"
    );
    assert_eq!(
        primary.memberships().unwrap(),
        standby.memberships().unwrap(),
        "{what}"
    );
    assert_eq!(
        primary.grants().unwrap(),
        standby.grants().unwrap(),
        "{what}"
    );
    assert_eq!(
        primary.revocations().unwrap(),
        standby.revocations().unwrap(),
        "{what}"
    );
    assert_eq!(
        primary.partition_map().unwrap(),
        standby.partition_map().unwrap(),
        "{what}: partition map"
    );
    assert_eq!(
        primary.event_schemas().unwrap(),
        standby.event_schemas().unwrap(),
        "{what}"
    );
    assert_eq!(
        format!("{:?}", primary.verify(&ids).unwrap()),
        format!("{:?}", standby.verify(&ids).unwrap()),
        "{what}: verify"
    );
    assert_eq!(
        primary.record_head().unwrap(),
        standby.record_head().unwrap(),
        "{what}: record head"
    );
}

#[test]
fn a_standby_that_applies_the_records_holds_the_same_store() {
    for (p, s) in PAIRS {
        let what = format!("{p:?} to {s:?}");
        let dir = Dir::new(&format!("follow-{p:?}-{s:?}"));
        let (primary, standby) = (dir.open(p, "primary"), dir.open(s, "standby"));
        writes(&*primary, 0);
        follow(&*primary, &*standby, 3);
        same(&*primary, &*standby, &what);
        writes(&*primary, 1);
        follow(&*primary, &*standby, 1000);
        same(&*primary, &*standby, &what);

        // Both open again where they stood.
        drop((primary, standby));
        let (primary, standby) = (dir.open(p, "primary"), dir.open(s, "standby"));
        same(&*primary, &*standby, &format!("{what}, reopened"));
        writes(&*primary, 2);
        follow(&*primary, &*standby, 7);
        same(&*primary, &*standby, &format!("{what}, reopened"));
    }
}

#[test]
fn a_standby_takes_an_image_then_follows() {
    for (p, s) in PAIRS {
        let what = format!("{p:?} to {s:?}");
        let dir = Dir::new(&format!("image-{p:?}-{s:?}"));
        // The primary wrote before its record log was on (SQLite): no standby can follow it
        // from the start.
        let primary = dir.plain(p, "primary");
        writes(&*primary, 0);
        primary.keep_record_log(true).unwrap();
        if let Sqlite = p {
            assert_eq!(primary.records_after(0, 10).unwrap(), None, "{what}");
        }
        writes(&*primary, 1);

        let standby = dir.open(s, "standby");
        let head = take_image(&*primary, &*standby);
        assert_eq!(standby.record_head().unwrap(), head, "{what}");
        same(&*primary, &*standby, &what);
        writes(&*primary, 2);
        follow(&*primary, &*standby, 5);
        same(&*primary, &*standby, &what);

        drop(standby);
        let standby = dir.open(s, "standby");
        writes(&*primary, 3);
        follow(&*primary, &*standby, 5);
        same(&*primary, &*standby, &format!("{what}, reopened"));
    }
}

#[test]
fn records_that_do_not_continue_the_log_are_refused() {
    for (p, s) in PAIRS {
        let what = format!("{p:?} to {s:?}");
        let dir = Dir::new(&format!("refused-{p:?}-{s:?}"));
        let (primary, standby) = (dir.open(p, "primary"), dir.open(s, "standby"));
        writes(&*primary, 0);
        let records = primary.records_after(0, 1000).unwrap().unwrap();
        let refused = |records: &[ReplicatedRecord], why: &str| {
            let head = standby.record_head().unwrap();
            let result = standby.apply(records);
            assert!(
                matches!(result, Err(Error::Diverged(_))),
                "{what}: {why}: {result:?}"
            );
            assert_eq!(standby.record_head().unwrap(), head, "{what}: {why}");
        };

        refused(&records[1..2], "a gap");
        let mut tampered = records[0].clone();
        tampered.hash[0] ^= 1;
        refused(&[tampered], "a wrong hash");

        // Up to the first commit; then the commit with a changed event, rehashed so that
        // only the event's own hash tells.
        let first_commit = records
            .iter()
            .position(|r| {
                matches!(
                    r.record.as_ref().and_then(|l| l.record.as_ref()),
                    Some(Record::Committed(_))
                )
            })
            .unwrap();
        standby.apply(&records[..first_commit]).unwrap();
        let head = standby.record_head().unwrap();
        let Some(Record::Committed(mut commit)) =
            records[first_commit].record.clone().and_then(|l| l.record)
        else {
            unreachable!()
        };
        let event = commit.events[0].event.as_mut().unwrap();
        event.payload = br#"{"type":"Forged"}"#.to_vec();
        refused(
            &[replication::next(&head, Record::Committed(commit))],
            "a forged event",
        );
        let enqueued = records[0].record.clone().and_then(|l| l.record).unwrap();
        refused(
            &[replication::next(&head, enqueued)],
            "a command enqueued twice",
        );
        refused(
            &[replication::next(&head, Record::Format(Default::default()))],
            "a record that starts a log",
        );

        // What was refused left nothing behind: the rest follows.
        follow(&*primary, &*standby, 1000);
        same(&*primary, &*standby, &what);
    }
}

#[test]
fn a_pruned_or_restarted_sqlite_log_sends_a_standby_to_an_image() {
    let dir = Dir::new("pruned");
    let primary = dir.open(Sqlite, "primary");
    writes(&*primary, 0);
    let head = primary.record_head().unwrap();
    assert_eq!(
        primary.record_hash_at(0).unwrap(),
        Some(replication::ZERO.to_vec())
    );
    let keep = head.seq - 2;
    let all = primary.records_after(0, 1000).unwrap().unwrap();
    primary.prune_records(keep).unwrap();
    assert_eq!(primary.records_after(0, 10).unwrap(), None);
    assert_eq!(primary.records_after(keep - 1, 10).unwrap(), None);
    assert_eq!(primary.record_hash_at(keep - 1).unwrap(), None);
    assert_eq!(
        primary.record_hash_at(keep).unwrap(),
        Some(all[keep as usize - 1].hash.clone())
    );
    let rest = primary.records_after(keep, 10).unwrap().unwrap();
    assert_eq!(rest, all[keep as usize..].to_vec());

    // Off and on again: a new log that continues nobody's.
    primary.keep_record_log(false).unwrap();
    assert!(matches!(primary.record_head(), Err(Error::NoRecordLog)));
    writes(&*primary, 1);
    primary.keep_record_log(true).unwrap();
    let new = primary.record_head().unwrap();
    assert_eq!(new.seq, 1);
    assert_ne!(new.hash, all[0].hash);
    assert_eq!(primary.records_after(0, 10).unwrap(), None);
    assert_eq!(primary.record_hash_at(0).unwrap(), None);
}

#[test]
fn the_file_log_finds_every_record_across_its_marks() {
    let dir = Dir::new("marks");
    let primary = dir.open(File, "primary");
    for i in 0..3000 {
        primary
            .save_listeners(&[listener("shop", i, i as i64)])
            .unwrap();
    }
    drop(primary);
    let primary = dir.open(File, "primary");
    let all = primary.records_after(0, 10_000).unwrap().unwrap();
    assert_eq!(all.len(), 3000);
    chained(&Head::default(), &all).unwrap();
    for seq in [0u64, 1, 1023, 1024, 1025, 2047, 2048, 2999, 3000] {
        let hash = primary.record_hash_at(seq).unwrap().unwrap();
        let expected = match seq {
            0 => replication::ZERO.to_vec(),
            n => all[n as usize - 1].hash.clone(),
        };
        assert_eq!(hash, expected, "hash of record {seq}");
        let after = primary.records_after(seq, 10).unwrap().unwrap();
        assert_eq!(
            after,
            all[seq as usize..(seq as usize + 10).min(3000)].to_vec(),
            "records after {seq}"
        );
    }
    assert_eq!(primary.records_after(3001, 1).unwrap(), None);
}
