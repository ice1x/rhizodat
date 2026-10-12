//! The Rust core against the language-neutral vectors in `conformance/`.
//!
//! The same files are run by the Python reference
//! (`tests/conformance/test_vectors.py`); both must pass them bit for bit.
//! The crash matrix is replayed on the fakes in `tests/fakes`, the port of the
//! reference's `tests/fakes.py`, with the same fault points.

mod fakes;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datorium_core::clock::{ClockError, HlcTimestamp, HybridLogicalClock};
use datorium_core::coordinator::{Coordinator, Outcome};
use datorium_core::dsn::parse_dsn;
use datorium_core::gid::{gid_for, parse_gid};
use datorium_core::identity::{IdError, IdGenerator};
use datorium_core::journal::{Journal, State, TransitionError, check_transition};
use datorium_core::recovery::Recovery;
use datorium_core::retry::Retry;
use datorium_core::timer::NoWait;
use fakes::{Fault, FaultyJournal, Wall, World, block, run};
use serde_json::{Value, json};
use uuid::Uuid;

fn vectors() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance")
}

fn load(name: &str) -> Value {
    let raw = std::fs::read_to_string(vectors().join(name)).expect("vector file");
    serde_json::from_str(&raw).expect("valid JSON")
}

fn int(v: &Value) -> i64 {
    v.as_i64().unwrap_or_else(|| panic!("not an int64: {v}"))
}

fn uint(v: &Value) -> u64 {
    v.as_u64().unwrap_or_else(|| panic!("not a uint64: {v}"))
}

fn text(v: &Value) -> &str {
    v.as_str().unwrap_or_else(|| panic!("not a string: {v}"))
}

#[test]
fn every_vector_file_is_exercised() {
    let used: BTreeSet<&str> = [
        "uuid7.json",
        "hlc.json",
        "gid.json",
        "journal.json",
        "dsn.json",
        "crash_matrix.json",
    ]
    .into();
    let present: BTreeSet<String> = std::fs::read_dir(vectors())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(present, used.iter().map(ToString::to_string).collect());
}

// --- UUIDv7 ----------------------------------------------------------------------------

#[test]
fn uuid7() {
    for case in load("uuid7.json")["cases"].as_array().unwrap() {
        let name = text(&case["name"]);
        let clock = Arc::new(Mutex::new(0_i64));
        let queue: Arc<Mutex<VecDeque<(u32, u64)>>> = Arc::default();
        let generator = {
            let (clock, queue) = (clock.clone(), queue.clone());
            IdGenerator::with_sources(
                move || *clock.lock().unwrap(),
                move |bits| {
                    let (want, value) = queue.lock().unwrap().pop_front().expect(
                        "the generator asked for more random values than the vector provides",
                    );
                    assert_eq!(want, bits, "random bit width");
                    value
                },
            )
        };
        let mut got = None;
        for step in case["steps"].as_array().unwrap() {
            *clock.lock().unwrap() = int(&step["clock_ms"]);
            let repeat = step.get("repeat").map_or(1, uint);
            for _ in 0..repeat {
                *queue.lock().unwrap() = step["random"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| (u32::try_from(uint(&r["bits"])).unwrap(), uint(&r["value"])))
                    .collect();
                let result = generator.generate();
                if let Some(error) = step.get("error") {
                    let kind = match result {
                        Err(IdError::InvalidClock(_)) => "invalid_clock",
                        Err(IdError::TimestampExhausted) => "timestamp_exhausted",
                        Ok(id) => panic!("{name}: expected an error, got {id}"),
                    };
                    assert_eq!(kind, text(error), "{name}");
                } else {
                    got = Some(result.unwrap_or_else(|e| panic!("{name}: {e}")));
                }
                assert!(
                    queue.lock().unwrap().is_empty(),
                    "{name}: random values left unused"
                );
            }
            if let Some(want) = step.get("uuid") {
                assert_eq!(got, Some(Uuid::parse_str(text(want)).unwrap()), "{name}");
            }
        }
    }
}

// --- HLC -------------------------------------------------------------------------------

#[test]
fn hlc() {
    for case in load("hlc.json")["cases"].as_array().unwrap() {
        let name = text(&case["name"]);
        let wall = Arc::new(Mutex::new(0_i64));
        let clock = {
            let wall = wall.clone();
            HybridLogicalClock::with_wall(
                move || *wall.lock().unwrap(),
                uint(&case["max_offset_ms"]),
            )
        };
        for step in case["steps"].as_array().unwrap() {
            *wall.lock().unwrap() = int(&step["wall_ms"]);
            let before = clock.last();
            let result = if text(&step["op"]) == "now" {
                clock.now()
            } else {
                let remote = &step["remote"];
                let remote = HlcTimestamp::new(
                    uint(&remote["wall_ms"]),
                    u16::try_from(uint(&remote["logical"])).unwrap(),
                )
                .unwrap();
                clock.update(remote)
            };
            let expect = &step["expect"];
            if let Some(error) = expect.get("error") {
                let kind = match result {
                    Err(ClockError::ClockSkew { .. }) => "clock_skew",
                    Err(ClockError::LogicalOverflow { .. }) => "logical_overflow",
                    other => panic!("{name}: expected {error}, got {other:?}"),
                };
                assert_eq!(kind, text(error), "{name}");
                if kind == "clock_skew" {
                    assert_eq!(
                        clock.last(),
                        before,
                        "{name}: a rejected receive changes nothing"
                    );
                }
                continue;
            }
            let ts = result.unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(
                (ts.wall_ms(), u64::from(ts.logical()), ts.encode()),
                (
                    uint(&expect["wall_ms"]),
                    uint(&expect["logical"]),
                    int(&expect["encoded"])
                ),
                "{name}"
            );
            assert_eq!(
                HlcTimestamp::decode(int(&expect["encoded"])).unwrap(),
                ts,
                "{name}"
            );
        }
    }
}

// --- gid -------------------------------------------------------------------------------

#[test]
fn gid() {
    let doc = load("gid.json");
    for case in doc["gid_for"].as_array().unwrap() {
        let txn = Uuid::parse_str(text(&case["dat_txn"])).unwrap();
        assert_eq!(gid_for(txn, text(&case["store"])), text(&case["gid"]));
    }
    for case in doc["parse"].as_array().unwrap() {
        let gid = text(&case["gid"]);
        let expect = match &case["result"] {
            Value::Null => None,
            r => Some((
                Uuid::parse_str(text(&r["dat_txn"])).unwrap(),
                text(&r["store"]).to_owned(),
            )),
        };
        assert_eq!(parse_gid(gid), expect, "{gid:?}");
    }
}

// --- journal ---------------------------------------------------------------------------

#[test]
fn journal_states_are_exactly_these() {
    let doc = load("journal.json");
    let want: Vec<&str> = doc["states"].as_array().unwrap().iter().map(text).collect();
    let got: Vec<&str> = State::ALL.iter().map(|s| s.as_str()).collect();
    assert_eq!(got, want);
    for s in State::ALL {
        assert_eq!(s.as_str().parse::<State>(), Ok(s));
    }
    assert!("active".parse::<State>().is_err());
}

#[test]
fn journal_transitions() {
    let doc = load("journal.json");
    let state = |v: &Value| text(v).parse::<State>().unwrap();
    for edge in doc["transitions"].as_array().unwrap() {
        let (from, to) = (state(&edge["from"]), state(&edge["to"]));
        let result = check_transition(from, to, Some(1));
        if edge["legal"].as_bool().unwrap() {
            assert_eq!(result, Ok(()), "{from} -> {to}");
        } else {
            assert_eq!(
                result,
                Err(TransitionError::Illegal { from, to }),
                "{from} -> {to}"
            );
        }
    }
    for s in doc["requires_dat_tx_time"].as_array().unwrap() {
        let to = state(s);
        let edge = doc["transitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| state(&e["to"]) == to && e["legal"].as_bool().unwrap())
            .unwrap();
        let from = state(&edge["from"]);
        assert_eq!(
            check_transition(from, to, None),
            Err(TransitionError::MissingTxTime)
        );
    }
}

// --- DSN -------------------------------------------------------------------------------

#[test]
fn dsn() {
    for case in load("dsn.json")["cases"].as_array().unwrap() {
        let raw = text(&case["raw"]);
        let expect = &case["expect"];
        let result = parse_dsn(raw);
        if expect.get("error").and_then(Value::as_bool) == Some(true) {
            let err = result.expect_err(raw);
            for secret in ["s3cret", "p@ss", "pw"] {
                assert!(
                    !err.to_string().contains(secret),
                    "{raw}: error echoes a secret"
                );
            }
            continue;
        }
        let d = result.unwrap_or_else(|e| panic!("{raw}: {e}"));
        let params: BTreeMap<&str, &str> = d
            .params()
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let got = json!({
            "store": d.store(),
            "transport": d.transport(),
            "host": d.host(),
            "port": d.port(),
            "user": d.user(),
            "password": d.password(),
            "path": d.path(),
            "database": d.database(),
            "params": params,
            "redacted": d.redacted(),
        });
        assert_eq!(&got, expect, "{raw}");
        assert_eq!(d.unredacted(), raw);
        // Display and Debug show the redacted form only.
        assert_eq!(d.to_string(), d.redacted(), "{raw}");
        assert_eq!(
            format!("{d:?}"),
            format!("Dsn({:?})", d.redacted()),
            "{raw}"
        );
    }
}

// --- crash matrix ----------------------------------------------------------------------

fn steps(seen: Vec<String>) -> Vec<String> {
    seen.into_iter()
        .filter(|p| p.rsplit_once('.').is_none_or(|(_, last)| last != "after"))
        .collect()
}

fn snapshot(world: &World, journal: &FaultyJournal, txn: Uuid) -> Value {
    let state = block(journal.get(txn)).unwrap().map(|r| r.state.as_str());
    let mut prepared: Vec<String> = world
        .leftover_prepared()
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    prepared.sort();
    json!({"journal": state, "prepared": prepared, "applied": world.applied_stores(txn)})
}

#[test]
fn crash_matrix() {
    let doc = load("crash_matrix.json");
    let stale_after_ms = int(&doc["stale_after_ms"]);
    let mut everything: Vec<String> = doc["stores"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| text(&s["name"]).to_owned())
        .collect();
    everything.sort();
    for case in doc["cases"].as_array().unwrap() {
        let name = case["crash_at"].as_str().unwrap_or("none").to_owned();
        let mut world = World::default();
        for store in doc["stores"].as_array().unwrap() {
            world.store(text(&store["name"]), store["two_phase"].as_bool().unwrap());
        }
        let wall = Wall::default();
        let journal = Arc::new(FaultyJournal::new(world.faults.clone(), wall.clone()));
        if let Some(point) = case["crash_at"].as_str() {
            world.faults.add(point, Fault::Crash, 1);
        }
        let txn = IdGenerator::new().generate().unwrap();
        let coordinator = Coordinator::new(
            Some(journal.clone()),
            world.participants(),
            Arc::new(HybridLogicalClock::new()),
            Arc::new(NoWait::default()),
        )
        .with_finish(
            Retry::new(
                1,
                std::time::Duration::from_secs(1),
                std::time::Duration::ZERO,
                std::time::Duration::ZERO,
            )
            .unwrap(),
        );
        let commit = match run(coordinator.commit(txn, &mut world.branches())) {
            Ok(Ok(Outcome::Committed { .. })) => "committed",
            Ok(Ok(Outcome::Pending { .. })) => "committed_pending",
            Ok(Err(e)) => panic!("{name}: {e}"),
            Err(()) => "crashed",
        };
        assert_eq!(commit, text(&case["commit"]), "{name}");
        assert_eq!(
            snapshot(&world, &journal, txn),
            case["after_crash"],
            "{name}"
        );
        assert_eq!(
            steps(world.faults.seen()),
            serde_json::from_value::<Vec<String>>(case["trace"].clone()).unwrap(),
            "{name}"
        );

        world.faults.clear_plan();
        world.faults.clear_seen();
        wall.advance(stale_after_ms + 1);
        let recovery = {
            let wall = wall.clone();
            Recovery::new(journal.clone(), world.participants(), stale_after_ms)
                .unwrap()
                .with_wall(move || wall.get())
        };
        let report = block(recovery.run_once(100)).unwrap();
        let action = if report.aborted.contains(&txn) {
            "aborted"
        } else if report.finished.contains(&txn) {
            "finished"
        } else {
            "none"
        };
        assert_eq!(action, text(&case["recovery"]), "{name}");
        assert_eq!(
            steps(world.faults.seen()),
            serde_json::from_value::<Vec<String>>(case["recovery_trace"].clone()).unwrap(),
            "{name}"
        );
        let after = snapshot(&world, &journal, txn);
        let applied: Vec<String> = serde_json::from_value(after["applied"].clone()).unwrap();
        assert!(
            applied.is_empty() || applied == everything,
            "{name}: recovery must end all-or-nothing"
        );
        let summary = json!({
            "journal": after["journal"],
            "applied": if applied.is_empty() { "none" } else { "all" },
            "prepared": after["prepared"],
        });
        assert_eq!(summary, case["after_recovery"], "{name}");
    }
}
