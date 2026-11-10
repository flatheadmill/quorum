use quorum::projection::{Projection, Tape, agreement_refusal};
use quorum::replay::{self, Mode};
use quorum::tape::{self, Header, NodeSpec, WorldConfig};
use quorum::{Context, Input, Node, NodeId, Simulator, Slot};
use quorum::{scenarios, vote};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::{AtomicU64, Ordering},
};

type ReceiveAction = dyn FnMut(&mut Context, &Input);

struct Script {
    crashed: bool,
    action: Box<ReceiveAction>,
}
impl Script {
    fn new(f: impl FnMut(&mut Context, &Input) + 'static) -> Self {
        Self {
            crashed: false,
            action: Box::new(f),
        }
    }
    fn silent() -> Self {
        Self::new(|_, _| {})
    }
}
impl Node for Script {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        (self.action)(ctx, input)
    }
    fn crashed(&self) -> bool {
        self.crashed
    }
    fn set_crashed(&mut self, value: bool) {
        self.crashed = value;
    }
}
fn header(nodes: i32, tick_interval: u64) -> Header {
    Header::new(
        "fixture",
        7,
        WorldConfig {
            base_latency: 1,
            jitter: 0,
            tick_interval,
            deadlock_threshold: 100,
        },
        (0..nodes)
            .map(|i| NodeSpec {
                node: NodeId(i),
                algorithm: "fixture".into(),
                config: json!({}),
            })
            .collect(),
        vec![],
    )
}
fn world(header: Header) -> Simulator {
    Simulator::new(header, Box::new(io::sink())).unwrap()
}
fn parsed(sim: &Simulator) -> Tape {
    Tape::read(&tape::encode(sim.rows())).unwrap()
}
fn evidence(name: &str, bytes: &[u8]) {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/slice2-evidence");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join(name), bytes).unwrap();
}

fn tmp_file(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "quorum-{}-{}-{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn existing_scenarios_are_green_deterministic_and_replayable() {
    for scenario in ["raft", "ping-pong"] {
        let a = scenarios::capture(scenario, Box::new(io::sink())).unwrap();
        let b = scenarios::capture(scenario, Box::new(io::sink())).unwrap();
        assert_eq!(tape::encode(a.rows()), tape::encode(b.rows()));
        let tape = parsed(&a);
        a.check_projection().unwrap();
        let replay = replay::replay(&tape, Mode::Strict, scenarios::factory).unwrap();
        assert_eq!(replay.completed_answers, tape.projection.inputs.len());
        assert_eq!(replay.tail, "finalized");
        for end in 0..=tape.rows.len() {
            Projection::from_rows(&tape.rows[..end]).unwrap();
        }
        if scenario == "raft" {
            for id in [NodeId(0), NodeId(1)] {
                assert_eq!(a.chosen()[&id][&Slot(0)], json!("alpha"));
            }
            let claims: Vec<_> = a
                .rows()
                .iter()
                .filter(|e| e.what == "result" && e.why == "commit")
                .map(|e| e.when)
                .collect();
            assert_eq!(claims, vec![27, 31]);
            assert_eq!(vote::verify(&tape).unwrap().grants.len(), 1);
        } else {
            assert!(a.chosen().values().all(BTreeMap::is_empty));
        }
    }
}

#[test]
fn refusal_preserves_the_fourth_desire_and_every_prefix() {
    let mut sim = world(header(1, 0));
    let mut calls = 0;
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(move |ctx, _| {
            calls += 1;
            if calls == 1 {
                ctx.commit(Slot(7), json!("alpha"));
            } else {
                ctx.save(json!({"record":"A"}));
                ctx.send(NodeId(9), json!({"type":"B"}));
                ctx.commit(Slot(7), json!("beta"));
                ctx.send(NodeId(9), json!({"type":"D"}));
            }
        })),
    )
    .unwrap();
    sim.client_message(NodeId(0), json!(null)).unwrap();
    assert!(
        sim.client_message(NodeId(0), json!(null))
            .unwrap_err()
            .contains("self_split_brain")
    );
    assert!(sim.run(1).is_err());
    let prefix = sim.check_projection().unwrap();
    assert_eq!(prefix.tail(), "refused");
    assert_eq!(prefix.account().unreached_desires.len(), 1);
    let answer = prefix.answers.values().last().unwrap();
    assert_eq!(answer.desires.len(), 4);
    assert!(answer.closed);
    let fourth = answer.desires[3].0;
    assert!(!sim.rows().iter().any(
        |r| r.what == "attempt" && r.get::<quorum::DesireId>("desire_id").ok() == Some(fourth)
    ));
    assert_eq!(sim.persistent()[&NodeId(0)], vec![json!({"record":"A"})]);
    assert_eq!(sim.pending().len(), 1);
    sim.finalize("refused").unwrap();
    let bytes = tape::encode(sim.rows());
    evidence("refused-third-effect.jsonl", &bytes);
    let mut classes = BTreeSet::new();
    classes.insert(Tape::read(&[]).unwrap().projection.tail());
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            classes.insert(Tape::read(&bytes[..=i]).unwrap().projection.tail());
        }
    }
    for expected in [
        "empty",
        "between_turns",
        "awaiting_dispatch",
        "open_invocation",
        "partial_answer",
        "interrupted_drain",
        "open_effect",
        "refused",
        "finalized",
    ] {
        assert!(classes.contains(expected), "{expected}");
    }
    assert!(Tape::read(&bytes[..bytes.len() - 1]).is_err());
}

#[test]
fn dispatch_is_on_disk_before_receive_and_private_desires_do_not_escape_a_panic() {
    let path = tmp_file("panic.jsonl");
    let check_path = path.clone();
    let mut sim = Simulator::new(header(1, 0), Box::new(fs::File::create(&path).unwrap())).unwrap();
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(move |ctx, _| {
            let tape = Tape::read(&fs::read(&check_path).unwrap()).unwrap();
            assert_eq!(tape.projection.tail(), "open_invocation");
            ctx.save(json!({"private":"never returned"}));
            panic!("inhabitant stopped");
        })),
    )
    .unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(
            || sim.client_message(NodeId(0), json!(null))
        ))
        .is_err()
    );
    let tape = Tape::read(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(tape.projection.tail(), "open_invocation");
    assert!(!tape.rows.iter().any(|r| r.what == "desire"));
    assert!(sim.client_message(NodeId(0), json!(null)).is_err());
    assert_eq!(sim.persistent()[&NodeId(0)], Vec::<Value>::new());
    fs::remove_file(path).unwrap();
}

#[test]
fn raw_values_and_colliding_names_survive_every_boundary() {
    let records = vec![
        Value::Null,
        json!(4),
        json!("text"),
        json!([1,{"what":"algorithm"}]),
        json!({"seq":99,"when":12,"who":"author","what":"mine","why":"mine","record":"custom","node":33}),
    ];
    let saved = records.clone();
    let message = json!({"type":"custom","message_type":"original","seq":99,"message_id":91,"parent_desire_id":42,"from":67,"to":68,"when":90,"what":"inhabitant","record":{"slot":2}});
    let sent = message.clone();
    let mut sim = world(header(1, 0));
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(move |ctx, input| {
            if matches!(input, Input::Client { .. }) {
                for r in &saved {
                    ctx.save(r);
                }
                ctx.send(NodeId(0), &sent);
                ctx.commit(Slot(9), json!(["entry", null]));
            }
        })),
    )
    .unwrap();
    sim.client_message(NodeId(0), json!({"what":"client","value":null}))
        .unwrap();
    sim.run(1).unwrap();
    sim.finalize("horizon").unwrap();
    let tape = parsed(&sim);
    assert_eq!(tape.projection.persistent[&NodeId(0)], records);
    for row in &tape.rows {
        if row.fields.contains_key("message_id") {
            assert_eq!(row.value("message").unwrap(), message);
        }
    }
    let dispatch = tape
        .rows
        .iter()
        .find(|r| r.what == "dispatch" && r.why == "message")
        .unwrap();
    if let Input::Message { envelope, .. } = dispatch.input().unwrap() {
        assert_eq!(envelope.send_tick, 0);
        assert_eq!(envelope.deliver_tick, 1);
        assert_eq!(envelope.payload, message);
    } else {
        panic!("message dispatch");
    }
    assert_eq!(
        tape.projection.chosen[&NodeId(0)][&Slot(9)],
        json!(["entry", null])
    );
}

#[test]
fn empty_answers_close_and_intermediate_run_returns_do_not_finalize() {
    let mut sim = world(header(1, 1));
    sim.spawn(NodeId(0), Box::new(Script::silent())).unwrap();
    sim.run(2).unwrap();
    assert!(sim.rows().iter().all(|r| r.what != "finalize"));
    sim.run(4).unwrap();
    sim.finalize("horizon").unwrap();
    assert_eq!(
        sim.rows().iter().filter(|r| r.what == "finalize").count(),
        1
    );
    for row in sim.rows().iter().filter(|r| r.what == "close") {
        assert_eq!(row.get::<u64>("count").unwrap(), 0);
    }
    for row in sim.rows().iter().filter(|r| r.what == "dispatch") {
        assert!(!row.fields.contains_key("message"));
    }
    assert!(sim.step().is_err());
    assert!(sim.finalize("horizon").is_err());
}

#[test]
fn missing_and_crashed_recipients_and_blocked_messages_are_witnessed() {
    let mut sim = world(header(2, 0));
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(|ctx, input| {
            if matches!(input, Input::Client { .. }) {
                ctx.send(NodeId(1), json!({"type":"one"}));
                ctx.send(NodeId(9), json!({"type":"missing"}));
            }
        })),
    )
    .unwrap();
    sim.spawn(NodeId(1), Box::new(Script::silent())).unwrap();
    sim.client_message(NodeId(0), json!(null)).unwrap();
    sim.crash(NodeId(1)).unwrap();
    sim.partition(NodeId(0), NodeId(1), 3).unwrap();
    sim.run(1).unwrap();
    let p = sim.check_projection().unwrap();
    assert_eq!(p.account().queued[0].status, "blocked");
    assert!(sim.rows().iter().any(|r| r.what == "drop"
        && r.get::<String>("reason").ok().as_deref() == Some("missing_recipient")));
    sim.run(3).unwrap();
    assert!(sim.pending().is_empty());
    assert!(sim.rows().iter().any(|r| r.what == "drop"
        && r.get::<String>("reason").ok().as_deref() == Some("crashed_recipient")));
    sim.client_message(NodeId(9), json!("lost command"))
        .unwrap();
    sim.finalize("horizon").unwrap();
    sim.check_projection().unwrap();
}

#[test]
fn logical_partitions_expire_without_traffic_and_self_partitions_are_refused() {
    let mut sim = world(header(1, 0));
    sim.spawn(NodeId(0), Box::new(Script::silent())).unwrap();
    assert!(
        sim.partition(NodeId(0), NodeId(0), 2)
            .unwrap_err()
            .contains("cannot be partitioned from itself")
    );
    sim.partition(NodeId(0), NodeId(1), 1).unwrap();
    sim.run(2).unwrap();
    let p = sim.check_projection().unwrap();
    assert!(!p.partitions.is_empty());
    assert!(p.effective_partitions().is_empty());
    assert!(sim.effective_partitions().is_empty());
}

#[test]
fn schema_errors_are_not_incomplete_outcomes() {
    let mut sim = world(header(1, 0));
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(|ctx, _| ctx.save(Value::Null))),
    )
    .unwrap();
    sim.client_message(NodeId(0), Value::Null).unwrap();
    sim.finalize("horizon").unwrap();
    let good = sim.rows().to_vec();
    let save = good
        .iter()
        .position(|r| r.what == "result" && r.why == "save")
        .unwrap();
    let attempt = good.iter().position(|r| r.what == "attempt").unwrap();
    let close = good.iter().position(|r| r.what == "close").unwrap();
    let mut bad = good.clone();
    bad[save].fields.remove("record");
    assert!(
        Tape::read(&tape::encode(&bad))
            .unwrap_err_string()
            .contains("requires fields")
    );
    let mut bad = good.clone();
    bad[save].fields.insert("record".into(), json!(1));
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let mut bad = good.clone();
    bad[attempt]
        .fields
        .insert("parent_input_id".into(), json!(999));
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let mut bad = good.clone();
    bad[close].fields.insert("count".into(), json!(2));
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let mut bad = good.clone();
    bad.swap(close, attempt);
    for (i, r) in bad.iter_mut().enumerate() {
        r.seq = i as u64;
    }
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let mut bad = good.clone();
    bad[1].seq = 9;
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let mut bad = good.clone();
    bad.last_mut().unwrap().fields["account"]["queued"] =
        json!([{"message_id":99,"status":"pending","blocked_until":null}]);
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let bytes = String::from_utf8(tape::encode(&good)).unwrap();
    let duplicate = bytes.replacen("\"seq\":0", "\"seq\":0,\"seq\":0", 1);
    assert!(
        Tape::read(duplicate.as_bytes())
            .unwrap_err_string()
            .contains("duplicate key")
    );
    let duplicate = bytes.replacen("\"record\":null", "\"record\":{\"x\":1,\"x\":2}", 1);
    assert!(
        Tape::read(duplicate.as_bytes())
            .unwrap_err_string()
            .contains("duplicate key")
    );
}
// Result::unwrap_err otherwise requires a Debug implementation for the whole tape.
trait TapeError {
    fn unwrap_err_string(self) -> String;
}
impl TapeError for Result<Tape, String> {
    fn unwrap_err_string(self) -> String {
        match self {
            Ok(_) => panic!("expected invalid tape"),
            Err(e) => e,
        }
    }
}

#[test]
fn replay_uses_inputs_and_choices_not_a_seeded_rerun_and_gates_identity() {
    let sim = scenarios::capture("ping-pong", Box::new(io::sink())).unwrap();
    let mut rows = sim.rows().to_vec();
    rows[0].fields.insert("seed".into(), json!(999));
    let tape = Tape::read(&tape::encode(&rows)).unwrap();
    replay::replay(&tape, Mode::Strict, scenarios::factory).unwrap();
    rows[0].fields["implementation"]["source_sha256"] = json!("0".repeat(64));
    let tape = Tape::read(&tape::encode(&rows)).unwrap();
    assert!(
        replay::replay(&tape, Mode::Strict, scenarios::factory)
            .unwrap_err()
            .contains("implementation identity")
    );
    assert_eq!(
        replay::replay(&tape, Mode::Diagnostic, scenarios::factory)
            .unwrap()
            .mode,
        "diagnostic"
    );
}

#[test]
fn replay_detects_a_changed_third_desire_before_any_effect_in_that_answer() {
    let mut sim = world(header(1, 0));
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(|ctx, _| {
            ctx.save(json!(1));
            ctx.save(json!(2));
            ctx.save(json!(3));
        })),
    )
    .unwrap();
    sim.client_message(NodeId(0), Value::Null).unwrap();
    sim.finalize("horizon").unwrap();
    let error = replay::replay(&parsed(&sim), Mode::Strict, |_| {
        Ok(Box::new(Script::new(|ctx, _| {
            ctx.save(json!(1));
            ctx.save(json!(2));
            ctx.save(json!(4));
        })))
    })
    .unwrap_err();
    assert!(error.contains("desire 2"));
    assert!(error.contains("0 earlier results"));
}

#[test]
fn slice_one_agreement_policy_and_distinct_slot_progress_remain() {
    let mut chosen = BTreeMap::new();
    chosen.insert(NodeId(0), BTreeMap::from([(Slot(7), json!("alpha"))]));
    assert_eq!(
        agreement_refusal(&chosen, NodeId(0), Slot(7), &json!("alpha")).as_deref(),
        Some("duplicate_commit")
    );
    assert_eq!(
        agreement_refusal(&chosen, NodeId(0), Slot(7), &json!("beta")).as_deref(),
        Some("self_split_brain")
    );
    assert_eq!(
        agreement_refusal(&chosen, NodeId(1), Slot(7), &json!("beta")).as_deref(),
        Some("split_brain")
    );
    assert_eq!(
        agreement_refusal(&chosen, NodeId(1), Slot(7), &json!("alpha")),
        None
    );
    assert_eq!(
        agreement_refusal(&chosen, NodeId(1), Slot(3), &json!("beta")),
        None
    );
    let mut h = header(2, 0);
    h.world.deadlock_threshold = 2;
    let mut sim = world(h);
    for id in 0..2 {
        sim.spawn(
            NodeId(id),
            Box::new(Script::new(|ctx, _| ctx.commit(Slot(7), json!("alpha")))),
        )
        .unwrap();
    }
    sim.client_message(NodeId(0), Value::Null).unwrap();
    assert!(sim.run(1).unwrap());
    sim.client_message(NodeId(1), Value::Null).unwrap();
    assert!(sim.run(3).unwrap());
    assert!(!sim.run(4).unwrap());
    let row = sim.rows().last().unwrap();
    assert_eq!(row.what, "deadlock");
    assert_eq!(row.get::<u64>("ticks_since_progress").unwrap(), 3);
}

fn vote_fixture(kind: &str, twice: bool) -> Tape {
    let mut h = header(2, 0);
    h.acceptors = vec![NodeId(0), NodeId(1)];
    for n in &mut h.nodes {
        n.algorithm = "raft".into();
        n.config = json!({"count":2,"election_timeout":1000});
    }
    let mut sim = world(h);
    let candidate = if kind == "identity" { 1 } else { 0 };
    sim.spawn(NodeId(0),Box::new(Script::new(move|ctx,input|if matches!(input,Input::Client{..}){ctx.send(NodeId(1),json!({"type":"request_vote","term":1,"candidate_id":candidate,"last_log_index":-1,"last_log_term":null}));}))).unwrap();
    let kind = kind.to_string();
    let mut first = true;
    sim.spawn(
        NodeId(1),
        Box::new(Script::new(move |ctx, input| {
            if let Input::Message { envelope, .. } = input {
                if kind == "send_first" {
                    ctx.send(
                        envelope.from,
                        json!({"type":"request_vote_response","term":1,"vote_granted":true}),
                    );
                }
                if first && kind != "unsupported" {
                    if kind != "missing_term" {
                        ctx.save(json!({"record":"current_term","term":1}));
                    }
                    ctx.save(json!({"record":"voted_for","term":1,"voted_for":candidate}));
                    if kind == "conflict" {
                        ctx.save(json!({"record":"current_term","term":1}));
                        ctx.save(json!({"record":"cleared_vote","term":1}));
                        ctx.save(json!({"record":"voted_for","term":1,"voted_for":1}));
                    }
                    if kind == "unknown_record" {
                        ctx.save(json!({"record":"invented"}));
                    }
                    if kind == "wrong_term" {
                        ctx.save(json!({"record":"voted_for","term":2,"voted_for":0}));
                    }
                    if kind == "decreased_term" {
                        ctx.save(json!({"record":"current_term","term":0}));
                    }
                }
                first = false;
                if kind != "send_first" {
                    ctx.send(
                        envelope.from,
                        json!({"type":"request_vote_response","term":1,"vote_granted":true}),
                    );
                }
            }
        })),
    )
    .unwrap();
    sim.client_message(NodeId(0), json!({"type":"propose","value":null}))
        .unwrap();
    sim.run(1).unwrap();
    if twice {
        sim.client_message(NodeId(0), json!({"type":"propose","value":null}))
            .unwrap();
        sim.run(2).unwrap();
    }
    sim.finalize("horizon").unwrap();
    parsed(&sim)
}
#[test]
fn vote_law_accepts_two_grants_with_one_saved_vote() {
    let tape = vote_fixture("valid", true);
    let report = vote::verify(&tape).unwrap();
    evidence("two-grants-one-save.jsonl", &tape::encode(&tape.rows));
    evidence(
        "two-grants-one-save.vote.json",
        &serde_json::to_vec_pretty(&report).unwrap(),
    );
    assert_eq!(report.grants.len(), 2);
    assert_eq!(report.grants[0].vote_line, report.grants[1].vote_line);
    for grant in report.grants {
        assert!(grant.vote_line < grant.admission_line);
    }
}
#[test]
fn vote_law_rejects_bad_order_identity_and_durable_transitions() {
    for (kind, expected) in [
        ("send_first", "unsupported"),
        ("unsupported", "unsupported"),
        ("identity", "request identity mismatch"),
        ("conflict", "second candidate"),
        ("unknown_record", "invalid Raft durable record"),
        ("wrong_term", "wrong term"),
        ("missing_term", "wrong term"),
        ("decreased_term", "term decreased"),
    ] {
        let tape = vote_fixture(kind, false);
        let error = vote::verify(&tape).unwrap_err();
        evidence(&format!("vote-{kind}.jsonl"), &tape::encode(&tape.rows));
        evidence(&format!("vote-{kind}.error.txt"), error.as_bytes());
        assert!(error.contains(expected), "{kind}: {error}");
    }
}

#[test]
fn vote_reports_hash_original_bytes_not_a_reserialization() {
    let tape = vote_fixture("valid", false);
    let plain = tape::encode(&tape.rows);
    let spaced = String::from_utf8(plain.clone())
        .unwrap()
        .lines()
        .map(|s| format!("  {s}\n"))
        .collect::<String>();
    let decorated = Tape::read(spaced.as_bytes()).unwrap();
    assert_ne!(tape.sha256, decorated.sha256);
    let report = vote::verify(&decorated).unwrap();
    assert_eq!(report.source_sha256, tape::hash(spaced.as_bytes()));
    assert_ne!(report.source_sha256, tape::hash(&plain));
}

#[test]
fn missing_nullable_fields_and_false_finalization_reasons_are_invalid() {
    let sim = scenarios::capture("raft", Box::new(io::sink())).unwrap();
    let good = sim.rows().to_vec();
    let mut bad = good.clone();
    bad.last_mut().unwrap().fields["account"]
        .as_object_mut()
        .unwrap()
        .remove("active_input");
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let mut bad = good.clone();
    bad.last_mut()
        .unwrap()
        .fields
        .insert("reason".into(), json!("no_progress"));
    assert!(Tape::read(&tape::encode(&bad)).is_err());
    let mut bad = good.clone();
    bad[0].fields["nodes"][0]
        .as_object_mut()
        .unwrap()
        .remove("config");
    assert!(Tape::read(&tape::encode(&bad)).is_err());
}

#[test]
fn failed_client_serialization_does_not_consume_an_input_identity() {
    struct CannotSerialize;
    impl serde::Serialize for CannotSerialize {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("not a value"))
        }
    }
    let mut sim = world(header(1, 0));
    sim.spawn(NodeId(0), Box::new(Script::silent())).unwrap();
    assert!(sim.client_message(NodeId(0), CannotSerialize).is_err());
    sim.client_message(NodeId(0), Value::Null).unwrap();
    sim.finalize("horizon").unwrap();
    sim.check_projection().unwrap();
}

#[test]
fn grant_with_client_ancestry_is_not_silently_skipped() {
    let mut h = header(2, 0);
    h.acceptors = vec![NodeId(0), NodeId(1)];
    for node in &mut h.nodes {
        node.algorithm = "raft".into();
        node.config = json!({"count":2,"election_timeout":1000});
    }
    let mut sim = world(h);
    sim.spawn(NodeId(0), Box::new(Script::silent())).unwrap();
    sim.spawn(
        NodeId(1),
        Box::new(Script::new(|ctx, _| {
            ctx.save(json!({"record":"current_term","term":1}));
            ctx.save(json!({"record":"voted_for","term":1,"voted_for":0}));
            ctx.send(
                NodeId(0),
                json!({"type":"request_vote_response","term":1,"vote_granted":true}),
            );
        })),
    )
    .unwrap();
    sim.client_message(NodeId(1), json!({"type":"propose","value":null}))
        .unwrap();
    sim.finalize("horizon").unwrap();
    assert!(
        vote::verify(&parsed(&sim))
            .unwrap_err()
            .contains("ancestry is not a dispatched peer request")
    );
}

fn renumber(rows: &mut [tape::Event]) {
    for (seq, row) in rows.iter_mut().enumerate() {
        row.seq = seq as u64;
    }
}
fn remove_empty_dispatch(rows: &mut Vec<tape::Event>, start: usize, count: usize, input_id: u64) {
    rows.drain(start..start + count);
    for row in rows.iter_mut() {
        for key in ["input_id", "parent_input_id"] {
            if let Some(id) = row.fields.get(key).and_then(Value::as_u64)
                && id > input_id
            {
                row.fields.insert(key.into(), json!(id - 1));
            }
        }
    }
    renumber(rows);
}

#[test]
fn omitted_deliverable_message_is_not_honest_pending_work() {
    let sim = scenarios::capture("ping-pong", Box::new(io::sink())).unwrap();
    let mut rows = sim.rows().to_vec();
    let index = (0..rows.len() - 2)
        .rfind(|&i| {
            rows[i].what == "arrive"
                && rows[i + 1].what == "dispatch"
                && rows[i + 2].what == "close"
                && rows[i + 2].get::<u64>("count").unwrap() == 0
        })
        .unwrap();
    let message: u64 = rows[index].get("message_id").unwrap();
    let input: u64 = rows[index + 1].get("input_id").unwrap();
    remove_empty_dispatch(&mut rows, index, 3, input);
    let queued = rows.last_mut().unwrap().fields["account"]["queued"]
        .as_array_mut()
        .unwrap();
    let position = queued
        .iter()
        .position(|q| q["message_id"].as_u64().unwrap() > message)
        .unwrap_or(queued.len());
    queued.insert(
        position,
        json!({"message_id":message,"status":"pending","blocked_until":null}),
    );
    let bytes = tape::encode(&rows);
    let error = Tape::read(&bytes).unwrap_err_string();
    assert!(error.contains("frozen ready queue"), "{error}");
    evidence("forged-starved.jsonl", &bytes);
    evidence("forged-starved.error.txt", error.as_bytes());
}

#[test]
fn network_order_immediate_dispatch_and_tick_obligations_are_checked() {
    let mut sim = world(header(1, 0));
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(|ctx, input| {
            if matches!(input, Input::Client { .. }) {
                ctx.send(NodeId(0), json!("first"));
                ctx.send(NodeId(0), json!("second"));
            }
        })),
    )
    .unwrap();
    sim.client_message(NodeId(0), Value::Null).unwrap();
    sim.run(1).unwrap();
    sim.finalize("horizon").unwrap();
    let good = sim.rows().to_vec();
    let arrivals: Vec<_> = good
        .iter()
        .enumerate()
        .filter(|(_, r)| r.what == "arrive")
        .map(|(i, _)| i)
        .collect();
    let mut reordered = good.clone();
    reordered.swap(arrivals[0], arrivals[1]);
    renumber(&mut reordered);
    assert!(
        Tape::read(&tape::encode(&reordered))
            .unwrap_err_string()
            .contains("frozen ready queue")
    );
    let mut interleaved = good;
    let later = interleaved.remove(arrivals[1]);
    interleaved.insert(arrivals[0] + 1, later);
    renumber(&mut interleaved);
    assert!(
        Tape::read(&tape::encode(&interleaved))
            .unwrap_err_string()
            .contains("followed immediately")
    );
    let mut sim = world(header(2, 1));
    for id in 0..2 {
        sim.spawn(NodeId(id), Box::new(Script::silent())).unwrap();
    }
    sim.run(1).unwrap();
    sim.finalize("horizon").unwrap();
    let mut rows = sim.rows().to_vec();
    let first = rows.iter().position(|r| r.what == "dispatch").unwrap();
    let id = rows[first].get("input_id").unwrap();
    remove_empty_dispatch(&mut rows, first, 2, id);
    assert!(
        Tape::read(&tape::encode(&rows))
            .unwrap_err_string()
            .contains("tick dispatch omitted or reordered")
    );
}

#[test]
fn zero_latency_and_between_step_partition_changes_keep_frozen_readiness() {
    let mut h = header(1, 1);
    h.world.base_latency = 0;
    let mut sent = false;
    let mut sim = world(h);
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(move |ctx, input| {
            if matches!(input, Input::Tick { .. }) && !sent {
                sent = true;
                ctx.send(NodeId(0), Value::Null);
            }
        })),
    )
    .unwrap();
    sim.run(1).unwrap();
    assert_eq!(sim.pending().len(), 1);
    sim.check_projection().unwrap();
    sim.run(2).unwrap();
    assert!(sim.pending().is_empty());
    sim.finalize("horizon").unwrap();
    let mut sim = world(header(2, 0));
    sim.spawn(
        NodeId(0),
        Box::new(Script::new(|ctx, _| ctx.send(NodeId(1), Value::Null))),
    )
    .unwrap();
    sim.spawn(NodeId(1), Box::new(Script::silent())).unwrap();
    sim.client_message(NodeId(0), Value::Null).unwrap();
    sim.partition(NodeId(0), NodeId(1), 10).unwrap();
    sim.run(2).unwrap();
    sim.partition(NodeId(0), NodeId(1), 0).unwrap(); // unblocked between steps, not a retroactive arrival
    sim.check_projection().unwrap();
    sim.run(3).unwrap();
    assert!(sim.pending().is_empty());
    sim.finalize("horizon").unwrap();
}

#[test]
fn no_progress_is_recomputed_and_its_observation_cannot_be_omitted() {
    let sim = scenarios::capture("raft", Box::new(io::sink())).unwrap();
    let mut rows = sim.rows().to_vec();
    rows[0].fields["world"]["deadlock_threshold"] = json!(30);
    let mut fake = tape::Event::new(
        40,
        "bench".into(),
        "deadlock",
        "no_progress",
        json!({"ticks_since_progress":31}),
    );
    fake.seq = rows.len() as u64 - 1;
    let end = rows.len() - 1;
    rows.insert(end, fake);
    rows.last_mut().unwrap().fields["reason"] = json!("no_progress");
    renumber(&mut rows);
    let bytes = tape::encode(&rows);
    let error = Tape::read(&bytes).unwrap_err_string();
    assert!(
        error.contains("contradicts distinct-slot progress"),
        "{error}"
    );
    evidence("forged-deadlock.jsonl", &bytes);
    evidence("forged-deadlock.error.txt", error.as_bytes());
    let mut h = header(1, 1);
    h.world.deadlock_threshold = 0;
    let mut sim = world(h);
    sim.spawn(NodeId(0), Box::new(Script::silent())).unwrap();
    assert!(!sim.run(1).unwrap());
    sim.finalize("no_progress").unwrap();
    let good = sim.rows().to_vec();
    let bell = good.iter().position(|r| r.what == "deadlock").unwrap();
    assert_eq!(
        Projection::from_rows(&good[..bell]).unwrap().tail(),
        "awaiting_no_progress"
    );
    let mut missing = good.clone();
    missing.remove(bell);
    missing.last_mut().unwrap().fields["reason"] = json!("horizon");
    renumber(&mut missing);
    assert!(Tape::read(&tape::encode(&missing)).is_err());
    let mut wrong = good;
    wrong[bell].fields["ticks_since_progress"] = json!(2);
    assert!(
        Tape::read(&tape::encode(&wrong))
            .unwrap_err_string()
            .contains("contradicts distinct-slot progress")
    );
}

#[test]
fn lifecycle_records_must_change_state_in_world_and_reader() {
    let mut sim = world(header(1, 0));
    sim.spawn(NodeId(0), Box::new(Script::silent())).unwrap();
    assert!(sim.recover(NodeId(0)).is_err());
    sim.crash(NodeId(0)).unwrap();
    assert!(sim.crash(NodeId(0)).is_err());
    sim.recover(NodeId(0)).unwrap();
    sim.finalize("horizon").unwrap();
    let good = sim.rows().to_vec();
    let mut bad = good.clone();
    let end = bad.len() - 1;
    bad.insert(
        end,
        tape::Event::new(
            0,
            "bench".into(),
            "recover",
            "node",
            json!({"node":0,"model":"retained_state"}),
        ),
    );
    renumber(&mut bad);
    let bytes = tape::encode(&bad);
    let error = Tape::read(&bytes).unwrap_err_string();
    assert!(error.contains("does not change state"));
    evidence("forged-recover.jsonl", &bytes);
    evidence("forged-recover.error.txt", error.as_bytes());
    let mut bad = good;
    let crash = bad.iter().position(|r| r.what == "crash").unwrap();
    bad.insert(crash + 1, bad[crash].clone());
    renumber(&mut bad);
    assert!(
        Tape::read(&tape::encode(&bad))
            .unwrap_err_string()
            .contains("does not change state")
    );
}

#[test]
fn replay_reports_uncertified_answers_and_interrupted_step_work() {
    let sim = scenarios::capture("raft", Box::new(io::sink())).unwrap();
    let end = sim
        .rows()
        .iter()
        .position(|r| r.what == "dispatch")
        .unwrap()
        + 1;
    let source = Tape::read(&tape::encode(&sim.rows()[..end])).unwrap();
    let report = replay::replay(&source, Mode::Strict, scenarios::factory).unwrap();
    assert_eq!(report.uncertified_answers, 1);
    assert_eq!(report.completed_answers, 0);
    let mut rows = sim.rows()[..end].to_vec();
    let mut finish = tape::Event::new(
        source.projection.tick,
        "bench".into(),
        "finalize",
        "scenario",
        json!({"reason":"interrupted","account":source.projection.account()}),
    );
    finish.seq = rows.len() as u64;
    rows.push(finish);
    let interrupted = Tape::read(&tape::encode(&rows)).unwrap();
    assert_eq!(interrupted.projection.account().due_ticks, vec![NodeId(1)]);
}

#[test]
fn real_raft_receiver_grants_initial_term_zero_with_a_saved_vote() {
    let mut h = header(2, 0);
    h.nodes[1].algorithm = "raft".into();
    h.nodes[1].config = json!({"count":2,"election_timeout":100});
    h.acceptors = vec![NodeId(0), NodeId(1)];
    let mut sim = world(h);
    sim.spawn(NodeId(0),Box::new(Script::new(|ctx,_|ctx.send(NodeId(1),json!({"type":"request_vote","term":0,"candidate_id":0,"last_log_index":-1,"last_log_term":null}))))).unwrap();
    sim.spawn(
        NodeId(1),
        Box::new(quorum::raft::RaftNode::with_election_timeout(
            NodeId(1),
            2,
            100,
        )),
    )
    .unwrap();
    sim.client_message(NodeId(0), Value::Null).unwrap();
    sim.run(1).unwrap();
    sim.finalize("horizon").unwrap();
    assert_eq!(
        sim.persistent()[&NodeId(1)],
        vec![json!({"record":"voted_for","term":0,"voted_for":0})]
    );
    let source = parsed(&sim);
    let report = vote::verify(&source).unwrap();
    assert_eq!(report.grants.len(), 1);
    assert_eq!(report.grants[0].term_line, None);
    assert_eq!(report.grants[0].term_origin, "initial_state_term_zero");
    evidence("initial-term-zero.jsonl", &tape::encode(&source.rows));
    evidence(
        "initial-term-zero.vote.json",
        &serde_json::to_vec_pretty(&report).unwrap(),
    );
}
