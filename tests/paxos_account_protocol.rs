use quorum::tape::{self, Event, Header, NodeSpec, WorldConfig};
use quorum::{Context, Desire, Input, Node, NodeId, Simulator, Slot};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs, io,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
};

// These are owned answers, not an invocation of the inhabitant or a replay
// oracle. A supported declaration deliberately includes mutants: the account
// must judge the contract rather than trust the implementation label.
enum Trigger {
    Client,
    Tick,
    Message { from: i32, kind: &'static str },
}
struct Step {
    trigger: Trigger,
    answer: Vec<Desire>,
    panic: bool,
}
fn client(answer: Vec<Desire>) -> Step {
    Step {
        trigger: Trigger::Client,
        answer,
        panic: false,
    }
}
fn tick(answer: Vec<Desire>) -> Step {
    Step {
        trigger: Trigger::Tick,
        answer,
        panic: false,
    }
}
fn message(from: i32, kind: &'static str, answer: Vec<Desire>) -> Step {
    Step {
        trigger: Trigger::Message { from, kind },
        answer,
        panic: false,
    }
}
struct Script {
    steps: VecDeque<Step>,
    crashed: bool,
}
impl Node for Script {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        let Some(next) = self.steps.front() else {
            return;
        };
        let matches = match (&next.trigger, input) {
            (Trigger::Client, Input::Client { .. }) | (Trigger::Tick, Input::Tick { .. }) => true,
            (Trigger::Message { from, kind }, Input::Message { envelope, .. }) => {
                envelope.from == NodeId(*from) && envelope.payload["type"] == *kind
            }
            _ => false,
        };
        if !matches {
            return;
        }
        let step = self.steps.pop_front().unwrap();
        assert!(!step.panic, "owned conflicting-report detection site");
        for desire in step.answer {
            match desire {
                Desire::Send { to, message } => ctx.send(to, message),
                Desire::Save { record } => ctx.save(record),
                Desire::Commit { slot, entry } => ctx.commit(slot, entry),
            }
        }
    }
    fn crashed(&self) -> bool {
        self.crashed
    }
    fn set_crashed(&mut self, crashed: bool) {
        self.crashed = crashed;
    }
}
struct Plan {
    node: i32,
    covered: bool,
    config: Option<Value>,
    steps: Vec<Step>,
}
fn plan(node: i32, steps: Vec<Step>) -> Plan {
    Plan {
        node,
        covered: true,
        config: None,
        steps,
    }
}
fn unsupported(node: i32, steps: Vec<Step>) -> Plan {
    Plan {
        node,
        covered: false,
        config: None,
        steps,
    }
}
fn configured(node: i32, config: Value, steps: Vec<Step>) -> Plan {
    Plan {
        node,
        covered: true,
        config: Some(config),
        steps,
    }
}
fn config(acceptors: &[i32], from_slot: u64, active: Option<i32>, round: u64) -> Value {
    json!({
        "acceptors":acceptors, "learners":[0,4], "proposers":[0,4],
        "active_proposer":active, "from_slot":from_slot,
        "initial_round":round, "initial_state":"empty"
    })
}
struct Rig {
    sim: Simulator,
    late: BTreeMap<i32, Script>,
}
impl Rig {
    fn spawn(&mut self, node: i32) {
        self.sim
            .spawn(NodeId(node), Box::new(self.late.remove(&node).unwrap()))
            .unwrap();
    }
}
fn fixture(acceptors: &[i32], from_slot: u64, plans: Vec<Plan>, ticks: bool, late: &[i32]) -> Rig {
    let mut all = BTreeMap::new();
    for node in [0, 4].into_iter().chain(acceptors.iter().copied()) {
        all.insert(node, plan(node, vec![]));
    }
    for p in plans {
        all.insert(p.node, p);
    }
    let specs: Vec<_> = all
        .values()
        .map(|p| NodeSpec {
            node: NodeId(p.node),
            algorithm: if p.covered {
                "multi-paxos"
            } else {
                "receiver-script"
            }
            .into(),
            config: if p.covered {
                p.config
                    .clone()
                    .unwrap_or_else(|| config(acceptors, from_slot, (p.node == 0).then_some(0), 2))
            } else {
                json!({"fixture":"paxos-account-protocol"})
            },
        })
        .collect();
    let header = Header::new(
        "paxos-account-protocol",
        42,
        WorldConfig {
            base_latency: 1,
            jitter: 0,
            tick_interval: u64::from(ticks),
            deadlock_threshold: 200,
        },
        specs,
        acceptors.iter().map(|n| NodeId(*n)).collect(),
    );
    let mut sim = Simulator::new(header, Box::new(io::sink())).unwrap();
    let mut deferred = BTreeMap::new();
    for (node, p) in all {
        let script = Script {
            steps: p.steps.into(),
            crashed: false,
        };
        if late.contains(&node) {
            deferred.insert(node, script);
        } else {
            sim.spawn(NodeId(node), Box::new(script)).unwrap();
        }
    }
    Rig {
        sim,
        late: deferred,
    }
}
fn ordinary(plans: Vec<Plan>, ticks: bool) -> Simulator {
    fixture(&[1, 2, 3], 0, plans, ticks, &[]).sim
}
fn propose(sim: &mut Simulator, node: i32, value: Value) {
    sim.client_message(NodeId(node), json!({"type":"propose","value":value}))
        .unwrap();
}
fn advance(sim: &mut Simulator, ticks: u64) {
    assert!(sim.run(sim.tick() + ticks).unwrap());
}
fn send(to: i32, message: Value) -> Desire {
    Desire::Send {
        to: NodeId(to),
        message,
    }
}
fn save(record: Value) -> Desire {
    Desire::Save { record }
}
fn commit(slot: u64, value: Value) -> Desire {
    Desire::Commit {
        slot: Slot(slot),
        entry: value,
    }
}
fn prepare(b: [i32; 2], from_slot: u64) -> Value {
    json!({"type":"prepare","ballot":b,"from_slot":from_slot})
}
fn promise(b: [i32; 2], from_slot: u64, accepted: Vec<Value>) -> Value {
    json!({"type":"promise","ballot":b,"from_slot":from_slot,"accepted":accepted})
}
fn accept(b: [i32; 2], slot: u64, value: Value) -> Value {
    json!({"type":"accept","ballot":b,"slot":slot,"value":value})
}
fn accepted(b: [i32; 2], slot: u64, value: Value) -> Value {
    json!({"type":"accepted","ballot":b,"slot":slot,"value":value})
}
fn entry(b: [i32; 2], slot: u64, value: Value) -> Value {
    json!({"slot":slot,"ballot":b,"value":value})
}
fn promised(b: [i32; 2], from_slot: u64) -> Value {
    json!({"record":"promised","ballot":b,"from_slot":from_slot})
}
fn saved(b: [i32; 2], slot: u64, value: Value) -> Value {
    json!({"record":"accepted","ballot":b,"slot":slot,"value":value})
}
fn nack(b: [i32; 2], p: [i32; 2]) -> Value {
    json!({"type":"nack","rejected_ballot":b,"promised_ballot":p})
}
fn broadcast(members: &[i32], payload: Value) -> Vec<Desire> {
    members.iter().map(|n| send(*n, payload.clone())).collect()
}
fn proposals(b: [i32; 2], values: &[(u64, Value)]) -> Vec<Desire> {
    values
        .iter()
        .flat_map(|(slot, value)| broadcast(&[1, 2, 3], accept(b, *slot, value.clone())))
        .collect()
}
fn persisted_reply(to: i32, b: [i32; 2], slot: u64, value: Value) -> Vec<Desire> {
    vec![
        save(promised(b, 0)),
        save(saved(b, slot, value.clone())),
        send(to, accepted(b, slot, value)),
    ]
}
fn matching_lines(rows: &[Event], predicate: impl Fn(&Event) -> bool) -> Vec<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(i, r)| predicate(r).then_some(i + 1))
        .collect()
}
fn admissions(rows: &[Event], node: i32, kind: &str) -> Vec<usize> {
    matching_lines(rows, |r| {
        r.what == "attempt"
            && r.why == "send"
            && r.fields["node"] == node
            && r.fields["message"]["type"] == kind
    })
}
fn dispatches(rows: &[Event], to: i32, from: i32, kind: &str) -> Vec<usize> {
    matching_lines(rows, |r| {
        r.what == "dispatch"
            && r.why == "message"
            && r.fields["to"] == to
            && r.fields["from"] == from
            && r.fields["message"]["type"] == kind
    })
}
fn save_results(rows: &[Event], node: i32, kind: &str) -> Vec<usize> {
    matching_lines(rows, |r| {
        r.what == "result"
            && r.why == "save"
            && r.fields["node"] == node
            && r.fields["record"]["record"] == kind
            && r.fields["outcome"] == "fulfilled"
    })
}
fn capture(name: &str, rows: &[Event]) -> Value {
    let bytes = tape::encode(rows);
    let report = quorum::paxos_account::examine(&bytes);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/paxos-account-evidence");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join(format!("{name}.jsonl")), &bytes).unwrap();
    fs::write(
        root.join(format!("{name}.report.json")),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["source_sha256"], tape::hash(&bytes));
    assert!(
        report["world"]["error"].is_null(),
        "{name}: {}",
        report["world"]
    );
    assert_eq!(report["world"]["valid_rows"], rows.len());
    report
}
fn obligations<'a>(report: &'a Value, name: &str) -> Vec<&'a Value> {
    report["obligations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["proposition"] == name)
        .collect()
}
fn obligation<'a>(report: &'a Value, name: &str, boundary: usize, verdict: &str) -> &'a Value {
    let found = obligations(report, name)
        .into_iter()
        .find(|p| p["boundary_line"] == boundary && p["verdict"] == verdict)
        .unwrap_or_else(|| {
            panic!(
                "missing {name} {verdict} at {boundary}: {}",
                report["obligations"]
            )
        });
    let key = if verdict == "violated" {
        "counterevidence"
    } else {
        "support"
    };
    assert!(
        !found[key].as_array().unwrap().is_empty(),
        "uncited {name}: {found}"
    );
    found
}
fn slot_obligation<'a>(report: &'a Value, name: &str, slot: u64, verdict: &str) -> &'a Value {
    let found = obligations(report, name)
        .into_iter()
        .find(|p| p["subject"]["slot"] == slot && p["verdict"] == verdict)
        .unwrap_or_else(|| {
            panic!(
                "missing {name} {verdict} slot {slot}: {}",
                report["obligations"]
            )
        });
    let boundary = found["boundary_line"].as_u64().unwrap() as usize;
    obligation(report, name, boundary, verdict);
    found
}
fn certificate<'a>(
    report: &'a Value,
    rows: &[Event],
    slot: u64,
    b: [i32; 2],
    value: Value,
) -> &'a Value {
    let choice = report["choices"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["slot"] == slot && c["ballot"] == json!(b) && c["value"] == value)
        .unwrap_or_else(|| {
            panic!(
                "missing certificate for {slot}/{b:?}/{value}: {}",
                report["choices"]
            )
        });
    let mut members = BTreeSet::new();
    let mut latest = 0;
    for support in choice["first_support"].as_array().unwrap() {
        let node = support["acceptor"].as_i64().unwrap();
        assert!(members.insert(node));
        let line = support["save_line"].as_u64().unwrap() as usize;
        latest = latest.max(line);
        let row = &rows[line - 1];
        assert_eq!((row.what.as_str(), row.why.as_str()), ("result", "save"));
        assert_eq!(row.fields["outcome"], "fulfilled");
        assert_eq!(row.fields["node"], node);
        assert_eq!(row.fields["record"], saved(b, slot, value.clone()));
    }
    assert_eq!(members.len(), choice["quorum"].as_u64().unwrap() as usize);
    assert_eq!(choice["first_quorum_line"], latest);
    choice
}
fn supported_claim(report: &Value, node: i32, slot: u64, value: Value) {
    let claim = report["claims"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| {
            c["node"] == node && c["slot"] == slot && c["value"] == value && c["kind"] == "commit"
        })
        .unwrap_or_else(|| panic!("missing received-quorum claim: {}", report["claims"]));
    for key in ["local_basis", "transitive_support"] {
        assert_eq!(claim[key]["verdict"], "established", "{key}: {claim}");
        assert!(!claim[key]["support"].as_array().unwrap().is_empty());
    }
}

#[test]
fn protocol_ack_before_save_stays_violated_and_exact_repeat_uses_prior_save() {
    let b = [2, 0];
    let request = accept(b, 0, json!("x"));
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    client(vec![send(1, request.clone()), send(2, request.clone())]),
                    client(vec![send(1, request)]),
                ],
            ),
            plan(
                1,
                vec![
                    message(
                        0,
                        "accept",
                        vec![
                            save(promised(b, 0)),
                            send(0, accepted(b, 0, json!("x"))),
                            save(saved(b, 0, json!("x"))),
                        ],
                    ),
                    message(0, "accept", vec![send(0, accepted(b, 0, json!("x")))]),
                ],
            ),
            plan(
                2,
                vec![message(0, "accept", persisted_reply(0, b, 0, json!("x")))],
            ),
        ],
        false,
    );
    propose(&mut sim, 0, json!("x"));
    advance(&mut sim, 2);
    let early = admissions(sim.rows(), 1, "accepted")[0];
    let saved_line = save_results(sim.rows(), 1, "accepted")[0];
    assert!(early < saved_line);
    let prefix = capture("protocol-ack-before-save-prefix", &sim.rows()[..early]);
    obligation(&prefix, "accepted_reply_support", early, "violated");
    propose(&mut sim, 0, json!("repeat-trigger"));
    advance(&mut sim, 2);
    sim.finalize("horizon").unwrap();
    let report = capture("protocol-ack-later-save-exact-repeat", sim.rows());
    let replies = admissions(sim.rows(), 1, "accepted");
    assert_eq!(replies.len(), 2);
    assert_eq!(save_results(sim.rows(), 1, "accepted").len(), 1);
    obligation(&report, "accepted_reply_support", replies[0], "violated");
    let repeat = obligation(&report, "accepted_reply_support", replies[1], "established");
    assert!(
        repeat["support"]
            .as_array()
            .unwrap()
            .contains(&json!(saved_line))
    );
    certificate(&report, sim.rows(), 0, b, json!("x"));
}

#[test]
fn protocol_stale_accepted_save_keeps_a_certificate_below_watermark() {
    let b = [2, 0];
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    client(broadcast(&[1, 2], accept(b, 0, json!("stale")))),
                    message(1, "accepted", vec![]),
                    message(2, "accepted", vec![commit(0, json!("stale"))]),
                ],
            ),
            plan(
                1,
                vec![message(
                    0,
                    "accept",
                    vec![
                        save(promised([3, 4], 0)),
                        save(saved(b, 0, json!("stale"))),
                        send(0, accepted(b, 0, json!("stale"))),
                    ],
                )],
            ),
            plan(
                2,
                vec![message(
                    0,
                    "accept",
                    persisted_reply(0, b, 0, json!("stale")),
                )],
            ),
        ],
        false,
    );
    propose(&mut sim, 0, json!("stale"));
    advance(&mut sim, 2);
    sim.finalize("horizon").unwrap();
    let report = capture("protocol-stale-accepted-watermark", sim.rows());
    obligation(
        &report,
        "accepted_watermark",
        save_results(sim.rows(), 1, "accepted")[0],
        "violated",
    );
    obligation(
        &report,
        "accepted_reply_support",
        admissions(sim.rows(), 1, "accepted")[0],
        "established",
    );
    certificate(&report, sim.rows(), 0, b, json!("stale"));
    supported_claim(&report, 0, 0, json!("stale"));
}

#[test]
fn protocol_promise_requires_equal_fulfilled_watermark() {
    for (label, watermark) in [("absent", None), ("different", Some([3, 4]))] {
        let mut answer = watermark
            .map(|b| vec![save(promised(b, 0))])
            .unwrap_or_default();
        answer.push(send(0, promise([2, 0], 0, vec![])));
        let mut sim = ordinary(
            vec![
                plan(0, vec![client(vec![send(1, prepare([2, 0], 0))])]),
                plan(1, vec![message(0, "prepare", answer)]),
            ],
            false,
        );
        propose(&mut sim, 0, json!("request-trigger"));
        advance(&mut sim, 1);
        let report = capture(&format!("protocol-promise-watermark-{label}"), sim.rows());
        obligation(
            &report,
            "promise_watermark",
            admissions(sim.rows(), 1, "promise")[0],
            "violated",
        );
        assert!(
            report["interpretation_findings"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn protocol_fresh_client_origin_does_not_replace_phase_one() {
    let b = [2, 0];
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    client(proposals(b, &[(0, json!("x"))])),
                    message(1, "accepted", vec![]),
                    message(2, "accepted", vec![commit(0, json!("x"))]),
                ],
            ),
            plan(
                1,
                vec![message(0, "accept", persisted_reply(0, b, 0, json!("x")))],
            ),
            plan(
                2,
                vec![message(0, "accept", persisted_reply(0, b, 0, json!("x")))],
            ),
        ],
        false,
    );
    propose(&mut sim, 0, json!("x"));
    advance(&mut sim, 2);
    sim.finalize("horizon").unwrap();
    let report = capture("protocol-phase2-without-phase1", sim.rows());
    for line in admissions(sim.rows(), 0, "accept") {
        obligation(&report, "phase2_gate", line, "violated");
        let adoption = obligation(&report, "adoption", line, "violated");
        assert!(adoption.get("unavailable_premises").is_none());
        assert_eq!(adoption["gaps"], json!([]));
    }
    slot_obligation(&report, "fresh_origin_count", 0, "established");
    assert_eq!(report["attempts"][0]["origin"]["kind"], "fresh");
    assert_eq!(
        report["attempts"][0]["origin"]["client_count"]["certain"],
        true
    );
    assert!(
        report["attempts"][0]["origin"]
            .get("unavailable_premises")
            .is_none()
    );
    certificate(&report, sim.rows(), 0, b, json!("x"));
    supported_claim(&report, 0, 0, json!("x"));
}

fn empty_campaign(
    proposed: Vec<(u64, Value)>,
    client_values: Vec<Value>,
    extra: Vec<Step>,
) -> Simulator {
    let mut p = client_values
        .iter()
        .map(|_| client(vec![]))
        .collect::<Vec<_>>();
    p.push(tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))));
    p.push(message(1, "promise", vec![]));
    p.push(message(2, "promise", proposals([2, 0], &proposed)));
    p.extend(extra);
    let mut plans = vec![plan(0, p)];
    for node in [1, 2] {
        let mut steps = vec![message(
            0,
            "prepare",
            vec![
                save(promised([2, 0], 0)),
                send(0, promise([2, 0], 0, vec![])),
            ],
        )];
        for (slot, value) in &proposed {
            steps.push(message(
                0,
                "accept",
                vec![
                    save(saved([2, 0], *slot, value.clone())),
                    send(0, accepted([2, 0], *slot, value.clone())),
                ],
            ));
        }
        plans.push(plan(node, steps));
    }
    let mut sim = ordinary(plans, true);
    for value in client_values {
        propose(&mut sim, 0, value);
    }
    advance(&mut sim, 4);
    sim
}

#[test]
fn protocol_quorum_completing_promise_licenses_its_own_closed_answer() {
    let sim = empty_campaign(vec![(0, json!("x"))], vec![json!("x")], vec![]);
    let rows = sim.rows();
    let completion = dispatches(rows, 0, 2, "promise")[0];
    let input = &rows[completion - 1].fields["input_id"];
    let close = matching_lines(rows, |r| {
        r.what == "close" && &r.fields["input_id"] == input
    })[0];
    let first = admissions(rows, 0, "accept")[0];
    assert!(completion < close && close < first);
    let report = capture("protocol-phase2-same-turn-quorum", rows);
    for line in admissions(rows, 0, "accept") {
        let gate = obligation(&report, "phase2_gate", line, "established");
        assert!(
            gate["support"]
                .as_array()
                .unwrap()
                .contains(&json!(completion))
        );
    }
    slot_obligation(&report, "fresh_origin_count", 0, "established");
}

#[test]
fn protocol_semantic_bad_promise_neither_counts_nor_poison_sender() {
    let bad_reports = [
        ("below-suffix", vec![entry([1, 0], 4, json!("bad"))]),
        (
            "duplicate-slot",
            vec![
                entry([1, 0], 5, json!("bad")),
                entry([1, 0], 5, json!("bad")),
            ],
        ),
        (
            "descending-slot",
            vec![
                entry([1, 0], 6, json!("bad")),
                entry([1, 0], 5, json!("bad")),
            ],
        ),
        ("above-ballot", vec![entry([3, 0], 5, json!("bad"))]),
        ("nonmember-owner", vec![entry([1, 9], 5, json!("bad"))]),
    ];
    for (label, bad) in bad_reports {
        let mut rig = fixture(
            &[1, 2, 3],
            5,
            vec![
                plan(
                    0,
                    vec![
                        client(vec![]),
                        client(vec![]),
                        tick(broadcast(&[1, 2, 3], prepare([2, 0], 5))),
                        message(1, "promise", vec![]),
                        message(2, "promise", proposals([2, 0], &[(5, json!("x"))])),
                        message(1, "promise", proposals([2, 0], &[(6, json!("x"))])),
                    ],
                ),
                unsupported(
                    1,
                    vec![
                        message(0, "prepare", vec![send(0, promise([2, 0], 5, bad))]),
                        client(vec![send(0, promise([2, 0], 5, vec![]))]),
                        message(
                            0,
                            "accept",
                            vec![
                                save(promised([2, 0], 5)),
                                save(saved([2, 0], 5, json!("x"))),
                                send(0, accepted([2, 0], 5, json!("x"))),
                            ],
                        ),
                        message(
                            0,
                            "accept",
                            vec![
                                save(saved([2, 0], 6, json!("x"))),
                                send(0, accepted([2, 0], 6, json!("x"))),
                            ],
                        ),
                    ],
                ),
                unsupported(
                    2,
                    vec![
                        message(0, "prepare", vec![send(0, promise([2, 0], 5, vec![]))]),
                        message(
                            0,
                            "accept",
                            vec![
                                save(promised([2, 0], 5)),
                                save(saved([2, 0], 5, json!("x"))),
                                send(0, accepted([2, 0], 5, json!("x"))),
                            ],
                        ),
                        message(
                            0,
                            "accept",
                            vec![
                                save(saved([2, 0], 6, json!("x"))),
                                send(0, accepted([2, 0], 6, json!("x"))),
                            ],
                        ),
                    ],
                ),
            ],
            true,
            &[],
        );
        propose(&mut rig.sim, 0, json!("x"));
        propose(&mut rig.sim, 0, json!("x"));
        advance(&mut rig.sim, 3);
        let early = admissions(rig.sim.rows(), 0, "accept");
        assert_eq!(early.len(), 3);
        propose(&mut rig.sim, 1, json!("valid-report-trigger"));
        advance(&mut rig.sim, 2);
        let report = capture(
            &format!("protocol-semantic-promise-{label}"),
            rig.sim.rows(),
        );
        assert!(
            report["interpretation_findings"]
                .as_array()
                .unwrap()
                .is_empty(),
            "semantic testimony is readable: {report}"
        );
        let bad_line = dispatches(rig.sim.rows(), 0, 1, "promise")[0];
        let observation = report["observations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| {
                o["kind"] == "semantic_promise_report" && o["node"] == 0 && o["line"] == bad_line
            })
            .unwrap_or_else(|| {
                panic!(
                    "missing semantic report rejection: {}",
                    report["observations"]
                )
            });
        assert_eq!(observation["valid"], false);
        assert_eq!(observation["counted"], false);
        assert!(!observation["reason"].as_str().unwrap().is_empty());
        obligation(
            &report,
            "promise_report_handling",
            close_of(rig.sim.rows(), bad_line),
            "established",
        );
        for line in early {
            obligation(&report, "phase2_gate", line, "violated");
            let adoption = obligation(&report, "adoption", line, "violated");
            assert!(adoption.get("unavailable_premises").is_none());
            assert_eq!(adoption["gaps"], json!([]));
        }
        let late = admissions(rig.sim.rows(), 0, "accept");
        assert_eq!(late.len(), 6);
        for line in &late[3..] {
            obligation(&report, "phase2_gate", *line, "established");
        }
        slot_obligation(&report, "adoption", 6, "established");
        slot_obligation(&report, "fresh_origin_count", 6, "established");
        certificate(&report, rig.sim.rows(), 5, [2, 0], json!("x"));
        certificate(&report, rig.sim.rows(), 6, [2, 0], json!("x"));
    }
}

#[test]
fn protocol_omitted_current_suffix_is_not_hidden_by_valid_watermark() {
    let b = [1, 0];
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    client(broadcast(&[1, 2], accept(b, 0, json!("x")))),
                    client(vec![send(1, prepare([2, 0], 0))]),
                ],
            ),
            plan(
                1,
                vec![
                    message(0, "accept", persisted_reply(0, b, 0, json!("x"))),
                    message(
                        0,
                        "prepare",
                        vec![
                            save(promised([2, 0], 0)),
                            send(0, promise([2, 0], 0, vec![])),
                        ],
                    ),
                ],
            ),
            plan(
                2,
                vec![message(0, "accept", persisted_reply(0, b, 0, json!("x")))],
            ),
        ],
        false,
    );
    propose(&mut sim, 0, json!("x"));
    advance(&mut sim, 2);
    propose(&mut sim, 0, json!("prepare-trigger"));
    advance(&mut sim, 1);
    let report = capture("protocol-promise-omitted-suffix", sim.rows());
    let admission = admissions(sim.rows(), 1, "promise")[0];
    obligation(&report, "promise_watermark", admission, "established");
    let suffix = obligation(&report, "promise_suffix", admission, "violated");
    assert!(
        suffix["counterevidence"]
            .as_array()
            .unwrap()
            .contains(&json!(save_results(sim.rows(), 1, "accepted")[0]))
    );
    certificate(&report, sim.rows(), 0, b, json!("x"));
}

// The old acceptance histories are explicit fulfilled saves, not assertions
// inferred from the later reports. The first phase-one quorum freezes slot 0/x.
fn adoption_campaign(
    values: Vec<(u64, Value)>,
    clients: Vec<Value>,
    claim_value: Option<Value>,
    late: bool,
) -> Simulator {
    let mut p = clients.iter().map(|_| client(vec![])).collect::<Vec<_>>();
    p.push(tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))));
    p.push(message(1, "promise", vec![]));
    p.push(message(2, "promise", proposals([2, 0], &values)));
    if late {
        p.push(message(3, "promise", vec![]));
    }
    if let Some(value) = claim_value {
        p.push(message(1, "accepted", vec![]));
        p.push(message(2, "accepted", vec![commit(0, value)]));
    }
    let mut plans = vec![plan(0, p)];
    for node in [1, 2] {
        let mut steps = vec![
            client(vec![
                save(promised([1, 4], 0)),
                save(saved([1, 4], 0, json!("x"))),
            ]),
            message(
                0,
                "prepare",
                vec![
                    save(promised([2, 0], 0)),
                    send(0, promise([2, 0], 0, vec![entry([1, 4], 0, json!("x"))])),
                ],
            ),
        ];
        for (slot, value) in &values {
            steps.push(message(
                0,
                "accept",
                vec![
                    save(saved([2, 0], *slot, value.clone())),
                    send(0, accepted([2, 0], *slot, value.clone())),
                ],
            ));
        }
        plans.push(plan(node, steps));
    }
    if late {
        plans.push(plan(
            3,
            vec![
                client(vec![
                    save(promised([1, 4], 0)),
                    save(saved([1, 4], 1, json!("late-y"))),
                ]),
                message(
                    0,
                    "prepare",
                    vec![
                        save(promised([2, 0], 0)),
                        send(
                            0,
                            promise([2, 0], 0, vec![entry([1, 4], 1, json!("late-y"))]),
                        ),
                    ],
                ),
            ],
        ));
    }
    let mut sim = ordinary(plans, true);
    for node in [1, 2] {
        propose(&mut sim, node, json!("history-trigger"));
    }
    if late {
        propose(&mut sim, 3, json!("minority-history-trigger"));
    }
    for value in clients {
        propose(&mut sim, 0, value);
    }
    advance(&mut sim, 5);
    sim
}

#[test]
fn protocol_wrong_adoption_keeps_conflicting_choices_and_supported_knowledge() {
    let mut sim = adoption_campaign(
        vec![(0, json!("y"))],
        vec![json!("y")],
        Some(json!("y")),
        false,
    );
    sim.finalize("horizon").unwrap();
    let report = capture(
        "protocol-wrong-adoption-conflicting-certificates",
        sim.rows(),
    );
    slot_obligation(&report, "adoption", 0, "violated");
    slot_obligation(&report, "fresh_origin_count", 0, "established");
    for line in admissions(sim.rows(), 0, "accept") {
        obligation(&report, "phase2_gate", line, "established");
    }
    certificate(&report, sim.rows(), 0, [1, 4], json!("x"));
    certificate(&report, sim.rows(), 0, [2, 0], json!("y"));
    supported_claim(&report, 0, 0, json!("y"));
    assert!(
        obligations(&report, "no_conflicting_choices")
            .iter()
            .any(|p| p["verdict"] == "violated")
    );
}

#[test]
fn protocol_legal_late_promise_does_not_revise_frozen_adoption() {
    let sim = adoption_campaign(
        vec![(0, json!("x")), (1, json!("a"))],
        vec![json!("a")],
        None,
        true,
    );
    let report = capture("protocol-legal-late-promise", sim.rows());
    let late = dispatches(sim.rows(), 0, 3, "promise")[0];
    assert!(
        admissions(sim.rows(), 0, "accept")
            .iter()
            .all(|line| *line < late)
    );
    slot_obligation(&report, "adoption", 0, "established");
    slot_obligation(&report, "fresh_origin_count", 1, "established");
    assert!(
        !obligations(&report, "proposal_immutability")
            .iter()
            .any(|p| p["verdict"] == "violated")
    );
    certificate(&report, sim.rows(), 0, [1, 4], json!("x"));
    certificate(&report, sim.rows(), 0, [2, 0], json!("x"));
    certificate(&report, sim.rows(), 1, [2, 0], json!("a"));
}

#[test]
fn protocol_equal_client_values_are_counted_not_deduplicated() {
    for (label, clients, second) in [
        ("one-input", vec![json!("x")], "violated"),
        ("two-inputs", vec![json!("x"), json!("x")], "established"),
    ] {
        let sim = empty_campaign(vec![(0, json!("x")), (1, json!("x"))], clients, vec![]);
        let report = capture(&format!("protocol-fresh-equal-values-{label}"), sim.rows());
        slot_obligation(&report, "fresh_origin_count", 0, "established");
        slot_obligation(&report, "fresh_origin_count", 1, second);
        certificate(&report, sim.rows(), 0, [2, 0], json!("x"));
        certificate(&report, sim.rows(), 1, [2, 0], json!("x"));
    }
}

#[test]
fn protocol_other_proposer_client_input_supplies_no_allowance() {
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))),
                    message(1, "promise", vec![]),
                    message(2, "promise", proposals([2, 0], &[(0, json!("x"))])),
                    message(1, "accepted", vec![]),
                    message(2, "accepted", vec![commit(0, json!("x"))]),
                ],
            ),
            plan(
                1,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![
                            save(promised([2, 0], 0)),
                            send(0, promise([2, 0], 0, vec![])),
                        ],
                    ),
                    message(
                        0,
                        "accept",
                        vec![
                            save(saved([2, 0], 0, json!("x"))),
                            send(0, accepted([2, 0], 0, json!("x"))),
                        ],
                    ),
                ],
            ),
            plan(
                2,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![
                            save(promised([2, 0], 0)),
                            send(0, promise([2, 0], 0, vec![])),
                        ],
                    ),
                    message(
                        0,
                        "accept",
                        vec![
                            save(saved([2, 0], 0, json!("x"))),
                            send(0, accepted([2, 0], 0, json!("x"))),
                        ],
                    ),
                ],
            ),
        ],
        true,
    );
    propose(&mut sim, 4, json!("x"));
    advance(&mut sim, 5);
    let report = capture("protocol-origin-other-proposer-input", sim.rows());
    slot_obligation(&report, "fresh_origin_count", 0, "violated");
    for line in admissions(sim.rows(), 0, "accept") {
        obligation(&report, "phase2_gate", line, "established");
    }
    certificate(&report, sim.rows(), 0, [2, 0], json!("x"));
    supported_claim(&report, 0, 0, json!("x"));
}

#[test]
fn protocol_adoption_credit_requires_same_slot_and_exact_value() {
    let cases = [
        (
            "same-slot",
            vec![(0, json!("x")), (1, json!("x"))],
            vec![json!("x")],
            1,
            "established",
        ),
        (
            "different-slot",
            vec![(1, json!("x"))],
            vec![json!("a")],
            1,
            "violated",
        ),
        (
            "wrong-value",
            vec![(0, json!("y"))],
            vec![json!("a")],
            0,
            "violated",
        ),
    ];
    for (label, values, clients, slot, verdict) in cases {
        let sim = adoption_campaign(values, clients, None, false);
        let report = capture(&format!("protocol-origin-adoption-{label}"), sim.rows());
        slot_obligation(&report, "fresh_origin_count", slot, verdict);
        if label == "same-slot" {
            slot_obligation(&report, "adoption", 0, "established");
        }
        if label == "wrong-value" {
            slot_obligation(&report, "adoption", 0, "violated");
        }
        certificate(&report, sim.rows(), 0, [1, 4], json!("x"));
        for attempt in report["attempts"].as_array().unwrap() {
            let adopted = label == "same-slot" && attempt["slot"] == 0;
            assert_eq!(
                attempt["origin"]["kind"],
                if adopted { "adopted" } else { "fresh" }
            );
            certificate(
                &report,
                sim.rows(),
                attempt["slot"].as_u64().unwrap(),
                [2, 0],
                attempt["value"].clone(),
            );
        }
    }
}

#[test]
fn protocol_close_allocates_one_broadcast_once_and_unclosed_consumes_nothing() {
    let mut sim = empty_campaign(
        vec![(0, json!("x")), (1, json!("x"))],
        vec![json!("x")],
        vec![],
    );
    let completion = dispatches(sim.rows(), 0, 2, "promise")[0];
    let input = &sim.rows()[completion - 1].fields["input_id"];
    let close = matching_lines(sim.rows(), |r| {
        r.what == "close" && &r.fields["input_id"] == input
    })[0];
    let open = capture("protocol-allocation-unclosed", &sim.rows()[..close - 1]);
    assert_eq!(open["attempts"].as_array().unwrap().len(), 2);
    assert!(
        !obligations(&open, "fresh_origin_count")
            .iter()
            .any(|p| p["verdict"] == "violated")
    );
    assert!(
        obligations(&open, "fresh_origin_count")
            .iter()
            .any(|p| p["verdict"] == "pending")
    );
    for a in open["attempts"].as_array().unwrap() {
        assert_eq!(a["origin"]["kind"], "undetermined");
        assert!(a["origin"]["allocation_line"].is_null());
        assert!(a["origin"]["client_count"].is_null());
    }
    let closed = capture(
        "protocol-allocation-closed-before-effects",
        &sim.rows()[..close],
    );
    slot_obligation(&closed, "fresh_origin_count", 0, "established");
    slot_obligation(&closed, "fresh_origin_count", 1, "violated");
    assert_eq!(closed["attempts"].as_array().unwrap().len(), 2);
    for a in closed["attempts"].as_array().unwrap() {
        let allocated = a["slot"].as_u64().unwrap() + 1;
        assert_eq!(a["sends"].as_array().unwrap().len(), 3);
        assert_eq!(a["origin"]["allocation_line"], close);
        assert_eq!(a["origin"]["client_count"]["available"], 1);
        assert_eq!(a["origin"]["client_count"]["allocated"], allocated);
        assert_eq!(a["origin"]["client_count"]["certain"], true);
    }
    sim.finalize("horizon").unwrap();
    let whole = capture("protocol-allocation-broadcast-once", sim.rows());
    slot_obligation(&whole, "fresh_origin_count", 0, "established");
    slot_obligation(&whole, "fresh_origin_count", 1, "violated");
    certificate(&whole, sim.rows(), 0, [2, 0], json!("x"));
    certificate(&whole, sim.rows(), 1, [2, 0], json!("x"));
}

fn close_of(rows: &[Event], dispatch: usize) -> usize {
    let input = &rows[dispatch - 1].fields["input_id"];
    matching_lines(rows, |r| {
        r.what == "close" && &r.fields["input_id"] == input
    })[0]
}
fn tick_dispatches(rows: &[Event], node: i32) -> Vec<usize> {
    matching_lines(rows, |r| {
        r.what == "dispatch" && r.why == "tick" && r.fields["to"] == node
    })
}

#[test]
fn protocol_first_tick_checks_self_other_null_and_declared_round() {
    let cases = [
        ("self", 0, Some(0), 7, Some([7, 0]), "established"),
        ("other", 4, Some(0), 11, None, "established"),
        ("null", 4, None, 11, None, "established"),
        ("wrong-round", 0, Some(0), 7, Some([8, 0]), "violated"),
        ("other-activates", 4, Some(0), 11, Some([11, 4]), "violated"),
        ("null-activates", 4, None, 11, Some([11, 4]), "violated"),
    ];
    for (label, node, active, round, emitted, verdict) in cases {
        let answer = emitted
            .map(|b| broadcast(&[1, 2, 3], prepare(b, 0)))
            .unwrap_or_default();
        let mut plans = vec![configured(
            node,
            config(&[1, 2, 3], 0, active, round),
            vec![tick(answer)],
        )];
        if node != 0 {
            plans.push(configured(0, config(&[1, 2, 3], 0, None, 2), vec![]));
        }
        let mut sim = ordinary(plans, true);
        advance(&mut sim, 2);
        let report = capture(&format!("protocol-activation-{label}"), sim.rows());
        let first = tick_dispatches(sim.rows(), node)[0];
        let checked = obligation(&report, "activation", close_of(sim.rows(), first), verdict);
        assert_eq!(checked["subject"]["input_line"], first);
        assert_eq!(
            admissions(sim.rows(), node, "prepare").len(),
            if emitted.is_some() { 3 } else { 0 }
        );
    }
}

#[test]
fn protocol_late_spawn_has_its_own_first_tick_and_never_restarts() {
    let mut rig = fixture(
        &[1, 2, 3],
        0,
        vec![
            configured(
                0,
                config(&[1, 2, 3], 0, Some(0), 7),
                vec![tick(broadcast(&[1, 2, 3], prepare([7, 0], 0)))],
            ),
            configured(
                4,
                config(&[1, 2, 3], 0, Some(4), 11),
                vec![tick(broadcast(&[1, 2, 3], prepare([11, 4], 0)))],
            ),
        ],
        true,
        &[4],
    );
    advance(&mut rig.sim, 3);
    rig.spawn(4);
    advance(&mut rig.sim, 3);
    let report = capture("protocol-activation-late-spawn-no-restart", rig.sim.rows());
    for node in [0, 4] {
        let ticks = tick_dispatches(rig.sim.rows(), node);
        assert_eq!(admissions(rig.sim.rows(), node, "prepare").len(), 3);
        obligation(
            &report,
            "activation",
            close_of(rig.sim.rows(), ticks[0]),
            "established",
        );
        for dispatch in &ticks[1..] {
            obligation(
                &report,
                "no_retry",
                close_of(rig.sim.rows(), *dispatch),
                "established",
            );
        }
    }
    assert_eq!(
        rig.sim.rows()[tick_dispatches(rig.sim.rows(), 4)[0] - 1].when,
        4
    );
}

#[test]
fn protocol_later_tick_cannot_restart_an_existing_campaign() {
    let mut sim = ordinary(
        vec![plan(
            0,
            vec![
                tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))),
                tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))),
            ],
        )],
        true,
    );
    advance(&mut sim, 2);
    let report = capture("protocol-activation-restart-mutant", sim.rows());
    let ticks = tick_dispatches(sim.rows(), 0);
    obligation(
        &report,
        "activation",
        close_of(sim.rows(), ticks[0]),
        "established",
    );
    obligation(
        &report,
        "no_retry",
        close_of(sim.rows(), ticks[1]),
        "violated",
    );
    assert_eq!(admissions(sim.rows(), 0, "prepare").len(), 6);
}

#[test]
fn protocol_nack_does_not_retry_or_prevent_another_acceptance_quorum() {
    for retry in [false, true] {
        let mut sim = ordinary(
            vec![
                plan(
                    0,
                    vec![
                        client(vec![]),
                        tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))),
                        message(1, "promise", vec![]),
                        message(2, "promise", proposals([2, 0], &[(0, json!("x"))])),
                        message(
                            1,
                            "nack",
                            if retry {
                                broadcast(&[1, 2, 3], prepare([3, 0], 0))
                            } else {
                                vec![]
                            },
                        ),
                        message(2, "accepted", vec![]),
                        message(3, "accepted", vec![commit(0, json!("x"))]),
                    ],
                ),
                plan(
                    1,
                    vec![
                        message(
                            0,
                            "prepare",
                            vec![
                                save(promised([2, 0], 0)),
                                send(0, promise([2, 0], 0, vec![])),
                            ],
                        ),
                        message(
                            0,
                            "accept",
                            vec![save(promised([3, 4], 0)), send(0, nack([2, 0], [3, 4]))],
                        ),
                    ],
                ),
                plan(
                    2,
                    vec![
                        message(
                            0,
                            "prepare",
                            vec![
                                save(promised([2, 0], 0)),
                                send(0, promise([2, 0], 0, vec![])),
                            ],
                        ),
                        message(0, "accept", persisted_reply(0, [2, 0], 0, json!("x"))),
                    ],
                ),
                plan(
                    3,
                    vec![
                        message(
                            0,
                            "prepare",
                            vec![
                                save(promised([2, 0], 0)),
                                send(0, promise([2, 0], 0, vec![])),
                            ],
                        ),
                        message(0, "accept", persisted_reply(0, [2, 0], 0, json!("x"))),
                    ],
                ),
            ],
            true,
        );
        propose(&mut sim, 0, json!("x"));
        advance(&mut sim, 5);
        let label = if retry {
            "retry-mutant"
        } else {
            "no-retry-control"
        };
        let report = capture(&format!("protocol-nack-{label}"), sim.rows());
        let received = dispatches(sim.rows(), 0, 1, "nack")[0];
        obligation(
            &report,
            "no_retry",
            close_of(sim.rows(), received),
            if retry { "violated" } else { "established" },
        );
        certificate(&report, sim.rows(), 0, [2, 0], json!("x"));
        supported_claim(&report, 0, 0, json!("x"));
    }
}

#[test]
fn protocol_ballot_owner_and_proposer_membership_do_not_filter_certificates() {
    for (label, actor, eligible) in [
        ("wrong-owner", 0, true),
        ("unconfigured-proposer", 4, false),
    ] {
        let b = [2, 4];
        let mut plans = vec![];
        for node in [0, 1, 2, 3, 4] {
            let mut cfg = config(&[1, 2, 3], 0, None, 2);
            if !eligible {
                cfg["proposers"] = json!([0]);
            }
            let steps = if node == actor {
                vec![
                    client(proposals(b, &[(0, json!("x"))])),
                    message(1, "accepted", vec![]),
                    message(2, "accepted", vec![commit(0, json!("x"))]),
                ]
            } else if [1, 2].contains(&node) {
                vec![message(
                    actor,
                    "accept",
                    persisted_reply(actor, b, 0, json!("x")),
                )]
            } else {
                vec![]
            };
            plans.push(configured(node, cfg, steps));
        }
        let mut sim = ordinary(plans, false);
        propose(&mut sim, actor, json!("x"));
        advance(&mut sim, 2);
        let report = capture(&format!("protocol-authority-{label}"), sim.rows());
        for line in admissions(sim.rows(), actor, "accept") {
            obligation(&report, "proposer_authority", line, "violated");
        }
        certificate(&report, sim.rows(), 0, b, json!("x"));
        supported_claim(&report, actor, 0, json!("x"));
    }
}

#[test]
fn protocol_duplicate_and_ineligible_promises_never_complete_phase_one() {
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    client(vec![]),
                    client(vec![]),
                    client(vec![]),
                    tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))),
                    message(1, "promise", vec![]),
                    message(1, "promise", proposals([2, 0], &[(0, json!("x"))])),
                    message(9, "promise", proposals([2, 0], &[(1, json!("x"))])),
                    message(2, "promise", proposals([2, 0], &[(2, json!("x"))])),
                ],
            ),
            unsupported(
                1,
                vec![message(
                    0,
                    "prepare",
                    vec![
                        send(0, promise([2, 0], 0, vec![])),
                        send(0, promise([2, 0], 0, vec![])),
                    ],
                )],
            ),
            unsupported(2, vec![client(vec![send(0, promise([2, 0], 0, vec![]))])]),
            unsupported(9, vec![client(vec![send(0, promise([2, 0], 0, vec![]))])]),
        ],
        true,
    );
    for _ in 0..3 {
        propose(&mut sim, 0, json!("x"));
    }
    advance(&mut sim, 3);
    propose(&mut sim, 9, json!("ineligible-trigger"));
    advance(&mut sim, 1);
    propose(&mut sim, 2, json!("eligible-trigger"));
    advance(&mut sim, 1);
    let report = capture("protocol-phase1-duplicate-ineligible-members", sim.rows());
    let sends = admissions(sim.rows(), 0, "accept");
    assert_eq!(sends.len(), 9);
    for line in &sends[..6] {
        obligation(&report, "phase2_gate", *line, "violated");
        let adoption = obligation(&report, "adoption", *line, "violated");
        assert!(adoption.get("unavailable_premises").is_none());
        assert_eq!(adoption["gaps"], json!([]));
    }
    for line in &sends[6..] {
        obligation(&report, "phase2_gate", *line, "established");
        obligation(&report, "adoption", *line, "established");
    }
    let valid = dispatches(sim.rows(), 0, 2, "promise")[0];
    let gate = obligation(&report, "phase2_gate", sends[6], "established");
    assert!(gate["support"].as_array().unwrap().contains(&json!(valid)));
    for attempt in report["attempts"].as_array().unwrap() {
        assert_eq!(attempt["origin"]["kind"], "fresh");
        assert_eq!(attempt["origin"]["client_count"]["certain"], true);
        assert!(attempt["origin"].get("unavailable_premises").is_none());
    }
    for slot in [0, 1, 2] {
        slot_obligation(&report, "fresh_origin_count", slot, "established");
    }
}

#[test]
fn protocol_choice_counts_distinct_header_acceptors_not_duplicate_or_outsider_saves() {
    let b = [2, 0];
    for (prefix_name, name, proposed, second, chosen) in [
        (
            "protocol-choice-duplicate-outsider-no-quorum",
            "protocol-choice-distinct-header-members",
            json!("x"),
            json!("x"),
            true,
        ),
        (
            "identity-zero-control-prefix",
            "identity-zero-control",
            json!(0.0),
            json!(0.0),
            true,
        ),
        (
            "identity-zero-signed-prefix",
            "identity-zero-signed",
            json!(0.0),
            json!(-0.0),
            true,
        ),
        (
            "identity-nested-zero-prefix",
            "identity-nested-zero",
            json!({"v":[0.0]}),
            json!({"v":[-0.0]}),
            true,
        ),
        (
            "identity-integer-float-prefix",
            "identity-integer-float",
            json!(0),
            json!(0.0),
            false,
        ),
    ] {
        let mut sim = ordinary(
            vec![
                plan(
                    0,
                    vec![
                        client(broadcast(&[1, 9], accept(b, 0, proposed.clone()))),
                        client(vec![send(2, accept(b, 0, proposed.clone()))]),
                    ],
                ),
                plan(
                    1,
                    vec![message(
                        0,
                        "accept",
                        vec![
                            save(promised(b, 0)),
                            save(saved(b, 0, proposed.clone())),
                            save(saved(b, 0, proposed.clone())),
                        ],
                    )],
                ),
                plan(
                    2,
                    vec![message(
                        0,
                        "accept",
                        vec![save(promised(b, 0)), save(saved(b, 0, second))],
                    )],
                ),
                unsupported(
                    9,
                    vec![message(
                        0,
                        "accept",
                        vec![save(promised(b, 0)), save(saved(b, 0, proposed.clone()))],
                    )],
                ),
            ],
            false,
        );
        propose(&mut sim, 0, proposed.clone());
        advance(&mut sim, 1);
        let before = capture(prefix_name, sim.rows());
        assert!(before["choices"].as_array().unwrap().is_empty());
        propose(&mut sim, 0, json!("second-member-trigger"));
        advance(&mut sim, 1);
        let report = capture(name, sim.rows());
        assert_eq!(report["attempts"].as_array().unwrap().len(), 1);
        if !chosen {
            assert!(report["choices"].as_array().unwrap().is_empty());
            assert!(report["attempts"][0]["exact_choice"].is_null());
            continue;
        }
        let choice = certificate(&report, sim.rows(), 0, b, proposed);
        assert_eq!(choice["first_quorum_line"], 54);
        assert_eq!(report["attempts"][0]["exact_choice"], choice["id"]);
        let members: BTreeSet<_> = choice["witnesses"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["acceptor"].as_i64().unwrap())
            .collect();
        assert_eq!(members, BTreeSet::from([1, 2]));
        let repeated = choice["witnesses"]
            .as_array()
            .unwrap()
            .iter()
            .find(|w| w["acceptor"] == 1)
            .unwrap();
        assert_eq!(repeated["save_lines"].as_array().unwrap().len(), 2);
    }
}

#[test]
fn protocol_conflicting_proposal_assertions_are_visible_before_any_commit_refusal() {
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    client(vec![]),
                    tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))),
                    message(1, "promise", vec![]),
                    message(2, "promise", proposals([2, 0], &[(0, json!("x"))])),
                    client(proposals([2, 0], &[(0, json!("y"))])),
                ],
            ),
            plan(
                1,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![
                            save(promised([2, 0], 0)),
                            send(0, promise([2, 0], 0, vec![])),
                        ],
                    ),
                    message(0, "accept", vec![save(saved([2, 0], 0, json!("x")))]),
                    message(0, "accept", vec![save(saved([2, 0], 0, json!("y")))]),
                ],
            ),
            plan(
                2,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![
                            save(promised([2, 0], 0)),
                            send(0, promise([2, 0], 0, vec![])),
                        ],
                    ),
                    message(0, "accept", vec![save(saved([2, 0], 0, json!("x")))]),
                    message(0, "accept", vec![save(saved([2, 0], 0, json!("y")))]),
                ],
            ),
        ],
        true,
    );
    propose(&mut sim, 0, json!("x"));
    advance(&mut sim, 4);
    propose(&mut sim, 0, json!("y"));
    advance(&mut sim, 1);
    let report = capture("protocol-immutable-proposal-assertions", sim.rows());
    let assertions = admissions(sim.rows(), 0, "accept");
    assert_eq!(assertions.len(), 6);
    let immutable = slot_obligation(&report, "proposal_immutability", 0, "violated");
    assert_eq!(immutable["subject"]["ballot"], json!([2, 0]));
    assert_eq!(immutable["subject"]["value"], "y");
    assert!(report["claims"].as_array().unwrap().is_empty());
    certificate(&report, sim.rows(), 0, [2, 0], json!("x"));
    certificate(&report, sim.rows(), 0, [2, 0], json!("y"));
}

#[test]
fn protocol_individually_valid_reports_keep_lower_ballot_contradiction_and_open_turn() {
    let members = [1, 2, 3, 5, 6];
    let mut failure = message(3, "promise", vec![]);
    failure.panic = true;
    let mut rig = fixture(
        &members,
        0,
        vec![
            plan(
                0,
                vec![
                    client(vec![]),
                    tick(broadcast(&members, prepare([2, 0], 0))),
                    message(1, "promise", vec![]),
                    message(2, "promise", vec![]),
                    failure,
                ],
            ),
            unsupported(
                1,
                vec![message(
                    0,
                    "prepare",
                    vec![
                        save(promised([1, 4], 0)),
                        save(saved([1, 4], 0, json!("x"))),
                        save(promised([2, 0], 0)),
                        send(0, promise([2, 0], 0, vec![entry([1, 4], 0, json!("x"))])),
                    ],
                )],
            ),
            unsupported(
                2,
                vec![message(
                    0,
                    "prepare",
                    vec![
                        save(promised([2, 0], 0)),
                        save(saved([2, 0], 0, json!("higher"))),
                        send(
                            0,
                            promise([2, 0], 0, vec![entry([2, 0], 0, json!("higher"))]),
                        ),
                    ],
                )],
            ),
            unsupported(
                3,
                vec![message(
                    0,
                    "prepare",
                    vec![
                        save(promised([1, 4], 0)),
                        save(saved([1, 4], 0, json!("y"))),
                        save(promised([2, 0], 0)),
                        send(0, promise([2, 0], 0, vec![entry([1, 4], 0, json!("y"))])),
                    ],
                )],
            ),
            plan(
                5,
                vec![client(vec![
                    save(promised([1, 4], 0)),
                    save(saved([1, 4], 0, json!("x"))),
                ])],
            ),
            plan(
                6,
                vec![client(vec![
                    save(promised([1, 4], 0)),
                    save(saved([1, 4], 0, json!("x"))),
                ])],
            ),
        ],
        true,
        &[],
    );
    for node in [5, 6] {
        propose(&mut rig.sim, node, json!("history-trigger"));
    }
    propose(&mut rig.sim, 0, json!("waiting-client"));
    assert!(catch_unwind(AssertUnwindSafe(|| advance(&mut rig.sim, 3))).is_err());
    let report = capture(
        "protocol-valid-report-contradiction-open-turn",
        rig.sim.rows(),
    );
    let first = dispatches(rig.sim.rows(), 0, 1, "promise")[0];
    let conflicting = dispatches(rig.sim.rows(), 0, 3, "promise")[0];
    let observation = report["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["kind"] == "conflicting_promise_reports" && o["node"] == 0)
        .unwrap_or_else(|| panic!("lost testimony contradiction: {}", report["observations"]));
    assert_eq!(observation["slot"], 0);
    assert_eq!(observation["ballot"], json!([1, 4]));
    let cited = observation["lines"].as_array().unwrap();
    assert!(cited.contains(&json!(first)) && cited.contains(&json!(conflicting)));
    assert!(
        observation["values"]
            .as_array()
            .unwrap()
            .contains(&json!("x"))
    );
    assert!(
        observation["values"]
            .as_array()
            .unwrap()
            .contains(&json!("y"))
    );
    let input = &rig.sim.rows()[conflicting - 1].fields["input_id"];
    assert!(
        matching_lines(rig.sim.rows(), |r| r.what == "close"
            && &r.fields["input_id"] == input)
        .is_empty()
    );
    assert!(!report["observations"].as_array().unwrap().iter().any(|o| {
        o["kind"] == "semantic_promise_report" && o["line"] == conflicting && o["valid"] == false
    }));
    certificate(&report, rig.sim.rows(), 0, [1, 4], json!("x"));
}

#[test]
fn protocol_fifo_is_separate_from_equal_value_allowances() {
    let sim = empty_campaign(
        vec![(0, json!("b")), (1, json!("a"))],
        vec![json!("a"), json!("b")],
        vec![],
    );
    let report = capture("protocol-fifo-wrong-head", sim.rows());
    slot_obligation(&report, "fresh_origin_count", 0, "established");
    slot_obligation(&report, "fresh_origin_count", 1, "established");
    slot_obligation(&report, "fifo", 0, "violated");
}

#[test]
fn protocol_adopted_traversal_preserves_fifo_and_does_not_consume_client_heads() {
    let values = [
        (0, json!("x")),
        (1, json!("a")),
        (2, json!("y")),
        (3, json!("b")),
    ];
    let reports = vec![entry([1, 4], 0, json!("x")), entry([1, 4], 2, json!("y"))];
    let mut plans = vec![plan(
        0,
        vec![
            client(vec![]),
            client(vec![]),
            tick(broadcast(&[1, 2, 3], prepare([2, 0], 0))),
            message(1, "promise", vec![]),
            message(2, "promise", proposals([2, 0], &values)),
        ],
    )];
    for node in [1, 2] {
        let mut steps = vec![
            client(vec![
                save(promised([1, 4], 0)),
                save(saved([1, 4], 0, json!("x"))),
                save(saved([1, 4], 2, json!("y"))),
            ]),
            message(
                0,
                "prepare",
                vec![
                    save(promised([2, 0], 0)),
                    send(0, promise([2, 0], 0, reports.clone())),
                ],
            ),
        ];
        for (slot, value) in &values {
            steps.push(message(
                0,
                "accept",
                vec![save(saved([2, 0], *slot, value.clone()))],
            ));
        }
        plans.push(plan(node, steps));
    }
    let mut sim = ordinary(plans, true);
    for node in [1, 2] {
        propose(&mut sim, node, json!("history-trigger"));
    }
    propose(&mut sim, 0, json!("a"));
    propose(&mut sim, 0, json!("b"));
    advance(&mut sim, 4);
    let report = capture("protocol-fifo-adopted-slot-traversal", sim.rows());
    for (slot, value) in values {
        slot_obligation(&report, "fifo", slot, "established");
        certificate(&report, sim.rows(), slot, [2, 0], value);
    }
    for slot in [0, 2] {
        slot_obligation(&report, "adoption", slot, "established");
    }
    for slot in [1, 3] {
        slot_obligation(&report, "fresh_origin_count", slot, "established");
    }
    for attempt in report["attempts"].as_array().unwrap() {
        let slot = attempt["slot"].as_u64().unwrap();
        assert_eq!(
            attempt["origin"]["kind"],
            if [0, 2].contains(&slot) {
                "adopted"
            } else {
                "fresh"
            }
        );
    }
}

#[test]
fn protocol_reply_request_is_separate_from_matching_durable_support() {
    let b = [2, 0];
    let mut sim = ordinary(
        vec![
            plan(
                0,
                vec![
                    client(broadcast(&[1, 2], accept(b, 0, json!("requested-x")))),
                    message(1, "accepted", vec![]),
                    message(2, "accepted", vec![commit(0, json!("asserted-y"))]),
                ],
            ),
            plan(
                1,
                vec![message(
                    0,
                    "accept",
                    persisted_reply(0, b, 0, json!("asserted-y")),
                )],
            ),
            plan(
                2,
                vec![message(
                    0,
                    "accept",
                    persisted_reply(0, b, 0, json!("asserted-y")),
                )],
            ),
        ],
        false,
    );
    propose(&mut sim, 0, json!("requested-x"));
    advance(&mut sim, 2);
    let report = capture("protocol-reply-request-versus-durable-support", sim.rows());
    for node in [1, 2] {
        let line = admissions(sim.rows(), node, "accepted")[0];
        obligation(&report, "accepted_reply_support", line, "established");
        obligation(&report, "reply_request", line, "violated");
    }
    certificate(&report, sim.rows(), 0, b, json!("asserted-y"));
    supported_claim(&report, 0, 0, json!("asserted-y"));
}

#[test]
fn protocol_nack_truthfulness_joins_request_and_current_watermark() {
    for (label, rejected, reported, verdict) in [
        ("truthful", [2, 0], [3, 4], "established"),
        ("wrong-rejected", [1, 0], [3, 4], "violated"),
        ("wrong-watermark", [2, 0], [4, 4], "violated"),
    ] {
        let mut sim = ordinary(
            vec![
                plan(
                    0,
                    vec![client(broadcast(&[1, 2, 3], accept([2, 0], 0, json!("x"))))],
                ),
                plan(
                    1,
                    vec![message(
                        0,
                        "accept",
                        vec![save(promised([3, 4], 0)), send(0, nack(rejected, reported))],
                    )],
                ),
                plan(
                    2,
                    vec![message(
                        0,
                        "accept",
                        persisted_reply(0, [2, 0], 0, json!("x")),
                    )],
                ),
                plan(
                    3,
                    vec![message(
                        0,
                        "accept",
                        persisted_reply(0, [2, 0], 0, json!("x")),
                    )],
                ),
            ],
            false,
        );
        propose(&mut sim, 0, json!("x"));
        advance(&mut sim, 2);
        let report = capture(&format!("protocol-nack-truthfulness-{label}"), sim.rows());
        obligation(
            &report,
            "nack_truthfulness",
            admissions(sim.rows(), 1, "nack")[0],
            verdict,
        );
        certificate(&report, sim.rows(), 0, [2, 0], json!("x"));
    }
}

#[test]
fn value_equality_groups_attempts_without_reallocating_signed_zero() {
    let b = [2, 0];
    for (name, first, repeated, same) in [
        ("identity-attempt-zero", json!(-0.0), json!(0.0), true),
        (
            "identity-attempt-nested",
            json!({"v":[-0.0]}),
            json!({"v":[0.0]}),
            true,
        ),
        (
            "identity-attempt-integer-float",
            json!(0),
            json!(0.0),
            false,
        ),
    ] {
        let mut sim = ordinary(
            vec![plan(
                0,
                vec![
                    client(vec![send(1, accept(b, 0, first.clone()))]),
                    client(vec![send(2, accept(b, 0, repeated))]),
                ],
            )],
            false,
        );
        propose(&mut sim, 0, first.clone());
        propose(&mut sim, 0, json!("rebroadcast"));
        let report = capture(name, sim.rows());
        let attempts = report["attempts"].as_array().unwrap();
        assert_eq!(attempts.len(), if same { 1 } else { 2 });
        assert_eq!(attempts[0]["value"], first);
        if name == "identity-attempt-zero" {
            assert!(attempts[0]["value"].as_f64().unwrap().is_sign_negative());
        }
        if same {
            assert_eq!(attempts[0]["sends"].as_array().unwrap().len(), 2);
            assert_eq!(
                attempts[0]["origin"]["client_count"],
                json!({"available":1,"allocated":1,"certain":true})
            );
            let counts = obligations(&report, "fresh_origin_count");
            assert_eq!(counts.len(), 1);
            assert_eq!(counts[0]["verdict"], "established");
        } else {
            assert!(
                obligations(&report, "proposal_immutability")
                    .iter()
                    .any(|p| p["verdict"] == "violated")
            );
        }
    }
}

#[test]
fn client_allowances_use_value_equality_not_serialized_number_spelling() {
    let b = [2, 0];
    for (name, client_value, proposal_value, available) in [
        (
            "identity-client-positive-negative",
            json!(0.0),
            json!(-0.0),
            1,
        ),
        (
            "identity-client-negative-positive",
            json!(-0.0),
            json!(0.0),
            1,
        ),
        (
            "identity-client-nested",
            json!({"v":[0.0]}),
            json!({"v":[-0.0]}),
            1,
        ),
        ("identity-client-integer-float", json!(0), json!(0.0), 0),
        ("identity-client-float-integer", json!(0.0), json!(0), 0),
    ] {
        let mut sim = ordinary(
            vec![plan(0, vec![client(proposals(b, &[(0, proposal_value)]))])],
            false,
        );
        propose(&mut sim, 0, client_value);
        let report = capture(name, sim.rows());
        assert_eq!(report["attempts"][0]["origin"]["kind"], "fresh");
        assert_eq!(
            report["attempts"][0]["origin"]["client_count"],
            json!({"available":available,"allocated":1,"certain":true})
        );
        slot_obligation(
            &report,
            "fresh_origin_count",
            0,
            if available == 1 {
                "established"
            } else {
                "violated"
            },
        );
    }
}

#[test]
fn signed_zero_clients_and_fresh_allocations_share_one_cumulative_allowance() {
    let b = [2, 0];
    let mut sim = ordinary(
        vec![plan(
            0,
            vec![
                client(vec![]),
                client(proposals(b, &[(0, json!(-0.0)), (1, json!(0.0))])),
                client(proposals(b, &[(2, json!(-0.0))])),
            ],
        )],
        false,
    );
    propose(&mut sim, 0, json!(0.0));
    propose(&mut sim, 0, json!(-0.0));
    propose(&mut sim, 0, json!("third-allocation"));
    let report = capture("identity-client-cumulative-exhaustion", sim.rows());
    for slot in 0..3 {
        let attempt = report["attempts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["slot"] == slot)
            .unwrap();
        assert_eq!(
            attempt["origin"]["client_count"],
            json!({"available":2,"allocated":slot+1,"certain":true})
        );
        slot_obligation(
            &report,
            "fresh_origin_count",
            slot,
            if slot < 2 { "established" } else { "violated" },
        );
    }
}
