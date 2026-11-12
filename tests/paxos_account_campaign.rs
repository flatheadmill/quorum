use quorum::paxos_account::examine;
use quorum::tape::{self, Event, Header, NodeSpec, WorldConfig};
use quorum::{Context, Desire, Input, Node, NodeId, Simulator, Slot};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    fs, io,
};

// Recorded answers are owned by each test, not by the Multi-Paxos inhabitant.
enum Trigger {
    Client,
    Tick,
    Message(i32, &'static str),
}
struct Step {
    trigger: Trigger,
    answer: Vec<Desire>,
}
fn client(answer: Vec<Desire>) -> Step {
    Step {
        trigger: Trigger::Client,
        answer,
    }
}
fn tick(answer: Vec<Desire>) -> Step {
    Step {
        trigger: Trigger::Tick,
        answer,
    }
}
fn message(from: i32, kind: &'static str, answer: Vec<Desire>) -> Step {
    Step {
        trigger: Trigger::Message(from, kind),
        answer,
    }
}
struct Script {
    steps: VecDeque<Step>,
    crashed: bool,
}
impl Node for Script {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        let Some(step) = self.steps.front() else {
            return;
        };
        let matches = match (&step.trigger, input) {
            (Trigger::Client, Input::Client { .. }) | (Trigger::Tick, Input::Tick { .. }) => true,
            (Trigger::Message(from, kind), Input::Message { envelope, .. }) => {
                envelope.from == NodeId(*from) && envelope.payload["type"] == *kind
            }
            _ => false,
        };
        if matches {
            for desire in self.steps.pop_front().unwrap().answer {
                match desire {
                    Desire::Send { to, message } => ctx.send(to, message),
                    Desire::Save { record } => ctx.save(record),
                    Desire::Commit { slot, entry } => ctx.commit(slot, entry),
                }
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
    steps: Vec<Step>,
}
fn plan(node: i32, steps: Vec<Step>) -> Plan {
    Plan {
        node,
        covered: true,
        steps,
    }
}
fn script(node: i32, steps: Vec<Step>) -> Plan {
    Plan {
        node,
        covered: false,
        steps,
    }
}
fn fixture(members: &[i32], active: Option<i32>, plans: Vec<Plan>) -> Simulator {
    let mut all = BTreeMap::new();
    for node in [0, 4].into_iter().chain(members.iter().copied()) {
        all.insert(node, plan(node, vec![]));
    }
    for p in plans {
        all.insert(p.node, p);
    }
    let specs = all
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
                json!({"acceptors":members,"learners":[0,4],"proposers":[0,4],
                "active_proposer":active,"from_slot":0,"initial_round":2,"initial_state":"empty"})
            } else {
                json!({"fixture":"paxos-account-campaign"})
            },
        })
        .collect();
    let header = Header::new(
        "paxos-account-campaign",
        42,
        WorldConfig {
            base_latency: 1,
            jitter: 0,
            tick_interval: 1,
            deadlock_threshold: 200,
        },
        specs,
        members.iter().map(|node| NodeId(*node)).collect(),
    );
    let mut sim = Simulator::new(header, Box::new(io::sink())).unwrap();
    for (node, p) in all {
        sim.spawn(
            NodeId(node),
            Box::new(Script {
                steps: p.steps.into(),
                crashed: false,
            }),
        )
        .unwrap();
    }
    sim
}
fn propose(sim: &mut Simulator, node: i32, value: &str) {
    sim.client_message(NodeId(node), json!({"type":"propose","value":value}))
        .unwrap();
}
fn send(to: i32, payload: Value) -> Desire {
    Desire::Send {
        to: NodeId(to),
        message: payload,
    }
}
fn save(record: Value) -> Desire {
    Desire::Save { record }
}
fn broadcast(members: &[i32], payload: Value) -> Vec<Desire> {
    members
        .iter()
        .map(|node| send(*node, payload.clone()))
        .collect()
}
fn prepare() -> Value {
    json!({"type":"prepare","ballot":[2,0],"from_slot":0})
}
fn promise(entries: Vec<Value>) -> Value {
    json!({"type":"promise","ballot":[2,0],"from_slot":0,"accepted":entries})
}
fn entry(ballot: [u64; 2], value: &str) -> Value {
    json!({"slot":0,"ballot":ballot,"value":value})
}
fn promised(ballot: [u64; 2]) -> Desire {
    save(json!({"record":"promised","ballot":ballot,"from_slot":0}))
}
fn saved(ballot: [u64; 2], slot: u64, value: &str) -> Desire {
    save(json!({"record":"accepted","ballot":ballot,"slot":slot,"value":value}))
}
fn accept(slot: u64, value: &str) -> Value {
    json!({"type":"accept","ballot":[2,0],"slot":slot,"value":value})
}
fn accept_reply(slot: u64, value: &str) -> Step {
    message(
        0,
        "accept",
        vec![
            saved([2, 0], slot, value),
            send(
                0,
                json!({"type":"accepted","ballot":[2,0],"slot":slot,"value":value}),
            ),
        ],
    )
}
fn capture(name: &str, rows: &[Event]) -> Value {
    let bytes = tape::encode(rows);
    let report = examine(&bytes);
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/paxos-account-evidence");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join(format!("{name}.jsonl")), &bytes).unwrap();
    fs::write(
        root.join(format!("{name}.report.json")),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["source_sha256"], tape::hash(&bytes));
    assert!(report["world"]["error"].is_null(), "{}", report["world"]);
    assert_eq!(report["world"]["valid_rows"], rows.len());
    report
}
fn lines(rows: &[Event], predicate: impl Fn(&Event) -> bool) -> Vec<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(i, row)| predicate(row).then_some(i + 1))
        .collect()
}
fn dispatches(rows: &[Event], from: i32, kind: &str) -> Vec<usize> {
    lines(rows, |row| {
        row.what == "dispatch"
            && row.why == "message"
            && row.fields["to"] == 0
            && row.fields["from"] == from
            && row.fields["message"]["type"] == kind
    })
}
fn close_of(rows: &[Event], dispatch: usize) -> usize {
    let id = &rows[dispatch - 1].fields["input_id"];
    lines(rows, |row| {
        row.what == "close" && &row.fields["input_id"] == id
    })[0]
}
fn obligation<'a>(report: &'a Value, name: &str, boundary: usize, verdict: &str) -> &'a Value {
    let found = report["obligations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["proposition"] == name && p["boundary_line"] == boundary)
        .unwrap_or_else(|| panic!("missing {name} at {boundary}: {}", report["obligations"]));
    assert_eq!(found["verdict"], verdict, "{found}");
    assert!(
        !found[if verdict == "violated" {
            "counterevidence"
        } else {
            "support"
        }]
        .as_array()
        .unwrap()
        .is_empty()
    );
    found
}
fn slot_obligations(report: &Value, name: &str, slot: u64, verdict: &str) {
    let found: Vec<_> = report["obligations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["proposition"] == name && p["subject"]["slot"] == slot)
        .collect();
    assert!(!found.is_empty(), "missing {name} for slot {slot}");
    for p in found {
        assert_eq!(p["verdict"], verdict, "{p}");
    }
}
fn first_quorum(report: &Value) -> &Value {
    report["observations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["kind"] == "first_promise_quorum" && o["node"] == 0)
        .unwrap()
}
fn real_gaps(report: &Value, gaps: &Value) {
    assert!(!gaps.as_array().unwrap().is_empty());
    for gap in gaps.as_array().unwrap() {
        assert!(
            report["interpretation_findings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|finding| &finding["id"] == gap),
            "invented gap {gap}"
        );
    }
}

fn unavailable_premise<'a>(proposition: &'a Value, name: &str) -> &'a Value {
    assert!(!proposition["reason"].as_str().unwrap().is_empty());
    proposition["unavailable_premises"]
        .as_array()
        .unwrap_or_else(|| panic!("missing unavailable dependency: {proposition}"))
        .iter()
        .find(|premise| premise["premise"] == name)
        .unwrap_or_else(|| panic!("missing {name} dependency: {proposition}"))
}

fn contradictory_map(proposition: &Value) {
    let map = unavailable_premise(proposition, "first_quorum_map");
    assert_eq!(map["gaps"], json!([]));
    assert_eq!(proposition["gaps"], json!([]));
    for line in [113, 119] {
        assert!(
            map["counterevidence"]
                .as_array()
                .unwrap()
                .contains(&json!(line))
        );
        assert!(
            proposition["counterevidence"]
                .as_array()
                .unwrap()
                .contains(&json!(line))
        );
    }
    for line in [113, 116, 119] {
        assert!(map["support"].as_array().unwrap().contains(&json!(line)));
    }
}

#[test]
fn campaign_inactive_readable_and_unreadable_prepare_keep_independent_judgments() {
    let members = [1, 2, 3];
    let mut sim = fixture(
        &members,
        None,
        vec![
            plan(
                0,
                vec![
                    client(vec![]),
                    tick(broadcast(&members, prepare())),
                    message(1, "promise", vec![]),
                    message(2, "promise", broadcast(&members, accept(0, "x"))),
                ],
            ),
            plan(
                1,
                vec![message(
                    0,
                    "prepare",
                    vec![promised([2, 0]), send(0, promise(vec![]))],
                )],
            ),
            plan(
                2,
                vec![message(
                    0,
                    "prepare",
                    vec![promised([2, 0]), send(0, promise(vec![]))],
                )],
            ),
        ],
    );
    propose(&mut sim, 0, "x");
    assert!(sim.run(3).unwrap());
    // Preserve the supplied control's exact open-admission boundary.
    let rows = &sim.rows()[..72];
    assert_eq!(dispatches(rows, 1, "promise"), vec![64]);
    assert_eq!(dispatches(rows, 2, "promise"), vec![67]);
    let readable = capture("campaign-inactive-readable", rows);
    obligation(&readable, "activation", 15, "violated");
    obligation(&readable, "phase2_gate", 72, "established");
    assert_eq!(readable["attempts"][0]["origin"]["kind"], "fresh");
    assert_eq!(
        readable["attempts"][0]["origin"]["client_count"]["certain"],
        true
    );
    slot_obligations(&readable, "fresh_origin_count", 0, "established");
    slot_obligations(&readable, "fifo", 0, "established");
    obligation(&readable, "proposal_drain", 71, "established");

    let mut unreadable_rows = rows.to_vec();
    for row in &mut unreadable_rows {
        if let Some(message) = row.fields.get_mut("message")
            && message["type"] == "prepare"
        {
            message["type"] = json!("unreadable_prepare");
        }
    }
    let unreadable = capture("campaign-inactive-unreadable", &unreadable_rows);
    obligation(&unreadable, "activation", 15, "violated");
    for (name, boundary) in [
        ("promise_report_handling", 65),
        ("promise_report_handling", 71),
        ("phase2_gate", 72),
        ("adoption", 72),
    ] {
        let proposition = obligation(&unreadable, name, boundary, "indeterminate");
        real_gaps(&unreadable, &proposition["gaps"]);
        real_gaps(
            &unreadable,
            &unavailable_premise(proposition, "first_quorum_map")["gaps"],
        );
    }
    let origin = &unreadable["attempts"][0]["origin"];
    assert_eq!(origin["kind"], "undetermined");
    assert_eq!(origin["client_count"]["allocated"], 0);
    assert_eq!(origin["client_count"]["certain"], false);
    real_gaps(&unreadable, &origin["gaps"]);
    real_gaps(
        &unreadable,
        &unavailable_premise(origin, "first_quorum_map")["gaps"],
    );
    slot_obligations(&unreadable, "fresh_origin_count", 0, "indeterminate");
    slot_obligations(&unreadable, "fifo", 0, "indeterminate");
}

#[test]
fn campaign_contradiction_freezes_actual_encounter_not_later_consistent_subset() {
    let members = [1, 2, 3, 5, 6];
    let mut proposals = broadcast(&members, accept(0, "higher"));
    proposals.extend(broadcast(&members, accept(1, "waiting-client")));
    let mut sim = fixture(
        &members,
        Some(0),
        vec![
            plan(
                0,
                vec![
                    client(vec![]),
                    tick(broadcast(&members, prepare())),
                    message(1, "promise", vec![]),
                    message(2, "promise", vec![]),
                    message(3, "promise", vec![]),
                    message(5, "promise", proposals),
                    message(
                        3,
                        "accepted",
                        vec![Desire::Commit {
                            slot: Slot(0),
                            entry: json!("higher"),
                        }],
                    ),
                    message(
                        3,
                        "accepted",
                        vec![Desire::Commit {
                            slot: Slot(1),
                            entry: json!("waiting-client"),
                        }],
                    ),
                    client(vec![send(1, accept(2, "later"))]),
                ],
            ),
            script(
                1,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![
                            promised([1, 4]),
                            saved([1, 4], 0, "x"),
                            promised([2, 0]),
                            send(0, promise(vec![entry([1, 4], "x")])),
                        ],
                    ),
                    accept_reply(0, "higher"),
                    accept_reply(1, "waiting-client"),
                ],
            ),
            script(
                2,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![
                            promised([2, 0]),
                            saved([2, 0], 0, "higher"),
                            send(0, promise(vec![entry([2, 0], "higher")])),
                        ],
                    ),
                    accept_reply(0, "higher"),
                    accept_reply(1, "waiting-client"),
                ],
            ),
            script(
                3,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![
                            promised([1, 4]),
                            saved([1, 4], 0, "y"),
                            promised([2, 0]),
                            send(0, promise(vec![entry([1, 4], "y")])),
                        ],
                    ),
                    accept_reply(0, "higher"),
                    accept_reply(1, "waiting-client"),
                ],
            ),
            script(
                5,
                vec![message(
                    0,
                    "prepare",
                    vec![promised([2, 0]), send(0, promise(vec![]))],
                )],
            ),
            script(6, vec![]),
        ],
    );
    propose(&mut sim, 0, "waiting-client");
    assert!(sim.run(3).unwrap());
    let reports: Vec<_> = [1, 2, 3, 5]
        .iter()
        .map(|from| dispatches(sim.rows(), *from, "promise")[0])
        .collect();
    assert_eq!(reports, vec![113, 116, 119, 122]);
    let prefix = capture("campaign-contradiction-first-encounter", &sim.rows()[..119]);
    let continued = capture("campaign-contradiction-continued", sim.rows());
    let frozen = first_quorum(&prefix);
    assert_eq!(frozen["dispatch_lines"], json!([113, 116, 119]));
    assert_eq!(frozen["line"], 119);
    assert_eq!(frozen["first_quorum_determined"], true);
    assert_eq!(frozen["adoption_map_determined"], false);
    assert!(frozen["adoption_map"].is_null());
    assert_eq!(frozen, first_quorum(&continued));
    assert!(
        continued["observations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["kind"] == "conflicting_promise_reports" && o["lines"] == json!([113, 119]))
    );
    obligation(
        &continued,
        "promise_report_handling",
        close_of(sim.rows(), 119),
        "violated",
    );
    obligation(&continued, "promise_report_handling", 133, "violated");
    assert_eq!(close_of(sim.rows(), 119), 120);
    contradictory_map(obligation(
        &continued,
        "proposal_drain",
        120,
        "indeterminate",
    ));
    assert!(
        !continued["obligations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| { p["proposition"] == "proposal_drain" && p["boundary_line"] == 133 })
    );
    for slot in [0, 1] {
        slot_obligations(&continued, "phase2_gate", slot, "established");
        for gate in continued["obligations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["proposition"] == "phase2_gate" && p["subject"]["slot"] == slot)
        {
            assert!(gate.get("unavailable_premises").is_none());
        }
        for name in ["adoption", "fifo", "fresh_origin_count"] {
            slot_obligations(&continued, name, slot, "indeterminate");
            for proposition in continued["obligations"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|p| p["proposition"] == name && p["subject"]["slot"] == slot)
            {
                contradictory_map(proposition);
            }
        }
    }
    for attempt in continued["attempts"].as_array().unwrap() {
        assert_eq!(attempt["origin"]["kind"], "undetermined");
        assert_eq!(attempt["origin"]["client_count"]["certain"], false);
        contradictory_map(&attempt["origin"]);
        assert!(
            !attempt["origin"]["support"]
                .as_array()
                .unwrap()
                .contains(&json!(122))
        );
    }
    assert!(sim.run(5).unwrap());
    propose(&mut sim, 0, "later");
    let later = capture("campaign-contradiction-later-knowledge", sim.rows());
    assert_eq!(frozen, first_quorum(&later));
    for (slot, value) in [(0, "higher"), (1, "waiting-client")] {
        assert!(
            later["choices"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["slot"] == slot && c["value"] == value)
        );
        let claim = later["claims"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["kind"] == "commit" && c["node"] == 0 && c["slot"] == slot)
            .unwrap();
        assert_eq!(claim["local_basis"]["verdict"], "established");
        assert_eq!(claim["transitive_support"]["verdict"], "established");
    }
    slot_obligations(&later, "fresh_origin_count", 2, "indeterminate");
    slot_obligations(&later, "fifo", 2, "indeterminate");
    slot_obligations(&later, "adoption", 2, "indeterminate");
    for proposition in later["obligations"].as_array().unwrap().iter().filter(|p| {
        p["subject"]["slot"] == 2
            && ["adoption", "fifo", "fresh_origin_count"]
                .iter()
                .any(|name| p["proposition"] == *name)
    }) {
        contradictory_map(proposition);
        if proposition["proposition"] != "adoption" {
            let cumulative = unavailable_premise(proposition, "cumulative_client_consumption");
            assert!(
                cumulative["support"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(133))
            );
        }
    }
    let origin = &later["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|attempt| attempt["slot"] == 2)
        .unwrap()["origin"];
    contradictory_map(origin);
    assert!(
        unavailable_premise(origin, "cumulative_client_consumption")["support"]
            .as_array()
            .unwrap()
            .contains(&json!(133))
    );
}

#[test]
fn campaign_postfreeze_gaps_and_late_reports_preserve_historical_map_and_exemption() {
    let members = [1, 2, 3];
    let mut sim = fixture(
        &members,
        Some(0),
        vec![
            plan(
                0,
                vec![
                    tick(broadcast(&members, prepare())),
                    message(1, "promise", vec![]),
                    message(2, "promise", vec![]),
                    client(vec![send(4, json!({"type":"unreadable_output"}))]),
                    client(vec![send(1, accept(0, "old"))]),
                ],
            ),
            script(
                1,
                vec![message(
                    0,
                    "prepare",
                    vec![
                        saved([1, 4], 0, "old"),
                        send(0, promise(vec![entry([1, 4], "old")])),
                    ],
                )],
            ),
            script(
                2,
                vec![message(
                    0,
                    "prepare",
                    vec![
                        saved([1, 4], 0, "old"),
                        send(0, promise(vec![entry([1, 4], "old")])),
                    ],
                )],
            ),
            script(
                3,
                vec![client(vec![send(0, promise(vec![entry([2, 0], "late")]))])],
            ),
            script(
                4,
                vec![client(vec![send(0, json!({"type":"unreadable_peer"}))])],
            ),
        ],
    );
    assert!(sim.run(3).unwrap());
    let boundary = dispatches(sim.rows(), 2, "promise")[0];
    let prefix = capture("campaign-postfreeze-map-prefix", &sim.rows()[..boundary]);
    assert_eq!(first_quorum(&prefix)["adoption_map_determined"], true);
    propose(&mut sim, 0, "noise");
    propose(&mut sim, 3, "late-report-trigger");
    propose(&mut sim, 4, "gap-trigger");
    assert!(sim.run(4).unwrap());
    propose(&mut sim, 0, "waiting");
    let report = capture("campaign-postfreeze-map-preserved", sim.rows());
    assert_eq!(first_quorum(&prefix), first_quorum(&report));
    let origin = &report["attempts"][0]["origin"];
    assert_eq!(origin["kind"], "adopted");
    assert!(origin["client_count"].is_null());
    assert_eq!(origin["acceptance_ancestry"]["verdict"], "established");
    slot_obligations(&report, "phase2_gate", 0, "established");
    slot_obligations(&report, "adoption", 0, "established");
    slot_obligations(&report, "fresh_origin_count", 0, "established");
    assert!(origin.get("unavailable_premises").is_none());
    let late = dispatches(sim.rows(), 3, "promise")[0];
    assert!(!origin["support"].as_array().unwrap().contains(&json!(late)));
}

#[test]
fn campaign_fully_witnessed_gate_survives_prefreeze_unreadable_input() {
    let members = [1, 2, 3];
    let mut sim = fixture(
        &members,
        Some(0),
        vec![
            plan(
                0,
                vec![
                    client(vec![]),
                    tick(broadcast(&members, prepare())),
                    message(1, "promise", vec![]),
                    message(2, "promise", vec![send(1, accept(0, "x"))]),
                ],
            ),
            script(
                1,
                vec![message(0, "prepare", vec![send(0, promise(vec![]))])],
            ),
            script(
                2,
                vec![message(0, "prepare", vec![send(0, promise(vec![]))])],
            ),
            script(
                4,
                vec![client(vec![send(0, json!({"type":"unreadable_peer"}))])],
            ),
        ],
    );
    propose(&mut sim, 0, "x");
    assert!(sim.run(1).unwrap());
    propose(&mut sim, 4, "gap-trigger");
    assert!(sim.run(3).unwrap());
    let report = capture("campaign-witnessed-gate-with-prefreeze-gap", sim.rows());
    slot_obligations(&report, "phase2_gate", 0, "established");
    slot_obligations(&report, "adoption", 0, "indeterminate");
    assert_eq!(first_quorum(&report)["adoption_map_determined"], false);
    assert_eq!(report["attempts"][0]["origin"]["kind"], "undetermined");
    real_gaps(&report, &report["attempts"][0]["origin"]["gaps"]);
    real_gaps(
        &report,
        &unavailable_premise(&report["attempts"][0]["origin"], "first_quorum_map")["gaps"],
    );
    for gate in report["obligations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["proposition"] == "phase2_gate")
    {
        assert!(gate.get("unavailable_premises").is_none());
    }
}

#[test]
fn campaign_later_readable_prepare_does_not_restore_unavailable_earlier_campaign() {
    let members = [1, 2, 3];
    let mut sim = fixture(
        &members,
        None,
        vec![
            plan(
                0,
                vec![
                    client(vec![send(4, json!({"type":"unreadable_prepare"}))]),
                    message(1, "promise", vec![]),
                    message(2, "promise", vec![]),
                    client(broadcast(&members, prepare())),
                    message(1, "promise", vec![]),
                    message(2, "promise", vec![send(1, accept(0, "x"))]),
                ],
            ),
            script(
                1,
                vec![
                    client(vec![send(0, promise(vec![]))]),
                    message(0, "prepare", vec![send(0, promise(vec![]))]),
                ],
            ),
            script(
                2,
                vec![
                    client(vec![send(0, promise(vec![]))]),
                    message(0, "prepare", vec![send(0, promise(vec![]))]),
                ],
            ),
        ],
    );
    propose(&mut sim, 0, "x");
    propose(&mut sim, 1, "early-report");
    propose(&mut sim, 2, "early-report");
    assert!(sim.run(1).unwrap());
    let early_first = dispatches(sim.rows(), 1, "promise")[0];
    let early_second = dispatches(sim.rows(), 2, "promise")[0];
    propose(&mut sim, 0, "resume");
    assert!(sim.run(3).unwrap());
    let report = capture("campaign-later-prepare-preserves-earlier-gap", sim.rows());
    for dispatch in [early_first, early_second] {
        real_gaps(
            &report,
            &obligation(
                &report,
                "promise_report_handling",
                close_of(sim.rows(), dispatch),
                "indeterminate",
            )["gaps"],
        );
    }
    assert_eq!(first_quorum(&report)["first_quorum_determined"], false);
    assert_eq!(first_quorum(&report)["adoption_map_determined"], false);
    slot_obligations(&report, "phase2_gate", 0, "established");
    slot_obligations(&report, "adoption", 0, "indeterminate");
    assert_eq!(report["attempts"][0]["origin"]["kind"], "undetermined");
    real_gaps(&report, &report["attempts"][0]["origin"]["gaps"]);
    real_gaps(
        &report,
        &unavailable_premise(&report["attempts"][0]["origin"], "first_quorum_map")["gaps"],
    );
}

#[test]
fn campaign_invalid_report_does_not_reserve_sender_membership() {
    for (label, invalid, syntax_gap) in [
        (
            "syntax",
            json!({"type":"promise","ballot":[2,0],"from_slot":0,"accepted":"not-an-array"}),
            true,
        ),
        (
            "semantic",
            promise(vec![entry([1, 4], "bad"), entry([1, 4], "bad")]),
            false,
        ),
    ] {
        let members = [1, 2, 3];
        let mut sim = fixture(
            &members,
            Some(0),
            vec![
                plan(
                    0,
                    vec![
                        client(vec![]),
                        tick(broadcast(&members, prepare())),
                        message(1, "promise", vec![]),
                        message(2, "promise", vec![]),
                        message(1, "promise", vec![send(1, accept(0, "x"))]),
                    ],
                ),
                script(
                    1,
                    vec![
                        message(0, "prepare", vec![send(0, invalid)]),
                        client(vec![send(0, promise(vec![]))]),
                    ],
                ),
                script(
                    2,
                    vec![message(0, "prepare", vec![send(0, promise(vec![]))])],
                ),
            ],
        );
        propose(&mut sim, 0, "x");
        assert!(sim.run(3).unwrap());
        let early = capture(
            &format!("campaign-invalid-{label}-before-replacement"),
            sim.rows(),
        );
        assert!(
            !early["observations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|o| o["kind"] == "first_promise_quorum")
        );
        let invalid_line = dispatches(sim.rows(), 1, "promise")[0];
        let other_sender = dispatches(sim.rows(), 2, "promise")[0];
        propose(&mut sim, 1, "valid-replacement");
        assert!(sim.run(4).unwrap());
        let valid_line = dispatches(sim.rows(), 1, "promise")[1];
        let report = capture(
            &format!("campaign-invalid-{label}-valid-replacement"),
            sim.rows(),
        );
        assert_eq!(
            first_quorum(&report)["dispatch_lines"],
            json!([other_sender, valid_line])
        );
        assert!(
            !first_quorum(&report)["dispatch_lines"]
                .as_array()
                .unwrap()
                .contains(&json!(invalid_line))
        );
        slot_obligations(&report, "phase2_gate", 0, "established");
        assert_eq!(
            !report["interpretation_findings"]
                .as_array()
                .unwrap()
                .is_empty(),
            syntax_gap
        );
        if syntax_gap {
            assert_eq!(first_quorum(&report)["adoption_map_determined"], false);
        } else {
            assert_eq!(first_quorum(&report)["adoption_map_determined"], true);
            obligation(
                &report,
                "promise_report_handling",
                close_of(sim.rows(), invalid_line),
                "established",
            );
            slot_obligations(&report, "fresh_origin_count", 0, "established");
        }
    }
}

#[test]
fn campaign_later_determined_map_preserves_original_consumption_uncertainty() {
    let members = [1, 2, 3];
    let next_prepare = json!({"type":"prepare","ballot":[3,0],"from_slot":0});
    let next_promise = json!({"type":"promise","ballot":[3,0],"from_slot":0,"accepted":[]});
    let mut sim = fixture(
        &members,
        Some(0),
        vec![
            plan(
                0,
                vec![
                    client(vec![]),
                    tick(broadcast(&members, prepare())),
                    message(1, "promise", vec![]),
                    message(2, "promise", vec![send(1, accept(0, "earlier"))]),
                    client(broadcast(&members, next_prepare)),
                    message(1, "promise", vec![]),
                    message(
                        2,
                        "promise",
                        vec![send(
                            1,
                            json!({"type":"accept","ballot":[3,0],"slot":0,"value":"later"}),
                        )],
                    ),
                ],
            ),
            script(
                1,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![send(0, promise(vec![entry([1, 4], "x")]))],
                    ),
                    message(0, "prepare", vec![send(0, next_promise.clone())]),
                ],
            ),
            script(
                2,
                vec![
                    message(
                        0,
                        "prepare",
                        vec![send(0, promise(vec![entry([1, 4], "y")]))],
                    ),
                    message(0, "prepare", vec![send(0, next_promise)]),
                ],
            ),
        ],
    );
    propose(&mut sim, 0, "earlier");
    assert!(sim.run(3).unwrap());
    let original_reports = [
        dispatches(sim.rows(), 1, "promise")[0],
        dispatches(sim.rows(), 2, "promise")[0],
    ];
    let original_allocation = close_of(sim.rows(), original_reports[1]);
    propose(&mut sim, 0, "later");
    assert!(sim.run(5).unwrap());
    let report = capture("campaign-later-map-preserves-consumption-cause", sim.rows());
    let later = report["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|attempt| attempt["ballot"] == json!([3, 0]))
        .unwrap();
    let origin = &later["origin"];
    assert_eq!(origin["kind"], "fresh");
    assert_eq!(
        origin["client_count"],
        json!({"available":1,"allocated":1,"certain":false})
    );
    let map = unavailable_premise(origin, "first_quorum_map");
    assert_eq!(map["gaps"], json!([]));
    for line in original_reports {
        assert!(
            map["counterevidence"]
                .as_array()
                .unwrap()
                .contains(&json!(line))
        );
        assert!(
            origin["counterevidence"]
                .as_array()
                .unwrap()
                .contains(&json!(line))
        );
    }
    assert!(
        unavailable_premise(origin, "cumulative_client_consumption")["support"]
            .as_array()
            .unwrap()
            .contains(&json!(original_allocation))
    );
    let mut seen = Vec::new();
    for proposition in report["obligations"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["subject"]["ballot"] == json!([3, 0]))
    {
        let name = proposition["proposition"].as_str().unwrap();
        match name {
            "phase2_gate" | "adoption" => {
                assert_eq!(proposition["verdict"], "established");
                assert!(proposition.get("unavailable_premises").is_none());
                seen.push(name);
            }
            "fifo" | "fresh_origin_count" => {
                assert_eq!(proposition["verdict"], "indeterminate");
                assert_eq!(unavailable_premise(proposition, "first_quorum_map"), map);
                assert!(
                    unavailable_premise(proposition, "cumulative_client_consumption")["support"]
                        .as_array()
                        .unwrap()
                        .contains(&json!(original_allocation))
                );
                seen.push(name);
            }
            _ => {}
        }
    }
    for name in ["phase2_gate", "adoption", "fifo", "fresh_origin_count"] {
        assert!(seen.contains(&name), "missing later-campaign {name}");
    }
    let next_completion = dispatches(sim.rows(), 2, "promise")[1];
    let drain = obligation(
        &report,
        "proposal_drain",
        close_of(sim.rows(), next_completion),
        "indeterminate",
    );
    assert_eq!(unavailable_premise(drain, "first_quorum_map"), map);
    assert!(
        unavailable_premise(drain, "cumulative_client_consumption")["support"]
            .as_array()
            .unwrap()
            .contains(&json!(original_allocation))
    );
}
