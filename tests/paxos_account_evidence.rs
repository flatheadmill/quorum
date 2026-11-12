use quorum::paxos_account::examine;
use quorum::tape::{self, Event, Header, NodeSpec, WorldConfig};
use quorum::{Context, Desire, Input, Node, NodeId, Simulator, Slot};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fs, io};

// These nodes emit the answers specified by each counterexample. They neither
// implement Paxos nor consult an inhabitant/replay to decide the expected report.
struct Script {
    rules: Vec<Rule>,
    crashed: bool,
}
struct Rule {
    trigger: Trigger,
    answer: Vec<Desire>,
}
enum Trigger {
    Client(Value),
    Peer(i32, Value),
    Tick,
}
impl Rule {
    fn client(value: impl Into<Value>, answer: Vec<Desire>) -> Self {
        Self {
            trigger: Trigger::Client(value.into()),
            answer,
        }
    }
    fn peer(from: i32, message: Value, answer: Vec<Desire>) -> Self {
        Self {
            trigger: Trigger::Peer(from, message),
            answer,
        }
    }
    fn tick(answer: Vec<Desire>) -> Self {
        Self {
            trigger: Trigger::Tick,
            answer,
        }
    }
}
impl Node for Script {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        let index = self
            .rules
            .iter()
            .position(|rule| match (&rule.trigger, input) {
                (Trigger::Tick, Input::Tick { .. }) => true,
                (Trigger::Client(value), Input::Client { payload, .. }) => {
                    payload.get("value") == Some(value)
                }
                (Trigger::Peer(from, message), Input::Message { envelope, .. }) => {
                    envelope.from == NodeId(*from) && &envelope.payload == message
                }
                _ => false,
            });
        if let Some(index) = index {
            for desire in self.rules.remove(index).answer {
                match desire {
                    Desire::Save { record } => ctx.save(record),
                    Desire::Send { to, message } => ctx.send(to, message),
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
fn send(to: i32, message: Value) -> Desire {
    Desire::Send {
        to: NodeId(to),
        message,
    }
}
fn save(record: Value) -> Desire {
    Desire::Save { record }
}
fn commit(slot: u64, value: &str) -> Desire {
    Desire::Commit {
        slot: Slot(slot),
        entry: json!(value),
    }
}
fn accepted(slot: u64, ballot: [u64; 2], value: &str) -> Value {
    json!({"type":"accepted","slot":slot,"ballot":ballot,"value":value})
}
fn accept(slot: u64, ballot: [u64; 2], value: &str) -> Value {
    json!({"type":"accept","slot":slot,"ballot":ballot,"value":value})
}
fn chosen(slot: u64, ballot: [u64; 2], value: &str) -> Value {
    json!({"type":"chosen","slot":slot,"ballot":ballot,"value":value})
}
fn promised(ballot: [u64; 2]) -> Value {
    json!({"record":"promised","from_slot":0,"ballot":ballot})
}
fn acceptance(slot: u64, ballot: [u64; 2], value: &str) -> Value {
    json!({"record":"accepted","slot":slot,"ballot":ballot,"value":value})
}
fn prepare(ballot: [u64; 2]) -> Value {
    json!({"type":"prepare","ballot":ballot,"from_slot":0})
}
fn promise(ballot: [u64; 2], entries: Value) -> Value {
    json!({"type":"promise","ballot":ballot,"from_slot":0,"accepted":entries})
}
fn nack(rejected: [u64; 2], watermark: [u64; 2]) -> Value {
    json!({"type":"nack","rejected_ballot":rejected,"promised_ballot":watermark})
}
fn config(nodes: usize) -> Value {
    let members: Vec<_> = (0..nodes).collect();
    json!({"acceptors":[0,1,2],"learners":members,"proposers":members,
        "active_proposer":null,"from_slot":0,"initial_round":1,"initial_state":"empty"})
}
fn spec(node: usize, nodes: usize) -> NodeSpec {
    NodeSpec {
        node: NodeId(node as i32),
        algorithm: "multi-paxos".into(),
        config: config(nodes),
    }
}
fn simulator(specs: Vec<NodeSpec>, scripts: Vec<Vec<Rule>>) -> Simulator {
    simulator_with_ticks(specs, scripts, 0)
}
fn simulator_with_ticks(
    specs: Vec<NodeSpec>,
    scripts: Vec<Vec<Rule>>,
    tick_interval: u64,
) -> Simulator {
    assert_eq!(specs.len(), scripts.len());
    let header = Header::new(
        "paxos-account-evidence",
        42,
        WorldConfig {
            base_latency: 1,
            jitter: 0,
            tick_interval,
            deadlock_threshold: 200,
        },
        specs,
        vec![NodeId(0), NodeId(1), NodeId(2)],
    );
    let mut sim = Simulator::new(header, Box::new(io::sink())).unwrap();
    for (node, rules) in scripts.into_iter().enumerate() {
        sim.spawn(
            NodeId(node as i32),
            Box::new(Script {
                rules,
                crashed: false,
            }),
        )
        .unwrap();
    }
    sim
}
fn covered(scripts: Vec<Vec<Rule>>) -> Simulator {
    let nodes = scripts.len();
    simulator((0..nodes).map(|node| spec(node, nodes)).collect(), scripts)
}
fn trigger(sim: &mut Simulator, node: i32, value: &str) {
    sim.client_message(NodeId(node), json!({"type":"propose","value":value}))
        .unwrap();
}
fn line(rows: &[Event], predicate: impl Fn(&Event) -> bool) -> usize {
    rows.iter()
        .position(predicate)
        .expect("specified world event")
        + 1
}
fn lines(rows: &[Event], predicate: impl Fn(&Event) -> bool) -> Vec<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(i, row)| predicate(row).then_some(i + 1))
        .collect()
}
fn is(row: &Event, what: &str, why: &str, key: &str, value: &Value) -> bool {
    row.what == what && row.why == why && row.fields.get(key) == Some(value)
}
fn report(name: &str, bytes: &[u8]) -> Value {
    let result = examine(bytes);
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/paxos-account-evidence");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join(format!("{name}.jsonl")), bytes).unwrap();
    fs::write(
        directory.join(format!("{name}.report.json")),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    assert_eq!(result["schema_version"], 1);
    assert_eq!(result["source_sha256"], tape::hash(bytes));
    result
}
fn captured(name: &str, sim: &Simulator) -> Value {
    report(name, &tape::encode(sim.rows()))
}
fn array<'a>(object: &'a Value, name: &str) -> &'a [Value] {
    object[name]
        .as_array()
        .unwrap_or_else(|| panic!("missing array {name}: {object}"))
}
fn claim<'a>(r: &'a Value, node: i32, kind: &str, slot: u64, value: &str) -> &'a Value {
    array(r, "claims")
        .iter()
        .find(|c| {
            c["node"] == node && c["kind"] == kind && c["slot"] == slot && c["value"] == value
        })
        .expect("specified claim")
}
fn choice<'a>(r: &'a Value, slot: u64, ballot: [u64; 2], value: &str) -> &'a Value {
    array(r, "choices")
        .iter()
        .find(|c| c["slot"] == slot && c["ballot"] == json!(ballot) && c["value"] == value)
        .expect("specified certificate")
}
fn obligation<'a>(r: &'a Value, name: &str, node: Option<i32>, verdict: &str) -> &'a Value {
    array(r, "obligations")
        .iter()
        .find(|p| {
            p["proposition"] == name
                && p["verdict"] == verdict
                && node.is_none_or(|node| p["subject"]["node"] == node)
        })
        .unwrap_or_else(|| panic!("missing {name} {node:?} {verdict}: {}", r["obligations"]))
}
fn propositions(c: &Value, local: &str, transitive: &str) {
    assert_eq!(c["local_basis"]["proposition"], "local_basis");
    assert_eq!(c["local_basis"]["verdict"], local, "{c}");
    assert_eq!(c["transitive_support"]["proposition"], "transitive_support");
    assert_eq!(c["transitive_support"]["verdict"], transitive, "{c}");
    assert_eq!(
        c["local_basis"]["boundary_line"],
        c["effect"]["attempt_line"]
    );
    assert_eq!(
        c["transitive_support"]["boundary_line"],
        c["effect"]["attempt_line"]
    );
}
fn cited(proposition: &Value, name: &str, expected: &[usize]) {
    let actual: BTreeSet<_> = array(proposition, name)
        .iter()
        .map(|v| v.as_u64().unwrap() as usize)
        .collect();
    for line in expected {
        assert!(
            *line > 0 && actual.contains(line),
            "missing line {line}: {proposition}"
        );
    }
}
fn certificate(r: &Value, rows: &[Event], slot: u64, ballot: [u64; 2], value: &str) {
    let saves = lines(rows, |row| {
        is(
            row,
            "result",
            "save",
            "record",
            &acceptance(slot, ballot, value),
        )
    });
    assert!(saves.len() >= 2);
    let c = choice(r, slot, ballot, value);
    assert_eq!(c["quorum"], 2);
    assert_eq!(c["first_quorum_line"], saves[1]);
    let cited_saves: BTreeSet<_> = array(c, "first_support")
        .iter()
        .map(|s| s["save_line"].as_u64().unwrap() as usize)
        .collect();
    assert_eq!(
        cited_saves,
        saves[..2].iter().copied().collect::<BTreeSet<_>>()
    );
}

#[test]
fn covered_false_chosen_is_not_repaired_by_receiver_trust() {
    let message = chosen(0, [1, 0], "false");
    let mut sim = covered(vec![
        vec![Rule::client("announce", vec![send(3, message.clone())])],
        vec![],
        vec![],
        vec![Rule::peer(0, message, vec![commit(0, "false")])],
    ]);
    trigger(&mut sim, 0, "announce");
    sim.run(1).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("covered-false-chosen", &sim);
    let sender = claim(&r, 0, "chosen", 0, "false");
    propositions(sender, "violated", "violated");
    let receiver = claim(&r, 3, "commit", 0, "false");
    propositions(receiver, "established", "violated");
    let received = line(sim.rows(), |row| {
        is(
            row,
            "dispatch",
            "message",
            "message",
            &chosen(0, [1, 0], "false"),
        )
    });
    cited(&receiver["local_basis"], "support", &[received]);
    assert!(array(&r, "choices").is_empty());
    assert!(array(receiver, "choice_at_boundary").is_empty());
    obligation(&r, "receiver_conformance", Some(3), "established");
}

#[test]
fn undeclared_owner_is_testimony_not_a_synthetic_sender_claim() {
    let message = chosen(0, [1, 0], "owned");
    let mut specs: Vec<_> = (0..4).map(|node| spec(node, 4)).collect();
    for node in [0, 2] {
        specs[node].algorithm = "receiver-script".into();
        specs[node].config = json!({});
    }
    let mut sim = simulator(
        specs,
        vec![
            vec![Rule::client("announce", vec![send(3, message.clone())])],
            vec![],
            vec![],
            vec![Rule::peer(0, message, vec![commit(0, "owned")])],
        ],
    );
    trigger(&mut sim, 0, "announce");
    sim.run(1).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("unsupported-authoritative-owner", &sim);
    assert_eq!(array(&r, "claims").len(), 1);
    propositions(
        claim(&r, 3, "commit", 0, "owned"),
        "established",
        "indeterminate",
    );
    obligation(&r, "receiver_conformance", Some(3), "established");
    let safety = obligation(&r, "no_conflicting_choices", None, "indeterminate");
    assert_eq!(
        safety["unavailable_premises"][0]["premise"],
        "initial_accepted_state"
    );
    assert_eq!(safety["unavailable_premises"][0]["nodes"], json!([0, 2]));
    cited(&safety["unavailable_premises"][0], "support", &[1]);
    assert!(array(&r, "interpretation_findings").is_empty());
    assert!(array(&r, "choices").is_empty());
    assert!(
        !array(&r, "obligations")
            .iter()
            .any(|p| p["subject"]["node"] == 0 || p["subject"]["node"] == 2),
        "Undeclared script actors have no sender-contract verdicts"
    );
}

#[test]
fn conflicting_adoption_certificate_precedes_the_refused_supported_commit() {
    let old = [1, 3];
    let new = [2, 4];
    let suffix = json!([{"slot":0,"ballot":old,"value":"x"}]);
    let report_x = promise(new, suffix);
    let mut specs: Vec<_> = (0..5).map(|node| spec(node, 5)).collect();
    specs[4].config["active_proposer"] = json!(4);
    specs[4].config["initial_round"] = json!(2);
    let mut sim = simulator_with_ticks(
        specs,
        vec![
            vec![
                Rule::client(
                    "seed",
                    vec![
                        save(promised(old)),
                        save(acceptance(0, old, "x")),
                        send(3, accepted(0, old, "x")),
                    ],
                ),
                Rule::peer(
                    4,
                    prepare(new),
                    vec![save(promised(new)), send(4, report_x.clone())],
                ),
                Rule::peer(
                    4,
                    accept(0, new, "y"),
                    vec![
                        save(acceptance(0, new, "y")),
                        send(4, accepted(0, new, "y")),
                    ],
                ),
            ],
            vec![
                Rule::client(
                    "seed",
                    vec![
                        save(promised(old)),
                        save(acceptance(0, old, "x")),
                        send(3, accepted(0, old, "x")),
                    ],
                ),
                Rule::peer(
                    4,
                    prepare(new),
                    vec![save(promised(new)), send(4, report_x.clone())],
                ),
                Rule::peer(
                    4,
                    accept(0, new, "y"),
                    vec![
                        save(acceptance(0, new, "y")),
                        send(4, accepted(0, new, "y")),
                    ],
                ),
            ],
            vec![],
            vec![Rule::peer(1, accepted(0, old, "x"), vec![commit(0, "x")])],
            vec![
                Rule::tick(vec![
                    send(0, prepare(new)),
                    send(1, prepare(new)),
                    send(2, prepare(new)),
                ]),
                Rule::peer(
                    1,
                    report_x,
                    vec![send(0, accept(0, new, "y")), send(1, accept(0, new, "y"))],
                ),
                Rule::peer(1, accepted(0, new, "y"), vec![commit(0, "y")]),
            ],
        ],
        1,
    );
    trigger(&mut sim, 0, "seed");
    trigger(&mut sim, 1, "seed");
    trigger(&mut sim, 4, "y");
    sim.run(1).unwrap();
    sim.run(4).unwrap();
    let cut = captured("illegal-adoption-before-conflicting-commit", &sim);
    certificate(&cut, sim.rows(), 0, old, "x");
    certificate(&cut, sim.rows(), 0, new, "y");
    obligation(&cut, "no_conflicting_choices", None, "violated");
    obligation(&cut, "adoption", Some(4), "violated");
    let observed = attempt(&cut, 4, 0, new, "y");
    assert_eq!(observed["exact_choice"], choice(&cut, 0, new, "y")["id"]);
    assert_eq!(observed["origin"]["kind"], "fresh");
    assert_eq!(observed["resolution"]["exact_choice"], true);
    assert_eq!(observed["resolution"]["world_open"], true);
    assert!(!array(&cut, "claims").iter().any(|c| c["value"] == "y"));
    assert!(sim.run(5).unwrap_err().contains("split_brain"));
    sim.finalize("refused").unwrap();
    let r = captured("illegal-adoption-refused-supported-commit", &sim);
    certificate(&r, sim.rows(), 0, old, "x");
    certificate(&r, sim.rows(), 0, new, "y");
    let c = claim(&r, 4, "commit", 0, "y");
    propositions(c, "established", "established");
    assert_eq!(c["effect"]["outcome"], "refused");
    assert_eq!(
        attempt(&r, 4, 0, new, "y")["resolution"]["state"],
        "exact_choice"
    );
    assert_eq!(
        array(c, "choice_at_boundary"),
        &[choice(&r, 0, new, "y")["id"].clone()]
    );
    let saves = lines(sim.rows(), |row| {
        is(row, "result", "save", "record", &acceptance(0, new, "y"))
    });
    cited(&c["transitive_support"], "support", &saves);
    propositions(claim(&r, 3, "commit", 0, "x"), "established", "established");
}

#[test]
fn complete_knowledge_survives_origin_authority_and_stale_acceptance_violations() {
    let b = [1, 0];
    let mut specs: Vec<_> = (0..5).map(|node| spec(node, 5)).collect();
    for spec in &mut specs {
        spec.config["proposers"] = json!([0]);
    }
    let mut sim = simulator(
        specs,
        vec![
            vec![Rule::peer(
                3,
                accept(0, b, "invented"),
                vec![
                    save(promised([2, 0])),
                    save(acceptance(0, b, "invented")),
                    send(3, accepted(0, b, "invented")),
                ],
            )],
            vec![Rule::peer(
                3,
                accept(0, b, "invented"),
                vec![
                    save(promised([2, 0])),
                    save(acceptance(0, b, "invented")),
                    send(3, accepted(0, b, "invented")),
                ],
            )],
            vec![],
            vec![
                Rule::client(
                    "trigger",
                    vec![
                        send(0, accept(0, b, "invented")),
                        send(1, accept(0, b, "invented")),
                    ],
                ),
                Rule::peer(
                    1,
                    accepted(0, b, "invented"),
                    vec![commit(0, "invented"), send(4, chosen(0, b, "invented"))],
                ),
            ],
            vec![Rule::peer(
                3,
                chosen(0, b, "invented"),
                vec![commit(0, "invented")],
            )],
        ],
    );
    trigger(&mut sim, 3, "trigger");
    sim.run(3).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("knowledge-despite-conformance", &sim);
    certificate(&r, sim.rows(), 0, b, "invented");
    for (node, kind) in [(3, "commit"), (3, "chosen"), (4, "commit")] {
        propositions(
            claim(&r, node, kind, 0, "invented"),
            "established",
            "established",
        );
    }
    obligation(&r, "fresh_origin_count", Some(3), "violated");
    obligation(&r, "proposer_authority", Some(3), "violated");
    obligation(&r, "accepted_watermark", Some(0), "violated");
    obligation(&r, "accepted_watermark", Some(1), "violated");
    obligation(&r, "receiver_conformance", Some(4), "violated");
}

#[test]
fn recursive_chosen_proof_keeps_a_successful_alternative_to_false_testimony() {
    let b = [1, 3];
    let message = chosen(0, b, "chain");
    let mut sim = covered(vec![
        vec![Rule::client(
            "persist",
            vec![
                save(promised(b)),
                save(acceptance(0, b, "chain")),
                send(3, accepted(0, b, "chain")),
            ],
        )],
        vec![Rule::client(
            "persist",
            vec![
                save(promised(b)),
                save(acceptance(0, b, "chain")),
                send(3, accepted(0, b, "chain")),
            ],
        )],
        vec![],
        vec![Rule::peer(
            1,
            accepted(0, b, "chain"),
            vec![send(4, message.clone())],
        )],
        vec![Rule::peer(
            3,
            message.clone(),
            vec![send(5, message.clone())],
        )],
        vec![
            Rule::client("false-first", vec![send(3, message.clone())]),
            Rule::peer(4, message, vec![commit(0, "chain")]),
        ],
    ]);
    trigger(&mut sim, 5, "false-first");
    sim.run(1).unwrap();
    trigger(&mut sim, 0, "persist");
    trigger(&mut sim, 1, "persist");
    sim.run(4).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("multihop-chosen-alternative-path", &sim);
    propositions(claim(&r, 5, "chosen", 0, "chain"), "violated", "violated");
    for (node, kind) in [(3, "chosen"), (4, "chosen"), (5, "commit")] {
        propositions(
            claim(&r, node, kind, 0, "chain"),
            "established",
            "established",
        );
    }
    let leaf = claim(&r, 5, "commit", 0, "chain");
    let saves = lines(sim.rows(), |row| {
        is(row, "result", "save", "record", &acceptance(0, b, "chain"))
    });
    let hops = lines(sim.rows(), |row| {
        is(
            row,
            "dispatch",
            "message",
            "message",
            &chosen(0, b, "chain"),
        )
    });
    cited(&leaf["transitive_support"], "support", &saves);
    // All three world-recorded Chosen dispatches remain visible; the first is
    // an unsuccessful alternative, not a reason to reject the complete path.
    assert_eq!(hops.len(), 3);
    let alternatives = array(claim(&r, 3, "chosen", 0, "chain"), "ballot_paths");
    assert!(
        alternatives
            .iter()
            .any(|p| p["kind"] == "chosen" && p["transitive_support"]["verdict"] == "violated")
    );
    assert!(
        alternatives.iter().any(|p| p["kind"] == "accepted_quorum"
            && p["transitive_support"]["verdict"] == "established")
    );
    let relay = claim(&r, 4, "chosen", 0, "chain");
    let joined = array(relay, "ballot_paths")
        .iter()
        .find(|p| p["kind"] == "chosen")
        .unwrap();
    let sender = array(&r, "observations")
        .iter()
        .find(|o| o["kind"] == "knowledge_evidence" && o["id"] == joined["sender_knowledge_id"])
        .expect("recursive sender evidence");
    assert!(
        array(sender, "ballot_paths")
            .iter()
            .any(|p| p["kind"] == "chosen" && p["transitive_support"]["verdict"] == "violated")
    );
    assert!(
        array(sender, "ballot_paths")
            .iter()
            .any(|p| p["kind"] == "accepted_quorum"
                && p["transitive_support"]["verdict"] == "established")
    );
    cited(&leaf["transitive_support"], "support", &hops[1..]);
}

#[test]
fn choice_seen_later_does_not_fix_a_claim_and_commit_never_guesses_a_ballot() {
    let b = [1, 0];
    let later = [2, 0];
    let mut sim = covered(vec![
        vec![
            Rule::client(
                "first",
                vec![save(promised(b)), save(acceptance(0, b, "same"))],
            ),
            Rule::client(
                "second",
                vec![save(promised(later)), save(acceptance(0, later, "same"))],
            ),
        ],
        vec![
            Rule::client(
                "first",
                vec![save(promised(b)), save(acceptance(0, b, "same"))],
            ),
            Rule::client(
                "second",
                vec![save(promised(later)), save(acceptance(0, later, "same"))],
            ),
        ],
        vec![],
        vec![Rule::client("early", vec![commit(0, "same")])],
        vec![Rule::client("ambiguous", vec![commit(0, "same")])],
    ]);
    trigger(&mut sim, 3, "early");
    let early = captured("claim-before-choice-prefix", &sim);
    propositions(
        claim(&early, 3, "commit", 0, "same"),
        "violated",
        "violated",
    );
    trigger(&mut sim, 0, "first");
    trigger(&mut sim, 1, "first");
    trigger(&mut sim, 0, "second");
    trigger(&mut sim, 1, "second");
    trigger(&mut sim, 4, "ambiguous");
    sim.finalize("horizon").unwrap();
    let r = captured("late-choice-ambiguous-value-only-commit", &sim);
    let c = claim(&r, 3, "commit", 0, "same");
    propositions(c, "violated", "violated");
    assert_eq!(
        c["local_basis"],
        claim(&early, 3, "commit", 0, "same")["local_basis"]
    );
    assert!(array(c, "choice_at_boundary").is_empty());
    let candidates: BTreeSet<_> = [b, later]
        .iter()
        .map(|b| choice(&r, 0, *b, "same")["id"].as_str().unwrap())
        .collect();
    let observed: BTreeSet<_> = array(c, "choices_seen_later")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(observed, candidates);
    let c = claim(&r, 4, "commit", 0, "same");
    propositions(c, "violated", "violated");
    assert!(c["ballot"].is_null());
    assert!(array(c, "ballot_paths").is_empty());
    assert_eq!(array(c, "choice_at_boundary").len(), 2);
    assert!(array(c, "choices_seen_later").is_empty());
    assert!(c["unsupported_by_tape"].is_object());
    certificate(&r, sim.rows(), 0, b, "same");
    certificate(&r, sim.rows(), 0, later, "same");
}

fn finding_at(r: &Value, first: usize) -> &Value {
    array(r, "interpretation_findings")
        .iter()
        .find(|f| f["first_line"] == first)
        .unwrap_or_else(|| {
            panic!(
                "missing finding at {first}: {}",
                r["interpretation_findings"]
            )
        })
}

#[test]
fn parsing_scope_links_causal_copies_but_not_identical_items_and_keeps_conflicts() {
    let bad = json!({"type":"accepted","slot":0,"ballot":[1,0]});
    let bad_save = json!({"record":"accepted","slot":0,"ballot":[1,0]});
    let mut specs: Vec<_> = (0..5).map(|node| spec(node, 5)).collect();
    specs[2].algorithm = "receiver-script".into();
    specs[2].config = json!({});
    let mut sim = simulator(
        specs,
        vec![
            vec![
                Rule::client(
                    "bad",
                    vec![
                        send(3, bad.clone()),
                        send(3, bad.clone()),
                        save(bad_save.clone()),
                    ],
                ),
                Rule::client(
                    "x",
                    vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))],
                ),
                Rule::client(
                    "y",
                    vec![save(promised([2, 0])), save(acceptance(0, [2, 0], "y"))],
                ),
            ],
            vec![
                Rule::client(
                    "x",
                    vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))],
                ),
                Rule::client(
                    "y",
                    vec![save(promised([2, 0])), save(acceptance(0, [2, 0], "y"))],
                ),
            ],
            vec![Rule::client(
                "bad",
                vec![save(bad_save.clone()), send(3, bad.clone())],
            )],
            vec![Rule::client("after-gap", vec![commit(7, "unknown")])],
            vec![Rule::client("before-gap", vec![commit(8, "absent")])],
        ],
    );
    // Unsupported null is a fixture trigger, not malformed protocol input.
    sim.client_message(NodeId(2), Value::Null).unwrap();
    trigger(&mut sim, 4, "before-gap");
    let fixed = captured("parse-scope-before-gap", &sim);
    propositions(
        claim(&fixed, 4, "commit", 8, "absent"),
        "violated",
        "violated",
    );
    trigger(&mut sim, 0, "bad");
    trigger(&mut sim, 2, "bad");
    sim.client_message(NodeId(3), json!({"type":"propose"}))
        .unwrap();
    sim.run(1).unwrap();
    trigger(&mut sim, 3, "after-gap");
    let no_conflict = captured("parse-gap-without-positive-conflict", &sim);
    let no = obligation(
        &no_conflict,
        "no_conflicting_choices",
        None,
        "indeterminate",
    );
    assert!(!array(no, "gaps").is_empty());
    propositions(
        claim(&no_conflict, 3, "commit", 7, "unknown"),
        "indeterminate",
        "indeterminate",
    );
    trigger(&mut sim, 0, "x");
    trigger(&mut sim, 1, "x");
    trigger(&mut sim, 0, "y");
    trigger(&mut sim, 1, "y");
    sim.finalize("horizon").unwrap();
    let r = captured("parse-scope-causal-dedup-conflict", &sim);
    certificate(&r, sim.rows(), 0, [1, 0], "x");
    certificate(&r, sim.rows(), 0, [2, 0], "y");
    obligation(&r, "no_conflicting_choices", None, "violated");
    assert_eq!(
        claim(&r, 4, "commit", 8, "absent")["local_basis"],
        claim(&fixed, 4, "commit", 8, "absent")["local_basis"]
    );
    let scoped_desires = lines(sim.rows(), |row| {
        is(row, "desire", "send", "message", &bad) && row.fields.get("node") == Some(&json!(0))
    });
    assert_eq!(scoped_desires.len(), 2);
    let mut identities = BTreeSet::new();
    for first in scoped_desires {
        let f = finding_at(&r, first);
        assert!(identities.insert(f["id"].as_str().unwrap()));
        let id = sim.rows()[first - 1].fields["desire_id"].clone();
        let copies = lines(sim.rows(), |row| {
            row.fields.get("desire_id") == Some(&id)
                || row.fields.get("parent_desire_id") == Some(&id)
        });
        assert_eq!(copies.len(), 5);
        cited(f, "occurrence_lines", &copies);
    }
    let unsupported_dispatch = line(sim.rows(), |row| {
        is(row, "dispatch", "message", "message", &bad) && row.fields.get("from") == Some(&json!(2))
    });
    finding_at(&r, unsupported_dispatch);
    let header_save = line(sim.rows(), |row| {
        is(row, "result", "save", "record", &bad_save) && row.fields.get("node") == Some(&json!(2))
    });
    finding_at(&r, header_save);
    let supported_save = line(sim.rows(), |row| {
        is(row, "desire", "save", "record", &bad_save) && row.fields.get("node") == Some(&json!(0))
    });
    finding_at(&r, supported_save);
    let client = line(sim.rows(), |row| {
        is(
            row,
            "dispatch",
            "client",
            "message",
            &json!({"type":"propose"}),
        )
    });
    finding_at(&r, client);
    assert_eq!(array(&r, "interpretation_findings").len(), 6);
    assert!(!array(&r, "interpretation_gap_dependencies").is_empty());
}

#[test]
fn strict_config_failures_are_not_well_formed_configuration_disagreements() {
    let mut controls = Vec::new();
    for (name, key, value) in [
        ("round-string", "initial_round", json!("1")),
        ("round-fraction", "initial_round", json!(1.5)),
        ("round-negative", "initial_round", json!(-1)),
        ("slot-boolean", "from_slot", json!(false)),
        (
            "slot-overflow",
            "from_slot",
            serde_json::from_str::<Value>("18446744073709551616").unwrap(),
        ),
        ("member-negative", "acceptors", json!([-1, 1, 2])),
        (
            "member-overflow",
            "acceptors",
            json!([0, 1, 2147483648_u64]),
        ),
        ("list-not-array", "acceptors", json!({"0":0})),
        ("list-empty", "acceptors", json!([])),
        ("list-duplicate", "acceptors", json!([0, 0, 1])),
        ("initial-state", "initial_state", json!("recovered")),
        ("active-not-proposer", "active_proposer", json!(99)),
    ] {
        let mut c = config(3);
        c[key] = value;
        controls.push((name, c));
    }
    let mut missing = config(3);
    missing.as_object_mut().unwrap().remove("active_proposer");
    controls.push(("missing-nullable", missing));
    let mut unknown = config(3);
    unknown["quorum"] = json!(2);
    controls.push(("unknown-field", unknown));
    let mut local = config(3);
    for role in ["acceptors", "learners", "proposers"] {
        local[role] = json!([1, 2]);
    }
    controls.push(("local-membership", local));
    for (name, c) in controls {
        let mut specs: Vec<_> = (0..3).map(|node| spec(node, 3)).collect();
        specs[0].config = c;
        let mut sim = simulator(specs, vec![vec![], vec![], vec![]]);
        sim.finalize("horizon").unwrap();
        let r = captured(&format!("config-malformed-{name}"), &sim);
        let f = finding_at(&r, 1);
        assert_eq!(f["node"], 0);
        assert!(!array(f, "affected_premises").is_empty());
        assert_eq!(array(&r, "interpretation_findings").len(), 1);
        obligation(&r, "no_conflicting_choices", None, "indeterminate");
    }
    for (name, key, value) in [
        ("acceptor-set", "acceptors", json!([0, 1, 3])),
        ("learner-set", "learners", json!([0, 1, 2])),
        ("proposer-set", "proposers", json!([0, 1, 2])),
        ("suffix", "from_slot", json!(1)),
    ] {
        let mut specs: Vec<_> = (0..4).map(|node| spec(node, 4)).collect();
        specs[0].config[key] = value;
        let mut sim = simulator(specs, vec![vec![], vec![], vec![], vec![]]);
        sim.finalize("horizon").unwrap();
        let r = captured(&format!("config-mismatch-{name}"), &sim);
        assert!(array(&r, "interpretation_findings").is_empty());
        assert!(
            array(&r, "obligations")
                .iter()
                .any(|p| p["category"] == "configuration"
                    && p["verdict"] == "violated"
                    && p["boundary_line"] == 1
                    && array(p, "counterevidence").contains(&json!(1)))
        );
        obligation(&r, "no_conflicting_choices", None, "established");
    }
    let mut specs: Vec<_> = (0..3).map(|node| spec(node, 3)).collect();
    specs[0].config["acceptors"] = json!([2, 0, 1]);
    specs[0].config["active_proposer"] = json!(0);
    specs[1].config["active_proposer"] = json!(1);
    specs[1].config["initial_round"] = json!(7);
    let mut sim = simulator(specs, vec![vec![], vec![], vec![]]);
    sim.finalize("horizon").unwrap();
    let r = captured("config-set-order-and-per-node-settings", &sim);
    assert!(array(&r, "interpretation_findings").is_empty());
    assert!(
        !array(&r, "obligations")
            .iter()
            .any(|p| p["category"] == "configuration" && p["verdict"] == "violated")
    );
}

fn attempt<'a>(r: &'a Value, proposer: i32, slot: u64, ballot: [u64; 2], value: &str) -> &'a Value {
    array(r, "attempts")
        .iter()
        .find(|a| {
            a["proposer"] == proposer
                && a["slot"] == slot
                && a["ballot"] == json!(ballot)
                && a["value"] == value
        })
        .expect("specified proposal attempt")
}

#[test]
fn attempts_without_claims_preserve_missing_replies_and_open_accept_or_reply_work() {
    let b = [1, 3];
    let message = accept(0, b, "unresolved");
    let mut sim = covered(vec![
        vec![Rule::peer(
            3,
            message.clone(),
            vec![
                save(promised(b)),
                save(acceptance(0, b, "unresolved")),
                send(3, accepted(0, b, "unresolved")),
            ],
        )],
        vec![],
        vec![],
        vec![
            Rule::client(
                "unresolved",
                vec![send(0, message.clone()), send(1, message)],
            ),
            Rule::client("never", vec![send(2, accept(1, b, "never"))]),
        ],
    ]);
    trigger(&mut sim, 3, "unresolved");
    trigger(&mut sim, 3, "never");
    let queued = captured("attempt-open-accepts", &sim);
    let a = attempt(&queued, 3, 0, b, "unresolved");
    assert!(a["exact_choice"].is_null());
    assert_eq!(a["resolution"]["state"], "world_open");
    assert_eq!(a["resolution"]["world_open"], true);
    assert!(array(&queued, "claims").is_empty());
    assert_eq!(array(a, "sends").len(), 2);
    assert!(!array(a, "world_work").is_empty());
    sim.run(1).unwrap();
    let reply = captured("attempt-open-reply", &sim);
    let a = attempt(&reply, 3, 0, b, "unresolved");
    let reply_send = line(sim.rows(), |row| {
        is(
            row,
            "result",
            "send",
            "message",
            &accepted(0, b, "unresolved"),
        )
    });
    assert!(array(a, "world_work").iter().any(|work| {
        work["line"] == reply_send
            || work["kind"]
                .as_str()
                .is_some_and(|kind| kind.contains("reply"))
    }));
    assert_eq!(a["resolution"]["world_open"], true);
    sim.run(2).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("attempt-no-quorum-no-claim", &sim);
    let a = attempt(&r, 3, 0, b, "unresolved");
    assert!(a["exact_choice"].is_null());
    assert_eq!(a["resolution"]["state"], "unresolved");
    assert_eq!(a["resolution"]["world_open"], false);
    assert_eq!(a["resolution"]["whole_world_empty"], true);
    assert!(array(a, "world_work").is_empty());
    assert!(array(&r, "choices").is_empty());
    assert!(array(&r, "claims").is_empty());
    let none = attempt(&r, 3, 1, b, "never");
    assert_eq!(none["resolution"]["state"], "unresolved");
    assert_eq!(none["resolution"]["whole_world_empty"], true);
    assert!(none["exact_choice"].is_null());
    assert!(
        array(none, "replies")
            .iter()
            .all(|entry| entry["no_reply_observed"] == true && array(entry, "received").is_empty())
    );
    assert_eq!(array(a, "replies").len(), 3);
    let silent = array(a, "replies")
        .iter()
        .find(|entry| entry["acceptor"] == 1)
        .unwrap();
    assert_eq!(silent["no_reply_observed"], true);
    assert!(array(silent, "received").is_empty());
    let one = array(a, "replies")
        .iter()
        .find(|entry| entry["acceptor"] == 0)
        .unwrap();
    assert_eq!(one["no_reply_observed"], false);
    assert_eq!(array(one, "received")[0]["classification"], "matching");
    let received = line(sim.rows(), |row| {
        is(
            row,
            "dispatch",
            "message",
            "message",
            &accepted(0, b, "unresolved"),
        )
    });
    assert_eq!(array(one, "received")[0]["line"], received);
    let invocation = report(
        "attempt-attributable-open-reply-invocation",
        &tape::encode(&sim.rows()[..received]),
    );
    let open = attempt(&invocation, 3, 0, b, "unresolved");
    assert_eq!(open["resolution"]["world_open"], true);
    assert!(
        array(open, "world_work")
            .iter()
            .any(|work| work["line"] == received)
    );
}

#[test]
fn nack_ancestry_is_accept_prepare_or_unattributed_never_a_ballot_guess() {
    let b = [1, 3];
    let reject = nack(b, [2, 0]);
    let mut sim = covered(vec![
        vec![Rule::peer(
            3,
            accept(0, b, "nacked"),
            vec![save(promised([2, 0])), send(3, reject.clone())],
        )],
        vec![Rule::peer(
            3,
            prepare(b),
            vec![save(promised([2, 0])), send(3, reject.clone())],
        )],
        vec![Rule::client(
            "script-trigger",
            vec![send(3, reject.clone())],
        )],
        vec![Rule::client(
            "nacked",
            vec![send(0, accept(0, b, "nacked")), send(1, prepare(b))],
        )],
    ]);
    trigger(&mut sim, 3, "nacked");
    trigger(&mut sim, 2, "script-trigger");
    sim.run(2).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("nack-three-ancestries", &sim);
    let a = attempt(&r, 3, 0, b, "nacked");
    let nacks: Vec<_> = array(&r, "observations")
        .iter()
        .filter(|o| o["kind"] == "nack")
        .collect();
    assert_eq!(nacks.len(), 3);
    for (from, ancestry) in [(0, "accept"), (1, "prepare"), (2, "unattributed")] {
        let o = nacks.iter().find(|o| o["from"] == from).unwrap();
        assert_eq!(o["node"], 3);
        assert_eq!(o["ancestry"], ancestry);
        assert_eq!(o["rejected_ballot"], json!(b));
        assert_eq!(o["promised_ballot"], json!([2, 0]));
        let receipt = line(sim.rows(), |row| {
            is(row, "dispatch", "message", "message", &reject)
                && row.fields.get("from") == Some(&json!(from))
        });
        assert_eq!(o["line"], receipt);
        if from == 0 {
            assert_eq!(o["attempt_id"], a["id"]);
        } else {
            assert!(o["attempt_id"].is_null());
        }
        if from == 1 {
            assert_eq!(o["campaign_ballot"], json!(b));
            let request = line(sim.rows(), |row| {
                is(row, "dispatch", "message", "message", &prepare(b))
            });
            assert_eq!(o["request_dispatch_line"], request);
        }
        if from == 2 {
            assert!(o["request_dispatch_line"].is_null());
        }
    }
    let received: Vec<_> = array(a, "replies")
        .iter()
        .flat_map(|entry| array(entry, "received"))
        .collect();
    assert_eq!(
        received
            .iter()
            .filter(|entry| entry["kind"] == "nack")
            .count(),
        1
    );
    assert_eq!(a["resolution"]["state"], "unresolved");
    assert_eq!(a["resolution"]["whole_world_empty"], true);
}

#[test]
fn invocation_allocation_and_effect_prefixes_are_not_refused_stranded_desires() {
    let b = [1, 0];
    let message = accept(1, b, "tail");
    let mut sim = covered(vec![
        vec![
            Rule::client("first", vec![commit(0, "first")]),
            Rule::client(
                "tail",
                vec![
                    send(1, accept(2, b, "head")),
                    commit(0, "second"),
                    send(1, message.clone()),
                    send(3, chosen(1, b, "tail")),
                ],
            ),
        ],
        vec![],
        vec![],
        vec![],
    ]);
    trigger(&mut sim, 0, "first");
    assert!(
        sim.client_message(NodeId(0), json!({"type":"propose","value":"tail"}))
            .is_err()
    );
    let rows = sim.rows();
    let input = line(rows, |row| {
        is(
            row,
            "dispatch",
            "client",
            "message",
            &json!({"type":"propose","value":"tail"}),
        )
    });
    let first_desire = line(rows, |row| {
        is(row, "desire", "send", "message", &accept(2, b, "head"))
    });
    let close = line(rows, |row| {
        row.what == "close" && row.seq >= (first_desire as u64 - 1)
    });
    let first_attempt = line(rows, |row| {
        is(row, "attempt", "send", "message", &accept(2, b, "head"))
    });
    let refusal = line(rows, |row| {
        is(row, "result", "commit", "entry", &json!("second"))
    });
    for (name, end, tail, has_attempt) in [
        ("empty", 0, "empty", false),
        ("invocation", input, "open_invocation", false),
        ("partial-answer", first_desire, "partial_answer", true),
        ("closed-unreached", close, "interrupted_drain", true),
        ("open-effect", first_attempt, "open_effect", true),
    ] {
        let r = report(&format!("prefix-{name}"), &tape::encode(&rows[..end]));
        assert_eq!(r["world"]["tail"], tail);
        if !has_attempt {
            assert!(array(&r, "attempts").is_empty());
            continue;
        }
        let a = attempt(&r, 0, 2, b, "head");
        assert_eq!(a["resolution"]["world_open"], true);
        assert!(!array(a, "world_work").is_empty());
        if end < close {
            assert!(a["origin"]["allocation_line"].is_null());
            assert!(a["origin"]["client_count"].is_null());
        } else {
            assert_eq!(a["origin"]["allocation_line"], close);
            let assertion = claim(&r, 0, "chosen", 1, "tail");
            assert!(assertion["effect"]["attempt_line"].is_null());
            assert!(assertion["local_basis"].is_null());
            assert!(assertion["transitive_support"].is_null());
            assert_eq!(assertion["effect"]["status"]["world_open"], true);
            assert!(assertion["effect"]["status"]["blocking_refusal_line"].is_null());
        }
        if end == first_attempt {
            assert_eq!(
                obligation(&r, "phase2_gate", Some(0), "violated")["boundary_line"],
                first_attempt
            );
        } else {
            assert!(
                !array(&r, "obligations")
                    .iter()
                    .any(|p| p["proposition"] == "phase2_gate"
                        && p["subject"]["node"] == 0
                        && p["subject"]["slot"] == 2)
            );
        }
    }
    sim.finalize("refused").unwrap();
    let r = captured("prefix-refused-stranded-finalized", &sim);
    let c = claim(&r, 0, "commit", 0, "second");
    assert_eq!(c["effect"]["outcome"], "refused");
    propositions(c, "violated", "violated");
    let stranded = attempt(&r, 0, 1, b, "tail");
    assert_eq!(stranded["origin"]["allocation_line"], close);
    assert_eq!(
        array(stranded, "sends")[0]["status"]["state"],
        "unreached/refused"
    );
    assert_eq!(array(stranded, "sends")[0]["status"]["world_open"], false);
    assert_eq!(
        array(stranded, "sends")[0]["status"]["blocking_refusal_line"],
        refusal
    );
    assert!(array(stranded, "world_work").is_empty());
    assert_eq!(stranded["resolution"]["state"], "unresolved");
    let unadmitted = claim(&r, 0, "chosen", 1, "tail");
    assert!(unadmitted["effect"]["attempt_line"].is_null());
    assert!(unadmitted["local_basis"].is_null());
    assert!(unadmitted["transitive_support"].is_null());
    assert_eq!(unadmitted["effect"]["status"]["state"], "unreached/refused");
}

#[test]
fn quorum_gap_makes_every_later_value_count_cumulatively_uncertain() {
    let b = [1, 3];
    let malformed = json!({"type":"promise","ballot":b,"from_slot":0,"accepted":"unreadable"});
    let good = promise(b, json!([]));
    let mut specs: Vec<_> = (0..4).map(|node| spec(node, 4)).collect();
    specs[3].config["active_proposer"] = json!(3);
    let mut sim = simulator_with_ticks(
        specs,
        vec![
            vec![
                Rule::client("bad-report", vec![send(3, malformed.clone())]),
                Rule::client("good-report", vec![send(3, good.clone())]),
            ],
            vec![Rule::client("good-report", vec![send(3, good.clone())])],
            vec![],
            vec![
                Rule::tick(vec![
                    send(0, prepare(b)),
                    send(1, prepare(b)),
                    send(2, prepare(b)),
                ]),
                Rule::client(
                    "a",
                    vec![send(0, accept(0, b, "a")), send(1, accept(0, b, "a"))],
                ),
                Rule::client("b", vec![send(0, accept(1, b, "b"))]),
                Rule::client("a-again", vec![send(0, accept(2, b, "a"))]),
                Rule::client(
                    "no-client-allowance",
                    vec![send(0, accept(3, b, "invented"))],
                ),
            ],
        ],
        1,
    );
    sim.run(1).unwrap();
    trigger(&mut sim, 0, "bad-report");
    sim.run(2).unwrap();
    trigger(&mut sim, 3, "a");
    let first = captured("quorum-gap-first-uncertain-allocation", &sim);
    let gap_line = line(sim.rows(), |row| {
        is(row, "dispatch", "message", "message", &malformed)
    });
    let gap = finding_at(
        &first,
        line(sim.rows(), |row| {
            is(row, "desire", "send", "message", &malformed)
        }),
    )["id"]
        .clone();
    let a = attempt(&first, 3, 0, b, "a");
    assert_eq!(a["origin"]["kind"], "undetermined");
    assert!(array(&a["origin"], "gaps").contains(&gap));
    assert!(!a["origin"]["allocation_line"].is_null());
    // A later readable quorum cannot retroactively recover the first quorum
    // or whether its earlier allocation consumed client allowance.
    trigger(&mut sim, 0, "good-report");
    trigger(&mut sim, 1, "good-report");
    sim.run(3).unwrap();
    trigger(&mut sim, 3, "b");
    trigger(&mut sim, 3, "a-again");
    trigger(&mut sim, 3, "no-client-allowance");
    sim.run(4).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("quorum-gap-cumulative-all-values", &sim);
    for (slot, value) in [(0, "a"), (1, "b"), (2, "a"), (3, "invented")] {
        let a = attempt(&r, 3, slot, b, value);
        assert_eq!(a["origin"]["kind"], "undetermined");
        assert!(array(&a["origin"], "gaps").contains(&gap));
        assert_eq!(a["origin"]["client_count"]["certain"], false);
        let p = array(&r, "obligations")
            .iter()
            .find(|p| {
                p["proposition"] == "fresh_origin_count"
                    && p["subject"]["node"] == 3
                    && p["subject"]["slot"] == slot
            })
            .expect("per-value count proposition");
        assert_eq!(p["verdict"], "indeterminate");
        assert!(array(p, "gaps").contains(&gap));
        assert!(p["boundary_line"].as_u64().unwrap() > gap_line as u64);
    }
    assert_eq!(
        attempt(&r, 3, 0, b, "a")["origin"],
        attempt(&first, 3, 0, b, "a")["origin"]
    );
}

#[test]
fn malformed_client_limits_counts_but_does_not_erase_known_fresh_origin_or_positive_knowledge() {
    let b = [1, 3];
    let mut sim = covered(vec![
        vec![Rule::peer(
            3,
            accept(0, b, "x"),
            vec![
                save(promised(b)),
                save(acceptance(0, b, "x")),
                send(3, accepted(0, b, "x")),
            ],
        )],
        vec![Rule::peer(
            3,
            accept(0, b, "x"),
            vec![
                save(promised(b)),
                save(acceptance(0, b, "x")),
                send(3, accepted(0, b, "x")),
            ],
        )],
        vec![],
        vec![
            Rule::client(
                "emit",
                vec![send(0, accept(0, b, "x")), send(1, accept(0, b, "x"))],
            ),
            Rule::peer(1, accepted(0, b, "x"), vec![commit(0, "x")]),
            Rule::client("later", vec![send(0, accept(1, b, "y"))]),
        ],
    ]);
    sim.client_message(NodeId(3), json!({"type":"propose"}))
        .unwrap();
    trigger(&mut sim, 3, "emit");
    sim.run(2).unwrap();
    trigger(&mut sim, 3, "later");
    sim.run(3).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("client-gap-origin-count-not-knowledge", &sim);
    certificate(&r, sim.rows(), 0, b, "x");
    propositions(claim(&r, 3, "commit", 0, "x"), "established", "established");
    let gap_line = line(sim.rows(), |row| {
        is(
            row,
            "dispatch",
            "client",
            "message",
            &json!({"type":"propose"}),
        )
    });
    let gap = finding_at(&r, gap_line)["id"].clone();
    for (slot, value) in [(0, "x"), (1, "y")] {
        let a = attempt(&r, 3, slot, b, value);
        assert_eq!(a["origin"]["kind"], "fresh");
        assert_eq!(a["origin"]["client_count"]["certain"], false);
        let p = array(&r, "obligations")
            .iter()
            .find(|p| {
                p["proposition"] == "fresh_origin_count"
                    && p["subject"]["node"] == 3
                    && p["subject"]["slot"] == slot
            })
            .unwrap();
        assert_eq!(p["verdict"], "indeterminate");
        assert!(array(p, "gaps").contains(&gap));
    }
    // Client payload uncertainty is not Phase-1 uncertainty.
    obligation(&r, "phase2_gate", Some(3), "violated");
}

#[test]
fn malformed_world_suffix_preserves_the_original_hash_and_last_valid_prefix() {
    let mut sim = covered(vec![
        vec![Rule::client(
            "x",
            vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))],
        )],
        vec![Rule::client(
            "x",
            vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))],
        )],
        vec![],
    ]);
    trigger(&mut sim, 0, "x");
    trigger(&mut sim, 1, "x");
    let prefix = tape::encode(sim.rows());
    let mut malformed = prefix.clone();
    malformed.extend_from_slice(b"{\"not\":\"a world row\"}\n");
    let r = report("malformed-world-after-certificate", &malformed);
    assert_eq!(r["world"]["valid_rows"], sim.rows().len());
    assert_eq!(r["world"]["total_rows"], sim.rows().len() + 1);
    assert_eq!(r["world"]["valid_prefix_bytes"], prefix.len());
    assert_eq!(r["world"]["error"]["line"], sim.rows().len() + 1);
    assert_ne!(r["source_sha256"], tape::hash(&prefix));
    certificate(&r, sim.rows(), 0, [1, 0], "x");
    assert!(array(&r, "interpretation_findings").is_empty());
    let no_header = report(
        "malformed-world-before-header",
        b"{\"not\":\"a world row\"}\n",
    );
    assert_eq!(no_header["world"]["valid_rows"], 0);
    assert_eq!(no_header["world"]["valid_prefix_bytes"], 0);
    assert_eq!(no_header["world"]["error"]["line"], 1);
    for section in ["choices", "obligations", "claims", "attempts"] {
        assert!(
            array(&no_header, section).is_empty(),
            "No world identities exist before a valid header"
        );
    }
}

#[test]
fn malformed_config_keeps_its_actor_in_output_and_acceptance_scope() {
    let mut specs: Vec<_> = (0..3).map(|node| spec(node, 3)).collect();
    specs[0]
        .config
        .as_object_mut()
        .unwrap()
        .remove("active_proposer");
    let bad = json!({"type":"chosen","slot":0,"ballot":[1,0]});
    let mut sim = simulator(
        specs,
        vec![
            vec![Rule::client(
                "x",
                vec![
                    save(promised([1, 0])),
                    save(acceptance(0, [1, 0], "x")),
                    send(1, bad.clone()),
                ],
            )],
            vec![Rule::client(
                "x",
                vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))],
            )],
            vec![],
        ],
    );
    trigger(&mut sim, 0, "x");
    trigger(&mut sim, 1, "x");
    sim.run(1).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("malformed-config-retains-parsing-scope", &sim);
    assert_eq!(finding_at(&r, 1)["node"], 0);
    let desire = line(sim.rows(), |row| is(row, "desire", "send", "message", &bad));
    finding_at(&r, desire);
    assert_eq!(array(&r, "interpretation_findings").len(), 2);
    certificate(&r, sim.rows(), 0, [1, 0], "x");
    obligation(&r, "no_conflicting_choices", None, "indeterminate");
}

#[test]
fn unreadable_save_limits_a_missing_join_but_later_positive_support_has_its_own_boundary() {
    let b = [1, 3];
    let bad = json!({"record":"accepted","slot":0,"ballot":b});
    let mut sim = covered(vec![
        vec![
            Rule::client(
                "early",
                vec![save(bad.clone()), send(3, accepted(0, b, "x"))],
            ),
            Rule::client(
                "late",
                vec![
                    save(promised(b)),
                    save(acceptance(0, b, "x")),
                    send(4, accepted(0, b, "x")),
                ],
            ),
        ],
        vec![
            Rule::client(
                "early",
                vec![
                    save(promised(b)),
                    save(acceptance(0, b, "x")),
                    send(3, accepted(0, b, "x")),
                ],
            ),
            Rule::client("late", vec![send(4, accepted(0, b, "x"))]),
        ],
        vec![],
        vec![Rule::peer(1, accepted(0, b, "x"), vec![commit(0, "x")])],
        vec![Rule::peer(1, accepted(0, b, "x"), vec![commit(0, "x")])],
    ]);
    trigger(&mut sim, 0, "early");
    trigger(&mut sim, 1, "early");
    sim.run(1).unwrap();
    let early = captured("save-gap-missing-durable-join", &sim);
    let c = claim(&early, 3, "commit", 0, "x");
    propositions(c, "established", "indeterminate");
    let first = line(sim.rows(), |row| is(row, "desire", "save", "record", &bad));
    let gap = finding_at(&early, first)["id"].clone();
    assert!(array(&c["transitive_support"], "gaps").contains(&gap));
    let early_ack = line(sim.rows(), |row| {
        is(row, "attempt", "send", "message", &accepted(0, b, "x"))
            && row.fields.get("node") == Some(&json!(0))
    });
    trigger(&mut sim, 0, "late");
    trigger(&mut sim, 1, "late");
    sim.run(2).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("save-gap-later-positive-support", &sim);
    certificate(&r, sim.rows(), 0, b, "x");
    assert_eq!(
        claim(&r, 3, "commit", 0, "x")["transitive_support"],
        c["transitive_support"]
    );
    let later = claim(&r, 4, "commit", 0, "x");
    propositions(later, "established", "established");
    let durable = line(sim.rows(), |row| {
        is(row, "result", "save", "record", &acceptance(0, b, "x"))
            && row.fields.get("node") == Some(&json!(0))
    });
    assert!(durable > early_ack);
    cited(&later["transitive_support"], "support", &[durable]);
    // A positive chain does not certify that no other value was chosen in the
    // unreadable durable history.
    obligation(&r, "no_conflicting_choices", None, "indeterminate");
}

#[test]
fn misdirected_nack_preserves_ancestry_without_becoming_a_received_reply() {
    let ballot = [1, 3];
    let message = accept(0, ballot, "misdirected");
    let mut sim = covered(vec![
        vec![Rule::peer(
            3,
            message.clone(),
            vec![save(promised([2, 3])), send(1, nack(ballot, [2, 3]))],
        )],
        vec![],
        vec![],
        vec![Rule::client("misdirected", vec![send(0, message)])],
    ]);
    trigger(&mut sim, 3, "misdirected");
    sim.run(2).unwrap();
    sim.finalize("horizon").unwrap();
    let r = captured("nack-misdirected-recipient", &sim);
    let a = attempt(&r, 3, 0, ballot, "misdirected");
    let member = array(a, "replies")
        .iter()
        .find(|reply| reply["acceptor"] == 0)
        .unwrap();
    assert_eq!(member["no_reply_observed"], true);
    assert!(array(member, "received").is_empty());
    let misdirected = array(a, "other_replies")
        .iter()
        .find(|reply| reply["kind"] == "nack")
        .unwrap();
    assert_eq!(misdirected["from"], 0);
    assert_eq!(misdirected["to"], 1);
    assert_eq!(misdirected["ancestry"], "accept");
    assert_eq!(misdirected["attempt_id"], a["id"]);
    assert_eq!(a["resolution"]["state"], "unresolved");
    assert_eq!(a["resolution"]["world_open"], false);
}

fn unexplained_history(unrelated_failure: bool, stale_second: bool, uncovered: bool) -> Simulator {
    let first = vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))];
    let second = if stale_second {
        vec![save(acceptance(0, [2, 0], "y"))]
    } else {
        vec![save(promised([2, 0])), save(acceptance(0, [2, 0], "y"))]
    };
    let mut specs: Vec<_> = (0..3).map(|node| spec(node, 3)).collect();
    if uncovered {
        specs[1].algorithm = "receiver-script".into();
        specs[1].config = json!({});
    }
    let mut sim = simulator(
        specs,
        vec![
            vec![
                Rule::client("unrelated", vec![save(acceptance(9, [7, 0], "elsewhere"))]),
                Rule::client("x", first.clone()),
                Rule::client("y", second.clone()),
                Rule::client("later", vec![save(acceptance(0, [3, 0], "later"))]),
            ],
            vec![Rule::client("x", first), Rule::client("y", second)],
            vec![],
        ],
    );
    if unrelated_failure {
        trigger(&mut sim, 0, "unrelated");
    }
    for value in ["x", "y"] {
        trigger(&mut sim, 0, value);
        trigger(&mut sim, 1, value);
    }
    sim
}

#[test]
fn conflict_self_check_exposes_missing_rules_and_ignores_unrelated_failures() {
    for unrelated in [false, true] {
        let sim = unexplained_history(unrelated, false, false);
        let r = captured(
            if unrelated {
                "self-check-unrelated-validity-failure"
            } else {
                "self-check-unexplained-conflict"
            },
            &sim,
        );
        let check = obligation(&r, "conflict_explanation", None, "violated");
        assert_eq!(check["subject"]["target"], "account");
        assert_eq!(check["subject"]["slot"], 0);
        assert_eq!(
            check["boundary_line"],
            choice(&r, 0, [2, 0], "y")["first_quorum_line"]
        );
        let finding = array(&r, "observations")
            .iter()
            .find(|o| o["kind"] == "unexplained_conflicting_choices")
            .unwrap();
        assert_eq!(finding["fully_covered"], true);
        assert_eq!(finding["account_deficiency"], true);
        assert!(array(finding, "explanatory_obligations").is_empty());
        if unrelated {
            assert!(
                array(&r, "obligations")
                    .iter()
                    .any(|p| p["proposition"] == "accepted_watermark"
                        && p["subject"]["slot"] == 9
                        && p["verdict"] == "violated")
            );
        }
    }
}

#[test]
fn conflict_self_check_links_relevant_failures_and_retains_coverage_limits() {
    let explained = captured(
        "self-check-explained-watermark-conflict",
        &unexplained_history(true, true, false),
    );
    obligation(&explained, "conflict_explanation", None, "established");
    let finding = array(&explained, "observations")
        .iter()
        .find(|o| o["kind"] == "conflict_explanation")
        .unwrap();
    let expected: BTreeSet<_> = array(&explained, "obligations")
        .iter()
        .filter(|p| {
            p["proposition"] == "accepted_watermark"
                && p["subject"]["slot"] == 0
                && p["verdict"] == "violated"
        })
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    let actual: BTreeSet<_> = array(finding, "explanatory_obligations")
        .iter()
        .map(|id| id.as_str().unwrap())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 2);
    let limited = captured(
        "self-check-undeclared-history",
        &unexplained_history(false, false, true),
    );
    obligation(&limited, "conflict_explanation", None, "indeterminate");
    assert!(
        !array(&limited, "observations")
            .iter()
            .any(|o| o["kind"] == "unexplained_conflicting_choices")
    );
    let coverage = array(&limited, "observations")
        .iter()
        .find(|o| o["kind"] == "conflict_explanation")
        .unwrap();
    assert_eq!(coverage["fully_covered"], false);
    assert_eq!(coverage["undeclared_contracts"], json!([1]));
}

#[test]
fn later_failure_cannot_explain_an_earlier_conflicting_quorum_boundary() {
    let mut sim = unexplained_history(false, false, false);
    let prefix = captured("self-check-before-later-failure", &sim);
    let original = obligation(&prefix, "conflict_explanation", None, "violated");
    trigger(&mut sim, 0, "later");
    let later = captured("self-check-after-later-failure", &sim);
    assert_eq!(
        obligation(&later, "conflict_explanation", None, "violated"),
        original
    );
}

#[test]
fn attempt_choice_relations_separate_exact_same_value_and_competing_values() {
    let mut sim = covered(vec![
        vec![
            Rule::client(
                "x",
                vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))],
            ),
            Rule::client(
                "x-again",
                vec![save(promised([2, 0])), save(acceptance(0, [2, 0], "x"))],
            ),
            Rule::client(
                "y",
                vec![save(promised([3, 0])), save(acceptance(0, [3, 0], "y"))],
            ),
        ],
        vec![
            Rule::client(
                "x",
                vec![save(promised([1, 0])), save(acceptance(0, [1, 0], "x"))],
            ),
            Rule::client(
                "x-again",
                vec![save(promised([2, 0])), save(acceptance(0, [2, 0], "x"))],
            ),
            Rule::client(
                "y",
                vec![save(promised([3, 0])), save(acceptance(0, [3, 0], "y"))],
            ),
        ],
        vec![],
        vec![Rule::client("x", vec![send(0, accept(0, [1, 0], "x"))])],
    ]);
    trigger(&mut sim, 3, "x");
    for value in ["x", "x-again", "y"] {
        trigger(&mut sim, 0, value);
        trigger(&mut sim, 1, value);
    }
    let r = captured("attempt-distinct-choice-relations", &sim);
    let a = attempt(&r, 3, 0, [1, 0], "x");
    assert_eq!(a["exact_choice"], choice(&r, 0, [1, 0], "x")["id"]);
    assert_eq!(
        a["same_value_elsewhere"],
        json!([choice(&r, 0, [2, 0], "x")["id"]])
    );
    assert_eq!(
        a["slot_witnessed_values"],
        json!([choice(&r, 0, [3, 0], "y")["id"]])
    );
}
