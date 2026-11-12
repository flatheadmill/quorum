use quorum::multi_paxos::propose;
use quorum::paxos_account::examine;
use quorum::tape::{self, Event, Header, NodeSpec, WorldConfig};
use quorum::{Context, Desire, Input, Node, NodeId, Simulator, Slot, scenarios};
use serde_json::{Value, json};
use std::{collections::BTreeSet, collections::VecDeque, fs, io, path::Path};

fn spec(
    node: i32,
    acceptors: &[i32],
    learners: &[i32],
    proposers: &[i32],
    active: Option<i32>,
    round: u64,
) -> NodeSpec {
    NodeSpec {
        node: NodeId(node),
        algorithm: "multi-paxos".into(),
        config: json!({
            "acceptors": acceptors, "learners": learners, "proposers": proposers,
            "active_proposer": active, "from_slot": 0, "initial_round": round,
            "initial_state": "empty",
        }),
    }
}

fn simulator(name: &str, nodes: Vec<NodeSpec>, acceptors: &[i32]) -> Simulator {
    Simulator::new(
        Header::new(
            name,
            42,
            WorldConfig {
                base_latency: 1,
                jitter: 0,
                tick_interval: 1,
                deadlock_threshold: 200,
            },
            nodes,
            acceptors.iter().copied().map(NodeId).collect(),
        ),
        Box::new(io::sink()),
    )
    .unwrap()
}

fn capture(name: &str, rows: &[Event]) -> Value {
    let bytes = tape::encode(rows);
    let report = examine(&bytes);
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/paxos-account-evidence");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join(format!("{name}.jsonl")), &bytes).unwrap();
    fs::write(
        directory.join(format!("{name}.report.json")),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["source_sha256"], tape::hash(&bytes));
    assert!(report["world"]["error"].is_null(), "{report}");
    assert_eq!(report["world"]["valid_rows"], rows.len());
    report
}

fn lines(rows: &[Event], predicate: impl Fn(&Event) -> bool) -> Vec<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(index, row)| predicate(row).then_some(index + 1))
        .collect()
}

fn close_line(rows: &[Event], dispatch: usize) -> usize {
    let input = &rows[dispatch - 1].fields["input_id"];
    lines(rows, |row| {
        row.what == "close" && row.fields["input_id"] == *input
    })[0]
}

fn answer(rows: &[Event], dispatch: usize) -> Vec<Desire> {
    let input = &rows[dispatch - 1].fields["input_id"];
    rows.iter()
        .filter(|row| row.what == "desire" && row.fields["parent_input_id"] == *input)
        .map(|row| row.desire().unwrap())
        .collect()
}

fn receiver(report: &Value, boundary: usize) -> &Value {
    let proposition = report["obligations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["proposition"] == "receiver_conformance" && p["boundary_line"] == boundary)
        .unwrap_or_else(|| panic!("missing receiver proposition at {boundary}: {report}"));
    assert_eq!(proposition["verdict"], "established", "{proposition}");
    assert!(
        proposition["counterevidence"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(proposition["gaps"].as_array().unwrap().is_empty());
    proposition
}

fn cited(proposition: &Value, expected: &[usize]) {
    let support = proposition["support"].as_array().unwrap();
    for line in expected {
        assert!(
            support.contains(&json!(line)),
            "missing {line}: {proposition}"
        );
    }
}

fn message(kind: &str, ballot: [u64; 2], slot: u64, value: &str) -> Value {
    json!({"type":kind,"ballot":ballot,"slot":slot,"value":value})
}

#[test]
fn learning_before_own_quorum_commits_once_and_announces_each_proposal() {
    let acceptors = [0, 1, 2];
    let nodes: Vec<_> = (0..4)
        .map(|node| {
            spec(
                node,
                &acceptors,
                &[0, 1, 2, 3],
                &[0, 3],
                match node {
                    0 => Some(0),
                    3 => Some(3),
                    _ => None,
                },
                if node == 3 { 2 } else { 1 },
            )
        })
        .collect();
    let mut sim = simulator(
        "learning-learned-before-own-quorum",
        nodes.clone(),
        &acceptors,
    );
    for node in &nodes[..3] {
        sim.spawn(node.node, scenarios::factory(node).unwrap())
            .unwrap();
    }
    sim.client_message(NodeId(0), propose(json!("x"))).unwrap();
    sim.run(5).unwrap();
    sim.spawn(nodes[3].node, scenarios::factory(&nodes[3]).unwrap())
        .unwrap();
    sim.client_message(NodeId(3), propose(json!("y"))).unwrap();
    sim.run(16).unwrap();
    sim.finalize("horizon").unwrap();

    let rows = sim.rows();
    let report = capture("learning-learned-before-own-quorum", rows);
    let prior_chosen = lines(rows, |row| {
        row.what == "dispatch"
            && row.why == "message"
            && row.fields["to"] == 3
            && row.fields["message"] == message("chosen", [1, 0], 0, "x")
    });
    assert_eq!(prior_chosen.len(), 1);
    let prior_close = close_line(rows, prior_chosen[0]);
    assert_eq!(
        answer(rows, prior_chosen[0]),
        vec![Desire::Commit {
            slot: Slot(0),
            entry: json!("x")
        }]
    );
    cited(
        receiver(&report, prior_close),
        &[prior_chosen[0], prior_close],
    );

    let replies = lines(rows, |row| {
        row.what == "dispatch"
            && row.why == "message"
            && row.fields["to"] == 3
            && row.fields["message"] == message("accepted", [2, 3], 0, "x")
    });
    assert_eq!(replies.len(), 3);
    let quorum_close = close_line(rows, replies[1]);
    assert!(prior_close < replies[0] && replies[0] < replies[1]);
    assert!(answer(rows, replies[0]).is_empty());
    assert_eq!(
        answer(rows, replies[1]),
        (0..4)
            .map(|node| Desire::Send {
                to: NodeId(node),
                message: message("chosen", [2, 3], 0, "x"),
            })
            .collect::<Vec<_>>()
    );
    assert!(answer(rows, replies[2]).is_empty());

    let commits = lines(rows, |row| {
        row.what == "desire"
            && row.why == "commit"
            && row.fields["node"] == 3
            && row.fields["slot"] == 0
    });
    assert_eq!(commits.len(), 1);
    assert!(prior_chosen[0] < commits[0] && commits[0] < prior_close);
    cited(
        receiver(&report, quorum_close),
        &[commits[0], replies[0], replies[1], quorum_close],
    );
    cited(
        receiver(&report, close_line(rows, replies[2])),
        &[quorum_close, replies[2]],
    );
    receiver(&report, close_line(rows, replies[0]));

    let announcements = lines(rows, |row| {
        row.what == "desire"
            && row.why == "send"
            && row.fields["node"] == 3
            && row.fields["message"] == message("chosen", [2, 3], 0, "x")
    });
    assert_eq!(announcements.len(), 4);
    let learners: BTreeSet<_> = announcements
        .iter()
        .map(|line| rows[line - 1].fields["to"].as_i64().unwrap())
        .collect();
    assert_eq!(learners, BTreeSet::from([0, 1, 2, 3]));

    let claims = report["claims"].as_array().unwrap();
    assert!(!claims.is_empty());
    for claim in claims {
        assert_eq!(claim["local_basis"]["verdict"], "established", "{claim}");
        assert_eq!(
            claim["transitive_support"]["verdict"], "established",
            "{claim}"
        );
    }
    let own_claims: Vec<_> = claims
        .iter()
        .filter(|claim| claim["node"] == 3 && claim["slot"] == 0 && claim["value"] == "x")
        .collect();
    assert_eq!(
        own_claims
            .iter()
            .filter(|claim| claim["kind"] == "commit")
            .count(),
        1
    );
    assert_eq!(
        own_claims
            .iter()
            .filter(|claim| claim["kind"] == "chosen")
            .count(),
        4
    );
    let first_commit = own_claims
        .iter()
        .find(|claim| claim["kind"] == "commit")
        .unwrap();
    cited(&first_commit["local_basis"], &[prior_chosen[0]]);
    for claim in own_claims.iter().filter(|claim| claim["kind"] == "chosen") {
        assert!(
            claim["ballot_paths"]
                .as_array()
                .unwrap()
                .iter()
                .any(|path| {
                    path["kind"] == "accepted_quorum"
                        && path["ballot"] == json!([2, 3])
                        && path["local_basis"]["verdict"] == "established"
                        && path["transitive_support"]["verdict"] == "established"
                }),
            "{claim}"
        );
    }
    for ballot in [[1, 0], [2, 3]] {
        assert!(report["choices"].as_array().unwrap().iter().any(|choice| {
            choice["slot"] == 0 && choice["value"] == "x" && choice["ballot"] == json!(ballot)
        }));
    }
    assert!(!report["obligations"].as_array().unwrap().iter().any(|p| {
        p["subject"]["node"] == 3
            && (p["proposition"] == "learning_dedup" || p["proposition"] == "learning_consistency")
    }));
}

// These peers deliberately repeat exact positive testimony. Only node 0 is a
// claimed inhabitant; the account must not confuse receiver conformance with
// durable/transitive evidence unavailable from these undeclared acceptors.
struct Peer {
    batches: VecDeque<Vec<Value>>,
    crashed: bool,
}

impl Node for Peer {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        if matches!(input, Input::Client { .. }) {
            for reply in self.batches.pop_front().expect("peer batch") {
                ctx.send(NodeId(0), reply);
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

#[test]
fn duplicate_accepted_replies_neither_supply_quorum_nor_repeat_announcements() {
    let accepted = message("accepted", [2, 0], 0, "x");
    let promise = json!({"type":"promise","ballot":[2,0],"from_slot":0,"accepted":[]});
    let programs = [
        vec![
            vec![promise.clone()],
            vec![accepted.clone(), accepted.clone()],
            vec![accepted.clone()],
        ],
        vec![
            vec![promise],
            vec![accepted.clone()],
            vec![accepted.clone()],
        ],
        vec![vec![accepted.clone()]],
    ];
    let mut nodes = vec![spec(0, &[1, 2, 3], &[0], &[0], Some(0), 2)];
    nodes.extend(
        programs
            .iter()
            .enumerate()
            .map(|(index, batches)| NodeSpec {
                node: NodeId(index as i32 + 1),
                algorithm: "receiver-script".into(),
                config: json!({"batches":batches}),
            }),
    );
    let mut sim = simulator(
        "learning-duplicate-accepted-announcement",
        nodes.clone(),
        &[1, 2, 3],
    );
    sim.spawn(NodeId(0), scenarios::factory(&nodes[0]).unwrap())
        .unwrap();
    for (index, batches) in programs.into_iter().enumerate() {
        sim.spawn(
            NodeId(index as i32 + 1),
            Box::new(Peer {
                batches: batches.into(),
                crashed: false,
            }),
        )
        .unwrap();
    }
    sim.client_message(NodeId(0), propose(json!("x"))).unwrap();
    sim.run(1).unwrap();
    for node in [1, 2] {
        sim.client_message(NodeId(node), Value::Null).unwrap();
    }
    sim.run(2).unwrap();
    sim.client_message(NodeId(1), Value::Null).unwrap();
    sim.run(3).unwrap();
    sim.client_message(NodeId(2), Value::Null).unwrap();
    sim.run(4).unwrap();
    for node in [1, 2, 3] {
        sim.client_message(NodeId(node), Value::Null).unwrap();
    }
    sim.run(7).unwrap();
    sim.finalize("horizon").unwrap();

    let rows = sim.rows();
    let report = capture("learning-duplicate-accepted-announcement", rows);
    let replies = lines(rows, |row| {
        row.what == "dispatch"
            && row.why == "message"
            && row.fields["to"] == 0
            && row.fields["message"] == accepted
    });
    assert_eq!(replies.len(), 6);
    assert_eq!(rows[replies[0] - 1].fields["from"], 1);
    assert_eq!(rows[replies[1] - 1].fields["from"], 1);
    assert!(answer(rows, replies[0]).is_empty());
    assert!(answer(rows, replies[1]).is_empty());
    assert_eq!(rows[replies[2] - 1].fields["from"], 2);
    assert_eq!(
        answer(rows, replies[2]),
        vec![
            Desire::Commit {
                slot: Slot(0),
                entry: json!("x")
            },
            Desire::Send {
                to: NodeId(0),
                message: message("chosen", [2, 0], 0, "x")
            },
        ]
    );
    let quorum_close = close_line(rows, replies[2]);
    cited(
        receiver(&report, quorum_close),
        &[replies[0], replies[2], quorum_close],
    );
    for dispatch in &replies[..2] {
        receiver(&report, close_line(rows, *dispatch));
    }
    for dispatch in &replies[3..] {
        assert!(answer(rows, *dispatch).is_empty());
        cited(
            receiver(&report, close_line(rows, *dispatch)),
            &[quorum_close, *dispatch],
        );
    }
    assert_eq!(
        lines(rows, |row| row.what == "desire" && row.why == "commit").len(),
        1
    );
    assert_eq!(
        lines(rows, |row| {
            row.what == "desire"
                && row.why == "send"
                && row.fields["node"] == 0
                && row.fields["message"]["type"] == "chosen"
        })
        .len(),
        1
    );
    assert!(report["choices"].as_array().unwrap().is_empty());
    let claims = report["claims"].as_array().unwrap();
    assert_eq!(claims.len(), 2);
    for claim in claims {
        assert_eq!(claim["local_basis"]["verdict"], "established", "{claim}");
        assert_eq!(
            claim["transitive_support"]["verdict"], "indeterminate",
            "{claim}"
        );
    }
}
