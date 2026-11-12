use quorum::multi_paxos::{Config, MultiPaxosNode, propose};
use quorum::projection::{Projection, Tape};
use quorum::replay::{self, Mode};
use quorum::tape::{self, Header, NodeSpec, WorldConfig};
use quorum::{Context, Desire, Input, InputId, Node, NodeId, Simulator, Slot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    panic::{AssertUnwindSafe, catch_unwind},
};

// These peers exercise receiver rules, not globally legal Paxos histories. Their
// complete, test-local programs live in the header so strict replay reconstructs
// them without consulting live test state or adding a public script interpreter.
#[derive(Clone, Serialize, Deserialize)]
struct Outbound {
    to: NodeId,
    message: Value,
}
#[derive(Serialize, Deserialize)]
struct PeerConfig {
    batches: Vec<Vec<Outbound>>,
}
struct Script {
    config: PeerConfig,
    next: usize,
    crashed: bool,
}
impl Node for Script {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        if matches!(input, Input::Client { .. }) {
            let batch = self
                .config
                .batches
                .get(self.next)
                .expect("script exhausted");
            self.next += 1;
            for outbound in batch {
                ctx.send(outbound.to, &outbound.message);
            }
        }
    }
    fn crashed(&self) -> bool {
        self.crashed
    }
    fn set_crashed(&mut self, value: bool) {
        self.crashed = value;
    }
}
fn factory(spec: &NodeSpec) -> quorum::Result<Box<dyn Node>> {
    match spec.algorithm.as_str() {
        "multi-paxos" => Ok(Box::new(MultiPaxosNode::new(
            spec.node,
            serde_json::from_value(spec.config.clone()).map_err(|e| e.to_string())?,
        )?)),
        "receiver-script" => Ok(Box::new(Script {
            config: serde_json::from_value(spec.config.clone()).map_err(|e| e.to_string())?,
            next: 0,
            crashed: false,
        })),
        other => Err(format!("unknown fixture algorithm {other}")),
    }
}
fn config(acceptors: &[i32], learner: i32, active: Option<i32>) -> Value {
    json!({
        "acceptors": acceptors,
        "learners": [learner],
        "proposers": [0, 1, 2],
        "active_proposer": active,
        "from_slot": 0,
        "initial_round": 2,
        "initial_state": "empty"
    })
}
fn batch(to: i32, messages: Vec<Value>) -> Vec<Outbound> {
    messages
        .into_iter()
        .map(|message| Outbound {
            to: NodeId(to),
            message,
        })
        .collect()
}
fn fixture(
    real: i32,
    config: Value,
    programs: Vec<(i32, Vec<Vec<Outbound>>)>,
    ticks: bool,
) -> Simulator {
    let mut peers: BTreeMap<i32, Vec<Vec<Outbound>>> = BTreeMap::new();
    for field in ["acceptors", "learners", "proposers"] {
        for id in config[field].as_array().unwrap() {
            peers.entry(id.as_i64().unwrap() as i32).or_default();
        }
    }
    for (id, program) in programs {
        assert_ne!(id, real);
        assert!(id >= 0);
        peers.insert(id, program);
    }
    peers.remove(&real);
    let mut nodes: Vec<_> = peers
        .into_iter()
        .map(|(id, batches)| NodeSpec {
            node: NodeId(id),
            algorithm: "receiver-script".into(),
            config: serde_json::to_value(PeerConfig { batches }).unwrap(),
        })
        .collect();
    nodes.push(NodeSpec {
        node: NodeId(real),
        algorithm: "multi-paxos".into(),
        config: config.clone(),
    });
    nodes.sort_by_key(|spec| spec.node);
    let acceptors = serde_json::from_value(config["acceptors"].clone()).unwrap();
    let header = Header::new(
        "multi-paxos-receiver",
        42,
        WorldConfig {
            base_latency: 1,
            jitter: 0,
            tick_interval: if ticks { 1 } else { 0 },
            deadlock_threshold: 200,
        },
        nodes,
        acceptors,
    );
    let specs = header.nodes.clone();
    let mut sim = Simulator::new(header, Box::new(io::sink())).unwrap();
    for spec in specs {
        sim.spawn(spec.node, factory(&spec).unwrap()).unwrap();
    }
    sim
}
fn trigger(sim: &mut Simulator, peer: i32) {
    sim.client_message(NodeId(peer), Value::Null).unwrap();
}
fn advance(sim: &mut Simulator) {
    assert!(sim.run(sim.tick() + 1).unwrap());
}
fn source(sim: &Simulator) -> Tape {
    Tape::read(&tape::encode(sim.rows())).unwrap()
}
fn prepare(ballot: [u64; 2]) -> Value {
    json!({"type":"prepare","ballot":ballot,"from_slot":0})
}
fn promise(ballot: [u64; 2], accepted: Vec<Value>) -> Value {
    json!({"type":"promise","ballot":ballot,"from_slot":0,"accepted":accepted})
}
fn accept(ballot: [u64; 2], slot: u64, value: Value) -> Value {
    json!({"type":"accept","ballot":ballot,"slot":slot,"value":value})
}
fn accepted(ballot: [u64; 2], slot: u64, value: Value) -> Value {
    json!({"type":"accepted","ballot":ballot,"slot":slot,"value":value})
}
fn chosen(ballot: [u64; 2], slot: u64, value: Value) -> Value {
    json!({"type":"chosen","ballot":ballot,"slot":slot,"value":value})
}
fn entry(ballot: [u64; 2], slot: u64, value: Value) -> Value {
    json!({"ballot":ballot,"slot":slot,"value":value})
}
fn promised(ballot: [u64; 2]) -> Value {
    json!({"record":"promised","from_slot":0,"ballot":ballot})
}
fn saved(ballot: [u64; 2], slot: u64, value: Value) -> Value {
    json!({"record":"accepted","ballot":ballot,"slot":slot,"value":value})
}
fn nack(rejected: [u64; 2], watermark: [u64; 2]) -> Value {
    json!({"type":"nack","rejected_ballot":rejected,"promised_ballot":watermark})
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
fn broadcasts(members: &[i32], message: Value) -> Vec<Desire> {
    members
        .iter()
        .map(|id| send(*id, message.clone()))
        .collect()
}
fn proposals(members: &[i32], values: &[(u64, Value)]) -> Vec<Desire> {
    values
        .iter()
        .flat_map(|(slot, value)| broadcasts(members, accept([2, 0], *slot, value.clone())))
        .collect()
}
struct Turn {
    id: InputId,
    from: NodeId,
    message: Value,
    desires: Vec<Desire>,
}
fn turns(tape: &Tape, node: i32, kind: &str) -> Vec<Turn> {
    tape.projection
        .inputs
        .iter()
        .filter_map(|(id, input)| {
            let Input::Message { envelope, .. } = input else {
                return None;
            };
            if envelope.to != NodeId(node) || envelope.payload["type"] != kind {
                return None;
            }
            let answer = &tape.projection.answers[id];
            Some(Turn {
                id: *id,
                from: envelope.from,
                message: envelope.payload.clone(),
                desires: answer
                    .desires
                    .iter()
                    .map(|(_, desire)| desire.clone())
                    .collect(),
            })
        })
        .collect()
}
fn all_desires(tape: &Tape, node: i32) -> Vec<Desire> {
    tape.projection
        .answers
        .values()
        .filter(|answer| answer.node == NodeId(node))
        .flat_map(|answer| answer.desires.iter().map(|(_, desire)| desire.clone()))
        .collect()
}
fn node_inputs(tape: &Tape, node: i32) -> Vec<InputId> {
    tape.projection
        .inputs
        .iter()
        .filter_map(|(id, input)| (input.to() == NodeId(node)).then_some(*id))
        .collect()
}
fn lines(tape: &Tape, inputs: &[InputId]) -> Vec<usize> {
    let desires: BTreeSet<_> = inputs
        .iter()
        .filter_map(|id| tape.projection.answers.get(id))
        .flat_map(|answer| answer.desires.iter().map(|(id, _)| *id))
        .collect();
    tape.rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            let input = row
                .get::<InputId>("input_id")
                .ok()
                .or_else(|| row.get("parent_input_id").ok());
            let desire = row.get::<quorum::DesireId>("desire_id").ok();
            (input.is_some_and(|id| inputs.contains(&id))
                || desire.is_some_and(|id| desires.contains(&id)))
            .then_some(index + 1)
        })
        .collect()
}
fn observation(tape: &Tape, inputs: &[InputId], claim: &str) -> Value {
    let cited = lines(tape, inputs);
    assert!(!cited.is_empty());
    json!({"observation":claim,"lines":cited})
}
fn persist(name: &str, tape: &Tape, observations: Vec<Value>) {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/calm-evidence");
    fs::create_dir_all(&directory).unwrap();
    let basename = format!("receiver-{name}");
    let bytes = tape::encode(&tape.rows);
    let filename = format!("{basename}.jsonl");
    fs::write(directory.join(&filename), &bytes).unwrap();
    assert_eq!(tape.sha256, tape::hash(&bytes));
    let mut tails = BTreeSet::new();
    for end in 0..=tape.rows.len() {
        let prefix = Projection::from_rows(&tape.rows[..end]).unwrap();
        tails.insert(prefix.tail().to_string());
    }
    let prefixes = json!({"source_sha256":tape.sha256,"checked_complete_row_prefixes":tape.rows.len()+1,"tails":tails});
    let prefix_filename = format!("{basename}.prefixes.json");
    fs::write(
        directory.join(&prefix_filename),
        serde_json::to_vec_pretty(&prefixes).unwrap(),
    )
    .unwrap();
    let open = tape.projection.tail() == "open_invocation";
    let replay_source = if open {
        assert_eq!(tape.rows.last().unwrap().what, "dispatch");
        let closed = Tape::read(&tape::encode(&tape.rows[..tape.rows.len() - 1])).unwrap();
        fs::write(
            directory.join(format!("{basename}.closed-prefix.jsonl")),
            tape::encode(&closed.rows),
        )
        .unwrap();
        closed
    } else {
        Tape::read(&bytes).unwrap()
    };
    let report = replay::replay(&replay_source, Mode::Strict, factory).unwrap();
    assert_eq!(report.uncertified_answers, 0);
    assert_eq!(
        report.completed_answers,
        replay_source.projection.answers.len()
    );
    let replay_filename = format!("{basename}.replay.json");
    fs::write(
        directory.join(&replay_filename),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    let evidence = json!({name:{
        "tape":filename,"source_sha256":tape.sha256,
        "scope":"real-node scripted-peer receiver observations; not global legal histories or certificate verification",
        "observations":observations,"prefix_report":prefix_filename,
        "strict_replay_report":replay_filename,
        "replayed_rows":replay_source.rows.len(),
        "replay_scope":if open { "preceding closed portion only; final dispatch is an expected panicking open invocation" } else { "complete tape" }
    }});
    fs::write(
        directory.join(format!("{basename}.evidence.json")),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
}
fn finish(mut sim: Simulator) -> Tape {
    sim.finalize("horizon").unwrap();
    source(&sim)
}
fn assert_open_panic(sim: &Simulator) -> Tape {
    let tape = source(sim);
    assert_eq!(tape.projection.tail(), "open_invocation");
    assert!(
        !tape
            .rows
            .iter()
            .any(|row| row.fields.get("outcome") == Some(&json!("refused")))
    );
    let last = tape.rows.last().unwrap();
    assert_eq!(last.what, "dispatch");
    let id = last.input().unwrap().input_id();
    assert!(!tape.projection.answers[&id].closed);
    assert!(tape.projection.answers[&id].desires.is_empty());
    tape
}

#[test]
fn higher_accept_saves_watermark_then_acceptance_before_reply() {
    let mut sim = fixture(
        1,
        config(&[0, 1, 2], 1, None),
        vec![(0, vec![batch(1, vec![accept([2, 0], 0, json!("x"))])])],
        false,
    );
    trigger(&mut sim, 0);
    advance(&mut sim);
    let tape = finish(sim);
    let received = turns(&tape, 1, "accept");
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].desires,
        vec![
            save(promised([2, 0])),
            save(saved([2, 0], 0, json!("x"))),
            send(0, accepted([2, 0], 0, json!("x"))),
        ]
    );
    let cuts: Vec<_> = tape
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.what == "result" && row.why == "save")
        .map(|(i, _)| i + 1)
        .collect();
    assert_eq!(cuts.len(), 2);
    let first = Projection::from_rows(&tape.rows[..cuts[0]]).unwrap();
    assert_eq!(first.persistent[&NodeId(1)], vec![promised([2, 0])]);
    assert!(!first.pending.iter().any(|e| e.from == NodeId(1)));
    let second = Projection::from_rows(&tape.rows[..cuts[1]]).unwrap();
    assert_eq!(
        second.persistent[&NodeId(1)],
        vec![promised([2, 0]), saved([2, 0], 0, json!("x"))]
    );
    assert!(!second.pending.iter().any(|e| e.from == NodeId(1)));
    let mut observations = vec![observation(
        &tape,
        &[received[0].id],
        "From None: ordered Promised save, Accepted save, matching reply; all effects fulfilled.",
    )];
    observations.push(json!({"observation":"First cut has only the watermark; second has acceptance; neither cut has admitted the reply.","lines":cuts}));
    persist("higher-accept", &tape, observations);
}

#[test]
fn exact_repeat_uses_prior_save_and_stale_check_precedes_repeat_or_conflict() {
    let old = accept([1, 0], 0, json!("x"));
    let mut sim = fixture(
        2,
        config(&[0, 1, 2], 2, None),
        vec![
            (
                0,
                vec![
                    batch(2, vec![old.clone()]),
                    batch(2, vec![old.clone()]),
                    batch(
                        2,
                        vec![old, accept([1, 0], 0, json!("different-but-stale"))],
                    ),
                ],
            ),
            (1, vec![batch(2, vec![prepare([2, 1])])]),
        ],
        false,
    );
    trigger(&mut sim, 0);
    advance(&mut sim);
    trigger(&mut sim, 0);
    advance(&mut sim);
    trigger(&mut sim, 1);
    advance(&mut sim);
    trigger(&mut sim, 0);
    advance(&mut sim);
    let tape = finish(sim);
    let accepts = turns(&tape, 2, "accept");
    assert_eq!(accepts.len(), 4);
    assert_eq!(
        accepts[0].desires,
        vec![
            save(promised([1, 0])),
            save(saved([1, 0], 0, json!("x"))),
            send(0, accepted([1, 0], 0, json!("x")))
        ]
    );
    assert_eq!(
        accepts[1].desires,
        vec![send(0, accepted([1, 0], 0, json!("x")))]
    );
    let prepares = turns(&tape, 2, "prepare");
    assert_eq!(
        prepares[0].desires,
        vec![
            save(promised([2, 1])),
            send(1, promise([2, 1], vec![entry([1, 0], 0, json!("x"))]))
        ]
    );
    for turn in &accepts[2..] {
        assert_eq!(turn.desires, vec![send(0, nack([1, 0], [2, 1]))]);
    }
    assert_eq!(
        tape.projection.persistent[&NodeId(2)],
        vec![
            promised([1, 0]),
            saved([1, 0], 0, json!("x")),
            promised([2, 1])
        ]
    );
    persist(
        "repeat-then-stale",
        &tape,
        vec![
            observation(
                &tape,
                &[accepts[0].id, accepts[1].id],
                "Exact repeat emits only Accepted and adds no durable save.",
            ),
            observation(
                &tape,
                &[prepares[0].id, accepts[2].id, accepts[3].id],
                "Higher Prepare retains the accepted suffix; both stale exact and stale conflicting Accepts Nack before repeat/conflict handling.",
            ),
        ],
    );
}

#[test]
fn equal_prepare_reports_current_growth_without_a_second_promise_save() {
    let mut sim = fixture(
        1,
        config(&[0, 1, 2], 1, None),
        vec![(
            0,
            vec![
                batch(1, vec![prepare([1, 0])]),
                batch(1, vec![accept([1, 0], 0, json!("x"))]),
                batch(1, vec![prepare([1, 0])]),
            ],
        )],
        false,
    );
    for _ in 0..3 {
        trigger(&mut sim, 0);
        advance(&mut sim);
    }
    let tape = finish(sim);
    let prepares = turns(&tape, 1, "prepare");
    let accepts = turns(&tape, 1, "accept");
    assert_eq!(prepares.len(), 2);
    assert_eq!(
        prepares[0].desires,
        vec![save(promised([1, 0])), send(0, promise([1, 0], vec![]))]
    );
    assert_eq!(
        accepts[0].desires,
        vec![
            save(saved([1, 0], 0, json!("x"))),
            send(0, accepted([1, 0], 0, json!("x")))
        ]
    );
    assert_eq!(
        prepares[1].desires,
        vec![send(0, promise([1, 0], vec![entry([1, 0], 0, json!("x"))]))]
    );
    assert_eq!(
        tape.projection.persistent[&NodeId(1)],
        vec![promised([1, 0]), saved([1, 0], 0, json!("x"))]
    );
    persist(
        "equal-prepare-growth",
        &tape,
        vec![observation(
            &tape,
            &[prepares[0].id, accepts[0].id, prepares[1].id],
            "Equal Prepare reports the newly accepted suffix and performs no new Promised save.",
        )],
    );
}

#[test]
fn duplicate_promise_does_not_count_and_first_self_member_quorum_freezes() {
    let mut sim = fixture(
        0,
        config(&[0, 1, 2], 0, Some(0)),
        vec![
            (
                1,
                vec![batch(
                    0,
                    vec![promise([2, 0], vec![]), promise([2, 0], vec![])],
                )],
            ),
            (
                2,
                vec![batch(
                    0,
                    vec![promise([2, 0], vec![entry([1, 2], 0, json!("late-x"))])],
                )],
            ),
        ],
        true,
    );
    sim.client_message(NodeId(0), propose(json!("a"))).unwrap();
    advance(&mut sim);
    trigger(&mut sim, 1);
    advance(&mut sim);
    let before_self = source(&sim);
    let duplicates = turns(&before_self, 0, "promise");
    assert_eq!(duplicates.len(), 2);
    assert!(
        duplicates
            .iter()
            .all(|turn| turn.from == NodeId(1) && turn.desires.is_empty())
    );
    advance(&mut sim);
    trigger(&mut sim, 2);
    advance(&mut sim);
    let tape = finish(sim);
    let promises = turns(&tape, 0, "promise");
    assert_eq!(promises.len(), 4);
    assert_eq!(
        promises.iter().map(|turn| turn.from).collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(1), NodeId(0), NodeId(2)]
    );
    assert_eq!(
        promises[2].desires,
        proposals(&[0, 1, 2], &[(0, json!("a"))])
    );
    assert!(promises[3].desires.is_empty());
    let prepares = turns(&tape, 0, "prepare");
    assert_eq!(prepares[0].from, NodeId(0));
    assert_eq!(
        prepares[0].desires,
        vec![save(promised([2, 0])), send(0, promise([2, 0], vec![]))]
    );
    let emitted: Vec<_> = all_desires(&tape, 0)
        .into_iter()
        .filter(
            |desire| matches!(desire, Desire::Send { message, .. } if message["type"] == "accept"),
        )
        .collect();
    assert_eq!(emitted, proposals(&[0, 1, 2], &[(0, json!("a"))]));
    persist(
        "duplicate-late-promise",
        &tape,
        vec![
            observation(
                &tape,
                &[
                    promises[0].id,
                    promises[1].id,
                    prepares[0].id,
                    promises[2].id,
                ],
                "Two acceptor-1 replies do not freeze; the ordinary self-network Promise from acceptor 0 forms the first distinct quorum and issues a.",
            ),
            observation(
                &tape,
                &[promises[2].id, promises[3].id],
                "The valid late higher-prior report cannot revise the frozen empty adoption map or the immutable slot-0 proposal.",
            ),
        ],
    );
}

fn adoption_case(
    name: &str,
    reports: [Vec<Value>; 2],
    clients: Vec<Value>,
    expected: Vec<(u64, Value)>,
) {
    // Intentionally unsorted membership makes the declared node-id broadcast
    // ordering visible without requiring any internal state inspection.
    let mut sim = fixture(
        0,
        config(&[3, 1, 2], 0, Some(0)),
        vec![
            (1, vec![batch(0, vec![promise([2, 0], reports[0].clone())])]),
            (2, vec![batch(0, vec![promise([2, 0], reports[1].clone())])]),
        ],
        true,
    );
    for value in clients {
        sim.client_message(NodeId(0), propose(value)).unwrap();
    }
    advance(&mut sim);
    trigger(&mut sim, 1);
    trigger(&mut sim, 2);
    advance(&mut sim);
    sim.run(5).unwrap();
    let tape = finish(sim);
    let promises = turns(&tape, 0, "promise");
    assert_eq!(promises.len(), 2);
    assert!(promises[0].desires.is_empty());
    assert_eq!(promises[1].desires, proposals(&[1, 2, 3], &expected));
    let all = all_desires(&tape, 0);
    let prepares: Vec<_> = all
        .iter()
        .filter(
            |desire| matches!(desire, Desire::Send { message, .. } if message["type"] == "prepare"),
        )
        .cloned()
        .collect();
    assert_eq!(prepares, broadcasts(&[1, 2, 3], prepare([2, 0])));
    let accepts: Vec<_> = all
        .into_iter()
        .filter(
            |desire| matches!(desire, Desire::Send { message, .. } if message["type"] == "accept"),
        )
        .collect();
    assert_eq!(accepts, proposals(&[1, 2, 3], &expected));
    assert!(tape.projection.chosen[&NodeId(0)].is_empty());
    persist(
        name,
        &tape,
        vec![observation(
            &tape,
            &node_inputs(&tape, 0),
            &format!(
                "First quorum freezes adoption; exact ordered slot/value output is {expected:?}; no later ticks retry Phase 1 or Phase 2."
            ),
        )],
    );
}
#[test]
fn adoption_interleaves_fifo_without_consuming_adopted_client_values() {
    adoption_case(
        "adoption-fifo",
        [
            vec![entry([1, 1], 0, json!("x")), entry([1, 2], 2, json!("y"))],
            vec![],
        ],
        vec![json!("a"), json!("b")],
        vec![
            (0, json!("x")),
            (1, json!("a")),
            (2, json!("y")),
            (3, json!("b")),
        ],
    );
    adoption_case(
        "adoption-equal-client",
        [vec![entry([1, 1], 0, json!("x"))], vec![]],
        vec![json!("x")],
        vec![(0, json!("x")), (1, json!("x"))],
    );
    adoption_case(
        "adoption-empty-fifo",
        [
            vec![entry([1, 1], 0, json!("x")), entry([1, 2], 2, json!("y"))],
            vec![],
        ],
        vec![],
        vec![],
    );
    adoption_case(
        "adoption-highest-ballot",
        [
            vec![entry([1, 1], 0, json!("lower"))],
            vec![entry([1, 2], 0, json!("higher"))],
        ],
        vec![json!("a")],
        vec![(0, json!("higher")), (1, json!("a"))],
    );
    adoption_case(
        "adoption-null",
        [vec![entry([1, 1], 0, Value::Null)], vec![]],
        vec![json!("a")],
        vec![(0, Value::Null), (1, json!("a"))],
    );
}

#[test]
fn first_quorum_reports_reject_a_conflict_hidden_below_the_highest_ballot() {
    let mut sim = fixture(
        0,
        config(&[1, 2, 3, 4, 5], 0, Some(0)),
        vec![
            (
                1,
                vec![batch(
                    0,
                    vec![promise([2, 0], vec![entry([1, 1], 0, json!("x"))])],
                )],
            ),
            (
                2,
                vec![batch(
                    0,
                    vec![promise([2, 0], vec![entry([1, 2], 0, json!("higher-y"))])],
                )],
            ),
            (
                3,
                vec![batch(
                    0,
                    vec![promise(
                        [2, 0],
                        vec![entry([1, 1], 0, json!("conflicting-z"))],
                    )],
                )],
            ),
        ],
        true,
    );
    sim.client_message(NodeId(0), propose(json!("client")))
        .unwrap();
    advance(&mut sim);
    trigger(&mut sim, 1);
    trigger(&mut sim, 2);
    advance(&mut sim);
    let preceding = source(&sim);
    let first_two = turns(&preceding, 0, "promise");
    assert_eq!(first_two.len(), 2);
    assert!(first_two.iter().all(|turn| turn.desires.is_empty()));
    trigger(&mut sim, 3);
    assert!(catch_unwind(AssertUnwindSafe(|| sim.run(3))).is_err());
    let tape = assert_open_panic(&sim);
    let promises = turns(&tape, 0, "promise");
    assert_eq!(promises.len(), 3);
    assert!(promises.iter().all(|turn| turn.desires.is_empty()));
    assert!(all_desires(&tape, 0).iter().all(
        |desire| !matches!(desire, Desire::Send { message, .. } if message["type"] == "accept")
    ));
    let ids: Vec<_> = promises.iter().map(|turn| turn.id).collect();
    persist(
        "conflicting-reports-panic",
        &tape,
        vec![observation(
            &tape,
            &ids,
            "With quorum three of five, lower-ballot x, higher-ballot y, then the same lower ballot with z contradict before freeze; retaining only the highest report would miss this invariant failure. The third invocation remains open without world refusal.",
        )],
    );
}

#[test]
fn non_stale_same_ballot_conflicting_accept_is_an_open_inhabitant_panic() {
    let mut sim = fixture(
        1,
        config(&[0, 1, 2], 1, None),
        vec![(
            0,
            vec![
                batch(1, vec![accept([2, 0], 0, json!("x"))]),
                batch(1, vec![accept([2, 0], 0, json!("y"))]),
            ],
        )],
        false,
    );
    trigger(&mut sim, 0);
    advance(&mut sim);
    trigger(&mut sim, 0);
    assert!(catch_unwind(AssertUnwindSafe(|| sim.run(2))).is_err());
    let tape = assert_open_panic(&sim);
    let accepts = turns(&tape, 1, "accept");
    assert_eq!(accepts.len(), 2);
    assert_eq!(
        accepts[0].desires,
        vec![
            save(promised([2, 0])),
            save(saved([2, 0], 0, json!("x"))),
            send(0, accepted([2, 0], 0, json!("x")))
        ]
    );
    assert!(accepts[1].desires.is_empty());
    assert_eq!(
        tape.projection.persistent[&NodeId(1)],
        vec![promised([2, 0]), saved([2, 0], 0, json!("x"))]
    );
    persist(
        "conflicting-accept-panic",
        &tape,
        vec![observation(
            &tape,
            &[accepts[0].id, accepts[1].id],
            "The non-stale conflicting value panics at receive, leaving its dispatch open, with no returned answer, new save, or world refusal.",
        )],
    );
}

fn nack_case(two_positives: bool) {
    let mut sim = fixture(
        0,
        config(&[1, 2, 3], 0, Some(0)),
        vec![
            (
                1,
                vec![
                    batch(0, vec![promise([2, 0], vec![])]),
                    batch(0, vec![nack([2, 0], [3, 1])]),
                ],
            ),
            (
                2,
                vec![
                    batch(0, vec![promise([2, 0], vec![])]),
                    batch(
                        0,
                        vec![
                            accepted([2, 0], 0, json!("x")),
                            accepted([2, 0], 0, json!("x")),
                        ],
                    ),
                ],
            ),
            (
                3,
                vec![batch(
                    0,
                    if two_positives {
                        vec![accepted([2, 0], 0, json!("x"))]
                    } else {
                        vec![]
                    },
                )],
            ),
            (4, vec![batch(0, vec![accepted([2, 0], 0, json!("x"))])]),
        ],
        true,
    );
    sim.client_message(NodeId(0), propose(json!("x"))).unwrap();
    advance(&mut sim);
    trigger(&mut sim, 1);
    trigger(&mut sim, 2);
    advance(&mut sim);
    trigger(&mut sim, 1);
    trigger(&mut sim, 2);
    trigger(&mut sim, 4);
    if two_positives {
        trigger(&mut sim, 3);
    }
    sim.run(8).unwrap();
    let tape = finish(sim);
    let nacks = turns(&tape, 0, "nack");
    assert_eq!(nacks.len(), 1);
    assert!(nacks[0].desires.is_empty());
    let positives = turns(&tape, 0, "accepted");
    assert_eq!(positives.len(), if two_positives { 4 } else { 3 });
    assert!(positives[..3].iter().all(|turn| turn.desires.is_empty()));
    let expected = vec![
        Desire::Commit {
            slot: Slot(0),
            entry: json!("x"),
        },
        send(0, chosen([2, 0], 0, json!("x"))),
    ];
    if two_positives {
        assert_eq!(positives[3].from, NodeId(3));
        assert_eq!(positives[3].desires, expected);
        assert_eq!(
            tape.projection.chosen[&NodeId(0)],
            BTreeMap::from([(Slot(0), json!("x"))])
        );
        let self_chosen = turns(&tape, 0, "chosen");
        assert_eq!(self_chosen.len(), 1);
        assert!(self_chosen[0].desires.is_empty());
    } else {
        assert!(tape.projection.chosen[&NodeId(0)].is_empty());
    }
    let emitted: Vec<_> = all_desires(&tape, 0).into_iter().filter(|desire| matches!(desire, Desire::Send { message, .. } if message["type"] == "prepare" || message["type"] == "accept")).collect();
    let mut initial = broadcasts(&[1, 2, 3], prepare([2, 0]));
    initial.extend(proposals(&[1, 2, 3], &[(0, json!("x"))]));
    assert_eq!(emitted, initial);
    let ids = node_inputs(&tape, 0);
    persist(
        if two_positives {
            "nack-other-quorum"
        } else {
            "nack-one-positive"
        },
        &tape,
        vec![observation(
            &tape,
            &ids,
            if two_positives {
                "Nack does not abandon the proposal; duplicate acceptor-2 replies and outsider 4 add no weight; distinct acceptors 2 and 3 license exactly Commit then self-Chosen."
            } else {
                "One eligible positive, its duplicate, and outsider 4 do not license learning; the Nack and later ticks emit no retry."
            },
        )],
    );
}
#[test]
fn nack_does_not_decide_and_only_distinct_eligible_positives_complete() {
    nack_case(true);
    nack_case(false);
}

#[test]
fn ineligible_or_mismatched_replies_do_not_supply_quorum_weight() {
    let mut wrong_suffix = promise([2, 0], vec![]);
    wrong_suffix["from_slot"] = json!(1);
    let mut sim = fixture(
        0,
        config(&[1, 2, 3], 0, Some(0)),
        vec![
            (
                1,
                vec![
                    batch(
                        0,
                        vec![
                            promise([1, 0], vec![]),
                            wrong_suffix,
                            promise([2, 0], vec![]),
                            promise([2, 0], vec![]),
                        ],
                    ),
                    batch(
                        0,
                        vec![
                            accepted([1, 0], 0, json!("x")),
                            accepted([2, 0], 1, json!("x")),
                            accepted([2, 0], 0, json!("wrong")),
                            accepted([2, 0], 0, json!("x")),
                            accepted([2, 0], 0, json!("x")),
                        ],
                    ),
                ],
            ),
            (
                2,
                vec![
                    batch(0, vec![promise([2, 0], vec![])]),
                    batch(0, vec![accepted([2, 0], 0, json!("x"))]),
                ],
            ),
            (
                4,
                vec![
                    batch(0, vec![promise([2, 0], vec![])]),
                    batch(0, vec![accepted([2, 0], 0, json!("x"))]),
                ],
            ),
        ],
        true,
    );
    sim.client_message(NodeId(0), propose(json!("x"))).unwrap();
    advance(&mut sim);
    trigger(&mut sim, 4);
    trigger(&mut sim, 1);
    advance(&mut sim);
    assert!(
        turns(&source(&sim), 0, "promise")
            .iter()
            .all(|turn| turn.desires.is_empty())
    );
    trigger(&mut sim, 2);
    advance(&mut sim);
    trigger(&mut sim, 4);
    trigger(&mut sim, 1);
    advance(&mut sim);
    assert!(sim.chosen()[&NodeId(0)].is_empty());
    trigger(&mut sim, 2);
    sim.run(6).unwrap();
    let tape = finish(sim);
    let promises = turns(&tape, 0, "promise");
    assert_eq!(promises.len(), 6);
    assert!(promises[..5].iter().all(|turn| turn.desires.is_empty()));
    assert_eq!(
        promises[5].desires,
        proposals(&[1, 2, 3], &[(0, json!("x"))])
    );
    let positives = turns(&tape, 0, "accepted");
    assert_eq!(positives.len(), 7);
    assert!(positives[..6].iter().all(|turn| turn.desires.is_empty()));
    assert_eq!(
        positives[6].desires,
        vec![
            Desire::Commit {
                slot: Slot(0),
                entry: json!("x")
            },
            send(0, chosen([2, 0], 0, json!("x")))
        ]
    );
    let ids: Vec<_> = promises
        .iter()
        .chain(&positives)
        .map(|turn| turn.id)
        .collect();
    persist(
        "eligible-matching-quorums",
        &tape,
        vec![observation(
            &tape,
            &ids,
            "Outsiders, duplicates, wrong suffix, wrong ballot, wrong slot and wrong value cannot create quorum weight; the second distinct matching eligible member does.",
        )],
    );
}

#[test]
fn null_is_a_real_value_and_later_ballot_learning_is_suppressed() {
    let mut sim = fixture(
        0,
        config(&[1, 2, 3], 0, Some(0)),
        vec![
            (
                1,
                vec![
                    batch(0, vec![promise([2, 0], vec![])]),
                    batch(0, vec![accepted([2, 0], 0, Value::Null)]),
                    batch(0, vec![chosen([3, 1], 0, Value::Null)]),
                ],
            ),
            (
                2,
                vec![
                    batch(0, vec![promise([2, 0], vec![])]),
                    batch(0, vec![accepted([2, 0], 0, Value::Null)]),
                ],
            ),
        ],
        true,
    );
    sim.client_message(NodeId(0), propose(Value::Null)).unwrap();
    advance(&mut sim);
    trigger(&mut sim, 1);
    trigger(&mut sim, 2);
    advance(&mut sim);
    trigger(&mut sim, 1);
    trigger(&mut sim, 2);
    advance(&mut sim);
    advance(&mut sim);
    trigger(&mut sim, 1);
    advance(&mut sim);
    let tape = finish(sim);
    let promises = turns(&tape, 0, "promise");
    assert_eq!(
        promises[1].desires,
        proposals(&[1, 2, 3], &[(0, Value::Null)])
    );
    let learned = turns(&tape, 0, "accepted");
    assert_eq!(
        learned[1].desires,
        vec![
            Desire::Commit {
                slot: Slot(0),
                entry: Value::Null
            },
            send(0, chosen([2, 0], 0, Value::Null))
        ]
    );
    let announcements = turns(&tape, 0, "chosen");
    assert_eq!(announcements.len(), 2);
    assert_eq!(announcements[1].message, chosen([3, 1], 0, Value::Null));
    assert!(announcements.iter().all(|turn| turn.desires.is_empty()));
    assert_eq!(
        tape.projection.chosen[&NodeId(0)],
        BTreeMap::from([(Slot(0), Value::Null)])
    );
    persist(
        "null-and-later-ballot",
        &tape,
        vec![observation(
            &tape,
            &[
                promises[1].id,
                learned[1].id,
                announcements[0].id,
                announcements[1].id,
            ],
            "Explicit null travels through Propose, Accept, Accepted and value-only Commit; self-Chosen and equal value under a later owned ballot do not repeat the claim.",
        )],
    );
}

#[test]
fn null_acceptance_is_durable_but_conflicting_learning_panics() {
    let mut sim = fixture(
        1,
        config(&[0, 1, 2], 1, None),
        vec![
            (
                0,
                vec![
                    batch(1, vec![accept([2, 0], 0, Value::Null)]),
                    batch(1, vec![chosen([2, 0], 0, Value::Null)]),
                ],
            ),
            (
                2,
                vec![batch(1, vec![chosen([3, 2], 0, json!("contradiction"))])],
            ),
        ],
        false,
    );
    trigger(&mut sim, 0);
    advance(&mut sim);
    trigger(&mut sim, 0);
    advance(&mut sim);
    trigger(&mut sim, 2);
    assert!(catch_unwind(AssertUnwindSafe(|| sim.run(3))).is_err());
    let tape = assert_open_panic(&sim);
    let accepts = turns(&tape, 1, "accept");
    assert_eq!(
        accepts[0].desires,
        vec![
            save(promised([2, 0])),
            save(saved([2, 0], 0, Value::Null)),
            send(0, accepted([2, 0], 0, Value::Null))
        ]
    );
    let announcements = turns(&tape, 1, "chosen");
    assert_eq!(announcements.len(), 2);
    assert_eq!(
        announcements[0].desires,
        vec![Desire::Commit {
            slot: Slot(0),
            entry: Value::Null
        }]
    );
    assert!(announcements[1].desires.is_empty());
    assert_eq!(
        tape.projection.chosen[&NodeId(1)],
        BTreeMap::from([(Slot(0), Value::Null)])
    );
    persist(
        "contradictory-learning-panic",
        &tape,
        vec![observation(
            &tape,
            &[accepts[0].id, announcements[0].id, announcements[1].id],
            "Null is accepted and learned; a later owned Chosen with a different value fails in the inhabitant, not as a world refusal.",
        )],
    );
}

#[test]
fn learner_requires_an_eligible_authoritative_ballot_owner() {
    let mut sim = fixture(
        1,
        config(&[0, 1, 2], 1, None),
        vec![
            (
                0,
                vec![batch(
                    1,
                    vec![
                        prepare([2, 2]),
                        accept([2, 2], 0, json!("not-owned")),
                        chosen([2, 2], 0, json!("not-owned")),
                        chosen([1, 0], 0, json!("owned")),
                    ],
                )],
            ),
            (
                3,
                vec![batch(1, vec![chosen([3, 3], 0, json!("not-eligible"))])],
            ),
        ],
        true,
    );
    trigger(&mut sim, 3);
    trigger(&mut sim, 0);
    sim.run(4).unwrap();
    let tape = finish(sim);
    let prepares = turns(&tape, 1, "prepare");
    let accepts = turns(&tape, 1, "accept");
    assert!(prepares[0].desires.is_empty());
    assert!(accepts[0].desires.is_empty());
    let announcements = turns(&tape, 1, "chosen");
    assert_eq!(announcements.len(), 3);
    assert!(
        announcements[..2]
            .iter()
            .all(|turn| turn.desires.is_empty())
    );
    assert_eq!(
        announcements[2].desires,
        vec![Desire::Commit {
            slot: Slot(0),
            entry: json!("owned")
        }]
    );
    assert!(tape.projection.persistent[&NodeId(1)].is_empty());
    assert_eq!(
        all_desires(&tape, 1),
        vec![Desire::Commit {
            slot: Slot(0),
            entry: json!("owned")
        }]
    );
    let ids = node_inputs(&tape, 1);
    persist(
        "authoritative-ballot-owner",
        &tape,
        vec![observation(
            &tape,
            &ids,
            "An unconfigured proposer and an envelope sender that does not own its ballot are ignored. A configured non-active owner can announce a value; null active_proposer and subsequent ticks start no campaign.",
        )],
    );
}

#[test]
fn required_fields_distinguish_explicit_null_from_missing_value() {
    let valid = config(&[0, 1, 2], 1, None);
    let parsed: Config = serde_json::from_value(valid.clone()).unwrap();
    MultiPaxosNode::new(NodeId(1), parsed).unwrap();
    for field in [
        "acceptors",
        "learners",
        "proposers",
        "active_proposer",
        "from_slot",
        "initial_round",
        "initial_state",
    ] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<Config>(missing).is_err(),
            "missing config field {field}"
        );
    }
    let malformed = [
        ("missing-propose-value", json!({"type":"propose"}), true),
        (
            "missing-accept-value",
            json!({"type":"accept","ballot":[2,0],"slot":0}),
            false,
        ),
        (
            "missing-accepted-value",
            json!({"type":"accepted","ballot":[2,0],"slot":0}),
            false,
        ),
        (
            "missing-chosen-value",
            json!({"type":"chosen","ballot":[2,0],"slot":0}),
            false,
        ),
        (
            "missing-report-value",
            json!({"type":"promise","ballot":[2,0],"from_slot":0,"accepted":[{"slot":0,"ballot":[1,0]}]}),
            false,
        ),
    ];
    for (name, message, client) in malformed {
        let programs = if client {
            vec![]
        } else {
            vec![(0, vec![batch(1, vec![message.clone()])])]
        };
        let mut sim = fixture(1, valid.clone(), programs, false);
        let stopped = if client {
            catch_unwind(AssertUnwindSafe(|| sim.client_message(NodeId(1), message)))
        } else {
            trigger(&mut sim, 0);
            catch_unwind(AssertUnwindSafe(|| sim.run(1).map(|_| ())))
        };
        assert!(stopped.is_err(), "{name} must be malformed, not null");
        let tape = assert_open_panic(&sim);
        let id = tape.rows.last().unwrap().input().unwrap().input_id();
        persist(
            name,
            &tape,
            vec![observation(
                &tape,
                &[id],
                "A required value field is absent, not explicit null: parsing panics with an open invocation even when no campaign is active.",
            )],
        );
    }
}
