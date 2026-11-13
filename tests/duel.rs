use quorum::paxos_account::examine;
use quorum::projection::Account;
use quorum::tape::{self, Event};
use quorum::{Context, Desire, Input, Node, NodeId, Simulator, Slot, multi_paxos, scenarios};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, io, path::Path};

#[derive(Clone, Copy)]
struct Duel {
    scenario: &'static str,
    seed: u64,
    rows: usize,
    body_sha256: &'static str,
    allocation: usize,
    old_save: usize,
    old_acceptor: i32,
    reports: [usize; 2],
    late: usize,
}

const INCLUSION: Duel = Duel {
    scenario: "duel-inclusion",
    seed: 331,
    rows: 480,
    body_sha256: "1ffb7c51ba0494fa84d379bd52b2340f6b1639007beb3cc9ad88f20f3bfb42f3",
    allocation: 172,
    old_save: 114,
    old_acceptor: 0,
    reports: [106, 165],
    late: 193,
};
const EXCLUSION: Duel = Duel {
    scenario: "duel-exclusion",
    seed: 1271,
    rows: 408,
    body_sha256: "3f10dd73c8600839fce8b4b4d1bb4fb5ff257614bf13a95140a1fcd5097bc4a8",
    allocation: 186,
    old_save: 126,
    old_acceptor: 1,
    reports: [159, 182],
    late: 197,
};

fn array<'a>(value: &'a Value, field: &str) -> &'a [Value] {
    value[field].as_array().unwrap()
}

fn capture(duel: Duel) -> Simulator {
    scenarios::capture(duel.scenario, Box::new(io::sink())).unwrap()
}

fn report(rows: &[Event]) -> Value {
    let report = examine(&tape::encode(rows));
    assert!(report["world"]["error"].is_null(), "{report}");
    assert!(array(&report, "interpretation_findings").is_empty());
    report
}

fn attempt(report: &Value, proposer: i32, slot: u64) -> &Value {
    let matching: Vec<_> = array(report, "attempts")
        .iter()
        .filter(|a| a["proposer"] == proposer && a["slot"] == slot)
        .collect();
    assert_eq!(matching.len(), 1);
    matching[0]
}

fn empty_account(account: &Account) {
    assert!(account.active_input.is_none());
    assert!(account.unreached_desires.is_empty());
    assert!(account.open_attempts.is_empty());
    assert!(account.queued.is_empty());
    assert!(account.arrived.is_empty());
    assert!(account.clients.is_empty());
    assert!(account.due_messages.is_empty());
    assert!(account.due_ticks.is_empty());
    assert!(account.no_progress_due.is_none());
}

fn body_hash(bytes: &[u8]) -> String {
    let first_newline = bytes.iter().position(|byte| *byte == b'\n').unwrap();
    tape::hash(&bytes[first_newline + 1..])
}

fn established(report: &Value) {
    assert!(!array(report, "obligations").is_empty());
    for obligation in array(report, "obligations") {
        assert_eq!(obligation["verdict"], "established", "{obligation}");
        assert!(array(obligation, "gaps").is_empty(), "{obligation}");
    }
}

fn first_quorum(report: &Value, node: i32) -> &Value {
    let matching: Vec<_> = array(report, "observations")
        .iter()
        .filter(|o| o["kind"] == "first_promise_quorum" && o["node"] == node)
        .collect();
    assert_eq!(matching.len(), 1);
    matching[0]
}

fn boundary_families(report: &Value, line: usize) -> Vec<&str> {
    let mut families: Vec<_> = array(report, "obligations")
        .iter()
        .filter(|o| o["boundary_line"] == line && o["proposition"] != "no_conflicting_choices")
        .map(|o| o["proposition"].as_str().unwrap())
        .collect();
    families.sort_unstable();
    families
}

#[test]
fn honest_duel_headers_workloads_and_complete_bodies_are_fixed() {
    for duel in [INCLUSION, EXCLUSION] {
        let header = scenarios::header(duel.scenario).unwrap();
        assert_eq!(header.scenario, duel.scenario);
        assert_eq!(header.seed, duel.seed);
        assert_eq!(header.acceptors, [NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(
            serde_json::to_value(&header.world).unwrap(),
            json!({"base_latency":1,"jitter":3,"tick_interval":1,"deadlock_threshold":200})
        );
        assert_eq!(header.nodes.len(), 3);
        for (node, spec) in header.nodes.iter().enumerate() {
            assert_eq!(spec.node, NodeId(node as i32));
            assert_eq!(spec.algorithm, "multi-paxos");
            assert_eq!(
                spec.config,
                json!({"acceptors":[0,1,2],"learners":[0,1,2],"proposers":[0,1],
                    "active_proposer":if node < 2 { Some(node) } else { None },
                    "from_slot":0,"initial_round":if node == 1 { 2 } else { 1 },
                    "initial_state":"empty"})
            );
        }
        let sim = capture(duel);
        let rows = sim.rows();
        assert_eq!(rows.len(), duel.rows);
        assert_eq!(body_hash(&tape::encode(rows)), duel.body_sha256);
        let spawns: Vec<_> = rows.iter().filter(|row| row.what == "spawn").collect();
        assert_eq!(spawns.len(), 3);
        for (node, row) in spawns.iter().enumerate() {
            assert_eq!(row.when, 0);
            assert_eq!(row.fields["node"], node);
        }
        let clients: Vec<_> = rows
            .iter()
            .filter(|row| row.what == "dispatch" && row.why == "client")
            .collect();
        assert_eq!(clients.len(), 2);
        for (row, node, value) in [(clients[0], 0, "x"), (clients[1], 1, "y")] {
            assert_eq!(row.when, 0);
            assert_eq!(row.fields["to"], node);
            assert_eq!(row.fields["message"], multi_paxos::propose(json!(value)));
        }
        for node in 0..2 {
            let first_tick = rows
                .iter()
                .find(|row| row.what == "dispatch" && row.why == "tick" && row.fields["to"] == node)
                .unwrap();
            assert_eq!(first_tick.when, 1);
            let prepare = rows
                .iter()
                .find(|row| {
                    row.what == "desire"
                        && row.fields["node"] == node
                        && row.fields["message"]["type"] == "prepare"
                })
                .unwrap();
            assert_eq!(prepare.when, 1);
            assert_eq!(
                prepare.fields["parent_input_id"],
                first_tick.fields["input_id"]
            );
        }
    }
}

fn assert_prefix_stability(cut: &Value, full: &Value, cut_line: usize) {
    let full_obligations: BTreeMap<_, _> = array(full, "obligations")
        .iter()
        .map(|o| (o["id"].as_str().unwrap(), o))
        .collect();
    let mut prefix_safety = 0;
    for obligation in array(cut, "obligations") {
        if obligation["proposition"] == "no_conflicting_choices"
            && obligation["boundary_line"] == cut_line
        {
            prefix_safety += 1;
            assert_eq!(obligation["verdict"], "established");
        } else {
            assert_eq!(
                Some(&obligation),
                full_obligations.get(obligation["id"].as_str().unwrap()),
                "cut {cut_line}: {obligation}"
            );
        }
    }
    assert_eq!(prefix_safety, 1);
    assert_eq!(array(cut, "attempts").len(), array(full, "attempts").len());
    for a in array(cut, "attempts") {
        let later = array(full, "attempts")
            .iter()
            .find(|later| later["id"] == a["id"])
            .unwrap();
        assert_eq!(a["origin"], later["origin"], "cut {cut_line}: {}", a["id"]);
    }
    let quorums = |r: &Value| {
        array(r, "observations")
            .iter()
            .filter(|o| o["kind"] == "first_promise_quorum")
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(quorums(cut), quorums(full));
}

fn assert_lower_open(cut: &Value, rows: &[Event], duel: Duel) {
    let lower = attempt(cut, 0, 0);
    assert!(lower["exact_choice"].is_null());
    assert_eq!(lower["resolution"]["state"], "world_open");
    assert_eq!(lower["resolution"]["world_open"], true);
    assert_eq!(lower["resolution"]["whole_world_empty"], false);
    assert_eq!(lower["resolution"]["attribution_uncertain"], false);
    let work = array(lower, "world_work");
    let expected = if duel.seed == 331 {
        vec![16, 17]
    } else {
        vec![16]
    };
    assert_eq!(
        work.iter()
            .map(|w| w["identity"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    for item in work {
        assert_eq!(item["kind"], "message");
        assert_eq!(item["state"], "pending");
        let source = &rows[item["line"].as_u64().unwrap() as usize - 1];
        assert_eq!(source.what, "result");
        assert_eq!(source.why, "send");
        assert_eq!(source.fields["message_id"], item["identity"]);
        assert_eq!(source.fields["message"]["type"], "nack");
        assert_eq!(source.fields["message"]["rejected_ballot"], json!([1, 0]));
        assert!(
            array(&cut["world"]["account"], "queued")
                .iter()
                .any(|q| q["message_id"] == item["identity"])
        );
    }
}

#[test]
fn allocation_and_first_admission_prefixes_preserve_the_decision_not_the_tail() {
    for duel in [INCLUSION, EXCLUSION] {
        let sim = capture(duel);
        let rows = sim.rows();
        let full = report(rows);
        let allocation = report(&rows[..duel.allocation]);
        let admission = report(&rows[..duel.allocation + 1]);
        established(&allocation);
        established(&admission);
        for (cut, line) in [
            (&allocation, duel.allocation),
            (&admission, duel.allocation + 1),
        ] {
            assert_prefix_stability(cut, &full, line);
            assert!(array(cut, "choices").is_empty());
            assert_lower_open(cut, rows, duel);
        }
        // This is an exhaustive no-choice proof, not an inference from the
        // unrelated no-conflicting-choices judgment or absent received replies.
        let coverage = array(&allocation["coverage"], "nodes");
        assert_eq!(coverage.len(), 3);
        for (node, covered) in coverage.iter().enumerate() {
            assert_eq!(covered["node"], node);
            assert_eq!(covered["contract"], "covered");
            assert_eq!(covered["initial_state_known"], true);
            assert_eq!(covered["header_acceptor"], true);
            let declaration = &rows[0].fields["nodes"][node]["config"];
            assert_eq!(declaration["initial_state"], "empty");
        }
        let inventory: Vec<_> = rows[..duel.allocation]
            .iter()
            .filter(|row| {
                row.what == "result"
                    && row.why == "save"
                    && row.fields["outcome"] == "fulfilled"
                    && row.fields["record"]["record"] == "accepted"
            })
            .map(|row| {
                json!({"save_line":row.seq + 1,"acceptor":row.fields["node"],
                    "record":row.fields["record"]})
            })
            .collect();
        assert_eq!(
            inventory,
            [
                json!({"save_line":duel.old_save,"acceptor":duel.old_acceptor,
                "record":{"record":"accepted","slot":0,"ballot":[1,0],"value":"x"}})
            ]
        );
        let quorum = first_quorum(&allocation, 1);
        assert_eq!(quorum["dispatch_lines"], json!(duel.reports));
        assert_eq!(quorum["first_quorum_determined"], true);
        assert_eq!(quorum["adoption_map_determined"], true);
        assert!(array(quorum, "gaps").is_empty());
        let adoption = if duel.seed == 331 {
            json!([{"slot":0,"ballot":[1,0],"value":"x","report_lines":[165]}])
        } else {
            json!([])
        };
        assert_eq!(quorum["adoption_map"], adoption);
        for (position, line) in duel.reports.iter().enumerate() {
            let promise = &rows[line - 1];
            assert_eq!(promise.what, "dispatch");
            assert_eq!(promise.fields["to"], 1);
            assert_eq!(promise.fields["message"]["type"], "promise");
            assert_eq!(promise.fields["message"]["ballot"], json!([2, 1]));
            let accepted = if duel.seed == 331 && position == 1 {
                json!([{"slot":0,"ballot":[1,0],"value":"x"}])
            } else {
                json!([])
            };
            assert_eq!(promise.fields["message"]["accepted"], accepted);
        }
        let close = &rows[duel.allocation - 1];
        assert_eq!(close.what, "close");
        assert_eq!(close.fields["node"], 1);
        let slots = if duel.seed == 331 {
            vec![(0, "x"), (1, "y")]
        } else {
            vec![(0, "y")]
        };
        let expected: Vec<_> = slots
            .iter()
            .flat_map(|(slot, value)| {
                (0..3).map(move |to| {
                    json!({"to":to,"message":{"type":"accept","ballot":[2,1],
                        "slot":slot,"value":value}})
                })
            })
            .collect();
        let answer: Vec<_> = rows[..duel.allocation]
            .iter()
            .filter(|row| {
                row.what == "desire" && row.fields["parent_input_id"] == close.fields["input_id"]
            })
            .map(|row| {
                assert_eq!(row.why, "send");
                json!({"to":row.fields["to"],"message":row.fields["message"]})
            })
            .collect();
        assert_eq!(answer, expected);
        assert_eq!(close.fields["count"], expected.len());
        assert_eq!(allocation["world"]["tail"], "interrupted_drain");
        assert_eq!(
            allocation["world"]["account"]["active_input"],
            close.fields["input_id"]
        );
        assert_eq!(
            array(&allocation["world"]["account"], "unreached_desires").len(),
            expected.len()
        );
        for a in array(&allocation, "attempts")
            .iter()
            .filter(|a| a["proposer"] == 1)
        {
            assert!(
                array(a, "sends")
                    .iter()
                    .all(|send| send["attempt_line"].is_null())
            );
        }
        let expected_close = if duel.seed == 331 {
            vec![
                "fifo",
                "fifo",
                "fresh_origin_count",
                "fresh_origin_count",
                "proposal_drain",
            ]
        } else {
            vec!["fifo", "fresh_origin_count", "proposal_drain"]
        };
        assert_eq!(
            boundary_families(&allocation, duel.allocation),
            expected_close
        );
        assert_eq!(
            boundary_families(&admission, duel.allocation + 1),
            ["adoption", "phase2_gate", "proposer_authority"]
        );
        assert_eq!(rows[duel.allocation].what, "attempt");
        assert_eq!(rows[duel.allocation].why, "send");
        let fresh = attempt(&allocation, 1, u64::from(duel.seed == 331));
        assert_eq!(fresh["value"], "y");
        assert_eq!(fresh["origin"]["kind"], "fresh");
        assert_eq!(
            fresh["origin"]["client_count"],
            json!({"allocated":1,"available":1,"certain":true})
        );
        assert!(fresh["origin"].get("acceptance_ancestry").is_none());
        if duel.seed == 331 {
            let adopted = attempt(&allocation, 1, 0);
            assert_eq!(adopted["origin"]["kind"], "adopted");
            assert!(adopted["origin"]["client_count"].is_null());
            let ancestry = &adopted["origin"]["acceptance_ancestry"];
            assert_eq!(ancestry["verdict"], "established");
            assert_eq!(ancestry["boundary_line"], 172);
            assert_eq!(ancestry["frozen_report_dispatch_lines"], json!([106, 165]));
            assert_eq!(array(ancestry, "paths").len(), 1);
            let path = &ancestry["paths"][0];
            assert_eq!(path["acceptor"], 0);
            assert_eq!(path["selected"], true);
            assert_eq!(path["verdict"], "established");
            assert_eq!(path["report_dispatch_line"], 165);
            assert_eq!(path["matching_saves"][0]["result_line"], 114);
            assert_eq!(path["promise_source"]["send_admission_line"], 131);
            assert!(array(path, "later_matching_saves").is_empty());
        }
        let late = &rows[duel.late - 1];
        assert_eq!(late.fields["message"]["type"], "promise");
        assert_eq!(late.fields["from"], 1);
        assert_eq!(late.fields["to"], 1);
        assert_eq!(
            late.fields["message"]["accepted"],
            if duel.seed == 331 {
                json!([])
            } else {
                json!([{"slot":0,"ballot":[1,0],"value":"x"}])
            }
        );
        assert!(array(&full, "observations").contains(&json!({
            "kind":"promise_not_counted","line":duel.late,"node":1,"from":1,"reason":"frozen_quorum"
        })));
        assert_eq!(first_quorum(&full, 1), quorum);
    }
}

fn assert_reply_ancestry(rows: &[Event], lower: &Value) {
    let mut counts = BTreeMap::new();
    assert_eq!(array(lower, "replies").len(), 3);
    for acceptor in array(lower, "replies") {
        assert_eq!(acceptor["no_reply_observed"], false);
        assert_eq!(array(acceptor, "received").len(), 1);
        for reply in array(acceptor, "received") {
            *counts.entry(reply["kind"].as_str().unwrap()).or_insert(0) += 1;
            assert_eq!(reply["classification"], "matching");
            assert_eq!(reply["eligible"], true);
            assert_eq!(reply["from"], acceptor["acceptor"]);
            assert_eq!(reply["to"], 0);
            let request = &rows[reply["request_dispatch_line"].as_u64().unwrap() as usize - 1];
            assert_eq!(request.what, "dispatch");
            assert_eq!(request.fields["from"], 0);
            assert_eq!(request.fields["to"], acceptor["acceptor"]);
            assert_eq!(
                request.fields["message"],
                json!({"type":"accept","slot":0,"ballot":[1,0],"value":"x"})
            );
            let send = &rows[reply["send_attempt_line"].as_u64().unwrap() as usize - 1];
            assert_eq!(send.what, "attempt");
            assert_eq!(send.why, "send");
            assert_eq!(send.fields["parent_input_id"], request.fields["input_id"]);
            let dispatch = &rows[reply["line"].as_u64().unwrap() as usize - 1];
            assert_eq!(dispatch.what, "dispatch");
            assert_eq!(
                dispatch.fields["parent_desire_id"],
                send.fields["desire_id"]
            );
            assert_eq!(dispatch.fields["message"], send.fields["message"]);
            assert_eq!(dispatch.fields["message_id"], reply["message_id"]);
            assert!(
                array(lower, "sends")
                    .iter()
                    .any(|s| s["message_id"] == request.fields["message_id"])
            );
            if reply["kind"] == "nack" {
                assert_eq!(reply["ancestry"], "accept");
                assert_eq!(reply["attempt_id"], lower["id"]);
                assert!(reply["campaign_ballot"].is_null());
                assert_eq!(reply["parent_input_line"], reply["request_dispatch_line"]);
                assert_eq!(reply["promised_ballot"], json!([2, 1]));
                assert_eq!(reply["rejected_ballot"], json!([1, 0]));
            } else {
                assert_eq!(reply["kind"], "accepted");
                assert_eq!(reply["ballot"], json!([1, 0]));
                assert_eq!(reply["value"], "x");
                assert!(rows[..reply["send_attempt_line"].as_u64().unwrap() as usize - 1]
                    .iter()
                    .any(|row| row.what == "result"
                        && row.why == "save"
                        && row.fields["outcome"] == "fulfilled"
                        && row.fields["parent_input_id"] == request.fields["input_id"]
                        && row.fields["record"] == json!({"record":"accepted","slot":0,"ballot":[1,0],"value":"x"})));
            }
        }
    }
    assert_eq!(counts, BTreeMap::from([("accepted", 1), ("nack", 2)]));
    assert!(array(lower, "other_replies").is_empty());
}

#[test]
fn full_accounts_distinguish_unresolved_attempts_choices_and_local_knowledge() {
    for duel in [INCLUSION, EXCLUSION] {
        let sim = capture(duel);
        let rows = sim.rows();
        let full = report(rows);
        established(&full);
        assert_eq!(
            full["interpretation"],
            "quorum/003_multi_paxos.md@sha256:d8886526fbaa53326c66a9b282a78f7b71de63d4f877bea3de65ebefaf0866d3"
        );
        assert_eq!(sim.tick(), 28);
        assert_eq!(full["world"]["tail"], "finalized");
        assert!(full["world"]["stopped"].is_null());
        empty_account(&sim.check_projection().unwrap().account());
        let before_final =
            quorum::projection::Projection::from_rows(&rows[..rows.len() - 1]).unwrap();
        empty_account(&before_final.account());
        let final_row = rows.last().unwrap();
        assert_eq!(final_row.what, "finalize");
        assert_eq!(final_row.when, 28);
        assert_eq!(final_row.fields["reason"], "horizon");
        assert_eq!(
            final_row.fields["account"],
            serde_json::to_value(before_final.account()).unwrap()
        );
        assert!(
            !rows
                .iter()
                .any(|row| row.what == "no_progress" || row.what == "fault")
        );
        // The fixed workload has no retry campaign. Five hops of D=4 from
        // tick 1 bound delivery of Chosen at tick 21, strictly before horizon.
        for node in 0..2 {
            assert_eq!(
                rows.iter()
                    .filter(|row| row.what == "desire"
                        && row.why == "send"
                        && row.fields["node"] == node
                        && row.fields["message"]["type"] == "prepare")
                    .count(),
                3
            );
        }
        for row in rows
            .iter()
            .filter(|row| row.what == "dispatch" && row.why == "message")
        {
            assert!(row.when <= 1 + 5 * 4);
        }
        let lower = attempt(&full, 0, 0);
        assert_reply_ancestry(rows, lower);
        assert_eq!(lower["resolution"]["state"], "unresolved");
        assert_eq!(lower["resolution"]["world_open"], false);
        assert_eq!(lower["resolution"]["whole_world_empty"], true);
        assert_eq!(lower["resolution"]["attribution_uncertain"], false);
        assert!(lower["exact_choice"].is_null());
        assert!(array(lower, "world_work").is_empty());
        let expected_choices = if duel.seed == 331 {
            json!([
                {"id":"choice:229","slot":1,"ballot":[2,1],"value":"y","first_quorum_line":229,
                    "first_support":[{"acceptor":1,"save_line":213},{"acceptor":0,"save_line":229}]},
                {"id":"choice:251","slot":0,"ballot":[2,1],"value":"x","first_quorum_line":251,
                    "first_support":[{"acceptor":1,"save_line":204},{"acceptor":0,"save_line":251}]}
            ])
        } else {
            json!([{"id":"choice:242","slot":0,"ballot":[2,1],"value":"y","first_quorum_line":242,
                "first_support":[{"acceptor":0,"save_line":219},{"acceptor":1,"save_line":242}]}])
        };
        let choices: Vec<_> = array(&full, "choices")
            .iter()
            .map(|c| {
                assert_eq!(c["quorum"], 2);
                assert_eq!(array(c, "witnesses").len(), 3);
                json!({"id":c["id"],"slot":c["slot"],"ballot":c["ballot"],"value":c["value"],
                "first_quorum_line":c["first_quorum_line"],"first_support":c["first_support"]})
            })
            .collect();
        assert_eq!(json!(choices), expected_choices);
        assert_eq!(
            lower["same_value_elsewhere"],
            if duel.seed == 331 {
                json!(["choice:251"])
            } else {
                json!([])
            }
        );
        assert_eq!(
            lower["slot_witnessed_values"],
            if duel.seed == 331 {
                json!([])
            } else {
                json!(["choice:242"])
            }
        );
        let expected_learned = if duel.seed == 331 {
            BTreeMap::from([(Slot(0), json!("x")), (Slot(1), json!("y"))])
        } else {
            BTreeMap::from([(Slot(0), json!("y"))])
        };
        assert_eq!(sim.chosen().len(), 3);
        for node in 0..3 {
            assert_eq!(sim.chosen()[&NodeId(node)], expected_learned);
        }
        let mut knowledge = BTreeMap::new();
        for observation in array(&full, "observations")
            .iter()
            .filter(|o| o["kind"] == "knowledge_evidence")
        {
            assert_eq!(observation["local_basis"]["verdict"], "established");
            assert_eq!(observation["transitive_support"]["verdict"], "established");
            let node = observation["node"].as_i64().unwrap();
            assert_eq!(
                observation["local_basis"]["reason"],
                if node == 1 {
                    "matching_received_accepted_quorum"
                } else {
                    "matching_received_chosen"
                }
            );
            if observation["ballot"].is_null() {
                assert!(
                    knowledge
                        .insert(
                            (node, observation["slot"].as_u64().unwrap()),
                            observation["boundary_line"].as_u64().unwrap()
                        )
                        .is_none()
                );
            }
        }
        assert_eq!(
            knowledge,
            if duel.seed == 331 {
                BTreeMap::from([
                    ((1, 1), 289),
                    ((1, 0), 324),
                    ((0, 1), 339),
                    ((2, 1), 359),
                    ((0, 0), 365),
                    ((2, 0), 378),
                ])
            } else {
                BTreeMap::from([((1, 0), 278), ((0, 0), 310), ((2, 0), 330)])
            }
        );
        for a in array(&full, "attempts")
            .iter()
            .filter(|a| a["proposer"] == 1)
        {
            assert_eq!(a["resolution"]["state"], "exact_choice");
            assert_eq!(a["resolution"]["world_open"], false);
            assert!(!a["exact_choice"].is_null());
            assert!(array(a, "world_work").is_empty());
        }
    }
}

// A declared answer mutant, never a production scenario or an honest replay
// oracle: only B's first adopted allocation is rewritten, not world semantics.
struct FaultyAnswer {
    inner: Box<dyn Node>,
    changed: bool,
}

impl Node for FaultyAnswer {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        let mut answer = Vec::new();
        self.inner
            .receive(&mut Context::new(ctx.node_id, &mut answer), input);
        let replace = !self.changed
            && answer.iter().any(|desire| {
                matches!(desire, Desire::Send { message, .. }
                if message["type"] == "accept" && message["slot"] == 0 && message["value"] == "x")
            });
        if replace {
            self.changed = true;
        }
        for desire in answer {
            match desire {
                Desire::Send { to, mut message } => {
                    if replace && message["type"] == "accept" {
                        if message["slot"] == 1 {
                            continue;
                        }
                        assert_eq!(message["slot"], 0);
                        message["value"] = json!("y");
                    }
                    ctx.send(to, message);
                }
                Desire::Save { record } => ctx.save(record),
                Desire::Commit { slot, entry } => ctx.commit(slot, entry),
            }
        }
    }

    fn crashed(&self) -> bool {
        self.inner.crashed()
    }

    fn set_crashed(&mut self, crashed: bool) {
        self.inner.set_crashed(crashed);
    }
}

fn faulty_control() -> Simulator {
    let mut header = scenarios::header("duel-inclusion").unwrap();
    header.scenario = "duel-declared-faulty-answer-control".into();
    let specs = header.nodes.clone();
    let mut sim = Simulator::new(header, Box::new(io::sink())).unwrap();
    for spec in &specs {
        let inner = scenarios::factory(spec).unwrap();
        let node: Box<dyn Node> = if spec.node == NodeId(1) {
            Box::new(FaultyAnswer {
                inner,
                changed: false,
            })
        } else {
            inner
        };
        sim.spawn(spec.node, node).unwrap();
    }
    sim.client_message(NodeId(0), multi_paxos::propose(json!("x")))
        .unwrap();
    sim.client_message(NodeId(1), multi_paxos::propose(json!("y")))
        .unwrap();
    assert!(sim.run(28).unwrap());
    sim
}

#[test]
fn declared_faulty_answer_is_fresh_once_but_violates_adoption_fifo_and_drain() {
    let sim = faulty_control();
    let all = report(sim.rows());
    let first_choice = &array(&all, "choices")[0];
    assert_eq!(first_choice["first_quorum_line"], 221);
    assert_eq!(first_choice["value"], "y");
    let rows = &sim.rows()[..221];
    let bytes = tape::encode(rows);
    assert_eq!(
        body_hash(&bytes),
        "a5946906ba6bab2daccafff6b8680170a1b8bc51fc666bf13549738c72160b39"
    );
    let control = report(rows);
    assert_eq!(control["world"]["valid_rows"], 221);
    assert_eq!(array(&control, "choices").len(), 1);
    let choice = &control["choices"][0];
    assert_eq!(choice["id"], "choice:221");
    assert_eq!(choice["slot"], 0);
    assert_eq!(choice["ballot"], json!([2, 1]));
    assert_eq!(choice["value"], "y");
    assert_eq!(
        choice["first_support"],
        json!([{"acceptor":1,"save_line":195},{"acceptor":0,"save_line":221}])
    );
    assert_eq!(choice["quorum"], 2);
    let violations: Vec<_> = array(&control, "obligations")
        .iter()
        .filter(|o| o["verdict"] == "violated")
        .map(|o| {
            (
                o["proposition"].as_str().unwrap(),
                o["boundary_line"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        violations,
        [
            ("fifo", 169),
            ("proposal_drain", 169),
            ("adoption", 170),
            ("adoption", 172),
            ("adoption", 174)
        ]
    );
    let mut safety = 0;
    for obligation in array(&control, "obligations") {
        assert!(array(obligation, "gaps").is_empty());
        if obligation["verdict"] != "violated" {
            assert_eq!(obligation["verdict"], "established", "{obligation}");
        }
        if obligation["proposition"] == "no_conflicting_choices" {
            safety += 1;
            assert_eq!(obligation["boundary_line"], 221);
            assert_eq!(obligation["verdict"], "established");
        }
        if obligation["verdict"] == "violated" {
            for line in [106, 165] {
                assert!(
                    array(obligation, "support").contains(&json!(line)),
                    "{obligation}"
                );
            }
            assert!(!array(obligation, "counterevidence").is_empty());
            assert!(array(obligation, "counterevidence").contains(&obligation["boundary_line"]));
            if obligation["proposition"] == "adoption" {
                let output_line = match obligation["boundary_line"].as_u64().unwrap() {
                    170 => 166,
                    172 => 167,
                    174 => 168,
                    line => panic!("unexpected adoption boundary {line}"),
                };
                assert!(
                    array(obligation, "support").contains(&json!(output_line)),
                    "{obligation}"
                );
            }
        }
    }
    assert_eq!(safety, 1);
    assert_eq!(
        boundary_families(&control, 169),
        ["fifo", "fresh_origin_count", "proposal_drain"]
    );
    for line in [170, 172, 174] {
        assert_eq!(
            boundary_families(&control, line),
            ["adoption", "phase2_gate", "proposer_authority"]
        );
    }
    let fresh = attempt(&control, 1, 0);
    assert_eq!(array(&control, "attempts").len(), 2);
    assert_eq!(fresh["value"], "y");
    assert_eq!(fresh["origin"]["kind"], "fresh");
    assert_eq!(fresh["origin"]["allocation_line"], 169);
    assert_eq!(
        fresh["origin"]["client_count"],
        json!({"available":1,"allocated":1,"certain":true})
    );
    assert!(fresh["origin"].get("acceptance_ancestry").is_none());
    assert_eq!(fresh["exact_choice"], "choice:221");
    assert_eq!(
        first_quorum(&control, 1)["dispatch_lines"],
        json!([106, 165])
    );
    assert_eq!(
        first_quorum(&control, 1)["adoption_map"],
        json!([{"ballot":[1,0],"slot":0,"value":"x","report_lines":[165]}])
    );
    assert_eq!(rows[168].what, "close");
    assert_eq!(rows[168].fields["count"], 3);
    for (position, row) in rows[165..168].iter().enumerate() {
        assert_eq!(row.what, "desire");
        assert_eq!(row.why, "send");
        assert_eq!(row.fields["to"], position);
        assert_eq!(
            row.fields["message"],
            json!({"type":"accept","ballot":[2,1],"slot":0,"value":"y"})
        );
    }
    assert!(array(&control, "claims").is_empty());
    // One test owns this artifact, so concurrent tests cannot overwrite it.
    let output = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/duel-evidence");
    fs::create_dir_all(&output).unwrap();
    fs::write(
        output.join("declared-faulty-answer-to-first-choice.jsonl"),
        bytes,
    )
    .unwrap();
}

// Derive the fixed histories' report/acceptance/answer relation directly from
// tape rows, without account judgments or pinned lines.
struct FrozenDecision {
    members: BTreeMap<i64, (usize, Value)>,
    freeze_dispatch: usize,
    close: usize,
    answer: Vec<(u64, Value)>,
    old: Vec<(i64, usize)>,
}

fn line(row: &Event) -> usize {
    row.seq as usize + 1
}

fn carries(row: &Event, what: &str, kind: &str) -> bool {
    let why = if what == "dispatch" {
        "message"
    } else {
        "send"
    };
    row.what == what && row.why == why && row.fields["message"]["type"] == kind
}

fn accepted_save(row: &Event) -> bool {
    row.what == "result"
        && row.why == "save"
        && row.fields["outcome"] == "fulfilled"
        && row.fields["record"]["record"] == "accepted"
}

fn promise_admission(rows: &[Event], acceptor: i64) -> usize {
    rows.iter()
        .find(|row| {
            carries(row, "attempt", "promise")
                && row.fields["node"] == acceptor
                && row.fields["to"] == 1
        })
        .map(line)
        .unwrap()
}

fn frozen_decision(rows: &[Event]) -> FrozenDecision {
    let header = &rows[0].fields;
    let quorum = header["acceptors"].as_array().unwrap().len() / 2 + 1;
    let ballot = |node: usize| json!([header["nodes"][node]["config"]["initial_round"], node]);
    let (lower, higher) = (ballot(0), ballot(1));
    let mut members = BTreeMap::new();
    let mut freeze = None;
    for row in rows {
        if freeze.is_none()
            && carries(row, "dispatch", "promise")
            && row.fields["to"] == 1
            && row.fields["message"]["ballot"] == higher
        {
            let report = row.fields["message"]["accepted"].clone();
            // This honest workload has one valid matching Promise per sender.
            members
                .entry(row.fields["from"].as_i64().unwrap())
                .or_insert((line(row), report));
            if members.len() == quorum {
                freeze = Some(row);
            }
        }
    }
    let freeze = freeze.expect("B freezes a first Promise quorum");
    let input = &freeze.fields["input_id"];
    let close = rows
        .iter()
        .find(|row| row.what == "close" && row.fields["input_id"] == *input)
        .map(line)
        .unwrap();
    let mut answer = Vec::new();
    for row in rows
        .iter()
        .filter(|row| carries(row, "desire", "accept") && row.fields["parent_input_id"] == *input)
    {
        let message = &row.fields["message"];
        let proposal = (message["slot"].as_u64().unwrap(), message["value"].clone());
        if !answer.contains(&proposal) {
            answer.push(proposal);
        }
    }
    let old = rows[..close]
        .iter()
        .filter(|row| accepted_save(row) && row.fields["record"]["ballot"] == lower)
        .map(|row| (row.fields["node"].as_i64().unwrap(), line(row)))
        .collect();
    FrozenDecision {
        members,
        freeze_dispatch: line(freeze),
        close,
        answer,
        old,
    }
}

#[test]
fn one_quorum_membership_decides_by_whether_the_old_acceptance_is_inside_it() {
    let old_x = json!([{"slot":0,"ballot":[1,0],"value":"x"}]);
    let mut memberships = Vec::new();
    for duel in [INCLUSION, EXCLUSION] {
        let sim = capture(duel);
        let rows = sim.rows();
        let decision = frozen_decision(rows);
        // The sole fulfilled acceptance before the close is the old one.
        assert_eq!(decision.old.len(), 1);
        let (holder, save) = decision.old[0];
        assert_eq!(
            rows[..decision.close]
                .iter()
                .filter(|row| accepted_save(row))
                .count(),
            1
        );
        // Each frozen report tells the truth about its acceptor's history at
        // the moment that Promise was admitted.
        for (&member, (dispatch, reported)) in &decision.members {
            assert!(*dispatch <= decision.freeze_dispatch);
            let held = member == holder && save < promise_admission(rows, member);
            assert_eq!(*reported, if held { old_x.clone() } else { json!([]) });
        }
        let inside = decision.members.contains_key(&holder);
        assert_eq!(inside, duel.seed == 331);
        assert_eq!(
            decision.answer,
            if inside {
                vec![(0, json!("x")), (1, json!("y"))]
            } else {
                vec![(0, json!("y"))]
            }
        );
        if !inside {
            // The excluded acceptance is B's own: real, already durable when
            // B promised itself, and reported only after the decision closed.
            assert_eq!(holder, 1);
            assert!(save < promise_admission(rows, holder));
            let own = rows
                .iter()
                .find(|row| {
                    carries(row, "dispatch", "promise")
                        && row.fields["to"] == 1
                        && row.fields["from"] == holder
                })
                .unwrap();
            assert!(line(own) > decision.close);
            assert_eq!(own.fields["message"]["accepted"], old_x);
        }
        memberships.push(decision.members.keys().copied().collect::<Vec<_>>());
    }
    // The same two acceptors decide in both histories.
    assert_eq!(memberships, [vec![0, 2], vec![0, 2]]);
}

#[test]
fn declared_wrapper_leaves_the_honest_history_untouched_until_one_answer() {
    let honest = capture(INCLUSION);
    let control = faulty_control();
    let (honest, control) = (honest.rows(), control.rows());
    let decision = frozen_decision(honest);
    // Row 0 is the header, which differs in scenario name by declaration.
    let first = (1..honest.len().min(control.len()))
        .find(|&row| honest[row] != control[row])
        .unwrap();
    // Every row through B's quorum-completing dispatch is identical; the
    // first difference is the first desire of that one answer.
    assert_eq!(first, decision.freeze_dispatch);
    let input = &honest[first - 1].fields["input_id"];
    let answer = |rows: &[Event]| {
        rows.iter()
            .filter(|row| row.what == "desire" && row.fields["parent_input_id"] == *input)
            .map(|row| (row.fields["to"].clone(), row.fields["message"].clone()))
            .collect::<Vec<_>>()
    };
    let (honest_answer, control_answer) = (answer(honest), answer(control));
    assert_eq!(honest_answer.len(), 6);
    assert_eq!(control_answer.len(), 3);
    for ((to, wrong), (honest_to, right)) in control_answer.iter().zip(&honest_answer) {
        assert_eq!(to, honest_to);
        assert_eq!(right["slot"], 0);
        assert_eq!(right["value"], "x");
        let mut expected = right.clone();
        expected["value"] = json!("y");
        assert_eq!(*wrong, expected);
    }
    // What the wrapper withheld is exactly the honest slot-1 broadcast of y.
    assert!(
        honest_answer[3..]
            .iter()
            .all(|(_, message)| message["slot"] == 1 && message["value"] == "y")
    );
}
