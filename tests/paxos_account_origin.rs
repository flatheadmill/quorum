use quorum::paxos_account::examine;
use quorum::tape::{self, Event, Header, NodeSpec, WorldConfig};
use quorum::{Context, Desire, Input, Node, NodeId, Simulator};
use serde_json::{Value, json};
use std::{fs, io};

// These are recorded-answer fixtures, not an inhabitant or replay oracle.
struct Rule {
    from: Option<i32>,
    message: Value,
    answer: Vec<Desire>,
}
struct Script {
    rules: Vec<Rule>,
    crashed: bool,
}
impl Node for Script {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        let matching = self.rules.iter().position(|rule| match input {
            Input::Tick { .. } => rule.from.is_none(),
            Input::Message { envelope, .. } => {
                rule.from == Some(envelope.from.0) && rule.message == envelope.payload
            }
            Input::Client { .. } => false,
        });
        if let Some(index) = matching {
            for desire in self.rules.remove(index).answer {
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
struct Actor {
    algorithm: &'static str,
    declaration: Option<Value>,
    entries: Vec<Value>,
    before: Vec<Value>,
    after: Vec<Value>,
}
impl Actor {
    fn empty() -> Self {
        Self {
            algorithm: "multi-paxos",
            declaration: None,
            entries: vec![],
            before: vec![],
            after: vec![],
        }
    }
    fn report(round: u64, proposer: i32, value: &str) -> Self {
        Self {
            entries: vec![entry(0, round, proposer, value)],
            ..Self::empty()
        }
    }
    fn saved(mut self, round: u64, proposer: i32, value: &str) -> Self {
        self.before.push(acceptance(round, proposer, value));
        self
    }
}
fn entry(slot: u64, round: u64, proposer: i32, value: &str) -> Value {
    json!({"slot":slot,"ballot":[round,proposer],"value":value})
}
fn acceptance(round: u64, proposer: i32, value: &str) -> Value {
    json!({"record":"accepted","slot":0,"ballot":[round,proposer],"value":value})
}
fn send(to: i32, message: Value) -> Desire {
    Desire::Send {
        to: NodeId(to),
        message,
    }
}
fn config(acceptors: usize) -> Value {
    let members: Vec<_> = (0..=acceptors).collect();
    json!({
        "acceptors":(0..acceptors).collect::<Vec<_>>(),
        "learners":members,"proposers":members,"active_proposer":acceptors,
        "from_slot":0,"initial_round":8,"initial_state":"empty"
    })
}
fn fixture(actors: Vec<Actor>) -> Simulator {
    let count = actors.len();
    let proposer = count as i32;
    let prepare = json!({"type":"prepare","ballot":[8,proposer],"from_slot":0});
    let promises: Vec<_> = actors.iter().map(|actor| {
        json!({"type":"promise","ballot":[8,proposer],"from_slot":0,"accepted":actor.entries})
    }).collect();
    let mut specs: Vec<_> = actors
        .iter()
        .enumerate()
        .map(|(node, actor)| NodeSpec {
            node: NodeId(node as i32),
            algorithm: actor.algorithm.into(),
            config: actor.declaration.clone().unwrap_or_else(|| config(count)),
        })
        .collect();
    specs.push(NodeSpec {
        node: NodeId(proposer),
        algorithm: "multi-paxos".into(),
        config: config(count),
    });
    let header = Header::new(
        "paxos-account-origin",
        42,
        WorldConfig {
            base_latency: 1,
            jitter: 0,
            tick_interval: 1,
            deadlock_threshold: 200,
        },
        specs,
        (0..count).map(|node| NodeId(node as i32)).collect(),
    );
    let mut sim = Simulator::new(header, Box::new(io::sink())).unwrap();
    for (node, actor) in actors.into_iter().enumerate() {
        let mut answer: Vec<_> = actor
            .before
            .into_iter()
            .map(|record| Desire::Save { record })
            .collect();
        answer.push(send(proposer, promises[node].clone()));
        answer.extend(
            actor
                .after
                .into_iter()
                .map(|record| Desire::Save { record }),
        );
        sim.spawn(
            NodeId(node as i32),
            Box::new(Script {
                rules: vec![Rule {
                    from: Some(proposer),
                    message: prepare.clone(),
                    answer,
                }],
                crashed: false,
            }),
        )
        .unwrap();
    }
    sim.spawn(
        NodeId(proposer),
        Box::new(Script {
            rules: vec![
                Rule {
                    from: None,
                    message: Value::Null,
                    answer: (0..count)
                        .map(|node| send(node as i32, prepare.clone()))
                        .collect(),
                },
                Rule {
                    from: Some((count / 2) as i32),
                    message: promises[count / 2].clone(),
                    answer: vec![send(
                        0,
                        json!({"type":"accept","slot":0,"ballot":[8,proposer],"value":"x"}),
                    )],
                },
            ],
            crashed: false,
        }),
    )
    .unwrap();
    sim.run(3).unwrap();
    sim
}
fn captured(name: &str, rows: &[Event]) -> Value {
    let bytes = tape::encode(rows);
    let report = examine(&bytes);
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/paxos-account-evidence");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join(format!("{name}.jsonl")), &bytes).unwrap();
    fs::write(
        directory.join(format!("{name}.report.json")),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert!(report["world"]["error"].is_null(), "{report}");
    assert_eq!(report["world"]["valid_rows"], rows.len());
    assert_eq!(report["source_sha256"], tape::hash(&bytes));
    report
}
fn array<'a>(object: &'a Value, field: &str) -> &'a [Value] {
    object[field]
        .as_array()
        .unwrap_or_else(|| panic!("missing {field}: {object}"))
}
fn attempt(report: &Value, proposer: i32) -> &Value {
    array(report, "attempts")
        .iter()
        .find(|attempt| {
            attempt["proposer"] == proposer
                && attempt["slot"] == 0
                && attempt["ballot"] == json!([8, proposer])
                && attempt["value"] == "x"
        })
        .expect("adopted proposal attempt")
}
fn ancestry(report: &Value, proposer: i32) -> &Value {
    let origin = &attempt(report, proposer)["origin"];
    assert_eq!(origin["kind"], "adopted", "{origin}");
    assert!(origin["client_count"].is_null(), "{origin}");
    assert_eq!(
        origin["acceptance_ancestry"]["selection"]["verdict"],
        "established"
    );
    &origin["acceptance_ancestry"]
}
fn path(report: &Value, acceptor: i32) -> &Value {
    array(report, "paths")
        .iter()
        .find(|path| path["acceptor"] == acceptor)
        .expect("frozen report path")
}
fn line(rows: &[Event], predicate: impl Fn(&Event) -> bool) -> usize {
    rows.iter()
        .position(predicate)
        .expect("original world boundary")
        + 1
}
fn report_line(rows: &[Event], from: i32) -> usize {
    line(rows, |row| {
        row.what == "dispatch"
            && row.why == "message"
            && row.fields["from"] == from
            && row.fields["message"]["type"] == "promise"
    })
}
fn assert_cited(object: &Value, field: &str, lines: &[usize]) {
    for &line in lines {
        assert!(
            line > 0 && array(object, field).contains(&json!(line)),
            "missing {line} in {object}"
        );
    }
}
fn assert_original_boundaries(
    rows: &[Event],
    traced: &Value,
    acceptor: i32,
    record: &Value,
) -> usize {
    let dispatch = report_line(rows, acceptor);
    let desire = line(rows, |row| {
        row.what == "desire"
            && row.why == "send"
            && row.fields["node"] == acceptor
            && row.fields["message"]["type"] == "promise"
    });
    let desire_id = &rows[desire - 1].fields["desire_id"];
    let input_id = &rows[desire - 1].fields["parent_input_id"];
    let close = line(rows, |row| {
        row.what == "close" && &row.fields["input_id"] == input_id
    });
    let admission = line(rows, |row| {
        row.what == "attempt" && &row.fields["desire_id"] == desire_id
    });
    let result = line(rows, |row| {
        row.what == "result" && &row.fields["desire_id"] == desire_id
    });
    assert_eq!(traced["report_dispatch_line"], dispatch);
    assert_eq!(traced["promise_source"]["desire_line"], desire);
    assert_eq!(traced["promise_source"]["close_line"], close);
    assert_eq!(traced["promise_source"]["attempt_line"], admission);
    assert_eq!(traced["promise_source"]["send_admission_line"], admission);
    assert_eq!(traced["promise_source"]["result_line"], result);
    let save_result = line(rows, |row| {
        row.what == "result"
            && row.why == "save"
            && row.fields["node"] == acceptor
            && &row.fields["record"] == record
    });
    let save_id = &rows[save_result - 1].fields["desire_id"];
    let save_desire = line(rows, |row| {
        row.what == "desire" && &row.fields["desire_id"] == save_id
    });
    let save_attempt = line(rows, |row| {
        row.what == "attempt" && &row.fields["desire_id"] == save_id
    });
    assert_eq!(array(traced, "matching_saves").len(), 1);
    let saved = &traced["matching_saves"][0];
    assert_eq!(saved["desire_line"], save_desire);
    assert_eq!(saved["close_line"], close);
    assert_eq!(saved["attempt_line"], save_attempt);
    assert_eq!(saved["result_line"], save_result);
    assert!(save_result < admission && admission < dispatch);
    assert_cited(
        traced,
        "support",
        &[
            save_desire,
            close,
            save_attempt,
            save_result,
            desire,
            admission,
            result,
            dispatch,
        ],
    );
    save_result
}
fn assert_exemption(report: &Value, proposer: i32) {
    let exemption = array(report, "obligations")
        .iter()
        .find(|p| p["proposition"] == "fresh_origin_count" && p["subject"]["node"] == proposer)
        .expect("independent adopted client exemption");
    assert_eq!(exemption["verdict"], "established", "{exemption}");
    assert!(array(exemption, "gaps").is_empty(), "{exemption}");
}

#[test]
fn frozen_script_reports_trace_original_saves_without_choice_or_late_reports() {
    let mut first = Actor::report(2, 3, "x").saved(2, 3, "x");
    first.algorithm = "answer-script";
    // The helper covers the adopted slot, not unrelated suffix assertions.
    first
        .entries
        .push(entry(1, 2, 3, "unbacked-unrelated-slot"));
    let mut second = Actor::report(1, 3, "other").saved(1, 3, "other");
    second.algorithm = "answer-script";
    let sim = fixture(vec![first, second, Actor::report(7, 3, "late")]);
    let report = captured("origin-script-positive-frozen-history", sim.rows());
    assert!(array(&report, "choices").is_empty());
    let history = ancestry(&report, 3);
    assert_eq!(history["verdict"], "established", "{history}");
    assert_eq!(array(history, "paths").len(), 2);
    assert_eq!(path(history, 0)["selected"], true);
    assert_eq!(path(history, 1)["selected"], false);
    assert_eq!(path(history, 1)["ballot"], json!([1, 3]));
    assert_eq!(path(history, 1)["value"], "other");
    assert_eq!(history["selection"]["ballot"], json!([2, 3]));
    let first_save =
        assert_original_boundaries(sim.rows(), path(history, 0), 0, &acceptance(2, 3, "x"));
    let second_save =
        assert_original_boundaries(sim.rows(), path(history, 1), 1, &acceptance(1, 3, "other"));
    assert_cited(history, "support", &[first_save, second_save]);
    assert_cited(
        &attempt(&report, 3)["origin"],
        "support",
        &[first_save, second_save],
    );
    assert_eq!(
        history["frozen_report_dispatch_lines"],
        json!([report_line(sim.rows(), 0), report_line(sim.rows(), 1)])
    );
    let late = report_line(sim.rows(), 2);
    assert!(!array(history, "support").contains(&json!(late)));
    assert!(array(history, "counterevidence").is_empty());
    assert!(array(history, "gaps").is_empty());
    assert_exemption(&report, 3);

    // Once the first allocation is closed, later dispatches do not change its
    // immutable ancestry. No later Promise is consulted even in the full tape.
    let close = attempt(&report, 3)["origin"]["allocation_line"]
        .as_u64()
        .unwrap() as usize;
    let prefix = captured(
        "origin-script-positive-before-late-report",
        &sim.rows()[..close],
    );
    assert_eq!(ancestry(&prefix, 3), history);
}

#[test]
fn covered_missing_and_later_acceptance_do_not_repair_earlier_promise_admission() {
    for late in [false, true] {
        let mut first = Actor::report(2, 3, "x");
        if late {
            first.after.push(acceptance(2, 3, "x"));
        }
        let sim = fixture(vec![first, Actor::empty(), Actor::empty()]);
        let name = if late {
            "origin-covered-later-save-nonrepair"
        } else {
            "origin-covered-missing-save"
        };
        let report = captured(name, sim.rows());
        let history = ancestry(&report, 3);
        assert_eq!(history["verdict"], "violated", "{history}");
        let traced = path(history, 0);
        assert_eq!(traced["verdict"], "violated");
        assert_eq!(
            traced["reason"],
            "matching_acceptance_not_fulfilled_before_promise_admission"
        );
        assert!(array(traced, "matching_saves").is_empty());
        assert!(array(history, "gaps").is_empty());
        assert_eq!(
            array(traced, "later_matching_saves").len(),
            usize::from(late)
        );
        if late {
            let saved = &traced["later_matching_saves"][0];
            let result = saved["result_line"].as_u64().unwrap() as usize;
            let admission = traced["promise_source"]["send_admission_line"]
                .as_u64()
                .unwrap() as usize;
            assert!(admission < result && result < report_line(sim.rows(), 0));
            assert_eq!(sim.rows()[result - 1].fields["outcome"], "fulfilled");
            assert_cited(
                traced,
                "counterevidence",
                &[result, admission, report_line(sim.rows(), 0)],
            );
        }
        assert_exemption(&report, 3);
    }
}

#[test]
fn unavailable_initial_or_fulfilled_save_history_stays_explicit_without_revoking_adoption() {
    for kind in ["script", "malformed-config", "unreadable-save"] {
        let mut first = Actor::report(2, 3, "x");
        let expected_reason = match kind {
            "script" => {
                first.algorithm = "answer-script";
                "undeclared_initial_acceptance_history"
            }
            "malformed-config" => {
                let mut declaration = config(3);
                declaration["initial_state"] = json!("unavailable");
                first.declaration = Some(declaration);
                "malformed_configuration_leaves_initial_acceptance_history_unknown"
            }
            _ => {
                first.before.push(
                    json!({"record":"unreadable_acceptance","slot":0,"ballot":[2,3],"value":"x"}),
                );
                "unreadable_fulfilled_save_before_promise_admission"
            }
        };
        let sim = fixture(vec![first, Actor::empty(), Actor::empty()]);
        let report = captured(&format!("origin-unknown-{kind}"), sim.rows());
        let history = ancestry(&report, 3);
        assert_eq!(history["verdict"], "indeterminate", "{history}");
        let traced = path(history, 0);
        assert_eq!(traced["verdict"], "indeterminate");
        assert_eq!(traced["reason"], expected_reason);
        assert!(array(traced, "matching_saves").is_empty());
        if kind == "script" {
            assert!(array(history, "gaps").is_empty());
        } else {
            assert_eq!(array(history, "gaps").len(), 1);
            for gap in array(history, "gaps") {
                assert!(
                    array(&report, "interpretation_findings")
                        .iter()
                        .any(|finding| finding["id"] == *gap)
                );
                assert!(array(&attempt(&report, 3)["origin"], "gaps").contains(gap));
            }
        }
        assert_exemption(&report, 3);
    }
}

#[test]
fn positive_acceptance_join_does_not_require_known_initial_state_or_readable_other_saves() {
    for malformed_config in [false, true] {
        let mut first = Actor::report(2, 3, "x").saved(2, 3, "x");
        first
            .before
            .insert(0, json!({"record":"unreadable_acceptance"}));
        if malformed_config {
            first.declaration = Some(json!({"not":"a Multi-Paxos initial-state declaration"}));
        } else {
            first.algorithm = "answer-script";
        }
        let sim = fixture(vec![first, Actor::empty(), Actor::empty()]);
        let name = if malformed_config {
            "origin-positive-malformed-initial"
        } else {
            "origin-positive-script-unreadable-history"
        };
        let report = captured(name, sim.rows());
        assert!(!array(&report, "interpretation_findings").is_empty());
        let history = ancestry(&report, 3);
        assert_eq!(history["verdict"], "established", "{history}");
        assert!(array(history, "gaps").is_empty());
        let save =
            assert_original_boundaries(sim.rows(), path(history, 0), 0, &acceptance(2, 3, "x"));
        assert_cited(&attempt(&report, 3)["origin"], "support", &[save]);
        assert_exemption(&report, 3);
    }
}

#[test]
fn lower_ballot_competing_entries_keep_every_failed_exact_acceptance_join() {
    let selected = Actor::report(3, 5, "x").saved(3, 5, "x");
    // Neither an acceptance at another ballot nor one of another value backs
    // the reported entry, even when all these Saves actually fulfilled.
    let lower_one = Actor::report(1, 5, "y").saved(2, 5, "y");
    let lower_two = Actor::report(2, 5, "z").saved(2, 5, "y");
    let sim = fixture(vec![
        selected,
        lower_one,
        lower_two,
        Actor::report(7, 5, "late"),
        Actor::empty(),
    ]);
    let report = captured("origin-lower-ballot-all-failed-paths", sim.rows());
    let history = ancestry(&report, 5);
    assert_eq!(history["verdict"], "violated", "{history}");
    assert_eq!(history["selection"]["ballot"], json!([3, 5]));
    assert_eq!(array(history, "paths").len(), 3);
    assert_eq!(path(history, 0)["selected"], true);
    assert_eq!(path(history, 0)["verdict"], "established");
    assert_original_boundaries(sim.rows(), path(history, 0), 0, &acceptance(3, 5, "x"));
    for (acceptor, round, value) in [(1, 1, "y"), (2, 2, "z")] {
        let failed = path(history, acceptor);
        assert_eq!(failed["selected"], false);
        assert_eq!(failed["ballot"], json!([round, 5]));
        assert_eq!(failed["value"], value);
        assert_eq!(failed["verdict"], "violated");
        assert!(array(failed, "matching_saves").is_empty());
        assert_cited(
            history,
            "counterevidence",
            &[report_line(sim.rows(), acceptor)],
        );
    }
    assert!(array(&report, "choices").is_empty());
    assert_exemption(&report, 5);
}
