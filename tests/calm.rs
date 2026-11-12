use quorum::projection::{Projection, Tape};
use quorum::replay::{self, Mode};
use quorum::tape::{self, Event};
use quorum::{Desire, InputId, NodeId, Slot, scenarios};
use serde_json::json;
use std::{collections::BTreeSet, io};

fn messages<'a>(rows: &'a [Event], what: &str, kind: &str) -> Vec<&'a Event> {
    let boundary = if what == "dispatch" {
        "message"
    } else {
        "send"
    };
    rows.iter()
        .filter(|row| {
            row.what == what && row.why == boundary && row.fields["message"]["type"] == kind
        })
        .collect()
}

fn broadcast(kind: &str, slot: u64, value: &str) -> Vec<Desire> {
    (0..3)
        .map(|node| Desire::Send {
            to: NodeId(node),
            message: json!({"type":kind,"ballot":[1,0],"slot":slot,"value":value}),
        })
        .collect()
}

#[test]
fn calm_has_one_campaign_then_choice_before_knowledge_and_two_learning_paths() {
    let sim = scenarios::capture("calm", Box::new(io::sink())).unwrap();
    let rows = sim.rows();
    let projection = sim.check_projection().unwrap();
    let commands = ["alpha", "beta", "gamma"];
    let clients: Vec<_> = rows
        .iter()
        .filter(|row| row.what == "external" && row.why == "client")
        .collect();
    assert_eq!(clients.len(), 3);
    for (row, value) in clients.iter().zip(commands) {
        assert_eq!(row.when, 0);
        assert_eq!(row.fields["to"], 0);
        assert_eq!(
            row.fields["message"],
            json!({"type":"propose","value":value})
        );
    }
    let client_dispatches: Vec<_> = rows
        .iter()
        .filter(|row| row.what == "dispatch" && row.why == "client")
        .collect();
    assert_eq!(client_dispatches.len(), 3);
    for (row, value) in client_dispatches.iter().zip(commands) {
        assert_eq!(row.when, 0);
        assert_eq!(row.fields["to"], 0);
        assert_eq!(
            row.fields["message"],
            json!({"type":"propose","value":value})
        );
        let id = row.get::<InputId>("input_id").unwrap();
        assert!(projection.answers[&id].desires.is_empty());
    }

    let first_tick = rows
        .iter()
        .find(|row| row.what == "dispatch" && row.why == "tick" && row.fields["to"] == 0)
        .unwrap();
    assert_eq!(first_tick.when, 1);
    let campaign = first_tick.get::<InputId>("input_id").unwrap();
    let prepares = messages(rows, "result", "prepare");
    assert_eq!(prepares.len(), 3);
    for (row, to) in prepares.iter().zip(0..3) {
        assert_eq!(row.when, 1);
        assert_eq!(row.fields["node"], 0);
        assert_eq!(row.fields["to"], to);
        assert_eq!(row.get::<InputId>("parent_input_id").unwrap(), campaign);
        assert_eq!(
            row.fields["message"],
            json!({"type":"prepare","ballot":[1,0],"from_slot":0})
        );
    }
    assert_eq!(projection.answers[&campaign].desires.len(), 3);
    let promises = messages(rows, "result", "promise");
    assert_eq!(promises.len(), 3);
    for (row, node) in promises.iter().zip(0..3) {
        assert_eq!(row.when, 2);
        assert_eq!(row.fields["node"], node);
        assert_eq!(row.fields["to"], 0);
        assert_eq!(
            row.fields["message"],
            json!({"type":"promise","ballot":[1,0],"from_slot":0,"accepted":[]})
        );
        let promised = rows
            .iter()
            .find(|save| {
                save.what == "result"
                    && save.why == "save"
                    && save.fields["node"] == node
                    && save.fields["record"]
                        == json!({"record":"promised","from_slot":0,"ballot":[1,0]})
            })
            .unwrap();
        assert_eq!(promised.when, 2);
        assert!(promised.seq < row.seq);
    }

    let promise_inputs = messages(rows, "dispatch", "promise");
    assert_eq!(promise_inputs.len(), 3);
    for (row, from) in promise_inputs.iter().zip(0..3) {
        assert_eq!(row.when, 3);
        assert_eq!(row.fields["from"], from);
    }
    let first = promise_inputs[0].get::<InputId>("input_id").unwrap();
    let quorum = promise_inputs[1].get::<InputId>("input_id").unwrap();
    let late = promise_inputs[2].get::<InputId>("input_id").unwrap();
    assert!(projection.answers[&first].desires.is_empty());
    assert!(projection.answers[&late].desires.is_empty());
    let expected_accepts: Vec<_> = commands
        .iter()
        .enumerate()
        .flat_map(|(slot, value)| broadcast("accept", slot as u64, value))
        .collect();
    assert_eq!(
        projection.answers[&quorum]
            .desires
            .iter()
            .map(|(_, desire)| desire.clone())
            .collect::<Vec<_>>(),
        expected_accepts
    );
    let accepts = messages(rows, "result", "accept");
    assert_eq!(accepts.len(), 9);
    for row in accepts {
        assert_eq!(row.when, 3);
        assert_eq!(row.fields["node"], 0);
        assert_eq!(row.get::<InputId>("parent_input_id").unwrap(), quorum);
    }
    let accept_inputs = messages(rows, "dispatch", "accept");
    assert_eq!(accept_inputs.len(), 9);
    assert!(accept_inputs.iter().all(|row| row.when == 4));
    let accepted_saves: Vec<_> = rows
        .iter()
        .filter(|row| {
            row.what == "result"
                && row.why == "save"
                && row.fields["record"]["record"] == "accepted"
        })
        .collect();
    assert_eq!(accepted_saves.len(), 9);
    assert!(accepted_saves.iter().all(|row| row.when == 4));
    let accepted = messages(rows, "result", "accepted");
    assert_eq!(accepted.len(), 9);
    assert!(accepted.iter().all(|row| row.when == 4));

    // In this declared empty-state history, these two fulfilled records are
    // choice support before any Accepted reply reaches the proposer. This is a
    // fixed-history observation, not a general certificate verifier.
    let alpha_saves: Vec<_> = accepted_saves
        .iter()
        .filter(|row| {
            row.fields["record"]
                == json!({"record":"accepted","slot":0,"ballot":[1,0],"value":"alpha"})
        })
        .collect();
    assert_eq!(alpha_saves.len(), 3);
    let choice_end = alpha_saves[1].seq as usize + 1;
    let choice = Projection::from_rows(&rows[..choice_end]).unwrap();
    assert_eq!(choice.tick, 4);
    let supporting_acceptors: BTreeSet<_> = alpha_saves[..2]
        .iter()
        .map(|row| row.get::<NodeId>("node").unwrap())
        .collect();
    assert_eq!(supporting_acceptors, BTreeSet::from([NodeId(0), NodeId(1)]));
    for node in supporting_acceptors {
        assert!(
            choice.persistent[&node]
                .contains(&json!({"record":"promised","from_slot":0,"ballot":[1,0]}))
        );
        assert!(
            choice.persistent[&node]
                .contains(&json!({"record":"accepted","slot":0,"ballot":[1,0],"value":"alpha"}))
        );
    }
    assert!(choice.chosen.values().all(|claims| claims.is_empty()));
    assert!(messages(&rows[..choice_end], "dispatch", "accepted").is_empty());
    assert!(!rows[..choice_end].iter().any(|row| row.why == "commit"));

    let accepted_inputs = messages(rows, "dispatch", "accepted");
    assert_eq!(accepted_inputs.len(), 9);
    assert!(accepted_inputs.iter().all(|row| row.when == 5));
    for (slot, value) in commands.iter().enumerate() {
        let replies: Vec<_> = accepted_inputs
            .iter()
            .filter(|row| row.fields["message"]["slot"] == slot)
            .collect();
        assert_eq!(replies.len(), 3);
        assert_eq!(replies[0].fields["from"], 0);
        assert_eq!(replies[1].fields["from"], 1);
        let first = replies[0].get::<InputId>("input_id").unwrap();
        let known = replies[1].get::<InputId>("input_id").unwrap();
        let extra = replies[2].get::<InputId>("input_id").unwrap();
        assert!(projection.answers[&first].desires.is_empty());
        assert!(projection.answers[&extra].desires.is_empty());
        let mut expected = vec![Desire::Commit {
            slot: Slot(slot as u64),
            entry: json!(value),
        }];
        expected.extend(broadcast("chosen", slot as u64, value));
        assert_eq!(
            projection.answers[&known]
                .desires
                .iter()
                .map(|(_, desire)| desire.clone())
                .collect::<Vec<_>>(),
            expected
        );
    }
    let chosen_sends = messages(rows, "result", "chosen");
    assert_eq!(chosen_sends.len(), 9);
    assert!(chosen_sends.iter().all(|row| row.when == 5));
    let chosen_inputs = messages(rows, "dispatch", "chosen");
    assert_eq!(chosen_inputs.len(), 9);
    assert!(chosen_inputs.iter().all(|row| row.when == 6));
    let self_chosen: Vec<_> = chosen_inputs
        .iter()
        .filter(|row| row.fields["from"] == 0 && row.fields["to"] == 0)
        .collect();
    assert_eq!(self_chosen.len(), 3);
    for row in self_chosen {
        let id = row.get::<InputId>("input_id").unwrap();
        assert!(projection.answers[&id].desires.is_empty());
    }
    let commits: Vec<_> = rows
        .iter()
        .filter(|row| row.what == "result" && row.why == "commit")
        .collect();
    assert_eq!(commits.len(), 9);
    for node in 0..3 {
        let claims: Vec<_> = commits
            .iter()
            .filter(|row| row.fields["node"] == node)
            .collect();
        assert_eq!(claims.len(), 3);
        for (row, (slot, value)) in claims.iter().zip(commands.iter().enumerate()) {
            assert_eq!(row.when, if node == 0 { 5 } else { 6 });
            assert_eq!(row.fields["slot"], slot);
            assert_eq!(row.fields["entry"], json!(value));
            assert_eq!(
                sim.chosen()[&NodeId(node)][&Slot(slot as u64)],
                json!(value)
            );
        }
    }
    assert!(!rows.iter().any(|row| row.why == "commit" && row.when < 5));
    assert_eq!(
        rows.iter()
            .filter(|row| row.what == "result" && row.why == "send")
            .count(),
        33
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.what == "result" && row.why == "save")
            .count(),
        12
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.what == "advance")
            .map(|row| row.when)
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert_eq!(sim.tick(), 8);
    assert!(sim.pending().is_empty());
    let final_row = rows.last().unwrap();
    assert_eq!(final_row.what, "finalize");
    assert_eq!(final_row.when, 8);
    assert_eq!(final_row.fields["reason"], "horizon");
    let empty_account = json!({
        "active_input":null,
        "unreached_desires":[],
        "open_attempts":[],
        "queued":[],
        "arrived":[],
        "clients":[],
        "due_messages":[],
        "due_ticks":[],
        "no_progress_due":null,
    });
    assert_eq!(final_row.fields["account"], empty_account);
    assert_eq!(
        serde_json::to_value(projection.account()).unwrap(),
        empty_account
    );
    assert_eq!(projection.tail(), "finalized");
}

#[test]
fn calm_captures_are_identical_strictly_replayable_and_prefix_interpretable() {
    let a = scenarios::capture("calm", Box::new(io::sink())).unwrap();
    let b = scenarios::capture("calm", Box::new(io::sink())).unwrap();
    let bytes = tape::encode(a.rows());
    assert_eq!(bytes, tape::encode(b.rows()));
    let tape = Tape::read(&bytes).unwrap();
    let report = replay::replay(&tape, Mode::Strict, scenarios::factory).unwrap();
    assert_eq!(report.source_sha256, tape.sha256);
    assert_eq!(report.completed_answers, tape.projection.inputs.len());
    assert_eq!(report.dispatched_inputs, report.completed_answers);
    assert_eq!(report.uncertified_answers, 0);
    assert_eq!(report.admitted_results, tape.projection.effects);
    assert_eq!(report.tail, "finalized");
    let mut tails = BTreeSet::from([Tape::read(&[]).unwrap().projection.tail()]);
    let mut prefixes = 1;
    for (offset, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            let prefix = Tape::read(&bytes[..=offset])
                .unwrap_or_else(|error| panic!("prefix through line {prefixes}: {error}"));
            tails.insert(prefix.projection.tail());
            prefixes += 1;
        }
    }
    assert_eq!(prefixes, tape.rows.len() + 1);
    for tail in [
        "empty",
        "awaiting_dispatch",
        "open_invocation",
        "partial_answer",
        "interrupted_drain",
        "open_effect",
        "finalized",
    ] {
        assert!(tails.contains(tail), "missing prefix classification {tail}");
    }
}
