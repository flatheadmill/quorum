//! Receiver behavior is checked separately from the truth of its testimony.
use super::evidence::{Citations, Evidence, proposition};
use super::protocol::Message;
use crate::{Desire, DesireId, Input, InputId, NodeId, Slot};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

struct Response {
    expected: Option<Vec<Desire>>,
    completed_proposal: Option<DesireId>,
    invariant_failure: bool,
    support: Vec<usize>,
    gaps: Vec<String>,
    reason: &'static str,
}

fn response(
    e: &Evidence,
    id: InputId,
    learned: &BTreeMap<(NodeId, Slot), (Value, usize)>,
    completed_proposals: &BTreeMap<DesireId, usize>,
) -> Response {
    let input = &e.projection.inputs[&id];
    let node = input.to();
    let line = e.projection.input_lines[&id];
    let mut result = Response {
        expected: None,
        completed_proposal: None,
        invariant_failure: false,
        support: vec![1, line],
        gaps: vec![],
        reason: "The local receiver follows its declared role and input transition; knowledge evidence is evaluated independently.",
    };
    let Some(config) = e.config(node) else {
        result.gaps.push(format!("config:{}", node.0));
        return result;
    };
    let Input::Message { envelope, .. } = input else {
        result.expected = Some(vec![]);
        return result;
    };
    let Some(message) = e.input_messages.get(&id) else {
        unreachable!()
    };
    let Ok(message) = message else {
        result.gaps.push(e.finding_id_for_input(id));
        return result;
    };
    let (ballot, slot, value, announce) = match message {
        Message::Chosen {
            ballot,
            slot,
            value,
        } => {
            if !config.learners.contains(&node)
                || !config.proposers.contains(&envelope.from)
                || ballot.1 != envelope.from
                || *slot < config.from_slot
            {
                result.expected = Some(vec![]);
                return result;
            }
            (*ballot, *slot, value, false)
        }
        Message::Accepted {
            ballot,
            slot,
            value,
        } => {
            if !config.proposers.contains(&node)
                || !config.acceptors.contains(&envelope.from)
                || ballot.1 != node
                || *slot < config.from_slot
            {
                result.expected = Some(vec![]);
                return result;
            }
            let proposal=e.outputs.values().find(|output|output.node==node && output.close_line.is_some_and(|close|close<line)
                && matches!(&output.message,Some(Ok(Message::Accept{ballot:b,slot:s,value:v})) if b==ballot && s==slot && v==value));
            if let Some(proposal) = proposal {
                result
                    .support
                    .extend([proposal.desire_line, proposal.close_line.unwrap()]);
                if let Some(prior_line) = completed_proposals.get(&proposal.id) {
                    result.support.push(*prior_line);
                    result.expected = Some(vec![]);
                    result.reason = "An exact proposal announces only at its first Accepted quorum; later replies cannot repeat its Chosen broadcast.";
                    return result;
                }
            } else {
                result.gaps.extend(
                    e.outputs
                        .values()
                        .filter(|output| {
                            output.node == node
                                && output.close_line.is_some_and(|close| close < line)
                                && output.message.as_ref().is_some_and(Result::is_err)
                        })
                        .map(|output| format!("send:{}", output.id.0)),
                );
                if result.gaps.is_empty() {
                    result.expected = Some(vec![]);
                }
                return result;
            }
            let mut members = BTreeSet::new();
            for (receipt, input) in &e.projection.inputs {
                if input.to() != node || e.projection.input_lines[receipt] > line {
                    continue;
                }
                if let Input::Message { envelope, .. } = input
                    && config.acceptors.contains(&envelope.from)
                    && matches!(e.input_messages.get(receipt),Some(Ok(Message::Accepted{ballot:b,slot:s,value:v})) if b==ballot && s==slot && v==value)
                    && members.insert(envelope.from)
                {
                    result.support.push(e.projection.input_lines[receipt]);
                }
            }
            if members.len() < config.acceptors.len() / 2 + 1 {
                result.gaps.extend(e.input_gap_ids(node, line));
                if result.gaps.is_empty() {
                    result.expected = Some(vec![]);
                }
                return result;
            }
            result.completed_proposal = Some(proposal.unwrap().id);
            (*ballot, *slot, value, true)
        }
        _ => {
            result.expected = Some(vec![]);
            return result;
        }
    };
    let mut expected = Vec::new();
    if let Some((prior, prior_line)) = learned.get(&(node, slot)) {
        result.support.push(*prior_line);
        if prior != value {
            result.invariant_failure = true;
            result.reason = "A second value for a locally learned slot is an invariant failure, with no ordinary returned answer.";
            return result;
        }
        result.reason = "Learning suppresses another Commit for the same slot/value, but each newly completed proposal still announces to configured learners.";
    } else {
        expected.push(Desire::Commit {
            slot,
            entry: value.clone(),
        });
    }
    if announce {
        expected.extend(config.learners.iter().map(|to| Desire::Send {
            to: *to,
            message: json!({"type":"chosen","ballot":ballot,"slot":slot,"value":value}),
        }));
    }
    result.expected = Some(expected);
    result
}

pub(super) fn evaluate(e: &Evidence) -> (Vec<Value>, Vec<Value>) {
    let mut learned = BTreeMap::<(NodeId, Slot), (Value, usize)>::new();
    // A proposal's first quorum is distinct from closed slot/value assertions.
    // The first matching Accept desire identifies the exact proposal tuple.
    let mut completed_proposals = BTreeMap::<DesireId, usize>::new();
    let mut responses = BTreeMap::<InputId, Response>::new();
    let mut obligations = Vec::new();
    let mut observations = Vec::new();
    for (offset, row) in e.rows.iter().enumerate() {
        let line = offset + 1;
        if row.what == "dispatch" {
            let id: InputId = row.get("input_id").unwrap();
            let input = &e.projection.inputs[&id];
            if !e.claimed.contains(&input.to()) {
                continue;
            }
            let expected = response(e, id, &learned, &completed_proposals);
            if expected.invariant_failure {
                observations.push(json!({"kind":"contradictory_learning","node":input.to(),"line":line,"input_id":id,"lines":expected.support,"reason":expected.reason}));
            }
            responses.insert(id, expected);
        }
        if row.what == "close" {
            let id: InputId = row.get("input_id").unwrap();
            let answer = &e.projection.answers[&id];
            if !e.claimed.contains(&answer.node) {
                continue;
            }
            let received = matches!(
                e.input_messages.get(&id),
                Some(Ok(Message::Accepted { .. } | Message::Chosen { .. }))
            );
            if let Some(proposal) = responses[&id].completed_proposal {
                completed_proposals.insert(proposal, line);
            }
            if received {
                let expected = &responses[&id];
                let condition = if expected.invariant_failure {
                    Some(false)
                } else {
                    expected.expected.as_ref().map(|expected| {
                        answer
                            .desires
                            .iter()
                            .map(|(_, desire)| desire)
                            .eq(expected.iter())
                    })
                };
                obligations.push(proposition("receiver_conformance", "validity", json!({"node":answer.node,"input_id":id,"input_line":e.projection.input_lines[&id]}), line, verdict(condition), Citations { support: expected.support.iter().copied().chain([line]).collect(), counterevidence: if condition==Some(false) {expected.support.iter().copied().chain([line]).collect()} else {vec![]}, gaps: expected.gaps.clone() }, expected.reason));
            }
            for (output_id, desire) in &answer.desires {
                let Desire::Commit { slot, entry } = desire else {
                    continue;
                };
                let output = &e.outputs[output_id];
                if let Some((prior, prior_line)) = learned.get(&(answer.node, *slot)) {
                    let name = if prior == entry {
                        "learning_dedup"
                    } else {
                        "learning_consistency"
                    };
                    obligations.push(proposition(name, "validity", json!({"node":answer.node,"slot":slot,"value":entry,"desire_id":output.id}), line, if e.config(answer.node).is_some() {"violated"} else {"indeterminate"}, Citations { support: vec![*prior_line,output.desire_line,line], counterevidence: vec![*prior_line,output.desire_line,line], gaps: if e.config(answer.node).is_none() {vec![format!("config:{}",answer.node.0)]} else {vec![]} }, "A closed local learning assertion neither repeats an already learned slot/value nor replaces it with a different value."));
                } else {
                    learned.insert((answer.node, *slot), (entry.clone(), output.desire_line));
                }
            }
        }
        if row.what == "attempt" && row.why == "commit" {
            let output = &e.outputs[&row.get("desire_id").unwrap()];
            if !e.claimed.contains(&output.node) {
                continue;
            }
            let expected = &responses[&output.input];
            let condition = if expected.invariant_failure {
                Some(false)
            } else {
                expected
                    .expected
                    .as_ref()
                    .map(|desires| desires.contains(&output.desire))
            };
            obligations.push(proposition("receiver_conformance", "validity", json!({"node":output.node,"desire_id":output.id,"input_line":e.projection.input_lines[&output.input]}), line, verdict(condition), Citations { support: expected.support.iter().copied().chain([output.desire_line,output.close_line.unwrap(),line]).collect(), counterevidence: if condition==Some(false) {vec![e.projection.input_lines[&output.input],output.desire_line,line]} else {vec![]}, gaps: expected.gaps.clone() }, expected.reason));
        }
    }
    if let Some(id) = e.projection.account().active_input
        && e.projection
            .answers
            .get(&id)
            .is_some_and(|answer| !answer.closed)
        && matches!(
            e.input_messages.get(&id),
            Some(Ok(Message::Accepted { .. } | Message::Chosen { .. }))
        )
        && let Some(expected) = responses.get(&id)
    {
        obligations.push(proposition("receiver_conformance", "validity", json!({"node":e.projection.inputs[&id].to(),"input_id":id,"input_line":e.projection.input_lines[&id]}), e.last_line(), "pending", Citations { support: expected.support.clone(), counterevidence: vec![], gaps: expected.gaps.clone() }, "The receiver invocation is explicitly world-open; no returned answer is inferred."));
    }
    (obligations, observations)
}
fn verdict(condition: Option<bool>) -> &'static str {
    match condition {
        Some(true) => "established",
        Some(false) => "violated",
        None => "indeterminate",
    }
}
