//! Check the account's explanatory coverage, not whether a protocol permits a
//! witnessed conflict. Only the two first certificates' causal histories count.
use super::evidence::{Citations, Evidence, proposition};
use super::protocol::{Message, Record};
use crate::{DesireId, Input, InputId, NodeId, Slot};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Default)]
struct History {
    outputs: BTreeSet<DesireId>,
    inputs: BTreeSet<InputId>,
    lines: BTreeSet<usize>,
    nodes: BTreeSet<NodeId>,
}

fn history(e: &Evidence, pair: [&Value; 2], observations: &[Value], boundary: usize) -> History {
    let slot = Slot(pair[0]["slot"].as_u64().unwrap());
    let mut history = History::default();
    let mut queue = VecDeque::new();
    for choice in pair {
        for witness in choice["first_support"].as_array().unwrap() {
            let line = witness["save_line"].as_u64().unwrap() as usize;
            let acceptance = e
                .acceptances
                .iter()
                .find(|acceptance| acceptance.line == line)
                .unwrap();
            queue.push_back(acceptance.output);
        }
    }
    let closes: BTreeMap<InputId, usize> = e
        .rows
        .iter()
        .enumerate()
        .filter(|(offset, row)| *offset < boundary && row.what == "close")
        .map(|(offset, row)| (row.get("input_id").unwrap(), offset + 1))
        .collect();
    let input_at: BTreeMap<_, _> = e
        .projection
        .input_lines
        .iter()
        .map(|(id, line)| (*line, *id))
        .collect();
    while let Some(id) = queue.pop_front() {
        if !history.outputs.insert(id) {
            continue;
        }
        let output = &e.outputs[&id];
        history.nodes.insert(output.node);
        history.lines.insert(output.desire_line);
        history.lines.extend(
            [output.close_line, output.attempt_line, output.result_line]
                .into_iter()
                .flatten()
                .filter(|line| *line <= boundary),
        );
        let mut inputs = vec![output.input];
        if let Some(Ok(Message::Accept { ballot, .. })) = &output.message {
            let parent_line = e.projection.input_lines[&output.input];
            for observation in observations {
                if observation["kind"] == "first_promise_quorum"
                    && observation["node"] == json!(output.node)
                    && observation["ballot"] == json!(ballot)
                    && observation["line"]
                        .as_u64()
                        .is_some_and(|line| line as usize <= parent_line)
                {
                    for line in observation["dispatch_lines"]
                        .as_array()
                        .into_iter()
                        .flatten()
                    {
                        if let Some(id) = line
                            .as_u64()
                            .and_then(|line| input_at.get(&(line as usize)))
                        {
                            inputs.push(*id);
                        }
                    }
                }
            }
        }
        for input_id in inputs {
            if !history.inputs.insert(input_id) {
                continue;
            }
            let input = &e.projection.inputs[&input_id];
            history.nodes.insert(input.to());
            history.lines.extend(closes.get(&input_id).copied());
            history.lines.insert(e.projection.input_lines[&input_id]);
            if let Input::Message { envelope, .. } = input {
                history.nodes.insert(envelope.from);
                queue.push_back(envelope.parent_desire_id);
            }
        }
        let state_boundary = match (&output.record, &output.message) {
            (
                Some(Ok(Record::Accepted {
                    slot: accepted_slot,
                    ..
                })),
                _,
            ) if *accepted_slot == slot => output.result_line,
            (_, Some(Ok(Message::Promise { .. }))) => output.attempt_line,
            _ => None,
        };
        if let Some(state_boundary) = state_boundary {
            // State used by these assertions, not all effects of a participant.
            if let Some(watermark) = e
                .outputs
                .values()
                .filter(|prior| {
                    prior.node == output.node
                        && prior.outcome.as_deref() == Some("fulfilled")
                        && prior.result_line.is_some_and(|line| line < state_boundary)
                        && matches!(&prior.record, Some(Ok(Record::Promised { .. })))
                })
                .max_by_key(|prior| prior.result_line)
            {
                queue.push_back(watermark.id);
            }
            for acceptance in &e.acceptances {
                if acceptance.node == output.node
                    && acceptance.slot == slot
                    && acceptance.line < state_boundary
                {
                    queue.push_back(acceptance.output);
                }
            }
        }
    }
    history
}

fn coverage_gaps(e: &Evidence, history: &History, boundary: usize) -> Vec<String> {
    let mut gaps = BTreeSet::new();
    for &node in &history.nodes {
        if e.config(node).is_none() {
            gaps.extend(
                e.findings
                    .iter()
                    .filter(|finding| {
                        finding["kind"] == "configuration" && finding["node"] == json!(node)
                    })
                    .filter_map(|finding| finding["id"].as_str().map(str::to_owned)),
            );
        }
        gaps.extend(e.input_gap_ids(node, boundary + 1));
        gaps.extend(e.save_gap_ids(node, boundary + 1));
        for output in e.outputs.values() {
            if output.node == node
                && output.close_line.is_some_and(|line| line <= boundary)
                && output.message.as_ref().is_some_and(Result::is_err)
            {
                let id = format!("send:{}", output.id.0);
                if e.findings.iter().any(|finding| finding["id"] == id) {
                    gaps.insert(id);
                }
            }
        }
    }
    gaps.into_iter().collect()
}

fn unavailable_configurations(e: &Evidence, history: &History, nodes: &[NodeId]) -> Vec<Value> {
    nodes
        .iter()
        .map(|node| {
            let mut support = BTreeSet::from([1]);
            for id in &history.outputs {
                let output = &e.outputs[id];
                if output.node == *node {
                    support.extend(
                        [
                            Some(output.desire_line),
                            output.close_line,
                            output.attempt_line,
                            output.result_line,
                        ]
                        .into_iter()
                        .flatten()
                        .filter(|line| history.lines.contains(line)),
                    );
                }
            }
            for input in &history.inputs {
                let event = &e.projection.inputs[input];
                if event.to() == *node
                    || matches!(event, Input::Message { envelope, .. } if envelope.from == *node)
                {
                    support.insert(e.projection.input_lines[input]);
                }
            }
            let gaps: Vec<_> = e
                .findings
                .iter()
                .filter(|finding| {
                    finding["kind"] == "configuration" && finding["node"] == json!(node)
                })
                .map(|finding| finding["id"].clone())
                .collect();
            json!({"premise":"configuration","nodes":[node],"support":support,
                "counterevidence":[],"gaps":gaps})
        })
        .collect()
}

fn suffix_failure_affects_slot(e: &Evidence, node: NodeId, line: usize, slot: Slot) -> bool {
    let Some(output) = e
        .outputs
        .values()
        .find(|output| output.node == node && output.attempt_line == Some(line))
    else {
        return false;
    };
    let Some(Ok(Message::Promise { accepted, .. })) = &output.message else {
        return false;
    };
    let expected = e
        .acceptances
        .iter()
        .filter(|acceptance| {
            acceptance.node == node && acceptance.slot == slot && acceptance.line < line
        })
        .max_by_key(|acceptance| (acceptance.ballot, acceptance.line));
    let mut actual = accepted.iter().filter(|entry| entry.slot == slot);
    match (expected, actual.next(), actual.next()) {
        (Some(expected), Some(actual), None) => {
            actual.ballot != expected.ballot || actual.value != expected.value
        }
        (None, None, _) => false,
        _ => true,
    }
}

fn response_failure_affects_slot(e: &Evidence, input: InputId, slot: Slot) -> bool {
    let request = e
        .input_messages
        .get(&input)
        .and_then(|message| message.as_ref().ok());
    e.projection.answers[&input].desires.iter().any(|(id, _)| {
        let Some(Ok(Record::Accepted { slot: accepted_slot, ballot, value })) = &e.outputs[id].record else { return false; };
        *accepted_slot == slot && !matches!(request, Some(Message::Accept { slot: requested_slot, ballot: requested_ballot, value: requested_value })
            if requested_slot == accepted_slot && requested_ballot == ballot && requested_value == value)
    })
}

fn explains(e: &Evidence, p: &Value, history: &History, slot: Slot, boundary: usize) -> bool {
    if p["verdict"] != "violated" {
        return false;
    }
    let Some(line) = p["boundary_line"].as_u64().map(|line| line as usize) else {
        return false;
    };
    if line > boundary {
        return false;
    }
    let subject = &p["subject"];
    if subject
        .get("slot")
        .is_some_and(|candidate| candidate.as_u64() != Some(slot.0))
    {
        return false;
    }
    let Some(node) = subject["node"]
        .as_i64()
        .and_then(|node| i32::try_from(node).ok())
        .map(NodeId)
    else {
        return false;
    };
    if !history.nodes.contains(&node) {
        return false;
    }
    // Header-universe agreement is a premise of the actual participants'
    // acceptance history, even though its declaration predates every answer.
    // Pairwise configuration differences and unrelated actors do not explain
    // the creation of these two header-majority certificates.
    if p["proposition"] == "config_agreement" {
        return p["category"] == "configuration"
            && subject["other"] == "header.acceptors"
            && e.config(node)
                .is_some_and(|config| config.acceptors != e.header_acceptors);
    }
    if !matches!(p["category"].as_str(), Some("agreement" | "validity"))
        || !history.lines.contains(&line)
    {
        return false;
    }
    match p["proposition"].as_str() {
        Some(
            "phase2_gate"
            | "adoption"
            | "proposal_immutability"
            | "proposer_authority"
            | "acceptor_watermark"
            | "accepted_watermark"
            | "accepted_consistency"
            | "acceptor_authority"
            | "promise_watermark"
            | "promise_report_handling",
        ) => true,
        Some("promise_suffix") => suffix_failure_affects_slot(e, node, line, slot),
        Some("acceptor_response") => {
            subject["input_id"]
                .as_u64()
                .map(InputId)
                .is_some_and(|input| {
                    history.inputs.contains(&input) && response_failure_affects_slot(e, input, slot)
                })
        }
        // Reply support, request conformance and learning rules concern received
        // knowledge, not the creation of conflicting acceptance certificates.
        _ => false,
    }
}

pub(super) fn evaluate(
    e: &Evidence,
    choices: &[Value],
    obligations: &[Value],
    observations: &[Value],
) -> (Vec<Value>, Vec<Value>) {
    let mut checks = Vec::new();
    let mut findings = Vec::new();
    for (index, later) in choices.iter().enumerate() {
        for earlier in &choices[..index] {
            if earlier["slot"] != later["slot"] || earlier["value"] == later["value"] {
                continue;
            }
            let boundary = later["first_quorum_line"].as_u64().unwrap() as usize;
            let slot = Slot(later["slot"].as_u64().unwrap());
            let history = history(e, [earlier, later], observations, boundary);
            let gaps = coverage_gaps(e, &history, boundary);
            let undeclared: Vec<_> = history
                .nodes
                .iter()
                .copied()
                .filter(|node| e.config(*node).is_none())
                .collect();
            let complete = undeclared.is_empty() && gaps.is_empty();
            let explanations: Vec<_> = obligations
                .iter()
                .filter(|p| explains(e, p, &history, slot, boundary))
                .collect();
            let verdict = if !explanations.is_empty() {
                "established"
            } else if complete {
                "violated"
            } else {
                "indeterminate"
            };
            let reason = match verdict {
                "established" => {
                    "A located agreement/validity failure or header-acceptor-universe mismatch on these first certificates' causal history explains why this is not a conforming safety history; unrelated failures do not count."
                }
                "violated" => {
                    "Fully covered conflicting quorums have no explaining agreement/validity failure or relevant header-acceptor-universe mismatch: this exposes a missing or overgenerous rule in this account, not protocol tolerance of the conflict."
                }
                _ if !undeclared.is_empty() => {
                    "The conflicting choices remain witnessed, but relevant actors' configuration declarations are unavailable, preventing a complete check of configuration agreement and contract-dependent explanatory predicates."
                }
                _ => {
                    "The conflicting choices remain witnessed, but unavailable relevant contract/history prevents a complete check of the account's explanatory predicates."
                }
            };
            let ids: Vec<_> = explanations.iter().map(|p| p["id"].clone()).collect();
            let mut support = history.lines.clone();
            if !undeclared.is_empty()
                || explanations
                    .iter()
                    .any(|p| p["proposition"] == "config_agreement")
            {
                support.insert(1);
            }
            let mut check = proposition(
                "conflict_explanation",
                "evidence",
                json!({"target":"account","slot":slot,"choices":[earlier["id"],later["id"]]}),
                boundary,
                verdict,
                Citations {
                    support: support.into_iter().collect(),
                    counterevidence: if verdict == "violated" {
                        vec![
                            earlier["first_quorum_line"].as_u64().unwrap() as usize,
                            boundary,
                        ]
                    } else {
                        vec![]
                    },
                    gaps: gaps.clone(),
                },
                reason,
            );
            let mut finding = json!({"kind":if verdict == "violated" {"unexplained_conflicting_choices"} else {"conflict_explanation"},
                "line":boundary,"proposition_id":check["id"],"choices":[earlier["id"],later["id"]],"slot":slot,
                "fully_covered":complete,"relevant_nodes":history.nodes,"undeclared_contracts":undeclared,"gaps":gaps,
                "explanatory_obligations":ids,"account_deficiency":verdict == "violated","reason":reason});
            if verdict == "indeterminate" && !undeclared.is_empty() {
                let premises = json!(unavailable_configurations(e, &history, &undeclared));
                check["unavailable_premises"] = premises.clone();
                finding["unavailable_premises"] = premises;
            }
            findings.push(finding);
            checks.push(check);
        }
    }
    (checks, findings)
}
