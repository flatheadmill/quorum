//! Observations survive conformance failures. This fold never runs a node.
use super::evidence::{Citations, Evidence, Output, proposition};
use super::protocol::{Ballot, Message, Record};
use crate::{Desire, Input, InputId, NodeId, Slot};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(super) fn choices(e: &Evidence) -> Vec<Value> {
    struct Group<'a> {
        slot: Slot,
        ballot: Ballot,
        value: &'a Value,
        witnesses: BTreeMap<NodeId, Vec<usize>>,
        first: Vec<(NodeId, usize)>,
        quorum_line: Option<usize>,
    }
    let quorum = e.header_acceptors.len() / 2 + 1;
    let mut groups = HashMap::<(u64, u64, i32, &Value), Group<'_>>::new();
    for acceptance in &e.acceptances {
        if !e.header_acceptors.contains(&acceptance.node) {
            continue;
        }
        let key = (
            acceptance.slot.0,
            acceptance.ballot.0,
            acceptance.ballot.1.0,
            &acceptance.value,
        );
        let group = groups.entry(key).or_insert_with(|| Group {
            slot: acceptance.slot,
            ballot: acceptance.ballot,
            value: &acceptance.value,
            witnesses: BTreeMap::new(),
            first: Vec::new(),
            quorum_line: None,
        });
        let witnesses = group.witnesses.entry(acceptance.node).or_default();
        if witnesses.is_empty() && group.quorum_line.is_none() {
            group.first.push((acceptance.node, acceptance.line));
        }
        witnesses.push(acceptance.line);
        if group.quorum_line.is_none() && group.witnesses.len() == quorum {
            group.quorum_line = Some(acceptance.line);
        }
    }
    let mut choices = Vec::new();
    for group in groups.into_values() {
        let Some(first_line) = group.quorum_line else {
            continue;
        };
        choices.push(json!({
            "id":format!("choice:{first_line}"),"slot":group.slot,"ballot":group.ballot,
            "value":group.value,"quorum":quorum,"first_quorum_line":first_line,
            "first_support":group.first.into_iter().map(|(node,line)|json!({"acceptor":node,"save_line":line})).collect::<Vec<_>>(),
            "witnesses":group.witnesses.into_iter().map(|(node,lines)|json!({"acceptor":node,"save_lines":lines})).collect::<Vec<_>>()
        }));
    }
    choices.sort_by_key(|choice| choice["first_quorum_line"].as_u64());
    choices
}

pub(super) fn no_conflicts(e: &Evidence, choices: &[Value]) -> (Vec<Value>, Vec<Value>) {
    if e.projection.header.is_none() {
        return (vec![], vec![]);
    }
    let mut obligations = Vec::new();
    let mut observations = Vec::new();
    let mut boundaries: BTreeSet<usize> = choices
        .iter()
        .map(|choice| choice["first_quorum_line"].as_u64().unwrap() as usize)
        .collect();
    boundaries.insert(e.last_line());
    let unknown_initial: Vec<_> = e
        .header_acceptors
        .iter()
        .copied()
        .filter(|node| e.config(*node).is_none())
        .collect();
    let initial_gaps: Vec<_> = unknown_initial
        .iter()
        .filter(|node| e.claimed.contains(*node))
        .map(|node| format!("config:{}", node.0))
        .collect();
    let mut prior_by_slot = BTreeMap::<u64, Vec<&Value>>::new();
    let mut next_choice = 0;
    let mut support = Vec::new();
    let mut conflict_lines = BTreeSet::new();
    for boundary in boundaries {
        while let Some(choice) = choices.get(next_choice)
            && choice["first_quorum_line"].as_u64().unwrap() as usize <= boundary
        {
            let line = choice["first_quorum_line"].as_u64().unwrap() as usize;
            let prior = prior_by_slot
                .entry(choice["slot"].as_u64().unwrap())
                .or_default();
            for earlier in prior.iter() {
                if earlier["value"] != choice["value"] {
                    let earlier_line = earlier["first_quorum_line"].as_u64().unwrap() as usize;
                    conflict_lines.extend([earlier_line, line]);
                    observations.push(
                        json!({"kind":"conflicting_choices","line":line,"slot":choice["slot"],
                        "choices":[earlier["id"],choice["id"]],"lines":[earlier_line,line]}),
                    );
                }
            }
            prior.push(choice);
            support.push(line);
            next_choice += 1;
        }
        let mut gaps = initial_gaps.clone();
        for &node in &e.header_acceptors {
            gaps.extend(e.save_gap_ids(node, boundary + 1));
        }
        let verdict = if !conflict_lines.is_empty() {
            "violated"
        } else if unknown_initial.is_empty() && gaps.is_empty() {
            "established"
        } else {
            "indeterminate"
        };
        let mut judgment = proposition(
            "no_conflicting_choices",
            "agreement",
            json!({"universe":"header.acceptors","acceptors":e.header_acceptors,"unknown_initial_state":unknown_initial}),
            boundary,
            verdict,
            Citations {
                support: support.clone(),
                counterevidence: conflict_lines.iter().copied().collect(),
                gaps,
            },
            "Only observed historical header-majority certificates are compared; an unreadable Save or undeclared initial accepted state prevents a negative safety conclusion, never a positive conflicting certificate.",
        );
        if verdict == "indeterminate" && !unknown_initial.is_empty() {
            judgment["support"]
                .as_array_mut()
                .unwrap()
                .insert(0, json!(1));
            judgment["unavailable_premises"] = json!([{
                "premise":"initial_accepted_state", "nodes":unknown_initial,
                "support":[1], "counterevidence":[], "gaps":initial_gaps
            }]);
            judgment["reason"] = json!(
                "The initial accepted state of the listed header acceptors is unavailable at their declaration; absence of a witnessed conflict cannot establish an absence-based safety conclusion."
            );
        }
        obligations.push(judgment);
    }
    (obligations, observations)
}

struct Judgment<'a> {
    name: &'a str,
    category: &'a str,
    line: usize,
    subject: Value,
    condition: Option<bool>,
    support: Vec<usize>,
    gaps: Vec<String>,
    reason: &'a str,
}

fn judge(
    e: &Evidence,
    obligations: &mut Vec<Value>,
    observations: &mut Vec<Value>,
    node: NodeId,
    judgment: Judgment<'_>,
) {
    let Judgment {
        name,
        category,
        line,
        subject,
        condition,
        support,
        mut gaps,
        reason,
    } = judgment;
    let covered = e.config(node).is_some();
    if !covered && e.claimed.contains(&node) {
        gaps.push(format!("config:{}", node.0));
    }
    let verdict = match (covered, condition) {
        (true, Some(true)) => "established",
        (true, Some(false)) => "violated",
        _ => "indeterminate",
    };
    if !covered && condition == Some(false) {
        observations.push(json!({"kind":"uncovered_inconsistency","node":node,"line":line,"proposition":name,"reason":reason,"lines":support}));
    }
    if !e.claimed.contains(&node) {
        return;
    }
    let counterevidence = if verdict == "violated" {
        let mut lines = support.clone();
        lines.push(line);
        lines
    } else {
        vec![]
    };
    obligations.push(proposition(
        name,
        category,
        subject,
        line,
        verdict,
        Citations {
            support,
            counterevidence,
            gaps,
        },
        reason,
    ));
}

pub(super) fn configuration(e: &Evidence) -> Vec<Value> {
    let mut out = Vec::new();
    for (&node, config) in &e.configs {
        let Ok(config) = config else {
            out.push(proposition("config_agreement", "configuration", json!({"node":node}), 1, "indeterminate", Citations { support: vec![1], counterevidence: vec![], gaps: vec![format!("config:{}",node.0)] }, "Malformed declaration withholds a complete Config; the actor remains in protocol parsing scope."));
            continue;
        };
        out.push(proposition("config_agreement", "configuration", json!({"node":node,"other":"header.acceptors"}), 1, if config.acceptors==e.header_acceptors {"established"} else {"violated"}, Citations { support: vec![1], counterevidence: if config.acceptors==e.header_acceptors {vec![]} else {vec![1]}, gaps: vec![] }, "The header alone defines observed global quorums; a differing local universe remains a configuration violation."));
        for (&other, other_config) in e
            .configs
            .range((std::ops::Bound::Excluded(node), std::ops::Bound::Unbounded))
        {
            let Ok(other_config) = other_config else {
                continue;
            };
            let equal = config.acceptors == other_config.acceptors
                && config.learners == other_config.learners
                && config.proposers == other_config.proposers
                && config.from_slot == other_config.from_slot;
            out.push(proposition("config_agreement", "configuration", json!({"node":node,"other":other}), 1, if equal {"established"} else {"violated"}, Citations { support: vec![1], counterevidence: if equal {vec![]} else {vec![1]}, gaps: vec![] }, "Role sets and suffix agree as sets. active_proposer and initial_round are per-node settings, not cluster-equality predicates."));
        }
    }
    out
}

#[derive(Clone)]
struct Stored {
    ballot: Ballot,
    value: Value,
    line: usize,
}
struct State {
    watermark: Option<(Ballot, Slot, usize)>,
    watermark_known: bool,
    accepted: BTreeMap<Slot, Stored>,
    accepted_known: bool,
    gaps: Vec<String>,
    conflict_lines: Vec<usize>,
}
impl State {
    fn new(known: bool) -> Self {
        Self {
            watermark: None,
            watermark_known: known,
            accepted: BTreeMap::new(),
            accepted_known: known,
            gaps: Vec::new(),
            conflict_lines: Vec::new(),
        }
    }
}
struct Expected {
    desires: Option<Vec<Desire>>,
    support: Vec<usize>,
    gaps: Vec<String>,
    panic_expected: bool,
}

fn reply(to: NodeId, message: Value) -> Desire {
    Desire::Send { to, message }
}
fn save(record: Value) -> Desire {
    Desire::Save { record }
}

fn expected(
    e: &Evidence,
    node: NodeId,
    sender: NodeId,
    message: &Message,
    state: &State,
) -> Expected {
    let mut result = Expected {
        desires: None,
        support: vec![1],
        gaps: state.gaps.clone(),
        panic_expected: false,
    };
    let Some(config) = e.config(node) else {
        return result;
    };
    let (ballot, slot) = match message {
        Message::Prepare { ballot, from_slot } => (*ballot, *from_slot),
        Message::Accept { ballot, slot, .. } => (*ballot, *slot),
        _ => return result,
    };
    let eligible = config.acceptors.contains(&node)
        && config.proposers.contains(&sender)
        && ballot.1 == sender
        && match message {
            Message::Prepare { from_slot, .. } => *from_slot == config.from_slot,
            _ => slot >= config.from_slot,
        };
    if !eligible {
        result.desires = Some(vec![]);
        result.gaps.clear();
        return result;
    }
    if !state.watermark_known {
        return result;
    }
    if let Some((promised, _, line)) = state.watermark {
        result.support.push(line);
        if ballot < promised {
            result.desires = Some(vec![reply(
                sender,
                json!({"type":"nack","rejected_ballot":ballot,"promised_ballot":promised}),
            )]);
            result.gaps.clear();
            return result;
        }
    }
    if !state.accepted_known {
        return result;
    }
    let mut desires = Vec::new();
    if let Message::Accept { value, .. } = message
        && let Some(previous) = state.accepted.get(&slot)
        && previous.ballot == ballot
    {
        result.support.push(previous.line);
        if previous.value != *value {
            result.panic_expected = true;
            return result;
        }
        result.desires = Some(vec![reply(
            sender,
            json!({"type":"accepted","ballot":ballot,"slot":slot,"value":value}),
        )]);
        result.gaps.clear();
        return result;
    }
    if state
        .watermark
        .is_none_or(|(promised, _, _)| ballot > promised)
    {
        desires.push(save(
            json!({"record":"promised","from_slot":config.from_slot,"ballot":ballot}),
        ));
    }
    match message {
        Message::Prepare { from_slot, .. } => {
            let accepted: Vec<_> = state
                .accepted
                .range(*from_slot..)
                .map(|(slot, stored)| {
                    result.support.push(stored.line);
                    json!({"slot":slot,"ballot":stored.ballot,"value":stored.value})
                })
                .collect();
            desires.push(reply(
                sender,
                json!({"type":"promise","ballot":ballot,"from_slot":from_slot,"accepted":accepted}),
            ));
        }
        Message::Accept { value, .. } => {
            desires.push(save(
                json!({"record":"accepted","slot":slot,"ballot":ballot,"value":value}),
            ));
            desires.push(reply(
                sender,
                json!({"type":"accepted","ballot":ballot,"slot":slot,"value":value}),
            ));
        }
        _ => {}
    }
    result.desires = Some(desires);
    result.gaps.clear();
    result
}

pub(super) fn acceptors(e: &Evidence) -> (Vec<Value>, Vec<Value>) {
    let mut obligations = Vec::new();
    let mut observations = Vec::new();
    let nodes: BTreeSet<_> = e.header_acceptors.union(&e.claimed).copied().collect();
    let mut states: BTreeMap<_, _> = nodes
        .into_iter()
        .map(|node| (node, State::new(e.config(node).is_some())))
        .collect();
    let mut expected_answers = BTreeMap::<InputId, Expected>::new();
    for (offset, row) in e.rows.iter().enumerate() {
        let line = offset + 1;
        if row.what == "dispatch" && row.why == "message" {
            let id = row.get("input_id").unwrap();
            let Input::Message { envelope, .. } = &e.projection.inputs[&id] else {
                unreachable!()
            };
            if let (Some(state), Some(Ok(message))) =
                (states.get(&envelope.to), e.input_messages.get(&id))
                && matches!(message, Message::Prepare { .. } | Message::Accept { .. })
            {
                expected_answers
                    .insert(id, expected(e, envelope.to, envelope.from, message, state));
            }
        }
        if row.what == "close" {
            let id = row.get("input_id").unwrap();
            if let Some(expected) = expected_answers.get(&id) {
                let answer = &e.projection.answers[&id];
                let condition = if expected.panic_expected {
                    Some(false)
                } else {
                    expected.desires.as_ref().map(|desires| {
                        answer
                            .desires
                            .iter()
                            .map(|(_, desire)| desire)
                            .eq(desires.iter())
                    })
                };
                let mut support = expected.support.clone();
                support.extend([e.projection.input_lines[&id], line]);
                judge(
                    e,
                    &mut obligations,
                    &mut observations,
                    answer.node,
                    Judgment {
                        name: "acceptor_response",
                        category: "validity",
                        line,
                        subject: json!({"node":answer.node,"input_id":id,"input_line":e.projection.input_lines[&id]}),
                        condition,
                        support,
                        gaps: expected.gaps.clone(),
                        reason: "The closed answer follows the declared acceptor transition, including stale-before-repeat, current suffix, and ordered persistence before reply; a conflicting non-stale Accept has no ordinary returned answer.",
                    },
                );
            }
        }
        if row.what == "result" && row.why == "save" {
            let output = &e.outputs[&row.get("desire_id").unwrap()];
            if output.outcome.as_deref() != Some("fulfilled") {
                continue;
            }
            let Some(state) = states.get_mut(&output.node) else {
                continue;
            };
            let node = output.node;
            match output.record.as_ref().unwrap() {
                Err(_) => {
                    state.watermark_known = false;
                    state.accepted_known = false;
                    state.gaps.push(format!("save:{}", output.id.0));
                }
                Ok(Record::Promised { from_slot, ballot }) => {
                    let monotone = state.watermark_known.then(|| {
                        state
                            .watermark
                            .is_none_or(|(old, suffix, _)| *ballot >= old && *from_slot == suffix)
                    });
                    let condition = monotone.map(|valid| {
                        valid
                            && e.config(node)
                                .is_none_or(|config| *from_slot == config.from_slot)
                    });
                    let mut support = vec![line];
                    if let Some((_, _, previous)) = state.watermark {
                        support.push(previous);
                    }
                    judge(
                        e,
                        &mut obligations,
                        &mut observations,
                        node,
                        Judgment {
                            name: "acceptor_watermark",
                            category: "validity",
                            line,
                            subject: json!({"node":node,"ballot":ballot,"from_slot":from_slot}),
                            condition,
                            support,
                            gaps: state.gaps.clone(),
                            reason: "A fulfilled promise never decreases or moves its configured suffix. A later explicit record identifies the current watermark without repairing an earlier unknown or illegal transition.",
                        },
                    );
                    state.watermark = Some((*ballot, *from_slot, line));
                    state.watermark_known = true;
                }
                Ok(Record::Accepted {
                    slot,
                    ballot,
                    value,
                }) => {
                    let mut support = vec![line];
                    if let Some((_, _, previous)) = state.watermark {
                        support.push(previous);
                    }
                    judge(
                        e,
                        &mut obligations,
                        &mut observations,
                        node,
                        Judgment {
                            name: "accepted_watermark",
                            category: "validity",
                            line,
                            subject: json!({"node":node,"slot":slot,"ballot":ballot,"value":value}),
                            condition: state.watermark_known.then(|| {
                                state
                                    .watermark
                                    .is_some_and(|(watermark, _, _)| watermark == *ballot)
                            }),
                            support,
                            gaps: state.gaps.clone(),
                            reason: "At this fulfilled Accepted result the ballot equals the durable promise watermark; the acceptance assertion remains observable even when this obligation fails.",
                        },
                    );
                    if let Some(config) = e.config(node) {
                        judge(
                            e,
                            &mut obligations,
                            &mut observations,
                            node,
                            Judgment {
                                name: "acceptor_authority",
                                category: "validity",
                                line,
                                subject: json!({"node":node,"slot":slot}),
                                condition: Some(
                                    config.acceptors.contains(&node) && *slot >= config.from_slot,
                                ),
                                support: vec![1, line],
                                gaps: vec![],
                                reason: "Acceptance uses the configured acceptor role and fixed suffix.",
                            },
                        );
                    }
                    let prior_same = e.acceptances.iter().find(|prior| {
                        prior.node == node
                            && prior.slot == *slot
                            && prior.ballot == *ballot
                            && prior.value != *value
                            && prior.line < line
                    });
                    let prior_higher = state
                        .accepted
                        .get(slot)
                        .filter(|prior| prior.ballot > *ballot);
                    let consistent = prior_same.is_none() && prior_higher.is_none();
                    let mut support = vec![line];
                    if let Some(prior) = prior_same {
                        support.push(prior.line);
                        state.accepted_known = false;
                        state.conflict_lines.extend([prior.line, line]);
                    }
                    if let Some(prior) = prior_higher {
                        support.push(prior.line);
                    }
                    judge(
                        e,
                        &mut obligations,
                        &mut observations,
                        node,
                        Judgment {
                            name: "accepted_consistency",
                            category: "validity",
                            line,
                            subject: json!({"node":node,"slot":slot,"ballot":ballot,"value":value}),
                            condition: if !consistent {
                                Some(false)
                            } else {
                                state.accepted_known.then_some(true)
                            },
                            support,
                            gaps: state.gaps.clone(),
                            reason: "Per-slot accepted ballots do not decrease and one slot/ballot cannot acquire two values. Historical observations are never discarded.",
                        },
                    );
                    if state
                        .accepted
                        .get(slot)
                        .is_none_or(|prior| *ballot > prior.ballot)
                    {
                        state.accepted.insert(
                            *slot,
                            Stored {
                                ballot: *ballot,
                                value: value.clone(),
                                line,
                            },
                        );
                    }
                }
            }
        }
        if row.what == "attempt" && row.why == "send" {
            let output = &e.outputs[&row.get("desire_id").unwrap()];
            if !e.claimed.contains(&output.node) {
                continue;
            }
            let Some(state) = states.get(&output.node) else {
                continue;
            };
            let Some(Ok(message)) = &output.message else {
                continue;
            };
            match message {
                Message::Promise {
                    ballot,
                    from_slot,
                    accepted,
                } => {
                    let mut support = vec![output.desire_line, line];
                    if let Some((_, _, prior)) = state.watermark {
                        support.push(prior);
                    }
                    judge(
                        e,
                        &mut obligations,
                        &mut observations,
                        output.node,
                        Judgment {
                            name: "promise_watermark",
                            category: "validity",
                            line,
                            subject: json!({"node":output.node,"ballot":ballot}),
                            condition: state.watermark_known.then(|| {
                                state
                                    .watermark
                                    .is_some_and(|(watermark, _, _)| watermark == *ballot)
                            }),
                            support,
                            gaps: state.gaps.clone(),
                            reason: "A Promise admission asserts exactly the durable watermark in force, not merely a ballot below or above it.",
                        },
                    );
                    let expected:Vec<_>=state.accepted.range(*from_slot..).map(|(slot,stored)|json!({"slot":slot,"ballot":stored.ballot,"value":stored.value})).collect();
                    let actual:Vec<_>=accepted.iter().map(|entry|json!({"slot":entry.slot,"ballot":entry.ballot,"value":entry.value})).collect();
                    let suffix = e
                        .config(output.node)
                        .map(|config| config.from_slot == *from_slot);
                    let mut support: Vec<_> =
                        state.accepted.values().map(|entry| entry.line).collect();
                    support.extend([output.desire_line, line]);
                    support.extend(&state.conflict_lines);
                    judge(
                        e,
                        &mut obligations,
                        &mut observations,
                        output.node,
                        Judgment {
                            name: "promise_suffix",
                            category: "validity",
                            line,
                            subject: json!({"node":output.node,"ballot":ballot,"from_slot":from_slot}),
                            condition: if suffix == Some(false) {
                                Some(false)
                            } else if state.accepted_known {
                                suffix.map(|_| expected == actual)
                            } else {
                                None
                            },
                            support,
                            gaps: state.gaps.clone(),
                            reason: "Promise reports the complete current ordered accepted suffix. A missing state premise is not a truthful-empty suffix.",
                        },
                    );
                    reply_request(
                        e,
                        output,
                        message,
                        line,
                        &mut obligations,
                        &mut observations,
                    );
                }
                Message::Accepted {
                    slot,
                    ballot,
                    value,
                } => {
                    let matching = e
                        .acceptances
                        .iter()
                        .filter(|acceptance| {
                            acceptance.node == output.node
                                && acceptance.slot == *slot
                                && acceptance.ballot == *ballot
                                && acceptance.value == *value
                                && acceptance.line < line
                        })
                        .map(|acceptance| acceptance.line)
                        .collect::<Vec<_>>();
                    let gaps = e.save_gap_ids(output.node, line);
                    let condition = if !matching.is_empty() {
                        Some(true)
                    } else if e.config(output.node).is_some() && gaps.is_empty() {
                        Some(false)
                    } else {
                        None
                    };
                    let mut support = matching;
                    support.extend([output.desire_line, line]);
                    judge(
                        e,
                        &mut obligations,
                        &mut observations,
                        output.node,
                        Judgment {
                            name: "accepted_reply_support",
                            category: "validity",
                            line,
                            subject: json!({"node":output.node,"slot":slot,"ballot":ballot,"value":value}),
                            condition,
                            support,
                            gaps,
                            reason: "Each Accepted reply has matching fulfilled durable support before Send admission; a prior turn can support an exact repeat, a later Save cannot repair it.",
                        },
                    );
                    reply_request(
                        e,
                        output,
                        message,
                        line,
                        &mut obligations,
                        &mut observations,
                    );
                }
                Message::Nack {
                    rejected_ballot,
                    promised_ballot,
                } => {
                    let request = e
                        .input_messages
                        .get(&output.input)
                        .and_then(|result| result.as_ref().ok());
                    let rejected = match request {
                        Some(Message::Prepare { ballot, .. } | Message::Accept { ballot, .. }) => {
                            Some(*ballot)
                        }
                        _ => None,
                    };
                    let mut support = vec![
                        e.projection.input_lines[&output.input],
                        output.desire_line,
                        line,
                    ];
                    if let Some((_, _, prior)) = state.watermark {
                        support.push(prior);
                    }
                    let unknown_request = e
                        .input_messages
                        .get(&output.input)
                        .is_some_and(Result::is_err);
                    let condition = if unknown_request {
                        None
                    } else if rejected != Some(*rejected_ballot) {
                        Some(false)
                    } else {
                        state.watermark_known.then(|| {
                            state.watermark.is_some_and(|(ballot, _, _)| {
                                ballot == *promised_ballot && *rejected_ballot < ballot
                            })
                        })
                    };
                    let mut gaps = state.gaps.clone();
                    if unknown_request {
                        gaps.push(e.finding_id_for_input(output.input));
                    }
                    judge(
                        e,
                        &mut obligations,
                        &mut observations,
                        output.node,
                        Judgment {
                            name: "nack_truthfulness",
                            category: "validity",
                            line,
                            subject: json!({"node":output.node,"rejected_ballot":rejected_ballot,"promised_ballot":promised_ballot}),
                            condition,
                            support,
                            gaps,
                            reason: "Nack joins its actual Prepare or Accept request and the durable higher watermark; its diagnosis is not itself authority.",
                        },
                    );
                    reply_request(
                        e,
                        output,
                        message,
                        line,
                        &mut obligations,
                        &mut observations,
                    );
                }
                _ => {}
            }
        }
    }
    (obligations, observations)
}

fn reply_request(
    e: &Evidence,
    output: &Output,
    message: &Message,
    line: usize,
    obligations: &mut Vec<Value>,
    observations: &mut Vec<Value>,
) {
    let request = e.input_messages.get(&output.input);
    let envelope = match &e.projection.inputs[&output.input] {
        Input::Message { envelope, .. } => Some(envelope),
        _ => None,
    };
    let to = match &output.desire {
        Desire::Send { to, .. } => *to,
        _ => unreachable!(),
    };
    let condition = if request.is_some_and(Result::is_err) {
        None
    } else {
        Some(
            envelope.is_some_and(|envelope| envelope.from == to)
                && match (message, request.and_then(|result| result.as_ref().ok())) {
                    (
                        Message::Promise {
                            ballot, from_slot, ..
                        },
                        Some(Message::Prepare {
                            ballot: b,
                            from_slot: s,
                        }),
                    ) => ballot == b && from_slot == s,
                    (
                        Message::Accepted {
                            ballot,
                            slot,
                            value,
                        },
                        Some(Message::Accept {
                            ballot: b,
                            slot: s,
                            value: v,
                        }),
                    ) => ballot == b && slot == s && value == v,
                    (
                        Message::Nack {
                            rejected_ballot, ..
                        },
                        Some(Message::Prepare { ballot, .. } | Message::Accept { ballot, .. }),
                    ) => rejected_ballot == ballot,
                    _ => false,
                },
        )
    };
    let gaps = if condition.is_none() && e.claimed.contains(&output.node) {
        vec![e.finding_id_for_input(output.input)]
    } else {
        vec![]
    };
    judge(
        e,
        obligations,
        observations,
        output.node,
        Judgment {
            name: "reply_request",
            category: "validity",
            line,
            subject: json!({"node":output.node,"to":to,"desire_id":output.id}),
            condition,
            support: vec![
                e.projection.input_lines[&output.input],
                output.desire_line,
                line,
            ],
            gaps,
            reason: "Reply identity and tuple match the authoritative originating request, not a guessed request with equal JSON.",
        },
    );
}
