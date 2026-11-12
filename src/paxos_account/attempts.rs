//! Proposal identity, reply ancestry and genuinely open world work.
use super::evidence::Evidence;
use super::protocol::{Ballot, Message};
use crate::{Desire, DesireId, Input, InputId, MessageId, NodeId, Slot};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub(super) struct Attempt {
    pub id: String,
    pub node: NodeId,
    pub ballot: Ballot,
    pub slot: Slot,
    pub value: Value,
    pub outputs: Vec<DesireId>,
    pub first: DesireId,
    pub first_relevant_line: usize,
    pub origin: Value,
    pub obligation_ids: Vec<String>,
}

pub(super) fn index(e: &Evidence) -> Vec<Attempt> {
    let mut reached = BTreeMap::<DesireId, usize>::new();
    for (id, input) in &e.projection.inputs {
        if let Input::Message { envelope, .. } = input
            && e.claimed.contains(&envelope.to)
            && matches!(e.input_messages.get(id), Some(Ok(Message::Accept { .. })))
        {
            reached
                .entry(envelope.parent_desire_id)
                .or_insert(e.projection.input_lines[id]);
        }
    }
    let mut attempts = Vec::<Attempt>::new();
    let mut identities = HashMap::new();
    for output in e.outputs.values() {
        let Some(Ok(Message::Accept {
            ballot,
            slot,
            value,
        })) = &output.message
        else {
            continue;
        };
        let first_relevant = if e.claimed.contains(&output.node) {
            output.desire_line
        } else if let Some(line) = reached.get(&output.id) {
            *line
        } else {
            continue;
        };
        let key = (output.node.0, slot.0, ballot.0, ballot.1.0, value);
        if let Some(index) = identities.get(&key).copied() {
            let attempt: &mut Attempt = &mut attempts[index];
            attempt.outputs.push(output.id);
            attempt.first_relevant_line = attempt.first_relevant_line.min(first_relevant);
        } else {
            // The first observed spelling labels the report entry; only Value
            // equality above decides whether another output is this proposal.
            let encoded = serde_json::to_string(value).unwrap();
            let id = format!(
                "attempt:{}:{}:{}:{}:{}",
                output.node.0,
                slot.0,
                ballot.0,
                ballot.1.0,
                crate::tape::hash(encoded.as_bytes())
            );
            identities.insert(key, attempts.len());
            attempts.push(Attempt {
                id,
                node: output.node,
                ballot: *ballot,
                slot: *slot,
                value: value.clone(),
                outputs: vec![output.id],
                first: output.id,
                first_relevant_line: first_relevant,
                origin: Value::Null,
                obligation_ids: vec![],
            });
        }
    }
    attempts.sort_by_key(|attempt| e.outputs[&attempt.first].desire_line);
    attempts
}

fn nack_ancestry(e: &Evidence, id: InputId, attempts: &[Attempt]) -> Value {
    let Input::Message { envelope, .. } = &e.projection.inputs[&id] else {
        unreachable!()
    };
    let Ok(Message::Nack {
        rejected_ballot,
        promised_ballot,
    }) = &e.input_messages[&id]
    else {
        unreachable!()
    };
    let response = &e.outputs[&envelope.parent_desire_id];
    let parent = &e.projection.inputs[&response.input];
    let (ancestry, request_dispatch_line, attempt, campaign) = match parent {
        Input::Message {
            envelope: request, ..
        } => match e
            .input_messages
            .get(&response.input)
            .and_then(|message| message.as_ref().ok())
        {
            Some(Message::Accept { .. }) => (
                "accept",
                Some(e.projection.input_lines[&response.input]),
                attempts
                    .iter()
                    .find(|attempt| attempt.outputs.contains(&request.parent_desire_id))
                    .map(|attempt| attempt.id.as_str()),
                None,
            ),
            Some(Message::Prepare { ballot, .. }) => (
                "prepare",
                Some(e.projection.input_lines[&response.input]),
                None,
                Some(*ballot),
            ),
            _ => ("unattributed", None, None, None),
        },
        _ => ("unattributed", None, None, None),
    };
    json!({"kind":"nack","node":envelope.to,"line":e.projection.input_lines[&id],"from":envelope.from,
        "message_id":envelope.message_id,"rejected_ballot":rejected_ballot,"promised_ballot":promised_ballot,
        "ancestry":ancestry,"request_dispatch_line":request_dispatch_line,"attempt_id":attempt,"campaign_ballot":campaign,
        "parent_input_line":e.projection.input_lines[&response.input],"send_desire_line":response.desire_line,"send_attempt_line":response.attempt_line})
}

pub(super) fn finish(
    e: &Evidence,
    attempts: &[Attempt],
    choices: &[Value],
) -> (Vec<Value>, Vec<Value>) {
    let mut observations = Vec::new();
    let mut nacks = BTreeMap::new();
    for (id, input) in &e.projection.inputs {
        if matches!(e.input_messages.get(id), Some(Ok(Message::Nack { .. }))) {
            let nack = nack_ancestry(e, *id, attempts);
            if e.claimed.contains(&input.to())
                || !nack["attempt_id"].is_null()
                || nack["ancestry"] == "prepare"
            {
                observations.push(nack.clone());
                nacks.insert(*id, nack);
            }
        }
    }
    let account = e.projection.account();
    let whole_world_empty = account.active_input.is_none()
        && account.unreached_desires.is_empty()
        && account.open_attempts.is_empty()
        && account.queued.is_empty()
        && account.arrived.is_empty()
        && account.clients.is_empty()
        && account.due_messages.is_empty()
        && account.due_ticks.is_empty()
        && account.no_progress_due.is_none();
    let mut report = Vec::new();
    for attempt in attempts {
        let exact = choices
            .iter()
            .find(|choice| {
                choice["slot"] == json!(attempt.slot)
                    && choice["ballot"] == json!(attempt.ballot)
                    && choice["value"] == attempt.value
            })
            .map(|choice| choice["id"].clone());
        let elsewhere = choices
            .iter()
            .filter(|choice| {
                choice["slot"] == json!(attempt.slot)
                    && choice["ballot"] != json!(attempt.ballot)
                    && choice["value"] == attempt.value
            })
            .map(|choice| choice["id"].clone())
            .collect::<Vec<_>>();
        let witnessed = choices
            .iter()
            .filter(|choice| {
                choice["slot"] == json!(attempt.slot) && choice["value"] != attempt.value
            })
            .map(|choice| choice["id"].clone())
            .collect::<Vec<_>>();
        let mut by_acceptor: BTreeMap<NodeId, Vec<Value>> = e
            .header_acceptors
            .iter()
            .map(|node| (*node, vec![]))
            .collect();
        let mut other_replies = Vec::new();
        let mut matched = BTreeSet::new();
        for (id, input) in &e.projection.inputs {
            let Input::Message { envelope, .. } = input else {
                continue;
            };
            let mut reply = None;
            if let Some(Ok(Message::Accepted {
                ballot,
                slot,
                value,
            })) = e.input_messages.get(id)
                && envelope.to == attempt.node
                && *slot == attempt.slot
            {
                let exact = *ballot == attempt.ballot && *value == attempt.value;
                let classification = if !exact {
                    "mismatching"
                } else if matched.insert(envelope.from) {
                    "matching"
                } else {
                    "duplicate"
                };
                let response = &e.outputs[&envelope.parent_desire_id];
                reply = Some(
                    json!({"kind":"accepted","classification":classification,"line":e.projection.input_lines[id],
                    "message_id":envelope.message_id,"request_dispatch_line":e.projection.input_lines[&response.input],
                    "eligible":e.header_acceptors.contains(&envelope.from),"from":envelope.from,"to":envelope.to,
                    "ballot":ballot,"slot":slot,"value":value,"send_attempt_line":response.attempt_line}),
                );
            }
            if let Some(nack) = nacks.get(id)
                && nack["attempt_id"] == attempt.id
            {
                let mut value = nack.clone();
                value["classification"] = json!(if envelope.to == attempt.node {
                    "matching"
                } else {
                    "mismatching"
                });
                value["eligible"] = json!(e.header_acceptors.contains(&envelope.from));
                value["to"] = json!(envelope.to);
                reply = Some(value);
            }
            if let Some(reply) = reply {
                if envelope.to == attempt.node
                    && let Some(replies) = by_acceptor.get_mut(&envelope.from)
                {
                    replies.push(reply);
                } else {
                    other_replies.push(reply);
                }
            }
        }
        let mut sends = Vec::new();
        for id in &attempt.outputs {
            let output = &e.outputs[id];
            let Desire::Send { to, .. } = &output.desire else {
                unreachable!()
            };
            let dispatch_lines = e
                .projection
                .inputs
                .iter()
                .filter_map(|(id, input)| match input {
                    Input::Message { envelope, .. } if envelope.parent_desire_id == output.id => {
                        Some(e.projection.input_lines[id])
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let drop_lines = e
                .rows
                .iter()
                .enumerate()
                .filter_map(|(offset, row)| {
                    (row.what == "drop"
                        && row.get::<DesireId>("parent_desire_id").ok() == Some(output.id))
                    .then_some(offset + 1)
                })
                .collect::<Vec<_>>();
            sends.push(json!({"to":to,"desire_id":output.id,"message_id":output.message_id,"desire_line":output.desire_line,
                "close_line":output.close_line,"attempt_line":output.attempt_line,"result_line":output.result_line,
                "outcome":output.outcome,"status":e.output_status(output),"dispatch_lines":dispatch_lines,"drop_lines":drop_lines}));
        }
        // Follow world parent identities, not payload resemblance. Descendants can
        // include a malformed reply without inventing a meaning for its bytes.
        let mut related_outputs: BTreeSet<DesireId> = attempt.outputs.iter().copied().collect();
        let mut related_inputs = BTreeSet::<InputId>::new();
        loop {
            let old = (related_outputs.len(), related_inputs.len());
            for (id, input) in &e.projection.inputs {
                if let Input::Message { envelope, .. } = input
                    && related_outputs.contains(&envelope.parent_desire_id)
                {
                    related_inputs.insert(*id);
                }
            }
            for output in e.outputs.values() {
                if related_inputs.contains(&output.input) {
                    related_outputs.insert(output.id);
                }
            }
            if old == (related_outputs.len(), related_inputs.len()) {
                break;
            }
        }
        let related_messages: BTreeMap<MessageId, _> = related_outputs
            .iter()
            .filter_map(|id| {
                let output = &e.outputs[id];
                output.message_id.map(|message_id| (message_id, output))
            })
            .collect();
        let mut world_work = Vec::new();
        for id in &related_outputs {
            let output = &e.outputs[id];
            let status = e.output_status(output);
            if status["world_open"] == true {
                world_work.push(json!({"kind":"effect","identity":id,"line":output.desire_line,"state":status["state"],"attempt_line":output.attempt_line,"input_id":output.input}));
            }
        }
        for pending in &account.queued {
            if let Some(output) = related_messages.get(&pending.message_id) {
                world_work.push(json!({"kind":"message","identity":pending.message_id,"line":output.result_line.or(output.attempt_line),"send_desire_id":output.id,"state":pending.status,"blocked_until":pending.blocked_until}));
            }
        }
        for id in &account.arrived {
            if let Some(output) = related_messages.get(id) {
                world_work.push(json!({"kind":"message","identity":id,"line":output.result_line.or(output.attempt_line),"send_desire_id":output.id,"state":"arrived"}));
            }
        }
        if let Some(id) = account.active_input {
            let root_input = e.outputs[&attempt.first].input;
            if (related_inputs.contains(&id) || id == root_input)
                && e.projection
                    .answers
                    .get(&id)
                    .is_some_and(|answer| !answer.closed)
            {
                world_work.push(json!({"kind":"input","identity":id,"line":e.projection.input_lines[&id],"state":"active"}));
            }
        }
        let world_open = !world_work.is_empty();
        let attribution_uncertain = related_outputs
            .iter()
            .any(|id| e.outputs[id].message.as_ref().is_some_and(Result::is_err))
            || attempt.origin["kind"] == "undetermined"
                && !attempt.origin["allocation_line"].is_null();
        report.push(json!({"id":attempt.id,"proposer":attempt.node,"slot":attempt.slot,"ballot":attempt.ballot,"value":attempt.value,
            "first_relevant_line":attempt.first_relevant_line,"origin":attempt.origin,"sends":sends,
            "replies":by_acceptor.into_iter().map(|(acceptor,received)|json!({"acceptor":acceptor,"no_reply_observed":received.is_empty(),"received":received})).collect::<Vec<_>>(),
            "other_replies":other_replies,"exact_choice":exact,"same_value_elsewhere":elsewhere,"slot_witnessed_values":witnessed,
            "world_work":world_work,"obligation_ids":attempt.obligation_ids,
            "resolution":{"state":if exact.is_some() {"exact_choice"} else if world_open {"world_open"} else {"unresolved"},
                "exact_choice":exact.is_some(),"world_open":world_open,"attribution_uncertain":attribution_uncertain,"whole_world_empty":whole_world_empty,
                "reason":"No reply is not refusal; Nack is not defeat. Historical exact choice and attributed open work are independent. An unresolved empty-world proposal is not a promise of future progress or permanent loss."}}));
    }
    (report, observations)
}
