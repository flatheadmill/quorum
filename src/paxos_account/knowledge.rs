use super::evidence::{Citations, Evidence, Output, proposition};
use super::protocol::{Ballot, Message};
use crate::{Desire, DesireId, Input, InputId, NodeId, Slot};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    Established,
    Violated,
    Indeterminate,
}

impl Verdict {
    fn name(self) -> &'static str {
        match self {
            Self::Established => "established",
            Self::Violated => "violated",
            Self::Indeterminate => "indeterminate",
        }
    }
}

#[derive(Clone, Debug)]
struct Decision {
    verdict: Verdict,
    support: Vec<usize>,
    counterevidence: Vec<usize>,
    gaps: Vec<String>,
    reason: &'static str,
}

impl Decision {
    fn new(
        verdict: Verdict,
        support: Vec<usize>,
        counterevidence: Vec<usize>,
        gaps: Vec<String>,
        reason: &'static str,
    ) -> Self {
        Self {
            verdict,
            support: unique(support),
            counterevidence: unique(counterevidence),
            gaps: unique(gaps),
            reason,
        }
    }

    fn value(&self) -> Value {
        json!({
            "verdict":self.verdict.name(), "support":self.support,
            "counterevidence":self.counterevidence, "gaps":self.gaps,
            "reason":self.reason
        })
    }
}

fn unique<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort();
    values.dedup();
    values
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Key {
    boundary: usize,
    node: NodeId,
    slot: Slot,
    ballot: Option<Ballot>,
    value: usize,
}

#[derive(Debug)]
struct Knowledge {
    id: String,
    local: Decision,
    transitive: Decision,
    paths: Vec<Value>,
}

#[derive(Clone, Copy, Debug)]
struct Receipt {
    input: InputId,
    line: usize,
    sender: NodeId,
    parent: DesireId,
}

struct Account<'a> {
    evidence: &'a Evidence,
    choices: &'a [Value],
    values: Vec<&'a Value>,
    cache: BTreeMap<Key, Rc<Knowledge>>,
    active: BTreeSet<Key>,
}

impl<'a> Account<'a> {
    fn value_id(&mut self, value: &'a Value) -> usize {
        if let Some(index) = self.values.iter().position(|known| *known == value) {
            index
        } else {
            self.values.push(value);
            self.values.len() - 1
        }
    }

    fn initial_gap(&self, node: NodeId) -> Option<(&'static str, Vec<String>)> {
        if self.evidence.config(node).is_some() {
            None
        } else if self.evidence.claimed.contains(&node) {
            Some((
                "malformed_configuration",
                vec![format!("config:{}", node.0)],
            ))
        } else {
            Some(("undeclared_initial_knowledge", Vec::new()))
        }
    }

    fn missing_knowledge(&self, node: NodeId, boundary: usize) -> Option<Decision> {
        let mut gaps = self.evidence.input_gap_ids(node, boundary);
        if let Some((reason, initial)) = self.initial_gap(node) {
            gaps.extend(initial);
            Some(Decision::new(
                Verdict::Indeterminate,
                Vec::new(),
                Vec::new(),
                gaps,
                reason,
            ))
        } else if !gaps.is_empty() {
            Some(Decision::new(
                Verdict::Indeterminate,
                Vec::new(),
                Vec::new(),
                gaps,
                "unreadable_prior_peer_input",
            ))
        } else {
            None
        }
    }

    fn certificate_ids(&self, key: Key, ballot: Ballot) -> Vec<Value> {
        let encoded = json!(ballot);
        self.choices
            .iter()
            .filter(|choice| {
                choice["slot"] == json!(key.slot)
                    && choice["ballot"] == encoded
                    && choice["value"] == *self.values[key.value]
                    && choice["first_quorum_line"]
                        .as_u64()
                        .is_some_and(|line| line <= key.boundary as u64)
            })
            .map(|choice| choice["id"].clone())
            .collect()
    }

    fn source(&self, receipt: Receipt) -> Option<&'a Output> {
        let output = self.evidence.outputs.get(&receipt.parent)?;
        let Input::Message { envelope, .. } =
            self.evidence.projection.inputs.get(&receipt.input)?
        else {
            return None;
        };
        let Desire::Send { to, message } = &output.desire else {
            return None;
        };
        if output.node == receipt.sender
            && *to == envelope.to
            && *message == envelope.payload
            && output.attempt_line.is_some_and(|line| line < receipt.line)
        {
            Some(output)
        } else {
            None
        }
    }

    fn source_value(&self, receipt: Receipt, output: Option<&Output>) -> Value {
        let message_id = match self.evidence.projection.inputs.get(&receipt.input) {
            Some(Input::Message { envelope, .. }) => Some(envelope.message_id),
            _ => None,
        };
        json!({
            "desire_id":receipt.parent, "message_id":message_id,
            "desire_line":output.map(|o| o.desire_line),
            "close_line":output.and_then(|o| o.close_line),
            "attempt_line":output.and_then(|o| o.attempt_line),
            "result_line":output.and_then(|o| o.result_line)
        })
    }

    fn accepted_support(&self, key: Key, ballot: Ballot, receipt: Receipt) -> (Decision, Value) {
        let Some(output) = self.source(receipt) else {
            return (
                Decision::new(
                    Verdict::Indeterminate,
                    Vec::new(),
                    vec![receipt.line],
                    Vec::new(),
                    "missing_recorded_reply_admission",
                ),
                json!({"save_line":null,"source":self.source_value(receipt,None)}),
            );
        };
        let admission = output.attempt_line.expect("source has an admission");
        let save = self.evidence.acceptances.iter().find(|accepted| {
            accepted.node == receipt.sender
                && accepted.slot == key.slot
                && accepted.ballot == ballot
                && accepted.value == *self.values[key.value]
                && accepted.line < admission
        });
        let source = self.source_value(receipt, Some(output));
        if let Some(save) = save {
            let mut support = vec![save.line, output.desire_line, admission, receipt.line];
            support.extend(output.close_line);
            return (
                Decision::new(
                    Verdict::Established,
                    support,
                    Vec::new(),
                    Vec::new(),
                    "matching_acceptance_fulfilled_before_reply_admission",
                ),
                json!({"save_line":save.line,"save_desire_id":save.output,"source":source}),
            );
        }
        let mut gaps = self.evidence.save_gap_ids(receipt.sender, admission);
        let (verdict, reason) = if let Some((reason, initial)) = self.initial_gap(receipt.sender) {
            gaps.extend(initial);
            (Verdict::Indeterminate, reason)
        } else if !gaps.is_empty() {
            (
                Verdict::Indeterminate,
                "unreadable_fulfilled_save_before_reply_admission",
            )
        } else {
            (
                Verdict::Violated,
                "matching_acceptance_not_fulfilled_before_reply_admission",
            )
        };
        let mut counterevidence = vec![output.desire_line, admission, receipt.line];
        counterevidence.extend(output.close_line);
        (
            Decision::new(verdict, Vec::new(), counterevidence, gaps, reason),
            json!({"save_line":null,"source":source}),
        )
    }

    fn knowledge(&mut self, key: Key) -> Rc<Knowledge> {
        if let Some(known) = self.cache.get(&key) {
            return Rc::clone(known);
        }
        let ballot_id = key.ballot.map_or_else(
            || "value".to_owned(),
            |ballot| format!("{}-{}", ballot.0, ballot.1.0),
        );
        let id = format!(
            "knowledge:{}:{}:{}:{}:{}",
            key.node.0, key.boundary, key.slot.0, ballot_id, key.value,
        );
        // Every recursive edge must move to an earlier Send attempt. The active
        // set also makes an unavailable/corrupt ancestry a finite failed join.
        if !self.active.insert(key) {
            let failure = Decision::new(
                Verdict::Indeterminate,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                "non_decreasing_knowledge_ancestry",
            );
            return Rc::new(Knowledge {
                id,
                local: failure.clone(),
                transitive: failure,
                paths: Vec::new(),
            });
        }
        let mut accepted: BTreeMap<Ballot, BTreeMap<NodeId, Vec<Receipt>>> = BTreeMap::new();
        let mut chosen = Vec::new();
        let mut paths = Vec::new();
        let mut relevant_lines = Vec::new();
        for (input_id, input) in &self.evidence.projection.inputs {
            let Some(&line) = self.evidence.projection.input_lines.get(input_id) else {
                continue;
            };
            if line >= key.boundary || input.to() != key.node {
                continue;
            }
            let Input::Message { envelope, .. } = input else {
                continue;
            };
            let Some(Ok(message)) = self.evidence.input_messages.get(input_id) else {
                continue;
            };
            let (ballot, slot, value, is_chosen) = match message {
                Message::Accepted {
                    ballot,
                    slot,
                    value,
                } => (*ballot, *slot, value, false),
                Message::Chosen {
                    ballot,
                    slot,
                    value,
                } => (*ballot, *slot, value, true),
                _ => continue,
            };
            if slot != key.slot || *value != *self.values[key.value] {
                continue;
            }
            relevant_lines.push(line);
            let receipt = Receipt {
                input: *input_id,
                line,
                sender: envelope.from,
                parent: envelope.parent_desire_id,
            };
            if key.ballot.is_some_and(|asserted| asserted != ballot) {
                paths.push(json!({
                    "kind":"rejected_testimony", "ballot":ballot,
                    "sender":envelope.from, "input_id":input_id,
                    "dispatch_line":line,"reason":"ballot_does_not_match_assertion"
                }));
            } else if is_chosen {
                chosen.push((ballot, receipt));
            } else if self.evidence.header_acceptors.contains(&envelope.from) {
                accepted
                    .entry(ballot)
                    .or_default()
                    .entry(envelope.from)
                    .or_default()
                    .push(receipt);
            } else {
                paths.push(json!({
                    "kind":"rejected_testimony", "ballot":ballot,
                    "sender":envelope.from, "input_id":input_id,
                    "dispatch_line":line,"reason":"non_header_acceptor"
                }));
            }
        }
        let quorum = self.evidence.header_acceptors.len() / 2 + 1;
        let mut local_alternatives = Vec::new();
        let mut transitive_alternatives = Vec::new();
        for (ballot, members) in accepted {
            let mut member_paths = Vec::new();
            let mut established = 0;
            let mut uncertain = 0;
            let mut support = Vec::new();
            let mut counterevidence = Vec::new();
            let mut gaps = Vec::new();
            let mut local_support = Vec::new();
            let mut uncertainty_reason = None;
            for (acceptor, receipts) in &members {
                let mut receipt_paths = Vec::new();
                let mut alternatives = Vec::new();
                for (index, receipt) in receipts.iter().enumerate() {
                    local_support.push(receipt.line);
                    let (decision, join) = self.accepted_support(key, ballot, *receipt);
                    receipt_paths.push(json!({
                        "input_id":receipt.input, "dispatch_line":receipt.line,
                        "duplicate_member":index != 0,
                        "durable_support":decision.value(), "join":join
                    }));
                    alternatives.push(decision);
                }
                if let Some(decision) = alternatives
                    .iter()
                    .find(|decision| decision.verdict == Verdict::Established)
                {
                    established += 1;
                    support.extend_from_slice(&decision.support);
                } else if let Some(decision) = alternatives
                    .iter()
                    .find(|decision| decision.verdict == Verdict::Indeterminate)
                {
                    uncertain += 1;
                    uncertainty_reason.get_or_insert(decision.reason);
                    for alternative in &alternatives {
                        gaps.extend(alternative.gaps.iter().cloned());
                        counterevidence.extend_from_slice(&alternative.counterevidence);
                    }
                } else {
                    for alternative in &alternatives {
                        counterevidence.extend_from_slice(&alternative.counterevidence);
                    }
                }
                member_paths.push(json!({"acceptor":acceptor,"receipts":receipt_paths}));
            }
            let local = if members.len() >= quorum {
                Decision::new(
                    Verdict::Established,
                    local_support,
                    Vec::new(),
                    Vec::new(),
                    "matching_received_accepted_quorum",
                )
            } else {
                Decision::new(
                    Verdict::Violated,
                    Vec::new(),
                    local_support,
                    Vec::new(),
                    "insufficient_distinct_header_acceptors_for_exact_tuple",
                )
            };
            let transitive = if established >= quorum {
                Decision::new(
                    Verdict::Established,
                    support,
                    Vec::new(),
                    Vec::new(),
                    "complete_historical_certificate_path",
                )
            } else if established + uncertain >= quorum {
                Decision::new(
                    Verdict::Indeterminate,
                    support,
                    counterevidence,
                    gaps,
                    uncertainty_reason.unwrap_or("unavailable_durable_support_premise"),
                )
            } else {
                Decision::new(
                    Verdict::Violated,
                    support,
                    counterevidence,
                    Vec::new(),
                    "insufficient_distinct_durably_supported_header_acceptors",
                )
            };
            paths.push(json!({
                "kind":"accepted_quorum", "ballot":ballot,"quorum":quorum,
                "distinct_header_acceptors":members.len(), "members":member_paths,
                "local_basis":local.value(), "transitive_support":transitive.value(),
                "certificate_candidates":self.certificate_ids(key,ballot)
            }));
            local_alternatives.push(local);
            transitive_alternatives.push(transitive);
        }
        for (ballot, receipt) in chosen {
            let local = Decision::new(
                Verdict::Established,
                vec![receipt.line],
                Vec::new(),
                Vec::new(),
                "matching_received_chosen",
            );
            let source = self.source(receipt);
            let (transitive, sender_knowledge) = if let Some(output) = source {
                let admission = output.attempt_line.expect("source has an admission");
                if admission < key.boundary {
                    let sender = self.knowledge(Key {
                        node: receipt.sender,
                        ballot: Some(ballot),
                        boundary: admission,
                        ..key
                    });
                    let mut decision = sender.transitive.clone();
                    let citations = if decision.verdict == Verdict::Established {
                        &mut decision.support
                    } else {
                        &mut decision.counterevidence
                    };
                    citations.extend([receipt.line, output.desire_line, admission]);
                    citations.extend(output.close_line);
                    decision.support = unique(decision.support);
                    decision.counterevidence = unique(decision.counterevidence);
                    (decision, Some(sender.id.clone()))
                } else {
                    (
                        Decision::new(
                            Verdict::Indeterminate,
                            Vec::new(),
                            vec![receipt.line],
                            Vec::new(),
                            "non_decreasing_knowledge_ancestry",
                        ),
                        None,
                    )
                }
            } else {
                (
                    Decision::new(
                        Verdict::Indeterminate,
                        Vec::new(),
                        vec![receipt.line],
                        Vec::new(),
                        "missing_recorded_chosen_admission",
                    ),
                    None,
                )
            };
            paths.push(json!({
                "kind":"chosen", "ballot":ballot,"sender":receipt.sender,
                "input_id":receipt.input,"dispatch_line":receipt.line,
                "source":self.source_value(receipt,source),
                "sender_knowledge_id":sender_knowledge,
                "local_basis":local.value(), "transitive_support":transitive.value(),
                "certificate_candidates":self.certificate_ids(key,ballot)
            }));
            local_alternatives.push(local);
            transitive_alternatives.push(transitive);
        }
        let missing = self.missing_knowledge(key.node, key.boundary);
        let mut local = combine(
            local_alternatives,
            missing.as_ref(),
            &relevant_lines,
            "no_qualifying_received_evidence",
        );
        let mut transitive = combine(
            transitive_alternatives,
            missing.as_ref(),
            &relevant_lines,
            "no_complete_historical_certificate_path",
        );
        for decision in [&mut local, &mut transitive] {
            if decision.verdict == Verdict::Violated {
                decision.counterevidence.push(key.boundary);
                decision.counterevidence = unique(std::mem::take(&mut decision.counterevidence));
            }
        }
        let knowledge = Rc::new(Knowledge {
            id,
            local,
            transitive,
            paths,
        });
        self.active.remove(&key);
        self.cache.insert(key, Rc::clone(&knowledge));
        knowledge
    }
}

fn combine(
    alternatives: Vec<Decision>,
    missing: Option<&Decision>,
    relevant_lines: &[usize],
    absence_reason: &'static str,
) -> Decision {
    if let Some(complete) = alternatives
        .iter()
        .find(|decision| decision.verdict == Verdict::Established)
    {
        return complete.clone();
    }
    let uncertain = alternatives
        .iter()
        .find(|decision| decision.verdict == Verdict::Indeterminate)
        .or(missing);
    let mut support = Vec::new();
    let mut counterevidence = relevant_lines.to_vec();
    let mut gaps = Vec::new();
    for decision in &alternatives {
        support.extend_from_slice(&decision.support);
        counterevidence.extend_from_slice(&decision.counterevidence);
        gaps.extend(decision.gaps.iter().cloned());
    }
    if let Some(missing) = missing {
        gaps.extend(missing.gaps.iter().cloned());
    }
    if let Some(uncertain) = uncertain {
        Decision::new(
            Verdict::Indeterminate,
            support,
            counterevidence,
            gaps,
            uncertain.reason,
        )
    } else {
        // A valid Config declares empty initial knowledge. Complete dispatch
        // coverage then makes absence a bounded, contract-supported negative.
        support.push(1);
        Decision::new(
            Verdict::Violated,
            support,
            counterevidence,
            Vec::new(),
            absence_reason,
        )
    }
}

fn claim_proposition(name: &str, output: &Output, subject: &Value, decision: &Decision) -> Value {
    let mut support = decision.support.clone();
    support.push(output.desire_line);
    support.extend(output.close_line);
    proposition(
        name,
        "evidence",
        subject.clone(),
        output
            .attempt_line
            .expect("only admitted claims have propositions"),
        decision.verdict.name(),
        Citations {
            support: unique(support),
            counterevidence: decision.counterevidence.clone(),
            gaps: decision.gaps.clone(),
        },
        decision.reason,
    )
}

pub(super) fn evaluate(evidence: &Evidence, choices: &[Value]) -> (Vec<Value>, Vec<Value>) {
    let mut account = Account {
        evidence,
        choices,
        values: Vec::new(),
        cache: BTreeMap::new(),
        active: BTreeSet::new(),
    };
    let mut outputs: Vec<_> = evidence.outputs.values().collect();
    outputs.sort_by_key(|output| (output.desire_line, output.id));
    let mut claims = Vec::new();
    let mut observations = Vec::new();
    for output in outputs {
        let (kind, slot, ballot, value) = match &output.desire {
            Desire::Commit { slot, entry } => ("commit", *slot, None, entry),
            Desire::Send { .. } if evidence.claimed.contains(&output.node) => {
                let Some(Ok(Message::Chosen {
                    ballot,
                    slot,
                    value,
                })) = &output.message
                else {
                    continue;
                };
                ("chosen", *slot, Some(*ballot), value)
            }
            _ => continue,
        };
        let value_id = account.value_id(value);
        let knowledge = output.attempt_line.map(|boundary| {
            account.knowledge(Key {
                boundary,
                node: output.node,
                slot,
                ballot,
                value: value_id,
            })
        });
        let subject = json!({
            "node":output.node,"slot":slot,"ballot":ballot,"value":value,
            "desire_id":output.id
        });
        let mut at_boundary = Vec::new();
        let mut later = Vec::new();
        let mut no_admission_candidates = Vec::new();
        for choice in choices {
            if choice["slot"] != json!(slot)
                || choice["value"] != *value
                || ballot.is_some_and(|asserted| choice["ballot"] != json!(asserted))
            {
                continue;
            }
            if let Some(boundary) = output.attempt_line {
                if choice["first_quorum_line"]
                    .as_u64()
                    .is_some_and(|line| line <= boundary as u64)
                {
                    at_boundary.push(choice["id"].clone());
                } else {
                    later.push(choice["id"].clone());
                }
            } else {
                no_admission_candidates.push(choice["id"].clone());
            }
        }
        if !no_admission_candidates.is_empty() {
            observations.push(json!({
                "kind":"unadmitted_claim_certificate_candidates",
                "desire_id":output.id,"desire_line":output.desire_line,
                "boundary_line":null,"certificate_candidates":no_admission_candidates,
                "reason":"no_claim_admission"
            }));
        }
        let unsupported = knowledge.as_ref().and_then(|known| {
            (known.transitive.verdict != Verdict::Established).then(|| {
                json!({
                    "reason":known.transitive.reason,"gaps":known.transitive.gaps
                })
            })
        });
        claims.push(json!({
            "id":format!("claim:{}",output.id.0),"kind":kind,"node":output.node,
            "slot":slot,"ballot":ballot,"value":value,
            "assertion":{
                "desire_id":output.id,"input_id":output.input,
                "desire_line":output.desire_line,"position":output.position,
                "close_line":output.close_line
            },
            "effect":{
                "attempt_line":output.attempt_line,"result_line":output.result_line,
                "outcome":output.outcome,"status":evidence.output_status(output)
            },
            "local_basis":knowledge.as_ref().map(|known|
                claim_proposition("local_basis",output,&subject,&known.local)),
            "transitive_support":knowledge.as_ref().map(|known|
                claim_proposition("transitive_support",output,&subject,&known.transitive)),
            "choice_at_boundary":at_boundary,"choices_seen_later":later,
            "ballot_paths":knowledge.as_ref().map_or(&[][..],|known| known.paths.as_slice()),
            "unsupported_by_tape":unsupported
        }));
    }
    for (key, knowledge) in &account.cache {
        observations.push(json!({
            "kind":"knowledge_evidence","id":knowledge.id,
            "node":key.node,"slot":key.slot,"ballot":key.ballot,
            "value":account.values[key.value],"boundary_line":key.boundary,
            "local_basis":knowledge.local.value(),
            "transitive_support":knowledge.transitive.value(),
            "ballot_paths":knowledge.paths
        }));
    }
    observations.sort_by(|left, right| {
        let line = |value: &Value| {
            value["boundary_line"]
                .as_u64()
                .or_else(|| value["desire_line"].as_u64())
                .unwrap_or(0)
        };
        line(left)
            .cmp(&line(right))
            .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
    });
    (claims, observations)
}
