//! First-quorum adoption and closed-answer allocation, independent of effects.
use super::attempts::Attempt;
use super::evidence::{Citations, Evidence, proposition};
use super::protocol::{Accepted, Ballot, Message};
use crate::{Desire, DesireId, Input, InputId, NodeId, Slot};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

struct Adopted {
    value: Value,
    ballot: Ballot,
    lines: Vec<usize>,
}
struct Campaign {
    from_slot: Slot,
    start_line: usize,
    reports: BTreeMap<NodeId, (usize, Vec<Accepted>)>,
    testimony: BTreeMap<(Slot, Ballot), (Value, usize)>,
    frozen: BTreeMap<Slot, Adopted>,
    quorum_line: Option<usize>,
    quorum_lines: Vec<usize>,
    gaps: Vec<String>,
    invalid_lines: Vec<usize>,
    conflict_lines: Vec<usize>,
}
impl Campaign {
    fn new(from_slot: Slot, line: usize, gaps: Vec<String>) -> Self {
        Self {
            from_slot,
            start_line: line,
            reports: BTreeMap::new(),
            testimony: BTreeMap::new(),
            frozen: BTreeMap::new(),
            quorum_line: None,
            quorum_lines: vec![],
            gaps,
            invalid_lines: vec![],
            conflict_lines: vec![],
        }
    }

    fn map_determined(&self) -> bool {
        self.quorum_line.is_some() && self.gaps.is_empty() && self.conflict_lines.is_empty()
    }
}
#[derive(Default)]
struct ValueCount {
    clients: Vec<usize>,
    fresh: usize,
}
#[derive(Default)]
struct Proposer {
    first_tick: Option<InputId>,
    campaigns: BTreeMap<Ballot, Campaign>,
    prepare_inputs: BTreeMap<Ballot, InputId>,
    values: HashMap<Value, ValueCount>,
    queue: VecDeque<(Value, usize)>,
    client_gaps: Vec<String>,
    allocation_gaps: Vec<String>,
    campaign_gaps: Vec<String>,
    consumption_premises: Vec<UnavailablePremise>,
    issued: BTreeMap<Ballot, BTreeSet<Slot>>,
}
struct Drain {
    expected: Option<Vec<(Slot, Value)>>,
    support: Vec<usize>,
    unavailable: Vec<UnavailablePremise>,
}

#[derive(Clone)]
struct UnavailablePremise {
    premise: &'static str,
    support: Vec<usize>,
    counterevidence: Vec<usize>,
    gaps: Vec<String>,
    reason: &'static str,
}

fn extend_premises(target: &mut Vec<UnavailablePremise>, source: &[UnavailablePremise]) {
    for premise in source {
        if let Some(prior) = target
            .iter_mut()
            .find(|prior| prior.premise == premise.premise && prior.reason == premise.reason)
        {
            prior.support.extend(&premise.support);
            prior.counterevidence.extend(&premise.counterevidence);
            prior.gaps.extend(premise.gaps.iter().cloned());
            prior.support.sort_unstable();
            prior.support.dedup();
            prior.counterevidence.sort_unstable();
            prior.counterevidence.dedup();
            prior.gaps.sort();
            prior.gaps.dedup();
        } else {
            target.push(premise.clone());
        }
    }
}

fn with_unavailable(mut report: Value, premises: &[UnavailablePremise]) -> Value {
    if premises.is_empty() {
        return report;
    }
    let mut reasons = Vec::new();
    let mut dependencies = Vec::new();
    for premise in premises {
        if !reasons.contains(&premise.reason) {
            reasons.push(premise.reason);
        }
        dependencies.push(json!({"premise":premise.premise,"support":premise.support,
            "counterevidence":premise.counterevidence,"gaps":premise.gaps}));
        for (field, extra) in [
            ("support", json!(premise.support)),
            ("counterevidence", json!(premise.counterevidence)),
            ("gaps", json!(premise.gaps)),
        ] {
            let citations = report
                .as_object_mut()
                .unwrap()
                .entry(field)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap();
            for citation in extra.as_array().unwrap() {
                if !citations.contains(citation) {
                    citations.push(citation.clone());
                }
            }
        }
    }
    report["unavailable_premises"] = json!(dependencies);
    report["reason"] = json!(reasons.join(" "));
    report
}

fn gap_premise(
    e: &Evidence,
    premise: &'static str,
    gaps: &[String],
    reason: &'static str,
) -> Vec<UnavailablePremise> {
    if gaps.is_empty() {
        return vec![];
    }
    vec![UnavailablePremise {
        premise,
        support: e
            .findings
            .iter()
            .filter(|finding| gaps.iter().any(|gap| finding["id"] == *gap))
            .filter_map(|finding| finding["first_line"].as_u64().map(|line| line as usize))
            .collect(),
        counterevidence: vec![],
        gaps: gaps.to_vec(),
        reason,
    }]
}

fn with_configuration(e: &Evidence, node: NodeId, report: Value) -> Value {
    let unavailable = gap_premise(
        e,
        "configuration",
        &config_gap(e, node),
        "The proposer declaration is unreadable, so its configured role, activation and bounded-campaign contract are unavailable.",
    );
    with_unavailable(report, &unavailable)
}

fn map_premises(
    e: &Evidence,
    state: &Proposer,
    node: NodeId,
    ballot: Ballot,
) -> Vec<UnavailablePremise> {
    let mut unavailable = gap_premise(
        e,
        "configuration",
        &config_gap(e, node),
        "The proposer declaration is unreadable, so its campaign and initial-state premises are unavailable.",
    );
    let campaign = state.campaigns.get(&ballot);
    let gaps = campaign.map_or(&state.campaign_gaps, |campaign| &campaign.gaps);
    let mut map = gap_premise(
        e,
        "first_quorum_map",
        gaps,
        "An unreadable pre-freeze input or earlier closed Send can hide the first campaign/quorum map; later readable reports do not reconstruct it.",
    );
    if let Some(campaign) = campaign {
        for premise in &mut map {
            premise.support.push(campaign.start_line);
            premise.support.extend(&campaign.quorum_lines);
        }
        if campaign.quorum_line.is_some() && !campaign.conflict_lines.is_empty() {
            map.push(UnavailablePremise {
                premise: "first_quorum_map",
                support: campaign.quorum_lines.clone(),
                counterevidence: campaign.conflict_lines.clone(),
                gaps: vec![],
                reason: "Individually valid reports in the immutable first quorum contradict the same slot/ballot testimony, so its adoption map is unavailable; the witnessed quorum gate remains independent.",
            });
        }
    }
    unavailable.extend(map);
    unavailable
}

fn consumption_premises(e: &Evidence, state: &Proposer) -> Vec<UnavailablePremise> {
    let mut unavailable = state.consumption_premises.clone();
    extend_premises(
        &mut unavailable,
        &gap_premise(
            e,
            "client_allowance",
            &state.client_gaps,
            "An unreadable client dispatch leaves its exact-value allowance and FIFO position unavailable.",
        ),
    );
    extend_premises(
        &mut unavailable,
        &gap_premise(
            e,
            "proposal_allocation",
            &state.allocation_gaps,
            "An earlier unreadable closed Send may conceal a proposal allocation, leaving cumulative client consumption and FIFO position unavailable.",
        ),
    );
    unavailable
}

fn subject(attempt: &Attempt, e: &Evidence) -> Value {
    json!({"node":attempt.node,"proposer":attempt.node,"slot":attempt.slot,"ballot":attempt.ballot,"value":attempt.value,"first_desire_line":e.outputs[&attempt.first].desire_line})
}
fn add(obligations: &mut Vec<Value>, attempt: &mut Attempt, p: Value) {
    attempt
        .obligation_ids
        .push(p["id"].as_str().unwrap().to_owned());
    obligations.push(p);
}
fn next_slot(state: &Proposer, ballot: Ballot, from_slot: Slot) -> Option<Slot> {
    let mut slot = from_slot;
    if let Some(issued) = state.issued.get(&ballot) {
        while issued.contains(&slot) {
            slot = Slot(slot.0.checked_add(1)?);
        }
    }
    Some(slot)
}
fn drain(e: &Evidence, state: &Proposer, node: NodeId, ballot: Ballot) -> Drain {
    let campaign = &state.campaigns[&ballot];
    let mut unavailable = map_premises(e, state, node, ballot);
    extend_premises(&mut unavailable, &consumption_premises(e, state));
    let mut support = campaign.quorum_lines.clone();
    support.extend(state.queue.iter().map(|(_, line)| *line));
    if !unavailable.is_empty() {
        return Drain {
            expected: None,
            support,
            unavailable,
        };
    }
    if campaign.quorum_line.is_none() {
        return Drain {
            expected: Some(vec![]),
            support,
            unavailable,
        };
    }
    let mut expected = Vec::new();
    let mut queued = state.queue.iter();
    let mut head = queued.next();
    let mut slot = next_slot(state, ballot, campaign.from_slot);
    while let (Some((value, _)), Some(current)) = (head, slot) {
        if let Some(adopted) = campaign.frozen.get(&current) {
            expected.push((current, adopted.value.clone()));
            support.extend(&adopted.lines);
        } else {
            expected.push((current, value.clone()));
            head = queued.next();
        }
        slot = current.0.checked_add(1).map(Slot);
    }
    Drain {
        expected: Some(expected),
        support,
        unavailable,
    }
}

pub(super) fn evaluate(e: &Evidence, attempts: &mut [Attempt]) -> (Vec<Value>, Vec<Value>) {
    let mut obligations = Vec::new();
    let mut observations = Vec::new();
    let mut states: BTreeMap<NodeId, Proposer> = e
        .claimed
        .iter()
        .map(|node| (*node, Proposer::default()))
        .collect();
    let mut drains = BTreeMap::<InputId, Drain>::new();
    let mut report_handling =
        BTreeMap::<InputId, (Option<bool>, String, Vec<usize>, Vec<String>)>::new();
    let output_attempt: BTreeMap<DesireId, usize> = attempts
        .iter()
        .enumerate()
        .flat_map(|(index, attempt)| attempt.outputs.iter().map(move |id| (*id, index)))
        .collect();
    for (offset, row) in e.rows.iter().enumerate() {
        let line = offset + 1;
        if row.what == "dispatch" {
            let id: InputId = row.get("input_id").unwrap();
            let input = &e.projection.inputs[&id];
            let node = input.to();
            let Some(state) = states.get_mut(&node) else {
                continue;
            };
            let config = e.config(node);
            match input {
                Input::Tick { .. } => {
                    if state.first_tick.is_none() {
                        state.first_tick = Some(id);
                        if let Some(config) = config
                            && config.active_proposer == Some(node)
                        {
                            state
                                .campaigns
                                .entry(Ballot(config.initial_round, node))
                                .or_insert_with(|| {
                                    Campaign::new(
                                        config.from_slot,
                                        line,
                                        state.campaign_gaps.clone(),
                                    )
                                });
                        }
                    }
                }
                Input::Client { .. } => match e.input_messages.get(&id) {
                    Some(Ok(Message::Propose { value })) => {
                        if let Some(count) = state.values.get_mut(value) {
                            count.clients.push(line);
                        } else {
                            state.values.insert(
                                value.clone(),
                                ValueCount {
                                    clients: vec![line],
                                    fresh: 0,
                                },
                            );
                        }
                        state.queue.push_back((value.clone(), line));
                        if let Some(ballot) = state
                            .campaigns
                            .iter()
                            .filter(|(_, campaign)| campaign.quorum_line.is_some())
                            .max_by_key(|(_, campaign)| campaign.start_line)
                            .map(|(ballot, _)| *ballot)
                        {
                            drains.insert(id, drain(e, state, node, ballot));
                        }
                    }
                    Some(Err(_)) => state.client_gaps.push(e.finding_id_for_input(id)),
                    _ => {}
                },
                Input::Message { envelope, .. } => match e.input_messages.get(&id) {
                    Some(Err(_)) => {
                        for campaign in state
                            .campaigns
                            .values_mut()
                            .filter(|campaign| campaign.quorum_line.is_none())
                        {
                            campaign.gaps.push(e.finding_id_for_input(id));
                        }
                    }
                    Some(Ok(Message::Promise {
                        ballot,
                        from_slot,
                        accepted,
                    })) => {
                        let Some(config) = config else {
                            continue;
                        };
                        let semantic = accepted.windows(2).all(|pair| pair[0].slot < pair[1].slot)
                            && accepted.iter().all(|entry| {
                                entry.slot >= *from_slot
                                    && entry.ballot <= *ballot
                                    && config.proposers.contains(&entry.ballot.1)
                            });
                        if !semantic {
                            let mut support = vec![line];
                            let mut ordinary = true;
                            if let Some(campaign) = state.campaigns.get_mut(ballot) {
                                campaign.invalid_lines.push(line);
                                support.extend(&campaign.conflict_lines);
                                ordinary = campaign.conflict_lines.is_empty();
                            }
                            observations.push(json!({"kind":"semantic_promise_report","node":node,"line":line,"valid":false,"counted":false,"from":envelope.from,
                                "reason":"Accepted suffix slots must be strictly increasing and in range; reported ballots cannot exceed the campaign and their owners must belong to the receiving proposer's configured proposer set."}));
                            report_handling.insert(id,(Some(ordinary),if ordinary {"A semantically invalid Promise is ignored without changing collected testimony; a later valid report from this sender can still count."} else {"A semantically invalid report does not repair the prior contradiction or permit ordinary continuation."}.into(),support,vec![]));
                            continue;
                        }
                        let configured = config.proposers.contains(&node)
                            && ballot.1 == node
                            && config.acceptors.contains(&envelope.from);
                        let Some(campaign) = state.campaigns.get_mut(ballot) else {
                            let gaps = if configured && *from_slot == config.from_slot {
                                state.campaign_gaps.clone()
                            } else {
                                vec![]
                            };
                            report_handling.insert(id,(gaps.is_empty().then_some(true),"No readable matching active campaign is known. An earlier unreadable closed Send may hide its Prepare, so a dependent negative judgment requires campaign coverage.".into(),vec![line],gaps));
                            continue;
                        };
                        let eligible = configured && *from_slot == campaign.from_slot;
                        if !eligible
                            || campaign.quorum_line.is_some()
                            || campaign.reports.contains_key(&envelope.from)
                        {
                            observations.push(json!({"kind":"promise_not_counted","node":node,"line":line,"from":envelope.from,"reason":if !eligible {"ineligible_sender_or_campaign"} else if campaign.quorum_line.is_some() {"frozen_quorum"} else {"duplicate_sender"}}));
                            let ordinary = campaign.conflict_lines.is_empty();
                            let mut support = vec![line];
                            support.extend(&campaign.conflict_lines);
                            report_handling.insert(id,(Some(ordinary),if ordinary {"Ineligible, duplicate and late Promise inputs do not change the collected quorum or frozen map."} else {"Contradictory collected testimony has no ordinary continuation; a late report cannot repair the first encounter."}.into(),support,vec![]));
                            continue;
                        }
                        for entry in accepted {
                            if let Some((value, prior)) =
                                campaign.testimony.get(&(entry.slot, entry.ballot))
                                && value != &entry.value
                            {
                                observations.push(json!({"kind":"conflicting_promise_reports","node":node,"slot":entry.slot,"ballot":entry.ballot,"lines":[prior,line],"values":[value,entry.value],
                                    "reason":"Individually valid collected reports contradict each other. This includes lower-ballot testimony hidden beneath a higher selected ballot; trace each report before attributing a source fault."}));
                                campaign.conflict_lines.extend([*prior, line]);
                            }
                        }
                        // Contradiction prevents a determined map, not membership
                        // in the first individually valid distinct encounter.
                        for entry in accepted {
                            campaign
                                .testimony
                                .entry((entry.slot, entry.ballot))
                                .or_insert_with(|| (entry.value.clone(), line));
                        }
                        campaign
                            .reports
                            .insert(envelope.from, (line, accepted.clone()));
                        if !campaign.conflict_lines.is_empty() {
                            report_handling.insert(id,(Some(false),"Contradictory collected testimony has no ordinary returned answer; it is an invariant failure, not a new frozen map.".into(),campaign.conflict_lines.clone(),vec![]));
                        }
                        if campaign.reports.len() > config.acceptors.len() / 2 {
                            campaign.quorum_line = Some(line);
                            campaign.quorum_lines =
                                campaign.reports.values().map(|(line, _)| *line).collect();
                            campaign.quorum_lines.sort_unstable();
                            if campaign.map_determined() {
                                for (report_line, entries) in campaign.reports.values() {
                                    for entry in entries {
                                        match campaign.frozen.get_mut(&entry.slot) {
                                            Some(prior) if prior.ballot > entry.ballot => {}
                                            Some(prior) if prior.ballot == entry.ballot => {
                                                prior.lines.push(*report_line)
                                            }
                                            _ => {
                                                campaign.frozen.insert(
                                                    entry.slot,
                                                    Adopted {
                                                        value: entry.value.clone(),
                                                        ballot: entry.ballot,
                                                        lines: vec![*report_line],
                                                    },
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            observations.push(json!({"kind":"first_promise_quorum","node":node,"ballot":ballot,"line":line,"dispatch_lines":campaign.quorum_lines,
                                "first_quorum_determined":campaign.gaps.is_empty(),"gaps":campaign.gaps,
                                "adoption_map_determined":campaign.map_determined(),"conflict_lines":campaign.conflict_lines,
                                "adoption_map":if campaign.map_determined() {json!(campaign.frozen.iter().map(|(slot,adopted)|json!({"slot":slot,"ballot":adopted.ballot,"value":adopted.value,"report_lines":adopted.lines})).collect::<Vec<_>>())} else {Value::Null} }));
                            drains.insert(id, drain(e, state, node, *ballot));
                        } else if campaign.conflict_lines.is_empty() {
                            report_handling.insert(id,(Some(true),"A valid non-quorum Promise is collected without Phase-2 output.".into(),vec![line],vec![]));
                        }
                    }
                    _ => {}
                },
            }
        }
        if row.what == "close" {
            let id: InputId = row.get("input_id").unwrap();
            let answer = &e.projection.answers[&id];
            let node = answer.node;
            let Some(state) = states.get_mut(&node) else {
                continue;
            };
            let config = e.config(node);
            let input = &e.projection.inputs[&id];
            let input_line = e.projection.input_lines[&id];
            if matches!(input, Input::Tick { .. }) {
                let first = state.first_tick == Some(id);
                let expected=config.map(|config| {
                    if first && config.active_proposer==Some(node) {
                        config.acceptors.iter().map(|to|Desire::Send{to:*to,message:json!({"type":"prepare","ballot":Ballot(config.initial_round,node),"from_slot":config.from_slot})}).collect::<Vec<_>>()
                    } else {vec![]}
                });
                let condition = expected.as_ref().map(|expected| {
                    answer
                        .desires
                        .iter()
                        .map(|(_, desire)| desire)
                        .eq(expected.iter())
                });
                obligations.push(with_configuration(e, node, proposition("activation", "workload", json!({"node":node,"input_id":id,"input_line":input_line,"first_tick":first}), line, verdict(condition), Citations { support: vec![1,input_line,line], counterevidence: counter(condition,line), gaps: config_gap(e,node) }, "Only the first dispatched Tick starts the one self-activated campaign, using this node's configured round; other/null activations and later Ticks return no new campaign.")));
                if !first {
                    let condition = config.map(|_| answer.desires.is_empty());
                    obligations.push(with_configuration(
                        e,
                        node,
                        proposition(
                            "no_retry",
                            "workload",
                            json!({"node":node,"input_id":id,"input_line":input_line}),
                            line,
                            verdict(condition),
                            Citations {
                                support: vec![input_line, line],
                                counterevidence: counter(condition, line),
                                gaps: config_gap(e, node),
                            },
                            "Later Ticks neither restart nor retry the bounded campaign.",
                        ),
                    ));
                }
            }
            if matches!(e.input_messages.get(&id), Some(Ok(Message::Nack { .. }))) {
                let condition = config.map(|_| answer.desires.is_empty());
                obligations.push(with_configuration(e, node, proposition("no_retry", "workload", json!({"node":node,"input_id":id,"input_line":input_line}), line, verdict(condition), Citations { support: vec![input_line,line], counterevidence: counter(condition,line), gaps: config_gap(e,node) }, "A Nack produces no retry, abandonment, or new campaign; its ordinary closed answer is empty.")));
            }
            if let Some((ignored, reason, support, gaps)) = report_handling.get(&id) {
                let condition = config
                    .and_then(|_| ignored.map(|ignored| ignored && answer.desires.is_empty()));
                let mut unavailable = gap_premise(
                    e,
                    "configuration",
                    &config_gap(e, node),
                    "The receiving proposer's declaration is unreadable, so its Promise-handling contract is unavailable.",
                );
                extend_premises(
                    &mut unavailable,
                    &gap_premise(
                        e,
                        "first_quorum_map",
                        gaps,
                        "An earlier unreadable closed Send may hide the matching campaign and collected reports, so ordinary missing-campaign handling cannot be determined.",
                    ),
                );
                obligations.push(with_unavailable(
                    proposition(
                        "promise_report_handling",
                        "validity",
                        json!({"node":node,"input_id":id,"input_line":input_line}),
                        line,
                        verdict(condition),
                        Citations {
                            support: support.iter().copied().chain([line]).collect(),
                            counterevidence: counter(condition, line),
                            gaps: config_gap(e, node)
                                .into_iter()
                                .chain(gaps.iter().cloned())
                                .collect(),
                        },
                        reason,
                    ),
                    if condition.is_none() {
                        &unavailable
                    } else {
                        &[]
                    },
                ));
            }
            // An unreadable closed Send can conceal both proposal allocation
            // and a previously unknown campaign. It does not erase an already
            // witnessed campaign or its historical first-quorum map.
            let mut first_allocations = Vec::new();
            for (output_id, _) in &answer.desires {
                let output = &e.outputs[output_id];
                if output.message.as_ref().is_some_and(Result::is_err) && e.claimed.contains(&node)
                {
                    state.allocation_gaps.push(format!("send:{}", output.id.0));
                    state.campaign_gaps.push(format!("send:{}", output.id.0));
                    let premise = UnavailablePremise {
                        premise: "proposal_allocation",
                        support: vec![output.desire_line, line],
                        counterevidence: vec![],
                        gaps: vec![format!("send:{}", output.id.0)],
                        reason: "An earlier unreadable closed Send may conceal a proposal allocation, leaving cumulative client consumption and FIFO position unavailable.",
                    };
                    extend_premises(&mut state.consumption_premises, &[premise]);
                }
                if let Some(Ok(Message::Prepare { ballot, from_slot })) = &output.message {
                    let prior = state.prepare_inputs.get(ballot).copied();
                    let retry = prior.is_some_and(|prior| prior != id)
                        || (!state.prepare_inputs.is_empty() && prior.is_none());
                    state.prepare_inputs.entry(*ballot).or_insert(id);
                    state.campaigns.entry(*ballot).or_insert_with(|| {
                        Campaign::new(*from_slot, input_line, state.campaign_gaps.clone())
                    });
                    if output.position==0 || !answer.desires[..output.position].iter().any(|(prior,_)|matches!(&e.outputs[prior].message,Some(Ok(Message::Prepare{ballot:b,..})) if b==ballot)) {
                        let condition=config.map(|_|!retry);
                        obligations.push(with_configuration(e, node, proposition("no_retry", "workload", json!({"node":node,"ballot":ballot,"input_id":id,"input_line":input_line}), line, verdict(condition), Citations { support: vec![input_line,output.desire_line,line], counterevidence: counter(condition,line), gaps: config_gap(e,node) }, "One answer may broadcast one campaign; repeating its Prepare in another invocation or starting another ballot is a retry.")));
                    }
                }
                let Some(&index) = output_attempt.get(output_id) else {
                    continue;
                };
                if attempts[index].first != *output_id {
                    continue;
                }
                let attempt = &mut attempts[index];
                first_allocations.push((attempt.slot, attempt.value.clone()));
                allocate(e, state, attempt, line, &mut obligations);
            }
            if let Some(drain) = drains.get(&id) {
                let mut unavailable = drain.unavailable.clone();
                for (output_id, _) in &answer.desires {
                    let output = &e.outputs[output_id];
                    if output.message.as_ref().is_some_and(Result::is_err) {
                        extend_premises(
                            &mut unavailable,
                            &[UnavailablePremise {
                                premise: "proposal_allocation",
                                support: vec![output.desire_line, line],
                                counterevidence: vec![],
                                gaps: vec![format!("send:{}", output.id.0)],
                                reason: "An unreadable Send in this closed answer may hide a proposal allocation, so the returned proposal drain cannot be reconstructed.",
                            }],
                        );
                    }
                }
                let condition = config.and_then(|_| {
                    unavailable
                        .is_empty()
                        .then(|| drain.expected.as_ref().unwrap() == &first_allocations)
                });
                obligations.push(with_unavailable(
                    proposition("proposal_drain", "workload", json!({"node":node,"input_id":id,"input_line":input_line}), line, verdict(condition), Citations { support: drain.support.iter().copied().chain([input_line,line]).collect(), counterevidence: counter(condition,line), gaps: vec![] }, "A ready client/first-quorum answer exhausts the nonempty FIFO. Adopted slots leave its head in place; an empty FIFO stops the loop even with adopted slots remaining."),
                    &unavailable,
                ));
            }
        }
        if row.what == "attempt" && row.why == "send" {
            let output = &e.outputs[&row.get("desire_id").unwrap()];
            let node = output.node;
            let Some(Ok(message)) = &output.message else {
                continue;
            };
            let (ballot, suffix) = match message {
                Message::Accept { ballot, slot, .. } | Message::Chosen { ballot, slot, .. } => {
                    (*ballot, Some((*slot, false)))
                }
                Message::Prepare { ballot, from_slot } => (*ballot, Some((*from_slot, true))),
                _ => continue,
            };
            if !e.claimed.contains(&node) {
                continue;
            }
            let config = e.config(node);
            let authority = config.map(|config| {
                config.proposers.contains(&node)
                    && ballot.1 == node
                    && suffix.is_none_or(|(slot, equal)| {
                        if equal {
                            slot == config.from_slot
                        } else {
                            slot >= config.from_slot
                        }
                    })
            });
            obligations.push(with_configuration(e, node, proposition("proposer_authority", "validity", json!({"node":node,"ballot":ballot,"desire_id":output.id}), line, verdict(authority), Citations { support: vec![1,output.desire_line,line], counterevidence: counter(authority,line), gaps: config_gap(e,node) }, "The authoritative sending node is a configured proposer, owns the ballot and uses its declared suffix. Ownership does not change what acceptance or knowledge evidence says.")));
            if let Some(&index) = output_attempt.get(&output.id) {
                let attempt = &mut attempts[index];
                let state = &states[&node];
                let campaign = state.campaigns.get(&attempt.ballot);
                let input_line = e.projection.input_lines[&output.input];
                let known = campaign
                    .and_then(|campaign| campaign.quorum_line)
                    .is_some_and(|quorum| quorum <= input_line);
                let map_unavailable = map_premises(e, state, node, attempt.ballot);
                let condition = if known {
                    Some(true)
                } else if !map_unavailable.is_empty() {
                    None
                } else {
                    Some(false)
                };
                let mut support = vec![
                    input_line,
                    output.desire_line,
                    output.close_line.unwrap(),
                    line,
                ];
                if let Some(campaign) = campaign {
                    support.extend(&campaign.quorum_lines);
                    support.extend(&campaign.invalid_lines);
                    support.extend(&campaign.conflict_lines);
                }
                add(
                    &mut obligations,
                    attempt,
                    with_unavailable(
                        proposition(
                            "phase2_gate",
                            "agreement",
                            subject(attempt, e),
                            line,
                            verdict(condition),
                            Citations {
                                support,
                                counterevidence: counter(condition, line),
                                gaps: vec![],
                            },
                            "Distinct configured acceptors supplied a syntactically and semantically valid matching Promise quorum no later than the dispatch whose closed answer proposed this tuple. Same-turn quorum completion qualifies; output desire, close and admission remain separate citations.",
                        ),
                        if condition.is_none() {
                            &map_unavailable
                        } else {
                            &[]
                        },
                    ),
                );
                let condition = if !map_unavailable.is_empty() {
                    None
                } else {
                    Some(
                        known
                            && campaign.is_some_and(|campaign| {
                                campaign
                                    .frozen
                                    .get(&attempt.slot)
                                    .is_none_or(|adopted| adopted.value == attempt.value)
                            }),
                    )
                };
                let mut support = vec![input_line, output.desire_line, line];
                if let Some(campaign) = campaign {
                    support.push(campaign.start_line);
                    support.extend(&campaign.quorum_lines);
                    support.extend(campaign.reports.values().map(|(line, _)| *line));
                    if let Some(adopted) = campaign.frozen.get(&attempt.slot) {
                        support.extend(&adopted.lines);
                    }
                } else {
                    support.push(1);
                }
                add(
                    &mut obligations,
                    attempt,
                    with_unavailable(
                        proposition(
                            "adoption",
                            "agreement",
                            subject(attempt, e),
                            line,
                            verdict(condition),
                            Citations {
                                support,
                                counterevidence: counter(condition, line),
                                gaps: vec![],
                            },
                            if !known && condition == Some(false) {
                                "No first distinct valid Promise quorum was present when this answer proposed the tuple, so the required adoption premise is false and earns no adoption credit."
                            } else {
                                "The first distinct valid quorum fixes an immutable highest-ballot map. A later Promise never rewrites it."
                            },
                        ),
                        &map_unavailable,
                    ),
                );
            }
        }
    }
    for index in 0..attempts.len() {
        let output = &e.outputs[&attempts[index].first];
        let node = attempts[index].node;
        let prior = attempts[..index].iter().find(|prior| {
            prior.node == node
                && prior.slot == attempts[index].slot
                && prior.ballot == attempts[index].ballot
                && prior.value != attempts[index].value
        });
        if !e.claimed.contains(&node) {
            if let Some(prior) = prior {
                observations.push(json!({"kind":"uncovered_inconsistency","node":node,
                    "line":output.desire_line,"attempt_id":attempts[index].id,
                    "lines":[e.outputs[&prior.first].desire_line,output.desire_line],
                    "reason":"Visible Accept assertions disagree for the same authoritative proposer/slot/ballot; no sender contract was declared."}));
            }
            attempts[index].origin = json!({"kind":"undetermined","allocation_line":output.close_line,
                "first_desire_line":output.desire_line,"first_desire_position":output.position,
                "client_count":output.close_line.map(|_|json!({"available":0,"allocated":0,"certain":false})),
                "support":[output.desire_line],"gaps":[],
                "unavailable_premises":[{"premise":"configuration","support":[1],"counterevidence":[],"gaps":[],"nodes":[node]}],
                "reason":"The originating actor declares no Multi-Paxos contract or initial client/campaign inventory. Its scoped Accept remains visible, without inventing sender workload verdicts or adoption credit."});
            continue;
        }
        let state = &states[&node];
        let gaps: Vec<_> = e
            .outputs
            .values()
            .filter(|candidate| {
                candidate.node == node
                    && candidate.desire_line < output.desire_line
                    && candidate.message.as_ref().is_some_and(Result::is_err)
                    && e.claimed.contains(&node)
            })
            .map(|candidate| format!("send:{}", candidate.id.0))
            .collect();
        let condition = if e.config(node).is_none() {
            None
        } else if prior.is_some() {
            Some(false)
        } else if gaps.is_empty() {
            Some(true)
        } else {
            None
        };
        let mut support = vec![output.desire_line];
        if let Some(prior) = prior {
            support.push(e.outputs[&prior.first].desire_line);
        }
        let mut unavailable = gap_premise(
            e,
            "configuration",
            &config_gap(e, node),
            "The originating proposer declaration is unreadable, so its immutable proposal contract is unavailable.",
        );
        extend_premises(
            &mut unavailable,
            &gap_premise(
                e,
                "proposal_allocation",
                &gaps,
                "An earlier unreadable Send may hide a conflicting assertion of this proposal tuple, so negative immutability coverage is unavailable.",
            ),
        );
        let p = with_unavailable(
            proposition(
                "proposal_immutability",
                "agreement",
                subject(&attempts[index], e),
                output.desire_line,
                verdict(condition),
                Citations {
                    support,
                    counterevidence: counter(condition, output.desire_line),
                    gaps,
                },
                "Repeated recipients share one exact proposal. A different value for the same authoritative proposer/slot/ballot remains a separate assertion and contradicts immutability.",
            ),
            if condition.is_none() {
                &unavailable
            } else {
                &[]
            },
        );
        add(&mut obligations, &mut attempts[index], p);
        if output.close_line.is_none() {
            let attempt = &mut attempts[index];
            attempt.origin = json!({"kind":"undetermined","allocation_line":null,"first_desire_line":output.desire_line,"first_desire_position":output.position,"client_count":null,"support":[output.desire_line],"gaps":[],"reason":"The answer is still unclosed: proposal allocation is explicitly world-open and consumes no client allowance."});
            add(
                &mut obligations,
                attempt,
                proposition(
                    "fresh_origin_count",
                    "workload",
                    subject(attempt, e),
                    e.last_line(),
                    "pending",
                    Citations {
                        support: vec![output.desire_line],
                        counterevidence: vec![],
                        gaps: vec![],
                    },
                    "An unclosed proposal answer has not allocated this proposal. No allowance has been consumed.",
                ),
            );
        }
        if !state.client_gaps.is_empty() || !state.allocation_gaps.is_empty() {
            observations.push(json!({"kind":"proposer_count_coverage","node":node,"attempt_id":attempts[index].id,"client_gaps":state.client_gaps,"allocation_gaps":state.allocation_gaps,"cumulative_consumption_uncertain":!state.consumption_premises.is_empty()}));
        }
    }
    (obligations, observations)
}

fn allocate(
    e: &Evidence,
    state: &mut Proposer,
    attempt: &mut Attempt,
    line: usize,
    obligations: &mut Vec<Value>,
) {
    let output = &e.outputs[&attempt.first];
    let config = e.config(attempt.node);
    let campaign = state.campaigns.get(&attempt.ballot);
    let map_unavailable = map_premises(e, state, attempt.node, attempt.ballot);
    let map_known = map_unavailable.is_empty();
    let adopted = map_known
        && campaign
            .filter(|campaign| campaign.quorum_line.is_some())
            .and_then(|campaign| campaign.frozen.get(&attempt.slot))
            .is_some_and(|adopted| adopted.value == attempt.value);
    let kind = if !map_known {
        "undetermined"
    } else if adopted {
        "adopted"
    } else {
        "fresh"
    };
    if kind == "undetermined" {
        extend_premises(&mut state.consumption_premises, &map_unavailable);
        let cumulative = UnavailablePremise {
            premise: "cumulative_client_consumption",
            support: vec![output.desire_line, line],
            counterevidence: map_unavailable
                .iter()
                .flat_map(|premise| premise.counterevidence.iter().copied())
                .collect(),
            gaps: map_unavailable
                .iter()
                .flat_map(|premise| premise.gaps.iter().cloned())
                .collect(),
            reason: "A closed allocation has unavailable fresh-versus-adopted origin; its original allocation boundary leaves cumulative client consumption and the FIFO head uncertain for later values and campaigns.",
        };
        extend_premises(&mut state.consumption_premises, &[cumulative]);
    }
    if kind == "fresh" {
        if let Some(count) = state.values.get_mut(&attempt.value) {
            count.fresh += 1;
        } else {
            state.values.insert(
                attempt.value.clone(),
                ValueCount {
                    clients: vec![],
                    fresh: 1,
                },
            );
        }
    }
    let value_count = state.values.get(&attempt.value);
    let available = value_count.map_or(0, |count| count.clients.len());
    let allocated = value_count.map_or(0, |count| count.fresh);
    let consumption_unavailable = consumption_premises(e, state);
    let mut count_unavailable = if adopted {
        vec![]
    } else {
        map_unavailable.clone()
    };
    if !adopted {
        extend_premises(&mut count_unavailable, &consumption_unavailable);
    }
    let certain = count_unavailable.is_empty();
    let mut support = vec![output.desire_line, line];
    if let Some(count) = value_count {
        support.extend(&count.clients);
    }
    if let Some(campaign) = campaign {
        support.extend(&campaign.quorum_lines);
        support.extend(&campaign.conflict_lines);
        if let Some(adopted) = campaign.frozen.get(&attempt.slot) {
            support.extend(&adopted.lines);
        }
    }
    attempt.origin = with_unavailable(
        json!({"kind":kind,"allocation_line":line,"first_desire_line":output.desire_line,"first_desire_position":output.position,
        "client_count":if adopted {Value::Null} else {json!({"available":available,"allocated":allocated,"certain":certain})},
        "support":support,"gaps":[],"reason":match kind {
            "adopted"=>"The exact slot/value is in the determined immutable first-quorum map; no client allowance is consumed.",
            "fresh"=>"No determined adopted value matches this exact slot/value; this distinct closed proposal consumes one unit of its value, irrespective of later Send admission or refusal.",
            _=>"The first quorum/map cannot be determined. No adoption credit or finite allowance is granted.",}}),
        &count_unavailable,
    );
    if adopted {
        let campaign = campaign.unwrap();
        let ancestry = super::origin::adoption_ancestry(
            e,
            attempt.node,
            attempt.ballot,
            attempt.slot,
            &attempt.value,
            &campaign.quorum_lines,
            line,
        );
        attempt.origin["support"]
            .as_array_mut()
            .unwrap()
            .extend(ancestry.support.into_iter().map(|line| json!(line)));
        attempt.origin["gaps"]
            .as_array_mut()
            .unwrap()
            .extend(ancestry.gaps.into_iter().map(Value::String));
        attempt.origin["acceptance_ancestry"] = ancestry.report;
    }
    let count_condition = if adopted {
        Some(true)
    } else if certain {
        Some(allocated <= available)
    } else {
        None
    };
    add(
        obligations,
        attempt,
        with_unavailable(
            proposition(
                "fresh_origin_count",
                "workload",
                subject(attempt, e),
                line,
                verdict(count_condition),
                Citations {
                    support: support.clone(),
                    counterevidence: counter(count_condition, line),
                    gaps: vec![],
                },
                "Client-origin counts are cumulative per proposer and exact JSON value at first closed allocation. One proposal broadcast counts once; adoption consumes none; no count transfers from another proposer.",
            ),
            &count_unavailable,
        ),
    );
    let next = config.and_then(|config| next_slot(state, attempt.ballot, config.from_slot));
    let slot_known = config.is_some() && state.allocation_gaps.is_empty();
    let fifo = if slot_known && next != Some(attempt.slot) {
        Some(false)
    } else if !map_known || !consumption_unavailable.is_empty() {
        None
    } else {
        Some(
            !state.queue.is_empty()
                && (adopted
                    || state
                        .queue
                        .front()
                        .is_some_and(|(value, _)| value == &attempt.value))
                && campaign
                    .and_then(|campaign| campaign.frozen.get(&attempt.slot))
                    .is_none_or(|adopted| adopted.value == attempt.value),
        )
    };
    let mut fifo_unavailable = map_unavailable;
    extend_premises(&mut fifo_unavailable, &consumption_unavailable);
    if fifo.is_some() {
        fifo_unavailable.clear();
    }
    add(
        obligations,
        attempt,
        with_unavailable(
            proposition(
                "fifo",
                "workload",
                subject(attempt, e),
                line,
                verdict(fifo),
                Citations {
                    support,
                    counterevidence: counter(fifo, line),
                    gaps: vec![],
                },
                "A new slot is the next locally unproposed suffix slot. A nonempty FIFO supplies fresh values in order; adoption leaves its head untouched. Equal JSON values, not invented request identities, are counted.",
            ),
            &fifo_unavailable,
        ),
    );
    state
        .issued
        .entry(attempt.ballot)
        .or_default()
        .insert(attempt.slot);
    if kind == "fresh"
        && let Some(position) = state
            .queue
            .iter()
            .position(|(value, _)| value == &attempt.value)
    {
        state.queue.remove(position);
    }
}
fn config_gap(e: &Evidence, node: NodeId) -> Vec<String> {
    if e.claimed.contains(&node) && e.config(node).is_none() {
        vec![format!("config:{}", node.0)]
    } else {
        vec![]
    }
}
fn verdict(condition: Option<bool>) -> &'static str {
    match condition {
        Some(true) => "established",
        Some(false) => "violated",
        None => "indeterminate",
    }
}
fn counter(condition: Option<bool>, line: usize) -> Vec<usize> {
    if condition == Some(false) {
        vec![line]
    } else {
        vec![]
    }
}
