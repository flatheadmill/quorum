//! Durable ancestry of the testimony in an immutable first Promise quorum.
use super::evidence::{Evidence, Output};
use super::protocol::{Accepted, Ballot, Message};
use crate::{Desire, Envelope, Input, InputId, NodeId, Slot};
use serde_json::{Value, json};

pub(super) struct Ancestry {
    pub report: Value,
    pub support: Vec<usize>,
    pub gaps: Vec<String>,
}

struct ReportEntry<'a> {
    dispatch: usize,
    input: InputId,
    envelope: &'a Envelope,
    entry: &'a Accepted,
}

fn unique<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort();
    values.dedup();
    values
}

fn boundaries(output: &Output) -> Value {
    json!({
        "desire_id":output.id, "parent_input_id":output.input,
        "desire_line":output.desire_line, "close_line":output.close_line,
        "attempt_line":output.attempt_line, "result_line":output.result_line
    })
}

fn cite(output: &Output, support: &mut Vec<usize>) {
    support.push(output.desire_line);
    support.extend(output.close_line);
    support.extend(output.attempt_line);
    support.extend(output.result_line);
}

/// This judges report-entry history, not selection, request ancestry, or choice.
/// A missing old Save must never revoke the caller's positive adoption fact.
pub(super) fn adoption_ancestry(
    e: &Evidence,
    proposer: NodeId,
    ballot: Ballot,
    slot: Slot,
    value: &Value,
    frozen_report_lines: &[usize],
    boundary: usize,
) -> Ancestry {
    let mut entries = Vec::new();
    let mut paths = Vec::new();
    let mut support = Vec::new();
    let mut counterevidence = Vec::new();
    let mut gaps = Vec::new();
    let mut unavailable = false;
    let mut violated = false;
    let mut selected_ballot = None;

    // Never search other received reports: later testimony cannot repair or
    // enlarge the first quorum, even if it has a higher reported ballot.
    for &dispatch in frozen_report_lines {
        let receipt = dispatch.checked_sub(1).and_then(|index| e.rows.get(index));
        let input_id = receipt
            .filter(|row| row.what == "dispatch" && dispatch <= boundary)
            .and_then(|row| row.get::<InputId>("input_id").ok());
        let input = input_id.and_then(|id| e.projection.inputs.get(&id));
        let report = input_id.and_then(|id| e.input_messages.get(&id));
        if let (
            Some(id),
            Some(Input::Message { envelope, .. }),
            Some(Ok(Message::Promise {
                ballot: reported_ballot,
                accepted,
                ..
            })),
        ) = (input_id, input, report)
            && envelope.to == proposer
            && *reported_ballot == ballot
        {
            support.push(dispatch);
            for entry in accepted.iter().filter(|entry| entry.slot == slot) {
                selected_ballot = Some(
                    selected_ballot.map_or(entry.ballot, |prior: Ballot| prior.max(entry.ballot)),
                );
                entries.push(ReportEntry {
                    dispatch,
                    input: id,
                    envelope,
                    entry,
                });
            }
        } else {
            let path_gaps: Vec<_> = input_id
                .map(|id| e.finding_id_for_input(id))
                .filter(|id| {
                    e.findings
                        .iter()
                        .any(|finding| finding["id"].as_str() == Some(id.as_str()))
                })
                .into_iter()
                .collect();
            gaps.extend(path_gaps.iter().cloned());
            counterevidence.push(dispatch);
            unavailable = true;
            paths.push(json!({
                "report_dispatch_line":dispatch, "acceptor":null, "slot":slot,
                "ballot":null, "value":null, "selected":false,
                "promise_source":null, "matching_saves":[], "later_matching_saves":[],
                "verdict":"indeterminate", "support":[], "counterevidence":[dispatch],
                "gaps":path_gaps, "reason":"frozen_promise_dispatch_unavailable"
            }));
        }
    }

    let mut selection_lines = Vec::new();
    for report in entries {
        let entry = report.entry;
        let selected = Some(entry.ballot) == selected_ballot && entry.value == *value;
        if selected {
            selection_lines.push(report.dispatch);
        }
        let output = e.outputs.get(&report.envelope.parent_desire_id);
        let mut source = output.map(boundaries).unwrap_or(Value::Null);
        if let Some(output) = output {
            source["message_id"] = json!(output.message_id);
            source["report_input_id"] = json!(report.input);
            source["send_admission_line"] = json!(output.attempt_line);
        }
        let mut path_support = vec![report.dispatch];
        let mut path_counterevidence = Vec::new();
        let mut path_gaps = Vec::new();
        let mut matching_saves = Vec::new();
        let mut later_matching_saves = Vec::new();
        let source_joined = output.is_some_and(|output| {
            matches!(&output.desire, Desire::Send { to, message }
                if output.node == report.envelope.from
                    && *to == proposer
                    && *message == report.envelope.payload)
                && output.message_id == Some(report.envelope.message_id)
                && output
                    .close_line
                    .zip(output.attempt_line)
                    .is_some_and(|(close, admission)| {
                        output.desire_line < close
                            && close < admission
                            && admission < report.dispatch
                    })
                && output.outcome.as_deref() == Some("fulfilled")
                && output
                    .result_line
                    .is_some_and(|line| line < report.dispatch)
        });
        let (verdict, reason) = if let Some(output) = output.filter(|_| source_joined) {
            cite(output, &mut path_support);
            let admission = output.attempt_line.expect("joined Send admission");
            for acceptance in e.acceptances.iter().filter(|acceptance| {
                acceptance.node == report.envelope.from
                    && acceptance.slot == entry.slot
                    && acceptance.ballot == entry.ballot
                    && acceptance.value == entry.value
                    && acceptance.line <= boundary
            }) {
                let save = &e.outputs[&acceptance.output];
                if acceptance.line < admission {
                    cite(save, &mut path_support);
                    matching_saves.push(boundaries(save));
                } else {
                    // An already fulfilled later Save remains an explicit failed
                    // temporal join, not a retroactive repair of this Promise.
                    path_counterevidence.push(acceptance.line);
                    later_matching_saves.push(boundaries(save));
                }
            }
            if !matching_saves.is_empty() {
                path_counterevidence.clear();
                (
                    "established",
                    "matching_acceptance_fulfilled_before_promise_admission",
                )
            } else {
                path_counterevidence.extend([output.desire_line, admission, report.dispatch]);
                path_counterevidence.extend(output.close_line);
                path_gaps = e.save_gap_ids(report.envelope.from, admission);
                path_gaps.retain(|id| {
                    e.findings
                        .iter()
                        .any(|finding| finding["id"].as_str() == Some(id.as_str()))
                });
                if e.config(report.envelope.from).is_none() {
                    let config_gap = format!("config:{}", report.envelope.from.0);
                    if e.findings
                        .iter()
                        .any(|finding| finding["id"].as_str() == Some(config_gap.as_str()))
                    {
                        path_gaps.push(config_gap);
                        (
                            "indeterminate",
                            "malformed_configuration_leaves_initial_acceptance_history_unknown",
                        )
                    } else {
                        ("indeterminate", "undeclared_initial_acceptance_history")
                    }
                } else if !path_gaps.is_empty() {
                    (
                        "indeterminate",
                        "unreadable_fulfilled_save_before_promise_admission",
                    )
                } else {
                    path_support.push(1);
                    (
                        "violated",
                        "matching_acceptance_not_fulfilled_before_promise_admission",
                    )
                }
            }
        } else {
            path_counterevidence.push(report.dispatch);
            (
                "indeterminate",
                "authoritative_promise_send_join_unavailable",
            )
        };
        violated |= verdict == "violated";
        unavailable |= verdict == "indeterminate";
        path_support = unique(path_support);
        path_counterevidence = unique(path_counterevidence);
        path_gaps = unique(path_gaps);
        support.extend_from_slice(&path_support);
        counterevidence.extend_from_slice(&path_counterevidence);
        gaps.extend(path_gaps.iter().cloned());
        paths.push(json!({
            "report_dispatch_line":report.dispatch, "acceptor":report.envelope.from,
            "slot":entry.slot, "ballot":entry.ballot, "value":entry.value,
            "selected":selected, "promise_source":source,
            "matching_saves":matching_saves, "later_matching_saves":later_matching_saves,
            "verdict":verdict, "support":path_support,
            "counterevidence":path_counterevidence, "gaps":path_gaps, "reason":reason
        }));
    }
    paths.sort_by_key(|path| path["report_dispatch_line"].as_u64());
    let selection_known = !selection_lines.is_empty();
    let (verdict, reason) = if violated {
        (
            "violated",
            "at_least_one_frozen_report_entry_lacks_prior_acceptance_in_covered_history",
        )
    } else if unavailable || !selection_known {
        (
            "indeterminate",
            "some_frozen_report_entry_ancestry_is_unavailable",
        )
    } else {
        (
            "established",
            "every_relevant_frozen_report_entry_has_matching_prior_fulfilled_acceptance",
        )
    };
    support = unique(support);
    gaps = unique(gaps);
    Ancestry {
        report: json!({
            "verdict":verdict, "boundary_line":boundary, "support":support,
            "counterevidence":unique(counterevidence), "gaps":gaps, "reason":reason,
            "selection":{
                "verdict":if selection_known {"established"} else {"indeterminate"},
                "proposer":proposer, "campaign_ballot":ballot, "slot":slot,
                "ballot":selected_ballot, "value":value,
                "report_dispatch_lines":unique(selection_lines),
                "reason":"highest_ballot_entry_in_the_immutable_first_quorum_not_a_durable_history_claim"
            },
            "frozen_report_dispatch_lines":frozen_report_lines, "paths":paths
        }),
        support,
        gaps,
    }
}
