//! A transcript predicate for Raft's durable vote, independent of the inhabitant.
//! Parsing all known variants does not claim to verify Raft's log rules.
use crate::projection::Tape;
use crate::{DesireId, Input, MessageId, NodeId, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    CurrentTerm { term: u64 },
    VotedFor { term: u64, voted_for: NodeId },
    ClearedVote { term: u64 },
    LogAppend { term: u64, entry: Value },
    LogTruncate { len: u64 },
}
#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Message {
    Propose {
        value: Value,
    },
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: i64,
        last_log_term: Option<u64>,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: i64,
        prev_log_term: Option<u64>,
        entries: Vec<(u64, Value)>,
        leader_commit: i64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: i64,
    },
}
fn message(value: Value) -> Result<Message> {
    // Option denotes a required, nullable protocol field, not missing evidence.
    if value.get("type").and_then(Value::as_str) == Some("request_vote")
        && value.get("last_log_term").is_none()
    {
        return Err("request_vote is missing last_log_term".into());
    }
    if value.get("type").and_then(Value::as_str) == Some("append_entries")
        && value.get("prev_log_term").is_none()
    {
        return Err("append_entries is missing prev_log_term".into());
    }
    serde_json::from_value(value).map_err(|e| format!("invalid Raft message: {e}"))
}
#[derive(Default)]
struct VoteState {
    term: u64,
    candidate: Option<NodeId>,
    term_line: Option<usize>,
    vote_line: Option<usize>,
}
#[derive(Debug, Serialize)]
pub struct Grant {
    pub node: NodeId,
    pub term: u64,
    pub candidate: NodeId,
    pub request_line: usize,
    pub term_line: Option<usize>,
    pub term_origin: String,
    pub vote_line: usize,
    pub admission_line: usize,
    pub desire_id: DesireId,
    pub message_id: MessageId,
}
#[derive(Debug, Serialize)]
pub struct Report {
    pub source_sha256: String,
    pub line_numbers: String,
    pub grants: Vec<Grant>,
    pub tail: String,
}
pub fn verify(tape: &Tape) -> Result<Report> {
    let header = tape
        .projection
        .header
        .as_ref()
        .ok_or("vote check needs a header")?;
    let members: BTreeSet<_> = header.acceptors.iter().copied().collect();
    let mut states: BTreeMap<_, _> = header
        .nodes
        .iter()
        .filter(|n| n.algorithm == "raft")
        .map(|n| (n.node, VoteState::default()))
        .collect();
    if states.is_empty() {
        return Err("tape has no Raft inhabitants".into());
    }
    if states.keys().any(|id| !members.contains(id)) {
        return Err("Raft voter absent from configured acceptors".into());
    }
    let mut grants = Vec::new();
    for row in &tape.rows {
        let line = row.seq as usize + 1;
        let check = (|| -> Result<()> {
            if row.what == "dispatch" {
                let input = row.input()?;
                if states.contains_key(&input.to()) {
                    match input {
                        Input::Message { envelope, .. } => {
                            message(envelope.payload)?;
                        }
                        Input::Client { payload, .. } => {
                            message(payload)?;
                        }
                        _ => {}
                    }
                }
            }
            if row.what == "result"
                && row.why == "save"
                && row.get::<String>("outcome")? == "fulfilled"
            {
                let node: NodeId = row.get("node")?;
                if let Some(state) = states.get_mut(&node) {
                    let raw = row.value("record")?;
                    if raw.get("record").and_then(Value::as_str) == Some("log_append")
                        && raw.get("entry").is_none()
                    {
                        return Err("log_append is missing entry".into());
                    }
                    let record: Record = serde_json::from_value(raw)
                        .map_err(|e| format!("invalid Raft durable record: {e}"))?;
                    match record {
                        Record::CurrentTerm { term } => {
                            if term < state.term {
                                return Err("inconsistent durable prefix: term decreased".into());
                            }
                            if term > state.term {
                                state.term = term;
                                state.candidate = None;
                                state.vote_line = None;
                            }
                            state.term_line = Some(line);
                        }
                        Record::ClearedVote { term } => {
                            if term != state.term {
                                return Err(
                                    "inconsistent durable prefix: clear in wrong term".into()
                                );
                            }
                            // The term's established vote cannot be erased by a
                            // repeated same-term declaration or clear.
                        }
                        Record::VotedFor { term, voted_for } => {
                            if term != state.term || !members.contains(&voted_for) {
                                return Err(
                                    "inconsistent durable prefix: vote has wrong term or candidate"
                                        .into(),
                                );
                            }
                            if state.candidate.is_some_and(|old| old != voted_for) {
                                return Err("inconsistent durable prefix: second candidate in the same term".into());
                            }
                            state.candidate = Some(voted_for);
                            state.vote_line = Some(line);
                        }
                        Record::LogAppend { .. } | Record::LogTruncate { .. } => {}
                    }
                }
            }
            if row.what == "attempt" && row.why == "send" {
                let node: NodeId = row.get("node")?;
                if let Some(state) = states.get(&node)
                    && let Message::RequestVoteResponse {
                        term,
                        vote_granted: true,
                    } = message(row.value("message")?)?
                {
                    let input_id = row.get("parent_input_id")?;
                    let input = tape
                        .projection
                        .inputs
                        .get(&input_id)
                        .ok_or("missing request ancestry")?;
                    let envelope = match input {
                        Input::Message { envelope, .. } => envelope,
                        _ => {
                            return Err("grant ancestry is not a dispatched peer request".into());
                        }
                    };
                    let (request_term, candidate) = match message(envelope.payload.clone())? {
                        Message::RequestVote {
                            term, candidate_id, ..
                        } => (term, candidate_id),
                        _ => return Err("grant ancestry is not RequestVote".into()),
                    };
                    let to: NodeId = row.get("to")?;
                    if envelope.to != node
                        || candidate != envelope.from
                        || to != candidate
                        || request_term != term
                        || !members.contains(&candidate)
                    {
                        return Err("request identity mismatch at granted-vote admission".into());
                    }
                    if state.term != term
                        || state.candidate != Some(candidate)
                        || state.vote_line.is_none()
                    {
                        return Err(
                            "unsupported granted-vote attempt admitted before durable support"
                                .into(),
                        );
                    }
                    grants.push(Grant {
                        node,
                        term,
                        candidate,
                        request_line: tape.projection.input_lines[&input_id],
                        term_line: state.term_line,
                        term_origin: if state.term_line.is_some() {
                            "fulfilled_save"
                        } else {
                            "initial_state_term_zero"
                        }
                        .into(),
                        vote_line: state.vote_line.unwrap(),
                        admission_line: line,
                        desire_id: row.get("desire_id")?,
                        message_id: row.get("message_id")?,
                    });
                }
            }
            Ok(())
        })();
        check.map_err(|e| format!("line {line}: {e}"))?;
    }
    Ok(Report {
        source_sha256: tape.sha256.clone(),
        line_numbers: "one-based JSONL lines; hash covers original bytes".into(),
        grants,
        tail: tape.projection.tail().into(),
    })
}
