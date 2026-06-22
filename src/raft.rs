//! Raft consensus algorithm — faithful to the paper, readable as specification.

use crate::{Context, Envelope, Node, NodeId, Term};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

// ---------------------------------------------------------------------------
// Message types — exhaustive, compiler-enforced
//
// Indices are i64 with -1 meaning "before the first entry." A last/prev log
// term is Option<Term>: None when the log is empty, which orders below any
// real term exactly as the paper's comparison wants.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Message {
    #[serde(rename = "propose")]
    Propose { value: Value },

    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: i64,
        last_log_term: Option<Term>,
    },

    RequestVoteResponse {
        term: Term,
        vote_granted: bool,
    },

    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: i64,
        prev_log_term: Option<Term>,
        entries: Vec<(Term, Value)>,
        leader_commit: i64,
    },

    AppendEntriesResponse {
        term: Term,
        success: bool,
        match_index: i64,
    },

    #[serde(rename = "tick")]
    Tick,
}

// ---------------------------------------------------------------------------
// Role
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

// ---------------------------------------------------------------------------
// RaftNode
// ---------------------------------------------------------------------------

pub struct RaftNode {
    node_id: NodeId,
    quorum_size: i32,
    is_crashed: bool,

    // Persistent state (Raft paper Figure 2)
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<(Term, Value)>,

    // Volatile state
    commit_index: i64,
    last_applied: i64,
    role: Role,

    // Leader state — per-follower, keyed by node rather than indexed by a
    // node-id-as-usize into a Vec.
    next_index: BTreeMap<NodeId, i64>,
    match_index: BTreeMap<NodeId, i64>,

    // Election
    votes_received: BTreeSet<NodeId>,
    election_timeout: u64,
    ticks_since_leader: u64,
}

impl RaftNode {
    pub fn new(node_id: NodeId, quorum_size: i32) -> Self {
        Self {
            node_id,
            quorum_size,
            is_crashed: false,

            current_term: Term(0),
            voted_for: None,
            log: Vec::new(),

            commit_index: -1,
            last_applied: -1,
            role: Role::Follower,

            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),

            votes_received: BTreeSet::new(),
            election_timeout: 15 + (node_id.0 as u64 * 3),
            ticks_since_leader: 0,
        }
    }

    fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        (0..self.quorum_size)
            .map(NodeId)
            .filter(move |p| *p != self.node_id)
    }

    fn last_log_term(&self) -> Option<Term> {
        self.log.last().map(|(term, _)| *term)
    }

    fn last_log_index(&self) -> i64 {
        self.log.len() as i64 - 1
    }

    fn majority(&self) -> usize {
        (self.quorum_size / 2 + 1) as usize
    }

    fn send_append_entries(&self, ctx: &mut Context, to: NodeId) {
        let next_idx = self.next_index.get(&to).copied().unwrap_or(0);
        let entries: Vec<(Term, Value)> = if next_idx < self.log.len() as i64 {
            self.log[next_idx as usize..].to_vec()
        } else {
            Vec::new()
        };

        let prev_log_index = next_idx - 1;
        let prev_log_term = if prev_log_index >= 0 {
            Some(self.log[prev_log_index as usize].0)
        } else {
            None
        };

        ctx.send(
            to,
            serde_json::to_value(Message::AppendEntries {
                term: self.current_term,
                leader_id: self.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            })
            .unwrap(),
        );
    }

    fn step_down(&mut self, ctx: &mut Context, term: Term) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.ticks_since_leader = 0;
        ctx.save(serde_json::json!({"action": "step_down", "term": term}));
    }

    fn try_advance_commit(&mut self, ctx: &mut Context) {
        for n in (self.commit_index + 1..self.log.len() as i64).rev() {
            // Only commit entries from the current term (Raft paper section 5.4.2)
            if self.log[n as usize].0 != self.current_term {
                continue;
            }

            let replicas = self.match_index.values().filter(|&&idx| idx >= n).count();
            if replicas >= self.majority() {
                self.commit_index = n;
                self.apply_committed(ctx);
                break;
            }
        }
    }

    fn apply_committed(&mut self, ctx: &mut Context) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let (_, ref entry) = self.log[self.last_applied as usize];
            ctx.log(entry.clone());
        }
    }
}

impl Node for RaftNode {
    fn crashed(&self) -> bool {
        self.is_crashed
    }

    fn set_crashed(&mut self, crashed: bool) {
        self.is_crashed = crashed;
    }

    fn receive(&mut self, ctx: &mut Context, envelope: &Envelope) {
        let msg: Message = serde_json::from_value(envelope.payload.clone()).unwrap();

        match msg {
            // ---------------------------------------------------------------
            // Client proposal — leader only
            // ---------------------------------------------------------------
            Message::Propose { value } => {
                if self.role != Role::Leader {
                    return;
                }

                self.log.push((self.current_term, value.clone()));
                ctx.save(serde_json::json!({
                    "action": "append",
                    "term": self.current_term,
                    "entry": value,
                }));

                // The leader counts as one replica of its own entry.
                self.match_index.insert(self.node_id, self.last_log_index());

                for peer in self.peers().collect::<Vec<_>>() {
                    self.send_append_entries(ctx, peer);
                }
            }

            // ---------------------------------------------------------------
            // RequestVote
            // ---------------------------------------------------------------
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                if term > self.current_term {
                    self.step_down(ctx, term);
                }

                let mut vote_granted = false;

                if term >= self.current_term
                    && (self.voted_for.is_none() || self.voted_for == Some(candidate_id))
                {
                    // Grant only if the candidate's log is at least as up to
                    // date as ours (Raft paper section 5.4.1). Option<Term>
                    // orders None — the empty log — below any real term.
                    let up_to_date = last_log_term > self.last_log_term()
                        || (last_log_term == self.last_log_term()
                            && last_log_index >= self.last_log_index());

                    if up_to_date {
                        vote_granted = true;
                        self.voted_for = Some(candidate_id);
                        ctx.save(serde_json::json!({
                            "action": "vote",
                            "term": term,
                            "for": candidate_id,
                        }));
                    }
                }

                ctx.send(
                    envelope.from,
                    serde_json::to_value(Message::RequestVoteResponse {
                        term: self.current_term,
                        vote_granted,
                    })
                    .unwrap(),
                );
            }

            // ---------------------------------------------------------------
            // RequestVoteResponse
            // ---------------------------------------------------------------
            Message::RequestVoteResponse { term, vote_granted } => {
                if self.role != Role::Candidate {
                    return;
                }

                if term > self.current_term {
                    self.step_down(ctx, term);
                    return;
                }

                if vote_granted {
                    self.votes_received.insert(envelope.from);

                    if self.votes_received.len() >= self.majority() {
                        // Become leader
                        self.role = Role::Leader;
                        let next = self.log.len() as i64;
                        self.next_index = (0..self.quorum_size)
                            .map(|i| (NodeId(i), next))
                            .collect();
                        self.match_index =
                            (0..self.quorum_size).map(|i| (NodeId(i), -1)).collect();
                        self.match_index.insert(self.node_id, self.last_log_index());

                        for peer in self.peers().collect::<Vec<_>>() {
                            self.send_append_entries(ctx, peer);
                        }
                    }
                }
            }

            // ---------------------------------------------------------------
            // AppendEntries — log consistency check (Raft paper Figure 2)
            // ---------------------------------------------------------------
            Message::AppendEntries {
                term,
                leader_id: _,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.ticks_since_leader = 0;

                if term > self.current_term {
                    self.step_down(ctx, term);
                }

                let mut success = false;

                if term >= self.current_term {
                    self.role = Role::Follower;

                    // The previous entry must match in term, or there must be
                    // no previous entry required (prev_log_index == -1).
                    let log_ok = if prev_log_index == -1 {
                        true
                    } else if (prev_log_index as usize) < self.log.len() {
                        Some(self.log[prev_log_index as usize].0) == prev_log_term
                    } else {
                        false
                    };

                    if log_ok {
                        // Delete conflicting entries and append new ones.
                        for (i, entry) in entries.iter().enumerate() {
                            let log_index = prev_log_index + 1 + i as i64;
                            if (log_index as usize) < self.log.len() {
                                if self.log[log_index as usize].0 != entry.0 {
                                    self.log.truncate(log_index as usize);
                                    self.log.push(entry.clone());
                                    ctx.save(serde_json::json!({
                                        "action": "append",
                                        "term": entry.0,
                                        "entry": entry.1,
                                    }));
                                }
                            } else {
                                self.log.push(entry.clone());
                                ctx.save(serde_json::json!({
                                    "action": "append",
                                    "term": entry.0,
                                    "entry": entry.1,
                                }));
                            }
                        }

                        if leader_commit > self.commit_index {
                            self.commit_index = leader_commit.min(self.last_log_index());
                            self.apply_committed(ctx);
                        }

                        success = true;
                    }
                }

                ctx.send(
                    envelope.from,
                    serde_json::to_value(Message::AppendEntriesResponse {
                        term: self.current_term,
                        success,
                        match_index: if success { self.last_log_index() } else { -1 },
                    })
                    .unwrap(),
                );
            }

            // ---------------------------------------------------------------
            // AppendEntriesResponse
            // ---------------------------------------------------------------
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
            } => {
                if self.role != Role::Leader {
                    return;
                }

                if term > self.current_term {
                    self.step_down(ctx, term);
                    return;
                }

                let follower = envelope.from;

                if !success {
                    // Walk next_index back and retry.
                    let ni = self.next_index.entry(follower).or_insert(0);
                    if *ni > 0 {
                        *ni -= 1;
                    }
                    self.send_append_entries(ctx, follower);
                    return;
                }

                self.match_index.insert(follower, match_index);
                self.next_index.insert(follower, match_index + 1);

                self.try_advance_commit(ctx);
            }

            // ---------------------------------------------------------------
            // Tick — election timeout and heartbeats
            // ---------------------------------------------------------------
            Message::Tick => {
                self.ticks_since_leader += 1;

                match self.role {
                    Role::Follower | Role::Candidate => {
                        if self.ticks_since_leader >= self.election_timeout {
                            // Start an election.
                            self.current_term = self.current_term.next();
                            self.role = Role::Candidate;
                            self.voted_for = Some(self.node_id);
                            self.votes_received = BTreeSet::from([self.node_id]);
                            self.ticks_since_leader = 0;

                            ctx.save(serde_json::json!({
                                "action": "election",
                                "term": self.current_term,
                            }));

                            let last_log_index = self.last_log_index();
                            let last_log_term = self.last_log_term();

                            for peer in self.peers().collect::<Vec<_>>() {
                                ctx.send(
                                    peer,
                                    serde_json::to_value(Message::RequestVote {
                                        term: self.current_term,
                                        candidate_id: self.node_id,
                                        last_log_index,
                                        last_log_term,
                                    })
                                    .unwrap(),
                                );
                            }
                        }
                    }

                    Role::Leader => {
                        // Periodic heartbeats.
                        if self.ticks_since_leader % 5 == 0 {
                            for peer in self.peers().collect::<Vec<_>>() {
                                self.send_append_entries(ctx, peer);
                            }
                        }
                    }
                }
            }
        }
    }
}
