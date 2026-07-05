//! Raft consensus algorithm, being reseated onto the desire contract.
//!
//! This is intentionally a porting shell, not a claim that Raft is complete on
//! the new bench. The point of this level is to name the world components Raft
//! needs as the old direct-context implementation meets the new seam.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use crate::{Context, Input, Node, NodeId, Term};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Message {
    Propose {
        value: Value,
    },
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
}

pub fn propose(value: Value) -> Value {
    serde_json::to_value(Message::Propose { value }).expect("serialize raft propose")
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum PersistentRecord<'a> {
    CurrentTerm { term: Term },
    VotedFor { term: Term, voted_for: NodeId },
    ClearedVote { term: Term },
    LogAppend { term: Term, entry: &'a Value },
    LogTruncate { len: usize },
}

pub struct RaftNode {
    node_id: NodeId,
    quorum_size: i32,
    is_crashed: bool,

    // Persistent state (Raft paper Figure 2). The bench can record saves, but
    // it cannot yet rebuild this state on recover. That gap is inventoried on
    // Codex's shelf.
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<(Term, Value)>,

    // Volatile state.
    commit_index: i64,
    last_applied: i64,
    role: Role,

    // Leader state.
    next_index: BTreeMap<NodeId, i64>,
    match_index: BTreeMap<NodeId, i64>,

    // Election state.
    votes_received: BTreeSet<NodeId>,
    election_timeout: u64,
    ticks_since_leader: u64,
}

impl RaftNode {
    pub fn new(node_id: NodeId, quorum_size: i32) -> Self {
        Self::with_election_timeout(node_id, quorum_size, 15 + (node_id.0 as u64 * 3))
    }

    pub fn with_election_timeout(node_id: NodeId, quorum_size: i32, election_timeout: u64) -> Self {
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

            election_timeout,
            ticks_since_leader: 0,
            votes_received: BTreeSet::new(),
        }
    }

    fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        (0..self.quorum_size)
            .map(NodeId)
            .filter(move |peer| *peer != self.node_id)
    }

    fn majority(&self) -> usize {
        (self.quorum_size / 2 + 1) as usize
    }

    fn last_log_index(&self) -> i64 {
        self.log.len() as i64 - 1
    }

    fn last_log_term(&self) -> Option<Term> {
        self.log.last().map(|(term, _)| *term)
    }

    fn save_term(&self, ctx: &mut Context) {
        ctx.save(PersistentRecord::CurrentTerm {
            term: self.current_term,
        });
    }

    fn save_vote(&self, ctx: &mut Context, voted_for: NodeId) {
        ctx.save(PersistentRecord::VotedFor {
            term: self.current_term,
            voted_for,
        });
    }

    fn save_cleared_vote(&self, ctx: &mut Context) {
        ctx.save(PersistentRecord::ClearedVote {
            term: self.current_term,
        });
    }

    fn send_append_entries(&self, ctx: &mut Context, to: NodeId) {
        let next_idx = self.next_index.get(&to).copied().unwrap_or(0);
        let entries = if next_idx < self.log.len() as i64 {
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
            Message::AppendEntries {
                term: self.current_term,
                leader_id: self.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        );
    }

    fn step_down(&mut self, ctx: &mut Context, term: Term) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.ticks_since_leader = 0;
        self.save_term(ctx);
        self.save_cleared_vote(ctx);
    }

    fn start_election(&mut self, ctx: &mut Context) {
        self.current_term = self.current_term.next();
        self.role = Role::Candidate;
        self.voted_for = Some(self.node_id);
        self.votes_received = BTreeSet::from([self.node_id]);
        self.ticks_since_leader = 0;
        self.save_term(ctx);
        self.save_vote(ctx, self.node_id);

        let last_log_index = self.last_log_index();
        let last_log_term = self.last_log_term();
        for peer in self.peers().collect::<Vec<_>>() {
            ctx.send(
                peer,
                Message::RequestVote {
                    term: self.current_term,
                    candidate_id: self.node_id,
                    last_log_index,
                    last_log_term,
                },
            );
        }
    }

    fn become_leader(&mut self, ctx: &mut Context) {
        self.role = Role::Leader;
        let next = self.log.len() as i64;
        self.next_index = (0..self.quorum_size).map(|id| (NodeId(id), next)).collect();
        self.match_index = (0..self.quorum_size).map(|id| (NodeId(id), -1)).collect();
        self.match_index.insert(self.node_id, self.last_log_index());

        for peer in self.peers().collect::<Vec<_>>() {
            self.send_append_entries(ctx, peer);
        }
    }

    fn handle_message(&mut self, ctx: &mut Context, from: NodeId, message: Message) {
        match message {
            Message::Propose { value } => self.handle_propose(ctx, value),
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
                    let up_to_date = last_log_term > self.last_log_term()
                        || (last_log_term == self.last_log_term()
                            && last_log_index >= self.last_log_index());

                    if up_to_date {
                        vote_granted = true;
                        self.voted_for = Some(candidate_id);
                        self.save_vote(ctx, candidate_id);
                        // Granting a vote resets the election timer so the voter
                        // does not immediately mount a rival candidacy.
                        self.ticks_since_leader = 0;
                    }
                }

                ctx.send(
                    from,
                    Message::RequestVoteResponse {
                        term: self.current_term,
                        vote_granted,
                    },
                );
            }
            Message::RequestVoteResponse { term, vote_granted } => {
                self.handle_request_vote_response(ctx, from, term, vote_granted);
            }
            Message::AppendEntries {
                term,
                leader_id: _,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if term > self.current_term {
                    self.step_down(ctx, term);
                }

                // The last log index this RPC establishes agreement on. The
                // follower reports and bounds its commit by this, not by its own
                // last_log_index, which may carry a stale divergent tail this
                // RPC never confirmed.
                let last_new_index = prev_log_index + entries.len() as i64;
                let mut success = false;
                if term >= self.current_term {
                    // A current-term AppendEntries is from the live leader, so
                    // reset the election timer even if the log check fails. A
                    // stale-term one must not, or a deposed leader's heartbeat
                    // would suppress a needed election.
                    self.ticks_since_leader = 0;
                    self.role = Role::Follower;
                    let log_ok = if prev_log_index == -1 {
                        true
                    } else if (prev_log_index as usize) < self.log.len() {
                        Some(self.log[prev_log_index as usize].0) == prev_log_term
                    } else {
                        false
                    };

                    if log_ok {
                        self.merge_entries(ctx, prev_log_index, &entries);
                        if leader_commit > self.commit_index {
                            self.commit_index = leader_commit.min(last_new_index);
                            self.apply_committed(ctx);
                        }
                        success = true;
                    }
                }

                ctx.send(
                    from,
                    Message::AppendEntriesResponse {
                        term: self.current_term,
                        success,
                        match_index: if success { last_new_index } else { -1 },
                    },
                );
            }
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
            } => self.handle_append_entries_response(ctx, from, term, success, match_index),
        }
    }

    fn handle_propose(&mut self, ctx: &mut Context, value: Value) {
        if self.role != Role::Leader {
            return;
        }

        self.log.push((self.current_term, value.clone()));
        ctx.save(PersistentRecord::LogAppend {
            term: self.current_term,
            entry: &value,
        });
        self.match_index.insert(self.node_id, self.last_log_index());

        for peer in self.peers().collect::<Vec<_>>() {
            self.send_append_entries(ctx, peer);
        }
    }

    fn handle_request_vote_response(
        &mut self,
        ctx: &mut Context,
        from: NodeId,
        term: Term,
        vote_granted: bool,
    ) {
        if self.role != Role::Candidate {
            return;
        }
        if term > self.current_term {
            self.step_down(ctx, term);
            return;
        }
        // A response for anything but the current term is stale; drop it, or a
        // grant from an earlier candidacy would count toward this one.
        if term < self.current_term {
            return;
        }
        if vote_granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.majority() {
                self.become_leader(ctx);
            }
        }
    }

    fn merge_entries(&mut self, ctx: &mut Context, prev_log_index: i64, entries: &[(Term, Value)]) {
        for (offset, entry) in entries.iter().enumerate() {
            let log_index = prev_log_index + 1 + offset as i64;
            if (log_index as usize) < self.log.len() {
                if self.log[log_index as usize].0 != entry.0 {
                    self.log.truncate(log_index as usize);
                    ctx.save(PersistentRecord::LogTruncate {
                        len: log_index as usize,
                    });
                    self.log.push(entry.clone());
                    ctx.save(PersistentRecord::LogAppend {
                        term: entry.0,
                        entry: &entry.1,
                    });
                }
            } else {
                self.log.push(entry.clone());
                ctx.save(PersistentRecord::LogAppend {
                    term: entry.0,
                    entry: &entry.1,
                });
            }
        }
    }

    fn handle_append_entries_response(
        &mut self,
        ctx: &mut Context,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: i64,
    ) {
        if self.role != Role::Leader {
            return;
        }
        if term > self.current_term {
            self.step_down(ctx, term);
            return;
        }
        // A response for anything but the current term is stale; drop it, or a
        // stale-term success would inflate match_index.
        if term < self.current_term {
            return;
        }
        if !success {
            let next_index = self.next_index.entry(from).or_insert(0);
            if *next_index > 0 {
                *next_index -= 1;
            }
            self.send_append_entries(ctx, from);
            return;
        }

        self.match_index.insert(from, match_index);
        self.next_index.insert(from, match_index + 1);
        self.try_advance_commit(ctx);
    }

    fn try_advance_commit(&mut self, ctx: &mut Context) {
        for index in (self.commit_index + 1..self.log.len() as i64).rev() {
            if self.log[index as usize].0 != self.current_term {
                continue;
            }

            let replicas = self
                .match_index
                .values()
                .filter(|&&match_index| match_index >= index)
                .count();
            if replicas >= self.majority() {
                self.commit_index = index;
                self.apply_committed(ctx);
                break;
            }
        }
    }

    fn apply_committed(&mut self, ctx: &mut Context) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let (_, entry) = &self.log[self.last_applied as usize];
            ctx.commit(entry.clone());
        }
    }
}

impl Node for RaftNode {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        match input {
            Input::Tick { .. } => {
                self.ticks_since_leader += 1;
                match self.role {
                    Role::Follower | Role::Candidate => {
                        if self.ticks_since_leader >= self.election_timeout {
                            self.start_election(ctx);
                        }
                    }
                    Role::Leader => {
                        if self.ticks_since_leader.is_multiple_of(5) {
                            for peer in self.peers().collect::<Vec<_>>() {
                                self.send_append_entries(ctx, peer);
                            }
                        }
                    }
                }
            }
            Input::Message { envelope, .. } => {
                let message: Message = serde_json::from_value(envelope.payload.clone())
                    .expect("strict parse raft message");
                self.handle_message(ctx, envelope.from, message);
            }
            Input::Client { payload, .. } => {
                let message: Message = serde_json::from_value(payload.clone())
                    .expect("strict parse raft client input");
                self.handle_message(ctx, NodeId::CLIENT, message);
            }
        }
    }

    fn crashed(&self) -> bool {
        self.is_crashed
    }

    fn set_crashed(&mut self, crashed: bool) {
        self.is_crashed = crashed;
    }
}
