//! Interpret complete rows, including valid unfinished prefixes. No inhabitant
//! code runs here; fulfilled effects alone form the durable and chosen views.
use crate::tape::{self, Event, Header, actor};
use crate::{Desire, DesireId, Envelope, Input, InputId, MessageId, NodeId, Result, Slot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug)]
pub struct Answer {
    pub node: NodeId,
    pub input_id: InputId,
    pub desires: Vec<(DesireId, Desire)>,
    pub closed: bool,
}
#[derive(Clone, Debug)]
struct Drain {
    input_id: InputId,
    next: usize,
    attempt: Option<DesireId>,
}
#[derive(Clone, Debug)]
struct Step {
    ready: VecDeque<MessageId>,
    ticks: VecDeque<NodeId>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pending {
    pub message_id: MessageId,
    pub status: String,
    pub blocked_until: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Account {
    pub active_input: Option<InputId>,
    pub unreached_desires: Vec<DesireId>,
    pub open_attempts: Vec<DesireId>,
    pub queued: Vec<Pending>,
    pub arrived: Vec<MessageId>,
    pub clients: Vec<InputId>,
    pub due_messages: Vec<MessageId>,
    pub due_ticks: Vec<NodeId>,
    pub no_progress_due: Option<u64>,
}
#[derive(Clone, Debug, Default)]
pub struct Projection {
    pub header: Option<Header>,
    pub tick: u64,
    pub persistent: BTreeMap<NodeId, Vec<Value>>,
    pub chosen: BTreeMap<NodeId, BTreeMap<Slot, Value>>,
    pub pending: Vec<Envelope>,
    pub arrived: BTreeMap<MessageId, Envelope>,
    pub partitions: BTreeMap<(NodeId, NodeId), u64>,
    pub nodes: BTreeMap<NodeId, bool>, // true means crashed, with retained memory
    pub inputs: BTreeMap<InputId, Input>,
    pub input_lines: BTreeMap<InputId, usize>,
    pub answers: BTreeMap<InputId, Answer>,
    pub history: BTreeMap<MessageId, Vec<(usize, String)>>,
    pub finalized: bool,
    pub stopped: Option<String>,
    pub effects: usize,
    clients: BTreeMap<InputId, Input>,
    active: Option<Drain>,
    next_input: u64,
    next_desire: u64,
    next_message: u64,
    rows: usize,
    last_event: String,
    step: Option<Step>,
    no_progress_due: Option<u64>,
    last_progress: u64,
    chosen_count: usize,
}
pub struct Tape {
    pub rows: Vec<Event>,
    pub projection: Projection,
    pub sha256: String,
}
impl Tape {
    pub fn read(bytes: &[u8]) -> Result<Self> {
        let rows = tape::decode(bytes)?;
        let projection = Projection::from_rows(&rows)?;
        Ok(Self {
            rows,
            projection,
            sha256: tape::hash(bytes),
        })
    }
}
impl Projection {
    pub fn from_rows(rows: &[Event]) -> Result<Self> {
        let mut out = Self::default();
        for row in rows {
            out.apply(row)?;
        }
        Ok(out)
    }
    pub fn apply(&mut self, row: &Event) -> Result<()> {
        self.consume(row)
            .map_err(|e| format!("line {}: {e}", self.rows + 1))?;
        self.finish_step();
        self.rows += 1;
        self.last_event = row.what.clone();
        Ok(())
    }
    fn consume(&mut self, row: &Event) -> Result<()> {
        row.check_shape()?;
        require(
            row.seq == self.rows as u64,
            "event sequence is not consecutive",
        )?;
        require(!self.finalized, "event after scenario finalization")?;
        if self.rows == 0 {
            require(
                row.what == "header" && row.when == 0 && row.who == "bench",
                "first row must be the header at tick zero",
            )?;
            let header: Header = serde_json::from_value(Value::Object(row.fields.clone()))
                .map_err(|e| e.to_string())?;
            require(
                serde_json::to_value(&header).map_err(|e| e.to_string())?
                    == Value::Object(row.fields.clone()),
                "header is missing a required nested field",
            )?;
            require(
                header.schema == 1 && header.clock == "logical_tick",
                "unsupported schema or clock",
            )?;
            require(
                header
                    .world
                    .base_latency
                    .checked_add(header.world.jitter)
                    .is_some(),
                "latency range overflows",
            )?;
            require(!header.nodes.is_empty(), "topology is empty")?;
            let mut ids = BTreeSet::new();
            for node in &header.nodes {
                require(
                    node.node.0 >= 0 && ids.insert(node.node),
                    "invalid or repeated node identity",
                )?;
                require(!node.algorithm.is_empty(), "algorithm identity is empty")?;
            }
            let mut members = BTreeSet::new();
            for node in &header.acceptors {
                require(
                    ids.contains(node) && members.insert(*node),
                    "invalid or repeated acceptor membership",
                )?;
            }
            require(
                !header.implementation.crate_name.is_empty()
                    && !header.implementation.version.is_empty(),
                "empty implementation identity",
            )?;
            require(
                hex(&header.implementation.revision, 40)
                    && hex(&header.implementation.source_sha256, 64),
                "invalid implementation content identity",
            )?;
            self.header = Some(header);
            return Ok(());
        }
        require(row.what != "header", "second header")?;
        if row.what == "advance" {
            require(
                row.when == self.tick.checked_add(1).ok_or("clock overflow")?,
                "clock must advance one tick",
            )?;
        } else {
            require(
                row.when == self.tick,
                "event time changed without an advance row",
            )?;
        }
        if self.stopped.is_some() {
            require(row.what == "finalize", "execution continued after refusal")?;
        }
        if self.active.is_some() {
            require(
                matches!(
                    row.what.as_str(),
                    "desire" | "close" | "attempt" | "result" | "finalize"
                ),
                "new world activity before the current turn drained",
            )?;
        }
        // A prefix may stop before owed work. A later row cannot skip it.
        // Explicit interrupted/refused finalization records the remaining work.
        if row.what != "finalize" && self.active.is_none() {
            if self.no_progress_due.is_some() {
                require(
                    row.what == "deadlock",
                    "required no-progress observation omitted",
                )?;
            } else if let Some((id, _)) = self.clients.first_key_value() {
                require(
                    matches!(row.what.as_str(), "dispatch" | "drop")
                        && row.why == "client"
                        && row.get::<InputId>("input_id")? == *id,
                    "external client input must be dispatched or dropped immediately",
                )?;
            } else if let Some((id, _)) = self.arrived.first_key_value() {
                require(
                    row.what == "dispatch"
                        && row.why == "message"
                        && row.get::<MessageId>("message_id")? == *id,
                    "arrival must be followed immediately by its dispatch",
                )?;
            } else if let Some(step) = &self.step {
                if let Some(id) = step.ready.front() {
                    require(
                        matches!(row.what.as_str(), "arrive" | "drop")
                            && row.why == "message"
                            && row.get::<MessageId>("message_id")? == *id,
                        "frozen ready queue must drain in order before ticks or other work",
                    )?;
                } else if let Some(node) = step.ticks.front() {
                    require(
                        row.what == "dispatch"
                            && row.why == "tick"
                            && row.get::<NodeId>("to")? == *node,
                        "configured tick dispatch omitted or reordered",
                    )?;
                }
            }
        }
        let expected_actor = match row.what.as_str() {
            "dispatch" => actor(row.input()?.to()),
            "desire" | "close" => actor(row.get("node")?),
            _ => "bench".into(),
        };
        require(
            row.who == expected_actor,
            "actor does not own the witnessed operation",
        )?;
        match row.what.as_str() {
            "advance" => {
                self.tick = row.when;
                let ready = self
                    .pending
                    .iter()
                    .filter(|e| e.deliver_tick <= self.tick && !self.blocked(e))
                    .map(|e| e.message_id)
                    .collect();
                let interval = self.header.as_ref().unwrap().world.tick_interval;
                let ticks = if interval > 0 && self.tick.is_multiple_of(interval) {
                    self.nodes
                        .iter()
                        .filter(|(_, crashed)| !**crashed)
                        .map(|(node, _)| *node)
                        .collect()
                } else {
                    VecDeque::new()
                };
                self.step = Some(Step { ready, ticks });
            }
            "spawn" => {
                let node: NodeId = row.get("node")?;
                require(
                    self.header
                        .as_ref()
                        .unwrap()
                        .nodes
                        .iter()
                        .any(|n| n.node == node),
                    "spawn outside topology",
                )?;
                require(!self.nodes.contains_key(&node), "node spawned twice")?;
                self.nodes.insert(node, false);
                self.persistent.insert(node, Vec::new());
                self.chosen.insert(node, BTreeMap::new());
            }
            "crash" | "recover" => {
                let node: NodeId = row.get("node")?;
                let state = self
                    .nodes
                    .get_mut(&node)
                    .ok_or("lifecycle operation for absent node")?;
                if row.what == "recover" {
                    require(
                        row.get::<String>("model")? == "retained_state",
                        "honest recovery is outside this schema",
                    )?;
                }
                require(
                    *state != (row.what == "crash"),
                    "lifecycle transition does not change state",
                )?;
                *state = row.what == "crash";
            }
            "partition" => {
                let from: NodeId = row.get("from")?;
                let to: NodeId = row.get("to")?;
                let until: u64 = row.get("until")?;
                require(from != to && until >= self.tick, "invalid partition")?;
                self.partitions.insert((from, to), until);
            }
            "external" => {
                let input = row.input()?;
                self.allocate_input(input.input_id())?;
                self.clients.insert(input.input_id(), input);
            }
            "dispatch" => {
                require(self.active.is_none(), "overlapping dispatch")?;
                let input = row.input()?;
                let node = input.to();
                self.alive(node)?;
                require(
                    !self.inputs.contains_key(&input.input_id()),
                    "input dispatched twice",
                )?;
                match &input {
                    Input::Client { input_id, .. } => {
                        require(
                            self.clients.remove(input_id).as_ref() == Some(&input),
                            "client dispatch differs from external input",
                        )?;
                    }
                    Input::Message { envelope, .. } => {
                        require(
                            self.arrived.remove(&envelope.message_id).as_ref() == Some(envelope),
                            "dispatch without matching arrival",
                        )?;
                        self.allocate_input(input.input_id())?;
                        self.history
                            .get_mut(&envelope.message_id)
                            .unwrap()
                            .push((self.rows + 1, "dispatched".into()));
                    }
                    Input::Tick { .. } => {
                        let interval = self.header.as_ref().unwrap().world.tick_interval;
                        require(
                            interval > 0 && self.tick > 0 && self.tick.is_multiple_of(interval),
                            "tick outside configured policy",
                        )?;
                        let step = self.step.as_mut().ok_or("tick dispatch outside a step")?;
                        require(
                            step.ready.is_empty() && step.ticks.front() == Some(&node),
                            "tick dispatch does not match step policy",
                        )?;
                        step.ticks.pop_front();
                        self.allocate_input(input.input_id())?;
                    }
                }
                let id = input.input_id();
                self.inputs.insert(id, input);
                self.input_lines.insert(id, self.rows + 1);
                self.answers.insert(
                    id,
                    Answer {
                        node,
                        input_id: id,
                        desires: Vec::new(),
                        closed: false,
                    },
                );
                self.active = Some(Drain {
                    input_id: id,
                    next: 0,
                    attempt: None,
                });
            }
            "desire" => {
                let active = self.active.as_ref().ok_or("desire without dispatch")?;
                let answer = self.answers.get_mut(&active.input_id).unwrap();
                require(!answer.closed, "desire after answer close")?;
                require(
                    row.get::<NodeId>("node")? == answer.node
                        && row.get::<InputId>("parent_input_id")? == answer.input_id,
                    "desire ancestry mismatch",
                )?;
                let id: DesireId = row.get("desire_id")?;
                require(
                    id.0 == self.next_desire,
                    "desire identity is not consecutive",
                )?;
                self.next_desire += 1;
                answer.desires.push((id, row.desire()?));
            }
            "close" => {
                let active = self.active.as_ref().ok_or("close without dispatch")?;
                let answer = self.answers.get_mut(&active.input_id).unwrap();
                require(!answer.closed, "answer closed twice")?;
                require(
                    row.get::<NodeId>("node")? == answer.node
                        && row.get::<InputId>("input_id")? == answer.input_id,
                    "close ancestry mismatch",
                )?;
                require(
                    row.get::<u64>("count")? == answer.desires.len() as u64,
                    "answer count mismatch",
                )?;
                answer.closed = true;
                if answer.desires.is_empty() {
                    self.active = None;
                }
            }
            "attempt" => {
                self.effect_matches(row)?;
                let active = self.active.as_mut().unwrap();
                require(active.attempt.is_none(), "overlapping effect attempts")?;
                let id: DesireId = row.get("desire_id")?;
                active.attempt = Some(id);
                if row.why == "send" {
                    let message: MessageId = row.get("message_id")?;
                    require(
                        message.0 == self.next_message,
                        "message identity is not consecutive",
                    )?;
                    self.next_message += 1;
                    self.history
                        .insert(message, vec![(self.rows + 1, "attempted".into())]);
                }
            }
            "result" => {
                self.effect_matches(row)?;
                let active = self.active.as_ref().unwrap();
                require(
                    active.attempt == Some(row.get("desire_id")?),
                    "result without matching attempt",
                )?;
                let node: NodeId = row.get("node")?;
                let outcome: String = row.get("outcome")?;
                let reason: Option<String> = row.get("reason")?;
                let refused = if let Desire::Commit { slot, entry } = row.desire()? {
                    agreement_refusal(&self.chosen, node, slot, &entry)
                } else {
                    None
                };
                require(
                    reason == refused
                        && outcome
                            == if refused.is_some() {
                                "refused"
                            } else {
                                "fulfilled"
                            },
                    "effect result contradicts world admission rules",
                )?;
                if refused.is_none() {
                    match row.desire()? {
                        Desire::Save { record } => {
                            self.persistent.get_mut(&node).unwrap().push(record)
                        }
                        Desire::Commit { slot, entry } => {
                            self.chosen.get_mut(&node).unwrap().insert(slot, entry);
                        }
                        Desire::Send { .. } => {
                            let envelope = row.send_envelope()?;
                            let history = self
                                .history
                                .get(&envelope.message_id)
                                .ok_or("send result without message attempt")?;
                            require(
                                history.last().map(|v| v.1.as_str()) == Some("attempted"),
                                "message already has a result",
                            )?;
                            // The sole open Send owns the most recently assigned identity.
                            require(
                                envelope.message_id.0 + 1 == self.next_message,
                                "result changed attempted message identity",
                            )?;
                            let delay: u64 = row.get("delay")?;
                            let config = &self.header.as_ref().unwrap().world;
                            require(
                                delay >= config.base_latency
                                    && delay <= config.base_latency + config.jitter,
                                "delay outside configured range",
                            )?;
                            require(
                                envelope.send_tick == self.tick
                                    && self.tick.checked_add(delay) == Some(envelope.deliver_tick),
                                "schedule does not match latency choice",
                            )?;
                            self.history
                                .get_mut(&envelope.message_id)
                                .unwrap()
                                .push((self.rows + 1, "scheduled".into()));
                            self.pending.push(envelope);
                        }
                    }
                }
                self.effects += 1;
                let active = self.active.as_mut().unwrap();
                active.next += 1;
                active.attempt = None;
                if let Some(reason) = refused {
                    self.stopped = Some(reason);
                } else if active.next == self.answers[&active.input_id].desires.len() {
                    self.active = None;
                }
            }
            "arrive" | "drop" if row.why == "message" => {
                let envelope = row.envelope()?;
                let step = self
                    .step
                    .as_mut()
                    .ok_or("network crossing outside a step")?;
                require(
                    step.ready.front() == Some(&envelope.message_id),
                    "network crossing is not the earliest frozen ready envelope",
                )?;
                step.ready.pop_front();
                let index = self
                    .pending
                    .iter()
                    .position(|m| m.message_id == envelope.message_id)
                    .ok_or("network event without queued message")?;
                require(
                    self.pending[index] == envelope,
                    "network witness changed envelope",
                )?;
                require(
                    envelope.send_tick < self.tick && envelope.deliver_tick <= self.tick,
                    "message crossed before it was deliverable",
                )?;
                require(!self.blocked(&envelope), "partitioned message crossed")?;
                if row.what == "arrive" {
                    self.alive(envelope.to)?;
                    self.arrived.insert(envelope.message_id, envelope.clone());
                } else {
                    require(
                        row.get::<String>("reason")?
                            == self
                                .unavailable(envelope.to)
                                .ok_or("drop of available recipient")?,
                        "incorrect drop reason",
                    )?;
                }
                self.pending.remove(index);
                self.history.get_mut(&envelope.message_id).unwrap().push((
                    self.rows + 1,
                    if row.what == "arrive" {
                        "arrived"
                    } else {
                        "dropped"
                    }
                    .into(),
                ));
            }
            "drop" if row.why == "client" => {
                let input = row.input()?;
                require(
                    self.clients.remove(&input.input_id()).as_ref() == Some(&input),
                    "client drop without matching external input",
                )?;
                require(
                    row.get::<String>("reason")?
                        == self
                            .unavailable(input.to())
                            .ok_or("client drop of available node")?,
                    "incorrect client-drop reason",
                )?;
            }
            "deadlock" => {
                let elapsed: u64 = row.get("ticks_since_progress")?;
                require(
                    self.no_progress_due == Some(elapsed),
                    "no-progress observation contradicts distinct-slot progress",
                )?;
                self.no_progress_due = None;
            }
            "finalize" => {
                let reason: String = row.get("reason")?;
                require(
                    matches!(
                        reason.as_str(),
                        "horizon" | "refused" | "no_progress" | "interrupted"
                    ),
                    "unknown finalization reason",
                )?;
                if reason == "refused" {
                    require(
                        self.stopped.is_some(),
                        "refused finalization without refusal",
                    )?;
                }
                if self.stopped.is_some() {
                    require(
                        reason == "refused",
                        "finalization must name the witnessed refusal",
                    )?;
                }
                if reason == "no_progress" {
                    require(
                        self.last_event == "deadlock",
                        "no-progress finalization without its observation",
                    )?;
                }
                if reason == "horizon" || reason == "no_progress" {
                    require(
                        self.active.is_none()
                            && self.clients.is_empty()
                            && self.arrived.is_empty()
                            && self.step.is_none()
                            && self.no_progress_due.is_none(),
                        "ordinary finalization during unfinished local work",
                    )?;
                }
                require(
                    row.value("account")?
                        == serde_json::to_value(self.account()).map_err(|e| e.to_string())?,
                    "terminal account disagrees with witnessed prefix",
                )?;
                self.finalized = true;
            }
            _ => return Err("unhandled event variant".into()),
        }
        Ok(())
    }
    fn finish_step(&mut self) {
        if self.finalized
            || self.stopped.is_some()
            || self.active.is_some()
            || !self.arrived.is_empty()
            || !self.clients.is_empty()
        {
            return;
        }
        if self
            .step
            .as_ref()
            .is_some_and(|s| s.ready.is_empty() && s.ticks.is_empty())
        {
            self.step = None;
            let count = self
                .chosen
                .values()
                .flat_map(|log| log.keys().copied())
                .collect::<BTreeSet<_>>()
                .len();
            if count > self.chosen_count {
                self.chosen_count = count;
                self.last_progress = self.tick;
            } else {
                let elapsed = self.tick - self.last_progress;
                if elapsed > self.header.as_ref().unwrap().world.deadlock_threshold {
                    self.no_progress_due = Some(elapsed);
                }
            }
        }
    }
    fn allocate_input(&mut self, id: InputId) -> Result<()> {
        require(id.0 == self.next_input, "input identity is not consecutive")?;
        self.next_input += 1;
        Ok(())
    }
    fn alive(&self, node: NodeId) -> Result<()> {
        require(
            self.nodes.get(&node) == Some(&false),
            "dispatch or arrival to unavailable node",
        )
    }
    fn unavailable(&self, node: NodeId) -> Option<&'static str> {
        match self.nodes.get(&node) {
            None => Some("missing_recipient"),
            Some(true) => Some("crashed_recipient"),
            Some(false) => None,
        }
    }
    fn effect_matches(&self, row: &Event) -> Result<()> {
        let active = self
            .active
            .as_ref()
            .ok_or("effect without a closed answer")?;
        let answer = &self.answers[&active.input_id];
        require(answer.closed, "effect before answer close")?;
        let (id, desire) = answer
            .desires
            .get(active.next)
            .ok_or("effect beyond returned batch")?;
        require(
            row.get::<NodeId>("node")? == answer.node
                && row.get::<InputId>("parent_input_id")? == answer.input_id
                && row.get::<DesireId>("desire_id")? == *id
                && row.desire()? == *desire,
            "effect differs from next returned desire",
        )
    }
    fn blocked(&self, e: &Envelope) -> bool {
        self.partitions
            .get(&(e.from, e.to))
            .is_some_and(|until| *until > self.tick)
    }
    pub fn effective_partitions(&self) -> BTreeMap<(NodeId, NodeId), u64> {
        self.partitions
            .iter()
            .filter(|(_, until)| **until > self.tick)
            .map(|(k, v)| (*k, *v))
            .collect()
    }
    pub fn account(&self) -> Account {
        let mut unreached = Vec::new();
        let mut open = Vec::new();
        if let Some(active) = &self.active {
            let answer = &self.answers[&active.input_id];
            if let Some(id) = active.attempt {
                open.push(id);
            }
            unreached.extend(
                answer
                    .desires
                    .iter()
                    .skip(active.next + usize::from(active.attempt.is_some()))
                    .map(|(id, _)| *id),
            );
        }
        Account {
            active_input: self.active.as_ref().map(|a| a.input_id),
            unreached_desires: unreached,
            open_attempts: open,
            queued: self
                .pending
                .iter()
                .map(|e| Pending {
                    message_id: e.message_id,
                    status: if self.blocked(e) {
                        "blocked"
                    } else {
                        "pending"
                    }
                    .into(),
                    blocked_until: self
                        .partitions
                        .get(&(e.from, e.to))
                        .copied()
                        .filter(|t| *t > self.tick),
                })
                .collect(),
            arrived: self.arrived.keys().copied().collect(),
            clients: self.clients.keys().copied().collect(),
            due_messages: self
                .step
                .as_ref()
                .map(|s| s.ready.iter().copied().collect())
                .unwrap_or_default(),
            due_ticks: self
                .step
                .as_ref()
                .map(|s| s.ticks.iter().copied().collect())
                .unwrap_or_default(),
            no_progress_due: self.no_progress_due,
        }
    }
    pub fn tail(&self) -> &'static str {
        if self.header.is_none() {
            "empty"
        } else if self.finalized {
            "finalized"
        } else if self.stopped.is_some() {
            "refused"
        } else if let Some(active) = &self.active {
            let answer = &self.answers[&active.input_id];
            if !answer.closed {
                if answer.desires.is_empty() {
                    "open_invocation"
                } else {
                    "partial_answer"
                }
            } else if active.attempt.is_some() {
                "open_effect"
            } else {
                "interrupted_drain"
            }
        } else if self.no_progress_due.is_some() {
            "awaiting_no_progress"
        } else if !self.clients.is_empty() || !self.arrived.is_empty() || self.step.is_some() {
            "awaiting_dispatch"
        } else {
            "between_turns"
        }
    }
    pub fn summary(&self) -> Value {
        json!({"rows":self.rows,"tick":self.tick,"tail":self.tail(),"persistent":self.persistent,"chosen":self.chosen,"account":self.account(),"effects":self.effects})
    }
}
fn require(test: bool, message: &str) -> Result<()> {
    if test { Ok(()) } else { Err(message.into()) }
}
fn hex(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Claim policy remains the slice-1 policy. Honest recovery must revisit the
/// identical-claim case; retained-memory resumption does not resolve it.
pub fn agreement_refusal(
    chosen: &BTreeMap<NodeId, BTreeMap<Slot, Value>>,
    node: NodeId,
    slot: Slot,
    entry: &Value,
) -> Option<String> {
    if let Some(previous) = chosen.get(&node).and_then(|log| log.get(&slot)) {
        return Some(
            if previous == entry {
                "duplicate_commit"
            } else {
                "self_split_brain"
            }
            .into(),
        );
    }
    if chosen
        .iter()
        .any(|(other, log)| *other != node && log.get(&slot).is_some_and(|v| v != entry))
    {
        return Some("split_brain".into());
    }
    None
}
