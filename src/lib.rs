//! Quorum — deterministic bench for consensus inhabitants.

pub mod ping_pong;
pub mod raft;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Protocol vocabulary
// ---------------------------------------------------------------------------

/// A node's identity. Client-originated inputs use the client sentinel until a
/// later level gives client traffic its own source vocabulary.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct NodeId(pub i32);

impl NodeId {
    pub const CLIENT: NodeId = NodeId(-1);
}

/// An election term. Raft is not active in this level, but the vocabulary stays
/// because it is part of the consensus domain rather than Raft scaffolding.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Term(pub u64);

impl Term {
    pub fn next(self) -> Term {
        Term(self.0 + 1)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct InputId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct DesireId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct MessageId(pub u64);

// ---------------------------------------------------------------------------
// Desires — what an inhabitant asks the world to do
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Desire {
    Send { to: NodeId, message: Value },
    Save { record: Value },
    Commit { entry: Value },
}

// ---------------------------------------------------------------------------
// Event log — one flat JSONL grammar
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum Actor {
    Bench,
    Node(NodeId),
}

impl Serialize for Actor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Actor::Bench => serializer.serialize_str("bench"),
            Actor::Node(node_id) => serializer.serialize_str(&format!("node:{}", node_id.0)),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Spawn,
    Deliver,
    Receive,
    Desire,
    Schedule,
    Drop,
    Save,
    Commit,
    Crash,
    Partition,
    Recover,
    Pending,
    Deadlock,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventReason {
    Node,
    Tick,
    Message,
    Client,
    Send,
    Save,
    Commit,
    NoProgress,
    RunEnded,
}

#[derive(Clone, Debug, Serialize)]
pub struct Event {
    pub when: u64,
    pub who: Actor,
    pub what: EventKind,
    pub why: EventReason,
    #[serde(flatten)]
    pub payload: Value,
}

impl Event {
    pub fn new(when: u64, who: Actor, what: EventKind, why: EventReason, payload: Value) -> Self {
        Self {
            when,
            who,
            what,
            why,
            payload,
        }
    }
}

// ---------------------------------------------------------------------------
// Inputs and envelopes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Input {
    Tick {
        input_id: InputId,
        to: NodeId,
    },
    Client {
        input_id: InputId,
        to: NodeId,
        payload: Value,
    },
    Message {
        input_id: InputId,
        envelope: Envelope,
    },
}

impl Input {
    pub fn input_id(&self) -> InputId {
        match self {
            Input::Tick { input_id, .. }
            | Input::Client { input_id, .. }
            | Input::Message { input_id, .. } => *input_id,
        }
    }

    pub fn to(&self) -> NodeId {
        match self {
            Input::Tick { to, .. } => *to,
            Input::Client { to, .. } => *to,
            Input::Message { envelope, .. } => envelope.to,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Envelope {
    pub message_id: MessageId,
    pub parent_desire_id: DesireId,
    pub from: NodeId,
    pub to: NodeId,
    pub payload: Value,
    pub send_tick: u64,
    pub deliver_tick: u64,
}

// ---------------------------------------------------------------------------
// Node — every inhabitant implements this
// ---------------------------------------------------------------------------

pub trait Node {
    fn receive(&mut self, ctx: &mut Context, input: &Input);
    fn crashed(&self) -> bool;
    fn set_crashed(&mut self, crashed: bool);
}

// ---------------------------------------------------------------------------
// Context — typed call-site helpers, concrete Value boundary
// ---------------------------------------------------------------------------

pub struct Context<'a> {
    pub node_id: NodeId,
    desires: &'a mut Vec<Desire>,
}

impl<'a> Context<'a> {
    pub fn new(node_id: NodeId, desires: &'a mut Vec<Desire>) -> Self {
        Self { node_id, desires }
    }

    pub fn send<T: Serialize>(&mut self, to: NodeId, message: T) {
        let message = serde_json::to_value(message).expect("serialize send desire");
        self.desires.push(Desire::Send { to, message });
    }

    pub fn save<T: Serialize>(&mut self, record: T) {
        let record = serde_json::to_value(record).expect("serialize save desire");
        self.desires.push(Desire::Save { record });
    }

    pub fn commit<T: Serialize>(&mut self, entry: T) {
        let entry = serde_json::to_value(entry).expect("serialize commit desire");
        self.desires.push(Desire::Commit { entry });
    }
}

// ---------------------------------------------------------------------------
// Network — pseudo-network with controllable delivery
// ---------------------------------------------------------------------------

pub struct Network {
    pub base_latency: u64,
    pub jitter: u64,
    pub pending: Vec<Envelope>,
    pub partitions: BTreeMap<(NodeId, NodeId), u64>,
    rng: StdRng,
}

impl Network {
    pub fn new(base_latency: u64, jitter: u64, seed: u64) -> Self {
        Self {
            base_latency,
            jitter,
            pending: Vec::new(),
            partitions: BTreeMap::new(),
            rng: StdRng::seed_from_u64(seed),
        }
    }

    pub fn enqueue(&mut self, mut envelope: Envelope, current_tick: u64) -> u64 {
        let delay = self.base_latency + self.rng.gen_range(0..=self.jitter);
        envelope.deliver_tick = current_tick + delay;
        self.pending.push(envelope);
        delay
    }

    pub fn deliverable(&mut self, tick: u64) -> Vec<Envelope> {
        let mut ready = Vec::new();
        let mut remaining = Vec::new();

        for envelope in self.pending.drain(..) {
            if envelope.deliver_tick > tick {
                remaining.push(envelope);
                continue;
            }

            let key = (envelope.from, envelope.to);
            if let Some(&until) = self.partitions.get(&key) {
                if until > tick {
                    remaining.push(envelope);
                    continue;
                }
                self.partitions.remove(&key);
            }

            ready.push(envelope);
        }

        self.pending = remaining;
        ready
    }

    pub fn partition(&mut self, from: NodeId, to: NodeId, until_tick: u64) {
        self.partitions.insert((from, to), until_tick);
    }
}

// ---------------------------------------------------------------------------
// Simulator — monomorphic world
// ---------------------------------------------------------------------------

pub struct SimulatorState {
    pub tick: u64,
    pub persistent: BTreeMap<NodeId, Vec<Value>>,
    pub atomic_logs: BTreeMap<NodeId, Vec<Value>>,
    pub events: Vec<Event>,
    pub desires: Vec<Desire>,
    pub last_commit_tick: u64,
    pub last_max_length: usize,
    pub next_input_id: u64,
    pub next_desire_id: u64,
    pub next_message_id: u64,
}

pub struct Simulator {
    pub state: SimulatorState,
    pub network: Network,
    pub nodes: BTreeMap<NodeId, Box<dyn Node>>,
    pub tick_interval: u64,
    pub deadlock_threshold: u64,
}

impl Simulator {
    pub fn new(seed: u64, deadlock_threshold: u64, tick_interval: u64) -> Self {
        Self {
            state: SimulatorState {
                tick: 0,
                persistent: BTreeMap::new(),
                atomic_logs: BTreeMap::new(),
                events: Vec::new(),
                desires: Vec::new(),
                last_commit_tick: 0,
                last_max_length: 0,
                next_input_id: 0,
                next_desire_id: 0,
                next_message_id: 0,
            },
            network: Network::new(10, 5, seed),
            nodes: BTreeMap::new(),
            tick_interval,
            deadlock_threshold,
        }
    }

    pub fn spawn(&mut self, node_id: NodeId, node: Box<dyn Node>) {
        self.nodes.insert(node_id, node);
        self.state.persistent.insert(node_id, Vec::new());
        self.state.atomic_logs.insert(node_id, Vec::new());
        self.log_event(
            Actor::Bench,
            EventKind::Spawn,
            EventReason::Node,
            serde_json::json!({ "node": node_id }),
        );
    }

    pub fn crash(&mut self, node_id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.set_crashed(true);
            self.log_event(
                Actor::Bench,
                EventKind::Crash,
                EventReason::Node,
                serde_json::json!({ "node": node_id }),
            );
        }
    }

    pub fn recover(&mut self, node_id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.set_crashed(false);
            self.log_event(
                Actor::Bench,
                EventKind::Recover,
                EventReason::Node,
                serde_json::json!({ "node": node_id }),
            );
        }
    }

    pub fn partition(&mut self, from: NodeId, to: NodeId, duration: u64) {
        let until = self.state.tick + duration;
        self.network.partition(from, to, until);
        self.log_event(
            Actor::Bench,
            EventKind::Partition,
            EventReason::Message,
            serde_json::json!({ "from": from, "to": to, "until": until }),
        );
    }

    pub fn client_message<T: Serialize>(&mut self, to: NodeId, message: T) {
        if self.nodes.get(&to).is_none_or(|node| node.crashed()) {
            return;
        }

        let payload = serde_json::to_value(message).expect("serialize client message");
        let input_id = self.next_input_id();
        let mut event_payload = message_fields(&payload);
        insert_field(&mut event_payload, "input_id", input_id);
        insert_field(&mut event_payload, "to", to);
        self.log_event(
            Actor::Bench,
            EventKind::Deliver,
            EventReason::Client,
            Value::Object(event_payload),
        );
        self.deliver_to(
            to,
            Input::Client {
                input_id,
                to,
                payload,
            },
        );
    }

    pub fn step(&mut self) -> bool {
        self.state.tick += 1;

        let ready = self.network.deliverable(self.state.tick);
        for envelope in ready {
            let to = envelope.to;
            if self.nodes.get(&to).is_none_or(|node| node.crashed()) {
                continue;
            }

            let input_id = self.next_input_id();
            let mut payload = message_fields(&envelope.payload);
            insert_field(&mut payload, "input_id", input_id);
            insert_field(&mut payload, "message_id", envelope.message_id);
            insert_field(&mut payload, "from", envelope.from);
            insert_field(&mut payload, "to", envelope.to);
            self.log_event(
                Actor::Bench,
                EventKind::Deliver,
                EventReason::Message,
                Value::Object(payload),
            );
            self.deliver_to(to, Input::Message { input_id, envelope });
        }

        if self.tick_interval > 0 && self.state.tick.is_multiple_of(self.tick_interval) {
            let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            for node_id in node_ids {
                if self.nodes.get(&node_id).is_none_or(|node| node.crashed()) {
                    continue;
                }

                let input_id = self.next_input_id();
                self.log_event(
                    Actor::Bench,
                    EventKind::Deliver,
                    EventReason::Tick,
                    serde_json::json!({ "input_id": input_id, "to": node_id }),
                );
                self.deliver_to(
                    node_id,
                    Input::Tick {
                        input_id,
                        to: node_id,
                    },
                );
            }
        }

        if self.check_progress() {
            self.state.last_commit_tick = self.state.tick;
        } else if self.state.tick - self.state.last_commit_tick > self.deadlock_threshold {
            self.log_event(
                Actor::Bench,
                EventKind::Deadlock,
                EventReason::NoProgress,
                serde_json::json!({
                    "ticks_since_progress": self.state.tick - self.state.last_commit_tick,
                }),
            );
            return false;
        }

        true
    }

    pub fn run(&mut self, max_ticks: u64) -> bool {
        while self.state.tick < max_ticks {
            if !self.step() {
                return false;
            }
        }
        true
    }

    pub fn validate(&self) -> (bool, &'static str) {
        if self.state.atomic_logs.is_empty() {
            return (true, "No logs");
        }

        let logs: Vec<&Vec<Value>> = self.state.atomic_logs.values().collect();
        let first = logs[0];

        for log in &logs[1..] {
            let min_len = first.len().min(log.len());
            if first[..min_len] != log[..min_len] {
                return (false, "Split brain detected");
            }
        }

        (true, "Valid")
    }

    pub fn log_event(&mut self, who: Actor, what: EventKind, why: EventReason, payload: Value) {
        self.state
            .events
            .push(Event::new(self.state.tick, who, what, why, payload));
    }

    fn deliver_to(&mut self, node_id: NodeId, input: Input) {
        let input_id = input.input_id();
        self.log_receive(node_id, &input);

        let mut node = self
            .nodes
            .remove(&node_id)
            .expect("node exists for delivery");
        {
            let mut ctx = Context::new(node_id, &mut self.state.desires);
            node.receive(&mut ctx, &input);
        }

        self.drain_desires(node_id, input_id);
        self.nodes.insert(node_id, node);
    }

    fn drain_desires(&mut self, node_id: NodeId, parent_input_id: InputId) {
        let desires: Vec<Desire> = self.state.desires.drain(..).collect();

        for desire in desires {
            let desire_id = self.next_desire_id();
            match desire {
                Desire::Send { to, message } => {
                    let mut payload = message_fields(&message);
                    insert_field(&mut payload, "desire_id", desire_id);
                    insert_field(&mut payload, "parent_input_id", parent_input_id);
                    insert_field(&mut payload, "to", to);
                    self.log_event(
                        Actor::Node(node_id),
                        EventKind::Desire,
                        EventReason::Send,
                        Value::Object(payload),
                    );

                    let message_id = self.next_message_id();
                    let mut payload = message_fields(&message);
                    insert_field(&mut payload, "message_id", message_id);
                    insert_field(&mut payload, "desire_id", desire_id);
                    insert_field(&mut payload, "from", node_id);
                    insert_field(&mut payload, "to", to);
                    let envelope = Envelope {
                        message_id,
                        parent_desire_id: desire_id,
                        from: node_id,
                        to,
                        payload: message,
                        send_tick: self.state.tick,
                        deliver_tick: 0,
                    };
                    let delay = self.network.enqueue(envelope, self.state.tick);
                    insert_field(&mut payload, "delay", delay);
                    insert_field(&mut payload, "deliver_at", self.state.tick + delay);
                    self.log_event(
                        Actor::Bench,
                        EventKind::Schedule,
                        EventReason::Send,
                        Value::Object(payload),
                    );
                }
                Desire::Save { record } => {
                    self.log_event(
                        Actor::Node(node_id),
                        EventKind::Desire,
                        EventReason::Save,
                        serde_json::json!({
                            "desire_id": desire_id,
                            "parent_input_id": parent_input_id,
                        }),
                    );
                    self.state
                        .persistent
                        .entry(node_id)
                        .or_default()
                        .push(record.clone());
                    self.log_event(
                        Actor::Bench,
                        EventKind::Save,
                        EventReason::Save,
                        serde_json::json!({
                            "desire_id": desire_id,
                            "node": node_id,
                            "record": record,
                        }),
                    );
                }
                Desire::Commit { entry } => {
                    self.log_event(
                        Actor::Node(node_id),
                        EventKind::Desire,
                        EventReason::Commit,
                        serde_json::json!({
                            "desire_id": desire_id,
                            "parent_input_id": parent_input_id,
                        }),
                    );
                    let index = self.append_commit(node_id, entry.clone());
                    self.log_event(
                        Actor::Bench,
                        EventKind::Commit,
                        EventReason::Commit,
                        serde_json::json!({
                            "desire_id": desire_id,
                            "node": node_id,
                            "index": index,
                            "entry": entry,
                        }),
                    );
                }
            }
        }
    }

    fn append_commit(&mut self, node_id: NodeId, entry: Value) -> usize {
        let log = self.state.atomic_logs.entry(node_id).or_default();
        let index = log.len();
        log.push(entry.clone());

        for (&other_id, other_log) in self.state.atomic_logs.iter() {
            if other_id == node_id {
                continue;
            }
            if other_log.len() > index && other_log[index] != entry {
                panic!(
                    "split brain at index {}: node {:?} committed {:?}, node {:?} committed {:?}",
                    index, node_id, entry, other_id, other_log[index]
                );
            }
        }

        self.state.last_commit_tick = self.state.tick;
        index
    }

    fn check_progress(&mut self) -> bool {
        if self.state.atomic_logs.is_empty() {
            return false;
        }

        let max_length = self
            .state
            .atomic_logs
            .values()
            .map(Vec::len)
            .max()
            .unwrap_or(0);

        if max_length > self.state.last_max_length {
            self.state.last_max_length = max_length;
            return true;
        }

        false
    }

    fn log_receive(&mut self, node_id: NodeId, input: &Input) {
        match input {
            Input::Tick { input_id, .. } => {
                self.log_event(
                    Actor::Node(node_id),
                    EventKind::Receive,
                    EventReason::Tick,
                    serde_json::json!({ "parent_input_id": input_id }),
                );
            }
            Input::Client {
                input_id, payload, ..
            } => {
                let mut event_payload = message_fields(payload);
                insert_field(&mut event_payload, "parent_input_id", input_id);
                self.log_event(
                    Actor::Node(node_id),
                    EventKind::Receive,
                    EventReason::Client,
                    Value::Object(event_payload),
                );
            }
            Input::Message { input_id, envelope } => {
                let mut payload = message_fields(&envelope.payload);
                insert_field(&mut payload, "parent_input_id", input_id);
                insert_field(&mut payload, "message_id", envelope.message_id);
                insert_field(&mut payload, "from", envelope.from);
                self.log_event(
                    Actor::Node(node_id),
                    EventKind::Receive,
                    EventReason::Message,
                    Value::Object(payload),
                );
            }
        }
    }

    fn next_input_id(&mut self) -> InputId {
        let id = InputId(self.state.next_input_id);
        self.state.next_input_id += 1;
        id
    }

    fn next_desire_id(&mut self) -> DesireId {
        let id = DesireId(self.state.next_desire_id);
        self.state.next_desire_id += 1;
        id
    }

    fn next_message_id(&mut self) -> MessageId {
        let id = MessageId(self.state.next_message_id);
        self.state.next_message_id += 1;
        id
    }
}

fn message_fields(message: &Value) -> Map<String, Value> {
    let object = message
        .as_object()
        .expect("message payload requires object");
    let message_type = object
        .get("type")
        .and_then(Value::as_str)
        .expect("message payload requires string type");

    let mut fields = Map::new();
    fields.insert(
        "message_type".to_string(),
        Value::String(message_type.to_string()),
    );
    for (key, value) in object {
        if key != "type" {
            fields.insert(key.clone(), value.clone());
        }
    }
    fields
}

fn insert_field<T: Serialize>(fields: &mut Map<String, Value>, key: &str, value: T) {
    fields.insert(
        key.to_string(),
        serde_json::to_value(value).expect("serialize event field"),
    );
}
