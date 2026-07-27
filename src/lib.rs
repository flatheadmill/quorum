//! Quorum — deterministic bench for consensus inhabitants.

pub mod ping_pong;
pub mod raft;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

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

/// A position whose chosen value the world can check for agreement.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Slot(pub u64);

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
    Commit { slot: Slot, entry: Value },
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

    pub fn commit<T: Serialize>(&mut self, slot: Slot, entry: T) {
        let entry = serde_json::to_value(entry).expect("serialize commit desire");
        self.desires.push(Desire::Commit { slot, entry });
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

    fn partition(&mut self, from: NodeId, to: NodeId, until_tick: u64) {
        self.partitions.insert((from, to), until_tick);
    }
}

// ---------------------------------------------------------------------------
// Simulator — monomorphic world
// ---------------------------------------------------------------------------

pub struct SimulatorState {
    pub tick: u64,
    pub persistent: BTreeMap<NodeId, Vec<Value>>,
    pub atomic_logs: BTreeMap<NodeId, BTreeMap<Slot, Value>>,
    pub events: Vec<Event>,
    pub desires: Vec<Desire>,
    pub last_commit_tick: u64,
    pub last_chosen_count: usize,
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
                last_chosen_count: 0,
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
        self.state.atomic_logs.insert(node_id, BTreeMap::new());
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
        assert_ne!(
            from, to,
            "self-partition is invalid: a node cannot be partitioned from itself"
        );
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
                Desire::Commit { slot, entry } => {
                    self.log_event(
                        Actor::Node(node_id),
                        EventKind::Desire,
                        EventReason::Commit,
                        serde_json::json!({
                            "desire_id": desire_id,
                            "parent_input_id": parent_input_id,
                            "slot": slot,
                        }),
                    );
                    self.append_commit(node_id, slot, entry.clone());
                    self.log_event(
                        Actor::Bench,
                        EventKind::Commit,
                        EventReason::Commit,
                        serde_json::json!({
                            "desire_id": desire_id,
                            "node": node_id,
                            "slot": slot,
                            "entry": entry,
                        }),
                    );
                }
            }
        }
    }

    fn append_commit(&mut self, node_id: NodeId, slot: Slot, entry: Value) {
        // Commit is a one-shot claim that a slot is chosen. Even an identical
        // second claim means the inhabitant's choose path fired twice.
        //
        // This refusal assumes no amnesia. Honest recovery in 002 will rebuild
        // volatile apply state behind the durable log, so a recovered inhabitant
        // may re-claim slots already recorded here. The recovery contract must
        // decide whether that is a duplicate, an absorbed no-op, or a distinct
        // re-learned event before recovery scenarios can rely on this check.
        if let Some(previous) = self
            .state
            .atomic_logs
            .get(&node_id)
            .and_then(|log| log.get(&slot))
        {
            if previous == &entry {
                panic!(
                    "duplicate commit at slot {}: node {:?} committed {:?} twice",
                    slot.0, node_id, entry
                );
            }
            panic!(
                "self split brain at slot {}: node {:?} committed {:?}, then {:?}",
                slot.0, node_id, previous, entry
            );
        }

        // Agreement is checked as each claim enters the world; the old batch
        // validate pass would only repeat this check after the fact.
        for (&other_id, other_log) in self.state.atomic_logs.iter() {
            if other_id == node_id {
                continue;
            }
            if let Some(other_entry) = other_log.get(&slot)
                && other_entry != &entry
            {
                panic!(
                    "split brain at slot {}: node {:?} committed {:?}, node {:?} committed {:?}",
                    slot.0, node_id, entry, other_id, other_entry
                );
            }
        }

        self.state
            .atomic_logs
            .entry(node_id)
            .or_default()
            .insert(slot, entry);
    }

    fn check_progress(&mut self) -> bool {
        if self.state.atomic_logs.is_empty() {
            return false;
        }

        let chosen_count = self
            .state
            .atomic_logs
            .values()
            .flat_map(|log| log.keys().copied())
            .collect::<BTreeSet<_>>()
            .len();

        if chosen_count > self.state.last_chosen_count {
            self.state.last_chosen_count = chosen_count;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Default)]
    struct SilentNode {
        crashed: bool,
    }

    impl Node for SilentNode {
        fn receive(&mut self, _ctx: &mut Context, _input: &Input) {}

        fn crashed(&self) -> bool {
            self.crashed
        }

        fn set_crashed(&mut self, crashed: bool) {
            self.crashed = crashed;
        }
    }

    #[test]
    #[should_panic(expected = "duplicate commit at slot 7")]
    fn refuses_identical_second_commit_from_one_node() {
        let mut sim = Simulator::new(0, 100, 1);
        sim.append_commit(NodeId(0), Slot(7), json!("alpha"));
        sim.append_commit(NodeId(0), Slot(7), json!("alpha"));
    }

    #[test]
    #[should_panic(expected = "self split brain at slot 7")]
    fn refuses_conflicting_second_commit_from_one_node() {
        let mut sim = Simulator::new(0, 100, 1);
        sim.append_commit(NodeId(0), Slot(7), json!("alpha"));
        sim.append_commit(NodeId(0), Slot(7), json!("beta"));
    }

    #[test]
    #[should_panic(expected = "split brain at slot 7")]
    fn refuses_cross_node_disagreement() {
        let mut sim = Simulator::new(0, 100, 1);
        sim.append_commit(NodeId(0), Slot(7), json!("alpha"));
        sim.append_commit(NodeId(1), Slot(7), json!("beta"));
    }

    #[test]
    fn permits_cross_node_agreement() {
        let mut sim = Simulator::new(0, 100, 1);
        sim.append_commit(NodeId(0), Slot(7), json!("alpha"));
        sim.append_commit(NodeId(1), Slot(7), json!("alpha"));

        assert_eq!(sim.state.atomic_logs[&NodeId(1)][&Slot(7)], json!("alpha"));
    }

    #[test]
    fn permits_different_values_in_disjoint_slots() {
        let mut sim = Simulator::new(0, 100, 1);
        sim.append_commit(NodeId(0), Slot(0), json!("alpha"));
        sim.append_commit(NodeId(1), Slot(1), json!("beta"));

        assert_eq!(sim.state.atomic_logs[&NodeId(0)][&Slot(0)], json!("alpha"));
        assert_eq!(sim.state.atomic_logs[&NodeId(1)][&Slot(1)], json!("beta"));
    }

    #[test]
    fn progress_counts_distinct_chosen_slots() {
        let mut sim = Simulator::new(0, 100, 1);

        sim.append_commit(NodeId(0), Slot(7), json!("alpha"));
        assert!(sim.check_progress());

        sim.append_commit(NodeId(1), Slot(7), json!("alpha"));
        assert!(!sim.check_progress());

        sim.append_commit(NodeId(1), Slot(3), json!("beta"));
        assert!(sim.check_progress());
        assert_eq!(sim.state.last_chosen_count, 2);
    }

    #[test]
    #[should_panic(expected = "a node cannot be partitioned from itself")]
    fn refuses_self_partition() {
        Simulator::new(0, 100, 1).partition(NodeId(1), NodeId(1), 10);
    }

    #[test]
    fn deadlock_event_fires_after_threshold_without_progress() {
        let mut sim = Simulator::new(0, 2, 1);
        sim.spawn(NodeId(0), Box::new(SilentNode::default()));

        assert!(sim.run(2));
        assert!(!sim.run(3));

        let event = sim.state.events.last().expect("deadlock event");
        assert_eq!(event.when, 3);
        assert!(matches!(event.who, Actor::Bench));
        assert!(matches!(event.what, EventKind::Deadlock));
        assert!(matches!(event.why, EventReason::NoProgress));
        assert_eq!(event.payload["ticks_since_progress"], json!(3));
    }
}
