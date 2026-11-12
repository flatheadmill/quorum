//! Quorum — deterministic bench for consensus inhabitants.

pub mod multi_paxos;
pub mod paxos_account;
pub mod ping_pong;
pub mod projection;
pub mod raft;
pub mod replay;
pub mod scenarios;
pub mod tape;
pub mod vote;
mod world;
pub use world::Simulator;

pub type Result<T> = std::result::Result<T, String>;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Desire {
    Send { to: NodeId, message: Value },
    Save { record: Value },
    Commit { slot: Slot, entry: Value },
}

// ---------------------------------------------------------------------------
// Inputs and envelopes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
