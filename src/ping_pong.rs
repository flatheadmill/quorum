use serde::{Deserialize, Serialize};

use crate::{Context, Input, Node, NodeId};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Ping { seq: u64 },
    Pong { seq: u64 },
}

pub struct PingPongNode {
    node_id: NodeId,
    count: i32,
    tick_count: u64,
    next_seq: u64,
    crashed: bool,
}

impl PingPongNode {
    pub fn new(node_id: NodeId, count: i32) -> Self {
        Self {
            node_id,
            count,
            tick_count: 0,
            next_seq: 0,
            crashed: false,
        }
    }

    fn peer(&self) -> NodeId {
        NodeId((self.node_id.0 + 1).rem_euclid(self.count))
    }
}

impl Node for PingPongNode {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        match input {
            Input::Tick { .. } => {
                self.tick_count += 1;
                if !self.tick_count.is_multiple_of(5) {
                    return;
                }

                self.next_seq += 1;
                ctx.send(self.peer(), Message::Ping { seq: self.next_seq });
            }
            Input::Message { envelope, .. } => {
                let message: Message = serde_json::from_value(envelope.payload.clone())
                    .expect("strict parse ping/pong message");
                match message {
                    Message::Ping { seq } => {
                        ctx.send(envelope.from, Message::Pong { seq });
                    }
                    Message::Pong { seq: _ } => {}
                }
            }
            Input::Client { .. } => {
                panic!("ping/pong does not accept client input");
            }
        }
    }

    fn crashed(&self) -> bool {
        self.crashed
    }

    fn set_crashed(&mut self, crashed: bool) {
        self.crashed = crashed;
    }
}
