use quorum::ping_pong::PingPongNode;
use quorum::raft::{RaftNode, propose};
use quorum::{NodeId, Simulator};
use serde_json::json;
use std::env;

fn main() {
    let scenario = env::args().nth(1).unwrap_or_else(|| "raft".to_string());
    let sim = match scenario.as_str() {
        "raft" => run_raft(),
        "ping-pong" => run_ping_pong(),
        other => panic!("unknown scenario {other:?}; expected raft or ping-pong"),
    };

    for event in sim.state.events {
        println!("{}", serde_json::to_string(&event).unwrap());
    }
}

fn run_raft() -> Simulator {
    let node_count = 2;
    let mut sim = Simulator::new(42, 200, 1);
    sim.network.base_latency = 1;
    sim.network.jitter = 0;

    let max_latency = sim.network.base_latency + sim.network.jitter;
    let timeout_base = 15;
    let timeout_margin = 1;
    let timeout_stride = max_latency + timeout_margin;

    for id in 0..node_count {
        let node_id = NodeId(id);
        let election_timeout = timeout_base + (id as u64 * timeout_stride);
        sim.spawn(
            node_id,
            Box::new(RaftNode::with_election_timeout(
                node_id,
                node_count,
                election_timeout,
            )),
        );
    }

    sim.run(25);
    sim.client_message(NodeId(0), propose(json!("alpha")));
    sim.run(40);
    sim
}

fn run_ping_pong() -> Simulator {
    let node_count = 2;
    let mut sim = Simulator::new(42, 1_000, 1);

    for id in 0..node_count {
        let node_id = NodeId(id);
        sim.spawn(node_id, Box::new(PingPongNode::new(node_id, node_count)));
    }

    sim.run(40);
    sim
}
