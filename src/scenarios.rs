//! Native scenario configuration and factories, outside the record-ignorant world.
use crate::tape::{Header, NodeSpec, WorldConfig};
use crate::{
    Node, NodeId, Result, Simulator,
    ping_pong::PingPongNode,
    raft::{RaftNode, propose},
};
use serde::Deserialize;
use serde_json::json;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RaftConfig {
    count: i32,
    election_timeout: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PingConfig {
    count: i32,
}
pub fn factory(spec: &NodeSpec) -> Result<Box<dyn Node>> {
    match spec.algorithm.as_str() {
        "raft" => {
            let c: RaftConfig =
                serde_json::from_value(spec.config.clone()).map_err(|e| e.to_string())?;
            if c.count <= 0 || spec.node.0 < 0 || spec.node.0 >= c.count || c.election_timeout == 0
            {
                return Err("invalid Raft node configuration".into());
            }
            Ok(Box::new(RaftNode::with_election_timeout(
                spec.node,
                c.count,
                c.election_timeout,
            )))
        }
        "ping-pong" => {
            let c: PingConfig =
                serde_json::from_value(spec.config.clone()).map_err(|e| e.to_string())?;
            if c.count <= 0 || spec.node.0 < 0 || spec.node.0 >= c.count {
                return Err("invalid Ping/Pong node configuration".into());
            }
            Ok(Box::new(PingPongNode::new(spec.node, c.count)))
        }
        other => Err(format!("no factory for algorithm {other}")),
    }
}
pub fn header(scenario: &str) -> Result<Header> {
    let (world, nodes, acceptors) = match scenario {
        "raft" => (
            WorldConfig {
                base_latency: 1,
                jitter: 0,
                tick_interval: 1,
                deadlock_threshold: 200,
            },
            (0..2)
                .map(|id| NodeSpec {
                    node: NodeId(id),
                    algorithm: "raft".into(),
                    config: json!({"count":2,"election_timeout":15+id*2}),
                })
                .collect(),
            vec![NodeId(0), NodeId(1)],
        ),
        "ping-pong" => (
            WorldConfig {
                base_latency: 10,
                jitter: 5,
                tick_interval: 1,
                deadlock_threshold: 1000,
            },
            (0..2)
                .map(|id| NodeSpec {
                    node: NodeId(id),
                    algorithm: "ping-pong".into(),
                    config: json!({"count":2}),
                })
                .collect(),
            vec![],
        ),
        _ => {
            return Err(format!(
                "unknown scenario {scenario:?}; expected raft or ping-pong"
            ));
        }
    };
    Ok(Header::new(scenario, 42, world, nodes, acceptors))
}
pub fn capture(scenario: &str, output: Box<dyn Write>) -> Result<Simulator> {
    let header = header(scenario)?;
    let specs = header.nodes.clone();
    let mut sim = Simulator::new(header, output)?;
    for spec in &specs {
        sim.spawn(spec.node, factory(spec)?)?;
    }
    if scenario == "raft" {
        if !sim.run(25)? {
            sim.finalize("no_progress")?;
            return Ok(sim);
        }
        sim.client_message(NodeId(0), propose(json!("alpha")))?;
    }
    let progressed = sim.run(40)?;
    sim.finalize(if progressed { "horizon" } else { "no_progress" })?;
    Ok(sim)
}
