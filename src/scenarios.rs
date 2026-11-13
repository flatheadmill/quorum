//! Native scenario configuration and factories, outside the record-ignorant world.
use crate::tape::{Header, NodeSpec, WorldConfig};
use crate::{
    Node, NodeId, Result, Simulator,
    multi_paxos::{self, MultiPaxosNode},
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
        "multi-paxos" => {
            let config: multi_paxos::Config =
                serde_json::from_value(spec.config.clone()).map_err(|e| e.to_string())?;
            Ok(Box::new(MultiPaxosNode::new(spec.node, config)?))
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
        "calm" => {
            let membership = vec![NodeId(0), NodeId(1), NodeId(2)];
            let config = json!({
                "acceptors": membership,
                "learners": membership,
                "proposers": membership,
                "active_proposer": 0,
                "from_slot": 0,
                "initial_round": 1,
                "initial_state": "empty",
            });
            let nodes = membership
                .iter()
                .map(|&node| NodeSpec {
                    node,
                    algorithm: "multi-paxos".into(),
                    config: config.clone(),
                })
                .collect();
            (
                WorldConfig {
                    base_latency: 1,
                    jitter: 0,
                    tick_interval: 1,
                    deadlock_threshold: 200,
                },
                nodes,
                membership,
            )
        }
        "duel-inclusion" | "duel-exclusion" => {
            let membership = vec![NodeId(0), NodeId(1), NodeId(2)];
            let nodes = membership
                .iter()
                .map(|&node| NodeSpec {
                    node,
                    algorithm: "multi-paxos".into(),
                    config: json!({
                        "acceptors": membership,
                        "learners": membership,
                        "proposers": [0, 1],
                        "active_proposer": if node.0 < 2 { Some(node) } else { None },
                        "from_slot": 0,
                        "initial_round": if node == NodeId(1) { 2 } else { 1 },
                        "initial_state": "empty",
                    }),
                })
                .collect();
            (
                WorldConfig {
                    base_latency: 1,
                    jitter: 3,
                    tick_interval: 1,
                    deadlock_threshold: 200,
                },
                nodes,
                membership,
            )
        }
        _ => {
            return Err(format!(
                "unknown scenario {scenario:?}; expected raft, ping-pong, calm, duel-inclusion or duel-exclusion"
            ));
        }
    };
    let seed = match scenario {
        "duel-inclusion" => 331,
        "duel-exclusion" => 1271,
        _ => 42,
    };
    Ok(Header::new(scenario, seed, world, nodes, acceptors))
}
pub fn capture(scenario: &str, output: Box<dyn Write>) -> Result<Simulator> {
    let header = header(scenario)?;
    let specs = header.nodes.clone();
    let mut sim = Simulator::new(header, output)?;
    for spec in &specs {
        sim.spawn(spec.node, factory(spec)?)?;
    }
    if scenario == "calm" {
        for value in ["alpha", "beta", "gamma"] {
            sim.client_message(NodeId(0), multi_paxos::propose(json!(value)))?;
        }
        let progressed = sim.run(8)?;
        sim.finalize(if progressed { "horizon" } else { "no_progress" })?;
        return Ok(sim);
    }
    if matches!(scenario, "duel-inclusion" | "duel-exclusion") {
        sim.client_message(NodeId(0), multi_paxos::propose(json!("x")))?;
        sim.client_message(NodeId(1), multi_paxos::propose(json!("y")))?;
        if !sim.run(28)? {
            return Err("Duel stopped before its declared horizon".into());
        }
        let account = sim.check_projection()?.account();
        if account.active_input.is_some()
            || !account.unreached_desires.is_empty()
            || !account.open_attempts.is_empty()
            || !account.queued.is_empty()
            || !account.arrived.is_empty()
            || !account.clients.is_empty()
            || !account.due_messages.is_empty()
            || !account.due_ticks.is_empty()
            || account.no_progress_due.is_some()
        {
            return Err("Duel horizon reached with unfinished world work".into());
        }
        sim.finalize("horizon")?;
        return Ok(sim);
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
