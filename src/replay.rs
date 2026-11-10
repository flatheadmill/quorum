//! Input-driven reexecution. Never calls the live scheduler or storage adapter.
use crate::projection::{Projection, Tape};
use crate::tape::{Implementation, NodeSpec};
use crate::{Context, Desire, InputId, Node, NodeId, Result};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Strict,
    Diagnostic,
}
#[derive(Debug, Serialize)]
pub struct Report {
    pub source_sha256: String,
    pub mode: String,
    pub replay_implementation: Implementation,
    pub completed_answers: usize,
    pub dispatched_inputs: usize,
    pub uncertified_answers: usize,
    pub admitted_results: usize,
    pub tail: String,
}
pub fn replay(
    tape: &Tape,
    mode: Mode,
    mut factory: impl FnMut(&NodeSpec) -> Result<Box<dyn Node>>,
) -> Result<Report> {
    let header = tape
        .projection
        .header
        .as_ref()
        .ok_or("cannot replay a prefix without a header")?;
    let implementation = Implementation::current();
    if mode == Mode::Strict && header.implementation != implementation {
        return Err("strict replay requires the recorded implementation identity; diagnostic mode may compare changed code".into());
    }
    let mut nodes: BTreeMap<NodeId, Box<dyn Node>> = BTreeMap::new();
    let mut answer: Option<(InputId, Vec<Desire>)> = None;
    let mut projected = Projection::default();
    let mut completed = 0;
    let mut dispatched = 0;
    for row in &tape.rows {
        match row.what.as_str() {
            "spawn" => {
                let id: NodeId = row.get("node")?;
                let spec = header
                    .nodes
                    .iter()
                    .find(|s| s.node == id)
                    .ok_or("spawn is not configured")?;
                let node = factory(spec)?;
                if node.crashed() {
                    return Err("factory returned a crashed initial node".into());
                }
                nodes.insert(id, node);
            }
            "crash" | "recover" => {
                nodes
                    .get_mut(&row.get::<NodeId>("node")?)
                    .ok_or("lifecycle without node")?
                    .set_crashed(row.what == "crash");
            }
            "dispatch" => {
                let input = row.input()?;
                let mut desires = Vec::new();
                let node = nodes
                    .get_mut(&input.to())
                    .ok_or("dispatch without inhabitant")?;
                if node.crashed() {
                    return Err("dispatch to crashed replay inhabitant".into());
                }
                node.receive(&mut Context::new(input.to(), &mut desires), &input);
                answer = Some((input.input_id(), desires));
                dispatched += 1;
            }
            "close" => {
                let id: InputId = row.get("input_id")?;
                let (actual_id, actual) = answer.take().ok_or("close without replayed answer")?;
                let expected: Vec<Desire> = tape.projection.answers[&id]
                    .desires
                    .iter()
                    .map(|(_, d)| d.clone())
                    .collect();
                if actual_id != id || actual != expected {
                    let first = actual
                        .iter()
                        .zip(&expected)
                        .position(|(a, b)| a != b)
                        .unwrap_or(actual.len().min(expected.len()));
                    return Err(format!(
                        "line {}: protocol divergence at input {}, desire {}: expected {:?}, got {:?}; stopped before this answer's effects ({} earlier results)",
                        row.seq + 1,
                        id.0,
                        first,
                        expected.get(first),
                        actual.get(first),
                        projected.effects
                    ));
                }
                completed += 1;
            }
            _ => {}
        }
        // Projection uses the captured delay and outcomes. No RNG is constructed,
        // no inputs are rescheduled, and no live effects can escape this function.
        projected.apply(row)?;
    }
    Ok(Report {
        source_sha256: tape.sha256.clone(),
        mode: if mode == Mode::Strict {
            "strict"
        } else {
            "diagnostic"
        }
        .into(),
        replay_implementation: implementation,
        completed_answers: completed,
        dispatched_inputs: dispatched,
        uncertified_answers: dispatched - completed,
        admitted_results: projected.effects,
        tail: projected.tail().into(),
    })
}
