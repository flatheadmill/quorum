//! An independent, boundary-indexed account of the bounded Multi-Paxos level.
//!
//! Only the world reader/projection and identity vocabulary are shared with the
//! simulator. Protocol parsing, state reconstruction and evidence joins live
//! here; neither MultiPaxosNode nor replay is an oracle for this report.
mod attempts;
mod evidence;
mod fold;
mod knowledge;
mod learning;
mod origin;
mod presentation;
mod proposer;
mod protocol;
mod self_check;

use evidence::Evidence;
pub use presentation::human;
use serde_json::{Value, json};

/// Examine original JSONL bytes, retaining the last world-valid prefix on a
/// reader failure. Protocol interpretation faults never truncate this prefix.
pub fn examine(bytes: &[u8]) -> Value {
    let e = Evidence::read(bytes);
    let choices = fold::choices(&e);
    let mut obligations = fold::configuration(&e);
    let (agreement, mut observations) = fold::no_conflicts(&e, &choices);
    obligations.extend(agreement);
    let (acceptors, acceptor_observations) = fold::acceptors(&e);
    obligations.extend(acceptors);
    observations.extend(acceptor_observations);
    let mut indexed_attempts = attempts::index(&e);
    let (proposers, proposer_observations) = proposer::evaluate(&e, &mut indexed_attempts);
    obligations.extend(proposers);
    observations.extend(proposer_observations);
    let (receivers, receiver_observations) = learning::evaluate(&e);
    obligations.extend(receivers);
    observations.extend(receiver_observations);
    let (claims, knowledge_observations) = knowledge::evaluate(&e, &choices);
    observations.extend(knowledge_observations);
    let (attempts, attempt_observations) = attempts::finish(&e, &indexed_attempts, &choices);
    observations.extend(attempt_observations);
    let (checks, check_observations) =
        self_check::evaluate(&e, &choices, &obligations, &observations);
    obligations.extend(checks);
    observations.extend(check_observations);
    obligations.sort_by(|a, b| {
        a["boundary_line"]
            .as_u64()
            .cmp(&b["boundary_line"].as_u64())
            .then_with(|| a["proposition"].as_str().cmp(&b["proposition"].as_str()))
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    observations.sort_by(|a, b| {
        a["line"]
            .as_u64()
            .cmp(&b["line"].as_u64())
            .then_with(|| a["kind"].as_str().cmp(&b["kind"].as_str()))
    });
    let nodes=e.projection.header.as_ref().map(|header|header.nodes.iter().map(|spec|json!({
        "node":spec.node,"algorithm":spec.algorithm,
        "contract":if e.config(spec.node).is_some() {"covered"} else if e.claimed.contains(&spec.node) {"malformed"} else {"undeclared"},
        "config":e.config(spec.node),"initial_state_known":e.config(spec.node).is_some(),
        "header_acceptor":e.header_acceptors.contains(&spec.node),
        "limits":if e.config(spec.node).is_some() {"Covered obligations are judged against this declaration, even if a test mutant does not implement it."} else {"Visible records and received evidence remain observable; absence-based protocol conclusions require a declared initial state and contract."}
    })).collect::<Vec<_>>()).unwrap_or_default();
    json!({
        "schema_version":1,"account_version":"multi-paxos-account/1",
        "interpretation":"quorum/003_multi_paxos.md@sha256:d8886526fbaa53326c66a9b282a78f7b71de63d4f877bea3de65ebefaf0866d3",
        "source_sha256":e.source_sha256,
        "world":{"valid_rows":e.rows.len(),"total_rows":e.total_rows,"valid_prefix_bytes":e.valid_prefix_bytes,
            "tail":e.projection.tail(),"account":e.projection.account(),"error":e.world_error,"stopped":e.projection.stopped},
        "coverage":{"nodes":nodes,"obligation_families":["configuration","agreement","validity","workload","evidence"],
            "global_quorum_universe":"header.acceptors","header_acceptors":e.header_acceptors,
            "limits":"This account explains the original observed world prefix. Established evidence is not a claim that an inhabitant obeyed the protocol, and unresolved protocol intent is not pending world work."},
        "interpretation_findings":e.findings,
        "choices":choices,"obligations":obligations,"claims":claims,"attempts":attempts,"observations":observations,
        "interpretation_gap_dependencies":evidence::gap_dependencies()
    })
}
