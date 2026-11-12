//! World-valid prefix and causal indexes. No inhabitant code is used here.
use super::protocol::{self, Ballot, Config, Message, Record};
use crate::projection::Projection;
use crate::tape::{self, Event};
use crate::{Desire, DesireId, Input, InputId, MessageId, NodeId, Slot};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Output {
    pub node: NodeId,
    pub input: InputId,
    pub id: DesireId,
    pub desire_line: usize,
    pub position: usize,
    pub close_line: Option<usize>,
    pub attempt_line: Option<usize>,
    pub result_line: Option<usize>,
    pub outcome: Option<String>,
    pub refusal_reason: Option<String>,
    pub message_id: Option<MessageId>,
    pub desire: Desire,
    pub message: Option<Result<Message, String>>,
    pub record: Option<Result<Record, String>>,
}

pub(super) struct Acceptance {
    pub node: NodeId,
    pub slot: Slot,
    pub ballot: Ballot,
    pub value: Value,
    pub line: usize,
    pub output: DesireId,
}

pub(super) struct Evidence {
    pub rows: Vec<Event>,
    pub projection: Projection,
    pub source_sha256: String,
    pub total_rows: usize,
    pub valid_prefix_bytes: usize,
    pub world_error: Option<Value>,
    pub claimed: BTreeSet<NodeId>,
    pub configs: BTreeMap<NodeId, Result<Config, String>>,
    pub header_acceptors: BTreeSet<NodeId>,
    pub outputs: BTreeMap<DesireId, Output>,
    pub input_messages: BTreeMap<InputId, Result<Message, String>>,
    pub acceptances: Vec<Acceptance>,
    pub findings: Vec<Value>,
}

impl Evidence {
    pub fn read(bytes: &[u8]) -> Self {
        let mut rows = Vec::new();
        let mut projection = Projection::default();
        let mut valid_prefix_bytes = 0;
        let mut world_error = None;
        let total_rows = bytes.split_inclusive(|b| *b == b'\n').count();
        for (index, bytes) in bytes.split_inclusive(|b| *b == b'\n').enumerate() {
            let decoded = tape::decode(bytes).and_then(|mut decoded| {
                let row = decoded.pop().ok_or("empty world row")?;
                projection.apply(&row)?;
                Ok(row)
            });
            match decoded {
                Ok(row) => {
                    valid_prefix_bytes += bytes.len();
                    rows.push(row);
                }
                Err(reason) => {
                    world_error = Some(json!({"line":index+1,"reason":reason}));
                    // apply may have updated an execution index before detecting
                    // the bad row. Reconstruct only the known valid prefix.
                    projection = Projection::from_rows(&rows).expect("previously validated prefix");
                    break;
                }
            }
        }
        let mut evidence = Self {
            rows,
            projection,
            source_sha256: tape::hash(bytes),
            total_rows,
            valid_prefix_bytes,
            world_error,
            claimed: BTreeSet::new(),
            configs: BTreeMap::new(),
            header_acceptors: BTreeSet::new(),
            outputs: BTreeMap::new(),
            input_messages: BTreeMap::new(),
            acceptances: Vec::new(),
            findings: Vec::new(),
        };
        evidence.index();
        evidence
    }

    fn index(&mut self) {
        if let Some(header) = &self.projection.header {
            self.header_acceptors
                .extend(header.acceptors.iter().copied());
            for spec in &header.nodes {
                if spec.algorithm == "multi-paxos" {
                    self.claimed.insert(spec.node);
                    self.configs
                        .insert(spec.node, protocol::config(spec.node, &spec.config));
                }
            }
        }
        let mut positions = BTreeMap::<InputId, usize>::new();
        for (offset, row) in self.rows.iter().enumerate() {
            let line = offset + 1;
            match row.what.as_str() {
                "desire" => {
                    let id: DesireId = row.get("desire_id").unwrap();
                    let input = row.get("parent_input_id").unwrap();
                    let desire = row.desire().unwrap();
                    let position = positions.entry(input).or_default();
                    let (message, record) = match &desire {
                        Desire::Send { message, .. } => (Some(protocol::peer(message)), None),
                        Desire::Save { record } => (None, Some(protocol::record(record))),
                        Desire::Commit { .. } => (None, None),
                    };
                    self.outputs.insert(
                        id,
                        Output {
                            node: row.get("node").unwrap(),
                            input,
                            id,
                            desire_line: line,
                            position: *position,
                            close_line: None,
                            attempt_line: None,
                            result_line: None,
                            outcome: None,
                            refusal_reason: None,
                            message_id: None,
                            desire,
                            message,
                            record,
                        },
                    );
                    *position += 1;
                }
                "close" => {
                    let input: InputId = row.get("input_id").unwrap();
                    for (id, _) in &self.projection.answers[&input].desires {
                        self.outputs.get_mut(id).unwrap().close_line = Some(line);
                    }
                }
                "attempt" => {
                    let output = self
                        .outputs
                        .get_mut(&row.get("desire_id").unwrap())
                        .unwrap();
                    output.attempt_line = Some(line);
                    if row.why == "send" {
                        output.message_id = Some(row.get("message_id").unwrap());
                    }
                }
                "result" => {
                    let output = self
                        .outputs
                        .get_mut(&row.get("desire_id").unwrap())
                        .unwrap();
                    output.result_line = Some(line);
                    output.outcome = Some(row.get("outcome").unwrap());
                    output.refusal_reason = row.get("reason").unwrap();
                }
                _ => {}
            }
        }
        for (id, input) in &self.projection.inputs {
            match input {
                Input::Client { payload, .. } => {
                    self.input_messages.insert(*id, protocol::client(payload));
                }
                Input::Message { envelope, .. } => {
                    self.input_messages
                        .insert(*id, protocol::peer(&envelope.payload));
                }
                Input::Tick { .. } => {}
            }
        }
        for output in self.outputs.values() {
            if output.outcome.as_deref() != Some("fulfilled")
                || !(self.claimed.contains(&output.node)
                    || self.header_acceptors.contains(&output.node))
            {
                continue;
            }
            if let Some(Ok(Record::Accepted {
                slot,
                ballot,
                value,
            })) = &output.record
            {
                self.acceptances.push(Acceptance {
                    node: output.node,
                    slot: *slot,
                    ballot: *ballot,
                    value: value.clone(),
                    line: output.result_line.unwrap(),
                    output: output.id,
                });
            }
        }
        self.acceptances.sort_by_key(|acceptance| acceptance.line);
        self.find_interpretation_gaps();
    }

    fn find_interpretation_gaps(&mut self) {
        let mut findings = BTreeMap::<String, Value>::new();
        for (node, config) in &self.configs {
            if let Err(reason) = config {
                let id = format!("config:{}", node.0);
                findings.insert(id.clone(), json!({
                    "id":id,"kind":"configuration","node":node,"first_line":1,
                    "occurrence_lines":[1],"world_identity":{"node":node,"header_line":1},
                    "failed_shape":"Config","reason":reason,
                    "affected_premises":["contract","membership","suffix","activation","initial_state"]
                }));
            }
        }
        // Visit relevant boundaries in tape order so first_line is the first
        // scoped interpretation, not an earlier unscoped copy of the payload.
        for (offset, row) in self.rows.iter().enumerate() {
            let line = offset + 1;
            let candidate = match row.what.as_str() {
                "desire" => {
                    let output = &self.outputs[&row.get("desire_id").unwrap()];
                    if !self.claimed.contains(&output.node) {
                        continue;
                    }
                    match (&output.message, &output.record) {
                        (Some(Err(reason)), _) => Some((
                            format!("send:{}", output.id.0),
                            "output_send",
                            output.node,
                            reason,
                            "Peer message",
                            json!({"desire_id":output.id,"parent_input_id":output.input}),
                        )),
                        (_, Some(Err(reason))) => Some((
                            format!("save:{}", output.id.0),
                            "output_save",
                            output.node,
                            reason,
                            "Durable record",
                            json!({"desire_id":output.id,"parent_input_id":output.input}),
                        )),
                        _ => None,
                    }
                }
                "dispatch" => {
                    let id: InputId = row.get("input_id").unwrap();
                    let input = &self.projection.inputs[&id];
                    if !self.claimed.contains(&input.to()) {
                        continue;
                    }
                    match (input, self.input_messages.get(&id)) {
                        (Input::Client { .. }, Some(Err(reason))) => Some((
                            format!("client:{}", id.0),
                            "client_input",
                            input.to(),
                            reason,
                            "Propose",
                            json!({"input_id":id}),
                        )),
                        (Input::Message { envelope, .. }, Some(Err(reason))) => Some((
                            format!("send:{}", envelope.parent_desire_id.0),
                            "peer_input",
                            input.to(),
                            reason,
                            "Peer message",
                            json!({"input_id":id,"message_id":envelope.message_id,"parent_desire_id":envelope.parent_desire_id}),
                        )),
                        _ => None,
                    }
                }
                "result" if row.why == "save" => {
                    let output = &self.outputs[&row.get("desire_id").unwrap()];
                    if !self.header_acceptors.contains(&output.node)
                        || output.outcome.as_deref() != Some("fulfilled")
                    {
                        continue;
                    }
                    match &output.record {
                        Some(Err(reason)) => Some((
                            format!("save:{}", output.id.0),
                            "fulfilled_save",
                            output.node,
                            reason,
                            "Durable record",
                            json!({"desire_id":output.id,"parent_input_id":output.input}),
                        )),
                        _ => None,
                    }
                }
                _ => None,
            };
            if let Some((id, kind, node, reason, shape, identity)) = candidate {
                findings.entry(id.clone()).or_insert_with(|| json!({
                    "id":id,"kind":kind,"node":node,"first_line":line,
                    "occurrence_lines":[],"world_identity":identity,
                    "failed_shape":shape,"reason":reason,
                    "affected_premises":match kind {
                        "client_input"=>vec!["client_counts","fifo"],
                        "peer_input"=>vec!["campaign_quorum","received_knowledge","request_identity"],
                        "output_send"=>vec!["proposal_allocation","campaign_existence","first_quorum_map","campaign_outputs","output_conformance"],
                        "output_save"=>vec!["save_assertion","durable_state_if_fulfilled"],
                        _=>vec!["watermark","accepted_suffix","durable_support","no_conflicting_choices"],
                    }
                }));
            }
        }
        for (id, finding) in &mut findings {
            if id.starts_with("config:") {
                continue;
            }
            let mut occurrences = Vec::new();
            for (offset, row) in self.rows.iter().enumerate() {
                let matches = if let Some(value) = id.strip_prefix("client:") {
                    row.why == "client"
                        && row
                            .get::<InputId>("input_id")
                            .is_ok_and(|input| input.0.to_string() == value)
                } else {
                    let (kind, value) = id.split_once(':').unwrap();
                    let own = row.why == if kind == "send" { "send" } else { "save" }
                        && row
                            .get::<DesireId>("desire_id")
                            .is_ok_and(|desire| desire.0.to_string() == value);
                    let crossing = kind == "send"
                        && row.why == "message"
                        && row
                            .get::<DesireId>("parent_desire_id")
                            .is_ok_and(|desire| desire.0.to_string() == value);
                    own || crossing
                };
                if matches {
                    occurrences.push(offset + 1);
                }
            }
            finding["occurrence_lines"] = json!(occurrences);
        }
        self.findings = findings.into_values().collect();
        self.findings.sort_by(|a, b| {
            a["first_line"]
                .as_u64()
                .cmp(&b["first_line"].as_u64())
                .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
        });
    }

    pub fn config(&self, node: NodeId) -> Option<&Config> {
        self.configs
            .get(&node)
            .and_then(|config| config.as_ref().ok())
    }
    pub fn last_line(&self) -> usize {
        self.rows.len()
    }

    pub fn output_status(&self, output: &Output) -> Value {
        let blocking = self
            .outputs
            .values()
            .find(|prior| {
                prior.input == output.input
                    && prior.position < output.position
                    && prior.outcome.as_deref() == Some("refused")
            })
            .and_then(|prior| prior.result_line);
        let (state, open) = match (
            output.close_line,
            output.attempt_line,
            output.outcome.as_deref(),
            blocking,
        ) {
            (None, _, _, _) => ("unclosed", true),
            (_, None, _, Some(_)) => ("unreached/refused", false),
            (_, None, _, None) => ("unreached", true),
            (_, Some(_), None, _) => ("attempted", true),
            (_, _, Some("refused"), _) => ("refused", false),
            _ => ("fulfilled", false),
        };
        json!({"state":state,"world_open":open,"blocking_refusal_line":blocking})
    }
    pub fn finding_id_for_input(&self, id: InputId) -> String {
        match &self.projection.inputs[&id] {
            Input::Message { envelope, .. } => format!("send:{}", envelope.parent_desire_id.0),
            _ => format!("client:{}", id.0),
        }
    }
    pub fn input_gap_ids(&self, node: NodeId, before: usize) -> Vec<String> {
        self.projection
            .inputs
            .iter()
            .filter_map(|(id, input)| {
                (matches!(input, Input::Message { .. })
                    && input.to() == node
                    && self.projection.input_lines[id] < before
                    && self.input_messages.get(id).is_some_and(Result::is_err))
                .then(|| self.finding_id_for_input(*id))
                .filter(|id| {
                    self.findings
                        .iter()
                        .any(|finding| finding["id"].as_str() == Some(id.as_str()))
                })
            })
            .collect()
    }
    pub fn save_gap_ids(&self, node: NodeId, before: usize) -> Vec<String> {
        self.outputs
            .values()
            .filter(|output| {
                output.node == node
                    && output.outcome.as_deref() == Some("fulfilled")
                    && output.result_line.is_some_and(|line| line < before)
                    && output.record.as_ref().is_some_and(Result::is_err)
            })
            .map(|output| format!("save:{}", output.id.0))
            .collect()
    }
}

pub(super) struct Citations {
    pub support: Vec<usize>,
    pub counterevidence: Vec<usize>,
    pub gaps: Vec<String>,
}

pub(super) fn proposition(
    name: &str,
    category: &str,
    subject: Value,
    boundary: usize,
    verdict: &str,
    citations: Citations,
    reason: &str,
) -> Value {
    let Citations {
        mut support,
        mut counterevidence,
        mut gaps,
    } = citations;
    support.sort_unstable();
    support.dedup();
    counterevidence.sort_unstable();
    counterevidence.dedup();
    gaps.sort();
    gaps.dedup();
    let identity = tape::hash(&serde_json::to_vec(&subject).unwrap());
    json!({"id":format!("{name}:{boundary}:{identity}"),"proposition":name,
        "category":category,"subject":subject,"boundary_line":boundary,
        "verdict":verdict,"support":support,"counterevidence":counterevidence,
        "gaps":gaps,"reason":reason})
}

pub(super) fn gap_dependencies() -> Value {
    json!([
        {"finding_class":"configuration","first_boundary":"header declaration","identity":"config:node; header line",
         "affected_premises":["declared contract","role membership","fixed suffix","activation","initial state"],
         "affected_propositions":["configuration and contract-dependent obligations","negative knowledge and initial accepted-state coverage","adopted acceptance ancestry","conflict_explanation"],
         "propagation":"Only predicates requiring unavailable configuration/initial premises; readable certificates and received positive evidence still count.",
         "restoration":"No replacement declaration in this fixed-profile tape. Independently witnessed positive evidence can establish an evidence proposition without restoring the missing contract.",
         "report_fields":["coverage.nodes","interpretation_findings","obligations.gaps","obligations.unavailable_premises","claims.transitive_support"]},
        {"finding_class":"client_input","first_boundary":"client dispatch to a claimed actor","identity":"input_id; external/dispatch/drop occurrences",
         "affected_premises":["per-value client allowance","FIFO head"],"affected_propositions":["fresh_origin_count","fifo"],
         "propagation":"Unknown client value can change cumulative allowance and queue order from this dispatch; unrelated quorum, durable and received-knowledge facts remain usable.",
         "restoration":"No invented value or request identity. A known lower bound can prove sufficient allowance, but missing cumulative/FIFO information is not erased by a later client input.",
         "report_fields":["interpretation_findings","attempts.origin","obligations.gaps","obligations.unavailable_premises"]},
        {"finding_class":"peer_input","first_boundary":"peer dispatch to a claimed actor","identity":"parent_desire_id/message_id/input_id; all boundary copies",
         "affected_premises":["first valid Promise quorum and frozen map","received knowledge","originating request"],
         "affected_propositions":["phase2_gate","promise_report_handling","adoption","fresh_origin_count","fifo","proposal_drain","claim evidence","reply_request","conflict_explanation"],
         "propagation":"An unreadable pre-freeze input can make the first quorum/map undetermined. Once an undetermined-origin allocation occurs, later counts for every value remain indeterminate in that campaign. Actual readable positive knowledge paths remain sufficient.",
         "restoration":"No later Promise rewrites a first quorum or restores uncertain cumulative consumption. A complete independent positive evidence path can establish claim support.",
         "report_fields":["interpretation_findings","attempts.origin","claims.ballot_paths","obligations.gaps","obligations.unavailable_premises"]},
        {"finding_class":"output_send","first_boundary":"Send desire from a claimed actor","identity":"desire_id joined to attempt/result/message crossings",
         "affected_premises":["proposal/campaign assertions","campaign existence and first quorum/map","allocation count at close","recipient knowledge only upon dispatch"],
         "affected_propositions":["activation","no_retry","phase2_gate","promise_report_handling","adoption","fresh_origin_count","fifo","proposal_drain","attempt coverage","conflict_explanation"],
         "propagation":"At close an unreadable assertion may hide either an allocation or a Prepare creating an unavailable campaign/map. Negative missing-campaign conclusions must carry that uncertainty. Already known campaigns and frozen maps are not tainted by unrelated later output. Downstream unreadable dispatch separately limits its receiver's premises under the same identity.",
         "restoration":"A later Prepare cannot reconstruct the missing earlier campaign/map. A readable matching distinct Promise quorum can independently establish the Phase-2 gate, not an uncertain first map or cumulative count. Positive certificates and independent knowledge paths remain usable.",
         "report_fields":["interpretation_findings","coverage","attempts.origin","obligations.gaps","obligations.unavailable_premises"]},
        {"finding_class":"output_save","first_boundary":"Save desire from a claimed actor","identity":"desire_id; desire/attempt/result occurrences",
         "affected_premises":["returned save assertion","durable state only if fulfilled"],
         "affected_propositions":["acceptor conformance","durable-dependent obligations after fulfillment"],
         "propagation":"No durable uncertainty follows from an unreached or open Save alone. At fulfillment apply the fulfilled_save dependencies, preserving the original finding and all occurrences.",
         "restoration":"Same as fulfilled_save after fulfillment; an unfulfilled Save cannot supply or erase durable support.",
         "report_fields":["interpretation_findings","obligations.gaps","world.account"]},
        {"finding_class":"fulfilled_save","first_boundary":"fulfilled Save result from a header acceptor unless already scoped at desire","identity":"desire_id, original output chain",
         "affected_premises":["watermark","accepted suffix","historical acceptance coverage","missing durable support"],
         "affected_propositions":["acceptor_watermark","accepted_watermark","promise_watermark","promise_suffix","accepted_reply_support","no_conflicting_choices","claim transitive_support","adopted acceptance ancestry","conflict_explanation"],
         "propagation":"Historical coverage remains incomplete. Later readable acceptance facts still contribute to every certificate. Absence of a recorded conflict or earlier matching save is not proof across the gap.",
         "restoration":"A later explicit Promised assertion can identify that current watermark, not erase uncertain earlier monotonicity or accepted history. A later matching fulfilled acceptance can support replies admitted after it, never earlier replies or global historical completeness.",
         "report_fields":["interpretation_findings","choices","obligations.gaps","claims.transitive_support","attempts.origin.acceptance_ancestry"]}
    ])
}
