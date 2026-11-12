use quorum::multi_paxos::propose;
use quorum::paxos_account::examine;
use quorum::tape::{self, Event, Header, NodeSpec, WorldConfig};
use quorum::{Context, Desire, Input, Node, NodeId, Simulator, scenarios};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, io};

fn config(acceptors: &[i32], members: &[i32], active: Option<i32>, round: u64) -> Value {
    json!({
        "acceptors":acceptors,"learners":members,"proposers":members,
        "active_proposer":active,"from_slot":0,"initial_round":round,"initial_state":"empty"
    })
}

fn simulator(specs: Vec<NodeSpec>, ticks: bool) -> Simulator {
    Simulator::new(
        Header::new(
            "paxos-account-self-check",
            42,
            WorldConfig {
                base_latency: 1,
                jitter: 0,
                tick_interval: u64::from(ticks),
                deadlock_threshold: 200,
            },
            specs,
            vec![NodeId(0), NodeId(1), NodeId(2)],
        ),
        Box::new(io::sink()),
    )
    .unwrap()
}

fn capture(name: &str, rows: &[Event]) -> Value {
    let bytes = tape::encode(rows);
    let report = examine(&bytes);
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/paxos-account-evidence");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join(format!("{name}.jsonl")), &bytes).unwrap();
    fs::write(
        root.join(format!("{name}.report.json")),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["source_sha256"], tape::hash(&bytes));
    assert!(report["world"]["error"].is_null(), "{report}");
    assert_eq!(report["world"]["valid_rows"], rows.len());
    report
}

fn array<'a>(value: &'a Value, name: &str) -> &'a [Value] {
    value[name].as_array().unwrap()
}

fn self_check(report: &Value) -> &Value {
    let checks: Vec<_> = array(report, "obligations")
        .iter()
        .filter(|p| p["proposition"] == "conflict_explanation")
        .collect();
    assert_eq!(checks.len(), 1, "{report}");
    checks[0]
}

fn coverage(report: &Value) -> &Value {
    array(report, "observations")
        .iter()
        .find(|o| {
            o["kind"] == "conflict_explanation" || o["kind"] == "unexplained_conflicting_choices"
        })
        .unwrap()
}

fn certificate<'a>(report: &'a Value, rows: &[Event], value: &str, ballot: [u64; 2]) -> &'a Value {
    let certificate = array(report, "choices")
        .iter()
        .find(|c| c["slot"] == 0 && c["ballot"] == json!(ballot) && c["value"] == value)
        .unwrap();
    assert_eq!(certificate["quorum"], 2);
    assert_eq!(array(certificate, "first_support").len(), 2);
    for witness in array(certificate, "first_support") {
        let line = witness["save_line"].as_u64().unwrap() as usize;
        let row = &rows[line - 1];
        assert_eq!(row.what, "result");
        assert_eq!(row.why, "save");
        assert_eq!(row.fields["outcome"], "fulfilled");
        assert_eq!(row.fields["node"], witness["acceptor"]);
        assert!([json!(0), json!(1), json!(2)].contains(&witness["acceptor"]));
        assert_eq!(
            row.fields["record"],
            json!({"record":"accepted","slot":0,"ballot":ballot,"value":value})
        );
    }
    assert_eq!(
        certificate["first_quorum_line"],
        array(certificate, "first_support")[1]["save_line"]
    );
    certificate
}

#[test]
fn real_five_member_history_explains_conflicting_header_certificates() {
    // Audit s2: x reaches two acceptors but not the declared five-member
    // majority. The second proposer's first declared quorum knows no x.
    let members = [0, 1, 2, 3, 4];
    let specs: Vec<_> = members
        .iter()
        .map(|&node| {
            let mut declaration = config(
                &members,
                &members,
                match node {
                    0 => Some(0),
                    4 => Some(4),
                    _ => None,
                },
                if node == 4 { 2 } else { 1 },
            );
            declaration["proposers"] = json!([0, 4]);
            NodeSpec {
                node: NodeId(node),
                algorithm: "multi-paxos".into(),
                config: declaration,
            }
        })
        .collect();
    let mut sim = simulator(specs.clone(), true);
    for spec in &specs[..4] {
        sim.spawn(spec.node, scenarios::factory(spec).unwrap())
            .unwrap();
    }
    sim.client_message(NodeId(0), propose(json!("x"))).unwrap();
    sim.run(3).unwrap();
    sim.partition(NodeId(0), NodeId(2), 9).unwrap();
    sim.partition(NodeId(0), NodeId(3), 9).unwrap();
    sim.run(5).unwrap();
    sim.spawn(specs[4].node, scenarios::factory(&specs[4]).unwrap())
        .unwrap();
    sim.client_message(NodeId(4), propose(json!("y"))).unwrap();
    sim.partition(NodeId(0), NodeId(4), 7).unwrap();
    sim.partition(NodeId(1), NodeId(4), 7).unwrap();
    sim.run(20).unwrap();
    sim.finalize("horizon").unwrap();

    let report = capture("self-check-real-header-universe-mismatch", sim.rows());
    assert!(array(&report, "interpretation_findings").is_empty());
    assert_eq!(report["coverage"]["header_acceptors"], json!([0, 1, 2]));
    assert_eq!(array(&report, "choices").len(), 2);
    let x = certificate(&report, sim.rows(), "x", [1, 0]);
    let y = certificate(&report, sim.rows(), "y", [2, 4]);
    assert!(x["first_quorum_line"].as_u64() < y["first_quorum_line"].as_u64());
    assert!(
        array(&report, "obligations").iter().any(|p| {
            p["proposition"] == "no_conflicting_choices" && p["verdict"] == "violated"
        })
    );
    let mismatches: Vec<_> = array(&report, "obligations")
        .iter()
        .filter(|p| {
            p["proposition"] == "config_agreement"
                && p["subject"]["other"] == "header.acceptors"
                && p["verdict"] == "violated"
        })
        .collect();
    assert_eq!(mismatches.len(), 5);
    let check = self_check(&report);
    assert_eq!(check["verdict"], "established");
    assert_eq!(check["boundary_line"], y["first_quorum_line"]);
    assert!(array(check, "support").contains(&json!(1)));
    assert!(check.get("unavailable_premises").is_none());
    let finding = coverage(&report);
    assert_eq!(finding["fully_covered"], true);
    assert_eq!(finding["account_deficiency"], false);
    assert_eq!(
        array(finding, "explanatory_obligations").len(),
        mismatches.len()
    );
    for mismatch in mismatches {
        assert!(array(finding, "relevant_nodes").contains(&mismatch["subject"]["node"]));
        assert!(array(finding, "explanatory_obligations").contains(&mismatch["id"]));
        assert_eq!(mismatch["counterevidence"], json!([1]));
    }

    // Header membership remains the claimant's basis, rather than quietly
    // switching to the five-member universe that the inhabitant actually used.
    let claims = array(&report, "claims");
    assert_eq!(claims.len(), 10);
    for claim in claims {
        assert_eq!(claim["value"], "y");
        assert_eq!(
            claim["local_basis"]["verdict"],
            if claim["node"] == 4 {
                "violated"
            } else {
                "established"
            }
        );
        assert_eq!(claim["transitive_support"]["verdict"], "violated");
        assert!(array(claim, "choice_at_boundary").contains(&y["id"]));
    }
}

// Negative controls deliberately assert acceptance histories without a Paxos
// campaign. They expose explanatory holes rather than trusting a node label.
struct Saves {
    answers: BTreeMap<String, Vec<Desire>>,
    crashed: bool,
}

impl Node for Saves {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        if let Input::Client { payload, .. } = input
            && let Some(value) = payload["value"].as_str()
            && let Some(answer) = self.answers.remove(value)
        {
            for desire in answer {
                let Desire::Save { record } = desire else {
                    unreachable!()
                };
                ctx.save(record);
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

#[derive(Clone, Copy)]
enum Contract {
    Covered,
    Malformed,
    Undeclared,
}

fn conflicting_saves(
    relevant_contract: Contract,
    unrelated_mismatch: bool,
    watermark_failure: bool,
) -> Simulator {
    let mut specs: Vec<_> = (0..4)
        .map(|node| NodeSpec {
            node: NodeId(node),
            algorithm: "multi-paxos".into(),
            config: config(&[0, 1, 2], &[0, 1, 2, 3], None, 1),
        })
        .collect();
    match relevant_contract {
        Contract::Covered => {}
        Contract::Malformed => {
            specs[1]
                .config
                .as_object_mut()
                .unwrap()
                .remove("initial_state");
        }
        Contract::Undeclared => {
            specs[1].algorithm = "self-check-script".into();
            specs[1].config = json!({});
        }
    }
    if unrelated_mismatch {
        specs[3].config["acceptors"] = json!([0, 1, 2, 3, 4]);
    }
    let mut sim = simulator(specs, false);
    for node in 0..4 {
        let mut answers = BTreeMap::new();
        if node < 2 {
            for (round, value) in [(1, "x"), (2, "y")] {
                let mut answer = Vec::new();
                if !(watermark_failure && node == 0 && round == 2) {
                    answer.push(Desire::Save {
                        record: json!({"record":"promised","from_slot":0,"ballot":[round,0]}),
                    });
                }
                answer.push(Desire::Save {
                    record: json!({"record":"accepted","slot":0,"ballot":[round,0],"value":value}),
                });
                answers.insert(value.into(), answer);
            }
        }
        sim.spawn(
            NodeId(node),
            Box::new(Saves {
                answers,
                crashed: false,
            }),
        )
        .unwrap();
    }
    for value in ["x", "y"] {
        for node in [0, 1] {
            sim.client_message(NodeId(node), propose(json!(value)))
                .unwrap();
        }
    }
    sim
}

#[test]
fn unrelated_configuration_failure_cannot_hide_an_account_deficiency() {
    let sim = conflicting_saves(Contract::Covered, true, false);
    let report = capture("self-check-unrelated-config-mismatch", sim.rows());
    assert_eq!(array(&report, "choices").len(), 2);
    assert_eq!(self_check(&report)["verdict"], "violated");
    let finding = coverage(&report);
    assert_eq!(finding["relevant_nodes"], json!([0, 1]));
    assert_eq!(finding["fully_covered"], true);
    assert_eq!(finding["account_deficiency"], true);
    assert!(array(finding, "explanatory_obligations").is_empty());
    assert!(array(&report, "obligations").iter().any(|p| {
        p["proposition"] == "config_agreement"
            && p["subject"] == json!({"node":3,"other":"header.acceptors"})
            && p["verdict"] == "violated"
    }));
    // Even the pairwise comparison of a causal participant to the unrelated
    // actor is not a header-universe mismatch belonging to that participant.
    assert!(array(&report, "obligations").iter().any(|p| {
        p["proposition"] == "config_agreement"
            && p["subject"] == json!({"node":0,"other":3})
            && p["verdict"] == "violated"
    }));
}

#[test]
fn unavailable_relevant_configuration_is_located_without_fabricated_parse_gaps() {
    for (contract, name, malformed) in [
        (
            Contract::Malformed,
            "self-check-malformed-relevant-config",
            true,
        ),
        (
            Contract::Undeclared,
            "self-check-undeclared-relevant-config",
            false,
        ),
    ] {
        let sim = conflicting_saves(contract, false, false);
        let report = capture(name, sim.rows());
        assert_eq!(array(&report, "choices").len(), 2);
        let check = self_check(&report);
        assert_eq!(check["verdict"], "indeterminate");
        let finding = coverage(&report);
        assert_eq!(finding["fully_covered"], false);
        assert_eq!(finding["account_deficiency"], false);
        assert_eq!(finding["undeclared_contracts"], json!([1]));
        let premises = array(check, "unavailable_premises");
        assert_eq!(premises.len(), 1);
        let premise = &premises[0];
        assert_eq!(premise["premise"], "configuration");
        assert_eq!(premise["nodes"], json!([1]));
        assert!(array(premise, "support").contains(&json!(1)));
        assert!(array(premise, "counterevidence").is_empty());
        for choice in array(&report, "choices") {
            let acceptance = array(choice, "first_support")
                .iter()
                .find(|w| w["acceptor"] == 1)
                .unwrap();
            assert!(array(premise, "support").contains(&acceptance["save_line"]));
        }
        let findings = array(&report, "interpretation_findings");
        let expected_gaps = if malformed {
            json!(["config:1"])
        } else {
            json!([])
        };
        assert_eq!(premise["gaps"], expected_gaps);
        assert_eq!(check["gaps"], expected_gaps);
        assert_eq!(findings.len(), usize::from(malformed));
        for gap in array(premise, "gaps") {
            assert!(findings.iter().any(|f| &f["id"] == gap));
        }
    }
}

#[test]
fn an_independent_explanation_survives_unavailable_relevant_configuration() {
    let sim = conflicting_saves(Contract::Undeclared, false, true);
    let report = capture(
        "self-check-known-failure-with-undeclared-config",
        sim.rows(),
    );
    let check = self_check(&report);
    assert_eq!(check["verdict"], "established");
    assert!(check.get("unavailable_premises").is_none());
    let finding = coverage(&report);
    assert_eq!(finding["fully_covered"], false);
    assert_eq!(finding["account_deficiency"], false);
    let failures: Vec<_> = array(&report, "obligations")
        .iter()
        .filter(|p| {
            p["proposition"] == "accepted_watermark"
                && p["subject"]["node"] == 0
                && p["subject"]["slot"] == 0
                && p["verdict"] == "violated"
        })
        .collect();
    assert_eq!(failures.len(), 1);
    assert_eq!(
        finding["explanatory_obligations"],
        json!([failures[0]["id"]])
    );
}
