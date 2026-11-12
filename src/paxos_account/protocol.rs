use crate::{NodeId, Slot};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(super) struct Ballot(pub u64, pub NodeId);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(super) struct Config {
    pub acceptors: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub proposers: BTreeSet<NodeId>,
    pub active_proposer: Option<NodeId>,
    pub from_slot: Slot,
    pub initial_round: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(super) struct Accepted {
    pub slot: Slot,
    pub ballot: Ballot,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum Message {
    Propose {
        value: Value,
    },
    Prepare {
        ballot: Ballot,
        from_slot: Slot,
    },
    Promise {
        ballot: Ballot,
        from_slot: Slot,
        accepted: Vec<Accepted>,
    },
    Accept {
        ballot: Ballot,
        slot: Slot,
        value: Value,
    },
    Accepted {
        ballot: Ballot,
        slot: Slot,
        value: Value,
    },
    Nack {
        rejected_ballot: Ballot,
        promised_ballot: Ballot,
    },
    Chosen {
        ballot: Ballot,
        slot: Slot,
        value: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub(super) enum Record {
    Promised {
        from_slot: Slot,
        ballot: Ballot,
    },
    Accepted {
        slot: Slot,
        ballot: Ballot,
        value: Value,
    },
}

fn object<'a>(
    raw: &'a Value,
    fields: &[&str],
    shape: &str,
) -> Result<&'a Map<String, Value>, String> {
    let object = raw
        .as_object()
        .ok_or_else(|| format!("{shape} must be an object"))?;
    for field in fields {
        if !object.contains_key(*field) {
            return Err(format!("{shape} is missing required field {field}"));
        }
    }
    if let Some(field) = object
        .keys()
        .find(|field| !fields.contains(&field.as_str()))
    {
        return Err(format!("{shape} has unknown field {field}"));
    }
    Ok(object)
}

fn unsigned(raw: &Value, field: &str) -> Result<u64, String> {
    raw.as_u64()
        .ok_or_else(|| format!("{field} must be an unsigned 64-bit integer"))
}

fn node_id(raw: &Value, field: &str) -> Result<NodeId, String> {
    raw.as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .map(NodeId)
        .ok_or_else(|| format!("{field} must be a signed 32-bit integer"))
}

fn role_node(raw: &Value, field: &str) -> Result<NodeId, String> {
    let node = node_id(raw, field)?;
    if node.0 < 0 {
        return Err(format!("{field} must be a nonnegative NodeId"));
    }
    Ok(node)
}

fn role_set(raw: &Value, field: &str) -> Result<BTreeSet<NodeId>, String> {
    let nodes = raw
        .as_array()
        .ok_or_else(|| format!("{field} must be a nonempty array of distinct NodeIds"))?;
    if nodes.is_empty() {
        return Err(format!("{field} must be nonempty"));
    }
    let mut set = BTreeSet::new();
    for raw in nodes {
        let node = role_node(raw, field)?;
        if !set.insert(node) {
            return Err(format!("{field} contains duplicate node {}", node.0));
        }
    }
    Ok(set)
}

pub(super) fn config(node: NodeId, raw: &Value) -> Result<Config, String> {
    let raw = object(
        raw,
        &[
            "acceptors",
            "learners",
            "proposers",
            "active_proposer",
            "from_slot",
            "initial_round",
            "initial_state",
        ],
        "Config",
    )?;
    let acceptors = role_set(&raw["acceptors"], "Config.acceptors")?;
    let learners = role_set(&raw["learners"], "Config.learners")?;
    let proposers = role_set(&raw["proposers"], "Config.proposers")?;
    let active_proposer = if raw["active_proposer"].is_null() {
        None
    } else {
        let active = role_node(&raw["active_proposer"], "Config.active_proposer")?;
        if !proposers.contains(&active) {
            return Err("Config.active_proposer must belong to proposers".into());
        }
        Some(active)
    };
    let from_slot = Slot(unsigned(&raw["from_slot"], "Config.from_slot")?);
    let initial_round = unsigned(&raw["initial_round"], "Config.initial_round")?;
    if raw["initial_state"].as_str() != Some("empty") {
        return Err("Config.initial_state must be exactly \"empty\"".into());
    }
    if !acceptors.contains(&node) && !learners.contains(&node) && !proposers.contains(&node) {
        return Err(format!(
            "configured node {} must belong to at least one role",
            node.0
        ));
    }
    Ok(Config {
        acceptors,
        learners,
        proposers,
        active_proposer,
        from_slot,
        initial_round,
    })
}

pub(super) fn ballot(raw: &Value) -> Result<Ballot, String> {
    let pair = raw
        .as_array()
        .filter(|pair| pair.len() == 2)
        .ok_or_else(|| "Ballot must be exactly [round, proposer]".to_string())?;
    Ok(Ballot(
        unsigned(&pair[0], "Ballot.round")?,
        node_id(&pair[1], "Ballot.proposer")?,
    ))
}

fn tag<'a>(raw: &'a Value, field: &str, shape: &str) -> Result<&'a str, String> {
    raw.as_object()
        .ok_or_else(|| format!("{shape} must be an object"))?
        .get(field)
        .ok_or_else(|| format!("{shape} is missing required field {field}"))?
        .as_str()
        .ok_or_else(|| format!("{shape}.{field} must be a string"))
}

pub(super) fn client(raw: &Value) -> Result<Message, String> {
    if tag(raw, "type", "client message")? != "propose" {
        return Err("client message type must be propose".into());
    }
    let raw = object(raw, &["type", "value"], "Propose")?;
    Ok(Message::Propose {
        value: raw["value"].clone(),
    })
}

fn accepted_entry(raw: &Value) -> Result<Accepted, String> {
    let raw = object(raw, &["slot", "ballot", "value"], "accepted suffix entry")?;
    Ok(Accepted {
        slot: Slot(unsigned(&raw["slot"], "accepted suffix entry.slot")?),
        ballot: ballot(&raw["ballot"])?,
        value: raw["value"].clone(),
    })
}

pub(super) fn peer(raw: &Value) -> Result<Message, String> {
    match tag(raw, "type", "peer message")? {
        "prepare" => {
            let raw = object(raw, &["type", "ballot", "from_slot"], "Prepare")?;
            Ok(Message::Prepare {
                ballot: ballot(&raw["ballot"])?,
                from_slot: Slot(unsigned(&raw["from_slot"], "Prepare.from_slot")?),
            })
        }
        "promise" => {
            let raw = object(raw, &["type", "ballot", "from_slot", "accepted"], "Promise")?;
            let ballot = ballot(&raw["ballot"])?;
            let from_slot = Slot(unsigned(&raw["from_slot"], "Promise.from_slot")?);
            let entries = raw["accepted"]
                .as_array()
                .ok_or_else(|| "Promise.accepted must be an array".to_string())?;
            let mut accepted = Vec::with_capacity(entries.len());
            for (index, entry) in entries.iter().enumerate() {
                accepted.push(
                    accepted_entry(entry)
                        .map_err(|reason| format!("Promise.accepted[{index}]: {reason}"))?,
                );
            }
            // Slot order, suffix, campaign bounds and owner eligibility are testimony
            // semantics. Preserve all syntactically readable entries for that judgment.
            Ok(Message::Promise {
                ballot,
                from_slot,
                accepted,
            })
        }
        kind @ ("accept" | "accepted" | "chosen") => {
            let raw = object(raw, &["type", "ballot", "slot", "value"], kind)?;
            let ballot = ballot(&raw["ballot"])?;
            let slot = Slot(unsigned(&raw["slot"], "peer message.slot")?);
            let value = raw["value"].clone();
            Ok(match kind {
                "accept" => Message::Accept {
                    ballot,
                    slot,
                    value,
                },
                "accepted" => Message::Accepted {
                    ballot,
                    slot,
                    value,
                },
                "chosen" => Message::Chosen {
                    ballot,
                    slot,
                    value,
                },
                _ => unreachable!(),
            })
        }
        "nack" => {
            let raw = object(raw, &["type", "rejected_ballot", "promised_ballot"], "Nack")?;
            Ok(Message::Nack {
                rejected_ballot: ballot(&raw["rejected_ballot"])?,
                promised_ballot: ballot(&raw["promised_ballot"])?,
            })
        }
        kind => Err(format!("unknown peer message type {kind:?}")),
    }
}

pub(super) fn record(raw: &Value) -> Result<Record, String> {
    match tag(raw, "record", "durable record")? {
        "promised" => {
            let raw = object(raw, &["record", "from_slot", "ballot"], "Promised record")?;
            Ok(Record::Promised {
                from_slot: Slot(unsigned(&raw["from_slot"], "Promised.from_slot")?),
                ballot: ballot(&raw["ballot"])?,
            })
        }
        "accepted" => {
            let raw = object(
                raw,
                &["record", "slot", "ballot", "value"],
                "Accepted record",
            )?;
            Ok(Record::Accepted {
                slot: Slot(unsigned(&raw["slot"], "Accepted.slot")?),
                ballot: ballot(&raw["ballot"])?,
                value: raw["value"].clone(),
            })
        }
        kind => Err(format!("unknown durable record type {kind:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn declaration() -> Value {
        json!({
            "acceptors": [2, 0, 1], "learners": [0, 1, 2], "proposers": [0, 3],
            "active_proposer": null, "from_slot": 0, "initial_round": 1,
            "initial_state": "empty"
        })
    }

    #[test]
    fn config_requires_every_field_and_rejects_extras() {
        let original = declaration();
        let parsed = config(NodeId(0), &original).unwrap();
        assert_eq!(parsed.active_proposer, None);
        assert_eq!(
            parsed.acceptors.into_iter().collect::<Vec<_>>(),
            vec![NodeId(0), NodeId(1), NodeId(2)]
        );
        for field in original.as_object().unwrap().keys() {
            let mut missing = original.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(config(NodeId(0), &missing).is_err(), "missing {field}");
        }
        let mut extra = original;
        extra["quorum"] = json!(2);
        assert!(config(NodeId(0), &extra).is_err());
    }

    #[test]
    fn config_checks_numeric_list_initial_and_local_role_shapes() {
        for field in ["acceptors", "learners", "proposers"] {
            for invalid in [
                json!([]),
                json!([0, 0]),
                json!([-1]),
                json!([2147483648u64]),
                json!([1.0]),
                json!([true]),
                json!(["0"]),
                Value::Null,
                json!({"0": true}),
            ] {
                let mut raw = declaration();
                raw[field] = invalid.clone();
                assert!(config(NodeId(0), &raw).is_err(), "{field}: {invalid}");
            }
        }
        for field in ["from_slot", "initial_round"] {
            for invalid in [
                json!(-1),
                json!(1.0),
                json!(true),
                json!("1"),
                Value::Null,
                serde_json::from_str("18446744073709551616").unwrap(),
            ] {
                let mut raw = declaration();
                raw[field] = invalid.clone();
                assert!(config(NodeId(0), &raw).is_err(), "{field}: {invalid}");
            }
        }
        for invalid in [Value::Null, json!("Empty"), json!({}), json!(0)] {
            let mut raw = declaration();
            raw["initial_state"] = invalid;
            assert!(config(NodeId(0), &raw).is_err());
        }
        for invalid in [
            json!(-1),
            json!(2147483648u64),
            json!(1.0),
            json!(true),
            json!("0"),
            json!(2),
        ] {
            let mut raw = declaration();
            raw["active_proposer"] = invalid;
            assert!(config(NodeId(0), &raw).is_err());
        }
        assert!(config(NodeId(9), &declaration()).is_err());
        assert!(config(NodeId(-1), &declaration()).is_err());
        assert!(config(NodeId(3), &declaration()).is_ok());
        let mut maximum = declaration();
        maximum["from_slot"] = json!(u64::MAX);
        maximum["initial_round"] = json!(u64::MAX);
        maximum["active_proposer"] = json!(3);
        assert_eq!(
            config(NodeId(3), &maximum).unwrap().active_proposer,
            Some(NodeId(3))
        );
    }

    #[test]
    fn ballot_preserves_representable_ineligible_owners_and_exact_integer_ranges() {
        assert_eq!(
            ballot(&json!([u64::MAX, i32::MIN])).unwrap(),
            Ballot(u64::MAX, NodeId(i32::MIN))
        );
        assert_eq!(
            ballot(&json!([0, i32::MAX])).unwrap(),
            Ballot(0, NodeId(i32::MAX))
        );
        assert!(Ballot(2, NodeId(-1)) > Ballot(1, NodeId(i32::MAX)));
        assert!(Ballot(2, NodeId(0)) > Ballot(2, NodeId(-1)));
        for invalid in [
            Value::Null,
            json!({"round": 1, "proposer": 0}),
            json!([]),
            json!([1]),
            json!([1, 0, 0]),
            json!([-1, 0]),
            json!([1.0, 0]),
            json!([1, 0.0]),
            json!([1, 2147483648u64]),
            json!([1, -2147483649i64]),
            json!([true, 0]),
            json!([1, "0"]),
            serde_json::from_str("[18446744073709551616,0]").unwrap(),
        ] {
            assert!(ballot(&invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn required_arbitrary_values_distinguish_null_from_missing() {
        assert_eq!(
            client(&json!({"type": "propose", "value": null})).unwrap(),
            Message::Propose { value: Value::Null }
        );
        assert!(client(&json!({"type": "propose"})).is_err());
        for kind in ["accept", "accepted", "chosen"] {
            let mut raw = json!({"type": kind, "ballot": [1, 0], "slot": 0, "value": null});
            assert!(peer(&raw).is_ok());
            raw.as_object_mut().unwrap().remove("value");
            assert!(peer(&raw).is_err());
        }
        let mut promise = json!({"type": "promise", "ballot": [1, 0], "from_slot": 0, "accepted": [{"slot": 0, "ballot": [0, 0], "value": null}]});
        assert!(peer(&promise).is_ok());
        promise["accepted"][0]
            .as_object_mut()
            .unwrap()
            .remove("value");
        assert!(peer(&promise).is_err());
        let mut accepted =
            json!({"record": "accepted", "slot": 0, "ballot": [1, 0], "value": null});
        assert!(record(&accepted).is_ok());
        accepted.as_object_mut().unwrap().remove("value");
        assert!(record(&accepted).is_err());
        let arbitrary = json!({"nested": [null, true, {"type": "not-a-message"}]});
        assert_eq!(
            client(&json!({"type": "propose", "value": arbitrary})).unwrap(),
            Message::Propose { value: arbitrary }
        );
    }

    #[test]
    fn every_message_and_record_requires_exact_fields_at_its_boundary() {
        let peers = [
            json!({"type": "prepare", "ballot": [1, 0], "from_slot": 0}),
            json!({"type": "promise", "ballot": [1, 0], "from_slot": 0, "accepted": []}),
            json!({"type": "accept", "ballot": [1, 0], "slot": 0, "value": null}),
            json!({"type": "accepted", "ballot": [1, 0], "slot": 0, "value": null}),
            json!({"type": "nack", "rejected_ballot": [1, 0], "promised_ballot": [2, 1]}),
            json!({"type": "chosen", "ballot": [1, 0], "slot": 0, "value": null}),
        ];
        for original in peers {
            assert!(peer(&original).is_ok());
            assert!(client(&original).is_err());
            assert!(record(&original).is_err());
            for field in original.as_object().unwrap().keys() {
                let mut missing = original.clone();
                missing.as_object_mut().unwrap().remove(field);
                assert!(peer(&missing).is_err(), "{original}: missing {field}");
            }
            let mut extra = original;
            extra["unexpected"] = Value::Null;
            assert!(peer(&extra).is_err());
        }
        for original in [
            json!({"record": "promised", "ballot": [1, 0], "from_slot": 0}),
            json!({"record": "accepted", "ballot": [1, 0], "slot": 0, "value": null}),
        ] {
            assert!(record(&original).is_ok());
            assert!(peer(&original).is_err());
            for field in original.as_object().unwrap().keys() {
                let mut missing = original.clone();
                missing.as_object_mut().unwrap().remove(field);
                assert!(record(&missing).is_err(), "{original}: missing {field}");
            }
            let mut extra = original;
            extra["unexpected"] = json!(0);
            assert!(record(&extra).is_err());
        }
        assert!(peer(&json!({"type": "propose", "value": null})).is_err());
        assert!(client(&json!({"type": "propose", "value": null, "extra": 1})).is_err());
        assert!(peer(&json!({"type": "future"})).is_err());
        assert!(record(&json!({"record": "future"})).is_err());
        assert!(peer(&json!({"type": null})).is_err());
        assert!(record(&json!({"record": 1})).is_err());
        assert!(client(&Value::Null).is_err());
    }

    #[test]
    fn promise_syntax_keeps_all_semantically_invalid_testimony() {
        let raw = json!({"type": "promise", "ballot": [2, 0], "from_slot": 5, "accepted": [
            {"slot": 3, "ballot": [1, 0], "value": "below suffix"},
            {"slot": 3, "ballot": [3, 0], "value": "duplicate and above campaign"},
            {"slot": 1, "ballot": [1, -1], "value": "descending and ineligible owner"}
        ]});
        let Message::Promise { accepted, .. } = peer(&raw).unwrap() else {
            panic!("expected Promise")
        };
        assert_eq!(
            accepted
                .iter()
                .map(|entry| entry.slot.0)
                .collect::<Vec<_>>(),
            vec![3, 3, 1]
        );
        assert_eq!(accepted[1].ballot, Ballot(3, NodeId(0)));
        assert_eq!(accepted[2].ballot, Ballot(1, NodeId(-1)));
        let mut malformed = raw.clone();
        malformed["accepted"][0]["extra"] = Value::Null;
        assert!(peer(&malformed).is_err());
        malformed = raw.clone();
        malformed["accepted"][0]["slot"] = json!(1.0);
        assert!(peer(&malformed).is_err());
        malformed = raw;
        malformed["accepted"] = Value::Null;
        assert!(peer(&malformed).is_err());
    }
}
