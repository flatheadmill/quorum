//! JSONL witness grammar. Algorithm Values are nested, never merged into it.
use crate::{Desire, DesireId, Envelope, Input, InputId, NodeId, Result};
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{fmt, io::Write};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Implementation {
    pub crate_name: String,
    pub version: String,
    pub revision: String,
    pub dirty: bool,
    pub source_sha256: String,
}
impl Implementation {
    pub fn current() -> Self {
        Self {
            crate_name: env!("CARGO_PKG_NAME").into(),
            version: env!("CARGO_PKG_VERSION").into(),
            revision: env!("QUORUM_REVISION").into(),
            dirty: env!("QUORUM_DIRTY") == "true",
            source_sha256: env!("QUORUM_SOURCE_SHA256").into(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorldConfig {
    pub base_latency: u64,
    pub jitter: u64,
    pub tick_interval: u64,
    pub deadlock_threshold: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    pub node: NodeId,
    pub algorithm: String,
    pub config: Value,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    pub schema: u32,
    pub implementation: Implementation,
    pub scenario: String,
    pub seed: u64,
    pub clock: String,
    pub world: WorldConfig,
    pub nodes: Vec<NodeSpec>,
    pub acceptors: Vec<NodeId>,
}
impl Header {
    pub fn new(
        scenario: &str,
        seed: u64,
        world: WorldConfig,
        nodes: Vec<NodeSpec>,
        acceptors: Vec<NodeId>,
    ) -> Self {
        Self {
            schema: 1,
            implementation: Implementation::current(),
            scenario: scenario.into(),
            seed,
            clock: "logical_tick".into(),
            world,
            nodes,
            acceptors,
        }
    }
}

/// The spine is always world-owned. Only world constructors supply these fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    pub when: u64,
    pub who: String,
    pub what: String,
    pub why: String,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}
impl Event {
    pub fn new(when: u64, who: String, what: &str, why: &str, fields: Value) -> Self {
        let fields = fields
            .as_object()
            .expect("world event fields are an object")
            .clone();
        assert!(
            ["seq", "when", "who", "what", "why"]
                .iter()
                .all(|k| !fields.contains_key(*k))
        );
        Self {
            seq: 0,
            when,
            who,
            what: what.into(),
            why: why.into(),
            fields,
        }
    }
    pub fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<T> {
        let value = self
            .fields
            .get(key)
            .ok_or_else(|| format!("missing {key}"))?;
        serde_json::from_value(value.clone()).map_err(|e| format!("{key}: {e}"))
    }
    pub fn value(&self, key: &str) -> Result<Value> {
        self.get(key)
    }
    pub fn input(&self) -> Result<Input> {
        let input_id = self.get("input_id")?;
        match self.why.as_str() {
            "tick" => Ok(Input::Tick {
                input_id,
                to: self.get("to")?,
            }),
            "client" => Ok(Input::Client {
                input_id,
                to: self.get("to")?,
                payload: self.value("message")?,
            }),
            "message" => Ok(Input::Message {
                input_id,
                envelope: self.envelope()?,
            }),
            _ => Err("not a dispatch variant".into()),
        }
    }
    pub fn desire(&self) -> Result<Desire> {
        match self.why.as_str() {
            "send" => Ok(Desire::Send {
                to: self.get("to")?,
                message: self.value("message")?,
            }),
            "save" => Ok(Desire::Save {
                record: self.value("record")?,
            }),
            "commit" => Ok(Desire::Commit {
                slot: self.get("slot")?,
                entry: self.value("entry")?,
            }),
            _ => Err("not a desire variant".into()),
        }
    }
    pub fn envelope(&self) -> Result<Envelope> {
        Ok(Envelope {
            message_id: self.get("message_id")?,
            parent_desire_id: self.get("parent_desire_id")?,
            from: self.get("from")?,
            to: self.get("to")?,
            payload: self.value("message")?,
            send_tick: self.get("send_tick")?,
            deliver_tick: self.get("deliver_tick")?,
        })
    }
    pub fn send_envelope(&self) -> Result<Envelope> {
        Ok(Envelope {
            message_id: self.get("message_id")?,
            parent_desire_id: self.get("desire_id")?,
            from: self.get("node")?,
            to: self.get("to")?,
            payload: self.value("message")?,
            send_tick: self.get("send_tick")?,
            deliver_tick: self.get("deliver_tick")?,
        })
    }
    pub fn check_shape(&self) -> Result<()> {
        let mut keys: Vec<&str> = match (self.what.as_str(), self.why.as_str()) {
            ("header", "configuration") => vec![
                "schema",
                "implementation",
                "scenario",
                "seed",
                "clock",
                "world",
                "nodes",
                "acceptors",
            ],
            ("advance", "tick") => vec![],
            ("spawn" | "crash", "node") => vec!["node"],
            ("recover", "node") => vec!["node", "model"],
            ("partition", "message") => vec!["from", "to", "until"],
            ("external", "client") | ("dispatch", "client") => vec!["input_id", "to", "message"],
            ("drop", "client") => vec!["input_id", "to", "message", "reason"],
            ("dispatch", "tick") => vec!["input_id", "to"],
            ("arrive" | "dispatch" | "drop", "message") => {
                let mut k = vec![
                    "message_id",
                    "parent_desire_id",
                    "from",
                    "to",
                    "message",
                    "send_tick",
                    "deliver_tick",
                ];
                if self.what == "dispatch" {
                    k.push("input_id");
                }
                if self.what == "drop" {
                    k.push("reason");
                }
                k
            }
            ("close", "receive") => vec!["node", "input_id", "count"],
            ("desire" | "attempt" | "result", "send" | "save" | "commit") => {
                let mut k = vec!["node", "parent_input_id", "desire_id"];
                match self.why.as_str() {
                    "send" => {
                        k.extend(["to", "message"]);
                        if self.what != "desire" {
                            k.push("message_id");
                        }
                    }
                    "save" => k.push("record"),
                    "commit" => k.extend(["slot", "entry"]),
                    _ => unreachable!(),
                }
                if self.what == "result" {
                    k.extend(["outcome", "reason"]);
                    if self.why == "send" {
                        k.extend(["delay", "send_tick", "deliver_tick"]);
                    }
                }
                k
            }
            ("deadlock", "no_progress") => vec!["ticks_since_progress"],
            ("finalize", "scenario") => vec!["reason", "account"],
            _ => {
                return Err(format!(
                    "unknown discriminator ({}, {})",
                    self.what, self.why
                ));
            }
        };
        keys.sort_unstable();
        let actual: Vec<&str> = self.fields.keys().map(String::as_str).collect();
        if actual != keys {
            return Err(format!("variant requires fields {keys:?}, got {actual:?}"));
        }
        Ok(())
    }
}
pub fn actor(node: NodeId) -> String {
    format!("node:{}", node.0)
}
pub fn envelope_fields(e: &Envelope) -> Value {
    json!({"message_id":e.message_id,"parent_desire_id":e.parent_desire_id,"from":e.from,"to":e.to,"message":e.payload,"send_tick":e.send_tick,"deliver_tick":e.deliver_tick})
}
pub fn input_fields(input: &Input) -> (&'static str, Value) {
    match input {
        Input::Tick { input_id, to } => ("tick", json!({"input_id":input_id,"to":to})),
        Input::Client {
            input_id,
            to,
            payload,
        } => (
            "client",
            json!({"input_id":input_id,"to":to,"message":payload}),
        ),
        Input::Message { input_id, envelope } => {
            let mut fields = envelope_fields(envelope);
            fields["input_id"] = json!(input_id);
            ("message", fields)
        }
    }
}
pub fn desire_fields(
    node: NodeId,
    input: InputId,
    id: DesireId,
    desire: &Desire,
) -> (&'static str, Value) {
    let mut fields = json!({"node":node,"parent_input_id":input,"desire_id":id});
    let why = match desire {
        Desire::Send { to, message } => {
            fields["to"] = json!(to);
            fields["message"] = message.clone();
            "send"
        }
        Desire::Save { record } => {
            fields["record"] = record.clone();
            "save"
        }
        Desire::Commit { slot, entry } => {
            fields["slot"] = json!(slot);
            fields["entry"] = entry.clone();
            "commit"
        }
    };
    (why, fields)
}

pub struct Recorder {
    output: Box<dyn Write>,
    rows: Vec<Event>,
    failed: bool,
}
impl Recorder {
    pub fn new(output: Box<dyn Write>) -> Self {
        Self {
            output,
            rows: Vec::new(),
            failed: false,
        }
    }
    pub fn rows(&self) -> &[Event] {
        &self.rows
    }
    pub fn append(&mut self, mut row: Event) -> Result<()> {
        if self.failed {
            return Err("recorder already failed; execution cannot continue".into());
        }
        row.seq = self.rows.len() as u64;
        row.check_shape()?;
        let mut bytes = serde_json::to_vec(&row).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        // A complete flushed row is the simulator commitment point, not fsync.
        if let Err(e) = self
            .output
            .write_all(&bytes)
            .and_then(|_| self.output.flush())
        {
            self.failed = true;
            return Err(format!("recording stopped before effect projection: {e}"));
        }
        self.rows.push(row);
        Ok(())
    }
}

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn encode(rows: &[Event]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut bytes, row).expect("event serialization");
        bytes.push(b'\n');
    }
    bytes
}
pub fn decode(bytes: &[u8]) -> Result<Vec<Event>> {
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err("incomplete final JSONL row (no newline)".into());
    }
    let mut rows = Vec::new();
    for (index, line) in bytes.split_inclusive(|b| *b == b'\n').enumerate() {
        let unique: Unique =
            serde_json::from_slice(line).map_err(|e| format!("line {}: {e}", index + 1))?;
        let row: Event =
            serde_json::from_value(unique.0).map_err(|e| format!("line {}: {e}", index + 1))?;
        rows.push(row);
    }
    Ok(rows)
}

// serde_json::Value otherwise silently accepts duplicate keys. Reject them at
// every depth before decoding the spine, including within algorithm Values.
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_none<E: de::Error>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = a.next_element::<Unique>()? {
                    out.push(v.0);
                }
                Ok(Unique(Value::Array(out)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut out = Map::new();
                while let Some(k) = a.next_key::<String>()? {
                    if out.contains_key(&k) {
                        return Err(de::Error::custom(format!("duplicate key {k}")));
                    }
                    out.insert(k, a.next_value::<Unique>()?.0);
                }
                Ok(Unique(Value::Object(out)))
            }
        }
        d.deserialize_any(V)
    }
}
