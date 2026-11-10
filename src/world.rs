use crate::projection::{Projection, agreement_refusal};
use crate::tape::{self, Event, Header, Recorder};
use crate::{
    Context, Desire, DesireId, Envelope, Input, InputId, MessageId, Node, NodeId, Result, Slot,
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
};

pub struct Simulator {
    header: Header,
    recorder: Recorder,
    nodes: BTreeMap<NodeId, Box<dyn Node>>,
    persistent: BTreeMap<NodeId, Vec<Value>>,
    chosen: BTreeMap<NodeId, BTreeMap<Slot, Value>>,
    pending: Vec<Envelope>,
    arrived: BTreeMap<MessageId, Envelope>,
    partitions: BTreeMap<(NodeId, NodeId), u64>,
    rng: StdRng,
    tick: u64,
    next_input: u64,
    next_desire: u64,
    next_message: u64,
    last_progress: u64,
    chosen_count: usize,
    turn_open: bool,
    stopped: bool,
    finalized: bool,
}
impl Simulator {
    pub fn new(header: Header, output: Box<dyn Write>) -> Result<Self> {
        let row = Event::new(
            0,
            "bench".into(),
            "header",
            "configuration",
            serde_json::to_value(&header).map_err(|e| e.to_string())?,
        );
        Projection::from_rows(std::slice::from_ref(&row))?;
        let mut recorder = Recorder::new(output);
        recorder.append(row)?;
        let rng = StdRng::seed_from_u64(header.seed);
        Ok(Self {
            header,
            recorder,
            nodes: BTreeMap::new(),
            persistent: BTreeMap::new(),
            chosen: BTreeMap::new(),
            pending: Vec::new(),
            arrived: BTreeMap::new(),
            partitions: BTreeMap::new(),
            rng,
            tick: 0,
            next_input: 0,
            next_desire: 0,
            next_message: 0,
            last_progress: 0,
            chosen_count: 0,
            turn_open: false,
            stopped: false,
            finalized: false,
        })
    }
    pub fn rows(&self) -> &[Event] {
        self.recorder.rows()
    }
    pub fn tick(&self) -> u64 {
        self.tick
    }
    pub fn persistent(&self) -> &BTreeMap<NodeId, Vec<Value>> {
        &self.persistent
    }
    pub fn chosen(&self) -> &BTreeMap<NodeId, BTreeMap<Slot, Value>> {
        &self.chosen
    }
    pub fn pending(&self) -> &[Envelope] {
        &self.pending
    }
    pub fn effective_partitions(&self) -> BTreeMap<(NodeId, NodeId), u64> {
        self.partitions
            .iter()
            .filter(|(_, until)| **until > self.tick)
            .map(|(k, v)| (*k, *v))
            .collect()
    }
    fn running(&self) -> Result<()> {
        if self.finalized || self.stopped || self.turn_open {
            Err("world is finalized, stopped, or has an unfinished turn".into())
        } else {
            Ok(())
        }
    }
    fn record(&mut self, who: String, what: &str, why: &str, fields: Value) -> Result<()> {
        self.recorder
            .append(Event::new(self.tick, who, what, why, fields))
    }
    pub fn spawn(&mut self, node: NodeId, inhabitant: Box<dyn Node>) -> Result<()> {
        self.running()?;
        if self.nodes.contains_key(&node)
            || !self.header.nodes.iter().any(|s| s.node == node)
            || inhabitant.crashed()
        {
            return Err(
                "spawn requires an unused configured identity and a live inhabitant".into(),
            );
        }
        self.record("bench".into(), "spawn", "node", json!({"node":node}))?;
        self.nodes.insert(node, inhabitant);
        self.persistent.insert(node, Vec::new());
        self.chosen.insert(node, BTreeMap::new());
        Ok(())
    }
    pub fn crash(&mut self, node: NodeId) -> Result<()> {
        self.running()?;
        if !self.nodes.contains_key(&node) {
            return Err("cannot crash absent node".into());
        }
        if self.nodes[&node].crashed() {
            return Err("cannot crash an already crashed node".into());
        }
        self.record("bench".into(), "crash", "node", json!({"node":node}))?;
        self.nodes.get_mut(&node).unwrap().set_crashed(true);
        Ok(())
    }
    /// Retains the old flag-flip model explicitly. This is not honest recovery.
    pub fn recover(&mut self, node: NodeId) -> Result<()> {
        self.running()?;
        if !self.nodes.contains_key(&node) {
            return Err("cannot resume absent node".into());
        }
        if !self.nodes[&node].crashed() {
            return Err("cannot resume a live node".into());
        }
        self.record(
            "bench".into(),
            "recover",
            "node",
            json!({"node":node,"model":"retained_state"}),
        )?;
        self.nodes.get_mut(&node).unwrap().set_crashed(false);
        Ok(())
    }
    pub fn partition(&mut self, from: NodeId, to: NodeId, duration: u64) -> Result<()> {
        self.running()?;
        if from == to {
            return Err("a node cannot be partitioned from itself".into());
        }
        let until = self
            .tick
            .checked_add(duration)
            .ok_or("partition time overflow")?;
        self.record(
            "bench".into(),
            "partition",
            "message",
            json!({"from":from,"to":to,"until":until}),
        )?;
        self.partitions.insert((from, to), until);
        Ok(())
    }
    fn input_id(&mut self) -> InputId {
        let id = InputId(self.next_input);
        self.next_input += 1;
        id
    }
    fn unavailable(&self, node: NodeId) -> Option<&'static str> {
        match self.nodes.get(&node) {
            None => Some("missing_recipient"),
            Some(n) if n.crashed() => Some("crashed_recipient"),
            _ => None,
        }
    }
    pub fn client_message<T: Serialize>(&mut self, to: NodeId, message: T) -> Result<()> {
        self.running()?;
        let payload = serde_json::to_value(message).map_err(|e| e.to_string())?;
        let input = Input::Client {
            input_id: self.input_id(),
            to,
            payload,
        };
        let (_, mut fields) = tape::input_fields(&input);
        self.record("bench".into(), "external", "client", fields.clone())?;
        if let Some(reason) = self.unavailable(to) {
            fields["reason"] = json!(reason);
            self.record("bench".into(), "drop", "client", fields)
        } else {
            self.deliver(input)
        }
    }
    pub fn step(&mut self) -> Result<bool> {
        self.running()?;
        let next = self.tick.checked_add(1).ok_or("clock overflow")?;
        self.recorder.append(Event::new(
            next,
            "bench".into(),
            "advance",
            "tick",
            json!({}),
        ))?;
        self.tick = next;
        // Freeze readiness at the start of the tick, preserving queue order.
        // A zero-latency send from a turn below waits until the following tick.
        let ready: Vec<MessageId> = self
            .pending
            .iter()
            .filter(|e| {
                e.deliver_tick <= self.tick
                    && self
                        .partitions
                        .get(&(e.from, e.to))
                        .is_none_or(|until| *until <= self.tick)
            })
            .map(|e| e.message_id)
            .collect();
        for id in ready {
            let index = self
                .pending
                .iter()
                .position(|e| e.message_id == id)
                .unwrap();
            let envelope = self.pending[index].clone();
            let mut fields = tape::envelope_fields(&envelope);
            if let Some(reason) = self.unavailable(envelope.to) {
                fields["reason"] = json!(reason);
                self.record("bench".into(), "drop", "message", fields)?;
                self.pending.remove(index);
            } else {
                self.record("bench".into(), "arrive", "message", fields)?;
                self.pending.remove(index);
                self.arrived.insert(id, envelope.clone());
                let input = Input::Message {
                    input_id: self.input_id(),
                    envelope: envelope.clone(),
                };
                self.deliver(input)?;
            }
            if self
                .partitions
                .get(&(envelope.from, envelope.to))
                .is_some_and(|until| *until <= self.tick)
            {
                self.partitions.remove(&(envelope.from, envelope.to));
            }
        }
        let interval = self.header.world.tick_interval;
        if interval > 0 && self.tick.is_multiple_of(interval) {
            let nodes: Vec<NodeId> = self.nodes.keys().copied().collect();
            for to in nodes {
                if self.unavailable(to).is_none() {
                    let input = Input::Tick {
                        input_id: self.input_id(),
                        to,
                    };
                    self.deliver(input)?;
                }
            }
        }
        let count = self
            .chosen
            .values()
            .flat_map(|log| log.keys().copied())
            .collect::<BTreeSet<_>>()
            .len();
        if count > self.chosen_count {
            self.chosen_count = count;
            self.last_progress = self.tick;
        } else if self.tick - self.last_progress > self.header.world.deadlock_threshold {
            self.record(
                "bench".into(),
                "deadlock",
                "no_progress",
                json!({"ticks_since_progress":self.tick-self.last_progress}),
            )?;
            return Ok(false);
        }
        Ok(true)
    }
    pub fn run(&mut self, until_tick: u64) -> Result<bool> {
        self.running()?;
        while self.tick < until_tick {
            if !self.step()? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn deliver(&mut self, input: Input) -> Result<()> {
        let node = input.to();
        let input_id = input.input_id();
        let (why, fields) = tape::input_fields(&input);
        self.record(tape::actor(node), "dispatch", why, fields)?;
        self.turn_open = true;
        if let Input::Message { envelope, .. } = &input {
            self.arrived.remove(&envelope.message_id);
        }
        let mut desires = Vec::new();
        // Context owns only this invocation's private answer. A panic here leaves
        // the committed dispatch and no close; even a caught panic cannot resume.
        self.nodes
            .get_mut(&node)
            .unwrap()
            .receive(&mut Context::new(node, &mut desires), &input);
        let result = self.answer_and_drain(node, input_id, desires);
        if result.is_ok() {
            self.turn_open = false;
        } else {
            self.stopped = true;
        }
        result
    }
    fn answer_and_drain(
        &mut self,
        node: NodeId,
        input: InputId,
        desires: Vec<Desire>,
    ) -> Result<()> {
        let mut answer = Vec::new();
        for desire in desires {
            let id = DesireId(self.next_desire);
            self.next_desire += 1;
            let (why, fields) = tape::desire_fields(node, input, id, &desire);
            self.record(tape::actor(node), "desire", why, fields)?;
            answer.push((id, desire));
        }
        self.record(
            tape::actor(node),
            "close",
            "receive",
            json!({"node":node,"input_id":input,"count":answer.len()}),
        )?;
        for (id, desire) in answer {
            let (why, mut fields) = tape::desire_fields(node, input, id, &desire);
            if matches!(desire, Desire::Send { .. }) {
                fields["message_id"] = json!(MessageId(self.next_message));
                self.next_message += 1;
            }
            self.record("bench".into(), "attempt", why, fields.clone())?;
            let refusal = if let Desire::Commit { slot, entry } = &desire {
                agreement_refusal(&self.chosen, node, *slot, entry)
            } else {
                None
            };
            fields["outcome"] = json!(if refusal.is_some() {
                "refused"
            } else {
                "fulfilled"
            });
            fields["reason"] = json!(refusal);
            let mut scheduled = None;
            if let Desire::Send { to, message } = &desire {
                let delay = self.header.world.base_latency
                    + self.rng.gen_range(0..=self.header.world.jitter);
                let deliver_tick = self
                    .tick
                    .checked_add(delay)
                    .ok_or("delivery time overflow")?;
                fields["delay"] = json!(delay);
                fields["send_tick"] = json!(self.tick);
                fields["deliver_tick"] = json!(deliver_tick);
                scheduled = Some(Envelope {
                    message_id: serde_json::from_value(fields["message_id"].clone()).unwrap(),
                    parent_desire_id: id,
                    from: node,
                    to: *to,
                    payload: message.clone(),
                    send_tick: self.tick,
                    deliver_tick,
                });
            }
            // A committed result is authoritative before its in-memory index.
            self.record("bench".into(), "result", why, fields)?;
            if let Some(reason) = refusal {
                return Err(format!("{reason}: node {} slot claim refused", node.0));
            }
            match desire {
                Desire::Send { .. } => self.pending.push(scheduled.unwrap()),
                Desire::Save { record } => self.persistent.get_mut(&node).unwrap().push(record),
                Desire::Commit { slot, entry } => {
                    self.chosen.get_mut(&node).unwrap().insert(slot, entry);
                }
            }
        }
        Ok(())
    }
    pub fn check_projection(&self) -> Result<Projection> {
        let p = Projection::from_rows(self.rows())?;
        if p.tick != self.tick
            || p.persistent != self.persistent
            || p.chosen != self.chosen
            || p.pending != self.pending
            || p.arrived != self.arrived
            || p.effective_partitions() != self.effective_partitions()
        {
            return Err("tape projections differ from live world state".into());
        }
        let nodes: BTreeMap<_, _> = self
            .nodes
            .iter()
            .map(|(id, node)| (*id, node.crashed()))
            .collect();
        if nodes != p.nodes {
            return Err("tape lifecycle projection differs from live nodes".into());
        }
        Ok(p)
    }
    pub fn finalize(&mut self, reason: &str) -> Result<()> {
        if self.finalized {
            return Err("scenario already finalized".into());
        }
        let p = self.check_projection()?;
        let mut row = Event::new(
            self.tick,
            "bench".into(),
            "finalize",
            "scenario",
            json!({"reason":reason,"account":p.account()}),
        );
        row.seq = self.rows().len() as u64;
        let mut checked = p;
        checked.apply(&row)?;
        self.recorder.append(row)?;
        self.finalized = true;
        Ok(())
    }
}
