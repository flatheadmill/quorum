//! Fixed-suffix Multi-Paxos: one campaign, FIFO proposals, and sparse learning.
//!
//! The node requests persistence before replies; the world's serial effect drain
//! fulfills those saves. Local state is not a recovery image. A Nack does not
//! retry or abandon a campaign, and learning is a value claim, not application.

use crate::{Context, Input, Node, NodeId, Result, Slot};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Ballot(pub u64, pub NodeId);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialState {
    Empty,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub acceptors: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    pub proposers: Vec<NodeId>,
    #[serde(deserialize_with = "required")]
    pub active_proposer: Option<NodeId>,
    pub from_slot: Slot,
    pub initial_round: u64,
    pub initial_state: InitialState,
}

// A present null is a value. The custom field deserializer prevents Serde's
// missing-field treatment of Value and Option from silently supplying null.
fn required<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound(deserialize = "V: Deserialize<'de>"))]
struct AcceptedValue<V = Value> {
    slot: Slot,
    ballot: Ballot,
    #[serde(deserialize_with = "required")]
    value: V,
}

// Incoming messages own their JSON values. Outgoing messages borrow the values
// already held by a role; broadcasts serialize the same message for each peer.
#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    deny_unknown_fields,
    bound(deserialize = "V: Deserialize<'de>")
)]
enum Message<V = Value> {
    Propose {
        #[serde(deserialize_with = "required")]
        value: V,
    },
    Prepare {
        ballot: Ballot,
        from_slot: Slot,
    },
    Promise {
        ballot: Ballot,
        from_slot: Slot,
        accepted: Vec<AcceptedValue<V>>,
    },
    Accept {
        ballot: Ballot,
        slot: Slot,
        #[serde(deserialize_with = "required")]
        value: V,
    },
    Accepted {
        ballot: Ballot,
        slot: Slot,
        #[serde(deserialize_with = "required")]
        value: V,
    },
    Nack {
        rejected_ballot: Ballot,
        promised_ballot: Ballot,
    },
    Chosen {
        ballot: Ballot,
        slot: Slot,
        #[serde(deserialize_with = "required")]
        value: V,
    },
}

#[derive(Serialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Record<'a> {
    Promised {
        from_slot: Slot,
        ballot: Ballot,
    },
    Accepted {
        slot: Slot,
        ballot: Ballot,
        value: &'a Value,
    },
}

pub fn propose(value: Value) -> Value {
    serde_json::to_value(Message::Propose { value }).expect("serialize Multi-Paxos proposal")
}

struct Campaign {
    ballot: Ballot,
    promises: BTreeSet<NodeId>,
    reports: BTreeMap<(Slot, Ballot), Value>,
    adopted: BTreeMap<Slot, AcceptedValue>,
    ready: bool,
}

struct Proposal {
    value: Value,
    replies: BTreeSet<NodeId>,
    learned: bool,
}

pub struct MultiPaxosNode {
    node_id: NodeId,
    config: Config,
    quorum: usize,
    crashed: bool,

    // Acceptor state: one watermark for the configured, unchanging suffix.
    promised: Option<Ballot>,
    accepted: BTreeMap<Slot, AcceptedValue>,

    // Proposer state: the first quorum freezes adoption for this sole campaign.
    campaign: Option<Campaign>,
    waiting: VecDeque<Value>,
    next_slot: Slot,
    proposals: BTreeMap<Slot, Proposal>,

    // Both learning paths share this guard. Ballot is not part of a world claim.
    learned: BTreeMap<Slot, Value>,
}

fn members(ids: &mut [NodeId]) -> Result<()> {
    ids.sort_unstable();
    if ids.is_empty() || ids[0].0 < 0 || ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("Multi-Paxos membership must be nonempty, distinct and nonnegative".into());
    }
    Ok(())
}

fn contains(ids: &[NodeId], node: NodeId) -> bool {
    ids.binary_search(&node).is_ok()
}

fn broadcast<T: Serialize>(ctx: &mut Context, recipients: &[NodeId], message: &T) {
    for &to in recipients {
        ctx.send(to, message);
    }
}

fn learn(ctx: &mut Context, learned: &mut BTreeMap<Slot, Value>, slot: Slot, value: Value) {
    if let Some(previous) = learned.get(&slot) {
        assert_eq!(
            previous, &value,
            "contradictory learned values for one slot"
        );
        return;
    }
    ctx.commit(slot, &value);
    learned.insert(slot, value);
}

impl MultiPaxosNode {
    pub fn new(node_id: NodeId, mut config: Config) -> Result<Self> {
        members(&mut config.acceptors)?;
        members(&mut config.learners)?;
        members(&mut config.proposers)?;
        if config
            .active_proposer
            .is_some_and(|id| !contains(&config.proposers, id))
            || !(contains(&config.acceptors, node_id)
                || contains(&config.learners, node_id)
                || contains(&config.proposers, node_id))
        {
            return Err("Multi-Paxos node or active proposer is not a configured member".into());
        }
        Ok(Self {
            node_id,
            quorum: config.acceptors.len() / 2 + 1,
            next_slot: config.from_slot,
            config,
            crashed: false,
            promised: None,
            accepted: BTreeMap::new(),
            campaign: None,
            waiting: VecDeque::new(),
            proposals: BTreeMap::new(),
            learned: BTreeMap::new(),
        })
    }

    fn owns_ballot(&self, sender: NodeId, ballot: Ballot) -> bool {
        sender == ballot.1 && contains(&self.config.proposers, sender)
    }

    fn start_campaign(&mut self, ctx: &mut Context) {
        if self.config.active_proposer != Some(self.node_id) || self.campaign.is_some() {
            return;
        }
        let ballot = Ballot(self.config.initial_round, self.node_id);
        self.campaign = Some(Campaign {
            ballot,
            promises: BTreeSet::new(),
            reports: BTreeMap::new(),
            adopted: BTreeMap::new(),
            ready: false,
        });
        broadcast(
            ctx,
            &self.config.acceptors,
            &Message::<&Value>::Prepare {
                ballot,
                from_slot: self.config.from_slot,
            },
        );
    }

    fn stale(&self, ctx: &mut Context, sender: NodeId, ballot: Ballot) -> bool {
        if let Some(promised) = self.promised
            && ballot < promised
        {
            ctx.send(
                sender,
                Message::<&Value>::Nack {
                    rejected_ballot: ballot,
                    promised_ballot: promised,
                },
            );
            return true;
        }
        false
    }

    fn promise(&mut self, ctx: &mut Context, ballot: Ballot) {
        if self.promised.is_none_or(|previous| ballot > previous) {
            self.promised = Some(ballot);
            ctx.save(Record::Promised {
                from_slot: self.config.from_slot,
                ballot,
            });
        }
    }

    fn prepare(&mut self, ctx: &mut Context, sender: NodeId, ballot: Ballot, from_slot: Slot) {
        if !contains(&self.config.acceptors, self.node_id)
            || !self.owns_ballot(sender, ballot)
            || from_slot != self.config.from_slot
            || self.stale(ctx, sender, ballot)
        {
            return;
        }
        self.promise(ctx, ballot);
        let accepted = self
            .accepted
            .range(from_slot..)
            .map(|(&slot, entry)| AcceptedValue {
                slot,
                ballot: entry.ballot,
                value: &entry.value,
            })
            .collect();
        ctx.send(
            sender,
            Message::Promise {
                ballot,
                from_slot,
                accepted,
            },
        );
    }

    fn accept(
        &mut self,
        ctx: &mut Context,
        sender: NodeId,
        ballot: Ballot,
        slot: Slot,
        value: Value,
    ) {
        if !contains(&self.config.acceptors, self.node_id)
            || !self.owns_ballot(sender, ballot)
            || slot < self.config.from_slot
            || self.stale(ctx, sender, ballot)
        {
            return;
        }
        if let Some(previous) = self.accepted.get(&slot)
            && previous.ballot == ballot
        {
            assert_eq!(
                previous.value, value,
                "conflicting Accept at one slot and ballot"
            );
            ctx.send(
                sender,
                Message::Accepted {
                    ballot,
                    slot,
                    value: &previous.value,
                },
            );
            return;
        }
        self.promise(ctx, ballot);
        ctx.save(Record::Accepted {
            slot,
            ballot,
            value: &value,
        });
        ctx.send(
            sender,
            Message::Accepted {
                ballot,
                slot,
                value: &value,
            },
        );
        self.accepted.insert(
            slot,
            AcceptedValue {
                slot,
                ballot,
                value,
            },
        );
    }

    fn receive_promise(
        &mut self,
        ctx: &mut Context,
        sender: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: Vec<AcceptedValue>,
    ) {
        let Some(campaign) = self.campaign.as_mut() else {
            return;
        };
        if campaign.ready
            || campaign.ballot != ballot
            || from_slot != self.config.from_slot
            || !contains(&self.config.acceptors, sender)
            || campaign.promises.contains(&sender)
        {
            return;
        }
        // A malformed report contributes no quorum weight. Do not normalize a
        // duplicate or unsorted suffix into a different, apparently valid one.
        let mut previous = None;
        for entry in &accepted {
            if entry.slot < from_slot
                || previous.is_some_and(|slot| entry.slot <= slot)
                || entry.ballot > ballot
                || !contains(&self.config.proposers, entry.ballot.1)
            {
                return;
            }
            previous = Some(entry.slot);
        }
        for entry in accepted {
            let key = (entry.slot, entry.ballot);
            if let Some(prior) = campaign.reports.get(&key) {
                assert_eq!(
                    entry.value, *prior,
                    "conflicting same-ballot Promise reports"
                );
            } else {
                campaign.reports.insert(key, entry.value);
            }
        }
        campaign.promises.insert(sender);
        if campaign.promises.len() == self.quorum {
            // Keep every reported ballot until the quorum is complete: even a
            // lower-ballot contradiction must not hide behind a higher value.
            // Ordered traversal leaves the highest accepted ballot per slot.
            for ((slot, ballot), value) in std::mem::take(&mut campaign.reports) {
                campaign.adopted.insert(
                    slot,
                    AcceptedValue {
                        slot,
                        ballot,
                        value,
                    },
                );
            }
            campaign.ready = true;
            self.propose_waiting(ctx);
        }
    }

    fn propose_waiting(&mut self, ctx: &mut Context) {
        let Some(campaign) = self.campaign.as_mut() else {
            return;
        };
        if !campaign.ready {
            return;
        }
        while !self.waiting.is_empty() {
            let slot = self.next_slot;
            let value = match campaign.adopted.remove(&slot) {
                Some(adopted) => adopted.value,
                None => self.waiting.pop_front().unwrap(),
            };
            self.next_slot = Slot(slot.0.checked_add(1).expect("Multi-Paxos slot overflow"));
            broadcast(
                ctx,
                &self.config.acceptors,
                &Message::Accept {
                    ballot: campaign.ballot,
                    slot,
                    value: &value,
                },
            );
            assert!(
                self.proposals
                    .insert(
                        slot,
                        Proposal {
                            value,
                            replies: BTreeSet::new(),
                            learned: false,
                        },
                    )
                    .is_none(),
                "proposal for a slot is immutable"
            );
        }
    }

    fn receive_accepted(
        &mut self,
        ctx: &mut Context,
        sender: NodeId,
        ballot: Ballot,
        slot: Slot,
        value: Value,
    ) {
        if !contains(&self.config.acceptors, sender)
            || self.campaign.as_ref().is_none_or(|c| c.ballot != ballot)
        {
            return;
        }
        let Some(proposal) = self.proposals.get_mut(&slot) else {
            return;
        };
        if proposal.value != value || proposal.learned || !proposal.replies.insert(sender) {
            return;
        }
        if proposal.replies.len() == self.quorum {
            proposal.learned = true;
            learn(ctx, &mut self.learned, slot, proposal.value.clone());
            broadcast(
                ctx,
                &self.config.learners,
                &Message::Chosen {
                    ballot,
                    slot,
                    value: &proposal.value,
                },
            );
        }
    }
}

impl Node for MultiPaxosNode {
    fn receive(&mut self, ctx: &mut Context, input: &Input) {
        match input {
            Input::Tick { .. } => self.start_campaign(ctx),
            Input::Client { payload, .. } => {
                let message = Message::<Value>::deserialize(payload)
                    .expect("strict parse Multi-Paxos client input");
                let Message::Propose { value } = message else {
                    panic!("Multi-Paxos client input must be Propose");
                };
                self.waiting.push_back(value);
                self.propose_waiting(ctx);
            }
            Input::Message { envelope, .. } => {
                let message = Message::<Value>::deserialize(&envelope.payload)
                    .expect("strict parse Multi-Paxos peer message");
                let sender = envelope.from;
                match message {
                    Message::Prepare { ballot, from_slot } => {
                        self.prepare(ctx, sender, ballot, from_slot)
                    }
                    Message::Promise {
                        ballot,
                        from_slot,
                        accepted,
                    } => self.receive_promise(ctx, sender, ballot, from_slot, accepted),
                    Message::Accept {
                        ballot,
                        slot,
                        value,
                    } => self.accept(ctx, sender, ballot, slot, value),
                    Message::Accepted {
                        ballot,
                        slot,
                        value,
                    } => self.receive_accepted(ctx, sender, ballot, slot, value),
                    Message::Nack { .. } => {
                        // No retry, preemption, or abandonment in Calm. Other
                        // acceptors' positive replies may still form a quorum.
                    }
                    Message::Chosen {
                        ballot,
                        slot,
                        value,
                    } => {
                        if contains(&self.config.learners, self.node_id)
                            && self.owns_ballot(sender, ballot)
                            && slot >= self.config.from_slot
                        {
                            learn(ctx, &mut self.learned, slot, value);
                        }
                    }
                    Message::Propose { .. } => {
                        panic!("Propose is a client input, not a peer message")
                    }
                }
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
