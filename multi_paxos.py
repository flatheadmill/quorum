"""
Multi-Paxos consensus algorithm for Quorum framework

Log replication using multiple Paxos instances with leader optimization

Simplified sketch - captures core logic but may omit edge cases
"""

from quorum import Node


class MultiPaxosNode(Node):
    def __init__(self, node_id, quorum_size):
        super().__init__(node_id, quorum_size)

        # Per-slot acceptor state (persistent)
        # slot -> {'promised': proposal_num, 'accepted': (proposal, value)}
        self.slots = {}

        # Proposer state (volatile)
        self.proposal_number = node_id  # Unique per node
        self.is_leader = False
        self.leader_for_slots = set()  # Slots where we completed Phase 1
        self.next_slot = 0

        # Per-slot proposer tracking
        # slot -> {'phase': 'prepare'|'accept', 'promises': {...}, 'accepts': set(), 'value': ...}
        self.proposals = {}

        # Learner state
        self.committed = {}  # slot -> value
        self.next_to_commit = 0

    def receive(self, ctx, msg):
        payload = msg['payload']

        match payload.get('type'):

            case 'propose':
                # Client wants to propose a value
                value = payload['value']
                slot = self.next_slot
                self.next_slot += 1

                if self.is_leader and slot in self.leader_for_slots:
                    # We're leader for this slot - skip to Phase 2
                    self._send_accept(ctx, slot, value)
                else:
                    # Not leader - need to run full Paxos (Phase 1)
                    self._start_prepare(ctx, slot, value)

            case 'Prepare':
                # Phase 1a: Prepare request for a slot
                slot = payload['slot']
                proposal = payload['proposal']

                # Get or create slot state
                if slot not in self.slots:
                    self.slots[slot] = {'promised': -1, 'accepted': None}

                slot_state = self.slots[slot]

                if proposal > slot_state['promised']:
                    # Promise not to accept lower proposals for this slot
                    slot_state['promised'] = proposal
                    ctx.save({
                        'action': 'promise',
                        'slot': slot,
                        'proposal': proposal
                    })

                    accepted = slot_state.get('accepted')
                    ctx.send(msg['from'], {
                        'type': 'Promise',
                        'slot': slot,
                        'proposal': proposal,
                        'accepted_proposal': accepted[0] if accepted else -1,
                        'accepted_value': accepted[1] if accepted else None
                    })
                else:
                    # Reject
                    ctx.send(msg['from'], {
                        'type': 'Promise',
                        'slot': slot,
                        'proposal': proposal,
                        'rejected': True,
                        'promised': slot_state['promised']
                    })

            case 'Promise':
                # Phase 1b: Promise response
                slot = payload['slot']
                proposal = payload['proposal']

                if slot not in self.proposals:
                    return

                prop = self.proposals[slot]
                if prop['phase'] != 'prepare' or proposal != prop['proposal']:
                    return

                if payload.get('rejected'):
                    # Rejected - give up on this attempt
                    del self.proposals[slot]
                    self.is_leader = False
                    return

                # Record promise
                prop['promises'][msg['from']] = (
                    payload['accepted_proposal'],
                    payload['accepted_value']
                )

                majority = (self.quorum_size // 2) + 1
                if len(prop['promises']) >= majority:
                    # Got majority - we're now leader for this slot
                    self.is_leader = True
                    self.leader_for_slots.add(slot)

                    # Choose value (highest previously accepted, or ours)
                    highest_accepted = -1
                    value_to_propose = prop['value']

                    for acc_prop, acc_val in prop['promises'].values():
                        if acc_prop > highest_accepted:
                            highest_accepted = acc_prop
                            if acc_val is not None:
                                value_to_propose = acc_val

                    prop['value'] = value_to_propose
                    self._send_accept(ctx, slot, value_to_propose)

            case 'Accept':
                # Phase 2a: Accept request
                slot = payload['slot']
                proposal = payload['proposal']
                value = payload['value']

                # Get or create slot state
                if slot not in self.slots:
                    self.slots[slot] = {'promised': -1, 'accepted': None}

                slot_state = self.slots[slot]

                if proposal >= slot_state['promised']:
                    # Accept
                    slot_state['accepted'] = (proposal, value)
                    ctx.save({
                        'action': 'accepted',
                        'slot': slot,
                        'proposal': proposal,
                        'value': value
                    })

                    ctx.send(msg['from'], {
                        'type': 'Accepted',
                        'slot': slot,
                        'proposal': proposal,
                        'value': value
                    })
                else:
                    # Reject
                    ctx.send(msg['from'], {
                        'type': 'Accepted',
                        'slot': slot,
                        'proposal': proposal,
                        'rejected': True,
                        'promised': slot_state['promised']
                    })

            case 'Accepted':
                # Phase 2b: Accepted response
                slot = payload['slot']
                proposal = payload['proposal']

                if slot not in self.proposals:
                    return

                prop = self.proposals[slot]
                if prop['phase'] != 'accept' or proposal != prop['proposal']:
                    return

                if payload.get('rejected'):
                    # Rejected - lost leadership
                    del self.proposals[slot]
                    self.is_leader = False
                    if slot in self.leader_for_slots:
                        self.leader_for_slots.remove(slot)
                    return

                # Record acceptance
                prop['accepts'].add(msg['from'])

                majority = (self.quorum_size // 2) + 1
                if len(prop['accepts']) >= majority:
                    # Value chosen for this slot
                    value = payload['value']
                    self.committed[slot] = value

                    # Clean up proposal state
                    del self.proposals[slot]

                    # Apply to log in order
                    self._apply_committed(ctx)

                    # Notify other learners
                    for node in range(self.quorum_size):
                        if node != self.node_id:
                            ctx.send(node, {
                                'type': 'Chosen',
                                'slot': slot,
                                'value': value
                            })

            case 'Chosen':
                # Learn about chosen value
                slot = payload['slot']
                value = payload['value']

                if slot not in self.committed:
                    self.committed[slot] = value
                    self._apply_committed(ctx)

    def _start_prepare(self, ctx, slot, value):
        """Start Phase 1 for a slot"""
        self.proposal_number += self.quorum_size

        self.proposals[slot] = {
            'phase': 'prepare',
            'proposal': self.proposal_number,
            'value': value,
            'promises': {},
            'accepts': set()
        }

        ctx.save({
            'action': 'prepare',
            'slot': slot,
            'proposal': self.proposal_number,
            'value': value
        })

        # Send Prepare to all
        for node in range(self.quorum_size):
            ctx.send(node, {
                'type': 'Prepare',
                'slot': slot,
                'proposal': self.proposal_number
            })

    def _send_accept(self, ctx, slot, value):
        """Send Phase 2 Accept (leader optimization)"""
        if slot not in self.proposals:
            self.proposals[slot] = {
                'phase': 'accept',
                'proposal': self.proposal_number,
                'value': value,
                'promises': {},
                'accepts': set()
            }
        else:
            self.proposals[slot]['phase'] = 'accept'
            self.proposals[slot]['accepts'] = set()

        # Send Accept to all
        for node in range(self.quorum_size):
            ctx.send(node, {
                'type': 'Accept',
                'slot': slot,
                'proposal': self.proposal_number,
                'value': value
            })

    def _apply_committed(self, ctx):
        """Apply committed entries to log in order"""
        while self.next_to_commit in self.committed:
            value = self.committed[self.next_to_commit]
            ctx.log(value)
            self.next_to_commit += 1


if __name__ == '__main__':
    from quorum import Simulator, Scenario

    scenario = Scenario('multi-paxos-basic')
    for i in range(3):
        scenario.add_node(i)

    # Multiple proposals
    scenario.add_event(10, 'propose', node=0, value='hello')
    scenario.add_event(20, 'propose', node=0, value='world')
    scenario.add_event(30, 'propose', node=0, value='!')

    sim = Simulator(seed=42)
    scenario.run(sim, MultiPaxosNode)
    sim.run(max_ticks=200)

    valid, msg = sim.validate()
    print(f"Validation: {msg}")
    print(f"Atomic logs: {sim.atomic_logs}")
