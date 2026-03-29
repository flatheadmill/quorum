"""
Mencius consensus algorithm for Quorum framework

Leaderless log replication with round-robin slot ownership

Simplified sketch - captures core logic but may omit edge cases
"""

from quorum import Node


class MenciusNode(Node):
    def __init__(self, node_id, quorum_size):
        super().__init__(node_id, quorum_size)

        # Per-slot state (persistent)
        # slot -> {'promised': proposal, 'accepted': (proposal, value), 'decided': value}
        self.slots = {}

        # Proposer state
        self.proposal_number = node_id  # Unique per node
        self.next_my_slot = node_id  # Next slot I own

        # Per-slot proposal tracking
        # slot -> {'phase': 'prepare'|'accept'|'skip', 'promises': {}, 'accepts': set(), 'value': ...}
        self.proposals = {}

        # Learner/execution state
        self.decided = {}  # slot -> value (or SKIP marker)
        self.next_to_execute = 0

        # Client queue
        self.pending_values = []

    def receive(self, ctx, msg):
        payload = msg['payload']

        match payload.get('type'):

            case 'propose':
                # Client proposal - enqueue it
                value = payload['value']
                self.pending_values.append(value)

                # Try to propose on our next slot
                self._try_propose_next(ctx)

            case 'Prepare':
                # Phase 1a: Prepare for a slot
                slot = payload['slot']
                proposal = payload['proposal']

                if slot not in self.slots:
                    self.slots[slot] = {'promised': -1, 'accepted': None, 'decided': None}

                slot_state = self.slots[slot]

                if proposal > slot_state['promised']:
                    slot_state['promised'] = proposal
                    ctx.save({'action': 'promise', 'slot': slot, 'proposal': proposal})

                    accepted = slot_state['accepted']
                    ctx.send(msg['from'], {
                        'type': 'Promise',
                        'slot': slot,
                        'proposal': proposal,
                        'accepted_proposal': accepted[0] if accepted else -1,
                        'accepted_value': accepted[1] if accepted else None
                    })
                else:
                    ctx.send(msg['from'], {
                        'type': 'Promise',
                        'slot': slot,
                        'proposal': proposal,
                        'rejected': True,
                        'promised': slot_state['promised']
                    })

            case 'Promise':
                # Phase 1b response
                slot = payload['slot']
                proposal = payload['proposal']

                if slot not in self.proposals:
                    return

                prop = self.proposals[slot]
                if prop['phase'] != 'prepare' or proposal != prop['proposal']:
                    return

                if payload.get('rejected'):
                    # Give up this attempt
                    del self.proposals[slot]
                    return

                prop['promises'][msg['from']] = (
                    payload['accepted_proposal'],
                    payload['accepted_value']
                )

                majority = (self.quorum_size // 2) + 1
                if len(prop['promises']) >= majority:
                    # Got majority - move to accept phase

                    # Choose value (highest accepted, or ours, or SKIP)
                    highest_accepted = -1
                    value_to_propose = prop['value']

                    for acc_prop, acc_val in prop['promises'].values():
                        if acc_prop > highest_accepted:
                            highest_accepted = acc_prop
                            if acc_val is not None:
                                value_to_propose = acc_val

                    prop['phase'] = 'accept'
                    prop['value'] = value_to_propose
                    prop['accepts'] = set()

                    # Send Accept
                    for node in range(self.quorum_size):
                        ctx.send(node, {
                            'type': 'Accept',
                            'slot': slot,
                            'proposal': proposal,
                            'value': value_to_propose
                        })

            case 'Accept':
                # Phase 2a: Accept request
                slot = payload['slot']
                proposal = payload['proposal']
                value = payload['value']

                if slot not in self.slots:
                    self.slots[slot] = {'promised': -1, 'accepted': None, 'decided': None}

                slot_state = self.slots[slot]

                if proposal >= slot_state['promised']:
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
                    ctx.send(msg['from'], {
                        'type': 'Accepted',
                        'slot': slot,
                        'proposal': proposal,
                        'rejected': True
                    })

            case 'Accepted':
                # Phase 2b response
                slot = payload['slot']
                proposal = payload['proposal']

                if slot not in self.proposals:
                    return

                prop = self.proposals[slot]
                if prop['phase'] != 'accept' or proposal != prop['proposal']:
                    return

                if payload.get('rejected'):
                    del self.proposals[slot]
                    return

                prop['accepts'].add(msg['from'])

                majority = (self.quorum_size // 2) + 1
                if len(prop['accepts']) >= majority:
                    # Decided!
                    value = payload['value']
                    self.decided[slot] = value

                    if slot not in self.slots:
                        self.slots[slot] = {'promised': -1, 'accepted': None, 'decided': None}
                    self.slots[slot]['decided'] = value

                    del self.proposals[slot]

                    # Notify others
                    for node in range(self.quorum_size):
                        if node != self.node_id:
                            ctx.send(node, {
                                'type': 'Decided',
                                'slot': slot,
                                'value': value
                            })

                    # Try to execute in order
                    self._try_execute(ctx)

                    # If this was our slot, advance and try next
                    if slot % self.quorum_size == self.node_id:
                        self.next_my_slot = slot + self.quorum_size
                        self._try_propose_next(ctx)

            case 'Decided':
                # Learn about decided value
                slot = payload['slot']
                value = payload['value']

                if slot not in self.decided:
                    self.decided[slot] = value
                    if slot not in self.slots:
                        self.slots[slot] = {'promised': -1, 'accepted': None, 'decided': None}
                    self.slots[slot]['decided'] = value
                    self._try_execute(ctx)

            case 'SKIP':
                # Owner of slot is proposing SKIP
                slot = payload['slot']
                proposal = payload['proposal']

                # Treat SKIP as Accept with special value
                if slot not in self.slots:
                    self.slots[slot] = {'promised': -1, 'accepted': None, 'decided': None}

                slot_state = self.slots[slot]

                if proposal >= slot_state['promised']:
                    slot_state['accepted'] = (proposal, 'SKIP')
                    ctx.save({'action': 'skip', 'slot': slot, 'proposal': proposal})

                    ctx.send(msg['from'], {
                        'type': 'SkipAck',
                        'slot': slot,
                        'proposal': proposal
                    })

            case 'SkipAck':
                # SKIP acknowledgment
                slot = payload['slot']
                proposal = payload['proposal']

                if slot not in self.proposals:
                    return

                prop = self.proposals[slot]
                if prop['phase'] != 'skip' or proposal != prop['proposal']:
                    return

                prop['accepts'].add(msg['from'])

                majority = (self.quorum_size // 2) + 1
                if len(prop['accepts']) >= majority:
                    # SKIP decided
                    self.decided[slot] = 'SKIP'

                    if slot not in self.slots:
                        self.slots[slot] = {'promised': -1, 'accepted': None, 'decided': None}
                    self.slots[slot]['decided'] = 'SKIP'

                    del self.proposals[slot]

                    # Notify others
                    for node in range(self.quorum_size):
                        if node != self.node_id:
                            ctx.send(node, {
                                'type': 'Decided',
                                'slot': slot,
                                'value': 'SKIP'
                            })

                    self._try_execute(ctx)

                    # Advance to next slot
                    if slot % self.quorum_size == self.node_id:
                        self.next_my_slot = slot + self.quorum_size
                        self._try_propose_next(ctx)

            case 'tick':
                # Periodic: try to make progress
                self._try_propose_next(ctx)

    def _try_propose_next(self, ctx):
        """Try to propose on our next owned slot"""
        slot = self.next_my_slot

        # Check slot ownership
        if slot % self.quorum_size != self.node_id:
            return

        # Don't propose if already proposing for this slot
        if slot in self.proposals:
            return

        # Don't propose if already decided
        if slot in self.decided:
            return

        if self.pending_values:
            # Have value to propose - run Paxos
            value = self.pending_values.pop(0)
            self.proposal_number += self.quorum_size

            self.proposals[slot] = {
                'phase': 'prepare',
                'proposal': self.proposal_number,
                'value': value,
                'promises': {},
                'accepts': set()
            }

            # Send Prepare
            for node in range(self.quorum_size):
                ctx.send(node, {
                    'type': 'Prepare',
                    'slot': slot,
                    'proposal': self.proposal_number
                })
        else:
            # No value - propose SKIP
            self.proposal_number += self.quorum_size

            self.proposals[slot] = {
                'phase': 'skip',
                'proposal': self.proposal_number,
                'value': 'SKIP',
                'promises': {},
                'accepts': set()
            }

            # Broadcast SKIP
            for node in range(self.quorum_size):
                ctx.send(node, {
                    'type': 'SKIP',
                    'slot': slot,
                    'proposal': self.proposal_number
                })

    def _try_execute(self, ctx):
        """Execute decided slots in order"""
        while self.next_to_execute in self.decided:
            value = self.decided[self.next_to_execute]

            if value != 'SKIP':
                # Execute real value
                ctx.log(value)

            self.next_to_execute += 1


if __name__ == '__main__':
    from quorum import Simulator, Scenario

    scenario = Scenario('mencius-basic')
    for i in range(3):
        scenario.add_node(i)

    # Proposals to different nodes
    scenario.add_event(10, 'propose', node=0, value='hello')
    scenario.add_event(15, 'propose', node=1, value='world')
    scenario.add_event(20, 'propose', node=2, value='!')

    # Periodic ticks to drive progress
    for t in range(1, 100, 5):
        scenario.add_event(t, 'tick', node=0)
        scenario.add_event(t, 'tick', node=1)
        scenario.add_event(t, 'tick', node=2)

    sim = Simulator(seed=42)
    scenario.run(sim, MenciusNode)
    sim.run(max_ticks=200)

    valid, msg = sim.validate()
    print(f"Validation: {msg}")
    print(f"Atomic logs: {sim.atomic_logs}")
