"""
Basic Paxos consensus algorithm for Quorum framework

Single-value consensus. For log replication, see multi_paxos.py

Simplified sketch - captures core logic but may omit edge cases
"""

from quorum import Node


class PaxosNode(Node):
    def __init__(self, node_id, quorum_size):
        super().__init__(node_id, quorum_size)

        # Acceptor state (persistent)
        self.promised_proposal = -1  # Highest proposal number promised
        self.accepted_proposal = -1  # Proposal number of accepted value
        self.accepted_value = None   # Accepted value (if any)

        # Proposer state (volatile)
        self.proposal_number = node_id  # Start with node_id for uniqueness
        self.proposing_value = None
        self.phase = None  # 'prepare' or 'accept' or None
        self.promises = {}  # {node_id: (accepted_proposal, accepted_value)}
        self.accepts = set()  # Set of nodes that accepted

        # Learner state
        self.chosen_value = None

    def receive(self, ctx, msg):
        payload = msg['payload']

        match payload.get('type'):

            case 'propose':
                # Client wants to propose a value
                if self.chosen_value is not None:
                    # Already decided
                    return

                value = payload['value']

                # Generate unique proposal number (simple: increment by quorum_size)
                self.proposal_number += self.quorum_size
                self.proposing_value = value
                self.phase = 'prepare'
                self.promises = {}
                self.accepts = set()

                ctx.save({'action': 'prepare', 'proposal': self.proposal_number, 'value': value})

                # Phase 1a: Send Prepare to all acceptors
                for node in range(self.quorum_size):
                    ctx.send(node, {
                        'type': 'Prepare',
                        'proposal': self.proposal_number
                    })

            case 'Prepare':
                # Phase 1a received - respond with promise
                proposal = payload['proposal']

                if proposal > self.promised_proposal:
                    # Promise not to accept lower proposals
                    self.promised_proposal = proposal
                    ctx.save({'action': 'promise', 'proposal': proposal})

                    ctx.send(msg['from'], {
                        'type': 'Promise',
                        'proposal': proposal,
                        'accepted_proposal': self.accepted_proposal,
                        'accepted_value': self.accepted_value
                    })
                else:
                    # Reject - already promised to higher
                    ctx.send(msg['from'], {
                        'type': 'Promise',
                        'proposal': proposal,
                        'rejected': True,
                        'promised': self.promised_proposal
                    })

            case 'Promise':
                # Phase 1b received
                if self.phase != 'prepare':
                    return

                proposal = payload['proposal']
                if proposal != self.proposal_number:
                    return

                if payload.get('rejected'):
                    # Our proposal was rejected - someone has higher number
                    # Give up and retry later with higher number
                    self.phase = None
                    return

                # Record promise
                self.promises[msg['from']] = (
                    payload['accepted_proposal'],
                    payload['accepted_value']
                )

                majority = (self.quorum_size // 2) + 1
                if len(self.promises) >= majority:
                    # Got majority promises - move to Phase 2

                    # Must use highest-numbered previously accepted value if any
                    highest_accepted = -1
                    value_to_propose = self.proposing_value

                    for accepted_proposal, accepted_value in self.promises.values():
                        if accepted_proposal > highest_accepted:
                            highest_accepted = accepted_proposal
                            if accepted_value is not None:
                                value_to_propose = accepted_value

                    self.proposing_value = value_to_propose
                    self.phase = 'accept'
                    self.accepts = set()

                    # Phase 2a: Send Accept to all acceptors
                    for node in range(self.quorum_size):
                        ctx.send(node, {
                            'type': 'Accept',
                            'proposal': self.proposal_number,
                            'value': value_to_propose
                        })

            case 'Accept':
                # Phase 2a received - accept if not promised to higher
                proposal = payload['proposal']
                value = payload['value']

                if proposal >= self.promised_proposal:
                    # Accept this proposal
                    self.accepted_proposal = proposal
                    self.accepted_value = value
                    ctx.save({
                        'action': 'accepted',
                        'proposal': proposal,
                        'value': value
                    })

                    ctx.send(msg['from'], {
                        'type': 'Accepted',
                        'proposal': proposal,
                        'value': value
                    })
                else:
                    # Reject - promised to higher
                    ctx.send(msg['from'], {
                        'type': 'Accepted',
                        'proposal': proposal,
                        'rejected': True,
                        'promised': self.promised_proposal
                    })

            case 'Accepted':
                # Phase 2b received
                if self.phase != 'accept':
                    return

                proposal = payload['proposal']
                if proposal != self.proposal_number:
                    return

                if payload.get('rejected'):
                    # Rejected - give up
                    self.phase = None
                    return

                # Record acceptance
                self.accepts.add(msg['from'])

                majority = (self.quorum_size // 2) + 1
                if len(self.accepts) >= majority:
                    # Value is chosen!
                    self.chosen_value = self.proposing_value
                    self.phase = None

                    # Apply to atomic log
                    ctx.log(self.chosen_value)

                    # Broadcast decision (optional - learners can discover independently)
                    for node in range(self.quorum_size):
                        if node != self.node_id:
                            ctx.send(node, {
                                'type': 'Chosen',
                                'value': self.chosen_value
                            })

            case 'Chosen':
                # Learn about chosen value
                if self.chosen_value is None:
                    self.chosen_value = payload['value']
                    ctx.log(self.chosen_value)


if __name__ == '__main__':
    from quorum import Simulator, Scenario

    scenario = Scenario('paxos-basic')
    for i in range(3):
        scenario.add_node(i)

    # Single proposal
    scenario.add_event(10, 'propose', node=0, value='hello')

    # Dueling proposers (both try to propose)
    # scenario.add_event(10, 'propose', node=0, value='hello')
    # scenario.add_event(11, 'propose', node=1, value='world')

    sim = Simulator(seed=42)
    scenario.run(sim, PaxosNode)
    sim.run(max_ticks=200)

    valid, msg = sim.validate()
    print(f"Validation: {msg}")
    print(f"Atomic logs: {sim.atomic_logs}")
    print(f"Chosen values: {[n.chosen_value for n in sim.nodes.values()]}')
