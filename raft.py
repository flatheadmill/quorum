"""
Raft consensus algorithm implementation for Quorum framework

Simplified sketch - captures core logic but may omit edge cases
"""

from quorum import Node


class RaftNode(Node):
    def __init__(self, node_id, quorum_size):
        super().__init__(node_id, quorum_size)

        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = []  # [(term, entry), ...]

        # Volatile state
        self.commit_index = -1
        self.last_applied = -1
        self.role = 'follower'

        # Leader state
        self.next_index = {}
        self.match_index = {}

        # Election
        self.votes_received = set()
        self.election_timeout = 15 + (node_id * 3)
        self.ticks_since_leader = 0

    def receive(self, ctx, msg):
        payload = msg['payload']

        match payload.get('type'):

            case 'propose':
                # Client proposal - leader only
                if self.role != 'leader':
                    return

                value = payload['value']
                self.log.append((self.current_term, value))
                ctx.save({'action': 'append', 'term': self.current_term, 'entry': value})

                # Update leader's own match_index
                self.match_index[self.node_id] = len(self.log) - 1

                # Replicate to all followers
                for node in range(self.quorum_size):
                    if node == self.node_id:
                        continue

                    next_idx = self.next_index.get(node, 0)
                    entries = self.log[next_idx:] if next_idx < len(self.log) else []

                    ctx.send(node, {
                        'type': 'AppendEntries',
                        'term': self.current_term,
                        'leader_id': self.node_id,
                        'prev_log_index': next_idx - 1,
                        'prev_log_term': self.log[next_idx - 1][0] if next_idx > 0 else -1,
                        'entries': entries,
                        'leader_commit': self.commit_index
                    })

            case 'RequestVote':
                term = payload['term']
                candidate = payload['candidate_id']

                # Discover higher term
                if term > self.current_term:
                    self.current_term = term
                    self.role = 'follower'
                    self.voted_for = None
                    self.ticks_since_leader = 0
                    ctx.save({'action': 'step_down', 'term': term})

                vote_granted = False

                if term >= self.current_term:
                    if self.voted_for is None or self.voted_for == candidate:
                        # Check candidate's log is up-to-date
                        my_last_index = len(self.log) - 1
                        my_last_term = self.log[-1][0] if self.log else -1

                        cand_index = payload['last_log_index']
                        cand_term = payload['last_log_term']

                        if cand_term > my_last_term or \
                           (cand_term == my_last_term and cand_index >= my_last_index):
                            vote_granted = True
                            self.voted_for = candidate
                            ctx.save({'action': 'vote', 'term': term, 'for': candidate})

                ctx.send(msg['from'], {
                    'type': 'RequestVoteResponse',
                    'term': self.current_term,
                    'vote_granted': vote_granted
                })

            case 'RequestVoteResponse':
                if self.role != 'candidate':
                    return

                if payload['term'] > self.current_term:
                    # Step down
                    self.current_term = payload['term']
                    self.role = 'follower'
                    self.voted_for = None
                    self.ticks_since_leader = 0
                    ctx.save({'action': 'step_down', 'term': payload['term']})
                    return

                if payload['vote_granted']:
                    self.votes_received.add(msg['from'])

                    majority = (self.quorum_size // 2) + 1
                    if len(self.votes_received) >= majority:
                        # Become leader
                        self.role = 'leader'
                        self.next_index = {i: len(self.log) for i in range(self.quorum_size)}
                        self.match_index = {i: -1 for i in range(self.quorum_size)}
                        self.match_index[self.node_id] = len(self.log) - 1

                        # Send immediate heartbeats
                        for node in range(self.quorum_size):
                            if node == self.node_id:
                                continue

                            next_idx = self.next_index[node]
                            entries = self.log[next_idx:] if next_idx < len(self.log) else []

                            ctx.send(node, {
                                'type': 'AppendEntries',
                                'term': self.current_term,
                                'leader_id': self.node_id,
                                'prev_log_index': next_idx - 1,
                                'prev_log_term': self.log[next_idx - 1][0] if next_idx > 0 else -1,
                                'entries': entries,
                                'leader_commit': self.commit_index
                            })

            case 'AppendEntries':
                term = payload['term']
                prev_log_index = payload['prev_log_index']
                prev_log_term = payload['prev_log_term']
                entries = payload.get('entries', [])
                leader_commit = payload['leader_commit']

                # Reset election timeout on valid AppendEntries
                self.ticks_since_leader = 0

                # Discover higher term
                if term > self.current_term:
                    self.current_term = term
                    self.role = 'follower'
                    self.voted_for = None
                    ctx.save({'action': 'step_down', 'term': term})

                success = False

                if term >= self.current_term:
                    self.role = 'follower'

                    # Check prev_log consistency (Raft paper Figure 2)
                    log_ok = False
                    if prev_log_index == -1:
                        # No previous entry required
                        log_ok = True
                    elif prev_log_index < len(self.log):
                        # Check term matches at prev_log_index
                        if self.log[prev_log_index][0] == prev_log_term:
                            log_ok = True

                    if log_ok:
                        # Delete conflicting entries and append new ones
                        for i, entry in enumerate(entries):
                            log_index = prev_log_index + 1 + i
                            if log_index < len(self.log):
                                if self.log[log_index][0] != entry[0]:
                                    # Conflict - truncate log from here
                                    self.log = self.log[:log_index]
                                    self.log.append(entry)
                                    ctx.save({'action': 'append', 'term': entry[0], 'entry': entry[1]})
                                # else: already have this entry, skip
                            else:
                                # Append new entry
                                self.log.append(entry)
                                ctx.save({'action': 'append', 'term': entry[0], 'entry': entry[1]})

                        # Update commit index
                        if leader_commit > self.commit_index:
                            self.commit_index = min(leader_commit, len(self.log) - 1)

                            # Apply committed entries
                            while self.last_applied < self.commit_index:
                                self.last_applied += 1
                                log_term, log_entry = self.log[self.last_applied]
                                ctx.log(log_entry)

                        success = True

                ctx.send(msg['from'], {
                    'type': 'AppendEntriesResponse',
                    'term': self.current_term,
                    'success': success,
                    'match_index': len(self.log) - 1 if success else -1
                })

            case 'AppendEntriesResponse':
                if self.role != 'leader':
                    return

                if payload['term'] > self.current_term:
                    # Step down
                    self.current_term = payload['term']
                    self.role = 'follower'
                    self.voted_for = None
                    self.ticks_since_leader = 0
                    ctx.save({'action': 'step_down', 'term': payload['term']})
                    return

                follower = msg['from']

                if not payload['success']:
                    # Decrement next_index and retry
                    if self.next_index.get(follower, 0) > 0:
                        self.next_index[follower] = self.next_index.get(follower, 0) - 1

                        # Send retry immediately
                        next_idx = self.next_index[follower]
                        entries = self.log[next_idx:] if next_idx < len(self.log) else []

                        ctx.send(follower, {
                            'type': 'AppendEntries',
                            'term': self.current_term,
                            'leader_id': self.node_id,
                            'prev_log_index': next_idx - 1,
                            'prev_log_term': self.log[next_idx - 1][0] if next_idx > 0 else -1,
                            'entries': entries,
                            'leader_commit': self.commit_index
                        })
                    return

                # Success - update match_index and next_index
                self.match_index[follower] = payload['match_index']
                self.next_index[follower] = payload['match_index'] + 1

                # Try to advance commit index
                for n in range(len(self.log) - 1, self.commit_index, -1):
                        # Only commit entries from current term
                        if self.log[n][0] == self.current_term:
                            # Count replicas
                            count = sum(1 for idx in self.match_index.values() if idx >= n)

                            majority = (self.quorum_size // 2) + 1
                            if count >= majority:
                                self.commit_index = n

                                # Apply committed entries
                                while self.last_applied < self.commit_index:
                                    self.last_applied += 1
                                    term, entry = self.log[self.last_applied]
                                    ctx.log(entry)

                                break

            case 'tick':
                self.ticks_since_leader += 1

                if self.role == 'follower' or self.role == 'candidate':
                    # Election timeout
                    if self.ticks_since_leader >= self.election_timeout:
                        # Start election
                        self.current_term += 1
                        self.role = 'candidate'
                        self.voted_for = self.node_id
                        self.votes_received = {self.node_id}
                        self.ticks_since_leader = 0

                        ctx.save({'action': 'election', 'term': self.current_term})

                        # RequestVote to all
                        last_log_index = len(self.log) - 1
                        last_log_term = self.log[-1][0] if self.log else -1

                        for node in range(self.quorum_size):
                            if node != self.node_id:
                                ctx.send(node, {
                                    'type': 'RequestVote',
                                    'term': self.current_term,
                                    'candidate_id': self.node_id,
                                    'last_log_index': last_log_index,
                                    'last_log_term': last_log_term
                                })

                elif self.role == 'leader':
                    # Periodic heartbeats
                    if self.ticks_since_leader % 5 == 0:
                        for node in range(self.quorum_size):
                            if node == self.node_id:
                                continue

                            next_idx = self.next_index.get(node, 0)
                            entries = self.log[next_idx:] if next_idx < len(self.log) else []

                            ctx.send(node, {
                                'type': 'AppendEntries',
                                'term': self.current_term,
                                'leader_id': self.node_id,
                                'prev_log_index': next_idx - 1,
                                'prev_log_term': self.log[next_idx - 1][0] if next_idx > 0 else -1,
                                'entries': entries,
                                'leader_commit': self.commit_index
                            })


if __name__ == '__main__':
    from quorum import Simulator, Scenario

    scenario = Scenario('raft-basic')
    for i in range(3):
        scenario.add_node(i)

    # Allow time for leader election before proposing
    scenario.add_event(80, 'propose', node=0, value='hello')
    scenario.add_event(100, 'propose', node=0, value='world')

    sim = Simulator(seed=42, deadlock_threshold=500)
    scenario.run(sim, RaftNode)
    sim.run(max_ticks=300)

    valid, msg = sim.validate()
    print(f"Validation: {msg}")
    print(f"Atomic logs: {sim.atomic_logs}")
