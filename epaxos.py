"""
EPaxos consensus algorithm for Quorum framework

Egalitarian Paxos - leaderless consensus with dependency tracking
Fast path: 1 RTT when replicas agree on dependencies
Slow path: 2 RTT when dependency conflict requires coordination

Simplified sketch - captures core logic but may omit edge cases
"""

from quorum import Node


class EPaxosNode(Node):
    def __init__(self, node_id, quorum_size):
        super().__init__(node_id, quorum_size)

        # Instance space: (replica_id, instance_num) -> instance state
        # State includes: command, deps, status (pre-accepted/accepted/committed), ballot
        self.instances = {}
        self.next_instance = 0

        # Dependency tracking
        self.command_deps = {}  # Track which commands depend on which
        self.executed = set()   # (replica_id, instance_num) pairs already executed
        self.pending_execution = []  # Commands waiting for dependencies

        # Fast quorum size (need all replicas for fast path)
        self.fast_quorum = quorum_size
        self.classic_quorum = (quorum_size // 2) + 1

        # Ballot number for this node
        self.ballot = node_id  # Will increment by quorum_size for uniqueness

    def receive(self, ctx, msg):
        payload = msg['payload']

        match payload.get('type'):

            case 'propose':
                # Client proposes new command
                value = payload['value']

                # Create new instance
                instance_id = (self.node_id, self.next_instance)
                self.next_instance += 1

                # Compute dependencies - all instances we know about
                deps = set()
                for (rid, inum), inst in self.instances.items():
                    if inst.get('command') is not None:
                        deps.add((rid, inum))

                # Initialize instance state
                self.instances[instance_id] = {
                    'command': value,
                    'deps': deps,
                    'status': 'pre-accepted',
                    'ballot': self.ballot,
                    'pre_accept_replies': {},  # node_id -> deps they saw
                    'accept_replies': set()
                }

                ctx.save({
                    'action': 'propose',
                    'instance': instance_id,
                    'command': value,
                    'deps': list(deps)
                })

                # Send PreAccept to all replicas
                for node in range(self.quorum_size):
                    ctx.send(node, {
                        'type': 'PreAccept',
                        'instance': instance_id,
                        'command': value,
                        'deps': list(deps),
                        'ballot': self.ballot
                    })

            case 'PreAccept':
                # Phase 1: PreAccept request
                instance_id = tuple(payload['instance'])
                command = payload['command']
                proposed_deps = set(tuple(d) for d in payload['deps'])
                ballot = payload['ballot']

                # Compute our view of dependencies
                my_deps = set()
                for (rid, inum), inst in self.instances.items():
                    # Don't depend on self or future instances
                    if (rid, inum) != instance_id and inst.get('command') is not None:
                        my_deps.add((rid, inum))

                # Add the proposed deps (union)
                merged_deps = my_deps | proposed_deps

                # Store instance if new or update if higher ballot
                if instance_id not in self.instances:
                    self.instances[instance_id] = {
                        'command': command,
                        'deps': merged_deps,
                        'status': 'pre-accepted',
                        'ballot': ballot
                    }
                    updated = True
                else:
                    inst = self.instances[instance_id]
                    if ballot > inst['ballot']:
                        inst['ballot'] = ballot
                        inst['command'] = command
                        inst['deps'] = merged_deps
                        inst['status'] = 'pre-accepted'
                        updated = True
                    else:
                        updated = False
                        merged_deps = inst['deps']

                if updated:
                    ctx.save({
                        'action': 'pre-accept',
                        'instance': instance_id,
                        'deps': list(merged_deps)
                    })

                # Reply with our dependency view
                ctx.send(msg['from'], {
                    'type': 'PreAcceptReply',
                    'instance': instance_id,
                    'deps': list(merged_deps),
                    'ballot': ballot
                })

            case 'PreAcceptReply':
                # Response to PreAccept
                instance_id = tuple(payload['instance'])
                reply_deps = set(tuple(d) for d in payload['deps'])
                ballot = payload['ballot']

                if instance_id not in self.instances:
                    return

                inst = self.instances[instance_id]

                # Only process if we're the coordinator and right ballot
                if instance_id[0] != self.node_id or ballot != inst['ballot']:
                    return

                if inst['status'] != 'pre-accepted':
                    return

                # Record this replica's dependency view
                inst['pre_accept_replies'][msg['from']] = reply_deps

                # Check if we have enough replies
                if len(inst['pre_accept_replies']) >= self.classic_quorum:
                    # Check if all replicas agree on dependencies (fast path)
                    all_deps_agree = True
                    original_deps = inst['deps']

                    for deps in inst['pre_accept_replies'].values():
                        if deps != original_deps:
                            all_deps_agree = False
                            break

                    if all_deps_agree and len(inst['pre_accept_replies']) >= self.fast_quorum - 1:
                        # Fast path - all agree, commit directly
                        inst['status'] = 'committed'

                        ctx.save({
                            'action': 'fast-commit',
                            'instance': instance_id,
                            'command': inst['command'],
                            'deps': list(inst['deps'])
                        })

                        # Notify all replicas
                        for node in range(self.quorum_size):
                            if node != self.node_id:
                                ctx.send(node, {
                                    'type': 'Commit',
                                    'instance': instance_id,
                                    'command': inst['command'],
                                    'deps': list(inst['deps']),
                                    'ballot': inst['ballot']
                                })

                        # Try to execute
                        self._try_execute(ctx, instance_id)

                    else:
                        # Slow path - disagreement on dependencies
                        # Merge all dependency views
                        merged = inst['deps'].copy()
                        for deps in inst['pre_accept_replies'].values():
                            merged = merged | deps

                        inst['deps'] = merged
                        inst['status'] = 'accepted'
                        inst['accept_replies'] = set()

                        ctx.save({
                            'action': 'slow-path',
                            'instance': instance_id,
                            'merged_deps': list(merged)
                        })

                        # Phase 2: Send Accept with merged deps
                        for node in range(self.quorum_size):
                            ctx.send(node, {
                                'type': 'Accept',
                                'instance': instance_id,
                                'command': inst['command'],
                                'deps': list(merged),
                                'ballot': inst['ballot']
                            })

            case 'Accept':
                # Phase 2: Accept request (slow path)
                instance_id = tuple(payload['instance'])
                command = payload['command']
                deps = set(tuple(d) for d in payload['deps'])
                ballot = payload['ballot']

                # Store or update instance
                if instance_id not in self.instances:
                    self.instances[instance_id] = {
                        'command': command,
                        'deps': deps,
                        'status': 'accepted',
                        'ballot': ballot
                    }
                    ctx.save({
                        'action': 'accept',
                        'instance': instance_id,
                        'command': command,
                        'deps': list(deps)
                    })
                else:
                    inst = self.instances[instance_id]
                    if ballot >= inst['ballot']:
                        inst['ballot'] = ballot
                        inst['command'] = command
                        inst['deps'] = deps
                        inst['status'] = 'accepted'
                        ctx.save({
                            'action': 'accept-update',
                            'instance': instance_id,
                            'deps': list(deps)
                        })

                # Acknowledge
                ctx.send(msg['from'], {
                    'type': 'AcceptReply',
                    'instance': instance_id,
                    'ballot': ballot
                })

            case 'AcceptReply':
                # Response to Accept
                instance_id = tuple(payload['instance'])
                ballot = payload['ballot']

                if instance_id not in self.instances:
                    return

                inst = self.instances[instance_id]

                # Only process if we're coordinator and right ballot
                if instance_id[0] != self.node_id or ballot != inst['ballot']:
                    return

                if inst['status'] != 'accepted':
                    return

                inst['accept_replies'].add(msg['from'])

                # Check if we have classic quorum
                if len(inst['accept_replies']) >= self.classic_quorum:
                    # Committed via slow path
                    inst['status'] = 'committed'

                    ctx.save({
                        'action': 'slow-commit',
                        'instance': instance_id,
                        'command': inst['command'],
                        'deps': list(inst['deps'])
                    })

                    # Notify all replicas
                    for node in range(self.quorum_size):
                        if node != self.node_id:
                            ctx.send(node, {
                                'type': 'Commit',
                                'instance': instance_id,
                                'command': inst['command'],
                                'deps': list(inst['deps']),
                                'ballot': inst['ballot']
                            })

                    # Try to execute
                    self._try_execute(ctx, instance_id)

            case 'Commit':
                # Learn about committed instance
                instance_id = tuple(payload['instance'])
                command = payload['command']
                deps = set(tuple(d) for d in payload['deps'])
                ballot = payload['ballot']

                # Store or update
                if instance_id not in self.instances:
                    self.instances[instance_id] = {
                        'command': command,
                        'deps': deps,
                        'status': 'committed',
                        'ballot': ballot
                    }
                else:
                    inst = self.instances[instance_id]
                    if ballot >= inst['ballot']:
                        inst['command'] = command
                        inst['deps'] = deps
                        inst['status'] = 'committed'
                        inst['ballot'] = ballot

                # Try to execute
                self._try_execute(ctx, instance_id)

    def _try_execute(self, ctx, instance_id):
        """
        Execute commands respecting dependency order.
        This is the hard part - topological sort with cycle breaking.
        """
        if instance_id in self.executed:
            return

        inst = self.instances.get(instance_id)
        if not inst or inst['status'] != 'committed':
            return

        # Check if all dependencies are executed
        for dep_id in inst['deps']:
            if dep_id not in self.executed:
                # Can't execute yet - dependency not ready
                if instance_id not in self.pending_execution:
                    self.pending_execution.append(instance_id)
                return

        # All dependencies satisfied - execute
        self.executed.add(instance_id)
        ctx.log(inst['command'])

        # Check if this unblocks any pending executions
        unblocked = []
        for pending_id in self.pending_execution:
            if pending_id == instance_id:
                continue
            pending_inst = self.instances.get(pending_id)
            if pending_inst and pending_inst['status'] == 'committed':
                # Check if all its deps are now satisfied
                can_execute = True
                for dep in pending_inst['deps']:
                    if dep not in self.executed:
                        can_execute = False
                        break
                if can_execute:
                    unblocked.append(pending_id)

        # Execute newly unblocked commands
        for unblocked_id in unblocked:
            self.pending_execution.remove(unblocked_id)
            self._try_execute(ctx, unblocked_id)


if __name__ == '__main__':
    from quorum import Simulator, Scenario

    scenario = Scenario('epaxos-basic')
    for i in range(3):
        scenario.add_node(i)

    # Concurrent proposals to test fast/slow path
    scenario.add_event(10, 'propose', node=0, value='cmd1')
    scenario.add_event(10, 'propose', node=1, value='cmd2')  # Concurrent
    scenario.add_event(30, 'propose', node=2, value='cmd3')

    sim = Simulator(seed=42)
    scenario.run(sim, EPaxosNode)
    sim.run(max_ticks=200)

    valid, msg = sim.validate()
    print(f"Validation: {msg}")
    print(f"Atomic logs: {sim.atomic_logs}")
    print(f"Executed: {[n.executed for n in sim.nodes.values()]}")