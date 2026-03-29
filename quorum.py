"""
Quorum - Consensus Algorithm Testing Framework

A deterministic simulator for implementing and testing consensus algorithms.
"""

import json
import random
from typing import Any, Dict, List, Optional
from abc import ABC, abstractmethod


class Node(ABC):
    """Base class for consensus algorithm implementations"""

    def __init__(self, node_id: int, quorum_size: int):
        self.node_id = node_id
        self.quorum_size = quorum_size
        self.crashed = False

    @abstractmethod
    def receive(self, ctx, msg: Dict[str, Any]):
        """
        Handle incoming message.
        Single entry point - use big switch on msg['type'].
        """
        pass

    def crash(self):
        """Mark node as crashed"""
        self.crashed = True

    def recover(self):
        """Recover from crash"""
        self.crashed = False


class Context:
    """Node interface to simulator"""

    def __init__(self, simulator, node_id: int):
        self.simulator = simulator
        self.node_id = node_id

    def send(self, to_node: int, payload: Dict[str, Any]):
        """Send message to another node"""
        msg = {
            'from': self.node_id,
            'to': to_node,
            'payload': payload,
            'send_tick': self.simulator.tick
        }
        self.simulator._enqueue_message(msg)

    def save(self, data: Dict[str, Any]):
        """Append JSONL entry to persistent storage"""
        self.simulator._save_persistent(self.node_id, data)

    def log(self, entry: Any):
        """Append to atomic log"""
        self.simulator._append_log(self.node_id, entry)


class Network:
    """Pseudo-network with controllable message delivery"""

    def __init__(self, base_latency: int = 10, jitter: int = 5, seed: Optional[int] = None):
        self.base_latency = base_latency
        self.jitter = jitter
        self.pending: List[Dict] = []
        self.partitions: Dict = {}
        self.rng = random.Random(seed)

    def enqueue(self, msg: Dict, current_tick: int):
        """Enqueue message with computed delivery time"""
        delay = self.base_latency + self.rng.randint(0, self.jitter)
        msg['deliver_tick'] = current_tick + delay
        self.pending.append(msg)

    def deliverable(self, tick: int) -> List[Dict]:
        """Get messages ready for delivery at this tick"""
        ready = []
        remaining = []

        for msg in self.pending:
            if msg['deliver_tick'] > tick:
                remaining.append(msg)
                continue

            partition_key = (msg['from'], msg['to'])
            if partition_key in self.partitions:
                if self.partitions[partition_key] > tick:
                    remaining.append(msg)
                    continue
                else:
                    del self.partitions[partition_key]

            ready.append(msg)

        self.pending = remaining
        return ready

    def partition(self, from_node: int, to_node: int, until_tick: int):
        """Block messages from -> to until specified tick"""
        self.partitions[(from_node, to_node)] = until_tick


class Simulator:
    """Deterministic event loop for consensus testing"""

    def __init__(self, seed: Optional[int] = None, deadlock_threshold: int = 100, tick_interval: int = 1):
        self.tick = 0
        self.nodes: Dict[int, Node] = {}
        self.network = Network(seed=seed)
        self.deadlock_threshold = deadlock_threshold
        self.tick_interval = tick_interval

        # Persistence - JSONL per node
        self.persistent: Dict[int, List[Dict]] = {}

        # Atomic log - what each node has committed
        self.atomic_logs: Dict[int, List[Any]] = {}

        # Trace
        self.trace: List[Dict] = []
        self.last_commit_tick = 0

    def spawn(self, node_id: int, node: Node):
        """Add a node to the simulation"""
        self.nodes[node_id] = node
        self.persistent[node_id] = []
        self.atomic_logs[node_id] = []
        self._log_event('spawn', {'node': node_id})

    def crash(self, node_id: int):
        """Crash a node"""
        if node_id in self.nodes:
            self.nodes[node_id].crash()
            self._log_event('crash', {'node': node_id})

    def recover(self, node_id: int):
        """Recover a crashed node (can read persistent storage)"""
        if node_id in self.nodes:
            self.nodes[node_id].recover()
            self._log_event('recover', {'node': node_id})

    def partition(self, from_node: int, to_node: int, duration: int):
        """Block messages from -> to for duration ticks"""
        self.network.partition(from_node, to_node, self.tick + duration)
        self._log_event('partition', {
            'from': from_node,
            'to': to_node,
            'until': self.tick + duration
        })

    def propose(self, node_id: int, value: Any):
        """Client proposes value to node"""
        if node_id not in self.nodes or self.nodes[node_id].crashed:
            return

        msg = {
            'from': -1,  # Client
            'to': node_id,
            'payload': {'type': 'propose', 'value': value},
            'send_tick': self.tick,
            'deliver_tick': self.tick
        }

        ctx = Context(self, node_id)
        self.nodes[node_id].receive(ctx, msg)
        self._log_event('propose', {'node': node_id, 'value': value})

    def step(self) -> bool:
        """
        Execute one tick.
        Returns False if deadlock detected.
        """
        self.tick += 1

        # Deliver messages
        for msg in self.network.deliverable(self.tick):
            if msg['to'] not in self.nodes:
                continue

            node = self.nodes[msg['to']]
            if node.crashed:
                continue

            ctx = Context(self, msg['to'])
            node.receive(ctx, msg)

            self._log_event('deliver', {
                'to': msg['to'],
                'from': msg['from'],
                'type': msg['payload'].get('type', 'unknown')
            })

        # Inject tick messages to all live nodes
        if self.tick_interval > 0 and self.tick % self.tick_interval == 0:
            for node_id, node in self.nodes.items():
                if node.crashed:
                    continue
                ctx = Context(self, node_id)
                tick_msg = {
                    'from': -1,
                    'to': node_id,
                    'payload': {'type': 'tick'},
                    'send_tick': self.tick,
                    'deliver_tick': self.tick
                }
                node.receive(ctx, tick_msg)

        # Check for progress
        if self._check_progress():
            self.last_commit_tick = self.tick
        elif self.tick - self.last_commit_tick > self.deadlock_threshold:
            self._log_event('deadlock', {
                'tick': self.tick,
                'ticks_since_progress': self.tick - self.last_commit_tick
            })
            return False

        return True

    def run(self, max_ticks: int = 1000) -> bool:
        """
        Run simulation until completion or deadlock.
        Returns True if completed successfully, False if deadlock.
        """
        while self.tick < max_ticks:
            if not self.step():
                return False
        return True

    def _enqueue_message(self, msg: Dict):
        """Called by Context.send()"""
        self.network.enqueue(msg, self.tick)

    def _save_persistent(self, node_id: int, data: Dict):
        """Called by Context.save() - append JSONL"""
        entry = {'tick': self.tick, 'data': data}
        self.persistent[node_id].append(entry)

    def _append_log(self, node_id: int, entry: Any):
        """Called by Context.log() - append to atomic log and validate"""
        log = self.atomic_logs[node_id]
        index = len(log)
        log.append(entry)

        # Check consistency across all nodes at this index
        for other_id, other_log in self.atomic_logs.items():
            if other_id == node_id:
                continue
            if len(other_log) > index:
                if other_log[index] != entry:
                    self._log_event('SPLIT_BRAIN', {
                        'index': index,
                        'node': node_id,
                        'entry': entry,
                        'other_node': other_id,
                        'other_entry': other_log[index]
                    })
                    raise Exception(f"Split brain at index {index}: node {node_id} vs {other_id}")

        self.last_commit_tick = self.tick

    def _check_progress(self) -> bool:
        """Check if any node committed new entries"""
        if not self.atomic_logs:
            return False

        max_length = max(len(log) for log in self.atomic_logs.values())
        if not hasattr(self, '_last_max_length'):
            self._last_max_length = 0

        if max_length > self._last_max_length:
            self._last_max_length = max_length
            return True

        return False

    def _log_event(self, event_type: str, data: Dict):
        """Record event"""
        event = {
            'tick': self.tick,
            'type': event_type,
            'data': data
        }
        self.trace.append(event)

    def save_trace(self, filename: str):
        """Save trace to JSONL"""
        with open(filename, 'w') as f:
            for event in self.trace:
                f.write(json.dumps(event) + '\n')

    def save_persistent(self, node_id: int, filename: str):
        """Save node's persistent storage to JSONL"""
        with open(filename, 'w') as f:
            for entry in self.persistent[node_id]:
                f.write(json.dumps(entry) + '\n')

    def validate(self) -> tuple:
        """Check safety properties"""
        if not self.atomic_logs:
            return True, "No logs"

        # All nodes should have same committed prefix
        logs = list(self.atomic_logs.values())
        first_log = logs[0]

        for log in logs[1:]:
            min_len = min(len(first_log), len(log))
            if first_log[:min_len] != log[:min_len]:
                return False, "Split brain detected"

        return True, "Valid"


class Scenario:
    """Declarative scenario specification"""

    def __init__(self, name: str):
        self.name = name
        self.nodes: List[int] = []
        self.events: List[tuple] = []

    def add_node(self, node_id: int):
        """Add a node to spawn"""
        self.nodes.append(node_id)

    def add_event(self, tick: int, event_type: str, **params):
        """Schedule an event at specific tick"""
        self.events.append((tick, event_type, params))

    def run(self, simulator: Simulator, node_factory):
        """Execute scenario on simulator"""
        for node_id in self.nodes:
            node = node_factory(node_id, len(self.nodes))
            simulator.spawn(node_id, node)

        sorted_events = sorted(self.events, key=lambda e: e[0])
        event_idx = 0

        while event_idx < len(sorted_events):
            tick, event_type, params = sorted_events[event_idx]

            while simulator.tick < tick:
                if not simulator.step():
                    return

            # Big switch on event type
            match event_type:
                case 'crash':
                    simulator.crash(params['node'])
                case 'recover':
                    simulator.recover(params['node'])
                case 'propose':
                    simulator.propose(params['node'], params['value'])
                case 'partition':
                    simulator.partition(params['from'], params['to'], params['duration'])

            event_idx += 1


if __name__ == '__main__':
    # Example: minimal echo node
    class EchoNode(Node):
        def receive(self, ctx, msg):
            # Big switch on message type
            match msg['payload'].get('type'):
                case 'propose':
                    value = msg['payload']['value']
                    ctx.log(value)
                    ctx.save({'committed': value})

    # Run basic test
    sim = Simulator(seed=42)
    for i in range(3):
        sim.spawn(i, EchoNode(i, 3))

    sim.propose(0, "hello")
    sim.propose(1, "world")
    sim.run(max_ticks=100)

    valid, msg = sim.validate()
    print(f"Validation: {msg}")
    print(f"Final tick: {sim.tick}")
    print(f"Logs: {sim.atomic_logs}")
