#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
evidence=target/calm-evidence
if [[ "${QUORUM_CALM_LOGGED:-0}" != 1 ]]; then
  rm -rf -- "$evidence"
  mkdir -p "$evidence"
  QUORUM_CALM_LOGGED=1 bash scripts/check-calm.sh 2>&1 | tee "$evidence/validation.txt"
  exit 0
fi

# Includes formatting, all tests (and receiver artifacts), clippy, build,
# plus the existing Raft/Ping-Pong capture, replay, prefix and vote evidence.
bash scripts/check-slice2.sh
target/debug/quorum capture calm "$evidence/calm.a.jsonl" > "$evidence/calm.a.capture.json"
target/debug/quorum calm > "$evidence/calm.b.jsonl"
cmp "$evidence/calm.a.jsonl" "$evidence/calm.b.jsonl"
target/debug/quorum inspect "$evidence/calm.a.jsonl" > "$evidence/calm.inspect.json"
target/debug/quorum replay "$evidence/calm.a.jsonl" > "$evidence/calm.replay.json"
target/debug/quorum prefixes "$evidence/calm.a.jsonl" > "$evidence/calm.prefixes.json"

# These are line references for this exact declared workload, not a general
# certificate verifier. The receiver fixtures likewise prove receiver behavior.
python3 - <<'PY_EVIDENCE'
from pathlib import Path
import hashlib
import json

root = Path('target/calm-evidence')
source = root / 'calm.a.jsonl'
raw = source.read_bytes()
lines = raw.splitlines(keepends=True)
rows = [json.loads(line) for line in lines]

def selected(predicate):
    return [(line, row) for line, row in enumerate(rows, 1) if predicate(row)]

def message(row, kind, what='dispatch'):
    return row['what'] == what and row['why'] in ('message', 'send') and row['message']['type'] == kind

def saved(row, kind):
    return row['what'] == 'result' and row['why'] == 'save' and row['outcome'] == 'fulfilled' and row['record']['record'] == kind

def committed(row):
    return row['what'] == 'result' and row['why'] == 'commit' and row['outcome'] == 'fulfilled'

def save_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

header = rows[0]
assert header['scenario'] == 'calm' and header['seed'] == 42
assert header['world'] == {'base_latency': 1, 'jitter': 0, 'tick_interval': 1, 'deadlock_threshold': 200}
assert header['acceptors'] == [0, 1, 2]
assert [node['node'] for node in header['nodes']] == [0, 1, 2]
for node in header['nodes']:
    assert node['algorithm'] == 'multi-paxos'
    assert node['config'] == {
        'acceptors': header['acceptors'], 'learners': [0, 1, 2],
        'proposers': [0, 1, 2], 'active_proposer': 0,
        'from_slot': 0, 'initial_round': 1, 'initial_state': 'empty',
    }

prepare = selected(lambda row: message(row, 'prepare', 'attempt'))
assert len(prepare) == 3
assert [row['to'] for _, row in prepare] == [0, 1, 2]
assert all(row['when'] == 1 and row['message'] == {'type': 'prepare', 'ballot': [1, 0], 'from_slot': 0} for _, row in prepare)
promised = selected(lambda row: saved(row, 'promised'))
assert len(promised) == 3 and all(row['when'] == 2 for _, row in promised)
accept_desires = selected(lambda row: message(row, 'accept', 'desire'))
assert len(accept_desires) == 9
freeze_id = accept_desires[0][1]['parent_input_id']
assert all(row['parent_input_id'] == freeze_id and row['when'] == 3 for _, row in accept_desires)
freeze = selected(lambda row: row['what'] == 'dispatch' and row['input_id'] == freeze_id)
assert len(freeze) == 1

alpha_saves = selected(lambda row: saved(row, 'accepted') and row['record'] == {'record': 'accepted', 'slot': 0, 'ballot': [1, 0], 'value': 'alpha'})
assert len(alpha_saves) == 3 and all(row['when'] == 4 for _, row in alpha_saves)
support = alpha_saves[:2]
assert len({row['node'] for _, row in support}) == 2
cut_line = support[-1][0]
assert not any(row['what'] == 'desire' and row['why'] == 'commit' for row in rows[:cut_line])
assert not any(message(row, 'accepted') for row in rows[:cut_line])
(root / 'calm.chosen-before-known.jsonl').write_bytes(b''.join(lines[:cut_line]))

learning = []
for slot, value in enumerate(('alpha', 'beta', 'gamma')):
    commits = selected(lambda row: committed(row) and row['slot'] == slot)
    assert [(row['node'], row['when'], row['entry']) for _, row in commits] == [(0, 5, value), (1, 6, value), (2, 6, value)]
    replies = selected(lambda row: message(row, 'accepted') and row['message'] == {'type': 'accepted', 'ballot': [1, 0], 'slot': slot, 'value': value})
    assert len(replies) == 3
    quorum = replies[:2]
    assert len({row['from'] for _, row in quorum}) == 2
    assert quorum[-1][0] < commits[0][0]
    self_chosen = selected(lambda row: message(row, 'chosen') and row['to'] == 0 and row['message']['slot'] == slot)
    assert len(self_chosen) == 1 and self_chosen[0][1]['when'] == 6
    input_id = self_chosen[0][1]['input_id']
    close = selected(lambda row: row['what'] == 'close' and row['input_id'] == input_id)
    assert len(close) == 1 and close[0][1]['count'] == 0
    chosen_sends = selected(lambda row: message(row, 'chosen', 'attempt') and row['message']['slot'] == slot)
    assert [row['to'] for _, row in chosen_sends] == [0, 1, 2]
    learning.append({
        'slot': slot, 'value': value,
        'accepted_quorum_dispatch_lines': [line for line, _ in quorum],
        'proposer_commit_result_line': commits[0][0],
        'chosen_send_attempt_lines': [line for line, _ in chosen_sends],
        'self_chosen_dispatch_line': self_chosen[0][0],
        'self_chosen_empty_close_line': close[0][0],
        'other_learner_commit_result_lines': [line for line, _ in commits[1:]],
    })
final = rows[-1]
assert final['what'] == 'finalize' and final['when'] == 8 and final['reason'] == 'horizon'
assert all(value is None or value == [] for value in final['account'].values())

anchors = {}
for name in ('start_campaign', 'prepare', 'promise', 'accept', 'receive_promise', 'propose_waiting', 'receive_accepted', 'learn'):
    text = Path('src/multi_paxos.rs').read_text().splitlines()
    matches = [index for index, line in enumerate(text, 1) if line.lstrip().startswith('fn ' + name + '(')]
    assert len(matches) == 1
    anchors[name] = 'src/multi_paxos.rs:' + str(matches[0])

map_value = {
    'scope': 'Fixed Calm workload observations and receiver-rule fixtures; not the Certificate Verifier.',
    'line_numbering': 'One-based JSONL lines in calm.a.jsonl; the cut preserves the original bytes.',
    'base_revision': 'db3f982c189c7207ecf8e6c6608ec1cec0698256',
    'implementation': rows[0]['implementation'],
    'calm_sha256': hashlib.sha256(raw).hexdigest(),
    'prepare_attempt_lines': [line for line, _ in prepare],
    'promised_save_result_lines': [line for line, _ in promised],
    'freeze_dispatch_line': freeze[0][0],
    'freeze_accept_desire_lines': [line for line, _ in accept_desires],
    'chosen_before_known': {
        'cut_after_line': cut_line,
        'cut_file': 'calm.chosen-before-known.jsonl',
        'fulfilled_acceptance_lines': [line for line, _ in support],
        'accept_dispatch_lines': [line for line, row in selected(lambda row: row['what'] == 'dispatch' and row['input_id'] in {saved_row['parent_input_id'] for _, saved_row in support})],
        'earlier_promise_result_lines': [line for line, row in promised if row['node'] in {saved_row['node'] for _, saved_row in support}],
        'observation': 'At tick 4 two distinct configured acceptors have fulfilled the matching acceptance; no Accepted input or Commit desire has yet reached the proposer. This is the level\'s hand-worked choice interpretation.',
    },
    'learning': learning,
    'finalization_line': len(rows),
    'source_anchors': anchors,
    'receiver_artifacts': sorted(path.name for path in root.glob('*.json*') if not path.name.startswith('calm.')),
}
save_json(root / 'evidence-map.json', map_value)

files = [Path('Cargo.toml'), Path('Cargo.lock'), Path('build.rs')]
files += sorted(Path('src').glob('*.rs')) + sorted(Path('tests').glob('*.rs'))
files += [Path('docs/tape.md')] + sorted(Path('scripts').glob('*.sh'))
(root / 'source-manifest.sha256').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path}\n' for path in files))
(root / 'tape-hashes.sha256').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n' for path in sorted(root.glob('*.jsonl'))))
PY_EVIDENCE

target/debug/quorum inspect "$evidence/calm.chosen-before-known.jsonl" > "$evidence/calm.chosen-before-known.inspect.json"
printf 'Calm: byte-identical captures; public projection, replay, prefixes and line-cited schedule evidence passed\n'
cat "$evidence/calm.replay.json" "$evidence/calm.prefixes.json" "$evidence/tape-hashes.sha256"
