#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 2 ]]; then
  echo "usage: $0 <retained-evidence-directory> <duel-level-file>" >&2
  exit 2
fi
# Resolve caller-relative inputs before entering the checkout; never supply a
# private workbench default or refresh the retained answer key.
retained=$(python3 -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve(strict=True))' "$1")
level=$(python3 -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve(strict=True))' "$2")
cd "$(dirname "$0")/.."
evidence=target/duel-evidence
python3 - "$retained" "$level" <<'PY_PATHS'
from pathlib import Path
import sys
for generated in ('duel-evidence', 'paxos-account-evidence', 'calm-evidence', 'slice2-evidence'):
    output = Path('target', generated).resolve()
    for value in sys.argv[1:]:
        path = Path(value)
        assert path != output and output not in path.parents and path not in output.parents, (path, output)
PY_PATHS
if [[ "${QUORUM_DUEL_LOGGED:-0}" != 1 ]]; then
  rm -rf -- "$evidence"
  mkdir -p "$evidence"
  QUORUM_DUEL_LOGGED=1 bash scripts/check-duel.sh "$retained" "$level" 2>&1 | tee "$evidence/validation.txt"
  # The tee has closed: the transcript is covered too, without a circular hash.
  python3 - "$evidence" <<'PY_HASHES'
from pathlib import Path
import hashlib, sys
root = Path(sys.argv[1])
files = sorted(path for path in root.rglob('*') if path.is_file() and path.name != 'SHA256SUMS')
(root/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}\n' for path in files))
print(f'Duel artifacts: {len(files)} files covered by {root}/SHA256SUMS')
PY_HASHES
  exit 0
fi
set -x
python3 - "$retained" "$level" <<'PY_INPUTS'
from pathlib import Path
import hashlib, json, subprocess, sys
root = Path('target/duel-evidence')
retained, level = map(Path, sys.argv[1:])
sha = lambda data: hashlib.sha256(data).hexdigest()
level_hash = 'e5a1518ff5f24b01dabe46520cf326256a75c0aeaafd6a38212a34e54f6d78fa'
manifest_hash = '9dced4bc3743e8e54b8ef5176ecf3494998ecc39418429988d9b46f2c5f5e836'
assert sha(level.read_bytes()) == level_hash, 'The commissioned Duel level changed'
manifest = (retained/'SHA256SUMS').read_bytes()
assert sha(manifest) == manifest_hash, 'The immutable retained root manifest changed'
records = []
for line in manifest.decode().splitlines():
    digest, name = line.split('  ', 1)
    path = retained/name
    assert not Path(name).is_absolute() and '..' not in Path(name).parts
    assert path.resolve().is_relative_to(retained) and not path.is_symlink(), name
    assert sha(path.read_bytes()) == digest, f'Retained evidence changed: {name}'
    records.append({'path': name, 'sha256': digest})
assert len(records) == len({record['path'] for record in records}) == 207
interpretation = retained/'account-interpretation-d8886526.md'
assert sha(interpretation.read_bytes()) == 'd8886526fbaa53326c66a9b282a78f7b71de63d4f877bea3de65ebefaf0866d3'
(root/'retained-inputs.sha256').write_bytes(manifest)
(root/'input-manifest.json').write_text(json.dumps({
    'schema_version': 1, 'duel_level': str(level), 'duel_level_sha256': level_hash,
    'retained_root': str(retained), 'retained_manifest_sha256': manifest_hash,
    'verified_retained_files': records, 'read_only_inputs': True,
    'account_interpretation': str(interpretation),
    'account_interpretation_sha256': sha(interpretation.read_bytes()),
    'account_review_root': str(retained/'account-review'),
    'account_audit_root': str(retained/'account-audit'),
}, sort_keys=True, indent=2)+'\n')
files = sorted({Path('Cargo.toml'), Path('Cargo.lock'), Path('build.rs'),
    *Path('src').rglob('*'), *Path('tests').rglob('*'), *Path('scripts').rglob('*.sh'),
    *Path('docs').rglob('*'), Path('README.md')})
files = [path for path in files if path.is_file()]
source = ''.join(f'{sha(path.read_bytes())}  {path}\n' for path in files)
(root/'source-manifest.sha256').write_text(source)
(root/'source-before.json').write_text(json.dumps({
    'validated_revision': subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip(),
    'git_status_porcelain': subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all'], text=True),
    'source_manifest_sha256': sha(source.encode()),
}, sort_keys=True, indent=2)+'\n')
print(f'Pinned Duel level and {len(records)} immutable retained files verified', flush=True)
PY_INPUTS
# This accepted command retains the d8886526 account interpretation and runs
# fmt, every Rust test (which emits the declared control), clippy, build and
# all existing Raft, Ping/Pong, Calm, review and audit evidence gates.
QUORUM_ACCOUNT_REVIEW_EVIDENCE="$retained/account-review" \
QUORUM_ACCOUNT_AUDIT_EVIDENCE="$retained/account-audit" \
bash scripts/check-paxos-account.sh "$retained" "$retained/account-interpretation-d8886526.md"
python3 - "$retained" "$level" <<'PY_DUEL'
from collections import Counter
from pathlib import Path
import copy, hashlib, json, platform, shlex, subprocess, sys

root = Path('target/duel-evidence')
retained, level = map(Path, sys.argv[1:])
explore = retained/'duel-explore'
account = Path('target/paxos-account-evidence')
binary = str(Path('target/debug/quorum').resolve())
sha = lambda data: hashlib.sha256(data).hexdigest()
account_hash = 'd8886526fbaa53326c66a9b282a78f7b71de63d4f877bea3de65ebefaf0866d3'
commissioned_base = '3b75d22c46d9d40748d692d075ed61ec13f44ad2'
accepted_account_base = '97c7ad0315f5bb7dcb06ebb29a7d05e9ac347bfe'

def save_json(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)+'\n')

def command(*args):
    print('+ '+shlex.join(map(str,args)), flush=True)
    return subprocess.check_output(list(map(str,args)))

def artifact(name, *args):
    data = command(*args)
    (root/name).write_bytes(data)
    print(f'  {name}: sha256 {sha(data)}', flush=True)
    return json.loads(data)

def rows_of(data):
    lines = data.splitlines(keepends=True)
    assert lines and all(line.endswith(b'\n') for line in lines), 'Only complete JSONL rows are accepted'
    return lines, [json.loads(line) for line in lines]

def semantic_report(report):
    result = copy.deepcopy(report)
    del result['source_sha256']
    # A changed implementation/scenario header changes byte offsets, not any
    # protocol interpretation or one-based row join.
    del result['world']['valid_prefix_bytes']
    return result

def report_for(path, retained_name):
    name = path.stem
    raw = path.read_bytes()
    report = artifact(f'{name}.report.json', binary, 'paxos-account', path, '--json')
    assert report['source_sha256'] == sha(raw)
    assert report['schema_version'] == 1 and report['interpretation'].endswith(account_hash)
    assert report['world']['error'] is None and report['world']['stopped'] is None
    assert report['world']['valid_rows'] == report['world']['total_rows'] == len(raw.splitlines())
    assert report['world']['valid_prefix_bytes'] == len(raw)
    assert not report['interpretation_findings']
    expected = json.loads((explore/retained_name).read_bytes())
    assert semantic_report(report) == semantic_report(expected), f'Account meaning differs from immutable answer key: {name}'
    human = command(binary, 'paxos-account', path)
    assert human, name
    (root/f'{name}.human.txt').write_bytes(human)
    return report

def obligation(report, proposition, boundary, verdict='established'):
    found = [p for p in report['obligations'] if p['proposition']==proposition and p['boundary_line']==boundary]
    assert found and all(p['verdict']==verdict for p in found), (proposition, boundary, found)
    return found

def first_quorums(report):
    return [o for o in report['observations'] if o['kind']=='first_promise_quorum']

def lower_attempt(report):
    found = [a for a in report['attempts'] if a['proposer']==0 and a['slot']==0 and a['ballot']==[1,0] and a['value']=='x']
    assert len(found)==1
    return found[0]

def accepted_inventory(rows):
    return [{'line': line, 'acceptor': row['node'], **row['record']}
        for line,row in enumerate(rows,1) if row['what']=='result' and row['why']=='save'
        and row['outcome']=='fulfilled' and row['record']['record']=='accepted']

def covered_empty(report, header):
    assert [node['node'] for node in report['coverage']['nodes']]==[0,1,2]
    assert report['coverage']['header_acceptors']==[0,1,2]
    assert all(node['contract']=='covered' and node['initial_state_known'] and node['header_acceptor']
        for node in report['coverage']['nodes'])
    assert all(node['config']['initial_state']=='empty' for node in header['nodes'])

judgment_hashes = {
    'review-judgments.json': 'db7c2d103cce6075844d1c5bb2a5005d3b1d923ef45066c5a50ca1ed1c648cb0',
    'audit-judgments.json': 'ecda9c8694d87ba0341a35a41192a8260080bbd8c219d334567db55d7aeb6edb',
    'postaccept-judgments.json': '51c4d177023af04b76d978f99a71b0ac181ecabff95922af1dc3f60f299f4bd0',
    'timing-judgments.json': '44631e78ae559435e50bba52f7d5f9705717dc756bcb54a91ac465a8f2ff0583',
}
for name,digest in judgment_hashes.items():
    assert sha((account/name).read_bytes())==digest, f'Accepted account judgment changed: {name}'
account_manifest = json.loads((account/'manifest.json').read_bytes())
assert account_manifest['base_revision']==accepted_account_base
assert account_manifest['interpretation_sha256']==account_hash
assert account_manifest['review_root']==str(retained/'account-review')
assert account_manifest['audit_root']==str(retained/'account-audit')
save_json(root/'account-judgment-hashes.json', judgment_hashes)

# Recompute build.rs's exact source identity, rather than trusting a stale binary.
source_hash = hashlib.sha256()
for path in sorted([Path('Cargo.toml'), Path('Cargo.lock'), Path('build.rs'),
        *(path for path in Path('src').rglob('*') if path.is_file())]):
    data = path.read_bytes()
    source_hash.update(str(path).encode())
    source_hash.update(b'\0')
    source_hash.update(len(data).to_bytes(8,'little'))
    source_hash.update(data)
head = command('git','rev-parse','HEAD').decode().strip()
source_status = command('git','status','--porcelain','--untracked-files=all','--','src','Cargo.toml','Cargo.lock','build.rs')
assert account_manifest['validated_revision']==head
implementation = None
catalog = []
cut_checks = []
terminal_checks = []
for kind,seed,close,old_save,old_acceptor,quorum,late,body_hash,choice_rows in (
    ('inclusion',331,172,114,0,[106,165],193,
        '1ffb7c51ba0494fa84d379bd52b2340f6b1639007beb3cc9ad88f20f3bfb42f3',[(1,'y',229),(0,'x',251)]),
    ('exclusion',1271,186,126,1,[159,182],197,
        '3f10dd73c8600839fce8b4b4d1bb4fb5ff257614bf13a95140a1fcd5097bc4a8',[(0,'y',242)]),
):
    scenario = f'duel-{kind}'
    historical = f'jitter-3-{kind}-seed-{seed}'
    path = root/f'{scenario}.a.jsonl'
    artifact(f'{scenario}.a.capture.json', binary, 'capture', scenario, path)
    second = root/f'{scenario}.b.jsonl'
    artifact(f'{scenario}.b.capture.json', binary, 'capture', scenario, second)
    raw = path.read_bytes()
    assert raw==second.read_bytes(), f'Non-deterministic capture: {scenario}'
    alias = command(binary, scenario)
    assert raw==alias, f'CLI alias differs from capture: {scenario}'
    lines,rows = rows_of(raw)
    old_lines,old_rows = rows_of((explore/f'{historical}.jsonl').read_bytes())
    body = b''.join(lines[1:])
    assert sha(body)==body_hash and body==b''.join(old_lines[1:])
    header = rows[0]
    assert header['scenario']==scenario and header['seed']==seed
    assert {k:v for k,v in header.items() if k not in ('scenario','implementation')} == {
        k:v for k,v in old_rows[0].items() if k not in ('scenario','implementation')}
    assert header['implementation']['revision']==head
    assert header['implementation']['source_sha256']==source_hash.hexdigest()
    assert header['implementation']['dirty']==bool(source_status)
    if implementation is None:
        implementation = header['implementation']
    assert implementation==header['implementation']
    projection = artifact(f'{scenario}.inspect.json', binary, 'inspect', path)
    replay = artifact(f'{scenario}.replay.json', binary, 'replay', path)
    prefixes = artifact(f'{scenario}.prefixes.json', binary, 'prefixes', path)
    assert projection['source_sha256']==replay['source_sha256']==prefixes['source_sha256']==sha(raw)
    assert replay['mode']=='strict' and replay['tail']=='finalized' and replay['uncertified_answers']==0
    assert replay['replay_implementation']==implementation
    assert prefixes['prefixes']==len(rows)+1 and sum(prefixes['tail_classes'].values())==len(rows)+1
    assert prefixes['tail_classes']['empty']==prefixes['tail_classes']['finalized']==1
    full = report_for(path, f'{historical}.report.json')
    covered_empty(full, header)
    assert all(p['verdict']=='established' for p in full['obligations'])
    assert [(c['slot'],c['value'],c['first_quorum_line']) for c in full['choices']]==choice_rows
    assert all(c['ballot']==[2,1] for c in full['choices'])
    assert all(c['local_basis']['verdict']==c['transitive_support']['verdict']=='established' for c in full['claims'])
    b_quorum = next(o for o in first_quorums(full) if o['node']==1)
    assert b_quorum['dispatch_lines']==quorum and b_quorum['first_quorum_determined'] and b_quorum['adoption_map_determined']
    assert b_quorum['adoption_map']==([{'slot':0,'value':'x','ballot':[1,0],'report_lines':[165]}] if kind=='inclusion' else [])
    assert any(o['kind']=='promise_not_counted' and o['node']==1 and o['line']==late and o['reason']=='frozen_quorum'
        for o in full['observations'])
    assert rows[late-1]['message']['type']=='promise'
    if kind=='exclusion':
        assert rows[late-1]['from']==1 and rows[late-1]['message']['accepted']==[
            {'slot':0,'ballot':[1,0],'value':'x'}]
    higher = [a for a in full['attempts'] if a['proposer']==1]
    assert [(a['slot'],a['value'],a['origin']['kind']) for a in higher]==(
        [(0,'x','adopted'),(1,'y','fresh')] if kind=='inclusion' else [(0,'y','fresh')])
    assert all(a['origin']['allocation_line']==close for a in higher)
    for a in higher:
        if a['origin']['kind']=='fresh':
            assert a['origin']['client_count']=={'available':1,'allocated':1,'certain':True}
        else:
            assert a['origin']['client_count'] is None
            ancestry = a['origin']['acceptance_ancestry']
            assert ancestry['verdict']==ancestry['selection']['verdict']=='established'
            assert ancestry['frozen_report_dispatch_lines']==quorum
            assert {s['result_line'] for p in ancestry['paths'] for s in p['matching_saves']}=={old_save}
            assert late not in a['origin']['support']
    low = lower_attempt(full)
    assert low['exact_choice'] is None and low['resolution']['state']=='unresolved'
    assert low['resolution']['whole_world_empty'] and not low['resolution']['world_open'] and not low['world_work']
    assert not low['resolution']['attribution_uncertain']
    replies = [reply for member in low['replies'] for reply in member['received']]
    assert Counter(r['kind'] for r in replies)=={'accepted':1,'nack':2}
    assert all(r['classification']=='matching' and r['eligible'] for r in replies)
    for reply in replies:
        request = rows[reply['request_dispatch_line']-1]
        assert request['what']=='dispatch' and request['why']=='message'
        assert request['to']==reply['from'] and request['from']==0
        assert request['message']=={'type':'accept','slot':0,'ballot':[1,0],'value':'x'}
        assert rows[reply['send_attempt_line']-1]['what']=='attempt'
    assert low['same_value_elsewhere']==(['choice:251'] if kind=='inclusion' else [])
    assert low['slot_witnessed_values']==([] if kind=='inclusion' else ['choice:242'])
    assert [(i['line'],i['acceptor']) for i in accepted_inventory(rows) if i['ballot']==[1,0]]==[(old_save,old_acceptor)]
    final = rows[-1]
    assert (final['what'],final['when'],final['reason'])==('finalize',28,'horizon')
    assert all(value is None or value==[] for value in final['account'].values())
    assert full['world']['tail']=='finalized' and full['world']['account']==final['account']
    assert all(not a['world_work'] and a['resolution']['whole_world_empty'] for a in full['attempts'])
    assert not any(row['what']=='fault' for row in rows)
    commits = [(line,row) for line,row in enumerate(rows,1) if row['what']=='result' and row['why']=='commit' and row['outcome']=='fulfilled']
    outcomes = [(0,'x'),(1,'y')] if kind=='inclusion' else [(0,'y')]
    assert sorted((row['node'],row['slot'],row['entry']) for _,row in commits)==sorted(
        (node,slot,value) for node in (0,1,2) for slot,value in outcomes)
    if kind=='inclusion':
        assert [row['entry'] for _,row in commits if row['node']==1]==['y','x']
    # The source-fixed finite workload has at most five D=4 hops from tick 1.
    assert 1+5*max(1,header['world']['base_latency']+header['world']['jitter']) < 28
    assert all(row.get('message',{}).get('ballot',[0,0]) <= [2,1] for row in rows)
    assert all(row['when']<=21 for row in rows if row['what']=='attempt' and row['why']=='send')
    prepare = [row for row in rows if row['what']=='attempt' and row['why']=='send' and row['message']['type']=='prepare']
    assert len(prepare)==6 and all(row['when']==1 for row in prepare)
    full_obligations = {p['id']:p for p in full['obligations']}
    full_attempts = {a['id']:a for a in full['attempts']}
    assert len(full_obligations)==len(full['obligations'])
    for cut in (close,close+1):
        cut_path = root/f'{scenario}.cut-{cut}.jsonl'
        cut_path.write_bytes(b''.join(lines[:cut]))
        cut_report = report_for(cut_path, f'{historical}.cut-{cut}.report.json')
        covered_empty(cut_report, header)
        assert not cut_report['choices'] and not cut_report['claims']
        assert all(p['verdict']=='established' for p in cut_report['obligations'])
        inventory = accepted_inventory(rows[:cut])
        assert inventory==[{'line':old_save,'acceptor':old_acceptor,'record':'accepted','slot':0,'ballot':[1,0],'value':'x'}]
        compared = []
        excluded = []
        for p in cut_report['obligations']:
            if p['proposition']=='no_conflicting_choices' and p['boundary_line']==cut:
                excluded.append(p['id'])
                continue
            assert p['id'] in full_obligations and p==full_obligations[p['id']], (scenario,cut,p)
            compared.append(p['id'])
        assert len(excluded)==1
        for a in cut_report['attempts']:
            assert a['origin']==full_attempts[a['id']]['origin'], (scenario,cut,a['id'])
        assert first_quorums(cut_report)==first_quorums(full)
        cut_low = lower_attempt(cut_report)
        assert cut_low['world_work'] and cut_low['resolution']['state']=='world_open'
        assert cut_low['resolution']['world_open'] and not cut_low['resolution']['whole_world_empty']
        obligation(cut_report,'fifo',close)
        obligation(cut_report,'fresh_origin_count',close)
        obligation(cut_report,'proposal_drain',close)
        assert rows[close-1]['what']=='close'
        assert rows[close-1]['count']==(6 if kind=='inclusion' else 3)
        assert cut_report['world']['account']['active_input']==rows[close-1]['input_id']
        assert cut_report['world']['account']['unreached_desires']
        if cut==close:
            assert cut_report['world']['tail']=='interrupted_drain'
            assert not cut_report['world']['account']['open_attempts']
        else:
            assert cut_report['world']['tail']=='open_effect'
            assert cut_report['world']['account']['open_attempts']
            for proposition in ('adoption','phase2_gate','proposer_authority'):
                obligation(cut_report,proposition,cut)
        cut_checks.append({'scenario':scenario,'cut_line':cut,'no_choices':True,
            'covered_declared_empty_initial_states':True,'no_interpretation_gaps':True,
            'fulfilled_acceptance_inventory':inventory,'full_equal_obligation_ids':compared,
            'prefix_only_safety_summary_ids':excluded,'all_attempt_origins_equal':True,
            'all_first_promise_quorums_equal':True,'lower_attempt_world_work':cut_low['world_work'],
            'world':cut_report['world']})
    terminal_checks.append({'scenario':scenario,'final_line':len(rows),'horizon':28,
        'empty_world_account':final['account'],'lower_attempt':low,
        'choice_rows':choice_rows,'commit_rows':[line for line,_ in commits],
        'finite_hop_bound':{'first_tick':1,'max_hops':5,'D':4,'latest_bound':21,'horizon':28}})
    catalog.append({'scenario':scenario,'seed':seed,'tape':str(path.relative_to(root)),
        'rows':len(rows),'sha256':sha(raw),'body_sha256':body_hash,
        'retained_tape':str(explore/f'{historical}.jsonl'),'strict_replay':replay,'every_complete_row_prefix':prefixes})

# This file must have been emitted by the Rust tests in the accepted account
# command above. Never reconstruct it here or put its wrapper in production.
control_path = root/'declared-faulty-answer-to-first-choice.jsonl'
control_raw = control_path.read_bytes()
control_lines,control_rows = rows_of(control_raw)
assert len(control_rows)==221
control_body = b''.join(control_lines[1:])
assert sha(control_body)=='a5946906ba6bab2daccafff6b8680170a1b8bc51fc666bf13549738c72160b39'
retained_control = explore/control_path.name
old_control_lines,old_control_rows = rows_of(retained_control.read_bytes())
assert control_body==b''.join(old_control_lines[1:])
assert 'fault' in control_rows[0]['scenario'] and control_rows[0]['scenario'] not in ('duel-inclusion','duel-exclusion')
assert control_rows[0]['implementation']==implementation
assert {k:v for k,v in control_rows[0].items() if k not in ('scenario','implementation')} == {
    k:v for k,v in old_control_rows[0].items() if k not in ('scenario','implementation')}
control = report_for(control_path,'declared-faulty-answer-to-first-choice.report.json')
covered_empty(control,control_rows[0])
assert [(c['slot'],c['ballot'],c['value'],c['first_quorum_line']) for c in control['choices']]==[(0,[2,1],'y',221)]
assert [s['save_line'] for s in control['choices'][0]['first_support']]==[195,221]
assert control_rows[168]['what']=='close' and control_rows[168]['count']==3
assert control_rows[169]['what']=='attempt' and control_rows[169]['why']=='send'
b_attempts = [a for a in control['attempts'] if a['proposer']==1]
assert len(b_attempts)==1
b = b_attempts[0]
assert (b['slot'],b['value'],b['origin']['kind'],b['origin']['allocation_line'])==(0,'y','fresh',169)
assert b['origin']['client_count']=={'available':1,'allocated':1,'certain':True}
assert 'acceptance_ancestry' not in b['origin']
for proposition in ('fifo','proposal_drain'):
    obligation(control,proposition,169,'violated')
obligation(control,'fresh_origin_count',169)
for boundary in (170,172,174):
    adoption = obligation(control,'adoption',boundary,'violated')
    assert all({106,165} <= set(p['support']) and boundary in p['counterevidence'] for p in adoption)
    for proposition in ('phase2_gate','proposer_authority'):
        obligation(control,proposition,boundary)
obligation(control,'no_conflicting_choices',221)
assert Counter(p['proposition'] for p in control['obligations'] if p['verdict']=='violated')=={
    'fifo':1,'proposal_drain':1,'adoption':3}
assert all(p['verdict'] in ('established','violated') for p in control['obligations'])
wrong_path = account/'protocol-wrong-adoption-conflicting-certificates.report.json'
wrong = json.loads(wrong_path.read_bytes())
assert [(c['slot'],c['value']) for c in wrong['choices']]==[(0,'x'),(0,'y')]
assert any(p['proposition']=='adoption' and p['verdict']=='violated' for p in wrong['obligations'])
assert any(p['proposition']=='no_conflicting_choices' and p['verdict']=='violated' for p in wrong['obligations'])
source = Path('tests/duel.rs')
assert source.is_file()
save_json(root/'declared-faulty-answer-provenance.json', {
    'schema_version':1,'kind':'Declared faulty-answer control', 'honest_duel':False,
    'unchanged_inhabitant_replay_claim':False,'world_fault':False,
    'generated_by':'cargo test --locked (via scripts/check-paxos-account.sh)',
    'generated_test_source':str(source),'generated_test_source_sha256':sha(source.read_bytes()),
    'implementation':implementation,'source_tape':control_path.name,'rows':221,
    'body_sha256':sha(control_body),'retained_tape':str(retained_control),
    'retained_source':str(explore/'faulty-answer-control.rs'),
    'retained_source_sha256':sha((explore/'faulty-answer-control.rs').read_bytes()),
    'retained_provenance':str(explore/'faulty-answer-control-provenance.json'),
    'retained_provenance_sha256':sha((explore/'faulty-answer-control-provenance.json').read_bytes()),
    'mutation':'At B allocation replace [x@0,y@1] with [y@0], omit slot-1 broadcasts, spend waiting y once; stop at the first y certificate.',
    'strict_replay_intentionally_inapplicable':'The test-only wrapper is not the unchanged factory inhabitant.',
    'paired_conflicting_certificates_regression':str(wrong_path),
})
save_json(root/'cut-judgments.json',cut_checks)
save_json(root/'terminal-judgments.json',terminal_checks)
save_json(root/'control-judgments.json',{'choices':control['choices'],'attempt':b,
    'violations':[p for p in control['obligations'] if p['verdict']=='violated'],
    'other_covered_obligations_established':True,'paired_wrong_adoption_choices':wrong['choices']})

# Recheck all read-only inputs after the run; source must not have moved either.
input_manifest = json.loads((root/'input-manifest.json').read_bytes())
assert sha(level.read_bytes())==input_manifest['duel_level_sha256']
assert sha((retained/'SHA256SUMS').read_bytes())==input_manifest['retained_manifest_sha256']
for record in input_manifest['verified_retained_files']:
    assert sha((retained/record['path']).read_bytes())==record['sha256'], record['path']
for line in (root/'source-manifest.sha256').read_text().splitlines():
    digest,name = line.split('  ',1)
    assert sha(Path(name).read_bytes())==digest, f'Source changed during validation: {name}'
status = command('git','status','--porcelain=v1','--untracked-files=all').decode()
before = json.loads((root/'source-before.json').read_bytes())
assert head==before['validated_revision'] and status==before['git_status_porcelain']
regressions = sorted(path for directory in ('paxos-account-evidence','calm-evidence','slice2-evidence')
    for path in Path('target',directory).rglob('*') if path.is_file())
regression_manifest = ''.join(f'{sha(path.read_bytes())}  {path}\n' for path in regressions)
(root/'regression-artifacts.sha256').write_text(regression_manifest)
save_json(root/'manifest.json', {
    'schema_version':1,'commissioned_base_revision':commissioned_base,'validated_revision':head,
    'git_status_porcelain':status,'implementation':implementation,
    'accepted_account_base_revision':accepted_account_base,'account_interpretation_sha256':account_hash,
    'duel_level':str(level),'duel_level_sha256':input_manifest['duel_level_sha256'],
    'retained_root':str(retained),'retained_manifest_sha256':input_manifest['retained_manifest_sha256'],
    'verified_retained_files':len(input_manifest['verified_retained_files']),
    'input_manifest':'input-manifest.json','source_manifest':'source-manifest.sha256',
    'source_manifest_sha256':sha((root/'source-manifest.sha256').read_bytes()),
    'account_manifest':str(account/'manifest.json'),'account_judgment_hashes':judgment_hashes,
    'regression_artifacts_manifest':'regression-artifacts.sha256',
    'regression_artifacts_manifest_sha256':sha(regression_manifest.encode()),
    'rustc':command('rustc','--version','--verbose').decode().strip(),
    'cargo':command('cargo','--version').decode().strip(),
    'python':sys.version,'platform':platform.platform(),
    'bash':command('bash','--version').decode().splitlines()[0],
    'git':command('git','--version').decode().strip(),
    'jq':command('jq','--version').decode().strip(),
    'reproduce':'bash scripts/check-duel.sh <retained-evidence-directory> <duel-level-file>',
    'honest_scenarios':catalog,'cut_judgments':'cut-judgments.json',
    'terminal_judgments':'terminal-judgments.json','control_judgments':'control-judgments.json',
    'control_provenance':'declared-faulty-answer-provenance.json',
    'transcript':'validation.txt','artifact_checksums':'SHA256SUMS','unresolved_discrepancies':[],
})
print('Duel: repeat captures, exact retained bodies, strict replay and every complete-row prefix passed',flush=True)
print('Duel: all full/cut account objects match the immutable answer key; every historical cut obligation, attempt origin and first quorum is fixed',flush=True)
print('Duel: closed-answer/admission boundary, covered no-choice inventory, complete outcomes, empty horizon and declared faulty-answer judgments passed',flush=True)
print('Duel: accepted account interpretation and four judgment hashes preserved; retained inputs and validation source unchanged',flush=True)
PY_DUEL
