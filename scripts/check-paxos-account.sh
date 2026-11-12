#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
evidence=target/paxos-account-evidence
if [[ $# -ne 2 ]]; then
  echo "usage: $0 <retained-evidence-directory> <interpretation-file>" >&2
  exit 2
fi
retained=$1
interpretation=$2
if [[ "${QUORUM_ACCOUNT_LOGGED:-0}" != 1 ]]; then
  rm -rf -- "$evidence"
  mkdir -p "$evidence"
  QUORUM_ACCOUNT_LOGGED=1 bash scripts/check-paxos-account.sh "$retained" "$interpretation" 2>&1 | tee "$evidence/validation.txt"
  python3 - "$evidence" <<'PY_HASHES'
from pathlib import Path
import hashlib, sys
root = Path(sys.argv[1])
files = sorted(path for path in root.rglob('*') if path.is_file() and path.name != 'SHA256SUMS')
(root/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}\n' for path in files))
print(f'Account artifacts: {len(files)} files covered by {root}/SHA256SUMS')
PY_HASHES
  exit 0
fi

# Existing replay remains a separate regression check. Its results never feed
# the independent account or serve as the oracle for a protocol judgment.
bash scripts/check-calm.sh
python3 - "$retained" "$interpretation" <<'PY_ACCOUNT'
from collections import Counter
from pathlib import Path
import hashlib, json, os, subprocess, sys

root = Path('target/paxos-account-evidence')
retained = Path(sys.argv[1]).resolve()
level = Path(sys.argv[2]).resolve()
review_directory = os.environ.get('QUORUM_ACCOUNT_REVIEW_EVIDENCE')
review = Path(review_directory).resolve() if review_directory else None
audit_directory = os.environ.get('QUORUM_ACCOUNT_AUDIT_EVIDENCE')
audit = Path(audit_directory).resolve() if audit_directory else None
binary = Path('target/debug/quorum').resolve()
level_hash = 'd8886526fbaa53326c66a9b282a78f7b71de63d4f877bea3de65ebefaf0866d3'
base = '97c7ad0315f5bb7dcb06ebb29a7d05e9ac347bfe'
sha = lambda data: hashlib.sha256(data).hexdigest()

def value_equal(left, right):
    # Match serde_json::Value, not JSON spelling or Python's int/float/bool coercion.
    if type(left) is not type(right):
        return False
    if isinstance(left, list):
        return len(left)==len(right) and all(value_equal(a,b) for a,b in zip(left,right))
    if isinstance(left, dict):
        return left.keys()==right.keys() and all(value_equal(left[key],right[key]) for key in left)
    return left == right

def value_integer(text):
    value = int(text)
    return value if -(1<<63) <= value < (1<<64) else float(text)

def command(*args):
    return subprocess.run(args, check=True, stdout=subprocess.PIPE).stdout

def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)+'\n')

assert sha(level.read_bytes()) == level_hash, 'The commissioned interpretation changed'
head = command('git', 'rev-parse', 'HEAD').decode().strip()
assert head == base, f'Expected fixed base {base}, found {head}'
retained_manifest = []
for line in (retained/'SHA256SUMS').read_text().splitlines():
    digest, name = line.split('  ', 1)
    assert sha((retained/name).read_bytes()) == digest, f'Retained evidence changed: {name}'
    retained_manifest.append((digest, name))
(root/'retained-inputs.sha256').write_text(''.join(f'{digest}  {name}\n' for digest, name in retained_manifest))
review_manifest = []
if review is not None:
    for line in (review/'SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        assert sha((review/name).read_bytes()) == digest, f'Review evidence changed: {name}'
        review_manifest.append((digest, name))
    (root/'review-inputs.sha256').write_text(''.join(f'{digest}  {name}\n' for digest, name in review_manifest))
audit_manifest = []
audit_manifests = {}
if audit is not None:
    for filename in ('SHA256SUMS', 'SHA256SUMS.postaccept', 'SHA256SUMS.timing'):
        path = audit/filename
        if filename != 'SHA256SUMS' and not path.exists():
            continue
        data = path.read_bytes()
        audit_manifests[filename] = sha(data)
        for line in data.decode().splitlines():
            digest, name = line.split('  ', 1)
            assert sha((audit/name).read_bytes()) == digest, f'Audit evidence changed: {name}'
            audit_manifest.append((digest, name))
    (root/'audit-inputs.sha256').write_text(''.join(f'{digest}  {name}\n' for digest, name in audit_manifest))

line_keys = {'line', 'first_line', 'boundary_line', 'desire_line', 'close_line', 'attempt_line',
    'result_line', 'save_line', 'first_quorum_line', 'request_dispatch_line', 'parent_input_line',
    'send_attempt_line', 'send_result_line', 'send_desire_line', 'allocation_line', 'first_desire_line',
    'input_line', 'first_relevant_line', 'blocking_refusal_line', 'header_line',
    'report_dispatch_line', 'send_admission_line'}
line_lists = {'support', 'counterevidence', 'occurrence_lines', 'dispatch_lines', 'drop_lines',
    'save_lines', 'quorum_lines', 'report_lines', 'lines', 'conflict_lines',
    'report_dispatch_lines', 'frozen_report_dispatch_lines'}
verdicts = {'established', 'violated', 'pending', 'indeterminate'}

def audit_shape(report, data):
    assert report['schema_version'] == 1
    assert report['source_sha256'] == sha(data)
    assert set(('choices','obligations','claims','attempts')) <= report.keys()
    for section in ('choices','obligations','claims','attempts'):
        assert isinstance(report[section], list)
    limit = report['world']['valid_rows']
    findings = {finding['id'] for finding in report['interpretation_findings']}
    def visit(value):
        if isinstance(value, list):
            for item in value: visit(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                # These fields contain arbitrary client JSON, not report schema.
                if key in ('value','values','entry','message','record','error'): continue
                if key in line_keys and item is not None:
                    assert isinstance(item, int) and not isinstance(item, bool) and 1 <= item <= limit, (key,item,limit)
                elif key in line_lists:
                    assert all(isinstance(line,int) and not isinstance(line,bool) and 1 <= line <= limit for line in item), (key,item,limit)
                elif key == 'gaps':
                    assert all(gap in findings for gap in item), item
                elif key == 'verdict':
                    assert item in verdicts, item
                visit(item)
    visit(report)
    rows = [json.loads(line, parse_int=value_integer) for line in data.splitlines()[:limit]]
    for choice in report['choices']:
        members = set()
        assert len(choice['first_support']) == choice['quorum']
        assert choice['first_quorum_line'] == max(witness['save_line'] for witness in choice['first_support'])
        for witness in choice['first_support']:
            row = rows[witness['save_line']-1]
            assert (row['what'],row['why'],row['outcome']) == ('result','save','fulfilled')
            assert row['node'] == witness['acceptor'] and row['node'] not in members
            members.add(row['node'])
            record = row['record']
            assert record['record'] == 'accepted'
            assert record['slot'] == choice['slot'] and record['ballot'] == choice['ballot']
            assert value_equal(record['value'], choice['value'])
    for claim in report['claims']:
        boundary = claim['effect']['attempt_line']
        if boundary is None:
            assert claim['local_basis'] is None and claim['transitive_support'] is None
            assert not claim['choice_at_boundary'] and not claim['choices_seen_later']
        else:
            assert rows[boundary-1]['what'] == 'attempt'
            for key in ('local_basis','transitive_support'):
                assert claim[key]['boundary_line'] == boundary
                assert all(line <= boundary for line in claim[key]['support']+claim[key]['counterevidence'])
    for proposition in report['obligations']:
        assert all(line <= proposition['boundary_line'] for line in proposition['support']+proposition['counterevidence'])
        unavailable = proposition.get('unavailable_premises', [])
        if proposition['verdict'] == 'indeterminate':
            assert proposition['gaps'] or unavailable, ('unlocated uncertainty', proposition)
        for dependency in unavailable:
            assert dependency['premise'] and (dependency['support'] or dependency['counterevidence'] or dependency['gaps']), dependency
            assert all(line <= proposition['boundary_line'] for line in dependency['support']+dependency['counterevidence']), dependency

catalog = []
reports = {}

def examine(path, destination, label, immutable):
    data = path.read_bytes()
    first = command(str(binary), 'paxos-account', str(path), '--json')
    second = command(str(binary), 'paxos-account', str(path), '--json')
    assert first == second, f'Non-deterministic JSON report: {label}'
    report = json.loads(first)
    audit_shape(report, data)
    if immutable:
        assert report['world']['error'] is None, (label, report['world']['error'])
        bad = [p for p in report['obligations'] if p['verdict'] == 'violated']
        assert not bad, (label, bad)
    destination.parent.mkdir(parents=True, exist_ok=True)
    Path(f'{destination}.report.json').write_bytes(first)
    human = command(str(binary), 'paxos-account', str(path))
    assert human == command(str(binary), 'paxos-account', str(path)), f'Non-deterministic human report: {label}'
    Path(f'{destination}.human.txt').write_bytes(human)
    reports[label] = report
    catalog.append({'source':label,'immutable_retained_input':immutable,'source_sha256':report['source_sha256'],
        'valid_rows':report['world']['valid_rows'],'world_error':report['world']['error'],
        'interpretation_findings':report['interpretation_findings'],
        'choices':report['choices'],
        'obligation_verdicts':dict(sorted(Counter(p['verdict'] for p in report['obligations']).items())),
        'violations':[p for p in report['obligations'] if p['verdict']=='violated'],
        'claims':[{'id':c['id'],'kind':c['kind'],'node':c['node'],'slot':c['slot'],'ballot':c['ballot'],'value':c['value'],
            'assertion':c['assertion'],'effect':c['effect'],'local_basis':c['local_basis'],
            'transitive_support':c['transitive_support'],'choice_at_boundary':c['choice_at_boundary'],
            'choices_seen_later':c['choices_seen_later'],'unsupported_by_tape':c['unsupported_by_tape']} for c in report['claims']],
        'attempts':[{'id':a['id'],'proposer':a['proposer'],'slot':a['slot'],'ballot':a['ballot'],'value':a['value'],
            'origin':a['origin'],'exact_choice':a['exact_choice'],'same_value_elsewhere':a['same_value_elsewhere'],
            'slot_witnessed_values':a['slot_witnessed_values'],'resolution':a['resolution']} for a in report['attempts']],
        'json_report':f'{destination}.report.json','human_report':f'{destination}.human.txt'})
    return report

for path in sorted(root.glob('*.jsonl')):
    examine(path, root/path.stem, f'counterexample/{path.name}', False)
for directory in ('calm-accepted','adoption-probe'):
    for path in sorted((retained/directory).glob('*.jsonl')):
        examine(path, root/'retained'/directory/path.stem, f'{directory}/{path.name}', True)
if review is not None:
    for path in sorted(review.glob('*.jsonl')):
        examine(path, root/'review'/path.stem, f'review/{path.name}', False)
    readable = reports['review/readable-inactive-campaign.jsonl']
    unreadable = reports['review/unreadable-inactive-campaign.jsonl']
    contradictory = reports['review/continued-contradictory-reports.jsonl']
    assert readable['source_sha256'] == '92c2efa444cadc24e23633da430e392f385521fb99e207b5b896ab10264c7689'
    assert unreadable['source_sha256'] == '4858e8079d52548d9b7512a451ef43dedab28264b70804d3407086392f4dd8d2'
    assert contradictory['source_sha256'] == '0a2093532677074eacfbdb301e96be7a8ce023191d40b01d402e82ef1d3576bc'
    assert all(r['world']['error'] is None for r in (readable, unreadable, contradictory))
    def obligation(report, name, boundary):
        return next(p for p in report['obligations'] if p['proposition']==name and p['boundary_line']==boundary)
    assert readable['attempts'][0]['origin']['kind'] == 'fresh'
    assert obligation(readable,'phase2_gate',72)['verdict'] == 'established'
    assert unreadable['attempts'][0]['origin']['kind'] == 'undetermined'
    for name, boundary in (('promise_report_handling',65),('promise_report_handling',71),('phase2_gate',72)):
        p = obligation(unreadable,name,boundary)
        assert p['verdict']=='indeterminate' and p['gaps'], p
    for r in (readable, unreadable):
        assert any(p['proposition']=='activation' and p['verdict']=='violated' for p in r['obligations'])
    first_quorum = next(o for o in contradictory['observations'] if o['kind']=='first_promise_quorum')
    assert first_quorum['dispatch_lines']==[113,116,119] and first_quorum['first_quorum_determined']
    assert not first_quorum['adoption_map_determined'] and first_quorum['adoption_map'] is None
    assert all(a['origin']['kind']=='undetermined' for a in contradictory['attempts'])
    assert all(p['verdict']=='established' for p in contradictory['obligations'] if p['proposition']=='phase2_gate')
    assert all(p['verdict']=='indeterminate' for p in contradictory['obligations'] if p['proposition'] in ('adoption','fifo'))
    assert obligation(contradictory,'proposal_drain',120)['verdict']=='indeterminate'
    drain = obligation(contradictory,'proposal_drain',120)
    assert {113,119} <= set(drain['counterevidence'])
    assert any(p['premise']=='first_quorum_map' and {113,119} <= set(p['counterevidence'])
        for p in drain['unavailable_premises'])
    for p in contradictory['obligations']:
        if p['proposition'] in ('adoption','fifo','fresh_origin_count') and p['verdict']=='indeterminate':
            assert {113,119} <= set(p['counterevidence']), p
            assert p['unavailable_premises'], p
    assert not any(p['proposition']=='proposal_drain' and p['boundary_line']==133 for p in contradictory['obligations'])
    assert obligation(contradictory,'promise_report_handling',133)['verdict']=='violated'
    assert not contradictory['choices'] and not contradictory['claims']
    continued = reports['counterexample/campaign-contradiction-later-knowledge.jsonl']
    assert [(c['slot'],c['value'],c['first_quorum_line']) for c in continued['choices']] == [(0,'higher',193),(1,'waiting-client',226)]
    assert [(c['slot'],c['local_basis']['verdict'],c['transitive_support']['verdict']) for c in continued['claims']] == [(0,'established','established'),(1,'established','established')]
    save_json(root/'review-judgments.json', {
        'readable_inactive':{'origin':readable['attempts'][0]['origin'],'gate':obligation(readable,'phase2_gate',72)},
        'unreadable_inactive':{'origin':unreadable['attempts'][0]['origin'],'gate':obligation(unreadable,'phase2_gate',72)},
        'contradictory_first_encounter':first_quorum,
        'extended_counterexample_choices':continued['choices'],'extended_counterexample_claims':continued['claims'],
        'contradictory_map_obligations':[p for p in contradictory['obligations'] if p['proposition'] in
            ('adoption','fifo','phase2_gate','proposal_drain','promise_report_handling')],
    })
if audit is not None:
    for _, name in audit_manifest:
        if name.endswith('.jsonl'):
            path = audit/name
            destination = root/'audit'/Path(name).with_suffix('')
            examine(path, destination, f'audit/{name}', False)
    learned = reports['audit/tapes/s1.jsonl']
    mismatch = reports['audit/tapes/s2.jsonl']
    assert learned['source_sha256']=='f481240a5d2322bd8c0b32f0916f0188f093f031418c1268b196c505a9263e81'
    assert mismatch['source_sha256']=='32d28e9ffa9a3c276a9121f59e3a8699d817526481771ed23001f4e421ce5b39'
    for label, r in reports.items():
        if label.startswith('audit/'):
            assert r['world']['error'] is None, (label,r['world']['error'])
        if label=='audit/tapes/s1.jsonl' or label.startswith('audit/tapes/rand/'):
            assert not any(p['verdict']=='violated' for p in r['obligations']), (label,[p for p in r['obligations'] if p['verdict']=='violated'])
    receiver = next(p for p in learned['obligations'] if p['proposition']=='receiver_conformance' and p['boundary_line']==336)
    assert receiver['verdict']=='established'
    announcements = [c for c in learned['claims'] if c['kind']=='chosen' and c['node']==3 and c['slot']==0 and c['ballot']==[2,3]]
    assert len(announcements)==4
    assert all(c['local_basis']['verdict']==c['transitive_support']['verdict']=='established' for c in announcements)
    assert len([c for c in learned['claims'] if c['kind']=='commit' and c['node']==3 and c['slot']==0])==1
    assert [(c['slot'],c['value']) for c in mismatch['choices']]==[(0,'x'),(0,'y')]
    config_failures = [p for p in mismatch['obligations'] if p['proposition']=='config_agreement' and p['verdict']=='violated']
    assert len(config_failures)==5 and all(p['subject']['other']=='header.acceptors' for p in config_failures)
    explanation = next(p for p in mismatch['obligations'] if p['proposition']=='conflict_explanation' and p['boundary_line']==299)
    assert explanation['verdict']=='established'
    explained = next(o for o in mismatch['observations'] if o.get('proposition_id')==explanation['id'])
    assert explained['account_deficiency'] is False
    assert set(explained['explanatory_obligations'])=={p['id'] for p in config_failures if p['subject']['node'] in explained['relevant_nodes']}
    assert any(p['proposition']=='no_conflicting_choices' and p['boundary_line']==299 and p['verdict']=='violated' for p in mismatch['obligations'])
    zero_control = reports['audit/tapes/zero-control.jsonl']
    zero_signed = reports['audit/tapes/zero-signed.jsonl']
    assert zero_control['source_sha256']=='0bdde7faf50521299848206188ac8352ae005629be7bb3a6869ac78f12da6aa2'
    assert zero_signed['source_sha256']=='507ffa0f96c9331e583997dcd50730002e183c878075246814f76bc93544232b'
    for r in (zero_control, zero_signed):
        assert len(r['choices'])==1 and r['choices'][0]['id']=='choice:54'
        assert r['choices'][0]['first_quorum_line']==54 and r['choices'][0]['quorum']==2
        assert [w['acceptor'] for w in r['choices'][0]['first_support']]==[1,2]
        assert len(r['attempts'])==1 and r['attempts'][0]['exact_choice']=='choice:54'
    assert [(p['proposition'],p['boundary_line'],p['verdict']) for p in zero_control['obligations']] == [
        (p['proposition'],p['boundary_line'],p['verdict']) for p in zero_signed['obligations']]
    save_json(root/'audit-judgments.json', {
        'learned_then_new_quorum':{'receiver':receiver,'new_announcements':announcements},
        'header_universe_mismatch':{'choices':mismatch['choices'],'config_failures':config_failures,
            'explanation':explanation,'observation':explained},
        'honest_samples':[label for label in reports if label.startswith('audit/tapes/rand/')],
        'signed_zero_identity':{'control_choice':zero_control['choices'][0],
            'signed_choice':zero_signed['choices'][0],'exact_choice':zero_signed['attempts'][0]['exact_choice']},
    })
    if 'SHA256SUMS.postaccept' in audit_manifests:
        timing = {}
        for name, expected_hash in (
            ('early', '078a0e8b2cac569cb27a14141c6ea592d4106a3539053b61d3022918af733d79'),
            ('control', '6bc492796296e242b46416339952ee7596dc8b542c90eff54d73ced11df63b0a'),
        ):
            r = reports[f'audit/tapes3/{name}.jsonl']
            assert r['source_sha256']==expected_hash
            assert not any(p['verdict']=='violated' for p in r['obligations']), r['obligations']
            rows = [json.loads(line) for line in (audit/f'tapes3/{name}.jsonl').read_bytes().splitlines()]
            proposal, output = next((line,row) for line,row in enumerate(rows,1)
                if row['what']=='desire' and row['why']=='send' and row['node']==0
                and row['message']=={'type':'accept','ballot':[2,0],'slot':0,'value':'x'})
            issued = next(line for line,row in enumerate(rows,1)
                if row['what']=='close' and row['input_id']==output['parent_input_id'])
            replies = [line for line,row in enumerate(rows,1)
                if row['what']=='dispatch' and row['why']=='message' and row['to']==0
                and row['message']=={'type':'accepted','ballot':[2,0],'slot':0,'value':'x'}]
            early = [line for line in replies if line < issued]
            genuine = [line for line in replies if line > issued]
            assert len(genuine)==2 and early==([44] if name=='early' else [])
            receivers = [p for p in r['obligations'] if p['proposition']=='receiver_conformance']
            assert all(p['verdict']=='established' for p in receivers), receivers
            for reply in genuine:
                p = next(p for p in receivers if p['subject']['input_line']==reply)
                assert {proposal,issued,reply} <= set(p['support']), p
                assert not set(early).intersection(p['support']), p
            if name=='early':
                assert (proposal,issued,genuine)==(81,84,[125,152])
                assert {126,155,156} <= {p['boundary_line'] for p in receivers}
            assert not r['choices']
            assert [c['kind'] for c in r['claims']]==['commit','chosen']
            for claim in r['claims']:
                assert claim['local_basis']['verdict']=='established'
                assert claim['transitive_support']['verdict']=='indeterminate'
                assert set(early) <= set(claim['local_basis']['support'])
            timing[name] = {'source_sha256':r['source_sha256'],'proposal_desire_line':proposal,
                'proposal_close_line':issued,'early_dispatch_lines':early,'counted_dispatch_lines':genuine,
                'receivers':receivers,'claims':r['claims']}
        save_json(root/'postaccept-judgments.json', timing)
    if 'SHA256SUMS.timing' in audit_manifests:
        timing_controls = {}
        expected_boundaries = {
            'prequorum': [51,54],
            'again': [45,126,155,156,171],
            'sametick_before': [78,126,155,156,171],
            'sametick_after': [90,128,129,144],
            'delayed': [118,147,148,163],
            'unread_pre': [126,155,156,171],
            'unread_post': [144,173,174,189],
        }
        for name, boundaries in expected_boundaries.items():
            r = reports[f'audit/tapes4/{name}.jsonl']
            receivers = [p for p in r['obligations'] if p['proposition']=='receiver_conformance']
            assert [p['boundary_line'] for p in receivers]==boundaries, (name,receivers)
            for p in receivers:
                uncertain = name=='unread_post' and p['boundary_line']==144
                assert p['verdict']==('indeterminate' if uncertain else 'established'), (name,p)
                assert p['gaps']==(['send:8'] if uncertain else []), (name,p)
            timing_controls[name] = {'source_sha256':r['source_sha256'],
                'receivers':receivers,'claims':r['claims']}
        r = reports['audit/tapes4/unread_client_post.jsonl']
        assert r['source_sha256']=='d1437101bf8a13e7a34a9a8e320c42701a3ed040f789d04ca72dfd69be704c4b'
        assert any(f['id']=='client:31' for f in r['interpretation_findings'])
        receivers = [p for p in r['obligations'] if p['proposition']=='receiver_conformance']
        assert any(p['boundary_line']==138 for p in receivers)
        assert all(p['verdict']=='established' and not p['gaps'] for p in receivers), receivers
        assert not r['choices']
        assert [c['kind'] for c in r['claims']]==['commit','chosen']
        assert all(c['local_basis']['verdict']=='established'
            and c['transitive_support']['verdict']=='indeterminate' for c in r['claims'])
        timing_controls['unread_client_post'] = {'source_sha256':r['source_sha256'],
            'receivers':receivers,'claims':r['claims'],'interpretation_findings':r['interpretation_findings']}
        save_json(root/'timing-judgments.json', timing_controls)

calm = reports['calm-accepted/calm.a.jsonl']
cut = reports['calm-accepted/calm.chosen-before-known.jsonl']
owner = reports['calm-accepted/receiver-authoritative-ballot-owner.jsonl']
adoption = reports['adoption-probe/probe.a.jsonl']
assert calm['source_sha256'] == '49d1922b3736724b22e011f66aae48acc31402f76d160ff5c81ced4d77a71f9a'
assert cut['source_sha256'] == '515f1883f74ceb0b4cf0f61a6a0e894ce64a61e554d62da8ad9f977bb59d5cf7'
assert owner['source_sha256'] == 'b8cdec23c01dc71548d08d2d58d4ad1e6554dedfcfe7db403f19626e8362348f'
assert adoption['source_sha256'] == '9b287059287e0655305284da5174ce5a7cea99d972e1df9fa4281b7177792b92'
assert [(c['slot'],c['ballot'],c['value'],c['first_quorum_line']) for c in calm['choices']] == [(0,[1,0],'alpha',123),(1,[1,0],'beta',150),(2,[1,0],'gamma',177)]
assert len(cut['choices']) == 1 and cut['choices'][0]['first_quorum_line'] == 123 and not cut['claims']
assert all(c['local_basis']['verdict'] == c['transitive_support']['verdict'] == 'established' for c in calm['claims']+adoption['claims'])
assert len(owner['claims']) == 1
owner_claim = owner['claims'][0]
assert (owner_claim['node'],owner_claim['kind'],owner_claim['effect']['attempt_line']) == (1,'commit',44)
assert owner_claim['local_basis']['verdict'] == 'established' and owner_claim['transitive_support']['verdict'] == 'indeterminate'
assert not owner['choices']
assert any(p['proposition']=='receiver_conformance' and p['boundary_line']==44 and p['verdict']=='established' for p in owner['obligations'])
assert [(c['slot'],c['ballot'],c['value'],c['first_quorum_line']) for c in adoption['choices']] == [(0,[1,0],'x',99),(0,[2,3],'x',314),(1,[2,3],'y',341)]
x = next(a for a in adoption['attempts'] if a['proposer']==3 and a['slot']==0)
y = next(a for a in adoption['attempts'] if a['proposer']==3 and a['slot']==1)
z = next(a for a in adoption['attempts'] if a['proposer']==0 and a['slot']==1)
assert x['origin']['kind']=='adopted' and x['origin']['allocation_line']==256 and x['origin']['client_count'] is None
assert y['origin']['kind']=='fresh' and y['origin']['allocation_line']==256 and y['origin']['client_count']=={'available':1,'allocated':1,'certain':True}
assert z['value']=='z' and z['origin']['kind']=='fresh' and z['origin']['allocation_line']==237
assert z['exact_choice'] is None and z['resolution']['state']=='unresolved' and z['resolution']['whole_world_empty'] and not z['resolution']['world_open']
assert sorted(reply['line'] for member in z['replies'] for reply in member['received'] if reply['kind']=='nack') == [354,357,360]
assert sorted(c['effect']['attempt_line'] for c in adoption['claims'] if c['node']==3 and c['kind']=='commit') == [381,402]
assert not any(c['node']==0 and c['slot']==1 and c['value']=='z' for c in adoption['claims'])
old_x = next(a for a in adoption['attempts'] if a['proposer']==0 and a['slot']==0)
assert x['slot_witnessed_values']==old_x['slot_witnessed_values']==y['slot_witnessed_values']==[]
assert z['slot_witnessed_values']==['choice:341']
assert x['exact_choice']=='choice:314' and old_x['exact_choice']=='choice:99'
assert x['same_value_elsewhere']==['choice:99'] and old_x['same_value_elsewhere']==['choice:314']
ancestry = x['origin']['acceptance_ancestry']
assert ancestry['verdict']=='established' and ancestry['selection']['verdict']=='established'
assert ancestry['frozen_report_dispatch_lines']==[246,249]
assert {save['result_line'] for path in ancestry['paths'] for save in path['matching_saves']}=={90,99}
assert {90,99,246,249} <= set(x['origin']['support']) and 270 not in x['origin']['support']

save_json(root/'judgments.json', catalog)
save_json(root/'interpretation-gap-dependencies.json', calm['interpretation_gap_dependencies'])
summary = [
    {'case':'Calm','source_sha256':calm['source_sha256'],'choice_lines':[123,150,177],'claims':len(calm['claims']),
     'judgment':'All observed choices, covered conformance/workload obligations and admitted knowledge proofs are established.'},
    {'case':'Calm chosen-before-known cut','source_sha256':cut['source_sha256'],'choice_line':123,'claims':0,
     'judgment':'The historical certificate already exists; local reply work is still open and no claim has been admitted.'},
    {'case':'Unsupported authoritative owner','source_sha256':owner['source_sha256'],'claim_admission':44,
     'local_basis':'established','transitive_support':'indeterminate','receiver_conformance':'established',
     'judgment':'No sender Chosen claim or sender contract verdict is invented for the undeclared script.'},
    {'case':'Real adoption history','source_sha256':adoption['source_sha256'],'choice_lines':[99,314,341],
     'allocation_lines':{'fresh_z':237,'adopted_x_and_fresh_y':256},'proposer_3_commit_admissions':[381,402],
     'judgment':'The immutable first quorum at 246/249 selects x, joined to prior fulfilled Saves 90/99; late Promise 270 cannot repair that basis. Adoption consumes no client allowance. x/old-x/y have no different-value slot links; z links only to y at 341 and remains unresolved after the world empties.'},
]
save_json(root/'retained-judgments.json', summary)
source_files = sorted({Path('Cargo.toml'),Path('Cargo.lock'),Path('build.rs'),Path('docs/tape.md'),
    *Path('src').rglob('*.rs'),*Path('tests').glob('*.rs'),*Path('scripts').glob('*.sh')})
manifest = ''.join(f'{sha(path.read_bytes())}  {path}\n' for path in source_files)
(root/'source-manifest.sha256').write_text(manifest)
status = command('git','status','--porcelain=v1','--untracked-files=all').decode()
save_json(root/'manifest.json', {'base_revision':head,'git_status_porcelain':status,
    'source_files':[str(path) for path in source_files],'source_manifest_sha256':sha(manifest.encode()),
    'interpretation':str(level),'interpretation_sha256':level_hash,'retained_root':str(retained),
    'retained_manifest_sha256':sha((retained/'SHA256SUMS').read_bytes()),'verified_retained_files':len(retained_manifest),
    'review_root':str(review) if review is not None else None,'verified_review_files':len(review_manifest),
    'review_tapes':sum(case['source'].startswith('review/') for case in catalog),
    'audit_root':str(audit) if audit is not None else None,'verified_audit_files':len(audit_manifest),
    'audit_tapes':sum(case['source'].startswith('audit/') for case in catalog),
    'audit_manifests_sha256':audit_manifests,
    'account_version':calm['account_version'],'schema_version':calm['schema_version'],
    'rustc':command('rustc','--version').decode().strip(),'cargo':command('cargo','--version').decode().strip(),
    'reproduce':'[QUORUM_ACCOUNT_REVIEW_EVIDENCE=review-directory] [QUORUM_ACCOUNT_AUDIT_EVIDENCE=audit-directory] bash scripts/check-paxos-account.sh <retained-evidence-directory> <interpretation-file>',
    'reports':len(catalog),'retained_tapes':sum(case['immutable_retained_input'] for case in catalog),
    'unresolved_discrepancies':[]})
print(f'Independent account: {len(catalog)} tapes, byte-stable JSON and human reports; original hashes and one-based evidence joins checked')
print(f'Retained evidence: {len(retained_manifest)} original files unchanged; all covered retained obligations non-violated')
if review is not None:
    print(f'Review evidence: {len(review_manifest)} original files unchanged; three repaired review scenarios checked')
if audit is not None:
    print(f'Audit evidence: {len(audit_manifest)} original files unchanged; learned-slot, header-mismatch, signed-zero pair and five fixed honest samples checked')
    if 'SHA256SUMS.postaccept' in audit_manifests:
        print('Post-acceptance evidence: early/control pair checked; receiver timing excludes early replies without filtering knowledge')
    if 'SHA256SUMS.timing' in audit_manifests:
        print('Timing evidence: seven immutable boundary controls preserved; malformed client excluded from receiver gaps')
print(f'Fixed source base: {head}; source manifest SHA-256 {sha(manifest.encode())}')
print(json.dumps(summary,indent=2))
PY_ACCOUNT
