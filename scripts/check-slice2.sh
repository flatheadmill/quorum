#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
evidence=target/slice2-evidence
if [[ "${QUORUM_SLICE2_LOGGED:-0}" != 1 ]]; then
  rm -rf -- "$evidence"
  mkdir -p "$evidence"
  QUORUM_SLICE2_LOGGED=1 bash scripts/check-slice2.sh 2>&1 | tee "$evidence/validation.txt"
  exit 0
fi
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked
for scenario in raft ping-pong; do
  target/debug/quorum capture "$scenario" "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.capture.json"
  target/debug/quorum capture "$scenario" "$evidence/$scenario.b.jsonl" > "$evidence/$scenario.capture-again.json"
  cmp "$evidence/$scenario.a.jsonl" "$evidence/$scenario.b.jsonl"
  target/debug/quorum inspect "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.inspect.json"
  target/debug/quorum replay "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.replay.json"
  target/debug/quorum prefixes "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.prefixes.json"
  jq -c 'select(.what == "result" and .why == "save" and .outcome == "fulfilled") | {node, record}' "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.saves.jsonl"
  jq -c 'select(.what == "attempt" and .why == "send") | {node, parent_input_id, desire_id, message_id, to, message}' "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.sends.jsonl"
  jq -c 'select(.what == "finalize") | .account' "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.pending.json"
  jq -c --argjson node 0 'select((.what == "dispatch" and .to == $node) or (.what == "attempt" and .node == $node))' "$evidence/$scenario.a.jsonl" > "$evidence/$scenario.node0.jsonl"
  printf '%s: byte-identical captures; projection, replay, prefixes and jq passed\n' "$scenario"
  shasum -a 256 "$evidence/$scenario.a.jsonl"
  cat "$evidence/$scenario.replay.json" "$evidence/$scenario.prefixes.json"
done
target/debug/quorum vote "$evidence/raft.a.jsonl" > "$evidence/raft.vote.json"
cat "$evidence/raft.vote.json"
git diff --check
python3 - <<'PY_MANIFEST'
from pathlib import Path
import hashlib
files = [Path('Cargo.toml'), Path('Cargo.lock'), Path('build.rs')]
files += sorted(Path('src').glob('*.rs')) + sorted(Path('tests').glob('*.rs'))
files += [Path('docs/tape.md'), Path('scripts/check-slice2.sh')]
manifest = ''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p}\n' for p in files)
Path('target/slice2-evidence/source-manifest.sha256').write_text(manifest)
PY_MANIFEST
