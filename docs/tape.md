# Complete simulator tape — level 003

The simulator writes and flushes each complete JSONL row before projecting its
world effect into memory. The file is authoritative; retained rows, per-node
records, chosen maps, and network queues are execution indexes. A successful
flush is this simulator's commitment boundary. It is **not** an `fsync`, a
physical WAL, or a promise to recover a torn write.

The deterministic simulator assumes its host remains stable for the duration
of a run. It owes logical write ordering and complete flushed rows, not survival
across power loss, kernel failure, or storage-device failure. `fsync`, journal
recovery, and torn-write repair belong to a future production storage adapter;
they are deliberately not requirements of the pedagogical bench.

A turn has this order:

```text
dispatch -> desire* -> close(count) -> (attempt -> result)*
```

The dispatch contains the complete `Input` before `receive` starts. Context
collects only a private vector. The entire answer, including an empty answer,
closes before any effect is attempted. A refused third effect leaves the fourth
desire visible and unreached. An inhabitant panic leaves an open invocation;
that simulator cannot dispatch another input after a caught panic.

The serializer only flattens **world-owned** fields. Algorithm messages, saved
records, and commit values stay intact under `message`, `record`, and `entry`.
No algorithm key is reserved or renamed. Saved null is a value; a missing
`record` is invalid. Duplicate JSON object keys are rejected at every depth.

## Build and run

```sh
cargo test --locked
cargo run --locked -- capture raft target/raft.jsonl
cargo run --locked -- inspect target/raft.jsonl
cargo run --locked -- prefixes target/raft.jsonl
cargo run --locked -- replay target/raft.jsonl
cargo run --locked -- vote target/raft.jsonl
```

`quorum raft`, `quorum ping-pong`, and `quorum calm` also stream directly to stdout. `capture`
writes its tape path and prints the final projection summary on stdout.
`inspect`, `prefixes`, `replay`, and `vote` consume the file without modifying it.
Errors go to stderr with a failing exit status. Reports hash the original bytes
and cite one-based JSONL line numbers; the row sequence itself is zero-based.
The CLI's `capture` output path is explicitly overwritten if it exists.

Run `bash scripts/check-slice2.sh` for the reproducible evidence set under
`target/slice2-evidence/`. In a restricted environment, Cargo's cache can be
placed in writable storage, for example:

```sh
CARGO_HOME=/private/tmp/quorum-slice2-cargo bash scripts/check-slice2.sh
```

The script replaces its generated evidence directory on each run, writes its
validation transcript and source manifest there, and captures both scenarios twice, compares their bytes, interprets all
row-boundary prefixes, replays both, checks Raft votes, and runs representative
`jq` queries. Generated artifacts are local, ignored build output; they are not
published.

## Multi-Paxos Calm

```sh
cargo run --locked -- capture calm target/calm.jsonl
cargo run --locked -- inspect target/calm.jsonl
cargo run --locked -- replay target/calm.jsonl
cargo run --locked -- prefixes target/calm.jsonl
CARGO_HOME=/private/tmp/quorum-slice2-cargo bash scripts/check-calm.sh
```

Calm uses nodes 0, 1 and 2 as acceptors, learners and eligible proposers, with
only node 0 active. Node configuration records those role memberships,
`active_proposer`, `from_slot: 0`, `initial_round: 1`, and
`initial_state: "empty"`. Quorum is derived from the acceptor set. The header
and node acceptor sets come from the same scenario membership. A null active
proposer disables campaigning in receiver fixtures; it does not make an
otherwise configured proposer ineligible to send owned protocol messages.

The seed is 42, latency is 1 with no jitter, ticks occur every step, and the
no-progress threshold is 200. Three tick-zero client commands queue alpha,
beta and gamma. The first Tick starts one fixed-suffix Prepare campaign,
including self through the network. The first distinct Promise quorum
freezes adoption at tick 3 and issues the three Accept broadcasts without
another Prepare. Fulfilled acceptance quorums exist at tick 4; proposer
knowledge and claims follow at tick 5, and other learners claim at tick 6.
Tick 8 finalizes with reason `horizon` and an empty unfinished-work account.

`src/multi_paxos.rs` owns ballots, protocol parsing and transitions:
`start_campaign`/`prepare`/`promise` cover Phase 1,
`receive_promise` freezes the quorum, and `propose_waiting` interleaves
adopted slots with FIFO clients without consuming a client for an adoption.
All reports' slot/ballot values are retained until freeze so even a
lower-ballot contradiction cannot hide behind a higher report.
`accept` orders persistence before acknowledgment; `receive_accepted`
licenses Commit/Chosen from distinct matching replies. `learn` guards both
learning paths, including self-Chosen and the same value under a later ballot.
Commit carries the JSON value alone, including null. Claims may be sparse;
they are not in-order application. Nack never triggers retry or abandonment.

The evidence script replaces `target/calm-evidence/` and invokes
`scripts/check-slice2.sh`, which also replaces its own generated directory.
It runs formatting checks, all tests, clippy and build; retains the existing
Raft/Ping-Pong evidence; compares a fresh `capture calm` with a fresh direct
`calm` execution; then checks public replay, projection and every complete-row
prefix. Its outputs include:

- `calm.a.jsonl` and `calm.b.jsonl`: byte-identical complete captures.
- `calm.replay.json` and `calm.prefixes.json`: public validation reports.
- `calm.chosen-before-known.jsonl` and its inspection report: the original
  bytes through the second matching fulfilled acceptance for slot 0, before
  any Accepted dispatch or Commit desire. This cut is an interrupted drain,
  not a finalized scenario.
- `evidence-map.json`: one-based tape lines for campaign, freeze, choice cut,
  quorum/Commit/Chosen, self-Chosen suppression and finalization, plus source
  anchors and the implementation identity.
- `receiver-*.jsonl`, `.evidence.json`, `.replay.json`, and `.prefixes.json`:
  real-node/scripted-peer transition fixtures, including adoption/FIFO
  controls, stale/repeat precedence, Nack outcomes and required null fields.
- `source-manifest.sha256`, `tape-hashes.sha256`, and `validation.txt`.

Receiver peers are reconstructed from recorded test-local configuration by
`tests/multi_paxos.rs`; they deliberately have no public CLI factory. Each
fixture interprets every row prefix and strictly replays its closed portion.
Deliberate conflicting-value and malformed-input panics retain their open
invocation; their `.closed-prefix.jsonl` and replay report certify only the
preceding closed portion, not the panicking input.

The fixtures establish receiver behavior, not globally legal Paxos histories.
The choice cut is the declared workload's hand-worked protocol interpretation.
Neither these observations nor strict replay is the independent Certificate
Verifier; that verifier and Duel remain outside Calm.

## Schema 1

Every row has `seq`, `when`, `who`, `what`, and `why`. `when` is a logical tick.
`who` is `bench` for world facts or `node:N` for a dispatch/returned answer.
`(what, why)` selects an exact set of fields; unknown variants, missing fields,
extra fields, changed envelopes, impossible ancestry, duplicate identities,
effects before close, and continuation after refusal/finalization are rejected.

| `what`, `why` | Required fields after the spine |
| --- | --- |
| `header`, `configuration` | `schema`, `implementation`, `scenario`, `seed`, `clock`, `world`, `nodes`, `acceptors` |
| `advance`, `tick` | none; `when` advances by exactly one |
| `spawn` / `crash`, `node` | `node` |
| `recover`, `node` | `node`, `model: "retained_state"` |
| `partition`, `message` | `from`, `to`, `until` |
| `external`, `client` | `input_id`, `to`, `message` |
| `dispatch`, `client` | `input_id`, `to`, `message` |
| `dispatch`, `tick` | `input_id`, `to` |
| `dispatch`, `message` | `input_id` plus the envelope fields below |
| `arrive`, `message` | envelope fields |
| `drop`, `message` | envelope fields plus `reason` |
| `drop`, `client` | `input_id`, `to`, `message`, `reason` |
| `desire`, `send` / `save` / `commit` | `node`, `parent_input_id`, `desire_id`, operation fields below |
| `close`, `receive` | `node`, `input_id`, `count` |
| `attempt`, `send` / `save` / `commit` | desire fields; Send also assigns `message_id` |
| `result`, `send` / `save` / `commit` | attempt fields plus `outcome`, `reason`; Send also has `delay`, `send_tick`, `deliver_tick` |
| `deadlock`, `no_progress` | `ticks_since_progress` |
| `finalize`, `scenario` | `reason`, `account` |

Envelope fields are `message_id`, `parent_desire_id`, `from`, `to`, `message`,
`send_tick`, and `deliver_tick`. Operation fields are `to` and `message` for
Send; `record` for Save; and `slot` and `entry` for Commit. Attempt/result rows
retain the exact operation in their originating closed answer. A result has
`outcome: "fulfilled"` and `reason: null`, or a refused Commit has
`outcome: "refused"` and the agreement-policy reason. Current simulator Saves
complete synchronously. It does not model failed or asynchronous storage.

The header declares all node identities, algorithm names, node configuration,
and acceptor membership. World configuration fixes latency/jitter, tick interval,
and the no-progress threshold. Implementation identity is the crate/version,
full source revision, dirty status, and a SHA-256 of named source files plus
Cargo manifests/lock and the build script. The build captures this identity;
runtime capture does not consult Git. Building requires Git and the source
checkout metadata; a bare source export without them is not supported. The digest includes paths relative to the
crate and file bytes, never checkout paths or timestamps. Strict replay requires
that recorded identity to equal the running build. No hostname, capture time,
or fresh run UUID enters this deterministic tape. Optional capture metadata
belongs in a separate manifest keyed by the finished tape hash.

`finalize` is explicit; `run(until_tick)` does not end the scenario. Its reason
is `horizon`, `no_progress`, `refused`, or `interrupted`. Its account contains
`active_input`, `unreached_desires`, `open_attempts`, `queued`, `arrived`, and
`clients`, plus `due_messages`, `due_ticks`, and required nullable
`no_progress_due` for unfinished tick-phase work. Each queued item has `message_id`, `status` (`pending` or `blocked`),
and required nullable `blocked_until`. The reader recomputes the account from
the preceding rows. It does not trust a terminal summary over contrary evidence.

## Prefixes, network testimony, and replay

An empty prefix is `empty`. Other tail classes are `between_turns`,
`awaiting_dispatch`, `open_invocation`, `partial_answer`, `interrupted_drain`,
`open_effect`, `awaiting_no_progress`, `refused`, and `finalized`. These classify observations, not
unwitnessed causes. A dispatch with no answer cannot distinguish an inhabitant
hang from a host stopping just after return. An attempted Send with no result
does not prove that bytes left. Prefix interpretation never promotes a desire
or attempt into a fulfilled Save.

The decoder accepts complete newline-terminated rows and rejects a torn last
row; it does not implement journal repair. Every complete-row prefix of a valid
tape is interpretable. A malformed row is an error, not an incomplete outcome.

Send identities exist at admission, before the local result. A scheduled
envelope later arrives and is dispatched, or is explicitly dropped because its
recipient is absent/crashed. Partitions delay messages; they do not drop them.
Readiness is frozen at tick start, so a new zero-latency send waits until the
next tick. Projections preserve pending queue order. Partition comparison uses
the effective relation at the current tick, since the live map expires entries
lazily when traffic encounters them.

The reader audits the declared tick policy as well as individual row feasibility.
At each advance it freezes the ready-message queue and live-node tick order.
Every ready message must arrive or drop in queue order; an arrival must be
followed immediately by its dispatch and complete turn. Then all configured
ticks run in node order. The next advance, scenario action, or ordinary
finalization cannot skip owed work. Explicit interrupted finalization preserves
that work in the terminal account. Readiness is not retroactively recomputed
when a scenario changes partitions between steps.

At step completion the reader recomputes distinct chosen slots and the last
step that made progress. A no-progress row must report the exact elapsed ticks,
and is required exactly when the configured threshold is exceeded. Another
node learning an existing slot does not reset progress. Lifecycle rows must
change state: an already crashed node cannot crash again, and a live node cannot
resume.

Replay constructs inhabitants from recorded configuration and feeds recorded
inputs in order. It compares the complete returned answer at each close, before
folding any effects of that answer. Its world uses recorded latency choices;
it does not construct an RNG, call `Simulator::step`, resend messages, or write
live storage. A changed third desire is found before the first two effects.
The report counts `uncertified_answers` explicitly. Open answers remain uncertified.
Replay checks responses to recorded inputs; whether the world supplied all inputs
owed by its policy is the projection validator's responsibility. Replay may enter the same open invocation;
there is no watchdog or claim that it can finish an originally hanging call.

`diagnose <tape>` explicitly permits a different implementation identity but
still stops at protocol divergence. The diagnostic demonstration is the Raft
unit test: a test-local observer writes role-after-receive observations absent
from the source tape into `<tape-sha256>.diagnostics.jsonl`, while replay checks
unchanged answers. Its source tape is saved beside it. It adds no inspection
method to `Node`, no protocol identities, and no annotations to the source file.

## First protocol question

`vote` is an independent Raft-specific consumer after world-schema validation.
It strict-parses known Raft records/messages, folds fulfilled term/vote records,
and checks durable support at **Send admission** for a granted vote. The request
candidate, authoritative sender, response recipient, and durable candidate must
match, and request/reply/durable terms must agree. Support can come from an
earlier turn. Two grants may share one earlier durable vote.

Initial term zero is the Raft constructor's declared default, identified by the
recorded implementation and configuration. The actual receiver can grant a
term-zero request after saving `VotedFor(0, candidate)` without a separate
CurrentTerm record, since the term never changed. The report names this origin
as `initial_state_term_zero` with `term_line: null`; it does not invent a Save.
An explicit CurrentTerm record instead yields `term_origin: fulfilled_save` and
its supporting line. A nonzero vote without the corresponding current term is
rejected. The peer-script fixture `initial-term-zero.jsonl` exercises the real
Raft receiver at this boundary; it is not a claim that an ordinary candidate
starts its election at term zero.

A higher durable term clears the old vote. Repeating the same term or a
same-term clear cannot erase its established candidate; a subsequent different
vote fails at that fulfilled record. Unknown records, missing request ancestry,
and malformed protocol evidence fail explicitly. The result establishes the
predicate on this finite tape, not full Raft correctness and not peer delivery.

Representative queries (after `inspect` validates the file):

```sh
jq -c 'select(.what == "result" and .why == "save" and .outcome == "fulfilled") | {node, record}' target/raft.jsonl
jq -c 'select(.what == "attempt" and .why == "send") | {node, parent_input_id, desire_id, message_id, to, message}' target/raft.jsonl
jq -c 'select(.what == "finalize") | .account' target/raft.jsonl
jq -c --argjson node 0 'select((.what == "dispatch" and .to == $node) or (.what == "attempt" and .node == $node))' target/raft.jsonl
```

The last query is the minimal node-view check: all local dispatches and attempts
retain their original causal identities. It is not a standalone node-tape export
format or a distributed merger.

## Boundary

No Certificate Verifier, Duel, honest recovery, socket adapter, physical WAL,
distributed merger, timeout-policy redesign, or Apply witness is included.
`recover` preserves the old flag-flip behavior and explicitly labels it
`retained_state`; its tape is not evidence of correct restart behavior. Future
recovery must rebuild only from fulfilled Save records and discard volatile
memory. Future storage adapters must block on unresolved Saves and stop the
incarnation on failure; this slice tests missing-result prefixes without adding
a failing-storage subsystem. Expected agreement refusals are now returned
errors with recorded results, rather than unwitnessed world panics.
