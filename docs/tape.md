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

`quorum raft`, `quorum ping-pong`, `quorum calm`, `quorum duel-inclusion`, and
`quorum duel-exclusion` also stream directly to stdout. `capture` writes its
tape path and prints the final projection summary on stdout.
`inspect`, `prefixes`, `replay`, `vote`, and `paxos-account` consume the file
without modifying it. Usage and file I/O errors fail on stderr. The independent
account instead returns protocol findings and world-reader errors inside its
report, with exit status zero. Reports hash original bytes and cite one-based
JSONL lines; the row sequence itself is zero-based.
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
Neither those observations nor strict replay substitutes for the independent
`paxos-account` below. It can judge the retained bytes without regenerating them.

## Multi-Paxos Duel

```sh
cargo run --locked -- capture duel-inclusion target/duel-inclusion.jsonl
cargo run --locked -- capture duel-exclusion target/duel-exclusion.jsonl
cargo run --locked -- paxos-account target/duel-inclusion.jsonl --json
bash scripts/check-duel.sh \
  "$RETAINED_EVIDENCE" \
  "$RETAINED_EVIDENCE/../duel-commission-e5a1518f.md"
```

The validation command requires the retained Multi-Paxos evidence root and the
exact commissioned Duel level file. The immutable second input is retained at
Athenaeum-relative path `projects/quorum/stacks/duel-commission-e5a1518f.md`,
SHA-256 `e5a1518ff5f24b01dabe46520cf326256a75c0aeaafd6a38212a34e54f6d78fa`;
the adjacent `.sha256` sidecar verifies its bytes. It writes new artifacts under
`target/duel-evidence/`; it never refreshes the retained answer-key inputs.

Duel uses the unchanged inhabitants at nodes 0, 1 and 2, all acceptors and
learners, with eligible proposers 0 and 1. Node 0 campaigns at `(1,0)` and
node 1 at `(2,1)`; node 2 is inactive. Spawn order is 0, 1, 2, followed by
tick-zero clients x to node 0 and y to node 1. Both campaigns begin on their
first Tick. Latency is 1 through 4, ticks occur every step, initial states are
empty, and the no-progress threshold is 200. No faults or retries are injected.

| Scenario | Seed | Allocation / first admission | Observed choices |
| --- | --- | --- | --- |
| `duel-inclusion` | 331 | 172 / 173 | y at slot 1, line 229; x at slot 0, line 251 |
| `duel-exclusion` | 1271 | 186 / 187 | y at slot 0, line 242 |

Inclusion freezes B's reports at 106/165, including the lone old x acceptance
at Save 114. Its answer adopts x at slot 0 before spending the waiting y at
slot 1. Exclusion freezes two empty reports at 159/182, despite the lone old
x acceptance at Save 126; B may use y at slot 0. Its later self-report at
197 remains visible but cannot revise that decision. The selected schedules
are not a one-message counterfactual or a frequency claim.

At both allocation cuts, absence of choice is established by three covered
declared-empty acceptors, no interpretation findings, and the complete
fulfilled-acceptance inventory containing only that one old Save.
`no_conflicting_choices` alone is not a no-choice proof. Every obligation
present in each allocation/admission cut equals its full-run object except
the safety summary at the cut line. Attempt origins and first Promise-quorum
observations also match; replies, resolution and other tail-dependent fields
are not required to match across prefixes.

A's lower attempt has one Accepted and two Accept-joined Nacks. It has work in
flight at the cuts but ends unresolved with no exact certificate and no
current world work. Inclusion relates it to B's same-value choice; exclusion
relates it to a different value chosen for its slot. Neither relation makes
the lower attempt a client completion, permanent loss, or pending scheduler
work. Before finalizing at horizon 28, the scenario requires an empty world
account. The fixed workload's five-hop bound at maximum delay 4 explains that
finite horizon; it is not a liveness theorem.

`tests/duel.rs` also declares an evidence-only wrong-answer wrapper. It replaces
B's inclusion answer `[x@0,y@1]` with `[y@0]` and retains only the prefix through
the first y certificate at line 221. FIFO/drain at 169 and adoption at
170/172/174 fail, while fresh count is one allowance spent once, gate and
authority remain established, and the ordinary certificate is not hidden.
There is no fabricated conflict or adopted ancestry. This wrapper is neither
a production scenario nor a strict-replay claim for the unchanged inhabitant.

The evidence command preserves the existing Raft, Ping/Pong, Calm and account
checks, captures each honest run twice, compares bytes, runs strict replay and
every complete-row prefix, and emits JSON/human accounts for both full tapes,
all four decisive cuts and the declared control. Rows 2 onward must match the
retained body hashes; only the header's scenario and implementation identity
change. Generated manifests record source/input identities, current revision,
representative judgments and the complete validation transcript; checksums
cover the generated artifacts.

The account retains interpretation `d8886526`; validation uses the exact
retained `account-interpretation-d8886526.md` and promoted `account-review/`
and `account-audit/` roots. Its accepted implementation base `97c7ad0` is
historical provenance, recorded separately from the current validated revision.
The Duel commission has its own pinned level identity. No account semantics
or provenance in the account report are retargeted.

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

## Independent Multi-Paxos account

```sh
cargo run --locked -- paxos-account target/calm.jsonl
cargo run --locked -- paxos-account target/calm.jsonl --json
bash scripts/check-paxos-account.sh retained-evidence/ 003_multi_paxos.md
```

`quorum::paxos_account::examine(&[u8]) -> serde_json::Value` and
`human(&Value) -> String` expose the same report as the CLI. The account shares
world validation and identity vocabulary, not Multi-Paxos Config/message/record
parsers, inhabitant state, or replay decisions. Its fixed interpretation is
`003_multi_paxos.md`, SHA-256
`d8886526fbaa53326c66a9b282a78f7b71de63d4f877bea3de65ebefaf0866d3`.
It is a bounded account of observed evidence, not a proof of an implementation's
general correctness or future progress.

### Report schema 1

The top-level keys are `schema_version`, `account_version`, `interpretation`,
`source_sha256`, `world`, `coverage`, `interpretation_findings`, `choices`,
`obligations`, `claims`, `attempts`, `observations`, and
`interpretation_gap_dependencies`. Arrays and identities are deterministic.
There is deliberately no overall pass/fail field.

Certificates, proposal attempts and client/fresh counts use the world's
`serde_json::Value` equality, not serialized JSON text. Floating `0.0` and
`-0.0` are equal, including inside arrays/objects; integer `0` and floating
`0.0` remain distinct. Reports preserve the first observed representative
without normalizing its rendering. Choice and attempt order follows their
first quorum/desire boundaries, independently of hash-map iteration.
An attempt's first spelling labels its report ID, never its semantic lookup.

| Peer section | Stable meaning and fields |
| --- | --- |
| `choices` | Historical exact `(slot, ballot, value)` header-majority certificates: `id`, `quorum`, immutable `first_quorum_line`/`first_support`, and accumulating per-acceptor `witnesses`. Illegal authority, watermark or adoption never filters an observable fulfilled acceptance. |
| `obligations` | Separate configuration, agreement, validity, workload and evidence propositions. Each has `id`, `proposition`, `category`, `subject`, `boundary_line`, `verdict`, `support`, `counterevidence`, `gaps`, and `reason`. |
| `claims` | Every Commit assertion and each scoped Chosen output: `id`, `kind`, `node`, `slot`, `ballot`, `value`, `assertion`, `effect`, `local_basis`, `transitive_support`, `choice_at_boundary`, `choices_seen_later`, `ballot_paths`, and `unsupported_by_tape`. |
| `attempts` | Each authoritative proposer/slot/ballot/value assertion, even without a claim: `id`, `proposer`, `slot`, `ballot`, `value`, `first_relevant_line`, `origin`, `sends`, `replies`, `other_replies`, `exact_choice`, `same_value_elsewhere`, `slot_witnessed_values`, `world_work`, `resolution`, and `obligation_ids`. |

The only proposition verdicts are `established`, `violated`, `pending`, and
`indeterminate`. Pending requires an explicitly open world operation.
Unsupported testimony is an ordinary reason for a violated or indeterminate
evidence proposition, not a fifth verdict. Undeclared script actors do not
receive sender-contract verdicts; their visible assertions and inconsistencies
remain reportable. Their complete positive evidence chains can still establish
knowledge, while undeclared initial knowledge prevents absence-based negatives.

Unavailable premises are located, not inferred from an `indeterminate` label.
Parse uncertainty cites actual finding IDs in `gaps` and the dependency table.
Non-parse uncertainty uses `unavailable_premises`: entries name the `premise`,
its `support`, `counterevidence` and real `gaps`, with `nodes` when a declaration
or initial state is unavailable. Contradictory first-quorum reports appear in
outer `counterevidence` as well as the unavailable map dependency. Later
cumulative-count uncertainty retains the original conflict/gap and allocation
boundaries, even if a later campaign has a determined map. Reasons identify
the unavailable premise rather than merely restating the successful invariant.

`coverage.nodes` names each declaration, its `covered`/`malformed`/`undeclared`
contract, parsed Config when available, initial-state coverage, and header
acceptor status. The header acceptor set alone defines global certificates;
covered proposer rules use their own declared acceptor set. Well-formed set or
suffix disagreement is a configuration violation, not an interpretation gap.
Per-node activation and initial-round differences are legal.

`world` contains `valid_rows`, `total_rows`, `valid_prefix_bytes`, `tail`,
the unchanged world `account`, `stopped`, and `error`. A malformed world row
sets `error: {line, reason}` and retains the last valid world prefix; it does not
change `Tape::read` or fabricate suffix identities. `source_sha256` still hashes
the complete original input, including the bad suffix. Protocol shape faults
instead leave the world prefix intact and allow later readable facts to count.

### Boundaries, origins, and evidence

Claim `assertion` records `desire_id`, `input_id`, `desire_line`, answer
`position`, and `close_line`. `effect` records `attempt_line`, `result_line`,
`outcome`, and `status`. Admission is the attempt: unreached assertions have
null knowledge propositions and no invented choice-comparison boundary.
An admitted but refused Commit retains its knowledge judgments. A later
desire stranded behind a refusal has state `unreached/refused`, not pending,
even if finalization preserves it in the world's terminal account.

Commit's `ballot` remains null. `ballot_paths` keeps separate exact-ballot
Accepted quorums and Chosen paths, including failed alternatives. Recursive
Chosen paths reference `knowledge_evidence` observations at the sender's
earlier Send admission. Accepted support requires a matching fulfilled Save
before that reply's Send admission; a later Save cannot repair an earlier
claim. One complete alternative establishes support without erasing failed
alternatives. Eligibility and conformance stay adjacent, never proof filters.

Learning a slot/value and announcing a completed proposal are separate
transitions. A learner that later completes its own proposal for that value
suppresses the duplicate Commit but still broadcasts Chosen to its configured
learners. Further Accepted replies for that exact completed proposal do not
repeat the broadcast. For receiver conformance, matching Accepted inputs count
only when dispatched after the close of the answer that first issued the exact
proposal. An earlier dispatch is not carried forward; a later dispatch from
that same acceptor can count. A reply sent before issuance but dispatched after
that close still counts. This timing rule does not filter either claim
proposition: earlier received testimony remains eligible for header-based
local and transitive evidence.
Only unreadable peer inputs dispatched after issuance can leave a below-quorum
receiver judgment indeterminate, and each cited gap must exist in the
interpretation findings. Earlier peer findings and malformed client inputs
remain in the report without making that proposal's reply count uncertain.

Attempt `origin.kind` is `adopted`, `fresh`, or `undetermined`.
`allocation_line` is the close containing the first Accept desire; its
`first_desire_line` and `first_desire_position` preserve within-answer order.
An unclosed allocation consumes nothing. Repeated broadcasts share one
proposal; refusal does not undo a closed allocation. Only the same slot/value
in a determined immutable first-quorum map receives adoption credit.
The first encounter counts distinct individually valid Promise senders even
when their reports contradict. Such a quorum can establish the Phase-2 gate
without determining an adoption map; later consistent subsets cannot replace
it or restore map-dependent adoption, FIFO or drain credit.

An adopted origin also exposes `acceptance_ancestry`: each relevant entry in
that frozen quorum joins its original Promise dispatch to the source Send
admission and exact earlier fulfilled acceptance Saves at that acceptor.
`selection` proves adoption separately from these `paths`; lower-ballot
competing entries and failed history joins remain explicit. A later Promise
or Save cannot repair the original basis. An undeclared script's positive
Save chain can establish ancestry without an initial-state contract. Missing
ancestry never revokes observed adoption or consumes a client allowance,
and prior choice is not required for adoption.

For allocated non-adopted origins, `client_count` exposes `available`,
`allocated`, and `certain`. Counts are per proposer and exact JSON value.
When uncertain, counts are observed lower bounds, not granted allowance.
Adopted and unclosed origins have null `client_count`. A client-only parse gap
can make counts/FIFO uncertain without making a known fresh origin unknown.
An undetermined first quorum/map makes origin undetermined; from that closed
allocation, consumption of every later value remains uncertain with no
within-campaign restoration. A known missing quorum instead yields fresh
origin and violates the Phase-2 gate and adoption's required quorum premise;
it is not an unavailable map and earns no adoption credit. An earlier closed
unreadable Send can instead hide a Prepare: missing-campaign/map conclusions
then remain uncertain with that finding's ID. Known campaigns, frozen maps
and independently witnessed activation failures remain intact.

Every header acceptor appears in `replies`, including no-reply observations.
Mismatching/duplicate replies and other senders or recipients remain explicit.
A Nack joins an attempt only through its actual Accept request ancestry;
Prepare ancestry is campaign testimony, and a script trigger stays unattributed.
Misdirected Nacks do not become replies received by the proposer.
`exact_choice` names only the exact tuple, `same_value_elsewhere` names other
ballots with the same slot/value, and `slot_witnessed_values` contains only
certificates for different values at that slot.
`resolution.exact_choice` and `resolution.world_open` are independent; its
display `state` prefers `exact_choice`, then `world_open`, then `unresolved`.
An empty world without a certificate is unresolved intent, not pending work,
client completion, or permanent defeat.

For each pair of conflicting certificates, `conflict_explanation` checks the
account itself at the second certificate's first-quorum boundary. It traces
the first supporting Saves, their request/Promise ancestry and relevant
durable history. A causally relevant agreement/validity failure or an actual
participant's header-acceptor-universe mismatch explains the conflict; a
failure on another slot, unrelated actor/operation or later boundary does not.
Pairwise-only configuration differences are not a substitute for this causal
join. Header certificates and conflicting-choice violations remain visible
even when a declared-universe mismatch explains them. If the history is fully
covered but no explaining predicate failed, `unexplained_conflicting_choices`
reports a missing or overgenerous account rule, not protocol tolerance.
Unavailable relevant configuration/history leaves this self-check
indeterminate with located dependencies, without hiding either choice or
weakening claimant evidence under the header universe.

### Interpretation gaps and reproducible evidence

Each interpretation finding has a causal `id`, `kind`, `node`, `first_line`,
all `occurrence_lines`, `world_identity`, `failed_shape`, `reason`, and
`affected_premises`. Scope follows declarations and world boundaries, not JSON
resemblance. Copies of one logical item share one finding; identical unrelated
items do not. Malformed Config does not remove its actor from parsing scope.

`interpretation_gap_dependencies` is the reviewable dependency table. Every
row names `finding_class`, `first_boundary`, `identity`, `affected_premises`,
`affected_propositions`, forward `propagation`, required `restoration`, and
the `report_fields` exposing those limits. Its six classes are configuration,
client input, peer input, output Send, output Save, and fulfilled Save.
An unfulfilled unreadable Save does not alter durable state. A later explicit
watermark can identify current state without repairing unknown history.
A later matching acceptance can support a later reply, not an earlier one.
`no_conflicting_choices` needs readable fulfilled-save and initial-state
coverage; two readable conflicting certificates still establish violation.

`check-paxos-account.sh` requires a retained-evidence directory and the exact
interpretation file named by the report. It replaces only generated account
evidence plus the existing Calm/Slice-2 generated directories.
The original retained tapes and checksums are read-only inputs, never recaptured.
`QUORUM_ACCOUNT_REVIEW_EVIDENCE` optionally supplies a checksummed review
directory containing the inactive-campaign pair and continued-contradiction
probe; the command preserves those inputs and verifies their repaired
judgments alongside the self-contained regressions.
`QUORUM_ACCOUNT_AUDIT_EVIDENCE` optionally supplies the fixed audit bundle.
Its checksummed learned-then-own-quorum, header-mismatch, signed-zero control
pair and five honest-history samples are read-only inputs. Certificate joins
use the same value equality rather than textual number spelling.
If that bundle includes `SHA256SUMS.postaccept`, the command also checks its
early-Accepted/control tapes and probe source without changing them. The pair
must have established receiver conformance while preserving early testimony
in claim evidence. Self-contained regressions additionally cover a whole early
quorum, a repeated member after issuance, and pre-issuance Send admission with
post-issuance dispatch, plus unreadable replies on each side of the issuance
boundary.
The optional `SHA256SUMS.timing` manifest supplies the immutable prequorum,
repeated-member, same-tick, delayed-dispatch and unreadable-input controls.
Its malformed-client probe must retain the client finding while establishing
receiver conformance at line 138 with no reply gap.
The command also requires every indeterminate obligation to carry a real gap
or located unavailable dependency.
The command runs the existing regression checks and independent counterexamples,
judges every retained receiver tape and adoption probe, checks deterministic
JSON/human output and original one-based citations, and writes:

- Per-input `.report.json` and `.human.txt` reports.
- `judgments.json`, with all retained/counterexample outcomes and cited findings.
- `retained-judgments.json`, covering Calm, the chosen-before-known cut,
  unsupported owner, and real adoption history.
- `review-judgments.json` and `review-inputs.sha256` when review inputs are supplied.
- `audit-judgments.json` and `audit-inputs.sha256` when audit inputs are supplied.
- `postaccept-judgments.json` when the post-acceptance audit manifest is supplied.
- `timing-judgments.json` when the timing audit manifest is supplied.
- `interpretation-gap-dependencies.json`.
- `manifest.json`, `source-manifest.sha256`, and `retained-inputs.sha256`,
  fixing the source base/status, interpretation and original evidence identities.
- `validation.txt` and `SHA256SUMS`, including the finished validation transcript.

## Boundary

No Duel, honest recovery, socket adapter, physical WAL, distributed merger,
timeout-policy redesign, or Apply witness is included.
`recover` preserves the old flag-flip behavior and explicitly labels it
`retained_state`; its tape is not evidence of correct restart behavior. Future
recovery must rebuild only from fulfilled Save records and discard volatile
memory. Future storage adapters must block on unresolved Saves and stop the
incarnation on failure; this slice tests missing-result prefixes without adding
a failing-storage subsystem. Expected agreement refusals are now returned
errors with recorded results, rather than unwitnessed world panics.
