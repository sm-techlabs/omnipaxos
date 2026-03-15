# OmniPaxos DOM — Event Flow: From Client Request to All-Replicas Decided

This guide traces the life of a single value from the moment a client appends it until every
replica has committed it. There are two paths: a **fast path** (1 RTT, requires a super-quorum)
and a **slow path** (fallback when the fast path stalls).

---

## Precondition: Stable cluster

Before any client request can be processed, leader election must have completed. The
`BallotLeaderElection` component runs periodic heartbeat rounds. The node with the
highest-priority ballot that can reach a majority increments its ballot and is elected. On
election, it transitions to `(Leader, Prepare)`, sends `Prepare` to all peers, collects `Promise`
replies from a majority, then transitions to `(Leader, Accept)` and sends `AcceptSync` to each
follower. Followers move to `(Follower, Accept)` upon receiving `AcceptSync`. The cluster is
now ready.

```
[P3 Ldr/Pre]  [LEADER] elected ballot=... → entering Prepare phase
[P1 Fol/Pre]  [RECV][PREPARE] from=3 → sending Promise
[P3 Ldr/Acc]  [LEADER] majority promises → Accept phase, sending AcceptSync
[P1 Fol/Acc]  [RECV][ACCEPT_SYNC] from=3 → entering Accept phase
```

---

## Fast path (happy path, N=3, fast quorum = 3)

### Step 1 — Client appends on a coordinator node

```rust
paxos.append(value)
```

The coordinator (any node, not necessarily the leader) calls `propose_entry`. This assigns
a **deadline** (`simulated_clock.now() + 50`) and a unique **request key**
`(coordinator_id, request_id)` where `request_id` is a random `u64`. It registers the entry in
`inflight_proposals` and broadcasts a `FastPropose` message to every node in the cluster,
including itself.

```
[P1 Fol/Acc]  [SEND][FAST_PROPOSE] coordinator=1 request=... deadline=...
[P1 Fol/Acc]  [APPEND] fast_propose coordinator=1 request=...
```

### Step 2 — Every node buffers the entry

Each node receives the `FastPropose` and inserts the entry into its **early buffer** — a min-heap
ordered by `(deadline, id)` where `id = (coordinator_id, request_id)`. Entries are *not* appended
to the log yet; they wait for their deadline to pass. This shared ordering guarantee is the core
of DOM: every node will release entries in the same sequence, regardless of network arrival order.

```
[P1 Fol/Acc]  [RECV] FastPropose from=1
[P2 Fol/Acc]  [RECV] FastPropose from=1
[P3 Ldr/Acc]  [RECV] FastPropose from=1
```

### Step 3 — tick() releases the entry

`tick()` is called periodically. On each tick the simulated clock advances and `release_message()`
is called in a `while let` loop. When `sim_clock.now() >= entry.deadline`, the entry is popped
from the early buffer and handed to `handle_released_fast_entry_leader` (on the leader) or
`handle_released_fast_entry_follower` (on followers), depending on the node's current role.

```
[P1 Fol/Acc]  [INFO][FAST_PATH][BUFFER] releasing request=... coordinator=1 deadline=...
[P2 Fol/Acc]  [INFO][FAST_PATH][BUFFER] releasing request=... coordinator=1 deadline=...
[P3 Ldr/Acc]  [INFO][FAST_PATH][BUFFER] releasing request=... coordinator=1 deadline=...
```

### Step 4 — Each node appends, hashes, and replies

Every node in `(*, Accept)` appends the entry to its log. The DOM records a **cumulative hash**
of all entries accepted so far (including this one) at the new `accepted_idx`. Then:

- **Non-leader nodes** send `FastAccepted` to the **leader** (carrying `accepted_idx` and `hash`)
  and `FastReply` to the **coordinator** (carrying `accepted_idx` and `hash`).
- **The leader** appends locally, records `accepted_idx`, and sends `FastReply` to the coordinator
  with the `hash` and the authoritative `accepted_idx`.

```
[P1 Fol/Acc]  [RECV][ACCEPT_DECIDE] accepted_idx=1 decided_idx=0 fast_path=true
[P1 Fol/Acc]  [SEND][FAST_ACCEPTED] to=3 request=... accepted_idx=1 hash=...
[P1 Fol/Acc]  [SEND][FAST_REPLY]    to=1 from=1 request=... accepted_idx=Some(1) hash=...
[P3 Ldr/Acc]  [INFO][FAST_PATH] leader appended coordinator=1 request=... accepted_idx=1 hash=...
[P3 Ldr/Acc]  [SEND][FAST_REPLY]    to=1 from=3 request=... accepted_idx=Some(1) hash=...
```

### Step 5 — Leader tallies FastAccepted; coordinator tallies FastReply

Two independent quorum checks race in parallel:

**At the leader**: each `FastAccepted` is recorded in `fast_accepted_tracker` keyed by
`accepted_idx`. Replies with a mismatching hash are discarded. When `fast_quorum` distinct nodes
have reported the same hash for `accepted_idx`, the leader calls `fast_decide`: it sets
`decided_idx` and broadcasts `Decide(decided_idx, hash)` to all peers.

**At the coordinator**: `FastReply` messages are collected in `fast_reply_tracker`. Only the
leader's reply carries the authoritative `accepted_idx`; other replies are only counted if their
hash matches the leader's. When `fast_quorum` matching replies are accumulated, the coordinator
calls `set_decided_idx` locally — this is the **1-RTT client commit point** — before any `Decide`
message has been sent.

The fast quorum size is `1 + ⌊(3(N−1) + 3) / 4⌋`. For N=3 this equals 3 (all nodes); for N=7
it equals 6.

```
[P3 Ldr/Acc]  [RECV][FAST_ACCEPTED] from=2 coordinator=1 request=... hash=...
[P3 Ldr/Acc]  [INFO][FAST_DECIDE] decided_idx=1 hash=... broadcasting Decide
[P1 Fol/Acc]  [INFO][FAST_PATH][DECIDE] coordinator=1 request=... accepted_idx=1 hash=...
```

### Step 6 — Decide is broadcast; followers commit

The leader sends `Decide(decided_idx, hash≠0)` to every peer. Each follower first checks for
missing entries: if `decided_idx > accepted_idx`, Phase 1 recovery is triggered. Otherwise it
compares `dec.hash` against its own recorded hash **at `dec.decided_idx`** specifically
(`dom.get_hash_at(dec.decided_idx)`), not against `last_log_hash`. This is important because the
follower may have fast-accepted entries beyond `decided_idx` whose hashes are still valid; those
must not be disturbed.

If there is a mismatch, `dom.patch_hash_at(decided_idx, dec.hash)` corrects the chain: it
computes the XOR delta between the old hash at `decided_idx` and the leader's correct hash, then
applies that delta to every entry in `log_hashes` from that position onwards, and to
`last_log_hash`. This preserves the validity of all entries accepted after `decided_idx`.

**Hash-correction Decides from the leader** (deadline reorder / Case 1 and Case 2 of
`HashMismatch`) carry `decided_idx = accepted_idx` — not `get_decided_idx()` — so the follower
always knows which exact log position the hash refers to.

```
[P3 Ldr/Acc]  [SEND][DECIDE] to=1 decided_idx=1 hash=... resend=false
[P3 Ldr/Acc]  [SEND][DECIDE] to=2 decided_idx=1 hash=... resend=false
[P1 Fol/Acc]  [RECV][DECIDE] from=3 decided_idx=1 hash=... → committed
[P2 Fol/Acc]  [RECV][DECIDE] from=3 decided_idx=1 hash=... → committed
```

All replicas have now decided. Total network delay: one round-trip
(client → coordinator → cluster → coordinator replies).

---

## Slow path (fast quorum not met)

When a node is partitioned or slow, it may not send `FastAccepted`/`FastReply` in time. The fast
quorum is never reached. The resend timer fires after `resend_message_timeout` (default 500 ms).

`resend_messages_leader` checks whether a **regular majority** has acknowledged `accepted_idx`
via `is_chosen`. If yes, the leader decides via the slow path:

1. Calls `set_decided_idx` locally.
2. Sends `Decide(decided_idx, hash=0)` to all peers. **Hash = 0 is the slow-path sentinel** —
   followers skip the DOM hash check and commit unconditionally.
3. Sends a fallback `AcceptDecide` to any follower whose `accepted_idx` is behind the leader's,
   carrying all missing log entries. This also advances the leader's per-follower sequence-number
   counter, so any still-unreachable follower will detect a gap on reconnect and trigger Phase 1
   recovery.

### Idempotency of fallback AcceptDecide

A race exists between DOM buffer release and the resend timer. A follower may have already
appended an entry via its own `tick()` (released from the early buffer) by the time the fallback
`AcceptDecide` arrives. Without a guard, the follower would append the entry again, creating a
duplicate.

To prevent this, every slow-path `AcceptDecide` carries a **`prev_idx`** field: the Paxos log
position immediately before the first entry in the message. When a follower processes a slow-path
`AcceptDecide`, it checks its current `accepted_idx` against the range `[prev_idx, prev_idx +
entries.len())`:

- **All entries already present** (`accepted_idx >= prev_idx + entries.len()`): skip the append
  entirely, only advance `decided_idx` if needed.
- **Partial overlap** (`accepted_idx > prev_idx`): drain the already-held prefix from `entries`
  before appending.
- **No overlap**: append normally.

Fast-path `FastPropose` messages set `prev_idx = 0` (unused — fast-path entries are handled
exclusively through the DOM early buffer and are not de-duplicated this way).

```
[P3 Ldr/Acc]  [SLOW_PATH][DECIDE] quorum met on resend; deciding accepted_idx=1
[P3 Ldr/Acc]  [SEND][DECIDE]       to=1 decided_idx=1 hash=0
[P3 Ldr/Acc]  [SEND][SLOW_PATH]    AcceptDecide fallback to=2 entries=1 decided_idx=1
[P2 Fol/Acc]  [ACCEPT_DECIDE][IDEMPOTENT] skipping duplicate: my_idx=1 entries=[0..1]
[P2 Fol/Acc]  [RECV][DECIDE]       from=3 decided_idx=1 hash=0 → committed
```

---

## Phase 1 recovery (reconnect after partition)

When a follower is missing log entries, it will receive a `Decide` whose `decided_idx` exceeds
its own `accepted_idx`. `handle_decide` detects this and triggers `reconnected(leader_pid)`.

`reconnected` transitions the node to `(Follower, Recover)` and sends a `PrepareReq` to the
node it detected the gap from (the leader). The leader responds immediately with a `Prepare`
(no majority wait — this is a reconnect, not a fresh election). The follower replies with a
`Promise` (attaching a log sync if it is more up to date). The leader sends an `AcceptSync`
immediately to the promiser containing all missing entries up to `decided_idx`. The follower
applies them, transitions to `(Follower, Accept)`, sets `dom.last_log_hash` from the AcceptSync's
`dom_hash` field, and sends `Accepted`. From this point the follower participates normally.

A sequence-number gap (`DroppedPreceding` from `handle_sequence_num`) also triggers
`reconnected`, for the case where a follower reconnects and receives a message with an ahead
sequence number before seeing a `Decide`.

### early_buffer is cleared on AcceptSync

A follower may have received a `FastPropose` while it was in `(Follower, Recover)`. The message
handler (`handle_fast_propose`) inserts the entry into `dom.early_buffer` regardless of phase,
because no phase check is appropriate at the buffering stage. Later, `handle_acceptsync` delivers
that same entry as part of the log suffix. Without a guard, `tick()` would subsequently pop the
entry from `early_buffer` and append it a second time.

`sync_to_log_position` (called from `handle_acceptsync`) now calls `early_buffer.clear()`. This
is correct because:

- Entries already in the AcceptSync suffix are in storage — they must not be appended again.
- Entries not in the suffix were rejected by the leader's log sync — they are stale.
- Any new fast-path entries will arrive as fresh `FastPropose` messages after the follower
  re-enters `(Follower, Accept)`.

```
[P7 Fol/---]  [RECOVER] gap detected from=6 → entering Recover phase, sending PrepareReq
[P6 Ldr/Acc]  [SEND][PREPARE]      to=7 decided_idx=4 accepted_idx=4
[P7 Fol/Pre]  [RECV][PREPARE]      from=6 → sending Promise
[P6 Ldr/Acc]  [SEND][ACCEPT_SYNC]  to=7 decided_idx=4
[P7 Fol/Acc]  [RECV][ACCEPT_SYNC]  from=6 → entering Accept phase (early_buffer cleared)
```

---

## Summary

| Phase | Triggered by | Key messages | State after |
|---|---|---|---|
| Leader election | BLE timeout | HeartbeatReq/Reply | `Ldr/Pre` → `Ldr/Acc`, peers `Fol/Acc` |
| Fast propose | `append()` | `FastPropose` → early buffer | buffered |
| Fast accept | `tick()` releases deadline | `FastAccepted` → leader, `FastReply` → coordinator | appended locally |
| Fast decide (leader) | fast_quorum `FastAccepted` at leader | leader sets `decided_idx`, broadcasts `Decide(hash≠0)` | all committed |
| Fast decide (coordinator) | fast_quorum `FastReply` at coordinator | coordinator sets `decided_idx` locally (1-RTT) | coordinator committed |
| Slow decide | resend timer | `Decide(hash=0)` + fallback `AcceptDecide` (idempotent via `prev_idx`) | all committed |
| Recovery | missing entries in `Decide` / seq-num gap | `PrepareReq` → `Prepare` → `Promise` → `AcceptSync` (clears `early_buffer`) → `Accepted` | `Fol/Acc` |
