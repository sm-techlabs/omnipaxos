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

The coordinator (any node, not necessarily the leader) calls `fast_propose_local`. This assigns
a **deadline** (`simulated_clock.now() + 50`) and a unique **request key**
`(coordinator_id, request_id)`. It registers the entry in `inflight_proposals` and broadcasts a
`FastPropose` message to every node in the cluster, including itself.

```
[P1 Fol/Acc]  [SEND][FAST_PROPOSE] coordinator=1 request=... deadline=...
[P1 Fol/Acc]  [APPEND] fast_propose coordinator=1 request=...
```

### Step 2 — Every node buffers the entry

Each node receives the `FastPropose` and inserts the entry into its **early buffer** — a min-heap
ordered by `(deadline, coordinator_id)`. Entries are *not* appended to the log yet; they wait
for their deadline to pass. This shared ordering guarantee is the core of DOM: every node will
release entries in the same sequence, regardless of network arrival order.

```
[P1 Fol/Acc]  [RECV] FastPropose from=1
[P2 Fol/Acc]  [RECV] FastPropose from=1
[P3 Ldr/Acc]  [RECV] FastPropose from=1
```

### Step 3 — tick() releases the entry

`tick()` is called periodically. On each tick the simulated clock advances and `release_message()`
is called in a `while let` loop. When `sim_clock.now() >= entry.deadline`, the entry is popped
from the early buffer and handed to `handle_released_fast_entry`.

```
[P1 Fol/Acc]  [INFO][FAST_PATH][BUFFER] releasing request=... coordinator=1 deadline=...
[P2 Fol/Acc]  [INFO][FAST_PATH][BUFFER] releasing request=... coordinator=1 deadline=...
[P3 Ldr/Acc]  [INFO][FAST_PATH][BUFFER] releasing request=... coordinator=1 deadline=...
```

### Step 4 — Each node appends, hashes, and replies

Every node in `(*, Accept)` appends the entry to its log. The DOM records a **cumulative hash**
of all entries accepted so far (including this one) at the new `accepted_idx`. Then:

- **Non-leader nodes** send `FastAccepted` to the **leader** (carrying `accepted_idx` and `hash`)
  and `FastReply` to the **coordinator** (carrying `hash`, but no `accepted_idx`).
- **The leader** appends locally, records `accepted_idx`, and sends `FastReply` to the coordinator
  with both the `hash` and the authoritative `accepted_idx`.

```
[P1 Fol/Acc]  [RECV][ACCEPT_DECIDE] accepted_idx=1 decided_idx=0 fast_path=true
[P1 Fol/Acc]  [SEND][FAST_ACCEPTED] to=3 request=... accepted_idx=1 hash=...
[P1 Fol/Acc]  [SEND][FAST_REPLY]    to=1 from=1 request=... accepted_idx=None hash=...
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
leader's reply carries `accepted_idx`; other replies are only counted if their hash matches the
leader's. When `fast_quorum` matching replies are accumulated, the coordinator calls
`set_decided_idx` locally — this is the **1-RTT client commit point** — before any `Decide`
message has been sent.

The fast quorum size is `1 + ⌊(3(N−1) + 3) / 4⌋`. For N=3 this equals 3 (all nodes); for N=7
it equals 6.

```
[P3 Ldr/Acc]  [RECV][FAST_ACCEPTED] from=2 coordinator=1 request=... hash=...
[P3 Ldr/Acc]  [INFO][FAST_DECIDE] decided_idx=1 hash=... broadcasting Decide
[P1 Fol/Acc]  [INFO][FAST_PATH][DECIDE] coordinator=1 request=... accepted_idx=1 hash=...
```

### Step 6 — Decide is broadcast; followers commit

The leader sends `Decide(decided_idx, hash≠0)` to every peer. Each follower checks its recorded
DOM hash at `accepted_idx - 1`. If it matches, the entry is committed. If it does **not** match
(the follower's log is out of sync), `reconnected()` is called, triggering Phase 1 recovery
(see below).

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
3. Sends fallback `AcceptDecide` to any follower whose `accepted_idx` is behind the leader's,
   carrying all missing log entries. This also advances the leader's per-follower sequence-number
   counter, so any still-unreachable follower will detect a gap on reconnect and trigger Phase 1
   recovery.

```
[P3 Ldr/Acc]  [SLOW_PATH][DECIDE] quorum met on resend; deciding accepted_idx=1
[P3 Ldr/Acc]  [SEND][DECIDE]       to=1 decided_idx=1 hash=0
[P3 Ldr/Acc]  [SEND][SLOW_PATH]    AcceptDecide fallback to=2 entries=1 decided_idx=1
[P2 Fol/Acc]  [RECV][ACCEPT_DECIDE] accepted_idx=1 decided_idx=0
[P2 Fol/Acc]  [RECV][DECIDE]       from=3 decided_idx=1 hash=0 → committed
```

---

## Phase 1 recovery (reconnect after partition)

When a follower reconnects after missing entries, it will eventually receive a message from the
leader whose session or sequence number doesn't match. `handle_sequence_num` detects the gap
(`DroppedPreceding`) and triggers `reconnected(leader)`.

`reconnected` transitions the node to `(Follower, Recover)` and sends a `PrepareReq` to the
leader. The leader responds with a `Prepare`. The follower replies with a `Promise` (attaching a
log sync if it is more up to date). The leader, once it has heard from a majority, sends an
`AcceptSync` containing all missing entries up to `decided_idx`. The follower applies them,
transitions to `(Follower, Accept)`, and sends `Accepted`. From this point the follower
participates normally.

```
[P7 Fol/---]  [RECOVER] gap detected from=6 → entering Recover phase
[P6 Ldr/Acc]  [SEND][PREPARE]      to=7 decided_idx=4 accepted_idx=4
[P7 Fol/Pre]  [RECV][PREPARE]      from=6 → sending Promise
[P6 Ldr/Acc]  [SEND][ACCEPT_SYNC]  to=7 decided_idx=4
[P7 Fol/Acc]  [RECV][ACCEPT_SYNC]  from=6 → entering Accept phase
```

---

## Summary

| Phase | Triggered by | Key messages | State after |
|---|---|---|---|
| Leader election | BLE timeout | HeartbeatReq/Reply | `Ldr/Pre` → `Ldr/Acc`, peers `Fol/Acc` |
| Fast propose | `append()` | `FastPropose` → early buffer | buffered |
| Fast accept | `tick()` releases deadline | `FastAccepted` → leader, `FastReply` → coordinator | appended locally |
| Fast decide | super-quorum at coordinator | coordinator commits; leader broadcasts `Decide(hash≠0)` | all committed |
| Slow decide | resend timer | `Decide(hash=0)` + fallback `AcceptDecide` | all committed |
| Recovery | seq-num gap / hash mismatch | `PrepareReq` → `Prepare` → `Promise` → `AcceptSync` → `Accepted` | `Fol/Acc` |
