# DOM Fast Path / Slow Path Agent Task

# Objective
Analyze this repository with the goal of understanding and validating the implementation of a Fast Path + Slow Path consensus mechanism built on top of Sequence Paxos.

The agent must perform a deep analysis of the codebase and determine:
A) How the Fast Path currently works and whether it is implemented correctly.
B) How the Slow Path reconciles inconsistencies when the Fast Path fails.

The agent must not make assumptions. If any part of the protocol or code behavior is unclear, clarification must be requested before implementation changes are made.

---

# Scope
The primary implementation area for this task is located under:

src/sequence_paxos/

Specifically focus on:

src/sequence_paxos/follower.rs
src/sequence_paxos/leader.rs
src/sequence_paxos/mod.rs

Avoid modifying other parts of the system unless it is absolutely necessary.

Existing structures and abstractions should be preserved whenever possible. Large refactors are discouraged. Only implement the minimal set of changes required to support the desired protocol behavior.

---

# Conceptual Distinction: Accept vs Decide

There are two distinct stages of log progression:

Accept
- A node appends a value to its in-memory log.
- This advances accepted_idx.
- The entry is NOT yet committed to storage.

Decide (Commit)
- The leader broadcasts a Decide message once a super quorum confirms acceptance.
- Nodes commit the entry to persistent storage.
- decided_idx advances.
- This step is NON-REVERSIBLE.

Safety invariant:
If even one node decides an entry, it must already have been accepted by the super quorum and therefore is guaranteed to eventually be decided by the quorum.

---

# Fast Path Protocol (Desired Behavior)

The Fast Path is designed to allow requests to complete in one round trip under optimistic conditions.

Step 1
Client sends request to any node (Coordinator).

Step 2
Coordinator broadcasts a FastPropose message to all nodes.

Step 3
When a node receives the request:
- The request is inserted into the Early Buffer.
- The request waits until its deadline.
- Once the deadline is reached, the request is released from the buffer.
- The node accepts the request into its in-memory log.
- accepted_idx advances.

Step 4
After accepting the entry, the node sends two messages:

FastReply -> Coordinator
FastAccepted -> Leader

The FastReply message must include the log index where the entry was accepted. This prevents ordering conflicts if multiple coordinators are active.

Step 5
The coordinator gathers FastReply messages.

If a super quorum of FastReply messages is collected:
- The coordinator advances decided_idx locally.
- The coordinator responds to the client.

This allows the client to receive a response after one RTT.

Step 6
The leader collects FastAccepted messages.

For each accepted index, the leader checks:
- Whether a super quorum of FastAccepted messages exists.
- Whether the accepted index is greater than the current decided_idx.

Step 7
Once a super quorum of FastAccepted messages is confirmed, the leader broadcasts:

Decide

All nodes then commit the entry to storage and advance decided_idx.

---

# Super Quorum Definition

For a cluster of N nodes:

f = N / 2
super_quorum = f + f/2 + 1

The quorum must include the leader.

---

# Coordinator Behavior

The coordinator does NOT assign log indices.

Instead:
- The coordinator assigns a deadline to the request.
- The deadline determines when the request will be released from the Early Buffer.
- The ordering of requests is determined by deadlines.

This allows multiple coordinators to propose concurrently without direct leader mediation.

---

# Early Buffer

The Early Buffer implements the Nezha protocol buffering model.

Messages are ordered by deadline and released when their deadline is reached.

Implementation:

early_buffer: BinaryHeap<AcceptDecide<T>>

Nodes maintain:

early_buffer: BinaryHeap<AcceptDecide<T>>
late_buffer: HashMap<(u64, u64), AcceptDecide<T>>
sim_clock: ClockState
last_released_timestamp: i64
last_log_hash: u64
log_hashes: Vec<u64>
fast_reply_tracker: HashMap<(u64, u64), FastReplyQuorum<T>>
fast_accepted_tracker: HashMap<usize, FastAcceptedQuorum>
fast_quorum_size: usize
metadata_log: Vec<DomMetadata>

Metadata structure:

pub struct DomMetadata {
    id: (u64, u64),
    deadline: i64,
}

Request identity is defined as:

(deadline, (req_id, replica_id))

Multiple requests may share the same deadline, but they cannot share both deadline and request id.

---

# Release Logic

Requests are released from the Early Buffer according to deadline ordering.

Current release logic example:

match self.dom.release_message() {
    None => return,
    Some(prop_msg) => {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "[INFO][FAST_PATH] releasing request={} coordinator={} deadline={}",
            prop_msg.id.1,
            prop_msg.id.0,
            prop_msg.deadline,
        );

        if self.state == (Role::Leader, Phase::Accept) {
            self.handle_released_fast_entry_leader(prop_msg);
        } else {
            let fast_reply = self.handle_released_fast_entry_follower(prop_msg);
            self.dispatch_fast_reply(fast_reply);
        }
    }
}

The agent must verify whether this implementation correctly satisfies the intended protocol.

---

# Late Buffer

The Late Buffer stores messages that arrive too late relative to the ordering constraints.

Structure:

late_buffer: HashMap<(u64, u64), AcceptDecide<T>>

These messages may trigger Slow Path reconciliation.

---

# Tie Breaking Rule

If two requests share the same deadline and ordering is ambiguous:

Use deterministic tie breaking based on:

node_id / pid

---

# Slow Path

The Slow Path exists to reconcile inconsistent logs when Fast Path assumptions break.

Situations that may trigger Slow Path:

1. A node believes it is out of sync.
2. A node receives messages that violate deadline ordering.
3. A message arrives after a later deadline has already been released.
4. Divergent logs appear under the same leader.

When this occurs:
- The request may be placed in the Late Buffer.
- The system transitions into reconciliation behavior.

---

# Temporary Slow Path Strategy

If a full protocol is not derivable from the existing implementation, a temporary reconciliation mechanism may be used.

Acceptable temporary implementation:

Leader overwrite strategy:

1. Leader broadcasts its log up to decided_idx.
2. Followers overwrite their local logs.
3. Followers synchronize their decided_idx with the leader.

This ensures that any chosen values eventually become decided across the cluster.

---

# Logging Requirements

The agent should enrich relevant code paths with structured debugging logs.

Use clear markers such as:

[SEND]
[RECV]
[FAST_PATH]
[SLOW_PATH]
[BUFFER]
[DECIDE]
[INFO]

Logs should include useful identifiers such as:

node_id
request_id
replica_id
deadline
log_index
ballot

Logging must improve debugging clarity without introducing unnecessary noise.

---

# Deliverable

After completing the analysis, create a document at the root of the repository:

ACTION_PLAN.md

The document must contain:

1. Fast Path Findings
- description of how the current implementation works
- deviations from the desired protocol
- potential correctness issues

2. Slow Path Findings
- how reconciliation currently works
- why it may be insufficient

3. Required Code Changes
- minimal set of modifications required
- files that must be updated
- specific logic that must be implemented

4. Testing Strategy
- how integration tests should validate the system

---

# Integration Tests

After implementing the Fast Path flow, create a DOM integration test suite compatible with the existing Kompact setup.

The tests must simulate multiple nodes and their message exchange.

Required scenarios:

Test 1 — Fast Path Success
Verify client request completes in one RTT and leader eventually broadcasts Decide.

Test 2 — Multiple Coordinators
Verify deadline ordering and deterministic tie breaking.

Test 3 — Partial Fast Quorum
Verify fallback to Slow Path behavior.

Test 4 — Coordinator Crash
Verify system still eventually decides entries.

Test 5 — Divergent Logs
Verify reconciliation through Slow Path.

---

# Final Rule

The agent must not invent protocol behavior.

If any aspect of the protocol or implementation is unclear, clarification must be requested before making implementation changes.