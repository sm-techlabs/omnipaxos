use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::messages::sequence_paxos::{AcceptDecide, FastAccepted, FastReply, FastSync};
use crate::simulated_clock::ClockState;
use crate::storage::Entry;
use crate::util::NodeId;
use std::hash::{DefaultHasher, Hash, Hasher};

/// This stores meta data that is used during the sync operation.
#[derive(Clone, Hash)]
pub struct DomMetadata {
    id: (u64, u64),
    deadline: i64,
}

/// Used to track what quorums have been reached for a given request
struct FastReplyQuorum<T>
where
    T: Entry,
{
    fast_response: HashSet<u64>,
    slow_response: HashSet<u64>,
    leader_response: Option<FastReply<T>>,
    pending_replies: Vec<FastReply<T>>,
}

struct FastAcceptedQuorum {
    replicas: HashSet<NodeId>,
    hash: u64,
}

/// The outcome of recording a single `FastAccepted` acknowledgement.
#[derive(Debug, PartialEq, Eq)]
pub enum FastAcceptedOutcome {
    /// This acknowledgement matched the reference hash but the fast quorum
    /// threshold has not yet been reached.
    Pending,
    /// The fast quorum threshold has been reached; the leader may now call
    /// `fast_decide`.
    QuorumReached,
    /// The incoming acknowledgement carries a hash that differs from the
    /// reference hash already established for this slot.  The reporting node
    /// accepted the entry with a different deadline than the leader assigned
    /// (out-of-order rewrite) and its DOM state is inconsistent.  The leader
    /// must trigger Phase-1 recovery on that node.
    HashMismatch,
}

/// The coordinator-side decision derived from a fast quorum of matching replies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FastPathDecision {
    /// The coordinator that originated the request.
    pub coordinator_id: u64,
    /// The request identifier assigned by the coordinator.
    pub request_id: u64,
    /// The accepted index chosen by the leader for the request.
    pub accepted_idx: usize,
    /// The agreed prefix hash at `accepted_idx`.
    pub hash: u64,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
// TODO:
// generateLogHash()
// sendSync({view-id, client-id,request-id,deadline,log-id})
// handleSync({view-id, client-id,request-id,deadline,log-id}) ->
// lateBufferLookup({request-id, client-id})
/// Deadline Ordered M
pub struct DOM<T>
where
    T: Entry,
{
    early_buffer: BinaryHeap<AcceptDecide<T>>,
    /// late buffer
    pub late_buffer: HashMap<(u64, u64), AcceptDecide<T>>,
    sim_clock: ClockState,
    last_released_timestamp: i64,
    /// hash value
    pub last_log_hash: u64,
    log_hashes: Vec<u64>,
    fast_reply_tracker: HashMap<(u64, u64), FastReplyQuorum<T>>,
    fast_accepted_tracker: HashMap<usize, FastAcceptedQuorum>,
    fast_quorum_size: usize,
    metadata_log: Vec<DomMetadata>,
}

impl<T> DOM<T>
where
    T: Entry,
{
    /// Returns a new DOM
    pub fn new(fqs: usize) -> DOM<T> {
        return DOM {
            early_buffer: BinaryHeap::new(),
            late_buffer: HashMap::new(),
            sim_clock: ClockState::new(0, 100, 100, 10),
            last_released_timestamp: 0,
            last_log_hash: 0,
            log_hashes: Vec::new(),
            fast_reply_tracker: HashMap::new(),
            fast_accepted_tracker: HashMap::new(),
            fast_quorum_size: Self::fast_quorum_size(fqs),
            metadata_log: Vec::new(),
        };
    }

    /// Handles a fast-path propose on a **follower** node.
    ///
    /// If the entry's deadline is still in the future relative to the last
    /// released timestamp it goes into the `early_buffer` for deadline-ordered
    /// release.  Otherwise it is already "late" and goes to the `late_buffer`
    /// so that a subsequent `FastSync` can reconcile it.
    pub fn handle_fast_propose(&mut self, ac: AcceptDecide<T>) {
        if ac.deadline > self.last_released_timestamp {
            self.early_buffer.push(ac);
        }
        // else {
        //     self.late_buffer.insert(ac.id, ac);
        // }
    }

    /// Handles a fast-path propose on the **leader** node.
    ///
    /// Unlike the follower path, the leader must guarantee that every entry it
    /// releases has a strictly-increasing deadline.  If the incoming message
    /// carries a deadline that is already past (`deadline <=
    /// last_released_timestamp`), the leader **reorders** it by bumping the
    /// deadline to `last_released_timestamp + 1` before inserting it into the
    /// `early_buffer`.
    ///
    /// Followers that received the same message with the original (earlier)
    /// deadline will fast-accept it and send `FastAccepted` with a hash that
    /// reflects the original deadline.  When those acknowledgements arrive at
    /// the leader it will detect the hash mismatch and trigger recovery on
    /// those followers (see `handle_fast_accepted`).
    pub fn handle_fast_propose_leader(&mut self, mut ac: AcceptDecide<T>) {
        if ac.deadline <= self.last_released_timestamp {
            ac.deadline = self.last_released_timestamp + 1;
        }
        self.early_buffer.push(ac);
    }

    /// Handles a fast path reply
    /// Returns true when the number of messages meets or exceeds fast quorum size reqs
    pub fn handle_fast_reply(
        &mut self,
        fr: FastReply<T>,
        leader: bool,
    ) -> Option<FastPathDecision> {
        let key = (fr.coordinator_id, fr.request_id);
        let qd = self
            .fast_reply_tracker
            .entry(key)
            .or_insert(FastReplyQuorum {
                fast_response: HashSet::new(),
                slow_response: HashSet::new(),
                leader_response: None,
                pending_replies: Vec::new(),
            });

        if leader {
            qd.fast_response.insert(fr.replica_id);
            qd.leader_response = Some(fr);
        } else {
            match qd.leader_response.as_ref() {
                Some(leader_reply) if leader_reply.hash == fr.hash => {
                    qd.fast_response.insert(fr.replica_id);
                }
                Some(_) => {}
                None => qd.pending_replies.push(fr),
            }
        }

        if let Some(leader_reply) = qd.leader_response.clone() {
            let pending_replies = std::mem::take(&mut qd.pending_replies);
            for pending_reply in pending_replies {
                if pending_reply.hash == leader_reply.hash {
                    qd.fast_response.insert(pending_reply.replica_id);
                }
            }

            if qd.fast_response.len() + qd.slow_response.len() >= self.fast_quorum_size {
                if let Some(accepted_idx) = leader_reply.accepted_idx {
                    return Some(FastPathDecision {
                        coordinator_id: leader_reply.coordinator_id,
                        request_id: leader_reply.request_id,
                        accepted_idx,
                        hash: leader_reply.hash,
                    });
                }
            }
        }

        None
    }

    /// Fake function to integrate slow responses for testing the
    /// fast reply handler
    pub fn fake_increment_slow_replies(&mut self, coordinator_id: u64, pid: u64, request_id: u64) {
        let key = (coordinator_id, request_id);
        let qd = self
            .fast_reply_tracker
            .entry(key)
            .or_insert(FastReplyQuorum {
                fast_response: HashSet::new(),
                slow_response: HashSet::new(),
                leader_response: None,
                pending_replies: Vec::new(),
            });
        qd.slow_response.insert(pid);
    }

    /// Tracks a `FastAccepted` acknowledgement at the leader and returns the
    /// outcome for this slot.
    ///
    /// The first `FastAccepted` received for a given `accepted_idx` establishes
    /// the **reference hash** for that slot.  Every subsequent acknowledgement
    /// is compared against it:
    ///
    /// * Equal hash → the replica is added to the quorum; returns
    ///   `QuorumReached` when the threshold is met, `Pending` otherwise.
    /// * Different hash → the replica accepted the entry with metadata that
    ///   differs from the reference (e.g. a different deadline due to
    ///   reordering on the leader); returns `HashMismatch`.  The caller is
    ///   responsible for triggering Phase-1 recovery on the mismatching node.
    pub fn handle_fast_accepted(
        &mut self,
        accepted: FastAccepted,
        from: NodeId,
    ) -> FastAcceptedOutcome {
        let qd = self
            .fast_accepted_tracker
            .entry(accepted.accepted_idx)
            .or_insert(FastAcceptedQuorum {
                replicas: HashSet::new(),
                hash: accepted.hash,
            });

        if qd.hash != accepted.hash {
            return FastAcceptedOutcome::HashMismatch;
        }

        qd.replicas.insert(from);
        if qd.replicas.len() >= self.fast_quorum_size {
            FastAcceptedOutcome::QuorumReached
        } else {
            FastAcceptedOutcome::Pending
        }
    }

    /// Returns the node IDs that have acknowledged slot `accepted_idx` with
    /// the reference hash stored in the tracker.
    ///
    /// Used by the leader when its own hash for a slot differs from what
    /// followers already reported (Case 1 reordering): the nodes currently
    /// in `replicas` all used the *wrong* hash and need recovery.
    pub fn get_replicas_with_wrong_hash(&self, accepted_idx: usize) -> Vec<NodeId> {
        self.fast_accepted_tracker
            .get(&accepted_idx)
            .map(|qd| qd.replicas.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Handles a fast sync message
    /// Compares the value at log_id to the metadata in the sync message
    /// returns true if all good
    /// returns false if not synchronize
    /// second value is update missed log entry if it can be found in the late buffer, else None
    pub fn handle_fast_sync(&mut self, fs: &FastSync) -> (bool, Option<AcceptDecide<T>>) {
        let md = self.metadata_log.get(fs.log_index);
        match md {
            None => return (false, None),
            Some(md) => {
                if md.id == (fs.client_id, fs.request_id) && md.deadline == fs.deadline {
                    return (true, None); // in sync
                } else {
                    // can we find the correct value in the late buffer
                    let key = (fs.client_id, fs.request_id);
                    match self.late_buffer.remove(&key) {
                        None => return (false, None), //oh fuck
                        Some(missed_log_entry) => {
                            // update log
                            let meta = DomMetadata {
                                id: missed_log_entry.id,
                                deadline: fs.deadline,
                            };
                            self.metadata_log[fs.log_index] = meta;
                            // update so the entry has the new deadline
                            let updated_log_entry = AcceptDecide {
                                deadline: fs.deadline,
                                dom_hash: 0, // fast-path sync: receiver computes hash
                                ..missed_log_entry
                            };
                            return (false, Some(updated_log_entry));
                        }
                    }
                }
            }
        }
    }

    /// Returns the prefix hash for `accepted_idx`.
    ///
    /// `release_message` (called by `tick` before this point) already ran
    /// `append_metadata` for the current entry, so `last_log_hash` and
    /// `log_hashes` are up to date.  We must NOT call `append_metadata` again
    /// here: if the node missed earlier entries and caught up via AcceptSync its
    /// `metadata_log` is shorter than `accepted_idx`, and a second XOR of the
    /// same metadata would cancel out, returning a stale hash.
    pub fn record_accepted_metadata(&mut self, accepted_idx: usize) -> u64 {
        self.get_hash_at(accepted_idx).unwrap_or(self.last_log_hash)
    }

    /// Releases a message from the queue if its deadline has passed
    /// Puts some metadata into the log for use during sync
    /// Sequence Paxos needs to get the hash value after this, as this updates the hash value
    pub fn release_message(&mut self) -> Option<AcceptDecide<T>> {
        let nxt_deadline = self.peek_next_deadline();
        match nxt_deadline {
            None => None,
            Some(nxt_deadline) => {
                if nxt_deadline <= self.sim_clock.get_time() {
                    let nxt_msg = self
                        .early_buffer
                        .pop()
                        .expect("No messages on DOM message release timer");
                    self.append_metadata(DomMetadata {
                        id: nxt_msg.id,
                        deadline: nxt_msg.deadline,
                    });
                    self.last_released_timestamp = nxt_msg.deadline;
                    return Some(nxt_msg);
                } else {
                    None
                }
            }
        }
    }

    /// Returns the expected max one way delay to be used as a deadline
    pub fn get_deadline(&mut self) -> i64 {
        self.sim_clock.get_time() + 50
    }

    /// lets us see the next deadline if there is a message in the early queue
    pub fn peek_next_deadline(&mut self) -> Option<i64> {
        let next_fp = self.early_buffer.peek()?;
        Some(next_fp.deadline)
    }

    /// Returns the metadata prefix hash recorded at `accepted_idx`, if present.
    pub fn get_hash_at(&self, accepted_idx: usize) -> Option<u64> {
        accepted_idx
            .checked_sub(1)
            .and_then(|idx| self.log_hashes.get(idx).copied())
    }

    fn fast_quorum_size(cluster_size: usize) -> usize {
        let non_leader_nodes = cluster_size.saturating_sub(1);
        1 + ((3 * non_leader_nodes + 3) / 4)
    }

    fn append_metadata(&mut self, meta: DomMetadata) {
        self.generate_log_hash(&meta);
        self.metadata_log.push(meta);
        self.log_hashes.push(self.last_log_hash);
    }

    fn generate_log_hash(&mut self, prop_val: &DomMetadata) {
        self.last_log_hash = self.last_log_hash ^ calculate_hash(prop_val);
    }
}
