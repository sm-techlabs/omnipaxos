// TODO:
// Replace OmniPaxosMessage with <T>: Entry


use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;
// use crate because not binary target (some languages make imports very difficult)
use crate::messages::sequence_paxos::{AcceptDecide, FastReply, FastSync};
use crate::simulated_clock::ClockState;
use crate::storage::Entry;

/// This stores meta data that is used during the sync operation.
pub struct DomMetadata {
    id: (u64, u64),
    deadline: i64,
}

// TODO:
// generateLogHash()
// sendSync({view-id, client-id,request-id,deadline,log-id})
// handleSync({view-id, client-id,request-id,deadline,log-id}) -> 
// lateBufferLookup({request-id, client-id})
/// Deadline Ordered M
pub struct DOM<T> 
where 
    T: Entry
{
    early_buffer: BinaryHeap<AcceptDecide<T>>,
    late_buffer: HashMap<(u64, u64), AcceptDecide<T>>,
    sim_clock: ClockState,
    last_released_timestamp: i64,
    last_log_hash: u64,
    fast_reply_tracker: HashMap<(u64, u64), u64>,
    fast_quorum_size: u64,
    metadata_log: Vec<DomMetadata>
}

impl<T> DOM<T> 
where 
    T: Entry
{
    /// Returns a new DOM
    pub fn new(fqs: u64) -> DOM<T> {
        return DOM {
            early_buffer: BinaryHeap::new(),
            late_buffer: HashMap::new(),
            sim_clock: ClockState::new(0, 100, 100, 10),
            last_released_timestamp: 0,
            last_log_hash: 0,
            fast_reply_tracker: HashMap::new(),
            fast_quorum_size: fqs, 
            metadata_log: Vec::new(),
        }
    }

    /// Handles a fast path propose
    pub fn handle_fast_propose(&mut self, ac: AcceptDecide<T>) {
        if ac.deadline > self.last_released_timestamp {
            self.early_buffer.push(ac);
        } else {
            self.late_buffer.insert(ac.id, ac);
        }
    }

    /// Handles a fast path reply
    /// Returns true when the number of messages meets or exceeds fast quorum size reqs
    pub fn handle_fast_reply(&mut self, fr: &FastReply<T>) -> bool {
        // hash value must match to know the proposed value is stored in a correct replica
        if fr.hash != self.last_log_hash {
            return false
        }
        let num_replies = self.fast_reply_tracker
            .entry((fr.replica_id, fr.request_id))
            .and_modify(|count| *count += 1)
            .or_insert(1);
        if *num_replies >= self.fast_quorum_size {
            return false;
        } else {
            return false;
        }
    }

    /// Handles a fast sync message 
    /// Compares the value at log_id to the metadata in the sync message
    pub fn handle_fast_sync(&mut self, fs: &FastSync) -> bool {
        let md = self.metadata_log.get(fs.log_index);
        match md {
            None => return false,
            Some(md) => {
                if md.id == (fs.client_id, fs.request_id) && md.deadline == fs.deadline {
                    return true; // in sync
                } else {
                    // can we find the correct value in the late buffer
                    let key = (fs.client_id, fs.request_id);
                    match self.late_buffer.remove(&key) {
                        None => return false, //oh fuck
                        Some(missed_log_entry) => {
                            // update log
                        }
                    }
                }
            }
        }
        return false;
    }

    /// Releases a message from the queue if its deadline has passed
    /// Puts some metadata into the log for use during sync
    pub fn release_message(&mut self) -> AcceptDecide<T> {
        let nxt_msg = self.early_buffer.pop().expect("No messages on DOM message release timer");
        let meta = DomMetadata{
            id: nxt_msg.id,
            deadline: nxt_msg.deadline,
        };
        self.metadata_log.push(meta);
        return nxt_msg;
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
}
