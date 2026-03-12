use std::collections::{BinaryHeap, HashMap};
// use crate because not binary target (some languages make imports very difficult)
use crate::messages::sequence_paxos::{AcceptDecide, FastReply, FastSync};
use crate::simulated_clock::ClockState;
use crate::storage::Entry;
use std::hash::{Hash, Hasher, DefaultHasher};

/// This stores meta data that is used during the sync operation.
#[derive(Hash)]
pub struct DomMetadata {
    id: (u64, u64),
    deadline: i64,
}

/// Used to track what quorums have been reached for a given request
pub struct QuorumData<T> 
where 
    T: Entry
{
    fast_response: u64,
    slow_response: u64,
    leader_response: bool,
    hash_value: u64,
    unhandled_replies: Vec<FastReply<T>>,
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
    T: Entry
{
    early_buffer: BinaryHeap<AcceptDecide<T>>,
    /// late buffer
    pub late_buffer: HashMap<(u64, u64), AcceptDecide<T>>,
    sim_clock: ClockState,
    last_released_timestamp: i64,
    /// hash value
    pub last_log_hash: u64,
    fast_reply_tracker: HashMap<(u64, u64), QuorumData<T>>,
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
    pub fn handle_fast_reply(&mut self, fr: FastReply<T>, leader: bool) -> bool {
        let key = (0, fr.request_id);
        let qd = self.fast_reply_tracker
            .entry(key)
            .or_insert(QuorumData { 
                fast_response: 0, 
                slow_response: 0, 
                leader_response: false, 
                hash_value: 0, 
                unhandled_replies: Vec::new(),
            });
        if leader {
            qd.fast_response += 1;
            qd.leader_response = true;
            qd.hash_value = fr.hash;
        } else {
            qd.unhandled_replies.push(fr);
        }
        if qd.leader_response {
            while let Some(fr) = qd.unhandled_replies.pop() {
                if fr.hash == qd.hash_value {
                    qd.fast_response += 1;
                }
            }
            if qd.fast_response + qd.slow_response >= self.fast_quorum_size {
                return true;
            } else {
                return false;
            }
            
        } else {
            return false;
        }
    }

    /// Fake function to integrate slow responses for testing the 
    /// fast reply handler
    pub fn fake_increment_slow_replies(pid: u64) {

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
                            let meta = DomMetadata{
                                id: missed_log_entry.id,
                                deadline: fs.deadline,
                            };
                            self.metadata_log[fs.log_index] = meta;
                            // update so the entry has the new deadline 
                            let updated_log_entry = AcceptDecide{
                                deadline: fs.deadline,
                                ..missed_log_entry
                            };
                            return (false, Some(updated_log_entry));
                        }
                    }
                }
            }
        }
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
                    let nxt_msg = self.early_buffer.pop().expect("No messages on DOM message release timer");
                    let meta = DomMetadata{
                        id: nxt_msg.id,
                        deadline: nxt_msg.deadline,
                    };
                    // log is updated here!!!
                    self.generate_log_hash(&meta);
                    self.metadata_log.push(meta);
                    self.last_released_timestamp = nxt_msg.deadline;
                    return Some(nxt_msg);
                } else {
                    None
                }
            },
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

    fn generate_log_hash(&mut self, prop_val: &DomMetadata) {
        self.last_log_hash = self.last_log_hash ^ calculate_hash(prop_val);
    }
}