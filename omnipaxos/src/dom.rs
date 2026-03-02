// TODO:
// Replace OmniPaxosMessage with <T>: Entry


use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;
// use crate because not binary target (some languages make imports very difficult)
use crate::messages::sequence_paxos::{AcceptDecide, FastReply, FastSync};
use crate::simulated_clock::ClockState;
use crate::storage::Entry;


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
    last_released_timestamp: u64,
    last_log_hash: u64,
}

impl<T> DOM<T> 
where 
    T: Entry
{
    /// Returns a new DOM
    pub fn new() -> DOM<T> {
        return DOM {
            early_buffer: BinaryHeap::new(),
            late_buffer: HashMap::new(),
            sim_clock: ClockState::new(0, 100, 100, 10),
            last_released_timestamp: 0,
            last_log_hash: 0,
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

    /// Handles a fast path reply, not implemented
    pub fn handle_fast_reply(&mut self, fr: FastReply<T>) {
        return
    }

    /// Handles a fast sync message, not implemented
    pub fn handle_fast_sync(&mut self, fs: FastSync) {
        return
    }

    /// Releases a message from the queue if its deadline has passed
    pub fn release_message(&mut self) -> AcceptDecide<T> {
        self.early_buffer.pop().expect("No messages on DOM message release timer")
    }

    /// Returns the expected max one way delay to be used as a deadline
    pub fn get_one_way_delay(&mut self) -> u64 {
        return 50;
    }

    // pub fn peek_next_deadline(&mut self) -> Option<Instant> {
    //     let next_nc = self.early_buffer.peek()?;
    //     // decide if we are doing micros or millis, current micros
    //     let delta = (next_nc.deadline() - self.sim_clock.get_time()).max(0);
    //     Some(Instant::now() + Duration::from_micros(delta.try_into().unwrap()))
    // }
}
