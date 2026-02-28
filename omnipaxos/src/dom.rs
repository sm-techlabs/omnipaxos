// TODO:
// Replace OmniPaxosMessage with <T>: Entry


use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;
use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
// use crate because not binary target (some languages make imports very difficult)
use crate::common::{kv::*, messages::*, utils::Timestamp};
use crate::simulated_clock::ClockState;

use tokio::sync::mpsc;
use tokio::time::{sleep_until, Instant};
use std::sync::Arc;

// TODO:
// generateLogHash()
// sendSync({view-id, client-id,request-id,deadline,log-id})
// handleSync({view-id, client-id,request-id,deadline,log-id}) -> 
// lateBufferLookup({request-id, client-id})
pub struct DOM {
    early_buffer: BinaryHeap<FastPropose<T>>,
    late_buffer: HashMap<(i64, i64), <FastPropose<T>>,
    sim_clock: ClockState,
    last_released_timestamp: i64,
    last_log_hash: u64,
}

pub enum DomInput {
    Message(NezhaCommand),
}

impl DOM {
    pub fn new() -> DOM {
        return DOM {
            early_buffer: BinaryHeap::new(),
            late_buffer: HashMap::new(),
            sim_clock: ClockState::new(0, 100, 100, 10),
            last_released_timestamp: 0,
            last_log_hash: 0,
        }
    }

    pub fn handle_nezha_message(&mut self, nc: NezhaCommand) {
        if nc.deadline() > self.last_released_timestamp {
            self.early_buffer.push(nc);
        } else {
            self.late_buffer.insert(nc.id(), nc.msg);
        }
    }

    pub fn release_message(&mut self) -> NezhaCommand {
        self.early_buffer.pop().expect("No messages on DOM message release timer")
    }

    pub fn peek_next_deadline(&mut self) -> Option<Instant> {
        let next_nc = self.early_buffer.peek()?;
        // decide if we are doing micros or millis, current micros
        let delta = (next_nc.deadline() - self.sim_clock.get_time()).max(0);
        Some(Instant::now() + Duration::from_micros(delta.try_into().unwrap()))
    }
}

pub fn start_dom() -> mpsc::Sender<DomInput> {
    let (tx, mut rx) = mpsc::channel(1024);

    tokio::spawn(async move {
        let mut dom = DOM::new();
        let mut next_deadline: Option<Instant> = None;

        loop {
            tokio::select! {
                Some(input) = rx.recv() => {
                    match input {
                        DomInput::Message(cmd) => {
                            dom.handle_nezha_message(cmd);
                            next_deadline = dom.peek_next_deadline();
                        }
                    }
                }

                _ = async {
                    if let Some(dl) = next_deadline {
                        sleep_until(dl).await;
                    }
                }, if next_deadline.is_some() => {
                    dom.release_message();
                    next_deadline = dom.peek_next_deadline();
                }
            }
        }
    });

    tx
}