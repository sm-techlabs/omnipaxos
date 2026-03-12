mod utils;
use omnipaxos::dom::DOM;
use omnipaxos::messages::sequence_paxos::{AcceptDecide, FastReply, FastSync};
use omnipaxos::util::SequenceNumber;
use omnipaxos::ballot_leader_election::Ballot;
use crate::utils::Value;
use std::time::{SystemTime, UNIX_EPOCH};


#[test]
fn test_handle_fast_propose_adds_to_early_buffer() {
    // Arrange: Initialize a DOM with a fast quorum size of 3
    let mut dom = DOM::<Value>::new(3);

    // Since dom.last_released_timestamp is initialized to 0, 
    // any deadline > 0 guarantees it goes into the early_buffer.
    let target_deadline = 100; 

    // Create a mock AcceptDecide message.
    // Note: You will need to fill in any other required fields that 
    // exist on your actual AcceptDecide struct (like n, seq_num, etc.)
    let ac_message = AcceptDecide {
        id: (1, 1),
        deadline: target_deadline,
        seq_num: SequenceNumber {
            session: 1,
            counter: 2,
        },
        decided_idx: 5,
        entries: vec![
            Value::with_id(1),
            Value::with_id(2),
            Value::with_id(3),
            Value::with_id(4),
            Value::with_id(5),
            Value::with_id(6),
        ],
        n: Ballot::with(0, 0, 0, 0),
    };

    // Act: Handle the proposal
    dom.handle_fast_propose(ac_message);

    // Assert: Verify the message is in the early_buffer by checking the next deadline.
    // peek_next_deadline() looks at the early_buffer's BinaryHeap.
    assert_eq!(
        dom.peek_next_deadline(), 
        Some(target_deadline),
        "The message should be in the early_buffer, making its deadline the next available."
    );
}


#[test]
fn test_handle_fast_propose_adds_to_late_buffer() {
    // Arrange: Initialize a DOM with a fast quorum size of 3
    let mut dom = DOM::<Value>::new(3);

    // Since dom.last_released_timestamp is initialized to 0, 
    // any deadline > 0 guarantees it goes into the early_buffer.
    let target_deadline = 100; 
    // Create a mock AcceptDecide message.
    // Note: You will need to fill in any other required fields that 
    // exist on your actual AcceptDecide struct (like n, seq_num, etc.)
    let ac_message = AcceptDecide {
        id: (1, 1),
        deadline: target_deadline,
        seq_num: SequenceNumber {
            session: 1,
            counter: 2,
        },
        decided_idx: 5,
        entries: vec![
            Value::with_id(1),
            Value::with_id(2),
            Value::with_id(3),
            Value::with_id(4),
            Value::with_id(5),
            Value::with_id(6),
        ],
        n: Ballot::with(0, 0, 0, 0),
    };
    let b_message = AcceptDecide {
        id: (1, 1),
        deadline: target_deadline - 50,
        seq_num: SequenceNumber {
            session: 1,
            counter: 2,
        },
        decided_idx: 5,
        entries: vec![
            Value::with_id(1),
            Value::with_id(2),
            Value::with_id(3),
            Value::with_id(4),
            Value::with_id(5),
            Value::with_id(6),
        ],
        n: Ballot::with(0, 0, 0, 0), 
    };

    // Act: Handle the proposal
    dom.handle_fast_propose(ac_message);
    dom.release_message();
    dom.handle_fast_propose(b_message);
    assert_eq!(dom.late_buffer.len(), 1);
}


#[test]
fn test_release_no_msg_past_deadline() {
    // Arrange: Initialize a DOM with a fast quorum size of 3
    let mut dom = DOM::<Value>::new(3);

    // Since dom.last_released_timestamp is initialized to 0, 
    // any deadline > 0 guarantees it goes into the early_buffer.
    let current_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time is broken").as_micros();
    let target_deadline = (current_time + 50000) as i64; // 5 ms  

    // Create a mock AcceptDecide message.
    // Note: You will need to fill in any other required fields that 
    // exist on your actual AcceptDecide struct (like n, seq_num, etc.)
    let ac_message = AcceptDecide {
        id: (1, 1),
        deadline: target_deadline,
        seq_num: SequenceNumber {
            session: 1,
            counter: 2,
        },
        decided_idx: 5,
        entries: vec![
            Value::with_id(1),
            Value::with_id(2),
            Value::with_id(3),
            Value::with_id(4),
            Value::with_id(5),
            Value::with_id(6),
        ],
        n: Ballot::with(0, 0, 0, 0),
    };

    // Act: Handle the proposal
    dom.handle_fast_propose(ac_message);

    // Assert: Verify the message is in the early_buffer by checking the next deadline.
    // peek_next_deadline() looks at the early_buffer's BinaryHeap.
    assert_eq!(
        dom.peek_next_deadline(), 
        Some(target_deadline),
        "The message should be in the early_buffer, making its deadline the next available."
    );
    let nxt_msg = dom.release_message();
    assert_eq!(nxt_msg, None);
}

#[test]
fn test_fast_reply_handler() {
    // Arrange: Initialize a DOM with a fast quorum size of 3
    let mut dom = DOM::<Value>::new(3);
    let request_id: u64 = 123;
    let bal = Ballot::with(0, 0, 0, 0);
    let hash = 123456;
    // 2 fast replies from followers
    let fr_follower1 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 1,
        result: None,
        hash: hash,
    };
    let fr_follower2 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 2,
        result: None,
        hash: hash,
    };
    // one fast reply from leader
    let fr_leader = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 0,
        result: None,
        hash: hash,
    };

    let rfr1 = dom.handle_fast_reply(fr_follower1, false);
    let rfr2 = dom.handle_fast_reply(fr_follower2, false);
    let rfr3 = dom.handle_fast_reply(fr_leader, true);
    assert_eq!(rfr1, false);
    assert_eq!(rfr2, false);
    assert_eq!(rfr3, true);
}

#[test]
fn test_fast_reply_handler_incorrect_hash() {
    // Arrange: Initialize a DOM with a fast quorum size of 3
    let mut dom = DOM::<Value>::new(3);
    let request_id: u64 = 123;
    let bal = Ballot::with(0, 0, 0, 0);
    let hash = 123456;
    // 2 fast replies from followers
    let fr_follower1 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 1,
        result: None,
        hash: hash,
    };
    let fr_follower2 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 2,
        result: None,
        hash: 54321,
    };
    // one fast reply from leader
    let fr_leader = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 0,
        result: None,
        hash: hash,
    };

    let rfr1 = dom.handle_fast_reply(fr_follower1, false);
    let rfr2 = dom.handle_fast_reply(fr_follower2, false);
    let rfr3 = dom.handle_fast_reply(fr_leader, true);
    assert_eq!(rfr1, false);
    assert_eq!(rfr2, false);
    assert_eq!(rfr3, false);
}

#[test]
fn test_fast_reply_handler_leader_first() {
    // Arrange: Initialize a DOM with a fast quorum size of 3
    let mut dom = DOM::<Value>::new(3);
    let request_id: u64 = 123;
    let bal = Ballot::with(0, 0, 0, 0);
    let hash = 123456;
    // 2 fast replies from followers
    let fr_follower1 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 1,
        result: None,
        hash: hash,
    };
    let fr_follower2 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 2,
        result: None,
        hash: hash,
    };
    // one fast reply from leader
    let fr_leader = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 0,
        result: None,
        hash: hash,
    };

    let rfr1 = dom.handle_fast_reply(fr_leader, true);
    let rfr2 = dom.handle_fast_reply(fr_follower2, false);
    let rfr3 = dom.handle_fast_reply(fr_follower1, false);
    assert_eq!(rfr1, false);
    assert_eq!(rfr2, false);
    assert_eq!(rfr3, true);
}

#[test]
fn test_fast_reply_handler_quorum_too_small() {
    // Arrange: Initialize a DOM with a fast quorum size of 4
    let mut dom = DOM::<Value>::new(4);
    let request_id: u64 = 123;
    let bal = Ballot::with(0, 0, 0, 0);
    let hash = 123456;
    // 2 fast replies from followers
    let fr_follower1 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 1,
        result: None,
        hash: hash,
    };
    let fr_follower2 = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 2,
        result: None,
        hash: hash,
    };
    // one fast reply from leader
    let fr_leader = FastReply::<Value> {
        n: bal,
        request_id: request_id,
        replica_id: 0,
        result: None,
        hash: hash,
    };

    let rfr1 = dom.handle_fast_reply(fr_leader, true);
    let rfr2 = dom.handle_fast_reply(fr_follower2, false);
    let rfr3 = dom.handle_fast_reply(fr_follower1, false);
    assert_eq!(rfr1, false);
    assert_eq!(rfr2, false);
    assert_eq!(rfr3, false);
}