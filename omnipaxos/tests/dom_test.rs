mod utils;
use omnipaxos::dom::DOM;
use omnipaxos::messages::sequence_paxos::{AcceptDecide, FastReply, FastSync};
use omnipaxos::util::SequenceNumber;
use omnipaxos::ballot_leader_election::Ballot;
use crate::utils::Value;


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