Update the existing codebase to satisfy the following fast path event flow:

Fast path event flow:
1. Client request -> Coordinator Node
2. ⁠Coordinator Broadcasts to Everyone 
3. ⁠Nodes put in Early Buffer -> Release on Deadline -> Insert to Log (Not Decided - Just Accepted)
4a. ⁠Send <FastReply> to Coordinator⁠ (Leader includes which index it appends it to, to prevent ordering conflicts from multiple coordinators)
  4b. ⁠Send <FastAccepted> to Leader
5. ⁠Coordinator gathers super quorum of fast replies, advances its decided_idx, responds to client request (1 RTT)
6. On each <FastAccepted> received, the leader checks if (super) quorum has been reached, for an accepted_idx that is > than the current decided_idx. 
7. ⁠When that condition is met, Leader sends <Decide> to all.

If two coordinators happen to get a client request and attach the exact same deadline, and a node receives 2 messages with the same deadline, we can tiebreak based on pid / node id.

The formula for a super quorum in the fast path, in a cluster of N nodes  where f = N/2: f + f/2 + 1 (must include leader)

Make sure you enrich processes with optional but rich logging/debugging messages, with important information, and visual clarity, such as [SEND], [RECV], [INFO].

Our changes are mainly made under the src/sequence_paxos directory, in the follower, leader, and mod.rs files. Do not make unnecessary changes to preexisting structures. Only touch what absolutely needs to be touched, for the purposes of implementing this flow.

After implementing the flow, you need to validate it by writing a dom_integration test suite. It should work with the existing Kompact setup, and correctly simulate multiple nodes, and their message exchange. 

You should not make any assumptions on your own. If anything is unclear, you should ask clarifying questions, and I will provide you with an answer.