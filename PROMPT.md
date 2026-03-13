Analyze my codebase thoroughly. You are to figure out 2 things: A) how does the fast path work? Is it implemented correctly? If not, what is missing? The desired fast path flow is the following:

Fast path event flow:
1. Client request -> Coordinator Node
2. ⁠Coordinator Broadcasts to Everyone 
3. ⁠Nodes put in Early Buffer -> Release on Deadline -> Insert to Log (Not Decided - Just Accepted)
4a. ⁠Send <FastReply> to Coordinator⁠ (Leader includes which index it appends it to, to prevent ordering conflicts from multiple coordinators)
  4b. ⁠Send <FastAccepted> to Leader
5. ⁠Coordinator gathers super quorum of fast replies, advances its decided_idx, responds to client request (1 RTT)
6. On each <FastAccepted> received, the leader checks if (super) quorum has been reached, for an accepted_idx that is > than the current decided_idx. 
7. ⁠When that condition is met, Leader sends <Decide> to all.

B) You are to figure out the "slow" path of
the system. How is it supposed to reconcile when the fast path has gone wrong? From a quick look, the self.reconnect function does not seem suitable, as it will only trigger the reconciliation phase if the node has an outdated ballot
number. But in our case, it might be needed if it has a divergent log under the same leader.

After you have reasoned about these things, write a brief action plan, detailing your descoveries in a file called ACTION_PLAN.md in the root of the repository. You should also describe how integration tests need to be implemented,
in order to test the desired flow. If there are any questions, ask for clarifications.

If two coordinators happen to get a client request and attach the exact same deadline, and a node receives 2 messages with the same deadline, we can tiebreak based on pid / node id.

The formula for a super quorum in the fast path, in a cluster of N nodes  where f = N/2: f + f/2 + 1 (must include leader)

Make sure you enrich processes with optional but rich logging/debugging messages, with important information, and visual clarity, such as [SEND], [RECV], [INFO].

Our changes are mainly made under the src/sequence_paxos directory, in the follower, leader, and mod.rs files. Do not make unnecessary changes to preexisting structures. Only touch what absolutely needs to be touched, for the purposes of implementing this flow.

After implementing the flow, you need to validate it by writing a dom_integration test suite. It should work with the existing Kompact setup, and correctly simulate multiple nodes, and their message exchange. 

You should not make any assumptions on your own. If anything is unclear, you should ask clarifying questions, and I will provide you with an answer.