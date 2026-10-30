# Lightweight Transactions in Scylla

The Lightweight Transactions (LWTs) are used in cases where 'linearizable' consistency model is required. The LWT implementation in Scylla relies on Paxos algorithm to achieve it.

The scope of Paxos is partition key. Each LWT request works with only one partition key, accessing many partitions in a single LWT request is not supported. LWT requests to different partitions are completely independent of each other and are executed in parallel. LWT requests to the same partition wait for each other if the same node handles them. Concurrent LWTs to the same partition from different nodes run in parallel. The semantics of such execution is specified by Paxos. Note that the same rules apply for requests to different clustering keys in the same partition.

A Scylla node maintains a local paxos state for each partition key in system.paxos table. This table uses the partition key from the base table as its own partition key. This ensures that all reads and writes to this table are local to the shard, handling LWT request for that partition key. This is important to avoid cross-shard accesses in cases when both system.paxos and the base table need to be accessed in the same request.

The goal of Paxos algorithm is to ensure consistency. If two different users observe the same state at the same time and want to make their own modifications, it must be guaranteed that either the first or the second modification goes first, and the other modification sees the effects of the first one. This requirement is known as linearizable consistency model. Scylla provides this consistency model only for LWT requests. If one user runs LWT requests and another accesses the same data through regular quorum reads and writes, linearizability is not guaranteed.

Modifications are called proposals in Paxos. Each proposal contains a mutation and a timestamp, which is called a ballot. Any node (LWT coordinator) can act as a 'proposer' -- by sending a new proposal to the replica set it _proposes_ its own new state (represented by the mutation inside the proposal) for the replica set to commit as its new acknowledged state.

The LWT entry point is `storage_proxy::cas` method. It takes the following parameters:
	partition_ranges
		Ranges of the token ring, affected by the CAS operation. Must contain exactly one partition.
	read_command
		Describes what input data (clustering keys and columns) we need to read from the nodes to be able to produce the result mutation. For example, consider the query "UPDATE t SET c=3 WHERE p=1 AND c=2 IF c = 2;", here p and c -- partition and clustering key for table t. To hadle this request we would need to run Paxos for partition p=1. The read_command in this case would specify that we only need the column c.
	cl_for_paxos
		Can be one of two values: consistency_level::SERIAL or consistency_level::LOCAL_SERIAL. The second option is an optimization -- only DC-local replicas will be considered for quorums in both prepare and accept Paxos stages.
	cl_for_learn
		Separate cl for the learn stage. Can be any valid cl, except consistency_level::SERIAL or consistency_level::LOCAL_SERIAL.
This method is used in several different contexts:
	* Single LWT requests.
	* Batch LWT requests. One LWT batch can access several clustering keys in the same partition.
	* Serial read requests (https://opensource.docs.scylladb.com/stable/features/lwt.html#reading-with-paxos) -- regular SELECT queries that explicitly set consistency_level to SERIAL or LOCAL_SERIAL.
	* Alternator (DynamoDB/Scylla adapter layer).

## Classical Paxos

Let's consider first the classical Paxos algorithm from the famous "Paxos Made Simple" paper.

Simple quorum-based communication between coordinator and other replicas is not enough to provide linearizability. Why? Suppose a client reads the current value for the key with quorum and computes a mutation based on it, which represents a new value. How could we commit the new value? We can't just blindly write it with quorum, since then we might overwrite some other modification from a different client. Instead of writing to the base table, each replica might temporarily 'stash' the new value in system.paxos and report to the coordinator if the current value in the base table is still the same as it was at the moment of quorum read. How then move this value to the base table? If the LWT coordinator collected a quorum of confirmations from the nodes that the value hasn't changed, this doesn't prevent concurrent paxos reads and writes to the same key, so the current value can be changed right at the moment we decide to move it to the base table! What we want is atomicity -- as soon as a quorum of replicas decided to accept a new value, there should be no way for other proposers to make the replica set to accept their value instead of ours.

This is where Paxos comes into play. It runs in two phases -- prepare and accept. At the first prepare phase, instead of stashing the entire mutation, the node remembers the ballot, i.e. the timestamp of the proposed modification. Such a ballot is called a promised ballot. A node promises not to accept (and prepare) any proposals with smaller ballot. And here's the trick -- if the node sees that some other proposal with a bigger ballot was accepted by this node, then it returns this accepted proposal, and the LWT coordinator is obliged to 'push forward' this proposal instead of its own. More specifically, the LWT coordinator runs prepare phase on all replicas, collects the quorum and looks at the maximum ballot of the accepted proposals that they report. If this maximum is greater than its own ballot, then the coordinator uses that maximum accepted proposal instead of its own.

The accept phase just sends accept to all replicas and waits for a quorum. The value is considered committed as soon as it's accepted by a quorum of replicas. Learn operation is reponsible for handling a new committed/learned value. Here's a pseudocode of this algorithm:

```python
from typing import Optional, List, Tuple
from dataclasses import dataclass

@dataclass
class Proposal:
    number: int
    value: Optional[str] = None

class Acceptor:
    def __init__(self):
        self.promised_number = None  # Highest prepare number promised
        self.accepted_proposal = None  # Accepted proposal (number, value)

    def on_prepare(self, proposal_number: int) -> Tuple[bool, Optional[Proposal]]:
        if self.promised_number is None or proposal_number > self.promised_number:
            self.promised_number = proposal_number
            return True, self.accepted_proposal  # Promise granted
        return False, None  # Promise denied

    def on_accept(self, proposal: Proposal) -> bool:
        if self.promised_number is None or proposal.number >= self.promised_number:
            self.promised_number = proposal.number
            self.accepted_proposal = proposal
            return True  # Accepted
        return False  # Rejected

class Proposer:
    def __init__(self, proposer_id: int, acceptors: List[Acceptor]):
        self.proposer_id = proposer_id
        self.acceptors = acceptors
        self.proposal_number = proposer_id  # Start with unique ID-based number
        self.quorum = len(acceptors) // 2 + 1  # Majority

    def prepare(self, value: str) -> bool:
        self.proposal_number += 10  # Increment to ensure uniqueness (simplified)
        promises = 0
        highest_accepted = None

        for acceptor in self.acceptors:
            promise, accepted_proposal = acceptor.on_prepare(self.proposal_number)
            if promise:
                promises += 1
                if accepted_proposal and (highest_accepted is None or 
                                         accepted_proposal.number > highest_accepted.number):
                    highest_accepted = accepted_proposal

        if promises >= self.quorum:
            # If we have a majority, pick value: use highest accepted or our own
            self.value = highest_accepted.value if highest_accepted else value
            return True
        return False

    def accept(self) -> bool:
        """Phase 2: Send accept requests with chosen value."""
        accepts = 0
        proposal = Proposal(self.proposal_number, self.value)

        for acceptor in self.acceptors:
            if acceptor.on_accept(proposal):
                accepts += 1

        return accepts >= self.quorum  # Consensus if majority accepts

class Learner:
    def __init__(self):
        self.learned_value = None

    def learn(self, value: str):
        self.learned_value = value
        print(f"Learned consensus value: {value}")

def run_paxos():
    acceptors = [Acceptor() for _ in range(5)]  # 5 acceptors
    proposer = Proposer(proposer_id=1, acceptors=acceptors)
    learner = Learner()

    # Proposer tries to propose a value
    if proposer.prepare("ValueA"):
        if proposer.accept():
            learner.learn(proposer.value)

if __name__ == "__main__":
    run_paxos()
```

## Scylla Paxos

Scylla Paxos implementation is more elaborate than the base scheme outlined above, and its inner logic is quite different.

The system.paxos table, besides the standard last promised ballot and last accepted proposal, also stores a most_recent_commit field. This field is used as an opmitization. The learn phase stores the committed value in both the base table and in this field. When the next prepare for this key comes to the node later, this committed value is returned. This allows the coordinator to avoid a separate quorum read in storage_service::cas, provided that the prune process has not yet removed this field. The prune is started when the learn is confirmed from the quorum of nodes. This means that this optimization doesn't apply if LWT requests are rare.

The `storage_proxy::cas` consists of the following main blocks:

1. `begin_and_repair_paxos`

2. quorum_read based on the partition_key and read_command

3. create mutation based on the read data from 2

4. create a proposal based on the ballot from 1 and mutation from 3

5. try to accept the proposal, learn if accepted, sleep/retry if failed

`begin_and_repair_paxos` is similar in purpose to the prepare phase from the Simple Paxos above, but its logic and invariants are very different.

It runs a loop, on each iteration it chooses a new wall-clock based ballot and tries to find a quorum of replicas that satisfy the following three invariant:
* no replica has promised a greater or equal ballot
* there were no previously accepted proposals on any replicas OR such an accepted proposal is greater than the most recently committed ballot
* there are no not-pruned commited proposals OR all replicas agree on its ballot

* It retries with a newer ballot as soon as it gets a first prepare reject, not trying to collect a (still possible) quorum of prepare. This catches the situation when some other proposal with higher ballot is already under way, so we give up early to not compete with it.

The prepare_summary structure:
	most_recent_commit
		is populated with the most recent value from the most_recent_commit column from the replicas, participated in the prepare round. If prune finished for this partition key, then this column will not be set on the replicas and in summary.most_recent_commit field.

	most_recent_proposal
		is populated with the most recent value from the accept_proposal column from the replicas, participated in the prepare round.

## Questions:

* Why we use db::consistency_level::ANY in repair code in begin_and_repair_paxos? Doesn't this mean that we only wait for one replica to confirm? This will be a different replica from the replica where we get the most recent commit in the first place, so in fact we get two confirmations, but it's still not enough for a quorum on a larger replica sets. It's not guaranteed that the quorum read for CAS conditions in storage_proxy::cas would return the latest value.

* Why we run "Finishing incomplete paxos round" code in begin_and_repair_paxos based on most_recent_commit? Doesn't this mean that we'll run it practically all the time? (summary.most_recent_commit is not set since prune clears it and most_recent_proposal is set since there were some accepted proposals for the key).

* How liveness is guaranteed in the condition for "Finishing incomplete paxos round"? What if prune will be able to clear the most_recent_commit column on all nodes before we reach the next iteration in the top-level loop in begin_and_repair_paxos?

* Why don't we update the `min_timestamp_micros_to_use` in `begin_and_repair_paxos` when `summary.promised` is false? This means that the call `summary.update_most_recent_promised_ballot(response);` when a replica rejects a proposal has no any practical effects. Why do we sleep for up to 50 millis? This is a huge amount of time, purely wasted.