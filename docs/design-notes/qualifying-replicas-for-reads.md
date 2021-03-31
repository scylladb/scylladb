# Qualifying replicas for read operations

## Failure detection based on last seen information

### Intro

When a read request arrives at the coordinator node, it is later
forwarded to one or more replicas in order to read the actual data
and return it back to the client. Each request has an associated
timeout, which indicates how long the client is willing to wait
for the query results.
This timeout information can be leveraged to lift some pressure off
the coordinator if some of the target replicas are in fact dead,
having network problems, etc.

### Gathering last seen information

The coordinator should be able to predict whether a replica is likely
to respond to a request in time or not. This prediction should be as
precise as possible, but at the same time have minimal overhead, because
performing read requests is on the hot path. A simple failure detection
mechanism with high granularity is implemented as follows:
 * each time a request is sent to a replica, this attempt's timestamp
   is recorded
 * first timestamp of a streak of unresponded requests is recorded separately
   in order to see how long ago the coordinator started to try to communicate
   with a replica; example:
    1. sent request A
    2. received a response for A
    3. sent request B <-- this is the interesting timestamp, since the node
         hasn't responded at all after this request was sent
    4. sent request C
    5. sent request D
 * each time a response is received from a replica, the node is marked as responsive

The "last time a request was sent without response" metrics is a cheap, rough
implementation of a failure detector - if the node hasn't responded for more than
X milliseconds since the previous request and the current request is going
to time out in X milliseconds, the target node gets more and more likely to be
qualified as dead.

Finally, when a replica was not contacted in a long while (e.g. the full timeout period),
a communication attempt will be performed anyway, just in case the replica came back
to full responsiveness.

### Omitting replicas not likely to respond in time

Based on the last seen information, the coordinator is now allowed
to make an educated guess whether a replica is alive and ready to respond.
A simple, probabilistic approach is implemented as follows.
At first, we compute how much time left (`TL`) the request still has until
it reaches its deadline and times out. From the last seen information
we fetch the last time without any response: `TWR`.
If `TL` is larger than `TWR`, then the node is likely alive, so the request
is let through. If, however, `TL < TWR`, it suggests that the replica might
not be alive, so it may be probabilistically omitted as a potential target.
The chance of omitting a replica is `(TWR-TL)/TL`, which means that the chance
increases linearly along with the time in which the node is not responsive,
i.e. the more likely it is that the replica is dead, the higher its chances
for being qualified as unreliable.
Once the time without response reaches `2*TL`, the chance reaches almost 100%,
but there always exist a small chance (`>=0.01%`) for letting a request through
in case the replica started responding.

### Letting reads through if the consistency level might not be fulfilled

One fail-safe mechanism is in place to prevent failing requests if they still
have a chance of success. Namely, if we're about to disqualify a replica
based on this failure detector, **but** this disqualification will also
cause the request to immediately fail due to not being able to fulfill
its consistency level, the request is let through anyway.
A trivial example when this mechanism can be observed is consistency level ALL:
all replicas are required to be alive for this level to be fulfilled, so the failure
detector will not try to disqualify any replicas prematurely.

### So, it's a dynamic snitch?

Kind of, but currently it's only expected to work once the replica is
likely dead and thus not able to respond. Ideally, it would have no influence
over how the requests are distributed as long as replicas regularly respond
to their requests in time. A scenario in which this mechanism is expected to be
particularly effective is when a replica dies suddenly without broadcasting
that it's going to die. In this case, the stock failure detector can take tens
of seconds to figure out that this replica is dead, which also means that
coordinators would try to send data to it, which results in a sharp increase
of background reads, which in turn lack a proper backpressure mechanism
(at least at the time of writing this doc). With the qualification mechanism
in place, the node is qualified as not likely to respond potentially much
quicker, especially if its requests have short timeout values (e.g. 500ms).
 
