# Qualifying replicas for read operations

## Intro

When a read request arrives at the coordinator node, it is later
forwarded to one or more replicas in order to read the actual data
and return it back to the client. Each request has an associated
timeout, which indicates how long the client is willing to wait
for the query results.
This timeout information can be leveraged to lift some pressure off
a replica which is overloaded or dead.

## Gathering last seen information

The coordinator should be able to predict whether a replica is likely
to respond to a request in time or not. This prediction should be as
precise as possible, but at the same time have minimal overhead, because
performing read requests is on the hot path. A simple estimation
of how responsive a replica is likely to be is implemented as follows:
 * each time a read request is sent to a replica, a timestamp is recorded
 * each time a response is received from a replica, a timestamp is recorded
 * each time a response is received from a replica, the request round trip time
   is recorded

The difference between the two above timestamps is a cheap, rough implementation
of a failure detector - if the node hasn't responded for more than X milliseconds since
the previous request and the current request is going to time out in X milliseconds,
it gets more and more likely to be dead.
The last recorded round trip time is, in turn, a cheap, rough estimate of how long
it may take the replica to respond to the next request - assuming that the conditions
haven't changed and the requests are identical, which might be a good enough estimate
for homogenous workloads.

Finally, when a replica was not contacted in a long while (e.g. 5s),
its previous last seen information is considered stale and dropped to avoid
reporting incorrect long periods of time without response.

## Omitting replicas not likely to respond in time

Based on the last seen information, the coordinator is now allowed
to make an educated guess whether a replica is going to respond in time.
A simple, probabilistic approach is implemented as follows.
At first, we compute how much time left (`TL`) the request still has until
it reaches its deadline and times out. From the last seen information
we fetch the last time without a response, estimated by subtracting
the last sent timestamp from the last responded timestamp - `TWR`,
as well as the last round trip time for a successful request - `RTT`.
These two are used to estimate the time it will take for a replica
to respond to the next query - `EST = max(TWR, RTT)`. Maximum is used
in order to prefer `TWR` in cases when a node is dead, and `RTT` in cases when
the node regularly responds, but latency of a single request remains large.
If `TL` is larger than `EST`, then the node is likely to still respond
in time, so the request is let through. If, however, `TL < EST`,
it suggests that the replica might not respond within the deadline,
so it may be probabilistically omitted as a potential target. The chance of
omitting a replica is `(EST-TL)/TL`, which means that the chance increases
linearly along with `EST`, i.e. the longer the replica is expected to spend
on responding, the higher its chances for being qualified as unreliable.
Once the estimation reaches `2*TL`, the chance reaches almost 100% - but there
always exist a small chance (`>=0.01%`) for letting a request through in case
the replica started responding.

## So, it's a dynamic snitch?

Kind of, but it's only expected to work once the replica is likely
to not be able to handle its requests within their deadline. Ideally,
it would have no influence over how the requests are distributed
as long as replicas regularly respond to their requests in time.
A scenario in which this mechanism is expected to be particularly
effective is when a replica dies suddenly without broadcasting
that it's going to die. In this case, the failure detector can take
tens of seconds to figure out that this replica is dead, which
also means that coordinators would try to send data to it,
which results in a sharp increase of background reads, which in turn
lack a proper backpressure mechanism (at least at the time of writing
this doc). With the qualification mechanism in place, the node
is qualified as not likely to respond potentially much quicker,
especially if its requests have short timeout values (e.g. 500ms).
 
