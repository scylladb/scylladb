# Per-partition rate limiting

Scylla clusters operate best when the data is spread across a large number
of small partitions, and reads/writes are spread uniformly across all shards
and nodes. Due to various reasons (bugs, malicious end users etc.) this
assumption may suddenly not hold anymore and one partition may start getting
a disproportionate number of requests. In turn, this usually leads to the owning
shards being overloaded - a scenario called "hot partition" - and the total
cluster latency becoming worse.

The _per partition rate limit_ feature allows users to limit the rate
of accepted requests on a per-partition basis. When a partition exceeds
the configured limit of operations of given type (reads/writes) per second,
the cluster will start responding with errors to some of the operations for that
partition so that, statistically, the rate of accepted requests is kept
at the configured limit. Rejected operations use less resources, therefore
this feature can help in the "hot partition" situation.

_NOTE_: this is an overload protection mechanism and may not be used to reliably
enforce limits in some situations. Due to Scylla's distributed nature,
the actual number of accepted requests depends on the cluster and driver
configuration and may be larger by a factor of RF (keyspace's replication
factor). It is recommended to set the limit to a value an order of magnitude
larger than the maximum expected per-partition throughput. See the
[Inaccurracies](#inaccurracies) section for more information.


## Usage

### Server-side configuration

Per-partition limits are set separately for reads and writes, on a per-table
basis. Limits can be set with the `per_partition_rate_limit` extension when
CREATE'ing or ALTER'ing a table using a schema extension:

```cql
ALTER TABLE ks.tbl WITH per_partition_rate_limit = {
    'max_reads_per_second': 123,
    'max_writes_per_second': 456
};
```

Both `max_reads_per_second` and `max_writes_per_second` are optional - omitting
one of them means "no limit" for that type of operation.

### Driver response

Rejected operations are reported as an ERROR response to the driver.
If the driver supports it, the response contains a scylla-specific error code
indicating that the operation was rejected. For more details about the error
code, see the [Rate limit error](./protocol-extensions.md#Rate%20limit%20error)
section in the `protocol-extensions.md` doc.

If the driver doesn't support the new error code, the `Config_error` code
is returned instead. The code was chosen in order for the retry policies
of the drivers not to retry the requests and instead propagate them directly
to the users.

## How it works

Accounting related to tracking per-partition limits is done by replicas.
Each replica keeps a map of counters which are identified by a combination
of (token, table, operation type). When the replica accounts an operation,
it increments the relevant counter. All counters are halved every second.

Depending on whether the coordinator is a replica or not, the flow is
a bit different. Here, "coordinator == replica" requirement also means
that the operation is handled on the correct shard.

Only reads and writes explicitly issued by the user are counted to the limit.
Read repair, hints, batch replay, CDC preimage query and internal system queries
are _not_ counted to the limit.

Paxos and counters are not covered in current implementation.

### Coordinator is not a replica

Coordinator generates a random number from range `[0, 1)` with uniform
distribution and sends it to replicas along with the operation request.
Each replica accounts the operation and then calculates a rejection threshold
based on the local counter value. If the number received from the coordinator
is above the threshold, the operation is rejected.

The assumption is that all replicas will converge to similar counter values.
Most of the time they will agree on the decision and not much work
will be wasted due to some replicas accepting and other rejecting.

### Coordinator is a replica

As before, the coordinator generates a random number. However, it does not
send requests to replicas immediately but rather calculates local rejection
threshold. If the number is above threshold, the whole operation is skipped
and the operation is only accounted on the coordinator. Otherwise, coordinator
proceeds with sending the requests, and replicas are told only to account
the operation but never reject it.

This strategy leads to no wasted replica work. However, when the coordinator
rejects the operation other replicas do not account it, so it may lead to
a bit more requests being accepted (but still not more than `RF * limit`).

### How to calculate rejection threshold

Let's assume the simplest case where there is only one replica. It will
increment its counter on every operation. Because all counters are halved
every second, assuming the rate of `V` ops/s the counter will eventually
oscillate between `V` and `2V`. If the limit is `L` ops/s, then we would
like to admit only `L` operation within each second - therefore the probability
should satisfy the following:

```
  L = Sum(i = V..2V) { P(i) }
```

This can be approximated with a definite integral:

```
  L = Int(x = V..2V) { P(x) }
```

A solution to this integral is:

```
  P(x) = L / (x * ln 2)
```

where `x` is the current value of the counter. This is the formula used
in the current implementation.

### Inaccurracies

In practice, RF is rarely 1 so there is more than one replica. Depending on
the type of the operation, this introduces some inaccurracies in counting.

- Writes are counted relatively well because all live replicas participate
  in a write operation, so all replicas should have an up-to-date counter
  value. Because of the "coordinator is replica" case, rejected writes
  will not be accounted on all replicas. In tests, the amount of accepted
  operations was quite close to the limit and much less than the theoretical
  `RF * limit`.
- Reads are less accurate because not all replicas may participate in a given
  read operation (this depends on CL). In the worst case of CL=ONE and
  round-robin strategy, up to `RF * limit` ops/s will be accepted. Higher
  consistencies are counted better, e.g. CL=ALL - although they are also
  susceptible to the inaccurracy introduced by "coordinator is replica" case.
- In case of non-shard-aware drivers, it is best to keep the clocks in sync.
  When the coordinator is not a replica, each replica decides whether to accept
  or not, based on the random number sent by coordinator. If the replicas have
  their clocks in sync, then their per-partition counters should have close
  values and they will agree on the decision whether to reject or not most of
  the time. If not, they will disagree more frequently which will result in
  wasted replica work and the effective rate limit will be lower or higher,
  depending on the consistency. In the worst case, it might be 30% lower or
  45% higher than the real limit.
