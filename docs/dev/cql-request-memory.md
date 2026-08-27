# CQL request memory

The coordinator reserves a share of each shard's memory for admitting client
requests, and refuses to start more requests than that share can pay for. The
reservation covers the request bytes, the response bytes and the coordinator-side
bookkeeping in between, and it is held from the moment a frame header is read
until the response has been written to the socket.

`cql_request_memory_fraction` sets the size of that budget, as a fraction of
shard memory. It also caps how large a single request may be: a frame whose
estimated cost exceeds the whole budget is rejected with
`request size too large`. Changing it requires a restart.

## Per service level

The budget is not one pool. It is divided between *tenants*, one per scheduling
group, which in practice means one per service level:

* a **dedicated share** per service level, `1 - cql_request_memory_shared_pool_fraction`
  of the budget split in proportion to each service level's `SHARES`. Nothing
  outside that service level can take it.
* a **shared pool**, the remaining `cql_request_memory_shared_pool_fraction`,
  which any service level may borrow from once its own share runs out. It is
  first come, first served, and is meant to absorb bursts.

So a flood of requests in one service level can slow that service level down,
but it cannot stop another one from being served. `cql_request_memory_shared_pool_fraction`
can be changed at runtime; 0 gives every service level a strictly private budget.

A connection picks its tenant when its scheduling group is decided, which is
after authentication. Before that — and for any scheduling group that has no
service level of its own — requests are charged to the default service level's
tenant. Alternator is charged there too: it has to reserve memory before the
signature is verified, so it does not know the user yet.

A tenant appears when the service level controller announces its service level.
At startup that is when the controller seeds its cache from the system tables,
which is later in the boot sequence than the limiter itself starts but still
before the CQL server accepts connections. The default service level's tenant is
the exception: the limiter creates it directly, because that service level exists
before the limiter has subscribed.

## Borrowing

A tenant is a `seastar::semaphore` whose capacity is its dedicated share and
whose *borrow source* is the shared pool (see `semaphore_borrow_source` in
seastar). Requests are paid for out of the tenant's own capacity first. When that
falls short, the whole shortfall is borrowed from the pool in one go — a partial
borrow could not admit the request anyway. Released memory repays the pool before
the tenant keeps any of it, so the pool is available again as soon as possible.

A tenant that could not be funded registers with the pool. When memory is
repaid, the pool offers it to each registered tenant in turn, bounded by how many
were registered when the pass started. One large repayment can therefore unblock
several service levels, and a tenant whose request is too big to fund does not
block a smaller one behind it — it goes to the back of the queue instead.

## Re-splitting the budget

The split is recomputed whenever a service level is added, removed or reweighted,
and whenever `cql_request_memory_shared_pool_fraction` changes. Every resize wakes
the tenants that are waiting for memory, so the reductions are applied before the
increases: memory moving from the dedicated shares into the pool has to be taken
off the shares before the pool offers it, or a waiting tenant is funded out of a
share it still holds and ends up with both.

A tenant that already handed out more than its new share keeps that memory - it
belongs to requests that are still in flight - so right after a re-split the
total in flight can exceed the budget by up to what the re-split moved. It
settles as those requests finish.

## Forward progress

A request can be larger than its service level's share plus the entire pool,
either because it is genuinely huge or because a service level was created while
it was queued and shrank everyone's share. Such a request is admitted anyway once
its tenant has nothing else outstanding, letting consumption exceed the share
rather than blocking forever. At most one such request per tenant is in flight at
a time, and the same escape is what keeps resizing the budget from parking a
queued request permanently. Reads do the same thing — see
[reader-concurrency-semaphore.md](reader-concurrency-semaphore.md).

## Removing a service level

Connections are not reclassified the moment a service level is dropped, so
requests keep arriving in its scheduling group for a while. Its tenant gives up
its dedicated share but keeps admitting out of the shared pool, and is only
destroyed once the memory it handed out has come back and no connection still
points at it. A tenant that stalled instead would hang those connections.

## Other limiters

The CQL maintenance socket has its own limiter which admits everything. It is the
operator's escape hatch, so a flood of user requests must not be able to block
it. The request size limit still applies to it.

## Metrics

Per service level, labelled by scheduling group name:

| Metric | Meaning |
| --- | --- |
| `cql_requests_memory_total` | the service level's own share |
| `cql_requests_memory_available` | how much of that share is free; negative when an oversized request was admitted |
| `cql_requests_memory_borrowed_from_shared_pool` | how much it has borrowed |
| `cql_requests_blocked_memory_current` | its requests waiting for memory right now |
| `cql_requests_blocked_memory` | its requests that ever had to wait |

For the pool: `cql_requests_shared_pool_total_memory`,
`cql_requests_shared_pool_available_memory` and
`cql_requests_shared_pool_waiting_service_levels`.

The shard-wide `requests_memory_available` and `requests_blocked_memory_current`
remain. `requests_blocked_memory_current` is a sum over the tenants;
`requests_memory_available` is that sum plus what the shared pool has not lent
out, so it still measures the whole budget, and is clamped at zero because a
tenant's own share goes negative when an oversized request is let through.
