# Invariant checking in scylla

Non obvious invariants should be checked. There are three ways of
doing it, asserts, throwing exceptions and logging failures.

If an invariant is critical for the stability of the system, use an
assert. For example, a broken invariant in the memory allocator can
lead to data corruption and there is no way to work around it, so
assert.

If an invariant is needed only by some features and the system can
shutdown cleanly without it, then a throw is appropriate. If the issue
was found while serving a user request the exception is propagated
back to the user. It if was a scylla internal operation, it will still
bring down the node.

When the operation can just be ignored, logging the failure is the
best option. The canonical example is when an element is "known" to be
in a container, but while trying to remove it we find out that it was
not there.
