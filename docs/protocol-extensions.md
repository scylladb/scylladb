Protocol extensions to the Cassandra Native Protocol
====================================================

This document specifies extensions to the protocol defined
by Cassandra's native_protocol_v4.spec and native_protocol_v5.spec.
The extensions are designed so that a driver supporting them can
continue to interoperate with Cassandra and other compatible servers
with no configuration needed; the driver can discover the extensions
and enable them conditionally.

An extension can be discovered by using the OPTIONS request; the
returned SUPPORTED response will have zero or more options beginning
with SCYLLA indicating extensions defined in this documented, in
addition to options documented by Cassandra. How to use the extension
is further explained in this document.

# Intranode sharding

This extension allows the driver to discover how Scylla internally
partitions data among logical cores. It can then create at least
one connection per logical core, and send queries directly to the
logical core that will serve them, greatly improving load balancing
and efficiency.

To use the extension, send the OPTIONS message. The data is returned
in the SUPPORTED message, as a set of key/value options. Numeric values
are returned as their base-10 ASCII representation.

The keys and values are:
  - `SCYLLA_SHARD` is an integer, the zero-based shard number this connection
    is connected to (for example, `3`).
  - `SCYLLA_NR_SHARDS` is an integer containing the number of shards on this
    node (for example, `12`). All shard numbers are smaller than this number.
  - `SCYLLA_PARTITIONER` is a the fully-qualified name of the partitioner in use (i.e.
    `org.apache.cassandra.partitioners.Murmur3Partitioner`).
  - `SCYLLA_SHARDING_ALGORITHM` is the name of an algorithm used to select how
    partitions are mapped into shards (described below)
  - `SCYLLA_SHARDING_IGNORE_MSB` is an integer parameter to the algorithm (also
    described below)

Currently, one `SCYLLA_SHARDING_ALGORITHM` is defined,
`biased-token-round-robin`. To apply the algorithm,
perform the following steps (assuming infinite-precision arithmetic):

  - subtract the minimum token value from the partition's token
    in order to bias it: `biased_token = token - (-2**63)`
  - shift `biased_token` left by `ignore_msb` bits, discarding any
    bits beyond the 63rd:
      `biased_token = (biased_token << SCYLLA_SHARDING_IGNORE_MSB) % (2**64)`
  - multiply by `SCYLLA_NR_SHARDS` and perform a truncating division by 2**64:
    `shard = (biased_token * SCYLLA_NR_SHARDS) / 2**64`

(this apparently convoluted algorithm replaces a slow division instruction with
a fast multiply instruction).

in C with 128-bit arithmetic support, these operations can be efficiently
performed in three steps:

```c++
    uint64_t biased_token = token + ((uint64_t)1 << 63);
    biased_token <<= ignore_msb;
    int shard = ((unsigned __int128)biased_token * nr_shards) >> 64;
```

In languages without 128-bit arithmetic support, use the following (this example
is for Java):

```Java
    private int scyllaShardOf(long token) {
        token += Long.MIN_VALUE;
        token <<= ignoreMsb;
        long tokLo = token & 0xffffffffL;
        long tokHi = (token >>> 32) & 0xffffffffL;
        long mul1 = tokLo * nrShards;
        long mul2 = tokHi * nrShards;
        long sum = (mul1 >>> 32) + mul2;
        return (int)(sum >>> 32);
    }
```

It is recommended that drivers open connections until they have at
least one connection per shard, then close excess connections.
