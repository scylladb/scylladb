# Redis API in Scylla

## 1. Overview

Redis is a very  famous NoSQL in-memory data structure store that stores a
mapping of keys to different types of values, and familiar to many developers.
It supports data structures such as STRINGs, HASHes, LISTs, SETs, ZSETs(sorted sets),
and other data structures. Redis has built-in replication, Lua
scripting, LRU eviction, transactions and different levels of on-disk
persistence. Now Redis as memory store service is provided by many cloud
platforms.

In this document, the detailed design and implementation of Redis that build on
top of Scylla is provided. At the beginning, the main feature, which is as a
data structures store, is design and  implmented. In the future, the reset
features of Redis will be supported.

## 2. Motivation

In contrast,  [Scylla](https://www.scylladb.com/) has advantage and amazing
features:

* Low and consistent latency
* Data persistence
* High availability
* High throughtput
* Highly scalable
* Auto tuning

If Redis build on the top of Scylla, it has the above features automatically.
It's achived great progress in cluster master managment, data persistence,
failover and replication.

The benefits to the users are easy to use and develop in their production
environment, and taking avantages of Scylla.

## 3. The Protocol Server

Redis clients communicate with the Redis server using a protocol called
**RESP** (REdis Serialization Protocol). The protocol is binary-safe and
easy to be implement at client side, which was designed specifically for
Redis.

Redis server accepts commands composed of different arguments. Once a
command is received, it's processed and a reply is sent back to the client.
Two kind of Redis clients are widely used, the smart Redis client and
simple Redis client. When using smart Redis client to connect the Redis
cluster, it will send the commands to the right Redis server. Because the
client will get the hash slots and server nodes mapping data at the bootrap
time. When using simple Redis client,  the server address is needed, and
all the requests are sent to this address, and the requests only send to
the single Redis server.

In Scylla terminology, the node receiving the request acts as the the
coordinator, and often passes the request on to one or more other nodes,
which hold copies of the requested data.  Any node of Scylla cluster can
process the request correctly. In other words, Client can send request to
any node of Redis cluster built on the top of Scylla. Obviously, the simple
client can accesses the Redis cluster built on the top of Scylla, when
provided address is not an IP but rather a single domain name, which a
client resloves a random Scylla node IP address. The smart Redis client
queries the mapping information between hash slots and Server nodes. We
just fill the mapping with random Scylla node IP address.

Before you can start using Scylla with Redis API, you must set the
`redis-port` configuration option (visa the command line or YAML),
to available port. By default, the Redis API is disabled. 
In Scylla, SSL for Redis API is supported. To enable it,
you must set the `redis-ssl-port` configuration option to available port.
This feature is disabled by default.

With Redis enabled, every Scylla node listens for Redis requests on the port.
These requests, in [RESP](https://redis.io/topics/protocol) format over TCP,
are parsed and result in calls to internal Scylla C++ functions.

## 4. Data Model

Out of box, every Redis cluster supports 16 databases. The default database
is 0 but it allows us to change that to any number from 0-15 (and allow us
to configure Redis to support more databases). Each database provides
a distinct keyspace, independent from the others. Use `SELECT n`
to change databases. Redis allows us to store keys that map to any one of
five different  basic data structure types:  STRINGs,  LISTs,  SETs,
HASHes, and  ZSETs. (In fact, Now Redis has support other structure types,
but the basic structure types are used widely).

In this proposal, We use the column family of Scylla to simulate the
database within Redis. At the bootrap phase, the column familes with fixed
name should be created (if not exists) or loaded.

We known that, Redis allows us to store keys that map to any one of the
basic data structures. Scylla supports variant type, which allows us to
define one table to store all kind of these Redis data. For example ,we
create the table with the schame as follow:

```
CREATE TABLE redis （
    key text,
    value variant\<text, list\<text\>, map\<text\>\>,
    PRIMARY KEY(key)
）WITH ...
```

However,  it's need to prefetch the whole elements of this type structure
into memory in Scylla, even modifying only one element of the structures.
The main limitation is that the size of the data structure is not allowed
to be exceeded the limit of the server's memory size. And the operations
will have more performance cost with bigger data structure.

> In Redis, maximum length of a list is 23^2  - 1 elements (4294967295,
> more than 4 billion of elements per list). And maximum length of
> element (as a string) is 512MB. It's very common that the size of data
> structure is exceeded the server's memory size.

So single table to store all the data strucutures is not good idea, instead
of five independent tables are created to hold the the different Redis
structure types within each column family related Redis database. In other
words, all of the STRINGs of the Redis will be stored in a table within the
column family, and all of the LISTs' elements will be stored in anther
table within the column family, and so on.

> When building Redis on top of Scylla, for every Redis  database (16
> databases as default), 1 column family with 5 tables in Scylla
> will be created.

The disadvantages of this proposal is that, we can not the TYPE command of
Redis. The different type strutures are stored in the different Scylla
tables, and each Scylla table provide the independent keyspace. In Redis,
keys are unique in the database. However,  the keys with column family of
Scylla are not unique. Beacause the keys are splited into independent
Scylla tables, it's different to original Redis.

**IMPORTANT NOTE**: The keys with database of Redis are not unique  in this
proposal, is the main difference with original Redis.

> This limitation is acceptable. The top layer of bussness system knows the
> context of the keys.

In the rest of this section, the details of schema of tables within column
family are provided.

### 4.1  Table Schema of STRINGs

In Redis, STRINGs are similar to strings that we see in other languages or
other key-value stores. Every data item only has two parts, KEY and VALUE.
The partition in the Scylla table owns the TTL property. We just need two
columns in the table to store Redis STRING data.

The maximum allowed key and value size is 512 MB. Very long key size is not
good idea.

Within the Scylla column family, we create the table named `STRINGs to
store the string structure of Redis. The table STRINGs is created by
following CQL:

```
CREATE TABLE STRINGs (
    pkey text,
    data text,
    PRIMARY KEY (key)
) WITH ... ;
```

Every Key-Value pair is stored as one partition within STRINGs table.
All commands of STRINGs and some shared commands will operate this table.

### 4.2 Table Schema of LISTs

In Redis, LISTs store an ordered sequence of strings which sorted by
insertion order. It allows us to push items to the front and the back of
the LIST with LPUSH/RPUSH, and to pop items from the front and back of the
list with LPOP/RPOP, and to fetch an item at a given position with LINDEX,
and to fetch a range of items with LRANGE.

Maximum length of a list is 23^2  - 1 elements (4294967295, more than 4
billion of elements per list). And maximum length of element (as a string) is 512MB.

As we known, Scylla supports 3 kinds of collections, lists, maps,
and sets. We do not use the lists collection to store Redis LISTs
structure. Because, the LISTs structure in Redis may contains many
elements ( Maximum is more than 4 billion of elements per list).
When updating the lists collection in Scylla, the whole elements
will be loaded into memory.

The better way is that the LISTs of Redis are stored as the
partition of the Scylla table. All commons of Redis LISTs will
operate on this table, which is created by following CQL:

```
CREATE TABLE LISTs (
    pkey text,
    ckey text,
    data text,
    PRIMARY KEY(pkey, ckey)
) WITH ... ;
```

The pkey is mapped to Redis LISTs key, and ckey is the UUID of the
insertion timestamp, which will be keep the right order as the insertion
order. The element's value is stored in the data column within LISTs table.

### 4.3  Table Schema of HASHes

In Redis, HASHes are maps between the string fields and the string values.
The fields are unique within a HASH structure in Redis. The values that can
be stored in HASHes are the same as what can be stored as normal STRINGs.

HASHes provide constant time basic operations like HGET, HSET, HEXISTS etc.

As mentioned above, Scylla provides the 3 kinds of collection data type,
including map.
We do not use this collection type of Scylla to strore HASHes data with the
same reason.

The better way is that the HASHes of Redis are stored as the partition of
the Scylla table. All commons of Redis HASHes will operate on this table,
which is created by following CQL:

```
CREATE TABLE HASHes (
    pkey text,
    ckey text,
    data text,
    PRIMARY KEY(pkey, ckey)
) WITH ... ;
```

The HASHes structure is stored as the partition of Scylla table. The pkey
is the partition key. The HASHes structure's field is storted as ckey
(cluster key in HASHes table). The HASHes structure's value is stored as
data column.

### 4.4 Table Schema of SETs

In Redis, SETs allows us to store sequence of strings, and uses a hash
table to keep all strings unique.

For the same reason as LISTs/HASHes, we do not use the collection type of
Scylla to store Redis SETs data. The table is created by following CQL:

```
CREATE TABLE SETs (
    pkey text,
    ckey text,
    PRIMARY KEY(pkey, ckey)
) WITH ... ;
```

Unlike HASHes, There is not data column in SETs table. Every SETs structure
is stored as a partition in SETs table. We use the cluster key to store the
item of Redis SETs.

### 4.5 Table Schema of ZSETs(Sorted SETs)

In fact, ZSETs in Redis is similar to HASHes, which maps the STRINGs key to
value. Like HASHes, the keys (called  MEMBERS) are unique. But the values (called
SCORES) are limited to floating-point numbers.  ZSETs have the
unique property in Redis of being able to be accessed by member (like aHASH),
but items can also be accessed by the sorted order and values of the scores.

Essentially speaking, the ZSETs looks like special SETs. The SETs structure
maps a key (as STRING) to a collection of STRINGs. But the ZSETs structure
maps a key (as STRING) to a collection of ITEMs which associated with a
score. It allows us to fetch data by score.

To store ZSETs data,  the scylla table is created by following CQL:

```
CREATE TABLE ZSETs (
    pkey text,
    ckey double,
    data text,
    PRIMARY KEY(pkey, ckey)
) WITH ... ;
```

Like other stutures mentioned above, a ZSETs strucutre is stored as a
partition within the ZSETs table.

## 5. Implementation of Commands

In Scylla, high write performance is achieved by ensuring that writes do
not require reads from disk. However, in Redis, the object level atomic
updates is [supported](https://redis.io/topics/transactions),  and the
transaction is supported (e.g. CAS).  In this proposal, the commands, which
require a read before the write (a.k.a read-modify-write), are implemented
in an unsafe way, that simply performs a read before the write.

Fortunately, the LWT is soon coming to Scylla.  By then the
read-modify-write commands will be implemented based on LWT.

**IMPORTANT NOTE**:

The commands, that require a read before the write, are not atomic. It's
anther difference with original Redis.

### 5.1 RMW Command

The RMW Commond is that the sequence of operations (Read-Modify-Write)
require by this command needs to be performed atomically.

For instance, APPEND command of STRINGs, is typical RMW command. First,
read the key from database, modify it's value (based on the exists value),
and write the new value back. These sequence of operations should be
performed atomically.

As mentioned above, Scylla currently does not support LWT. The RMW commands
are implemented in an unsafe way in this proposal. It simply performs a
read before the write.

Assuming Scylla has supported the LWT, there is no difference with the
original Reids about the RMW commands.

### 5.2 Non-RMW Command

The Non-RMW command only performs a single read or write operation. For
instance, The SET, GET, EXISTS of STRINGs strucutures. The behaviour of
these commands are not different with the original Redis.

### 5.3 Transaction

For the same reason as RMW command, the transaction is not supported
currently.

> In fact, some commands (e.g. MSET of STRINGs) are also not supported,
> which also need the transcation mechanism.

### 5.4 Time to Live (TTL)

In Redis, the TTL is only specified to entrie key-value pair. Scylla has
compaction mechanism to remove expired data.  We can set the TTL the
partition for the given partition key. We also can rewrite the  partition
with new TTL value.

The partition ( stores the key-value pair of Redis strucutres) will be
deleted eventually, when the associated TTL is older than current
timestamp.

### 5.5 Consistency

Redis Cluster is not able to [guarantee strong
onsistency](https://redis.io/topics/cluster-tutorial).

In Scylla, data is always replicated automatically.  Read  or  write
operations can occur to data stored on any of the replicated nodes. The
[Consistency Level
(CL)](https://docs.scylladb.com/glossary/#term-consistency-level-cl)
determines how many replicas in a cluster that must acknowledge read or
[write
operations](https://docs.scylladb.com/glossary/#term-write-operation)
before it is considered successful.

We can use the fault tolerance mechanism of Scylla, to make the
consistency of Redis operations configurable.

### 5.6 Implementation of Shared Commands

In Redis, data structures have the shared commands (e.g. DEL, EXISTS).
And other commands usually operate on special one data structure of Redis.
As mentioned above,
we split the key set into 5 different Scylla table. The shared commands
should operate on these tables in parallel. For instance, DEL command,
which allows user to remove any key in Redis database, should delete the
given key from 5 different Scylla tables.

## 6 API Reference

### Commands

The Redis API in Scylla supports the following subset of the Redis commands.

| Command | Description |
| ------- | ----------- |
| **Connection** | |
| `ECHO message` | Echo a `message` back to the client. The server returns `message´ as a response. |
| `PING [message]` | Check connection liveness or measure reponse time. The server returns `PONG` as a response. |
| `SELECT index` | Select logical database database for the current connection. |
| **Keys** | |
| `DEL key [key ...]` | Delete `key` from the database. |
| `EXISTS key [key..]` | Check if `key` exists in the database. |
| `TTL key` | Get the time to live (TTL) for `key`. |
| **String data type** | |
| `GET key` | Get the value for a `key`. |
| `SET key value [EX seconds\|PX milliseconds] [NX\|XX] [KEEPTTL]` | Set the value of `key`. |
| `SETEX key seconds value` | Set the value and the expiration of `key`. |
| **Hash data type** | |
| `HGET key field` | Get the value for a `key` and `field`. |
| `HSET key field value` | Set the value of `key` and `field`. Multiple field/value is not yet supported. Return value is always 1 whether the key exists or not. |
| `HGETALL key` | Get all values for a `key`. |
| `HDEL key field` | Delete a value for a `key` and `field`. Return value is always the number of fields whether the fields existed or not. |
| `HEXISTS key field` | Returns 1 if a value exists for a `key` and `field` or 0 if it doesn't. |
| **Server** | |
| `LOLWUT [VERSION version]` | Return Redis version. |
