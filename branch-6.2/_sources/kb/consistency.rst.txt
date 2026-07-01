============================
Consistency in ScyllaDB
============================

This article explains what `consistency <https://www.scylladb.com/glossary/database-consistency/>`_ is and covers consistency-related concepts.

What is Consistency
----------------------

In database management systems, **consistency** (sometimes also called correctness) means that after a successful write, update, 
or delete request of a value, any read request receives the latest value. 

Another way to understand it is that in a consistent database, any given `database transaction <https://en.wikipedia.org/wiki/Database_transaction>`_ 
can only change the affected data in the allowed ways. Any written data has to be valid according to the defined rules, constraints, and triggers. 

What Is Eventual Consistency and How Is It Different from Strong Consistency
-------------------------------------------------------------------------------

Consistency is one of the guarantees defined in relational transactional databases. They provide `ACID guarantees <https://en.wikipedia.org/wiki/ACID>`_ 
and are sometimes also referred to as **strongly consistent databases**. 

There are ambiguities in the definition of the guarantees. One definition for an ACID transaction is:

* **Atomicity**: If any part of the transaction fails, the entire operation rolls back.
* **Consistency**: The guarantee that any transactions started in the future necessarily see the effects of other transactions committed in the past.
* **Isolation**: Each transaction is independent of other transactions.
* **Durability**: All transaction results are permanently preserved.

ACID compliance is a complex and often contested topic. The consistency guarantee is incredibly difficult to deliver in a globally distributed 
database topology that involves multiple clusters, each containing many nodes. For this reason, ACID-compliant databases are usually very expensive and difficult to scale. Since SQL databases are all ACID compliant to 
varying degrees, they also share these downsides. 

Some relational database systems relax the ACID guarantees to offset the downsides, as achieving resilient, distributed SQL database deployments can 
be extraordinarily challenging and expensive.

In contrast to SQL’s ACID guarantees, NoSQL databases provide BASE guarantees:

* **Basic Availability**: Data is available most of the time, even during a partial system failure.
* **Soft State**: Replicas are not consistent all the time.
* **Eventual Consistency**: Data will become consistent at some point in time, with no guarantee when.

This is also related to the `CAP theorem <https://groups.google.com/d/forum/scylladb-users>`_, which states that in a distributed data system, 
only two out of the following three guarantees can be satisfied:

* **(Atomic) Consistency**: The same response is given to all identical requests.
* **Availability**: Requests receive responses even during a partial system failure.
* **Partition Tolerance**: Operations remain intact even when some nodes are unavailable.

When discussing consistency in the context of the CAP theorem, the term typically applies to a single request/response operation sequence. 
This is different from the meaning in the ACID database context, where consistency refers to transactions.

ScyllaDB, as well as `Apache Cassandra <https://cassandra.apache.org/>`_ and other NoSQL databases, sacrifice a degree of consistency in order to 
increase availability. Rather than providing strong consistency, they provide `eventual consistency <https://www.scylladb.com/glossary/eventual-consistency/>`_. This means that in some cases, a read request will fail to return the result of the latest WRITE. 

In ScyllaDB (and Apache Cassandra), consistency is tunable - for a given query, the client can specify a `Consistency Level <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/architecture/topic/consistency-level-cl/>`_.
A Consistency Level is the number of replicas required to respond to a request for the request to be considered successful.  The possibility to
specify a Consistency Level per query is known as :term:`Tunable Consistency`. 

A database using the ACID model expects a unit of work to be atomic. It is all-or-nothing. While that unit of work is being done, the records or 
tables affected may be locked. 

On the other hand, distributed databases with a BASE model trade off strong consistency with high availability. The records stay available, 
but once the query has been completed across a majority of `nodes <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/architecture/topic/node/>`_, 
or as defined by the Consistency Level, the transaction is deemed successful. Data replication across all nodes can take some time, but the data in all 
the nodes will become consistent eventually. Once the query is deemed successful, queries for the data will consistently provide the updated data, 
even before data replication reaches the last few nodes.

Early results of eventual consistency data queries may not have the most recent updates. This is because it takes time for updates to reach replicas 
across a distributed database cluster. In strong consistency, however, data is sent to every replica the moment a query is made. This causes delays 
because responses to any new requests must wait while the replicas are updated. 

To summarize, in contrast to SQL’s ACID guarantees, NoSQL databases generally provide BASE guarantees. This enables high availability and relaxes 
the stringent consistency.

A related feature is :doc:`Lightweight Transactions (LWT) </features/lwt>`. LWT in ScyllaDB allow the client 
to modify data based on its current state: that is, to perform an update that is executed only if a row does not exist or contains a certain value. 
LWT are limited to a single conditional statement, which allows an “atomic compare and set” operation. That is, it checks if a condition is true, 
and if so, it conducts the transaction. If the condition is not met, the transaction does not go through.


Additional References
----------------------

* `Jepsen and ScyllaDB: Putting Consistency to the Test blog post <https://www.scylladb.com/2020/12/23/jepsen-and-scylla-putting-consistency-to-the-test/>`_ 
* `Nauto: Achieving Consistency in an Eventually Consistent Environment blog post <https://www.scylladb.com/2020/02/20/nauto-achieving-consistency-in-an-eventually-consistent-environment/>`_ 
* `Consistency Levels documentation <https://docs.scylladb.com/stable/cql/consistency.html>`_ 
* `High Availability lesson on ScyllaDB University <https://university.scylladb.com/courses/scylla-essentials-overview/lessons/high-availability/>`_ 
* `Lightweight Transactions lesson on ScyllaDB University <https://university.scylladb.com/courses/data-modeling/lessons/lightweight-transactions/>`_ 
* `Getting the Most out of Lightweight Transactions in ScyllaDB blog post <https://www.scylladb.com/2020/07/15/getting-the-most-out-of-lightweight-transactions-in-scylla/>`_ 
* Ports, D.R.K.; Clements, A.T.; Zhang, I.; Madden, S.; Liskov, B. "Transactional Consistency and Automatic Management in an Application Data Cache"
* Gilbert, S.; Lynch N. "Brewer's Conjecture and the Feasibility of Consistent, Available, Partition-Tolerant Web Services". 
