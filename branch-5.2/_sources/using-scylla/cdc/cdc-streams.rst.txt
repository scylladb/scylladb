===========
CDC Streams
===========

Streams are partitions in CDC log tables. They are identified by their keys: *stream identifiers*. 
When you perform a base table write, Scylla chooses a stream ID for the corresponding CDC log entries based on two things:

* the currently operating *CDC generation* (:doc:`./cdc-stream-generations`),
* the base write's partition key.

Example:

.. code-block:: cql
      
    CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    INSERT INTO ks.t (pk,ck,v) values (0,0,0);
    INSERT INTO ks.t (pk,ck,v) values (0,1,0);
    INSERT INTO ks.t (pk,ck,v) values (0,2,0);
    INSERT INTO ks.t (pk,ck,v) values (2,0,0);
    INSERT INTO ks.t (pk,ck,v) values (2,1,0);
    INSERT INTO ks.t (pk,ck,v) values (2,2,0);
    SELECT "cdc$stream_id", pk, ck FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$stream_id                      | pk | ck
    ------------------------------------+----+----
     0x365fd1a9ae34373954529ac8169dfb93 |  2 |  0
     0x365fd1a9ae34373954529ac8169dfb93 |  2 |  1
     0x365fd1a9ae34373954529ac8169dfb93 |  2 |  2
     0x166eddaa68db9a95af83968998626f7c |  0 |  0
     0x166eddaa68db9a95af83968998626f7c |  0 |  1
     0x166eddaa68db9a95af83968998626f7c |  0 |  2

    (6 rows)

Observe that in the example above, all base writes made to partition ``0`` were sent to the same stream. The same is true for all base writes made to partition ``1``.

Underneath, Scylla uses the token of the base write's partition key to decide the stream ID. 
It stores a mapping from the token ring (the set of all tokens, which are 64-bit integers) to the set of stream IDs associated with the currently operating CDC generation. 
Thus, choosing a stream proceeds in two steps:

.. code-block:: none

   base partition key |--- partitioner ---> token |--- stream ID mapping ---> stream ID

Therefore, at any given moment, the stream ID chosen for a single base partition key will be the same, but two different partition keys might get mapped to two different streams IDs. 
But the set of used stream IDs is much smaller than the set of all tokens, so we will often see two base partitions appearing in a single stream:

.. code-block:: cql
      
    CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled': true};
    INSERT INTO ks.t (pk,ck,v) values (2,0,0);
    INSERT INTO ks.t (pk,ck,v) values (5,0,0);
    SELECT "cdc$stream_id", pk, ck FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$stream_id                      | pk | ck
    ------------------------------------+----+----
     0x365fd1a9ae34373954529ac8169dfb93 |  2 |  0
     0x365fd1a9ae34373954529ac8169dfb93 |  5 |  0

    (2 rows)

.. note:: To make the above example we simply kept inserting rows with different partition keys until we found two that went to the same stream. 

.. note:: For a given stream there is no straightforward way to find a partition key which will get mapped to this stream, because of the partitioner, which uses the murmur3 hash function underneath (the truth is you can efficiently find such a key, as murmur3 is not a cryptographic hash, but it's not completely obvious).

The set of used stream IDs is independent from the table. It's a global property of the Scylla cluster:

.. code-block:: cql
      
   CREATE TABLE ks.t1 (pk int, ck int, v int, primary key (pk, ck)) WITH cdc = {'enabled':'true'};
   CREATE TABLE ks.t2 (pk int, ck int, v int, primary key (pk, ck)) WITH cdc = {'enabled':'true'};
   INSERT INTO ks.t1 (pk,ck,v) values (0,0,0);
   INSERT INTO ks.t2 (pk,ck,v) values (0,0,0);
   SELECT "cdc$stream_id", pk, ck FROM ks.t1_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$stream_id                      | pk | ck
    ------------------------------------+----+----
     0x166eddaa68db9a95af83968998626f7c |  0 |  0

    (1 rows)

.. code-block:: cql
      
   SELECT "cdc$stream_id", pk, ck FROM ks.t2_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$stream_id                      | pk | ck
    ------------------------------------+----+----
     0x166eddaa68db9a95af83968998626f7c |  0 |  0

    (1 rows)

As the example above illustrates, even writes made to two different tables will use the same stream ID for their corresponding CDC log entries if their partition keys are the same, assuming that the operating CDC generation doesn't change in between those writes.

More generally, two base writes will use the same stream IDs if the tokens of their partition keys get mapped to the same stream ID by the CDC generation.

Ordering
^^^^^^^^

All considerations related to partition and clustering keys apply to CDC log tables. In particular, when performing a partition scan of the CDC log table, all entries from one stream will appear before all entries from another:

.. code-block:: cql
      
   CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH cdc = {'enabled':'true'};
   INSERT INTO ks.t (pk,ck,v) values (0,0,0);
   INSERT INTO ks.t (pk,ck,v) values (2,0,0);
   INSERT INTO ks.t (pk,ck,v) values (0,1,0);
   INSERT INTO ks.t (pk,ck,v) values (2,1,0);
   INSERT INTO ks.t (pk,ck,v) values (0,2,0);
   INSERT INTO ks.t (pk,ck,v) values (2,2,0);
   SELECT "cdc$stream_id", totimestamp("cdc$time"), pk, ck FROM ks.t_scylla_cdc_log;

returns:

.. code-block:: none

     cdc$stream_id                      | system.totimestamp(cdc$time)    | pk | ck
    ------------------------------------+---------------------------------+----+----
     0x365fd1a9ae34373954529ac8169dfb93 | 2020-03-25 13:12:59.195000+0000 |  2 |  0
     0x365fd1a9ae34373954529ac8169dfb93 | 2020-03-25 13:12:59.196000+0000 |  2 |  1
     0x365fd1a9ae34373954529ac8169dfb93 | 2020-03-25 13:12:59.197000+0000 |  2 |  2
     0x166eddaa68db9a95af83968998626f7c | 2020-03-25 13:12:59.194000+0000 |  0 |  0
     0x166eddaa68db9a95af83968998626f7c | 2020-03-25 13:12:59.195000+0000 |  0 |  1
     0x166eddaa68db9a95af83968998626f7c | 2020-03-25 13:12:59.197000+0000 |  0 |  2

    (6 rows)

Therefore there is no global time ordering between all writes in the CDC log; you only get time-based ordering within a stream, for each stream.
