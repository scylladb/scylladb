=======================
Data Consistency in CDC
=======================

----------------------------------
CDC Log and Base Table Co-location
----------------------------------

CDC tries its best to keep CDC Log entries co-located with its relevant Base Table entries in cluster nodes and shards within nodes.
For example, if we have a 6 node cluster composed of nodes A, B, C, D, E, and F, and there is a Base Table partition P that is stored on nodes A, C, and E, then all CDC Log entries reflecting changes to the partition P are also stored in a CDC stream that is also stored on nodes A, C, and E.
In fact, those CDC Log entries are even stored on the same **shards** on nodes A, C, and E as partition P.
When the cluster topology changes, there are some rare cases where this invariant breaks.

------------------------------
CDC operations are synchronous
------------------------------

All CDC writes are done together with the writes to the Base Table.
The co-location described in the previous section is extremely helpful because it allows sending both writes to the same set of replicas.
As a result, the write to the Base Table is acknowledged to the client only after both writes are successfully persisted on the same number of replicas determined by the Consistency Level of the original write.

-------------------------------------------------------------
CDC operations have to follow CL of the base table operations
-------------------------------------------------------------

As a consequence of the previously described CDC synchronous nature, CDC Log writes are done with the same CL as Base Table writes that cause those CDC Log writes.
This means that in order to achieve strong consistency, the CDC Log has to be read with an appropriate CL level matching the CL of the Base Table writes.
For example, if the Base Table writes are done with CL = ONE then the CDC Log needs to be read with CL = ALL.
If the Base Table is written with CL = QUORUM then the CDC Log reads have to be performed with CL = QUORUM at least.

-------------------------------------------------
When do CDC updates become visible to the client?
-------------------------------------------------

CDC Log writes are performed together with Base Table writes.
They are executed using the exact mechanism as regular writes, subject to the same eventual consistency behavior.
A write to the CDC Log may be visible to the client as soon as the first replica persists that write.
To talk about any guarantee we need to consider the CL of the CDC Log read.
CDC Log write is guaranteed to be visible as soon as it's acknowledged by WRITE CL replicas and the read is performed with READ CL such that READ CL + WRITE CL > RF.
In general, failed writes come with no guarantee. A failed write can appear in a later read from the Base Table but don't show up in the later read for the CDC Log. This means that the client should keep retrying writes that failed. As long as the co-location of the Base Table and the CDC log is preserved this shouldn't happen. Such a co-location should be the case most of the time, but there are a few cases where the phenomenon does not occur.
