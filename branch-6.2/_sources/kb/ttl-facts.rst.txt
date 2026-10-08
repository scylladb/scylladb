=================================
Time to Live (TTL) and Compaction
=================================

**Topic: TTL, gc_grace_seconds, and Compaction**

**Learn: How data is removed when using TTL**

**Audience: All**


Synopsis
--------
It is not always clear under which circumstances data is deleted when using Time to Live (TTL) and :ref:`gc_grace_seconds <gc_grace_seconds>` arguments in table definitions.
This article clarifies what may not be apparent.
It corrects some assumptions you may have that are not exactly true.


Facts About Expiring Data
-------------------------

#. When compaction is running on an SSTable and it scans a piece of data that has expired the following happens:

   * If the `data time stamp + gc_grace_seconds` is less than or equal to the current time (now), the data is thrown away and a tombstone is **not** created.
   * If not, a tombstone is created and its timestamp is the same as its corresponding data. This means that the tombstone is going to be deleted after (at least) gc_grace_seconds from the time when the corresponding data element has been written.

   For example:

   In this example there is an expired cell that becomes a tombstone, note how the tombstone's local_deletion_time is the expired cell's timestamp.

   .. code-block:: none

      "rows" : [
        {
          "type" : "row",
          "position" : 120,
          "liveness_info" : { "tstamp" : "2017-04-09T17:07:12.702597Z",
      "ttl" : 20, "expires_at" : "2017-04-09T17:07:32Z", "expired" : true },
      "cells" : [
      { "name" : "country", "value" : "1" },
      ]

   Is compacted into:

   .. code-block:: none


      "rows" : [
      {
      "type" : "row",
      "position" : 79,
      "cells" : [
      { "name" : "country", "deletion_info" :
      { "local_delete_time" : "2017-04-09T17:07:12Z" },
      "tstamp" : "2017-04-09T17:07:12.702597Z"
      },
      ]

#. If you set the TTL to be *greater* than the gc_grace_seconds, the expired data will **never** generate a tombstone.

   Example: When Time Window Compaction Strategy (TWCS) estimates if a particular SSTable can be deleted it is going to treat expired data similarly as a tombstone:

   .. code-block:: none

      gc grace seconds: 0
      sstable A: token range: [10, 10], timestamp range: [1, 1], ttl: 2, max deletion time: 3 (1+2)
      sstable B: token range: [10, 10], timestamp range: [1, 5], ttl: 2, max deletion time: 7 (5+2)
      now: 6

   From this you can see that:

   * SSTable A is fully expired because its max deletion time (3) is lower than now (6).
   * SSTable B is NOT fully expired because its max deletion time (7) is greater than now (6).
   * SSTable A will be added as a candidate for fully expired SSTable and will be purged.

   During Compaction:

   * Compaction sees that SSTable B overlaps with SSTable A in token range.
   * Compaction also sees that SSTable A overlaps with B in timestamp range, and it thinks that SSTable A could have a tombstone that shadows data in SSTable B.
   * Compaction decides not to purge SSTable A even though it's fully expired.

#. If you use updates in conjunction with TWCS, it may prevent the "window compacted" SSTables from being deleted when they should have been.
   However, when the corresponding data expires (in all SSTables), all relevant SSTables are will be deleted during the next compaction.


Additional Information
----------------------

* :doc:`How to Change gc_grace_seconds for a Table </kb/gc-grace-seconds/>`
* :doc:`Expiring Data with Time to Live (TTL) </cql/time-to-live/>`
* :ref:`CQL Reference: Table Options <create-table-general-options>`
