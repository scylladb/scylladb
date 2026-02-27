CDC Parameters
--------------

The following table contains parameters available inside the ``cdc`` option:

.. list-table::
   :widths: 33 33 33
   :header-rows: 1

   * - Parameter name
     - Definition
     - Default
   * - enabled
     - If true, the log table is created and each base table write will get a corresponding log table write: a delta row. The delta row describes "what has changed".
     - false
   * - preimage
     - If true, each base write will get a corresponding preimage row in the log table. Preimage rows exist to show the affected row's state `prior` to the write. The amount of information can be changed: ``true`` value of the ``'preimage'`` parameter configures the preimages to contain only the columns that were changed by the write; ``'full'`` value to the ``'preimage'`` configures the preimages to contain the entire row (how it was before the write was made). In the case of collection columns, preimage contains the state of the whole collection before the change (not only the affected cells of the collection). Note that preimages are costly: they require an additional read-before-write.
     - false
   * - postimage
     - If true, each base write will get a corresponding postimage row in the log table. Postimage rows exist to show the affected row's state `after` to the write. The postimage row always contains all the columns no matter if they were affected by the change or not. Note that postimages, similarly to preimages, are costly: they require an additional read-before-write. However, if you enable both preimage and postimage, only one read will be required for both of them.
     - false
   * - delta
     - If ``'full'``, each delta row will contain information about every modified column. If ``'keys'``, only the primary key of the change will be recorded in the delta row. You may want to use the second option if you're only interested in preimages and want to save some space.
     - full
   * - ttl
     - Each log table row has a TTL (time-to-live) set on each of its columns, so that the log doesn't grow endlessly. This option specifies what the TTL should be in seconds; the default is 86400 seconds (24 hours). You can also set it to 0, which means that the TTL won't be set, thus log rows won't be removed. Be careful however: in that case the log will consume more and more disk space. You will probably want to setup a separate cleaning mechanism if you set TTL to 0.
     - 86400

