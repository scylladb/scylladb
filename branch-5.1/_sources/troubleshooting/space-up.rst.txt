Space Utilization Keeps Going Up During Normal Operation
========================================================

This troubleshooting guide describes what to do when Scylla space usage keeps going up.

Problem 
^^^^^^^

Over the lifetime of the cluster, old data is compacted together into new SSTables, removing the old.
Spikes in storage utilization are expected during compactions but if it doesn't reduce after a compaction
finishes it can be indicative of a problem.
You can use the ``lsof`` Linux utility to check if there are files that Scylla has deleted but whose
deletion are not reflected in the filesystem.

For example:

.. code-block:: shell

   lsof -p `pidof scylla`

   scylla   5864   scylla   936r   REG   9,127   106689876 17184087558 /var/lib/scylla/data/<keyspace>/<table>/la-21647-Data.db (deleted)
   scylla   5864   scylla   938r   REG   9,127   199501989 17184087553 /var/lib/scylla/data/<keyspace>/<table>/la-21609-Data.db (deleted)
  
Solution
^^^^^^^^

1. If you are running repairs or large reads, those could keep references to old files. Monitor those operations to see if space utilization goes down once they finish.

2. If the utilization problem persists and you are not running repairs or performing large reads, it could be an indication of a Scylla bug.
   Contact the Scylla team an provide the following data:
 
   * ``journalctl -u scylla-server > scylla_logs.txt``
   
   * ``ls -lhRS find /var/lib/scylla/data/ > file_list.txt``

3. In the mean time, restarting the Scylla nodes will release the references and free up the space.

.. include:: /rst_include/scylla-commands-restart-index.rst


