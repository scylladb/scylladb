Nodetool status
===============
**status** - This command prints the cluster information for a single keyspace or all keyspaces.

The keyspace argument is required to calculate effective ownership information (``Owns`` column).
For tablet keyspaces, a table is also required for effective ownership.

For example:

::

    nodetool status my_keyspace

Example output:

.. code-block:: console


    Datacenter: datacenter1
    =======================
    Status=Up/Down/eXcluded
    |/ State=Normal/Leaving/Joining/Moving
    --  Address    Load       Tokens  Owns (effective)  Host ID                               Rack
    UN  127.0.0.1  394.97 MB  256     33.4%             292a6c7f-2063-484c-b54d-9015216f1750  rack1
    UN  127.0.0.2  151.07 MB  256     34.3%             102b6ecd-2081-4073-8172-bf818c35e27b  rack1
    UN  127.0.0.3  249.07 MB  256     32.3%             20db6ecd-2981-447s-l172-jf118c17o27y  rack1
    XL  127.0.0.4  149.07 MB  256     32.3%             dd961642-c7c6-4962-9f5a-ea774dbaed77  rack1

+----------+---------------------------------------------------------------+
|Parameter |Description                                                    |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
+==========+===============================================================+
|Datacenter|The data center that holds                                     |
|          |the information.                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
+----------+---------------------------------------------------------------+
|Status    |``U`` - The node is up.                                        |
|          |                                                               |
|          |``D`` - The node is down.                                      |
|          |                                                               |
|          |``X`` - The node is :ref:`excluded <status-excluded>`.         |
+----------+---------------------------------------------------------------+
|State     |``N`` - Normal                                                 |
|          |                                                               |
|          |``L`` - Leaving                                                |
|          |                                                               |
|          |``J`` - Joining                                                |
|          |                                                               |
|          |``M`` - Moving                                                 |
+----------+---------------------------------------------------------------+
|Address   |The IP address of the node.                                    |
|          |                                                               |
+----------+---------------------------------------------------------------+
|Load      |The size on disk the ScyllaDB data                             |
|          | takes up (updates every 60 seconds).                          |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
+----------+---------------------------------------------------------------+
|Tokens    |The number of tokens per node.                                 |
|          |                                                               |
|          |                                                               |
|          |                                                               |
+----------+---------------------------------------------------------------+
|Owns      |The percentage of data owned by                                |
|          |the node (per datacenter) multiplied by                        |
|          |the replication factor you are using.                          |
|          |                                                               |
|          |For example, if the node owns 25% of                           |
|          |the data and the replication factor                            |
|          |is 4, the value will equal 100%.                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
|          |                                                               |
+----------+---------------------------------------------------------------+
|Host ID   |The unique identifier (UUID)                                   |
|          |automatically assigned to the node.                            |
|          |                                                               |
+----------+---------------------------------------------------------------+
|Rack      |The name of the rack.                                          |
+----------+---------------------------------------------------------------+

.. _status-excluded:

Nodes in the Excluded status (``X``) are down nodes which were marked as excluded
by `removenode` or `replace`, and means that they are considered lost. They are no longer
contacted by the cluster and are not supposed to come back up. This marking is permanent.
They must be eventually removed or replaced.
The reason for this status is that knowing that the node is not coming back allows
topology operations to proceed without synchronizing with those nodes while still
maintaining cluster consistency.

.. include:: nodetool-index.rst
