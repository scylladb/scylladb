Nodetool ring
=============
**ring** ``[<keyspace>] [<table>]`` - The nodetool ring command displays the token
ring information. The token ring is responsible for managing the
partitioning of data within the ScyllaDB cluster. This command is
critical if a cluster is facing data consistency issues.

By default, ``ring`` command shows all keyspaces.

For example:

.. code:: sh

    nodetool ring

This will show all the nodes that are involved in the ‘ring’ and the
tokens that are assigned to each one of them. It will also show the
status of each of the nodes.

+------------+-----+-----------+-------+--------------+-----------+---------------------------+
|Address     |Rack |  Status   |State  |      Load    |  Owns     |  Token                    |
+============+=====+===========+=======+==============+===========+===========================+
|172.30.0.64 | 1b  |    Up     | Normal|551.31 MB     | Mykespace | 1006916943685901788       |
+------------+-----+-----------+-------+--------------+-----------+---------------------------+
|172.30.0.62 | 1b  |    Up     | Normal|541.59 MB     | Mykespace | 1024434117767101090       |
+------------+-----+-----------+-------+--------------+-----------+---------------------------+
|172.30.0.61 | 1b  |    Up     | Normal|541.59 MB     | Mykespace | 1043327858966261499       |
+------------+-----+-----------+-------+--------------+-----------+---------------------------+

You can specify a ``<keyspace>`` name to filter the output and focus on
a specific keyspace. Another optional argument ``<table>`` allows you
to further narrow down. For keyspaces with :doc:`tablets </architecture/tablets>`
enabled, you need to provide both ``<keyspace>`` and ``<table>``. This
will display the partition ranges for that specific table.

.. code:: sh

   nodetool ring <keyspace> <table>

.. include:: nodetool-index.rst
