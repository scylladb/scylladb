Maintenance socket
==================

It enables interaction with the node through CQL protocol without authentication. It gives full-permission access.
The maintenance socket is available by Unix domain socket with file permissions `755`, thus it is not accessible from outside of the node and from other POSIX groups on the node.
It is created before the node joins the cluster.

To set up the maintenance socket, use the `maintenance-socket` option when starting the node.

* If set to `ignore` maintenance socket will not be created.
* If set to `workdir` maintenance socket will be created in `<node's workdir>/cql.m`.
* Otherwise maintenance socket will be created in the specified path.


The maintenance socket path has to satisfy following restrictions:

* the path has to be shorter than `108` chars (due to linux limits),
* a file or a directory cannot exists in this path.

Option `maintenance-socket-group` sets the owning group of the maintenance socket. If not set, the group will be the same as the user running the scylla node.
The user running the scylla node has to be in the group specified by `maintenance-socket-group` option or have root privileges.

Usage
-----
To access the maintenance socket, the user must belong to the same group as the socket's owner group.
By default, the maintenance socket is owned by the user running the scylla node. It can be changed by `maintenance-socket-group` option.
To connect to the maintenance socket, the user can add themselves to the 'scylla' group, run cqlsh as 'scylla', or adjust ownership via the maintenance-socket-group flag.

Connect to maintenance socket
-----------------------------

With python driver
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.connection import UnixSocketEndPoint
    from cassandra.policies import WhiteListRoundRobinPolicy
    
    socket = UnixSocketEndPoint("<node's workdir>/cql.m")
    cluster = Cluster([socket],
                      # Driver tries to connect to other nodes in the cluster, so we need to filter them out.
                      load_balancing_policy=WhiteListRoundRobinPolicy([socket]))
    session = cluster.connect()

With :doc:`CQLSh</cql/cqlsh/>`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

    cqlsh <node's workdir>/cql.m
