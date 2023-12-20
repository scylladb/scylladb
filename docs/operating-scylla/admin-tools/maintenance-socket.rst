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

Connect to maintenance socket
-----------------------------

With python driver
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.connection import UnixSocketEndPoint
    from cassandra.policies import HostFilterPolicy, RoundRobinPolicy
    
    socket = "<node's workdir>/cql.m"
    cluster = Cluster([UnixSocketEndPoint(socket)],
                      # Driver tries to connect to other nodes in the cluster, so we need to filter them out.
                      load_balancing_policy=HostFilterPolicy(RoundRobinPolicy(), lambda h: h.address == socket))
    session = cluster.connect()
