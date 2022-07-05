``--host <node IP>``

Specifies the hostname or IP of the node that will be used to discover other nodes belonging to the cluster.
Note that this will be persisted and used every time Scylla Manager starts. You can use either an IPv4 or IPv6 address.

=====

``-n, --name <alias>``

When a cluster is added, it is assigned a unique identifier.
Use this parameter to identify the cluster by an alias name which is more meaningful.
This alias name can be used with all commands that accept ``-c, --cluster`` parameter.

=====

``--auth-token <token>``

Specifies the :ref:`authentication token <manager-2.1-generate-auth-token>` you identified in ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

=====

``-u, --username <cql username>``

Optional CQL username, for security reasons this user should NOT have access to your data.
If you specify the CQL username and password, the CQL health check you see in `status`_ would try to login and execute a query against system keyspace.
Otherwise CQL health check is based on sending `CQL OPTIONS frame <https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L302>`_ and does not start a CQL session.

=====

``-p, --password <password>``

CQL password associated with username.

=====

``--without-repair`` 

When cluster is added, Manager schedules repair to repeat every 7 days. To create a cluster without a scheduled repair, use this flag.


