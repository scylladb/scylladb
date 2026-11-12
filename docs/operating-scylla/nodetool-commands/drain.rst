Nodetool drain
==============
**drain** - Flushes all memtables from a node to the SSTables that are on the disk. ScyllaDB stops listening for connections from the client and other nodes. You need to restart ScyllaDB after running this command. This command is usually executed before upgrading a node to a new version or before any maintenance action is performed. When you want to simply flush memtables to disk, use the :doc:`nodetool flush </operating-scylla/nodetool-commands/flush/>` command.

For example:

``nodetool drain``

Pending requests
----------------

Drain and graceful process shutdown use the same stages:

#. Enter the local ``DRAINING`` state and transfer tablet Raft leadership to
   other replicas where possible.
#. Stop admitting new client requests and publish ``RPC_READY=false``. CQL
   clients subscribed to status events receive ``STATUS_CHANGE DOWN`` and can
   route requests elsewhere. Existing requests retain their response connections.
   Alternator rejects new API requests and returns an unavailable health status.
#. Announce that the node is draining. Peer coordinators exclude it from new
   queries and wait for existing requests, including background read
   reconciliation and forwarded strongly consistent reads and writes, to
   receive their replies. Late forwarded queries are routed to another
   replica without executing on the draining node, even before peers learn
   its announcement.
#. Finish stopping client protocols, announce gossip shutdown, and close internode
   connections. Stop tablet Raft groups after admitted client requests finish.
#. Finish draining local services and flushing data, then enter the local
   ``DRAINED`` state. Process shutdown exits after these stages.

Concurrent drain and shutdown requests share the same completion result,
including failures. Drain preserves cluster membership and data ownership.
It does not decommission or replace the node, or move its tablets elsewhere.
Restarting the node clears its drain announcement and lets it serve again.

A new or restarted coordinator synchronizes drain state before selecting peers
for reads or forwarding strongly consistent queries. If an offline peer does
not answer this exchange, startup can finish, but these queries exclude that
peer until a background retry succeeds. An ordinary gossip UP notification
alone does not restore its query eligibility.

The read wait keeps the requested consistency level. A new read at ``ALL`` can
be unavailable once a replica is draining. Later batches of an existing range
scan also exclude draining replicas and can become unavailable at ``ALL``.
A read that requires a quorum can continue when enough other replicas are
available. Leadership transfer does not replace waiting for replies. Replication
and voting remain available while admitted requests still need them. A draining
node continues to receive replica mutations and Raft traffic needed by these
operations; stopping query admission does not remove its replicas.

This behavior is enabled automatically through the ``GRACEFUL_READ_SHUTDOWN``
cluster feature after all nodes support it. During a rolling upgrade, while any
node lacks support, local client admission still stops after leadership
transfer, but the new peer synchronization is unavailable. Old coordinators
can keep selecting the node, and pending requests can still fail when its
internode connections close. The client status event is advisory; local
admission also stops clients that ignore it. As with other persistent cluster
features, once this feature is enabled, a binary that does not support it cannot
restart or join the cluster.

The whole prepare phase, including waiting for gossip locks, is limited by
``request_timeout_on_shutdown_in_seconds`` (default: 30 seconds). Draining CQL
requests and waiting for peer coordinator requests each use a separate timeout, so
the total shutdown time can exceed this value. Shutdown continues if a peer
fails or the request wait times out; emergency isolation bypasses this wait.

See also

:doc:`Nodetool flush </operating-scylla/nodetool-commands/flush/>`

.. include:: nodetool-index.rst
