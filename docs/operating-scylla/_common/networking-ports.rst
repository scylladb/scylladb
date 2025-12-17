
.. _networking-ports:

ScyllaDB uses the following ports:

======  ============================================  ========
Port    Description                                   Protocol
======  ============================================  ========
9042    CQL (native_transport_port)                   TCP
------  --------------------------------------------  --------
9142    SSL CQL (secure client to node)               TCP
------  --------------------------------------------  --------
7000    Inter-node communication (RPC)                TCP
------  --------------------------------------------  --------
7001    SSL inter-node communication (RPC)            TCP
------  --------------------------------------------  --------
10000   ScyllaDB REST API                               TCP
------  --------------------------------------------  --------
9180    Prometheus API                                TCP
------  --------------------------------------------  --------
9100    node_exporter (Optionally)                    TCP
------  --------------------------------------------  --------
19042   Native shard-aware transport port             TCP
------  --------------------------------------------  --------
19142   Native shard-aware transport port  (ssl)         TCP
======  ============================================  ========

If you're using ScyllaDB Alternator, ensure that the ports configured
for Alternator with the ``alternator_port`` or ``alternator_https_port`` parameter
are open. See :doc:`ScyllaDB Alternator </alternator/alternator>` for details.

.. note:: For ScyllaDB Manager ports, see the `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_ documentation.
