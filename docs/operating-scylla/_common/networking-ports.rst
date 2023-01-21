
Scylla uses the following ports:

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
7199    JMX management                                TCP
------  --------------------------------------------  --------
10000   Scylla REST API                               TCP
------  --------------------------------------------  --------
9180    Prometheus API                                TCP
------  --------------------------------------------  --------
9100    node_exporter (Optionally)                    TCP
------  --------------------------------------------  --------
9160    Scylla client port (Thrift)                   TCP
------  --------------------------------------------  --------
19042   Native shard-aware transport port             TCP
------  --------------------------------------------  --------
19142   Native shard-aware transport port  (ssl)         TCP
======  ============================================  ========

.. note:: For Scylla Manager ports, see `Scylla Manager <https://manager.docs.scylladb.com/>`.
