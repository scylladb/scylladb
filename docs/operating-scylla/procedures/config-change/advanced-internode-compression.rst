==========================================
Advanced Internode (RPC) Compression
==========================================

Internode (RPC) compression controls whether traffic between nodes is
compressed. If enabled, it reduces network bandwidth usage.

To further reduce network traffic, you can configure ScyllaDB to use
ZSTD-based compression and shared dictionary compression. You can enable one or
both of these features to limit network throughput and reduce network transfer costs.

Enabling ZSTD-based Compression
----------------------------------

ZSTD-based compression utilizes the ZSTD algorithm for RPC compression.

To enable ZSTD-based compression:

#. Set ``internode_compression_enable_advanced`` to ``true``.
#. Set ``internode_compression_zstd_max_cpu_fraction`` to
   a non-zero value in the range [0, 1], where:

   * 0 - The node will never use ZSTD for RPC compression.
   * 1 - The node will always use ZSTD for RPC compression (for all messages,
     regardless of CPU consumption).

A good rule of thumb is to start from ``0.05``. As a result, the node will spend
not more than 5% of its total CPU on RPC ZSTD compression.

Note that enabling ZSTD increases CPU usage. We recommend monitoring metrics
to ensure additional CPU consumption does not disrupt the workload. If ZSTD
impacts performance, try setting ``internode_compression_zstd_max_cpu_fraction``
to a lower value.


Enabling Shared Dictionary Compression
-------------------------------------------

Shared dictionary compression allows you to limit network throughput and reduce
network transfer costs.

If enabled, the message-by-message compression is replaced with a more
efficient compression based on a dictionary trained on one node and shared
with other nodes. The dictionary is periodically re-trained on the node’s
RPC traffic and then distributed across the cluster. 

To enable shared dictionaries:

#. Set ``internode_compression_enable_advanced`` to ``true``.
#. Configure the ``rpc_dict_training_when`` parameter.
   In typical scenarios, you should set it to ``when_leader``.
   As a result, the :doc:`Raft leader </architecture/raft/>` node will
   train and publish new dictionaries.

.. code::

    internode_compression_enable_advanced: true
    rpc_dict_training_when: when_leader

.. warning:: Enabling shared dictionary training might leak unencrypted data to disk.

             Trained dictionaries contain randomly chosen samples of data transferred between
             nodes. The data samples are persisted in the Raft log, which is not encrypted.
             As a result, some data from otherwise encrypted tables might be stored on disk
             unencrypted.


Reference
------------

See the *Inter-node settings* section in :doc:`Configuration Parameters </reference/configuration-parameters/>`
for a complete list of internode compression options.
