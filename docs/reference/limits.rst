==============
Limits
==============

This document provides hard and soft limits in ScyllaDB.

Cluster and Node Limits
-----------------------------

.. list-table:: 
   :widths: 50 50
   :header-rows: 1

   * - Component
     - Limit 
   * - Nodes per cluster
     - Low hundreds
   * - Node size
     - 4096 CPUs
  
See :ref:`Hardware Requirements <system-requirements-hardware>` for storage
and memory requirements and limits.

CQL Limits
--------------

.. list-table:: 
   :widths: 50 50
   :header-rows: 1

   * - Component
     - Limit
   * - Keyspaces per cluster 
     - Thousands (tested with 1000)
   * - Tables per keyspace 
     - Low thousands (tested with 5000)
   * - Tables per cluster (including indexes)
     - Thousands
   * - Tables with CDC enabled
     - No limit. CDC can be enabled for all tables in a cluster.
   * - Materialized views and secondary indexes per table
     - Low tens
   * - Columns per table
     - Hundreds
   * - Columns per cluster
     - Tens of thousands
   * - Partition size
     - Gigabytes
   * - Rows per partition
     - No limit
   * - Row size
     - Latency-related soft limit: 
     
       Hundreds of kilobytes (good latency) or megabytes (mediocre latency)
   * - Key length
     - 65533
   * - Keyspace / Table / View / Index name length
     - 192 characters
   * - Query parameters in a query
     - 65535 (2^16-1)
   * - Statements in a batch
     - 65535 (2^16-1)
   * - Fields in a tuple
     - 32768 (2^15) (just a few fields, such as 2-10, are recommended)
   * - Collection (List)
     - ~2 billion (2^31)
   * - Collection (Set)
     - ~2 billion (2^31)
   * - Collection (Map)
     - Number of keys: 65535 (2^16-1)
   * - Blob size
     - 2 GB ( less than 1 MB is recommended)
   * - Dimension of a vector
     - 16000 (according to OpenSearch limitations)
 
.. _limits-system:

System Limits
--------------

When ScyllaDB runs as a systemd service, the following resource limits are
set in the ``/lib/systemd/system/scylla-server.service`` unit file.

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Name
     - Value
     - Description
   * - ``LimitCORE``
     - ``infinity``
     - Maximum core dump size
   * - ``LimitMEMLOCK``
     - ``infinity``
     - Maximum locked-in-memory address space
   * - ``LimitNOFILE``
     - ``infinity``
     - Maximum number of open file descriptors (resolved by systemd to
       the ``fs.nr_open`` sysctl value)
   * - ``LimitAS``
     - ``infinity``
     - Address space limit
   * - ``LimitNPROC``
     - ``8096``
     - Maximum number of processes

Limits configured in ``/etc/security/limits.d`` do not apply to systemd
services. To override the default values, see
:doc:`Increase ScyllaDB resource limits over systemd </kb/scylla-limits-systemd>`.
