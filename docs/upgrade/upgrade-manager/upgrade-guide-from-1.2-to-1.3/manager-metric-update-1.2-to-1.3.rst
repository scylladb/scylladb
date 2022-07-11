========================================================
Scylla Manager Metric Update - Scylla Manager 1.2 to 1.3
========================================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla Manager 1.3 Dashboards are available for use with Scylla Monitoring Stack

The following metrics are new in Scylla Manager 1.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* `scylla_manager.healthcheck.cql_status` - This metric measures the CQL status and is used in the Scylla Manager Health Check. The metric is labeled with the cluster and host and reports the following values:

  - `0` - not checked
  - `1` - success
  - `-1` - failure

* `scylla_manager.healthcheck.cql_rtt_ms` - This metric measures the CQL latency and is used in the Scylla Manager Health Check. The metric is labeled with the cluster and host and reports the latency in milliseconds. 

The following metric was updated from Scylla Manager 1.2 to Scylla Manager 1.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `scylla_manager.repair.progress` now aggregates progress in percentage and has the following additional labels:
 
  - cluster
  - task
  - keyspace 
  - host

Previously, this metric reported the aggregated keyspace and host progress. Now it reports the aggregated keyspace progress and aggregated total progress. This is implemented by leaving appropriate labels blank. In the total progress metric the only set label is `cluster` and `task`.

