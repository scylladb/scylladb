========================================================
Scylla Manager Metric Update - Scylla Manager 1.3 to 1.4
========================================================

.. toctree::
   :maxdepth: 2
   :hidden:

Scylla Manager 1.4 Dashboards are available for use with Scylla Monitoring Stack

The following metrics are new in Scylla Manager 1.4
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* `scylla_manager_healthcheck_rest_status` - This metric measures the CQL status and is used in the Scylla Manager Health Check. The metric is labeled with the cluster and host and reports the following values:

  - `0` - not checked
  - `1` - success
  - `-1` - failure

* `scylla_manager_healthcheck_rest_rtt_ms` - This metric measures the CQL latency and is used in the Scylla Manager Health Check. The metric is labeled with the cluster and host and reports the latency in milliseconds. 


