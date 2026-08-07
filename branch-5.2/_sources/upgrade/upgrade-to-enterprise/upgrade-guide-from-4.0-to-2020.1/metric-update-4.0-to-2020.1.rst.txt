=============================================================
Scylla Metric Update - Scylla 4.0 to Scylla Enterprise 2019.1
=============================================================


The following metrics are new in Scylla Enterprise 2020.1 compared to Scylla Open Source 4.0:

* *scylla_storage_proxy_coordinator_cas_dropped_prune* : How many times a coordinator did not perfom prune after cas
* *scylla_storage_proxy_coordinator_cas_prune* : How many times paxos prune was done after successful cas operation
* *scylla_storage_proxy_replica_cas_dropped_prune* : How many times a coordinator did not perfom prune after cas

The following metrics are not available in Scylla Enterprise 2020.1 compared to Scylla Open Source 4.0:

* *scylla_thrift_current_connections*
* *scylla_thrift_served*
* *scylla_thrift_thrift_connections*
