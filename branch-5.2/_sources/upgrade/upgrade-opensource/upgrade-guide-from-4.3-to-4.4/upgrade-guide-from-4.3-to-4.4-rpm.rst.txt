.. |OS| replace:: Red Hat Enterprise Linux 7/8 or CentOS 7/8
.. |SRC_VERSION| replace:: 4.3
.. |NEW_VERSION| replace:: 4.4
.. |SCYLLA_NAME| replace:: Scylla
.. |PKG_NAME| replace:: scylla
.. |SCYLLA_REPO| replace:: Scylla rpm repo
.. _SCYLLA_REPO: https://www.scylladb.com/download/?platform=centos&version=scylla-4.4
.. |SCYLLA_METRICS| replace:: Scylla Metrics Update - Scylla 4.3 to 4.4
.. _SCYLLA_METRICS: ../metric-update-4.3-to-4.4
.. |SCYLLA_MONITOR| replace:: Scylla Monitoring 3.6.1
.. _SCYLLA_MONITOR: /operating-scylla/monitoring/
.. |PAXOS_DESC| replace:: There is no schema change of system.paxos between two releases, we should not revert writes to system.paxos table, whose state is needed to provide the linearizability guarantee for tables using LWT.
.. include:: /upgrade/_common/upgrade-guide-v3-rpm.rst
