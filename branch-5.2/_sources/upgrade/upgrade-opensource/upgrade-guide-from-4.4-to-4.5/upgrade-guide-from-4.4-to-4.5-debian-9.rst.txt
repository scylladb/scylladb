.. |OS| replace:: Debian 9
.. |ROLLBACK| replace:: rollback
.. _ROLLBACK: ./#rollback-procedure
.. |SRC_VERSION| replace:: 4.4
.. |NEW_VERSION| replace:: 4.5
.. |SCYLLA_NAME| replace:: Scylla
.. |PKG_NAME| replace:: scylla
.. |SCYLLA_REPO| replace:: Scylla deb repo
.. _SCYLLA_REPO: https://www.scylladb.com/download/?platform=debian-9&version=scylla-4.5
.. |SCYLLA_METRICS| replace:: Scylla Metrics Update - Scylla 4.4 to 4.5
.. _SCYLLA_METRICS: ../metric-update-4.4-to-4.5
.. |SCYLLA_MONITOR| replace:: Scylla Monitoring 3.7.1
.. _SCYLLA_MONITOR: /operating-scylla/monitoring/
.. |PAXOS_DESC| replace:: There is no schema change of system.paxos between two releases, we should not revert writes to system.paxos table, whose state is needed to provide the linearizability guarantee for tables using LWT.
.. include:: /upgrade/_common/upgrade-guide-v3-ubuntu-and-debian.rst
