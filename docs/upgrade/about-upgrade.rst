================
About Upgrade
================

ScyllaDB upgrade is a rolling procedure - it does not require a full cluster
shutdown and is performed without any downtime or disruption of service.

To ensure a successful upgrade, follow
the :doc:`documented upgrade procedures <upgrade-guides/index>` tested by
ScyllaDB. This means that:

* You should perform the upgrades consecutively across major versions - to each
  successive X.Y version. Within the same major version you may upgrade from
  any earlier minor release to a later minor release of that same major.
  For example, you can upgrade to 2025.4 from 2025.1, 2025.2, or 2025.3.
  Do not skip major Scylla versions unless there is a documented upgrade
  procedure that explicitly allows it.
* Before you upgrade to the next version, the whole cluster (each node) must
  be upgraded to the previous version.
* You cannot perform an upgrade by replacing the nodes in the cluster with new
  nodes with a different ScyllaDB version. You should never add a new node with
  a different version to a cluster - if you
  :doc:`add a node </operating-scylla/procedures/cluster-management/add-node-to-cluster>`,
  it must have the same X.Y.Z (major.minor.patch) version as the other nodes in
  the cluster.

Upgrading to each patch version by following the Maintenance Release Upgrade
Guide is optional. However, we recommend upgrading to the latest patch release
for your version before upgrading to a new version.

