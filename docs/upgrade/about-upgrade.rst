================
About Upgrade
================

ScyllaDB upgrade is a rolling procedure - it does not require a full cluster
shutdown and is performed without any downtime or disruption of service.

To ensure a successful upgrade, follow
the :doc:`documented upgrade procedures <upgrade-guides/index>` tested by
ScyllaDB. This means that:

* You should follow the upgrade policy:

   * Starting with version **2025.4**, upgrades can skip minor versions as long
     as they remain within the same major version (for example, upgrading directly
     from 2025.1 → 2025.4 is supported).
   * For versions **prior to 2025.4**, upgrades must be performed consecutively—
     each successive X.Y version must be installed in order, **without skipping
     any major or minor version** (for example, upgrading directly from 2025.1 → 2025.3
     is not supported).
   * You cannot skip major versions. Upgrades must move from one major version to
     the next using the documented major-version upgrade path.
   * You should upgrade to a supported version of ScyllaDB.
     See `ScyllaDB Version Support <https://docs.scylladb.com/stable/versioning/version-support.html>`_.
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

