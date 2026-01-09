# Size based load balancing

Until size based load balancing was introduced, ScyllaDB performed disk based balancing
based on disk capacity and tablet count with the assumption that every tablet uses the
same amount of disk space. This means that the number of tablets located on a node was
proportional to the gross disk capacity of that node. Because the used disk space of
different tablets can vary greatly, this could create imbalance in disk utilization.

Size based load balancing aims to achieve better disk utilization accross nodes in a
cluster. The load balancer will continuously gather information about available disk
space and tablet sizes from all the nodes. It then incrementally computes tablet
migration plans which equalize disk utilization accross the cluster.

# Basic operation

The load balancer runs as a background task on the same shard that the topology
coordinator is running on. The topology coordinator collects information needed
for the load balancer to make decisions about which tablets to migrate. This
information is collected periodically by the coordinator from all the nodes in
the cluster in the form of the data structure ``load_stats``. The information in
this struct relevant for the load balancer is stored in its member tablet_stats,
which contains:

- ``tablet_sizes``: the disk size in bytes of all the tablets on the given node.
- ``effective_capacity``: contains the sum of available disk space and all the tablet sizes on the given node.

The balancer will use this information to compute the disk load (disk utilization)
on every node and shard. It will then migrate tablets from the most to the least
loaded nodes and shards.

# Table balance

The secondary goal of the load balancer is to achieve table balance. This means that
the balancer needs to equalize the following ratios:

- disk used by the table on a shard / total disk used by the table in the rack
- effective capacity of a shard / effective capacity of the rack

Otherwise, we could have imbalance where a shard contains more data from a table,
which can potentially overload the CPU of that shard in case the given table is hot.

# ``load_stats`` reconciliation

Because the ``load_stats`` collection interval is 1 minute, by the time the balancer
starts using the information in ``load_stats``, that information can be stale. This can
be caused by tablet migrations or table resize (split or merge). To get around this,
we need to update the information about tablet sizes in ``load_stats`` after a migration
or resize. Issued tablet migrations update this information in ``load_stats`` by also
migrating the tablet size from one host to another. This tablet size migration
reconciliation is performed during the end_migration stage of the tablet migration.
Tablet size reconciliation for split or merge is performed during tablet resize
finalization. For a split, the reconciliation will divide the tablet size, and create
two tablets in place of the original tablet pre-split. For merge, it will accumulate
the tablet sizes of the tablets pre-merge, create a merged tablet size and remove the
tablet sizes pre-merge from ``load_stats``.

# Force capacity based balancing

The load balancer has the ability to fall back on capacity based balancing. This can be
enforced by a config parameter force_capacity_based_balancing. During capacity based
balancing, the balancer will not look up the actual tablet sizes from ``load_stats``,
and will instead assume each tablet size is equal to default_target_tablet_sizes. It will
also use the gross disk capacity (sent in ``load_stats`` struct in the capacity member),
instead of effective capacity.

# Excluding nodes with incomplete tablet sizes

Even with tablet size reconciliation (during migration and table resize), it is still
possible for the tablet size information in ``load_stats`` to not match the current tablet
information found in the tablet map. In order to avoid problems with the balancer
making decisions based on incomplete or incorrect data, size based load balancing
will not balance nodes which have incomplete tablet sizes. Instead, it will ignore
these nodes (these nodes will not be selected as sources or destinations for tablet
migrations), and will wait for correct tablet sizes to arrive after the next ``load_stats``
refresh by the topology coordinator.

One exception to this are nodes which have been excluded from the cluster. These nodes
are down and therefor are not able to send fresh ``load_stats``. But they have to be drained
of their tablets (via tablet rebuild), and the balancer must do this even with incomplete
tablet data. So, only excluded nodes are allowed to have missing tablet sizes.

# Size based balancing cluster feature

During rolling upgrades, it can take hours or even days until all the nodes in a cluster
have been upgraded. This means that the non-upgraded nodes will send ``load_stats`` without
tablet sizes. Considering the balancer ignores these nodes during balancing, we would
have a problem with some of the nodes not being load balanced for extended periods of
time. To avoid this, size based load balancing is only enabled after the cluster feature
is enabled. Until that time, the balancer will fall back on capacity based balancing.

# Minimal tablet size

After a table is created and it begins to receive data, there can be a period of time
where the data in some of the tablets has been flushed, while others have not. During
this time, the load balancer will migrate seemingly empty (but actually not yet flushed)
tablet away from the nodes where they have been created. Later, when all the tablets
have been flushed, the balancer will migrate the tablets again. In order to avoid these
unnecessary early migrations, we introduce the idea of minimal tablet size. The balancer
will treat any tablet smaller than minimal tablet size as having a size of minimal
tablet size. This reduces early migrations.

# Balance threshold percentage

The balancer considers if a set nodes are balanced by computing the delta of disk load
between the most loaded and least loaded node, and dividing it with the load of the most
loaded node:

    delta = (most_loaded - least_loaded) / most_loaded

If this computed value is below a threshold, the nodes are considered balanced. This threshold
can be configured with the ``size_based_balance_threshold_percentage`` config option.
