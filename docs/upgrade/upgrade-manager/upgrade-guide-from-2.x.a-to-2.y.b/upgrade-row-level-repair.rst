=========================
Fix for Row-level Repairs
=========================

.. toctree::
   :maxdepth: 2

Upgrade to  Manager 2.0.2 for Improved Repair Speeds in Scylla 3.1 and Higher
=============================================================================

One of the useful features of the Manager is how it handles repairs.
Manager breaks down token ranges into smaller segments in order distribute load over all available shards.
Result of this approach is more efficient repair execution.

With the release of `Scylla 3.1 <https://www.scylladb.com/2019/10/15/introducing-scylla-open-source-3-1/>`_ new improvement called `row-level repair <https://www.scylladb.com/2019/08/13/scylla-open-source-3-1-efficiently-maintaining-consistency-with-row-level-repair/>`_ was introduced.
This change approaches repair on the more granular level which makes optimizations done by the Manager obsolete.
In practice we noticed degradation of repair execution time with repairs done by Manager on Scylla clusters with row-level repair feature enabled.

Manager 2.0.2 introduces fix for handling repairs on clusters with row-level repair feature enabled.
When Scylla Manager detects the feature is enabled, it delegates sharding to the Scylla node thus avoiding any split shards.
If you experience slow repairs, please upgrade to Manager 2.0.2 or newer.

Slowdowns are still possible if Scylla Manager is not configured correctly.
If `segments_per_repair` configuration option (`scylla-manager.yaml`) is set to a low value, repair can still take a long time to finish.
So for clusters with row-level repair it is recommended to set `segments_per_repair` to at least 16.
The row-level repair feature is available from Scylla Open Source 3.1 and higher.
