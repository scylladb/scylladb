Maintenance mode
================

In this mode, the node is not reachable from the outside, i.e.

 * it refuses all incoming RPC connections,
 * it does not join the cluster, thus

   * all operations that need synchronizing with other nodes are disabled (e.g. schema changes),
   * all cluster-wide operations are disabled for this node (e.g. repair),
   * other nodes see this node as dead,
   * cannot read or write data from/to other nodes,
 * it does not open Alternator and Redis transport ports and the TCP CQL port.

The only way to make CQL queries is to use :doc:`the maintenance socket </operating-scylla/admin-tools/maintenance-socket/>`. The node serves only local data.

To start the node in maintenance mode, use the `--maintenance-mode true` flag or set `maintenance_mode: true` in the configuration file.

REST API works as usual, but some routes are disabled:

  * authorization_cache
  * failure_detector
  * hinted_hand_off_manager
