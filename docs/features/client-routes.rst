.. _client-routes:

Client Routes
=============

Client Routes allows ScyllaDB drivers to connect to clusters whose nodes are
exposed through proxy endpoints. It is required to support private connectivity
services such as AWS PrivateLink and Google Cloud Private Service Connect (PSC),
where the addresses and ports reachable by a client differ from the cluster's
internal CQL endpoints.

How Client Routes Work
----------------------

A driver normally discovers every node's address from cluster metadata and
connects to that address directly. With AWS PrivateLink or Google Cloud PSC,
those internal addresses are not reachable by the client. Instead, the driver:

#. Connects to a contact point supplied by the private connectivity service.
#. Discovers the cluster topology and the stable host ID of each node.
#. Reads ``system.client_routes`` for the private connections it is configured
   to use.
#. Matches each node's host ID to a client-reachable proxy address and port.
#. Uses the resulting proxy endpoint for subsequent connections to that node.

The ``system.client_routes`` Table
----------------------------------

The table stores one route for each combination of private connection and
ScyllaDB node:

.. code-block:: cql

   CREATE TABLE system.client_routes (
       connection_id text,
       host_id uuid,
       address text,
       port int,
       tls_port int,
       alternator_port int,
       alternator_https_port int,
       PRIMARY KEY (connection_id, host_id)
   );

``connection_id`` is the partition key and ``host_id`` is the clustering key.
Together they uniquely identify a route. The same node can therefore have
different routes through different private connections.

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Column
     - Meaning
   * - ``connection_id``
     - Identifies a particular AWS PrivateLink or Google Cloud PSC connection.
       The deployment's networking or control-plane software assigns this
       string, and the driver is configured with the connection IDs it may use.
       It is not a CQL session ID or a node ID.
   * - ``host_id``
     - The stable UUID of the ScyllaDB node reached by this route. It matches
       the host ID that the driver obtains from cluster metadata.
   * - ``address``
     - The client-reachable hostname or IP address of the proxy endpoint. It is
       not the node's internal broadcast address. Drivers resolve the value
       when it is a hostname.
   * - ``port``
     - The proxy port for an unencrypted CQL connection.
   * - ``tls_port``
     - The proxy port for a TLS-encrypted CQL connection.
   * - ``alternator_port``
     - The proxy port for an unencrypted Alternator connection.
   * - ``alternator_https_port``
     - The proxy port for an HTTPS-encrypted Alternator connection.

The four port columns are optional because a route may expose only some
protocols. At least one port must be present in an entry created through the
Client Routes REST API, and every specified port must be between 1 and 65535.
For example, a CQL driver connects to ``address:port`` for an unencrypted
connection or ``address:tls_port`` for a TLS connection.

Multiple Private Connections
----------------------------

A deployment may provide more than one private connection, such as one proxy
endpoint per availability zone. Each connection has its own ``connection_id``
partition containing a row for every node reachable through that connection.
The driver reads only partitions matching its configured connection IDs and
selects a matching route for each node.

For a driver to maintain full cluster connectivity, every node it discovers
must have a usable route through at least one of those connections. Different
rows may use the same proxy address with different ports, or different proxy
addresses with the same port.

Who Manages Client Routes
-------------------------

ScyllaDB stores and distributes Client Routes, but it does not discover the
proxy topology or populate the table automatically. Drivers only consume the
routes. The infrastructure that creates and manages the private connectivity
service is responsible for keeping the table current:

* In ScyllaDB Cloud, the ScyllaDB Cloud infrastructure and control plane create,
  update, and remove the records.
* In a custom deployment, the deployment infrastructure must perform the same
  reconciliation between the private connectivity service, the ScyllaDB
  topology, and ``system.client_routes``.

The infrastructure should update the routes whenever:

* A private connection is created or deleted.
* A ScyllaDB node is added, removed, or replaced.
* A proxy address or port changes.
* The set of protocols exposed through a route changes.

For every active private connection, the infrastructure should maintain a row
for each ScyllaDB node that clients must be able to reach. Stale rows should be
removed when either the connection or the node no longer exists.

Managing Routes with the REST API
---------------------------------

Deployment infrastructure should manage the records through the
``/v2/client-routes`` REST API instead of writing directly to the system table.
The API is available after every node in the cluster supports and enables the
``CLIENT_ROUTES`` cluster feature.

.. list-table::
   :widths: 15 35 50
   :header-rows: 1

   * - Method
     - Operation
     - Request body
   * - ``GET``
     - Lists all Client Routes.
     - No body.
   * - ``POST``
     - Creates new routes or updates existing routes with the same
       ``(connection_id, host_id)`` key.
     - A JSON array of complete route entries.
   * - ``DELETE``
     - Deletes routes identified by their keys.
     - A JSON array containing ``connection_id`` and ``host_id`` pairs.

For example, the following request creates or updates a route:

.. code-block:: console

   curl -X POST "http://node-api-address:10000/v2/client-routes" \
     -H "Content-Type: application/json" \
     --data '[
       {
         "connection_id": "private-connection-a",
         "host_id": "8ad27d74-8f8b-4bb4-9af7-7650a6cddf01",
         "address": "private-endpoint.example.com",
         "port": 19042,
         "tls_port": 19142
       }
     ]'

``POST`` is an upsert operation. Sending another entry with the same
``connection_id`` and ``host_id`` replaces its address and port values. A
request can contain routes for multiple connections and nodes.

List the current routes with:

.. code-block:: console

   curl "http://node-api-address:10000/v2/client-routes"

Delete a route by its composite key:

.. code-block:: console

   curl -X DELETE "http://node-api-address:10000/v2/client-routes" \
     -H "Content-Type: application/json" \
     --data '[
       {
         "connection_id": "private-connection-a",
         "host_id": "8ad27d74-8f8b-4bb4-9af7-7650a6cddf01"
       }
     ]'

The API accepts arrays so infrastructure can reconcile multiple routes in one
request. Changes made through any node are stored cluster-wide.

Route Updates
-------------

Route mappings can change without a node joining or leaving the cluster.
ScyllaDB sends subscribed drivers a ``CLIENT_ROUTES_CHANGE`` protocol event
after routes are inserted, updated, or deleted. The event contains the
``UPDATE_NODES`` change type and lists the affected connection IDs and host IDs.
It does not contain the new endpoint values; the driver reads the current rows
and updates its connection pools. This allows routing changes to take effect
without restarting the application.

Driver Requirement
------------------

Using Client Routes requires a driver that supports the
``system.client_routes`` table and the ``CLIENT_ROUTES_CHANGE`` protocol event.
