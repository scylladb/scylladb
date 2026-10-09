.. SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
.. Copyright (C) 2026 ScyllaDB

.. _client-routes-api:

Client Routes
=============

The ``/v2/client-routes`` Admin REST API manages the routes used by drivers
connecting through private proxy endpoints. It is available after every node
supports and enables the ``CLIENT_ROUTES`` cluster feature. Deployment
infrastructure should use this API to reconcile routes instead of writing
directly to ``system.client_routes``.

.. warning::

   The Admin REST API binds to ``127.0.0.1`` by default. Keep it on loopback
   or an isolated, trusted management network. Use a secure management path
   for remote access; do not expose the API publicly.

``GET /v2/client-routes`` lists all route entries. ``POST /v2/client-routes``
creates or replaces routes with the same ``(connection_id, host_id)`` key. Its
body is a JSON array of complete route entries, each with an address and at
least one port. A specified port must be between 1 and 65535. For example:

.. code-block:: console

   curl -X POST "http://127.0.0.1:10000/v2/client-routes" \
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

Sending another entry with the same key replaces its address and port values.
A request can contain routes for multiple connections and nodes. List the
current routes with:

.. code-block:: console

   curl "http://127.0.0.1:10000/v2/client-routes"

``DELETE /v2/client-routes`` accepts a JSON array of ``connection_id`` and
``host_id`` pairs and removes the corresponding routes:

.. code-block:: console

   curl -X DELETE "http://127.0.0.1:10000/v2/client-routes" \
     -H "Content-Type: application/json" \
     --data '[
       {
         "connection_id": "private-connection-a",
         "host_id": "8ad27d74-8f8b-4bb4-9af7-7650a6cddf01"
       }
     ]'

Changes made through any node are stored cluster-wide. Drivers subscribed to
``CLIENT_ROUTES_CHANGE`` receive an event and refresh affected routes.

Swagger specification
---------------------

Each node serves the existing Swagger 2.0 Admin API specification at
``GET /v2``. It includes the Client Routes operations and their request and
response schemas.
