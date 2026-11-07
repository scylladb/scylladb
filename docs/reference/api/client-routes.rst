.. SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
.. Copyright (C) 2026 ScyllaDB

:exclude-doctools:

.. _client-routes-api:

Client Routes
=============

.. warning::

   The Admin REST API binds to ``127.0.0.1`` by default. Keep it on loopback
   or an isolated, trusted management network. Use a secure management path
   for remote access; do not expose the API publicly.

.. scylladb_swagger_inc::

.. scylladb_swagger::
   :spec: api/api-doc/client_routes.json
   :template: client_routes_swagger.tmpl
