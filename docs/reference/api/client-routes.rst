.. SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
.. Copyright (C) 2026 ScyllaDB

:exclude-doctools:

.. _client-routes-api:

Client Routes
=============

.. warning::

   The shipped ``scylla.yaml`` sets ``api_address`` to ``127.0.0.1``.
   When ``api_address`` is empty, ScyllaDB uses ``rpc_address`` instead.
   Check the effective bind address and keep the Admin REST API on loopback
   or an isolated, trusted management network. Use a secure management path
   for remote access; do not expose the API publicly.

This page is a reference and does not execute requests against your cluster.
Run API requests from a node or through a secure management connection, using
the node's configured API address and port.

.. scylladb_swagger_inc::

.. scylladb_swagger::
   :spec: api/api-doc/client_routes.json
