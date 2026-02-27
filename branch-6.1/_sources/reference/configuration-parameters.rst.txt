========================
Configuration Parameters
========================

This section contains a list of properties that can be configured in ``scylla.yaml`` - the main configuration file for ScyllaDB.
In addition, properties that support live updates (liveness) can be updated via the ``system.config`` virtual table or the :doc:`REST API </operating-scylla/rest>`.

Live update means that parameters can be modified dynamically while the server
is running. If ``liveness`` of a parameter is set to ``true``, sending the ``SIGHUP``
signal to the server processes will trigger ScyllaDB to re-read its configuration
and override the current configuration with the new value.

**Configuration Precedence**

As the parameters can be configured in more than one place, ScyllaDB applies them
in the following order with ``scylla.yaml`` parameters updated via ``SIGHUP``
having the highest priority:

#. Live update via ``scylla.yaml`` (with ``SIGHUP``) or REST API
#. ``system.config`` table
#. command line options
#. ``scylla.yaml``

.. scylladb_config_list:: ../../db/config.hh ../../db/config.cc
  :template: db_config.tmpl
  :value_status: Used
