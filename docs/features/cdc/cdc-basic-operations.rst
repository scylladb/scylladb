=======================
Basic operations in CDC
=======================

The CDC log table reflects operations that are performed on the base table. Different types of operations give different corresponding entries in the CDC log. These operations are:

* inserts,
* updates,
* single row deletions,
* row range deletions,
* partition deletions.

Note that TTL expirations are not operations, and not reflected in the CDC
log tables. If you do need CDC events when entire rows expire, consider
using `per-row TTL <https://docs.scylladb.com/stable/cql/cql-extensions.html#per-row-ttl>`_
which does generate special CDC events when rows expire.

The following sections describe how each of these operations are handled by the CDC log.

.. include:: /features/cdc/_common/cdc-updates.rst
.. include:: /features/cdc/_common/cdc-inserts.rst
.. include:: /features/cdc/_common/cdc-row-deletions.rst
.. include:: /features/cdc/_common/cdc-row-range-deletions.rst
.. include:: /features/cdc/_common/cdc-partition-deletions.rst
