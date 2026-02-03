=======================
Basic operations in CDC
=======================

The CDC log table reflects operations that are performed on the base table. Different types of operations give different corresponding entries in the CDC log. These operations are:

* inserts,
* updates,
* single row deletions,
* row range deletions,
* partition deletions.

The following sections describe how each of these operations are handled by the CDC log.

.. include:: /features/cdc/_common/cdc-updates.rst
.. include:: /features/cdc/_common/cdc-inserts.rst
.. include:: /features/cdc/_common/cdc-row-deletions.rst
.. include:: /features/cdc/_common/cdc-row-range-deletions.rst
.. include:: /features/cdc/_common/cdc-partition-deletions.rst
