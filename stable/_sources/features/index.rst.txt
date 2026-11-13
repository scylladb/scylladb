Features
========================

This document highlights ScyllaDB's key data modeling features.

.. toctree::
   :maxdepth: 1
   :hidden:

   Lightweight Transactions </features/lwt/>
   Global Secondary Indexes </features/secondary-indexes/>
   Local Secondary Indexes </features/local-secondary-indexes/>
   Materialized Views </features/materialized-views/>
   Counters </features/counters/>
   Change Data Capture </features/cdc/index>
   Workload Attributes </features/workload-attributes>

.. panel-box::
  :title: ScyllaDB Features
  :id: "getting-started"
  :class: my-panel
   
  * Secondary Indexes and Materialized Views provide efficient search mechanisms
    on non-partition keys by creating an index.

    * :doc:`Global Secondary Indexes </features/secondary-indexes/>`
    * :doc:`Local Secondary Indexes </features/local-secondary-indexes/>`
    * :doc:`Materialized Views </features/materialized-views/>`

  * :doc:`Lightweight Transactions </features/lwt/>` provide conditional updates
    through linearizability.
  * :doc:`Counters </features/counters/>` are columns that only allow their values
    to be incremented, decremented, read, or deleted.
  * :doc:`Change Data Capture </features/cdc/index>` allows you to query the current
    state and the history of all changes made to tables in the database.
  * :doc:`Workload Attributes </features/workload-attributes>` assigned to your workloads
    specify how ScyllaDB will handle requests depending on the workload.
