=============================================
CQL Query Does Not Display Entire Result Set
=============================================

**Audience: ScyllaDB administrators**

If you send a cqlsh query similar to:

.. code-block:: none

   SELECT * FROM 'keyspace.table' limit 100;

and the results show a single row with ``--More--``, the ``--More--`` indicates that there are additional pages - if you click Enter, additional rows are displayed.

As the query is using paging (from cqlsh by default page size is 100) - ScyllaDB uses this information internally and will fetch internally page size results. Some of these may be discarded and not returned to you or the output may reveal blank pages where you will see the ``--More--`` prompt causing you to page through empty pages. Neither of these outputs is desired.

If you need text result of this query as a single run, without the page delimiter, you can :ref:`turn off paging <cqlsh-paging>`.

It is also recommended to turn ``tracing on`` (please keep the limit to 100 as well).
