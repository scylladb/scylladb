Scylla payload sent duplicated static columns
=============================================
Scylla payload, which refers to the actual network packets transferred from the Scylla server to the client as a result of a query, contains duplicate static columns.

Issue description
-----------------
TCP trace analysis of the response for a query like the one listed below shows duplicate data  (e.g., the current timestamp) was sent by the server.

.. code-block:: none

   select some_column, currentTimestamp() from some_table where pk = ?

In another perspective, if you have 10 rows with a static column and a 10KB blob, the entire payload with all non-static columns included will exceed 100KB.


Resolution
----------
A workaround for this issue would be to select the static column in a second request with LIMIT 1.
