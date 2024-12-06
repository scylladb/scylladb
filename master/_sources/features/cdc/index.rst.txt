=========================
Change Data Capture (CDC)
=========================

.. note:: CDC is not supported in keyspaces with :doc:`tablets</architecture/tablets>` enabled.

.. toctree::
   :maxdepth: 2
   :hidden:

   cdc-intro
   cdc-log-table
   cdc-basic-operations
   cdc-streams
   cdc-stream-generations
   cdc-querying-streams
   cdc-advanced-types
   cdc-preimages
   cdc-consistency


:abbr:`CDC (Change Data Capture)` is a feature that allows you to not only query the current state of a database's table, but also query the history of all changes made to the table.

.. panel-box::
  :title: Change Data Capture
  :id: "getting-started"
  :class: my-panel

  * :doc:`CDC Overview <cdc-intro>`
  * :doc:`The CDC Log Table <cdc-log-table>`
  * :doc:`Basic Operations in CDC <cdc-basic-operations>`
  * :doc:`CDC Streams <cdc-streams>`
  * :doc:`CDC Stream Generations <cdc-stream-generations>`
  * :doc:`Querying CDC Streams <cdc-querying-streams>`
  * :doc:`Advanced Column Types <cdc-advanced-types>`
  * :doc:`Preimages and Postimages <cdc-preimages>`
  * :doc:`Data consistency <cdc-consistency>`


.. panel-box::
  :title: CDC Integration Projects
  :id: "getting-started"
  :class: my-panel

  * `Go (scylla-cdc-go) <https://github.com/scylladb/scylla-cdc-go>`_
  * `Java (scylla-cdc-java) <https://github.com/scylladb/scylla-cdc-java>`_
  * `Rust (scylla-cdc-rust) <https://github.com/scylladb/scylla-cdc-rust>`_
  * :doc:`CDC Source Connector (compatible with Kafka Connect) </using-scylla/integrations/scylla-cdc-source-connector>`
  * :doc:`CDC Source Connector Quickstart </using-scylla/integrations/scylla-cdc-source-connector-quickstart>`

