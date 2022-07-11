Nodetool version
================
**version** - Displays the Apache Cassandra version which your version of Scylla is most compatible with, not your current Scylla version.
To display the Scylla version, refer to :ref:`Check your current version of Scylla <check-your-current-version-of-scylla>`.
To display additional compatibility metrics, such as CQL spec version, refer to :ref:`SHOW VERSION <cqlsh-show-version>`.


For example:

.. code:: sh

    nodetool version

Returns (your results may be different):

.. code:: none

    ReleaseVersion: 3.0.8

.. include:: nodetool-index.rst
