CQL Command ``COPY FROM`` fails - field larger than the field limit
===================================================================

This troubleshooting guide describes what to do when ScyllaDB fails to import data using the CQL ``COPY FROM`` command


Problem
^^^^^^^

When trying to use the CQL command ``COPY FROM``, the following error message is displayed:

.. code-block:: console

   Failed to import XXX rows: Error - field larger than field limit (131072),  given up after Y attempts

 
Solution
^^^^^^^^

1. Locate your ``.cqlshrc`` file, which is usually under ``$HOME``.
 
2. Add the following line to that file:

.. code-block:: console

   [csv]
   field_size_limit = 1000000000

3. Optionally, adjust ``field_size_limit`` according to fit.

4. Try again

Additional References

.. include:: /troubleshooting/_common/ts-return.rst

:doc:`CQL Reference </cql/index>`

