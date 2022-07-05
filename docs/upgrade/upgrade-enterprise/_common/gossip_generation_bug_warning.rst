
.. note::

   If **any** of your instances are running Scylla Enterprise 2019.1.6 or earlier, **and** one of your Scylla nodes is up for more than a year, you might have been exposed to issue `#6063 <https://github.com/scylladb/scylla/pull/6083>`_.
   One way to check this is by comparing `Generation No`  (from `nodetool gossipinfo` output) with the current time in Epoch format (`date +%s`), and check if the difference is higher than one year (31536000 seconds).
   See `scylla-check-gossiper-generation <https://github.com/scylladb/scylla-code-samples/tree/master/scylla-check-gossiper-generation>`_ for a script to do just that.

   If this is the case, do **not** initiate the upgrade process before consulting with Scylla Support for further instructions.
   
