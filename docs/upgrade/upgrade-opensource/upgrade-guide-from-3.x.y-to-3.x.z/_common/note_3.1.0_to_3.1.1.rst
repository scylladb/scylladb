
.. warning::

   If and only if you installed a fresh Scylla 3.1.0, you must add the following line to ``scylla.yaml`` of each node before upgrading to a later Scylla version:

   ``enable_3_1_0_compatibility_mode: true``

   This is not relevant if your cluster was upgraded to 3.1.0 from an older version, or you are upgrading from or to any other Scylla release.


