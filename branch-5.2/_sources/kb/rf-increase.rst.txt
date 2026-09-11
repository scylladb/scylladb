=======================================================
How to Safely Increase the Replication Factor
=======================================================


**Topic: What can happen when you increase RF**


**Audience: Scylla administrators**


Issue
-----

When a Replication Factor (RF) is increased, using the :ref:`ALTER KEYSPACE <alter-keyspace-statement>` command, the data consistency is effectively dropped
by the difference of the RF_new value and the RF_old value for all pre-existing data.
Consistency will only be restored after running a repair.

Resolution
----------

When one increases an RF, one should consider that the pre-existing data will **not be streamed** to new replicas (a common misconception).

As a result, in order to make sure that you can keep on reading the old data with the same level of consistency, increase the read Consistency Level (CL) according to the following formula:

``CL_new = CL_old + RF_new - RF_old``

After you run a repair, you can decrease the CL. If RF has only been changed in a particular Data Center (DC) only the nodes in that DC have to be repaired.

Example
=======

In this example your five node cluster RF is 3 and your CL is TWO. You want to increase your RF from 3 to 5.

#. Increase the read CL by a RF_new - RF_old value.
   Following the example the RF_new is 5 and the RF_old is 3 so, 5-3 =2. You need to increase the CL by 2.
   As the CL is currently TWO, increasing it more would be QUORUM, but QUORUM would not be high enough. For now, increase it to ALL.
#. Change the RF to RF_new. In this example, you would increase the RF to 5.
#. Repair all tables of the corresponding keyspace. If RF has only been changed in a particular Data Center (DC), only the DC nodes have to be repaired.
#. Restore the reads CL to the originally intended value. For this example, QUORUM.


If you do not follow the procedure above you may start reading stale or null data after increasing the RF.

More Information
----------------

* :doc:`Fault Tolerance </architecture/architecture-fault-tolerance/>`
* :ref:`Set the RF (first time) <create-keyspace-statement>`
* :ref:`Change the RF (increase or decrease) <alter-keyspace-statement>`
