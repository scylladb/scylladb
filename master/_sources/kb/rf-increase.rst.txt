=======================================================
How to Safely Increase the Replication Factor
=======================================================

A replication factor (RF) is configured per keyspace. You can change the RF
using the :ref:`ALTER KEYSPACE <alter-keyspace-statement>` command. 

To increase the RF safely, ensure you follow the guidelines below.
The guidelines differ depending on whether your a keyspace is tablets-based
(the default) or has tablets disabled. See :doc:`Data Distribution with Tablets </architecture/tablets>`
for more information about tablets.

Increasing the RF in Tablets-based Keyspaces
-------------------------------------------------

If a keyspace has tablets enabled (the default), changing the RF does not
impact data consistency in the cluster.

However, due to limitations in the current protocol used to pass tablet data
to drivers, drivers will not pick up new replicas after the RF is increased.
As a result, drivers will not route requests to new replicas, causing imbalance.

To avoid this issue, restart the client applications after the ALTER statement
that changes the RF completes successfully.

Increasing the RF in Keyspaces with Tablets Disabled
----------------------------------------------------------

If you :ref:`opted out of tablets when creating a keyspace <tablets-enable-tablets>`,
so your keyspace is vnodes-based, increasing the RF will impact data consistency.

Data consistency in your cluster is effectively dropped by the difference
between the RF_new value and the RF_old value for all pre-existing data.
Consistency will only be restored after running a repair.


Resolution
========================

When you increase the RF, you should be aware that the pre-existing data will
**not be streamed** to new replicas (a common misconception).

As a result, in order to make sure that you can keep on reading the old data
with the same level of consistency:

#. Increase the read Consistency Level (CL) according to the following formula:

   .. code::

      CL_new = CL_old + RF_new - RF_old

#. Run repair.
#. Decrease the CL.


If RF has only been changed in a particular Datacenter (DC), only the nodes in
that DC have to be repaired.


Example
=======

In this example, your five-node cluster RF is 3 and your CL is TWO. You want to increase your RF from 3 to 5.

#. Increase the read CL by a RF_new - RF_old value.
   Following the example the RF_new is 5 and the RF_old is 3 so, 5-3 =2. You need to increase the CL by 2.
   As the CL is currently TWO, increasing it more would be QUORUM, but QUORUM would not be high enough. For now, increase it to ALL.
#. Change the RF to RF_new. In this example, you would increase the RF to 5.
#. Repair all tables of the corresponding keyspace. If RF has only been changed in a particular Data Center (DC), only the DC nodes have to be repaired.
#. Restore the reads CL to the originally intended value. For this example, QUORUM.


If you do not follow the procedure above, you may start reading stale or null data after increasing the RF.

References
----------------

* :doc:`Fault Tolerance </architecture/architecture-fault-tolerance/>`
* :ref:`Set the RF (first time) <create-keyspace-statement>`
* :ref:`Change the RF (increase or decrease) <alter-keyspace-statement>`
