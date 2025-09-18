=====================
Recreate RAID devices
=====================

Scylla creates a RAID device on all storage devices assigned to it as part of Scylla setup. However, there are situations in which we want to redo just this step, without invoking the entire setup phase again. One example of such a situation is when Scylla is used in Clouds with ephemeral storage. After a hard stop, the storage devices will be reset and the previous setup will be destroyed. 
To recreate your RAID devices, run this script:

.. code-block:: shell

   scylla_raid_setup --raiddev /dev/md0 --disks <comma-separated list of disks>

After this step the storage will be mounted, formatted, and /var/lib/scylla created.


