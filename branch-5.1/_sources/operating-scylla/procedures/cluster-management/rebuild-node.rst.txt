============================================
Rebuild a Node After Losing the Data Volume
============================================

When running in Scylla on EC2, it is recommended to use i3 type instances, storing the data on fast, ephemeral SSD drives.
Stopping the node and starting it for whatever reason means the data on the node is lost.
This data loss applies not only to i3 type instances but to the i2 type as well.
The good news is Scylla is a HA database, and replicas of the data are stored on additional nodes.
This is also why it's highly recommended to use a replication factor of at least three per Data Center (for example, 1 DC, RF = 3, 2 DCs RF = 6).

To recover the data and rebuild the node, follow this procedure:

#. If ``auto_bootstrap`` disabled, enable it by setting the value to ``true`` in the scylla.yaml.
   Auto-bootstrap is enabled by default. To disable it, a setting is normally added to the scylla.yaml.
   The Scylla configuration file is located in ``/etc/scylla/scylla.yaml``.
   Look for ``auto_bootstrap: false``. If this setting is in the file, change it to true.

   .. note:: If the ``auto_bootstrap`` parameter is missing from ``/etc/scylla/scylla.yaml``, default setting ``true`` is applied.

#. With the yaml file still open, add, if not present,else edit, the ``replace_address_first_boot`` parameter and change it to the
   IP of the node before it restarted it might be the same IP after restart.
#. Stop Scylla Server

   .. include:: /rst_include/scylla-commands-stop-index.rst

#. If there are multiple disks, execute a RAID setup for the disks by running the following script: ``/opt/scylladb/scylla-machine-image/scylla_create_devices``.
#. Start Scylla Server

   .. include:: /rst_include/scylla-commands-start-index.rst

#. Revert the ``auto_bootstrap`` and ``replace_address_first_boot`` settings to what they were before you ran this procedure.
   For ease of use, you can comment out the ``replace_address_first_boot`` parameter.

