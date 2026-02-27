Inaccessible "/var/lib/scylla" and "/var/lib/systemd/coredump" after ScyllaDB upgrade 
======================================================================================

Problem
^^^^^^^
When you reboot the machine after a ScyllaDB upgrade, you cannot access data directories under ``/var/lib/scylla``, and 
coredump saves to ``rootfs``.


The problem may occur when you upgrade ScylaDB Open Source 4.6 or later to a version of ScyllaDB Enterprise if
the ``/etc/systemd/system/var-lib-scylla.mount`` and ``/etc/systemd/system/var-lib-systemd-coredump.mount`` are 
deleted by RPM.

To avoid losing the files, the upgrade procedure includes a step to backup the .mount files. The following 
example shows the command to backup the files before the upgrade from version 5.0:

.. code-block:: console

    for conf in $( rpm -qc $(rpm -qa | grep scylla) | grep -v contains ) /etc/systemd/system/{var-lib-scylla,var-lib-systemd-coredump}.mount; do sudo cp -v $conf $conf.backup-5.0; done
 
If you don't backup the .mount files before the upgrade, the files may be lost.


Solution
^^^^^^^^

If you didn't backup the .mount files before the upgrade and the files were deleted during the upgrade, 
you need to restore them manually.

To restore ``/etc/systemd/system/var-lib-systemd-coredump.mount``, run the following:

.. code-block:: console

   $ cat << EOS | sudo tee /etc/systemd/system/var-lib-systemd-coredump.mount
   [Unit]
   Description=Save coredump to scylla data directory
   Conflicts=umount.target
   Before=scylla-server.service
   After=local-fs.target
   DefaultDependencies=no
   [Mount]
   What=/var/lib/scylla/coredump
   Where=/var/lib/systemd/coredump
   Type=none
   Options=bind
   [Install]
   WantedBy=multi-user.target
   EOS

To restore ``/etc/systemd/system/var-lib-scylla.mount``, run the following (specifying your data disk):

.. code-block:: console

   $ UUID=`blkid -s UUID -o value <specify your data disk, eg: /dev/md0>`
   $ cat << EOS | sudo tee /etc/systemd/system/var-lib-scylla.mount
   [Unit]
   Description=ScyllaDB data directory
   Before=scylla-server.service
   After=local-fs.target
   DefaultDependencies=no
   [Mount]
   What=/dev/disk/by-uuid/$UUID
   Where=/var/lib/scylla
   Type=xfs
   Options=noatime
   [Install]
   WantedBy=multi-user.target
   EOS

After restoring .mount files, you need to enable them:

.. code-block:: console

   $ sudo systemctl daemon-reload
   $ sudo systemctl enable --now var-lib-scylla.mount 
   $ sudo systemctl enable --now var-lib-systemd-coredump.mount


.. include:: /troubleshooting/_common/ts-return.rst