=================================================
Set Up an NVMe/TCP Block Device for ScyllaDB
=================================================

ScyllaDB performs best on local NVMe storage, but it can also run on network
block storage exported over NVMe/TCP, such as a SAN or a cloud provider's
disaggregated storage service. With network storage, the defaults of the Linux
NVMe/TCP initiator leave much of the available performance unused, because
they assume a single path to the storage and a shallow queue.

This article describes how to attach NVMe/TCP volumes to a ScyllaDB node so
that ScyllaDB can drive them at full throughput:

* Connect to every virtual IP (portal) the storage array exposes, and let
  native NVMe multipath spread I/O across all of them.
* If the storage vendor recommends several volumes per host, attach all of
  them and combine them with software RAID-0.
* Increase the queue depth of each I/O queue to 1024.
* Use one I/O queue per vCPU.

.. note:: Follow the storage vendor's documentation for anything specific to
   the array, such as provisioning volumes, granting the host access, the
   number of volumes per host, and timeouts. Where the vendor's
   recommendations conflict with this article, the vendor's take precedence.

Why These Settings Matter
-------------------------

ScyllaDB keeps many I/O requests in flight on every shard at once. With local
NVMe, each request completes in tens of microseconds; over the network, each
request takes much longer. To sustain the same throughput at a higher latency,
more requests must be in flight concurrently. The Linux NVMe/TCP defaults
limit that concurrency in three places:

* **Paths**: Every TCP connection to the array goes to a single portal, and
  usually a single storage controller behind that portal. Arrays expose
  several virtual IPs so that hosts can spread their load across controllers
  and survive a controller failover. Without multipath, only one of them is
  used. With native NVMe multipath, the default ``numa`` I/O policy still
  sends all I/O from a NUMA node through one path.
* **Queue depth**: The default NVMe/TCP queue size is 128 commands per I/O
  queue. Linux allows up to 1024.
* **Queue count**: Each I/O queue is its own TCP connection, processed by its
  own kernel worker. Using one queue per vCPU lets every CPU submit and
  complete I/O without contending with other CPUs, and spreads the network
  processing over all of them.

Some arrays also limit the throughput of a single volume, or of a single
connection. The vendor then recommends attaching several smaller volumes
instead of one large one; RAID-0 reassembles them into a single device for
ScyllaDB.

Prerequisites
-------------

* The ``nvme-cli`` and ``mdadm`` packages.
* The ``nvme-tcp`` kernel module, loaded at boot:

  .. code-block:: shell

     sudo modprobe nvme-tcp
     echo nvme-tcp | sudo tee /etc/modules-load.d/nvme-tcp.conf

  On Ubuntu cloud images, the module is in the ``linux-modules-extra``
  package, which is not installed by default, and ``modprobe`` reports that
  the module is not found. Install the package for the running kernel, and
  install ``linux-image-generic`` so that it is also installed with future
  kernel updates; otherwise the volumes are not connected after the node
  boots into an updated kernel:

  .. code-block:: shell

     sudo apt-get install linux-modules-extra-$(uname -r) linux-image-generic

* Native NVMe multipath enabled. It is enabled by default on most
  distributions; verify that the following command prints ``Y``:

  .. code-block:: shell

     cat /sys/module/nvme_core/parameters/multipath

  If it prints ``N``, add ``nvme_core.multipath=Y`` to the kernel command line
  and reboot.

* The host NQN, registered with the storage array so that the host is allowed
  to access the volumes. If ``/etc/nvme/hostnqn`` does not exist, generate it
  first:

  .. code-block:: shell

     sudo sh -c 'nvme gen-hostnqn > /etc/nvme/hostnqn'
     cat /etc/nvme/hostnqn

* The list of virtual IPs the storage array exposes for NVMe/TCP, and the
  volumes provisioned for this host.

The examples below use two virtual IPs, ``192.0.2.10`` and ``192.0.2.11``,
the standard discovery port ``8009``, and a 32-vCPU host. Replace them with
the values for your environment.

Connect to the Volumes
----------------------

#. Query the discovery controller on each virtual IP to see the subsystems and
   portals available to this host:

   .. code-block:: shell

      sudo nvme discover --transport=tcp --traddr=192.0.2.10 --trsvcid=8009
      sudo nvme discover --transport=tcp --traddr=192.0.2.11 --trsvcid=8009

   Each subsystem should be listed once for every virtual IP.

#. Connect to all discovered subsystems through all virtual IPs, with a queue
   depth of 1024 and one I/O queue per vCPU:

   .. code-block:: shell

      for ip in 192.0.2.10 192.0.2.11; do
          sudo nvme connect-all --transport=tcp --traddr=$ip --trsvcid=8009 \
              --nr-io-queues=$(nproc) --queue-size=1024
      done

   Repeating the command for every virtual IP ensures that all paths are
   connected, even if a discovery controller only reports its own portal.
   Connecting the same subsystem through several virtual IPs does not create
   duplicate disks: the kernel merges all paths to a namespace into a single
   block device, such as ``/dev/nvme1n1``.

   The kernel never creates more I/O queues than there are online CPUs, so
   ``--nr-io-queues=$(nproc)`` is also the maximum.

#. Verify that every volume has a path through every virtual IP, and that all
   paths are ``live``:

   .. code-block:: shell

      sudo nvme list-subsys

#. Verify that the storage array granted the requested queue count and depth.
   For every controller (one per path), ``queue_count`` includes the admin
   queue, so it should be the vCPU count plus one, and ``sqsize`` is zero-based,
   so it should be 1023:

   .. code-block:: shell

      for c in /sys/class/nvme/nvme*; do
          echo "$(basename $c) $(cat $c/transport) queues=$(cat $c/queue_count) sqsize=$(cat $c/sqsize)"
      done

   If the values are lower, the array limits them. Check the vendor's
   documentation for how to raise the limits on the array side.

Spread I/O Across All Paths
---------------------------

Set the multipath I/O policy of every NVMe subsystem so that I/O is spread
across all paths, rather than sent through one path per NUMA node. The
``queue-depth`` policy, available since Linux 6.11 and backported to some
distribution kernels, sends each request to the path with the fewest
outstanding requests, and copes best with paths of uneven speed. On kernels
without it, use ``round-robin``.

#. Create the udev rule ``/etc/udev/rules.d/71-nvme-iopolicy.rules``, so that
   the policy is applied every time a subsystem appears, including after a
   reboot:

   .. code-block:: none

      ACTION=="add|change", SUBSYSTEM=="nvme-subsystem", ATTR{iopolicy}="queue-depth"

#. Apply the rule to the subsystems that are already connected, and verify:

   .. code-block:: shell

      sudo udevadm control --reload
      sudo udevadm trigger --action=change --subsystem-match=nvme-subsystem
      cat /sys/class/nvme-subsystem/nvme-subsys*/iopolicy

   If the policy is still ``numa``, the kernel does not support
   ``queue-depth`` and rejected it. Replace ``queue-depth`` with
   ``round-robin`` in the rule, and repeat this step.

Reconnect at Boot
-----------------

``nvme-cli`` ships the ``nvmf-autoconnect`` service, which runs
``nvme connect-all`` at boot for every discovery controller listed in
``/etc/nvme/discovery.conf``.

#. Add one line for every virtual IP to ``/etc/nvme/discovery.conf``:

   .. code-block:: none

      --transport=tcp --traddr=192.0.2.10 --trsvcid=8009 --nr-io-queues=32 --queue-size=1024
      --transport=tcp --traddr=192.0.2.11 --trsvcid=8009 --nr-io-queues=32 --queue-size=1024

#. Disconnect the volumes when the service stops at shutdown, while the
   network is still up. Otherwise, the network is shut down first, and the
   kernel keeps trying to reconnect to the volumes for up to 10 minutes,
   holding up the shutdown. Create
   ``/etc/systemd/system/nvmf-autoconnect.service.d/disconnect-on-stop.conf``:

   .. code-block:: ini

      [Service]
      RemainAfterExit=yes
      ExecStop=nvme disconnect-all

#. Enable and start the service. The volumes are already connected, so
   starting it connects nothing new, but it must be running for
   ``ExecStop`` to run at the next shutdown:

   .. code-block:: shell

      sudo systemctl daemon-reload
      sudo systemctl enable --now nvmf-autoconnect.service

Create the File System
----------------------

Use ``scylla_raid_setup`` to create the XFS file system and mount it at
``/var/lib/scylla``. List all the NVMe/TCP volumes; when there is more than
one, ``scylla_raid_setup`` combines them with RAID-0 first.

Do this on a new node, before ScyllaDB is started for the first time. The new
file system is mounted over ``/var/lib/scylla``, hiding anything already
stored there.

.. warning:: ``scylla_raid_setup`` erases all data on the listed devices. The
   device names (``/dev/nvme1n1``, ...) depend on the order in which the
   volumes were connected; use ``nvme list`` to confirm that each device is a
   volume intended for ScyllaDB, identifying it by its serial number or
   namespace ID, before running the command.

.. code-block:: shell

   sudo nvme list
   sudo scylla_raid_setup --disks /dev/nvme1n1,/dev/nvme2n1,/dev/nvme3n1,/dev/nvme4n1 \
       --raiddev /dev/md0 --enable-on-nextboot

Always pass the multipath devices (``/dev/nvmeXnY``), never individual path
devices: a RAID member or file system on a single path loses access to the
volume when that path fails.

If the storage vendor recommends against discard (TRIM), add
``--online-discard False``.

Mount After the Network
-----------------------

``scylla_raid_setup`` creates the systemd mount unit
``/etc/systemd/system/var-lib-scylla.mount``, which assumes local storage and
mounts the file system early in boot, before the network is up. For NVMe/TCP
volumes, the mount must wait until the volumes are connected.

#. Disable the mount unit:

   .. code-block:: shell

      sudo systemctl disable var-lib-scylla.mount

#. Edit ``/etc/systemd/system/var-lib-scylla.mount``:

   * Replace ``local-fs.target`` with ``remote-fs.target`` in both the
     ``Before=`` and the ``WantedBy=`` lines.
   * Delete the ``DefaultDependencies=no`` line, so that the file system is
     unmounted before the network is shut down.
   * Add ``After=`` and ``Wants=`` lines to order the mount after the network
     and the NVMe/TCP connections.
   * Append ``,_netdev`` to the ``Options=`` line.

   Keep the ``What=`` line and the rest of the ``Options=`` line that
   ``scylla_raid_setup`` generated. The result looks similar to:

   .. code-block:: ini

      [Unit]
      Description=Scylla data directory
      Before=remote-fs.target scylla-server.service
      After=network-online.target nvmf-autoconnect.service
      Wants=network-online.target nvmf-autoconnect.service

      [Mount]
      What=/dev/disk/by-uuid/<uuid>
      Where=/var/lib/scylla
      Type=xfs
      Options=noatime,discard,lazytime,_netdev

      [Install]
      WantedBy=remote-fs.target

#. Reload systemd and re-enable the mount unit. ``scylla_raid_setup`` has
   already made ScyllaDB depend on the mount (in
   ``/etc/systemd/system/scylla-server.service.d/mounts.conf``), so ScyllaDB
   never starts on an empty ``/var/lib/scylla``:

   .. code-block:: shell

      sudo systemctl daemon-reload
      sudo systemctl enable var-lib-scylla.mount

#. Verify that the network manager's wait-online service is enabled, so that
   ``network-online.target`` is not reached before the storage network is
   configured. Otherwise, ``nvmf-autoconnect`` may run too early, fail to
   connect, and leave ScyllaDB without its data directory:

   .. code-block:: shell

      systemctl is-enabled NetworkManager-wait-online.service    # NetworkManager
      systemctl is-enabled systemd-networkd-wait-online.service  # systemd-networkd

   If the service for the network manager in use is not enabled, enable it
   with ``sudo systemctl enable``.

#. Reboot, and verify that the volumes are reconnected, the RAID device is
   assembled, and the file system is mounted:

   .. code-block:: shell

      sudo nvme list-subsys
      cat /proc/mdstat
      findmnt /var/lib/scylla

Tune ScyllaDB's I/O Scheduler
-----------------------------

Finally, measure the storage so that ScyllaDB's I/O scheduler is configured
for it rather than for local disks. Run the measurement after all the steps
above, so that it reflects the final number of paths, queues, and volumes:

.. code-block:: shell

   sudo scylla_io_setup

Run it again whenever any of these change.

Additional Information
----------------------

* :doc:`Recreate RAID devices </kb/raid-device>`
* :doc:`Setup Scripts </getting-started/system-configuration>`
* ``nvme-connect-all(1)`` and ``nvme-list-subsys(1)`` manual pages
