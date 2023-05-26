Configure Scylla
================

System configuration steps are performed automatically by the Scylla RPM and deb packages. For information on getting started with Scylla, see :doc:`Getting Started </getting-started/index>`.

All Scylla AMIs and Docker images are pre-configured by a script with the following steps. This document is provided as a reference.

.. _system-configuration-files-and-scripts:

System Configuration Files and Scripts
--------------------------------------
Several system configuration settings should be applied. For ease of use, the necessary scripts and configuration files are provided. Files are under :code:`dist/common` and :code:`seastar/scripts` in the Scylla source code and installed in the appropriate system locations. (For information on Scylla’s own configuration file, see Scylla Configuration.)

.. list-table:: System Configuration Files
   :widths: 50 50
   :header-rows: 1

   * - File Name
     - Description
   * - scylla.conf
     - Remove system resource limits
   * - scylla-server
     - Server startup options
   * - (written by ``scylla_coredump_setup``, below)
     - Configure core dumps to use the ``scylla_save_coredump`` script

.. _system-configuration-scripts:

Scylla Scripts
--------------

The following scripts are available for you to run for configuring Scylla. Some of these scripts are included in the scylla_setup script. This script is used for configuring Scylla the first time, or when the system hardware changes.


.. list-table:: Scylla Setup Scripts
   :widths: 40 60
   :header-rows: 1

   * - perftune.py
     - Configures various system parameters in order to improve the Seastar application performance
   * - scylla_bootparam_setup
     - Sets the kernel options in the bootloader.  In addition, it tunes Linux boot-time parameters for the node that Scylla is running on (e.g. huge page setup).
   * - scylla_coredump_setup
     - Sets up coredump facilities for Scylla. This may include uninstalling existing crash reporting software for compatibility reasons.
   * - scylla_io_setup
     - Benchmarks the disks and generates the io.conf and io_properties.yaml files.
   * - scylla_ntp_setup
     - Configures Network Time Protocol
   * - scylla_prepare
     - This script is run automatically every time Scylla starts and the machine needs to be tuned.
   * - scylla_raid_setup
     - Configures RAID and makes an XFS filesystem.
   * - scylla_save_coredump
     - Compresses a core dump file (Ubuntu only)
   * - scylla_setup
     - Sets up the Scylla configuration. Many of these scripts are included in the setup script.
   * - scylla_stop
     - Resets network mode if running in virtio or DPDK mode.
   * - scylla_swap_setup
     - Configures a swap space on the host.
   * - scylla_sysconfig_setup
     - Rewrites the /etc/sysconfig/scylla file.


.. list-table:: Scylla Scripts (Not included with Scylla-Setup)
   :widths: 40 60
   :header-rows: 1

   * - Script Name
     - Description
   * - node_health_check
     - Gathers metrics and information on the node, checking that the node is configured correctly.
   * - scylla-blocktune
     - Tunes the filesystem and block layer (e.g. block size I/O scheduler configuration) for Scylla.
   * - scylla_cpuscaling_setup
     - Configures the CPU frequency scaling (IOW, puts the CPU in "performance" mode, instead of the slower "powersave" mode).
   * - scylla_cpuset_setup
     - Configures which CPUs the Scylla server threads run on.
   * - scylla_fstrim
     - Runs ``fstrim``, which cleans up unused blocks of data from your SSD storage device. It runs automatically if you run scylla_fstrim_set up (see below).
   * - scylla_fstrim_setup
     - Configures a job so that ``fstrim`` runs automatically.
   * - scylla-housekeeping
     - Checks if there are new versions of Scylla available, and also shares some telemetry information for us to keep track of what versions are installed on the field.
   * - scylla_rsyslog_setup
     - Configures the "rsyslog" service, which is used to send logs to a remote server.
   * - scylla_selinux_setup
     - Disables SELinux for Scylla.

.. _note-io:

.. include:: /getting-started/_common/note-io.rst

Bootloader Settings
-------------------
If Scylla is installed on an Amazon AMI, the bootloader should provide the :code:`clocksource=tsc` and :code:`tsc=reliable` options. This enables an accurate, high-resolution `Time Stamp Counter (TSC) <https://software.intel.com/en-us/blogs/2013/06/20/eliminate-the-dreaded-clocksource-is-unstable-message-switch-to-tsc-for-a-stable>`_ for setting the system time.

This configuration is provided in the file :code:`/usr/lib/scylla/scylla_bootparam_setup`.

Remove Crash Reporting Software
-------------------------------
Remove the :code:`apport-noui` or :code:`abrt` packages if present, and set up a location and file name pattern for core dumps.

This configuration is provided in the file :code:`/usr/lib/scylla/scylla_bootparam_setup`.

Set Up Network Time Synchronization
-----------------------------------
It is highly recommended to enforce time synchronization between Scylla servers.

Run :code:`ntpstat` on all nodes to check that system time is synchronized. If you are running in a virtualized environment and your system time is set on the host, you may not need to run NTP on the guest. Check the documentation for your platform.

If you have your own time servers shared with an application using Scylla, use the same NTP configuration as for your application servers. The script :code:`/usr/lib/scylla/scylla_ntp_setup` provides sensible defaults, using Amazon NTP servers if installed on the Amazon cloud, and other pool NTP servers otherwise.

Set Up RAID and Filesystem
--------------------------
Setting the file system to XFS is the most important and mandatory for production. Scylla will significantly slow down without it.

The script :code:`/usr/lib/scylla/scylla_raid_setup` performs the necessary RAID configuration and XFS filesystem creation for Scylla.

Arguments to the script are

* :code:`-d` specify disks for RAID
* :code:`-r` MD device name for RAID
* :code:`-u` update /etc/fstab for RAID

On the Scylla AMI, the RAID configuration is handled automatically in the :code:`/usr/lib/scylla/scylla_prepare script`.

CPU Pinning
-----------

When installing Scylla, it is highly recommended to use the :doc:`scylla_setup </getting-started/system-configuration>` script.
Scylla should not share CPUs with any CPU consuming process. In addition, when running Scylla on AWS, we recommend pinning all NIC IRQs to CPU0 (due to the same reason). As a result, Scylla should be prevented from running on CPU0 and its hyper-threading siblings. To verify that Scylla is pinning CPU0, use the command below:
If the node has four or fewer CPUs, don't use this option.

To verify:

.. code-block:: shell

   cat /etc/scylla.d/cpuset.conf 


Example output:

.. code-block:: shell

   --cpuset `1-15,17-31`

Networking
----------
On AWS:
^^^^^^^
1. Prevent irqbalance from moving your NICs’ IRQs.
2. Bind all NICs’ HW queues to CPU0:

.. code-block:: shell

   for irq in `cat /proc/interrupts | grep <networking iface name> | cut -d":" -f1`
   do echo "Binding IRQ $irq to CPU0" echo 1 > /proc/irq/$irq/smp_affinity done

3. Enable RPS and bind RPS queues to CPUs other than CPU0 and its hyper-threading siblings.
4. Enable XPS and distribute all XPS queues among all available CPUs.

The `posix_net_conf.sh <https://github.com/scylladb/seastar/blob/master/scripts/posix_net_conf.sh>`_ script does all of the above.*

On Bare Metal Setups with Multi-Queue NICs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1. Prevent irqbalance from moving your NICs IRQs.
2. Bind each NIC’s IRQ to a separate CPU.
3. Enable XPS exactly the same way as for AWS above.
4. Set higher values for a listen() socket backlog and for unacknowledged pending connections backlog:

.. code-block:: shell

   echo 4096 > /proc/sys/net/core/somaxconn
   echo 4096 > /proc/sys/net/ipv4/tcp_max_syn_backlog

The `posix_net_conf.sh <https://github.com/scylladb/seastar/blob/master/scripts/posix_net_conf.sh>`_ script with the :code:`-mq` parameter does all of the above.

Configuring Scylla
------------------
Configuration for Scylla itself is in the :ref:`Scylla Configuration <admin-scylla-configuration>` section of the administration guide.

Development System Configuration
--------------------------------
*The following item is not required in production.*

When working on DPDK support for Scylla, enable hugepages.

.. code-block:: shell

   NR_HUGEPAGES=128
   mount -t hugetlbfs -o pagesize=2097152 none /mnt/huge
   mount -t hugetlbfs -o pagesize=2097152 none /dev/hugepages/
   for n in /sys/devices/system/node/node?; do
       echo $NR_HUGEPAGES > $n/hugepages/hugepages-2048kB/nr_hugepages;
   done

Huge page configuration is written to :code:`/etc/sysconfig/scylla-server` by the script :code:`/usr/lib/scylla/sysconfig_setup`




Related Topics
--------------

:doc:`System Limits </kb/system-limits>` - outlines the system limits which should be set or removed.

.. include:: /rst_include/advance-index.rst
