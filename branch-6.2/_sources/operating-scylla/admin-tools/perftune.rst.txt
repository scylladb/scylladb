Seastar Perftune
================

ScyllaDB's Seastar Perftune helps you run ScyllaDB with its maximum performance.
The `perftune.py <https://github.com/scylladb/seastar/blob/master/scripts/perftune.py>`_ script 
handles network and disk tuning, reassigning IRQs to specific CPU cores and freeing 
the other cores for ScyllaDB’s exclusive use.

More specifically, running the script will:

* Ban relevant IRQs from being moved by irqbalance.
* Configure various system parameters in /proc/sys.
* Distribute the IRQs (using SMP affinity configuration) among CPUs according to 
  the ``irq_cpu_mask`` value. Note that more than one physical core will be used 
  for IRQ handling for large machines.

To ensure that the tuning persists after a host reboot, we recommend creating a systemd unit 
that will run on host startup and re-tune the hosts.

perftune.yaml
------------------

The ``perfrune.yaml`` file is the output file of the ``perftune.py`` script 
(with the ``--dump-options-file`` option enabled). The script determines 
the ``irq_cpu_mask`` and prints it to ``perftume.yaml`` on the first machine.

It's important to use the same ``irq_cpu_mask`` value on other machines, even if more machines are 
added to the cluster, to avoid a  mixed-cluster situation.

Note that ``perftune.yaml`` is generated only if the file doesn’t exist.

Available Options
------------------------

You can run ``perftune.py`` with the following options: 

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Option
     - Description
   * - ``--arfs``
     - Enables/disables aRFS.
   * - ``--cpu-mask``
     - The masks of cores to use. The default is all available cores.
   * - ``--dev``
     - Specifies the device to optimize; for example, ``sda1``. You can use it more than 
       once to specify multiple devices.
   * - ``--dir``
     - Specifies the directory to optimize. You can use it more than once to specify 
       multiple directories.
   * - ``--dry-run``
     - Prints recommendations on what to do without taking any action.
   * - ``--dump-options-file``
     - Prints the configuration YAML file containing the current configuration.
   * - ``--get-cpu-mask``
     - Prints the CPU mask to be used for computation.
   * - ``--get-cpu-mask-quiet``
     - Prints the CPU mask to be used for computation. Prints the zero CPU set if that's 
       what it is.
   * - ``--get-irq-cpu-mask``
     - Prints the CPU mask to be used for IRQs binding.
   * - ``--irq-core-auto-detection-ratio``
     - Specifies a ratio for IRQ mask auto-detection. For example, if 8 is given and 
       auto-detection is requested, a single IRQ CPU core is going to be allocated 
       for every 8 CPU cores out of available according to the ``cpu_mask`` value. 
       The default is 16.
   * - ``--irq-cpu-mask``
     - The mask of the cores to be used for IRQs binding.
   * - ``--nic``
     - The network interface name. The default is ``eth0``. You can use it more than once 
       to specify multiple interfaces.
   * - ``--num-rx-queues``
     - Sets a given number of Rx queues.
   * - ``--options-file``
     - The configuration YAML file.
   * - ``--tune``
     - Specifies the components to configure. You can use it more than once to specify 
       multiple values. The available values are:

         * ``net`` - Enables tuning the network.
         * ``disks`` - Enables tuning the disks.
         * ``system`` - Enables tuning system-related components.
   * - ``--tune-clock``
     - Forces tuning of /system/clocksource. The default is ``false``. Requires the ``--tune`` option.
   * - ``--verbose``
     - Provides additional details about operations and their results.
   * - ``--write-back-cache`` 
     - Enables/disables the *write back* write cache mode.
 