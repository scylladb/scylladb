Seastar Perftune
================

Configure various system parameters to improve the seastar application performance, called from of scylla_setup script.

This script will:

* Ban relevant IRQs from being moved by irqbalance.
* Configure various system parameters in /proc/sys.
* Distribute the IRQs (using SMP affinity configuration) among CPUs according to the configuration mode (see below).

As a result, some of the CPUs may be destined only to handle the IRQs and taken out of the CPU set to be used to run the seastar application ("compute CPU set").


perftune.py [options]

Options:

* ``--mode`` -  configuration mode (see below)
* ``--nic`` -  network interface name(s), by default uses *eth0* (may appear more than once)
* ``--tune-clock`` -  Force tuning of the system clocksource
* ``--get-cpu-mask`` - print the CPU mask to be used for compute
* ``--get-cpu-mask-quiet`` - print the CPU mask to be used for compute, print the zero CPU set if that's what it turns out to be
* ``--verbose`` - be more verbose about operations and their result
* ``--tune`` -  components to configure (may be given more than once)
* ``--cpu-mask`` - mask of cores to use, by default use all available cores
* ``--irq-cpu-mask``- mask of cores to use for IRQs binding
* ``--dir`` - directory to optimize (may appear more than once)
* ``--dev`` device to optimize (may appear more than once), e.g. sda1
* ``--options-file`` - configuration YAML file
* ``--dump-options-file`` - Print the configuration YAML file containing the current configuration
* ``--dry-run`` - Don't take any action, just recommend what to do
* ``--write-back-cache`` Enable/Disable *write back* write cache mode

Modes description:

* *sq* - set all IRQs of a given NIC to CPU0 and configure RPS to spreads NAPIs' handling between other CPUs.
* *sq_split* - divide all IRQs of a given NIC between CPU0 and its HT siblings and configure RPS to spreads NAPIs' handling between other CPUs.
* *mq* - distribute NIC's IRQs among all CPUs instead of binding them all to CPU0. In this mode RPS is always enabled to spreads NAPIs' handling between all CPUs.

If there isn't any mode given script will use a default mode:

- If number of physical CPU cores per Rx HW queue is greater than 4 - use the 'sq-split' mode.
- Otherwise, if number of hyperthreads per Rx HW queue is greater than 4 - use the 'sq' mode.
- Otherwise use the 'mq' mode.

Default values:

* --nic NIC: eth0
* --cpu-mask MASK: all available cores mask
* --tune-clock: false
