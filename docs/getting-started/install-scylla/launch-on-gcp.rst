=============================================
Launch ScyllaDB |CURRENT_VERSION| on GCP
=============================================

This article will guide you through self-managed ScyllaDB deployment on GCP. For a fully-managed deployment of ScyllaDB 
as-a-service, see `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_.

Prerequisites
----------------

* Active GCP account
* `Google SDK <https://cloud.google.com/sdk/docs/install>`_, which includes the ``gcloud`` command-line tool
* ScyllaDB Image requires at least 2 vCPU servers.

Launching ScyllaDB on GCP
------------------------------

#. Choose an instance type. See :ref:`Cloud Instance Recommendations for GCP <system-requirements-gcp>` for the list of recommended instances.

   Other instance types will work, but with lesser performance. If you choose an instance type other than the recommended ones, make sure to run the :ref:`scylla_setup <system-configuration-scripts>` script.

#. See the following table to obtain image information for the latest patch release. 
   For earlier releases, see :doc:`GCP Images </reference/gcp-images/>`

   .. scylladb_gcp_images_template::
      :exclude: rc,dev
      :only_latest:

#. Launch a ScyllaDB instance on GCP with ``gcloud`` using the information from the previous step. Use the following syntax:

   .. code-block:: console
      
        gcloud compute instances create <name of new instance> --image <ScyllaDB image name> --image-project < ScyllaDB project name> --local-ssd interface=nvme --zone=<GCP zone - optional> --machine-type=<machine type>
   
   For example:

   .. code-block:: console
   
        gcloud compute instances create scylla-node1 --image scylladb-5-2-1 --image-project scylla-images --local-ssd interface=nvme --machine-type=n1-highmem-8
   
   To add more storage to the VM, add multiple ``--local-ssd interface=nvme`` options to the command. For example, the following 
   command will launch a VM with 4 SSD, and 1.5TB of data (4 * `375 GB <https://cloud.google.com/compute/docs/disks/local-ssd>`_):

   .. code-block:: console
      
        gcloud compute instances create scylla-node1 --image scylladb-5-2-1 --image-project scylla-images --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --machine-type=n1-highmem-8
   
   For more information about GCP image `create` see the `Google Cloud SDK documentation <https://cloud.google.com/sdk/gcloud/reference/compute/images/create>`_.

   To customize the ScyllaDB configuration at launch (cluster name, seeds, networking, and more),
   pass cloud-init user data as described in `Configuring ScyllaDB with User Data`_.

#. (Optional) Configure firewall rules.

   Ensure that all :ref:`ScyllaDB ports <networking-ports>` are open.

#. Connect to the servers:

     .. code-block:: console

        gcloud compute ssh <name of the created instance>
    
    For example:

     .. code-block:: console
        
        gcloud compute ssh scylla-node1
   
   To check that the ScyllaDB server is running, run:

     .. code-block:: console

        nodetool status

Configuring ScyllaDB with User Data
--------------------------------------

You can customize the ScyllaDB configuration at launch time by passing cloud-init
user data to the instance. On GCP, user data is provided through the instance
``user-data`` metadata key, for example with ``--metadata-from-file``:

.. code-block:: console

   gcloud compute instances create scylla-node1 --image scylladb-5-2-1 --image-project scylla-images \
       --local-ssd interface=nvme --machine-type=n2-standard-8 \
       --metadata-from-file user-data=./user-data.yaml

The user data is a JSON or YAML document. The most commonly used options are:

.. list-table::
   :header-rows: 1
   :widths: 25 12 15 48

   * - Option
     - Type
     - Default
     - Description
   * - ``scylla_yaml``
     - object
     - ``{}``
     - Settings passed directly to ``scylla.yaml`` (for example ``cluster_name`` and ``seed_provider``). See :ref:`scylla.yaml <admin-scylla.yaml>`.
   * - ``developer_mode``
     - boolean
     - ``false``
     - Enable developer mode (relaxes production checks; not recommended for production).
   * - ``post_configuration_script``
     - string
     - ``""``
     - A bash script (optionally base64-encoded) executed after ScyllaDB configuration completes.
   * - ``post_configuration_script_timeout``
     - integer
     - ``600``
     - Timeout, in seconds, for ``post_configuration_script``.
   * - ``start_scylla_on_first_boot``
     - boolean
     - ``true``
     - Start ``scylla-server`` automatically on the first boot.
   * - ``tier1_networking``
     - boolean
     - auto-detect
     - **GCP only.** Force the network bandwidth tier ScyllaDB assumes when tuning I/O and streaming throughput. When omitted, it is auto-detected. See `GCP Tier 1 networking`_.
   * - ``device_wait_seconds``
     - integer
     - ``0``
     - Maximum number of seconds to wait for storage devices to appear before configuring them (``300`` is recommended).

For the full list of supported user-data options, see the
`ScyllaDB Machine Image documentation <https://github.com/scylladb/scylla-machine-image>`_.

Example ``user-data.yaml``:

.. code-block:: yaml

   scylla_yaml:
     cluster_name: my-cluster
     seed_provider:
       - class_name: org.apache.cassandra.locator.SimpleSeedProvider
         parameters:
           - seeds: 10.0.1.1,10.0.1.2
   start_scylla_on_first_boot: true
   device_wait_seconds: 300

GCP Tier 1 networking
^^^^^^^^^^^^^^^^^^^^^^^^

`Tier 1 networking <https://cloud.google.com/compute/docs/networking/configure-vm-with-high-bandwidth-configuration>`_
provides higher egress bandwidth on supported GCP machine types — up to 100 Gbps
on N2 and N2D instances with 48 or more vCPUs, and up to 200 Gbps on Z3 instances.
ScyllaDB uses the assumed network bandwidth to tune internal I/O and streaming
throughput, so it needs to know whether the instance runs with Tier 1 bandwidth.

By default ScyllaDB detects this automatically, in the following order of precedence:

#. The ``tier1_networking`` user-data option (``true`` / ``false``), when set.
#. The ``scylla_tier1_networking`` instance metadata attribute, when set at VM creation time.
#. The NIC link speed reported by the kernel (``/sys/class/net/<iface>/speed``).

Auto-detection requires no additional GCP API permissions and works out of the box.
Set ``tier1_networking`` explicitly only when you want to override the detected
value — for example, to force the Tier 1 bandwidth assumption on an instance where
detection is not reliable:

.. code-block:: yaml

   tier1_networking: true

.. note::

   The ``tier1_networking`` option only controls the bandwidth ScyllaDB *assumes*
   when tuning itself. To actually obtain Tier 1 bandwidth, the instance must be
   created with Tier 1 networking enabled — a supported machine type, a gVNIC
   network interface, and
   ``--network-performance-configs=total-egress-bandwidth-tier=TIER_1``. See the
   `GCP high-bandwidth configuration documentation <https://cloud.google.com/compute/docs/networking/configure-vm-with-high-bandwidth-configuration>`_.

Next Steps
---------------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.
