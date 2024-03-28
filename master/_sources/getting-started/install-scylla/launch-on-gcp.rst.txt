==========================
Launch ScyllaDB on GCP
==========================

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

#. Go to `ScyllaDB for GCP <https://www.scylladb.com/download/?platform=gcp#open-source>`_ in ScyllaDB's download center to obtain the image information:

   * image name
   * image id
   * project name

#. Launch a ScyllaDB instance on GCP with ``gcloud`` using the information from the previous step. Use the following syntax:

   .. code-block:: console
      
        gcloud compute instances create <name of new instance> --image <ScyllaDB image name> --image-project < ScyllaDB project name> --local-ssd interface=nvme --zone <GCP zone - optional> --machine-type=<machine type>
   
   For example:

   .. code-block:: console
   
        gcloud compute instances create scylla-node1 --image scylladb-5-2-1 --image-project scylla-images --local-ssd interface=nvme --machine-type=n1-highmem-8
   
   To add more storage to the VM, add multiple ``--local-ssd interface=nvm`` options to the command. For example, the following 
   command will launch a VM with 4 SSD, and 1.5TB of data (4 * `375 GB <https://cloud.google.com/compute/docs/disks/local-ssd>`_):

   .. code-block:: console
      
        gcloud compute instances create scylla-node1 --image scylladb-5-2-1 --image-project scylla-images --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --machine-type=n1-highmem-8
   
   For more information about GCP image `create` see the `Google Cloud SDK documentation <https://cloud.google.com/sdk/gcloud/reference/compute/images/create>`_.

#. (Optional) Configure firewall rules.

   Ensure that all :ref:`ScyllaDB ports <networking-ports>` are open.

#. Connect to the servers:

     .. code-block:: console

        gcloud compute ssh <name of the created instance>
    
    For example:

     .. code-block:: console
        
        gcloud compute ssh scylla-node1
   
   To check that the ScyllaDB server and the JMX component are running, run:

     .. code-block:: console
      
        nodetool status

Next Steps
---------------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.