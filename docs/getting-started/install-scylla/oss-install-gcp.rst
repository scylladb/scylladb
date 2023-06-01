.. |VERSION| replace:: 5.2
.. |IMAGE-NAME| replace:: scylladb-5-2-1
.. |IMAGE-ID| replace:: 8305314263548747356
.. |PROJECT-NAME| replace:: scylla-images

==========================
Launch ScyllaDB on GCP
==========================

This article will guide you through self-managed ScyllaDB deployment on GCP. For a fully-managed deployment of ScyllaDB 
as-a-service, see `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_.

With the right setting, ScyllaDB Image can be used for production. For the ScyllaDB Image documentation, see 
the `ScyllaDB Machine Image project <https://github.com/scylladb/scylla-machine-image>`_.

Prerequisites
----------------

* Active GCP account
* `Google SDK <https://cloud.google.com/sdk/docs/install>`_, which includes the ``gcloud`` command-line tool
* ScyllaDB Image requires at least 2 vCPU servers.

ScyllaDB |VERSION| Image Information
---------------------------------------

* Image name: |IMAGE-NAME|
* Image ID: |IMAGE-ID|
* Project name: |PROJECT-NAME|


Launching ScyllaDB on GCP
------------------------------

#. Choose an instance type. See :ref:`Cloud Instance Recommendations for GCP <system-requirements-gcp>` for the list of recommended instances.
    
   Other instance types will work, but with lesser performance. If you choose an instance type other than the recommended ones, make sure to run the :ref:`scylla_setup <system-configuration-scripts>` script.

#. Launch a ScyllaDB instance on GCP with the ``gcloud``, using the following syntax:

   .. code-block:: console
      :substitutions:

        gcloud compute instances create <name of new instance> --image |IMAGE-NAME| --image-project |PROJECT-NAME| --local-ssd interface=nvme --zone <GCP zone - optional> --machine-type=<machine type>


   For example:

   .. code-block:: console
      :substitutions:

        gcloud compute instances create scylla-node1 --image |IMAGE-NAME| --image-project |PROJECT-NAME|--local-ssd interface=nvme --machine-type=n1-highmem-8

   To add more storage to the VM, add multipe ``--local-ssd interface=nvm`` options to the command. For example, the following 
   command will launch a VM with 4 SSD, and 1.5TB of data (4 * `375 GB <https://cloud.google.com/compute/docs/disks/local-ssd>`_):

   .. code-block:: console
      :substitutions:

        gcloud compute instances create scylla-node1 --image |IMAGE-NAME| --image-project |PROJECT-NAME| --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --local-ssd interface=nvme --machine-type=n1-highmem-8

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
===========

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.
* Learn about ScyllaDB at `ScyllaDB University <https://university.scylladb.com/>`_