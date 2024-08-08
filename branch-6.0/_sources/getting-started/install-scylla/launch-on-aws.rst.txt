==========================
Launch ScyllaDB on AWS
==========================

This article will guide you through self-managed ScyllaDB deployment on AWS. For a fully-managed deployment of ScyllaDB 
as-a-service, see `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_.

Launching Instances from ScyllaDB AMI 
---------------------------------------

#. Choose your region, and click the **Node** link to open the EC2 instance creation wizard.
   
   The following table shows the latest patch release. See :doc:`AWS Images </reference/aws-images/>` for earlier releases.

   .. scylladb_aws_images_template::
      :exclude: rc,dev
      :only_latest:

#. Choose the instance type. See :ref:`Cloud Instance Recommendations for AWS <system-requirements-aws>` for the list of recommended instances.

   Other instance types will work, but with lesser performance. If you choose an instance type other than the recommended ones, make sure to run the :ref:`scylla_setup <system-configuration-scripts>` script.

#. Configure your instance details. 

   * **Number of instances** – If you are launching more than one instance, make sure to correctly set the IP of the first instance with the ``seeds`` parameter - either in the User Data (see below) or after launch.
   * **Network** – Configure the network settings.

     * Select your VPC.
     * Configure the security group. Ensure that all :ref:`ScyllaDB ports <networking-ports>` are open.

   * **Advanced Details> User Data** – Here, you can add ScyllaDB configuration options in the JSON format.
     See :ref:`scylla.yaml <admin-scylla.yaml>` for information about supported options.

     .. TODO Replace the link to scylla.yaml to the full list of supported options - when all the options are documented.
    
     The following example shows a configuration using the most popular options. 

     * ``cluster_name`` - The name of the cluster.
     * ``seed_provider`` - The IP of the first node. New nodes will use the IP of this seed node to connect to the cluster and learn the cluster topology and state. See :doc:`ScyllaDB Seed Nodes </kb/seed-nodes>`.
     * ``post_configuration_script`` - A base64 encoded bash script that will be executed after the configuration is completed.
     * ``start_scylla_on_first_boot`` - Starts ScyllaDB once the configuration is completed.

     
     Example:

     .. code-block:: json

        {
             "scylla_yaml": {
                 "cluster_name": "test-cluster",
                 "seed_provider": [{"class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
                                    "parameters": [{"seeds": "10.0.219.209"}]}],
             },
             "post_configuration_script": "#! /bin/bash\nyum install cloud-init-cfn",
             "start_scylla_on_first_boot": true
        }
      
     For full documentation of ScyllaDB AMI user data, see the `ScyllaDB Image documentation <https://github.com/scylladb/scylla-machine-image>`_.


#. Add storage.

   * ScyllaDB AMI requires XFS to work. You **must** attach at least one drive for ScyllaDB to use as XFS for the data directory. 
     When attaching more than one drive, the AMI setup will install RAID0 on all of them.
   * The ScyllaDB AMI requires at least two instance store volumes. The ScyllaDB data directory will be formatted with XFS when the instance 
     first boots. ScyllaDB will fail to start if only one volume is configured.

#. Tag your instance.
#. Click **Launch Cluster**. You now have a running ScyllaDB cluster on EC2.
#. Connect to the servers using the username ``scyllaadm``.

   .. code-block:: console

        ssh -i your-key-pair.pem scyllaadm@ec2-public-ip

   The default file paths:

   * The ``scylla.yaml`` file: ``/etc/scylla/scylla.yaml``
   * Data: ``/var/lib/scylla/``

   To check that the ScyllaDB server and the JMX component are running, run:

   .. code-block:: console
    
        nodetool status
   
Next Steps
-----------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.
* Learn about ScyllaDB at `ScyllaDB University <https://university.scylladb.com/>`_
