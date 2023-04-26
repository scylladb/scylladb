There are two alternative upgrade procedures:

* :ref:`Upgrading ScyllaDB and simultaneously updating 3rd party and OS packages <upgrade-image-recommended-procedure>`. It is recommended if you are running a ScyllaDB official image (EC2 AMI, GCP, and Azure images), which is based on Ubuntu 20.04.

* :ref:`Upgrading ScyllaDB without updating any external packages <upgrade-image-enterprise-upgrade-guide-regular-procedure>`.

.. _upgrade-image-recommended-procedure:

**To upgrade ScyllaDB and update 3rd party and OS packages (RECOMMENDED):**

Choosing this upgrade procedure allows you to upgrade your ScyllaDB version and update the 3rd party and OS packages using one command. 

#. Update the |SCYLLA_REPO|_ to |NEW_VERSION|.

#. Load the new repo:

    .. code:: sh 
    
       sudo apt-get update

#. Run the following command to update the manifest file:
    
    .. code:: sh 
    
       cat scylla-enterprise-packages-<version>-<arch>.txt | sudo xargs -n1 apt-get install -y
    
    Where:

      * ``<version>`` - The ScyllaDB version to which you are upgrading ( |NEW_VERSION| ).
      * ``<arch>`` - Architecture type: ``x86_64`` or ``aarch64``.
    
    The file is included in the ScyllaDB packages downloaded in the previous step. The file location is ``http://downloads.scylladb.com/downloads/scylla-enterprise/aws/manifest/scylla-enterprise-packages-<version>-<arch>.txt``.

    Example:
    
        .. code:: console 
           
           cat scylla-enterprise-packages-2022.1.10-x86_64.txt | sudo xargs -n1 apt-get install -y


        .. note:: 

           Alternatively, you can update the manifest file with the following command:

           ``sudo apt-get install $(awk '{print $1'} scylla-enterprise-packages-<version>-<arch>.txt) -y``




.. _upgrade-image-enterprise-upgrade-guide-regular-procedure: