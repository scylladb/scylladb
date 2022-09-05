**To upgrade ScyllaDB and update 3rd party and OS packages (RECOMMENDED):**

.. versionadded:: Scylla 5.0
.. versionadded:: Scylla Enterprise 2021.1.10

This installation upgrade method allows you to upgrade your ScyllaDB version and update the 3rd party and OS packages using one command. This method is recommended if you run a ScyllaDB official image (EC2 AMI, GCP, and Azure images), which is based on Ubuntu 20.04.

#. Update the |SCYLLA_REPO|_ to |NEW_VERSION|.

#. Run the following command to update the manifest file:
    
    .. code:: sh 
    
       cat scylla-packages-<version>-<arch>.txt | sudo xargs -n1 apt-get -y
    
    Where:

      * ``<version>`` - The Scylla version to which you are upgrading ( |NEW_VERSION| ).
      * ``<arch>`` - Architecture type: ``x86_64`` or ``aarch64``.
    
    The file is included in the ScyllaDB packages downloaded in the previous step. The file location is:

     * ScyllaDB Enterprise: ``http://downloads.scylladb.com/downloads/scylla-enterprise/aws/manifest/scylla-enterprise-packages-<version>-<arch>.txt``
     * ScyllaDB Open Source: ``http://downloads.scylladb.com/downloads/scylla/aws/manifest/scylla-packages-<version>-<arch>.txt``

    Example:
    
        .. code:: sh 
           
           cat scylla-packages-5.1.2-x86_64.txt | sudo xargs -n1 apt-get -y
