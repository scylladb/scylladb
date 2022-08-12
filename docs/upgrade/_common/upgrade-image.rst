**To upgrade ScyllaDB and update 3rd party and OS packages (RECOMMENDED):**

.. versionadded:: Scylla 5.0
.. versionadded:: Scylla Enterprise 2021.1.10

This installation upgrade method allows you to upgrade your ScyllaDB version and update the 3rd party and OS packages using one command. This method is recommended if you run a ScyllaDB official image (EC2 AMI, GCP, and Azure images), which is based on Ubuntu 20.04.

#. Update the |SCYLLA_REPO|_ to |NEW_VERSION|.

#. Run the following command:
    
    .. code:: sh 
    
       cat scylla-packages-xxx-x86_64.txt | sudo xargs -n1 apt-get -y
    
    
    Where xxx is the relevant Scylla version ( |NEW_VERSION| ). The file is included in the Scylla packages downloaded in the previous step.
    
    For example
    
        .. code:: sh 
           
           cat scylla-packages-5.1.2-x86_64.txt | sudo xargs -n1 apt-get -y
