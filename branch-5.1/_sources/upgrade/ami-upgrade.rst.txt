======================================================
Upgrade ScyllaDB Image: EC2 AMI, GCP, and Azure Images
======================================================

Upgrading ScyllaDB images requires updating:

* ScyllaDB packages.
* Underlying OS packages. Starting with ScyllaDB 4.6, each ScyllaDB version includes a list of 3rd party and 
  OS packages tested with the ScyllaDB release. The list depends on the base OS:
  
  * ScyllaDB Open Source **4.4** and ScyllaDB Enterprise **2020.1** or earlier are based on **CentOS 7**.
  * ScyllaDB Open Source **4.5** and ScyllaDB Enterprise **2021.1** or later are based on **Ubuntu 20.04**.

If you're running ScyllaDB Open Source 5.0 or later or ScyllaDB Enterprise 2021.1.10 or later, you can 
automatically update 3rd party and OS packages together with the ScyllaDB packages - by running one command. 

In earlier ScyllaDB versions, you have to first update the ScyllaDB packages and then update the OS packages 
in the next step.

See the relevant :doc:`upgrade guide <./index>` for detailed instructions for upgrading your ScyllaDB version.

To check your Scylla version, run the ``scylla --version`` command.
