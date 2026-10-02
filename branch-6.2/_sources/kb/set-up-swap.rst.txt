===========================
How to Set up a Swap Space
===========================

.. note:: It is recommended to run the ``scylla_setup`` script to setup your swap space. ``scylla_setup`` runs a script called ``scylla_swap_setup``.
   You can also run the script using the `Set Up Swap Space with a Script`_ procedure below. If this doesn't work use the procedure below to `Set Up Swap Space with a File`_.

Set Up Swap Space with a Script
-------------------------------

The help for the script can be accessed by ``scylla_swap_setup --help``.

.. code-block:: shell

   scylla_swap_setup --help
   usage: scylla_swap_setup [-h] [--swap-directory SWAP_DIRECTORY] [--swap-size SWAP_SIZE]

   Configure swap for ScyllaDB.

   optional arguments:
    -h, --help            show this help message and exit
    --swap-directory SWAP_DIRECTORY
      specify swapfile directory
    --swap-size SWAP_SIZE
      specify swapfile size in GB


**Procedure**

#. Run the script setup command in a terminal specifying the target directory and size.

.. code-block:: shell

   sudo scylla_swap_setup --swap-directory /tmp/swp --swap-size SWAP_SIZE 500


Set Up Swap Space with a File
-----------------------------

Use this procedure if the `Set Up Swap Space with a Script`_ procedure did not give you the desired results.

This tutorial is suitable for any Linux distribution.

This procedure adds 6GB of swap to your server. If you want to add a different amount, replace ``6G`` with the size you require.

Keep in mind that

``swap`` size should be set to either ``total_mem``/3 or 16GB - lower of the two.

``total_mem`` is the total size of the nodes memory.

For example:

* If the node ``total_mem`` is 18GB ``swap`` size should be set to 6GB.

* If the node ``total_mem`` is 240GB ``swap`` size should be set to 16GB.

**Procedure**

#. Create a file that will be used for swap.

   .. code-block:: shell

      sudo fallocate -l 6G /swapfile

#. Change the permissions setting on the file so that only the root user is be able to write and read the swap file.

   .. code-block:: shell

      sudo chmod 600 /swapfile

#. Run the Use *mkswap* utility to set up the file as Linux swap area.

   .. code-block:: shell

      sudo mkswap /swapfile

#. Enable the swap.

   .. code-block:: shell

      sudo swapon /swapfile

#. To make the change sustainable, open the ``/etc/fstab`` file and append the following:

   .. code-block:: shell

      /swapfile swap swap defaults 0 0

#. Verify that swap is active.

   .. code-block:: shell

      sudo swapon --show
      NAME      TYPE  SIZE   USED PRIO
      /swapfile file 6024M 507.4M   -1

Remove a Swap File
-------------------

#. Deactivate Swap.

   .. code-block:: none

      sudo swapoff -v /swapfile

#. Remove the sap file entry by editing the ``/etc/fstab`` file and removing ``/swapfile swap swap defaults 0 0``.

#. Delete the swap file.

   .. code-block:: none

      sudo rm /swapfile


Additional Information
----------------------

* `Configure swap for ScyllaDB <https://github.com/scylladb/scylla/blob/master/dist/common/scripts/scylla_swap_setup>`_
* :doc:`Setup Scripts </getting-started/system-configuration>`.
