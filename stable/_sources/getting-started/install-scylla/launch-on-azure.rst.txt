==========================
Launch ScyllaDB on Azure
==========================

This article will guide you through self-managed ScyllaDB deployment on Azure. For a fully-managed deployment of ScyllaDB 
as-a-service, see `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_.

.. note::
    The article covers launching a ScyllaDB image from CLI. As an alternative, you can launch a ScyllaDB instance from 
    the `Azure VM portal <https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Compute%2FVirtualMachines>`_ -
    search for *ScyllaDB Image* in Community Images.

Prerequisites
----------------

* Active Azure account
* `Azure CLI <https://learn.microsoft.com/en-us/cli/azure/install-azure-cli>`_
* ScyllaDB Image requires at least 2 vCPU servers.

Launching ScyllaDB on Azure
------------------------------

#. Choose an instance type. See :ref:`Cloud Instance Recommendations for Azure <system-requirements-azure>` for the list of recommended instances.

   Other instance types will work, but with lesser performance. If you choose an instance type other than the recommended ones, make sure to run the :ref:`scylla_setup <system-configuration-scripts>` script.

#. Log in to your account with ``az login``.
#. If you have more than one subscription, choose one with ``az account set`` (see `Change the active subscription <https://learn.microsoft.com/en-us/cli/azure/manage-azure-subscriptions-azure-cli#change-the-active-subscription>`_ for details).
#. If you do not have a resource group, create one with ``az group create`` (see `az group create <https://learn.microsoft.com/en-us/cli/azure/group?view=azure-cli-latest#az-group-create>`_ for details). 

   .. code-block:: console
    
        az group create --name <name of the group> --location <region name>

   For example:

   .. code-block:: console
    
        az group create --name my-group --location eastus

#. See the following table to obtain image information for the latest patch release. 
   For earlier releases, see :doc:`Azure Images </reference/azure-images/>`

   .. scylladb_azure_images_template::
      :exclude: rc,dev
      :only_latest:

#. Get the ScyllaDB image ID using the information from the previous step:

   .. code-block:: console
      :substitutions:
    
        scyllaImageID=$(az sig image-version show-community --location <your region name> --gallery-image-definition <ScyllaDB gallery-image-definition> --gallery-image-version <ScyllaDB gallery-image-version> --public-gallery-name <ScyllDB public-gallery-name> --query ['uniqueId'] --output tsv)
   
   For example:

   .. code-block:: console
      :substitutions:
    
        scyllaImageID=$(az sig image-version show-community --location eastus --gallery-image-definition scylla-5.2 --gallery-image-version 5.2.1 --public-gallery-name 6c268694-47ab-43ab-b306-3c5514bc4112 --query ['uniqueId'] --output tsv)

#. Create VM using `az vm create <https://learn.microsoft.com/en-us/cli/azure/vm?view=azure-cli-latest#az-vm-create>`_, providing the ScyllaDB image ID from the previous step:

   .. code-block:: console
      
        az vm create --resource-group <name of your resource group> --name scylladb-vm --image $scyllaImageID --admin-username <username for the VM> --ssh-key-name <existing SSH key resource in Azure> --size <VM size to be created> --location <region name> --accept-term --public-ip-sku Standard
   
   For example:

   .. code-block:: console
      
        az vm create --resource-group my-group --name scylladb-vm --image $scyllaImageID --admin-username scyllaadm --ssh-key-name ssh-key --size Standard_L8s_v3 --location eastus --accept-term --public-ip-sku Standard

#. Connect to the servers using the SSH key and admin-username used when creating the VM. For example:

   .. code-block:: console
        
        ssh -i ~/.ssh/ssh-key.pem scyllaadm@public-ip
 
   To check that the ScyllaDB server is running, run:

   .. code-block:: console
      
        nodetool status

Next Steps
------------------

* :doc:`Configure ScyllaDB </getting-started/system-configuration>`
* Manage your clusters with `ScyllaDB Manager <https://manager.docs.scylladb.com/>`_
* Monitor your cluster and data with `ScyllaDB Monitoring <https://monitoring.docs.scylladb.com/>`_
* Get familiar with ScyllaDB’s :doc:`command line reference guide </operating-scylla/nodetool>`.
* Learn about ScyllaDB at `ScyllaDB University <https://university.scylladb.com/>`_