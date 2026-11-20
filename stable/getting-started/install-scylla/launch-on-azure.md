# Launch ScyllaDB on Azure

This article will guide you through self-managed ScyllaDB deployment on Azure. For a fully-managed deployment of ScyllaDB
as-a-service, see [ScyllaDB Cloud documentation](https://cloud.docs.scylladb.com/).

#### NOTE
The article covers launching a ScyllaDB image from CLI. As an alternative, you can launch a ScyllaDB instance from
the [Azure VM portal](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Compute%2FVirtualMachines) -
search for *ScyllaDB Image* in Community Images.

## Prerequisites

* Active Azure account
* [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
* ScyllaDB Image requires at least 2 vCPU servers.

## Launching ScyllaDB on Azure

1. Choose an instance type. See [Cloud Instance Recommendations for Azure](https://opensource.docs.scylladb.com/stable/getting-started/cloud-instance-recommendations.md#system-requirements-azure) for the list of recommended instances.

   Other instance types will work, but with lesser performance. If you choose an instance type other than the recommended ones, make sure to run the [scylla_setup](https://opensource.docs.scylladb.com/stable/getting-started/system-configuration.md#system-configuration-scripts) script.
2. Log in to your account with `az login`.
3. If you have more than one subscription, choose one with `az account set` (see [Change the active subscription](https://learn.microsoft.com/en-us/cli/azure/manage-azure-subscriptions-azure-cli#change-the-active-subscription) for details).
4. If you do not have a resource group, create one with `az group create` (see [az group create](https://learn.microsoft.com/en-us/cli/azure/group?view=azure-cli-latest#az-group-create) for details).
   ```console
   az group create --name <name of the group> --location <region name>
   ```

   For example:
   ```console
   az group create --name my-group --location eastus
   ```
5. See the following table to obtain image information for the latest patch release.
   For earlier releases, see [Azure Images](https://opensource.docs.scylladb.com/stable/reference/azure-images.md)
   <!-- -*- mode: rst -*- -->

   ### 6.2.3

   | Gallery Image Definition   | Gallery Image Version   | Public Gallery Name                           |
   |----------------------------|-------------------------|-----------------------------------------------|
   | scylla-6.2                 | 6.2.3                   | scylladb-7e8d8a04-23db-487d-87ec-0e175c0615bb |
6. Get the ScyllaDB image ID using the information from the previous step:
   ```console
     scyllaImageID=$(az sig image-version show-community --location <your region name> --gallery-image-definition <ScyllaDB gallery-image-definition> --gallery-image-version <ScyllaDB gallery-image-version> --public-gallery-name <ScyllDB public-gallery-name> --query ['uniqueId'] --output tsv)
   ```

   For example:
   ```console
     scyllaImageID=$(az sig image-version show-community --location eastus --gallery-image-definition scylla-5.2 --gallery-image-version 5.2.1 --public-gallery-name 6c268694-47ab-43ab-b306-3c5514bc4112 --query ['uniqueId'] --output tsv)
   ```
7. Create VM using [az vm create](https://learn.microsoft.com/en-us/cli/azure/vm?view=azure-cli-latest#az-vm-create), providing the ScyllaDB image ID from the previous step:
   ```console
   az vm create --resource-group <name of your resource group> --name scylladb-vm --image $scyllaImageID --admin-username <username for the VM> --ssh-key-name <existing SSH key resource in Azure> --size <VM size to be created> --location <region name> --accept-term --public-ip-sku Standard
   ```

   For example:
   ```console
   az vm create --resource-group my-group --name scylladb-vm --image $scyllaImageID --admin-username scyllaadm --ssh-key-name ssh-key --size Standard_L8s_v3 --location eastus --accept-term --public-ip-sku Standard
   ```
8. Connect to the servers using the SSH key and admin-username used when creating the VM. For example:
   ```console
   ssh -i ~/.ssh/ssh-key.pem scyllaadm@public-ip
   ```

   To check that the ScyllaDB server is running, run:
   ```console
   nodetool status
   ```

## Next Steps

* [Configure ScyllaDB](https://opensource.docs.scylladb.com/stable/getting-started/system-configuration.md)
* Manage your clusters with [ScyllaDB Manager](https://manager.docs.scylladb.com/)
* Monitor your cluster and data with [ScyllaDB Monitoring](https://monitoring.docs.scylladb.com/)
* Get familiar with ScyllaDB’s [command line reference guide](https://opensource.docs.scylladb.com/stable/operating-scylla/nodetool.md).
* Learn about ScyllaDB at [ScyllaDB University](https://university.scylladb.com/)
