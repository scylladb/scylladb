# How to Set up a Swap Space

#### NOTE
It is recommended to run the `scylla_setup` script to setup your swap space. `scylla_setup` runs a script called `scylla_swap_setup`.
You can also run the script using the [Set Up Swap Space with a Script]() procedure below. If this doesn’t work use the procedure below to [Set Up Swap Space with a File]().

## Set Up Swap Space with a Script

The help for the script can be accessed by `scylla_swap_setup --help`.

```shell
scylla_swap_setup --help
usage: scylla_swap_setup [-h] [--swap-directory SWAP_DIRECTORY] [--swap-size SWAP_SIZE]

Configure swap for Scylla.

optional arguments:
 -h, --help            show this help message and exit
 --swap-directory SWAP_DIRECTORY
   specify swapfile directory
 --swap-size SWAP_SIZE
   specify swapfile size in GB
```

**Procedure**

1. Run the script setup command in a terminal specifying the target directory and size.

```shell
sudo scylla_swap_setup --swap-directory /tmp/swp --swap-size SWAP_SIZE 500
```

## Set Up Swap Space with a File

Use this procedure if the [Set Up Swap Space with a Script]() procedure did not give you the desired results.

This tutorial is suitable for any Linux distribution.

This procedure adds 6GB of swap to your server. If you want to add a different amount, replace `6G` with the size you require.

Keep in mind that

`swap` size should be set to either `total_mem`/3 or 16GB - lower of the two.

`total_mem` is the total size of the nodes memory.

For example:

* If the node `total_mem` is 18GB `swap` size should be set to 6GB.
* If the node `total_mem` is 240GB `swap` size should be set to 16GB.

**Procedure**

1. Create a file that will be used for swap.
   ```shell
   sudo fallocate -l 6G /swapfile
   ```
2. Change the permissions setting on the file so that only the root user is be able to write and read the swap file.
   ```shell
   sudo chmod 600 /swapfile
   ```
3. Run the Use *mkswap* utility to set up the file as Linux swap area.
   ```shell
   sudo mkswap /swapfile
   ```
4. Enable the swap.
   ```shell
   sudo swapon /swapfile
   ```
5. To make the change sustainable, open the `/etc/fstab` file and append the following:
   ```shell
   /swapfile swap swap defaults 0 0
   ```
6. Verify that swap is active.
   ```shell
   sudo swapon --show
   NAME      TYPE  SIZE   USED PRIO
   /swapfile file 6024M 507.4M   -1
   ```

## Remove a Swap File

1. Deactivate Swap.
   ```none
   sudo swapoff -v /swapfile
   ```
2. Remove the sap file entry by editing the `/etc/fstab` file and removing `/swapfile swap swap defaults 0 0`.
3. Delete the swap file.
   ```none
   sudo rm /swapfile
   ```

## Additional Information

* [Configure swap for Scylla](https://github.com/scylladb/scylla/blob/master/dist/common/scripts/scylla_swap_setup)
* [Setup Scripts](https://opensource.docs.scylladb.com/branch-6.0/getting-started/system-configuration.md).
