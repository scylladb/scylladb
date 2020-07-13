# Universal Restore Utility

Restore a backup of a set of SSTable files taken from any given Scylla/Cassandra cluster to any size/topology cluster. 


Overview of what it does and how it does it:

As a user, I have a set of sstable files I have backed up from my old cluster. The set looks like this[1]:

`root-dir/host/keyspace/table/sstable_files`

Also, the user provides me with:

- the original schema dump in a CQL file (can be sanitized to include only keyspaces/tables)
- ssh credentials for a sudo user on all nodes (key and password access are supported)
- One of the node IPs in the cluster for cluster metadata gathering
- If needed, API URL and CQLSH credentials

I have a new cluster into which I'd like to load the data. The node count and topology is irrelevant (though if the new cluster 
is multi-DC based, it would make sense to limit the destination node list to a local DC and adjust the replicas after restore, 
in order to avoid cross DC traffic)

I assume the schema has already been loaded into the new cluster.

## The workflow:

1. Parse the original schema dump, to have a list of valid keyspaces and tables.
2. Extract the cluster node IP list. 
3. Ensure schema versions are up to date, and in sync across the cluster.
4. Extract the token ring data for each valid keyspace [temporarily unavailable]

5. Find the correct destination paths for each ks/table (use system tables to avoid looking at dropped tables)
  5.1 Ensure the destination dirs are writeable
6. Build restore plan
  6.1 organize source files as bundles of 9 sstable files with source paths
  6.2 [temporarily unavailable] add the first/last tokens set for each bundle
  6.3 [temporarily unavailable] add the list of hosts for each keyspace where the file token range overlaps the node/ks token range
  6.4 add destination path (ks/table/upload) to each bundle
7. Iterate over the bundled file sets, go one source host per iteration, copying the largest bundles first.
  7.1 Measure the bundle (9 sstable files) size
  7.2 Check `df` on the destination nodes
    7.2.1 Confirm There's room for the incoming bundle
    7.2.2 Make sure the preset percentage of free space is not exceeded
    7.2.3 If disk space is low or below the threshold
      7.2.3.1 Send the API call to stop compactions
      7.2.3.2 Set the correct file permissions on the data in the upload dirs
      7.2.3.3 Call nodetool refresh[2], and wait for it to finish[3]
  7.3 If enough free space is available copy the 9-file bundle into all the relevant nodes in parallel to save time[4]
  7.4 Mark bundle as complete in the restore plan file, so that if something fails, the script can be resumed.
  7.5 When all the files from a source host have been restored, call a refresh.[5]

At this point, all the data should be in all the relevant nodes, with refresh having run.

If a restore is resumed from a ready file, goto step 7 directly.


## Usage:

```sh
    ./scylla_restore.py --help

    usage: scylla_restore.py [-h] [--config CONFIG] [--resume RESUME_FILE]
                            [-l LOGLEVEL]

    Restore a set of sstable files to a working Scylla cluster
    See restore_config.yml for configuration details

    optional arguments:
    -h, --help            show this help message and exit
    --config CONFIG, -c CONFIG
                            Use specified configuration file path

    --resume RESUME_FILE, -r RESUME_FILE
                            Resume failed file copies from a restore plan file
    -l LOGLEVEL, --loglevel LOGLEVEL
                            Set loglevel, default is INFO

```

## Optimization:


- [temporarily unavailable] Raphael provided the code to read the sstable tokens directly, without utilizing sstablemetadata. The difference in speed is insane. We are not using this optimization atm.
- [temporarily unavailable] In some cases we should be able to save on copies since not all files will have to go to all the nodes.
- Copying is done in parallel, in small controlled batches, should save time without losing track of what went where and whether it went successfully.
- Starting with the larger bundles we try to push the most data at first, when there is the most free space available.
- Copy bandwidth can be limited via the configuration setting. 

## TODO:

- Add reasonable timeouts
- Verify with a ka restore set

## Comments
[1] For the future: add support for Manager's backups on S3 instead of local file tree
[2] All the calls are via the API, not touching nodetool
[3] By monitoring the upload directory, until it's empty
[4] if a copy process fails, we cleanup the partially copied bundle and exit. The bundle where the exit happened remains 'pending' in the restore_plan.json file and we can resume from this point
[5] This way there is no need to try and avoid duplicate file names incoming from different source nodes
