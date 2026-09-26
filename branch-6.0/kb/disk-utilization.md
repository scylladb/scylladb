# Snapshots and Disk Utilization

**Topic: nodetool snapshot and disk space**

**Learn: understand how nodetool snapshot utilizes disk space**

**Audience: Scylla administrators**

When you create a snapshot using [nodetool snapshot](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/snapshot.md) command, Scylla is not going to copy existing SStables to the snapshot directory as one could have expected. Instead, it is going to create hard links to them. While this may seem trivial, what should be noted is the following:

* The snapshot disk space at first will start at zero and will grow to become equal to the size of the node data set when the snapshot was created. So at the beginning there will not be any significant increase in the disk space utilization.
* While it may seem plausible to believe that the snapshot image is immediately created, the snapshot eventually grows to its expected size
* Background compactions will delete data from SSTables as normal, but any table with a hard link (part of the snapshot) will not be deleted. This will take up disk space.
* This in particular means that once you have created the snapshot, its “true size” begins at zero and will eventually become equal to the size of the node data set when the snapshot was created.

In this example, a snapshot was created. As you can see, its `True size` value is 0 bytes:

```none
nodetool listsnapshots
Snapshot Details:

Snapshot name Keyspace name Column family name True size Size on disk
1574708464997 ks3           standard1          0 bytes   2.23 GB

Total TrueDiskSpaceUsed: 0 bytes
```

After time, you can see its `True size` is the same as the space it utilizes on disk:

```none
nodetool compact ks3
nodetool listsnapshots

Snapshot Details:
Snapshot name Keyspace name Column family name True size Size on disk
1574708464997 ks3           standard1          2.23 GB   2.23 GB

Total TrueDiskSpaceUsed: 2.23 GiB
```

#### NOTE
A major compaction makes the true snapshot size jump to the data size immediately as the old SSTables are being deleted.

## Additional References

* [Nodetool Snapshoot](https://opensource.docs.scylladb.com/branch-6.0/operating-scylla/nodetool-commands/snapshot.md)
