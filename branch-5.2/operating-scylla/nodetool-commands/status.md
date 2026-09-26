# Nodetool status

**status** - This command prints the cluster information for a single keyspace or all keyspaces.

For example:

```default
nodetool status
```

Example output:

```console
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load       Tokens  Owns (effective)  Host ID                               Rack
UN  127.0.0.1  394.97 MB  256     33.4%             292a6c7f-2063-484c-b54d-9015216f1750  rack1
UN  127.0.0.2  151.07 MB  256     34.3%             102b6ecd-2081-4073-8172-bf818c35e27b  rack1
UN  127.0.0.3  249.07 MB  256     32.3%             20db6ecd-2981-447s-l172-jf118c17o27y  rack1
```

| Parameter   | Description                                                                                                                                                                                                                                      |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Datacenter  | The data center that holds<br/>the information.                                                                                                                                                                                                  |
| Status      | `U` - The node is up.<br/><br/>`D` - The node is down.                                                                                                                                                                                           |
| State       | `N` - Normal<br/><br/>`L` - Leaving<br/><br/>`J` - Joining<br/><br/>`M` - Moving                                                                                                                                                                 |
| Address     | The IP address of the node.                                                                                                                                                                                                                      |
| Load        | The size on disk the Scylla data takes<br/>up (updates every 60 seconds).                                                                                                                                                                        |
| Tokens      | The number of tokens per node.                                                                                                                                                                                                                   |
| Owns        | The percentage of data owned by<br/>the node (per datacenter) multiplied by<br/>the replication factor you are using.<br/><br/>For example, if the node owns 25% of<br/>the data and the replication factor<br/>is 4, the value will equal 100%. |
| Host ID     | The unique identifier (UUID)<br/>automatically assigned to the node.                                                                                                                                                                             |
| Rack        | The name of the rack.                                                                                                                                                                                                                            |

[Nodetool Reference](https://opensource.docs.scylladb.com/branch-5.2/operating-scylla/nodetool.md)
