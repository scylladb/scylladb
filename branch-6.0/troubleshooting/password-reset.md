# Reset Authenticator Password

This procedure describes what to do when a user loses his password and can not reset it with a superuser role.
The procedure requires cluster downtime and as a result, all auth data is deleted.

(Note: If you upgraded from version 5.4 without
[enabling consistent topology updates](https://opensource.docs.scylladb.com/branch-6.0/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.md),
the keyspace name is `system_auth`.)

## Procedure

1. Stop Scylla nodes (**Stop all the nodes in the cluster**).
<br/>
```shell
sudo systemctl stop scylla-server
```

2. Remove system tables starting with `role` prefix from `/var/lib/scylla/data/system` directory.
<br/>
```shell
rm -rf /var/lib/scylla/data/system/role*
```

3. Start Scylla nodes.
<br/>
```shell
sudo systemctl start scylla-server
```

4. Verify that you can log in to your node using `cqlsh` command.
<br/>
The access is only possible using Scylla superuser.
<br/>
```cql
cqlsh -u cassandra -p cassandra
```

5. Recreate the users
<br/>

[Troubleshoot](https://opensource.docs.scylladb.com/branch-6.0/troubleshooting/index.md)
