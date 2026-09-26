# Reset Authenticator Password

This procedure describes what to do when a user loses his password and can not reset it with a superuser role.
The procedure requires cluster downtime and as a result, all of the system_auth data is deleted.

## Procedure

1. Stop Scylla nodes (**Stop all the nodes in the cluster**).
<br/>
```shell
sudo systemctl stop scylla-server
```

2. Remove your tables under `/var/lib/scylla/data/system_auth/`.
<br/>
```shell
rm -rf /var/lib/scylla/data/system_auth/
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

[Troubleshoot](https://opensource.docs.scylladb.com/branch-5.1/troubleshooting/index.md)
