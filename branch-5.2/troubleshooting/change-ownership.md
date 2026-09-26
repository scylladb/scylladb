# ScyllaDB Fails to Start Due to Wrong Ownership Problems

In cases where a Scylla node fails to start because there is improper ownership, the following steps will help.

## Phenomena

Scylla node fails to start.

In cases where the Scylla node fails to start, check Scylla [logs](https://opensource.docs.scylladb.com/branch-5.2/getting-started/logging.md). If you see the following error message:
Could not access `<PATH>: Permission denied std::system_error (error system:13, Permission denied)`.

For example:

```shell
Jul 01 07:31:48 ip-172-16-12-198 scylla[12189]:  [shard 0] init - Could not access /var/lib/scylla/commitlog: std::system_error (error system:13, Permission denied)
Jul 01 07:31:48 ip-172-16-12-198 scylla[12189]:  [shard 0] init - Could not access /var/lib/scylla/data: std::system_error (error system:13, Permission denied)
Jul 01 07:31:48 ip-172-16-12-198 scylla[12189]:  [shard 0] seastar - Exiting on unhandled exception: std::system_error (error system:13, Permission denied)
```

## Problem

The data directories `/var/lib/scylla/data` and `/var/lib/scylla/commitlog` exist but are not owned by the Scylla user.

For example:

```shell
[centos@ip-172-16-12-132 scylla]$ ls /var/lib/scylla/data
total 4
drwxr-xr-x 2 root root 4096 Jun 18 09:37 commitlog
drwxr-xr-x 7 root root   97 Jun 18 09:37 data
```

In this example, the user root is the owner of the directories.

## Solution

1. Change the data directory ownership.

```shell
sudo chown scylla:scylla /var/lib/scylla/data
```

```shell
sudo chown scylla:scylla /var/lib/scylla/commitlog
```

1. Verify that the change completed successfully

```shell
[centos@ip-172-16-12-132 scylla]$ ls /var/lib/scylla/data
total 4
drwxr-xr-x 2 scylla scylla 4096 Jun 18 09:37 commitlog
drwxr-xr-x 7 scylla scylla   97 Jun 18 09:37 data
```

1. Start Scylla node.

Supported OS

```shell
sudo systemctl start scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl start scylla
```

(with *some-scylla* container already running)

1. Verify Scylla node is working

Supported OS

```shell
sudo systemctl status scylla-server
```

[Troubleshoot](https://opensource.docs.scylladb.com/branch-5.2/troubleshooting/index.md)
