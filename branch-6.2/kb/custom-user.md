# Run ScyllaDB and supporting services as a custom user:group

**Topic: Planning and setup**
By default, ScyllaDB runs as user `scylla` in group `scylla`. The following procedure will allow you to use a custom user and group to run ScyllaDB.
1. Create the new user and update file permissions

```sh
useradd test
groupadd test
usermod test -G test
chown -R test:test /var/lib/scylla
```

1. Edit `/etc/sysconfig/scylla-server` and change the USER and GROUP

```sh
USER=test
GROUP=test
```

1. Edit `/etc/systemd/system/multi-user.target.wants/scylla-server.service`

```sh
User=test
```

1. Edit `/etc/systemd/system/multi-user.target.wants/node-exporter.service`

```sh
User=test
Group=test
```

1. Reload the daemon settings and start ScyllaDB and node_exporter

```sh
systemctl daemon-reload
systemctl start scylla-server
systemctl start node-exporter
```

At this point, all  services should be started as test:test user:

```sh
test      8760     1 11 14:42 ?        00:00:01 /usr/bin/scylla --log-to-syslog 1 --log-to-std ...
test     13638     1  0 14:30 ?        00:00:00 /usr/bin/node_exporter --collector.interrupts
```
