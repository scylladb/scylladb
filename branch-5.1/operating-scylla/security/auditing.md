# Scylla Auditing Guide

#### NOTE
This feature is only available with Scylla Enterprise. If you are using Scylla Open Source, this feature will not be available.

Auditing allows the administrator to monitor activities on a Scylla cluster, including queries and data changes.
The information is stored in a Syslog or a Scylla table.

## Enabling Audit

Enabling auditing is controlled by the `audit:` parameter in the `scylla.yaml` file.
You can set the following options:

* `none` - Audit is disabled (default).
* `table` - Audit is enabled, and messages are stored in a Scylla table.
* `syslog` - Audit is enabled, and messages are sent to Syslog.

Configuring any other value results in an error at Scylla startup.

## Configuring Audit

The audit can be tuned using the following flags or `scylla.yaml` entries:

| Flag             | Default Value   | Description                                                                                                                                                        |
|------------------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| audit_categories | “”              | Comma-separated list of statement categories that should be audited                                                                                                |
| audit_tables     | “”              | Comma-separated list of table names that should be audited, in the format of <keyspacename>.<tablename>                                                            |
| audit_keyspaces  | “”              | Comma-separated list of keyspaces that should be audited. You must specify at least one keyspace.<br/>If you leave this option empty, no keyspace will be audited. |

To audit all the tables in a keyspace, set the `audit_keyspaces` with the keyspace you want to audit and leave `audit_tables` empty.

You can use DCL, AUTH, and ADMIN audit categories without including any keyspace or table.

### audit_categories parameter description

| Parameter   | Logs Description                                                                                                                                                                                                                                                                                                           |
|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| AUTH        | Logs login events                                                                                                                                                                                                                                                                                                          |
| DML         | Logs insert, update, delete, and other data manipulation language (DML) events                                                                                                                                                                                                                                             |
| DDL         | Logs object and role create, alter, drop, and other data definition language (DDL) events                                                                                                                                                                                                                                  |
| DCL         | Logs grant, revoke, create role, drop role, and list roles events                                                                                                                                                                                                                                                          |
| QUERY       | Logs all queries                                                                                                                                                                                                                                                                                                           |
| ADMIN       | Logs service level operations: create, alter, drop, attach, detach, list.<br/>For [service level](https://opensource.docs.scylladb.com/branch-5.1/using-scylla/workload-prioritization.md#workload-priorization-service-level-management)<br/>auditing, this parameter is available in Scylla Enterprise 2019.1 and later. |

Note that audit for every DML or QUERY might impact performance and consume a lot of storage.

## Configuring Audit Storage

Auditing messages can be sent to [Syslog](#auditing-syslog-storage) or stored in a Scylla [table](#auditing-table-storage).
Currently, auditing messages can only be saved to one location at a time. You cannot log into both a table and the Syslog.

<a id="auditing-syslog-storage"></a>

### Storing Audit Messages in Syslog

**Procedure**

1. Set the `audit` parameter in the `scylla.yaml` file to `syslog`.

   For example:
   ```shell
   # audit setting
   # by default, Scylla does not audit anything.
   # It is possible to enable auditing to the following places:
   #   - audit.audit_log column family by setting the flag to "table"
   audit: "syslog"
   #
   # List of statement categories that should be audited.
   audit_categories: "DCL,DDL,AUTH"
   #
   # List of tables that should be audited.
   audit_tables: "mykespace.mytable"
   #
   # List of keyspaces that should be fully audited.
   # All tables in those keyspaces will be audited
   audit_keyspaces: "mykespace"
   ```
2. Restart the Scylla node.

Supported OS

```shell
sudo systemctl restart scylla-server
```

Docker

```shell
docker exec -it some-scylla supervisorctl restart scylla
```

(without restarting *some-scylla* container)

By default, audit messages are written to the same destination as Scylla [logging](https://opensource.docs.scylladb.com/branch-5.1/getting-started/logging.md), with `scylla-audit` as the process name.

Logging output example (drop table):

```shell
Mar 18 09:53:52 ip-10-143-2-108 scylla-audit[28387]: "10.143.2.108", "DDL", "ONE", "team_roster", "nba", "DROP TABLE nba.team_roster ;", "127.0.0.1", "anonymous", "false"
```

To redirect the Syslog output to a file, follow the steps below (available only for CentOS) :

1. Install rsyslog sudo `dnf install rsyslog`.
2. Edit `/etc/rsyslog.conf` and append the following to the file: `if $programname contains 'scylla-audit' then /var/log/scylla-audit.log`.
3. Start rsyslog `systemctl start rsyslog`.
4. Enable rsyslog `systemctl enable rsyslog`.

<a id="auditing-table-storage"></a>

### Storing Audit Messages in a Table

Messages are stored in a Scylla table named `audit.audit_log`.

For example:

```shell
CREATE TABLE IF NOT EXISTS audit.audit_log (
      date timestamp,
      node inet,
      event_time timeuuid,
      category text,
      consistency text,
      table_name text,
      keyspace_name text,
      operation text,
      source inet,
      username text,
      error boolean,
      PRIMARY KEY ((date, node), event_time));
```

**Procedure**

1. Set the `audit` parameter in the `scylla.yaml` file to `table`.

   For example:
   ```shell
   # audit setting
   # by default, Scylla does not audit anything.
   # It is possible to enable auditing to the following places:
   #   - audit.audit_log column family by setting the flag to "table"
   audit: "table"
   #
   # List of statement categories that should be audited.
   audit_categories: "DCL,DDL,AUTH"
   #
   # List of tables that should be audited.
   audit_tables: "mykespace.mytable"
   #
   # List of keyspaces that should be fully audited.
   # All tables in those keyspaces will be audited
   audit_keyspaces: "mykespace"
   ```
2. Restart Scylla node.

   Supported OS
   ```shell
   sudo systemctl restart scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl restart scylla
   ```

   (without restarting *some-scylla* container)

   Table output example (drop table):
   ```shell
   SELECT * FROM audit.audit_log ;
   ```

   returns:
   ```none
    date                    | node         | event_time                           | category | consistency | error | keyspace_name | operation                    | source          | table_name  | username |
   -------------------------+--------------+--------------------------------------+----------+-------------+-------+---------------+------------------------------+-----------------+-------------+----------+
   2018-03-18 00:00:00+0000 | 10.143.2.108 | 3429b1a5-2a94-11e8-8f4e-000000000001 |      DDL |         ONE | False |           nba | DROP TABLE nba.team_roster ; | 127.0.0.1       | team_roster | Scylla   |
   (1 row)
   ```

#### Additional Resources

* [Authorization](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md)
* [Authentication](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authentication.md)
