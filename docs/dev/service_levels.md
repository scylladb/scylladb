## Service Level Distributed Data

There are two system tables that are used to facilitate the service level feature.


### Service Level Attachment Table

```
    CREATE TABLE system.role_attributes (
    role text,
    attribute_name text,
    attribute_value text,
    PRIMARY KEY (role, attribute_name))
```
The table was created with generality in mind, but its purpose is to record
information about roles. The table columns meaning are:
*role* - the name of the role that the attribute belongs to.
*attribute_name* - the name of the attribute for the role.
*attribute_value* - the value of the specified attribute.

For the service level, the relevant attribute name is `service_level`.
So for example in order to find out which `service_level` is attached to role `r`
one can run the following query:

```
SELECT * FROM  system.role_attributes WHERE role='r' and attribute_name='service_level'

```

### Service Level Configuration Table

```
    CREATE TABLE system_distributed.service_levels (
    service_level text PRIMARY KEY,
    timeout duration,
    workload_type text,
    shares int);
```

The table is used to store and distribute the service levels configuration.
The table column names meanings are:
*service_level* - the name of the service level.
*timeout* - timeout for operations performed by users under this service level
*workload_type* - type of workload declared for this service level (NULL, interactive or batch)
*shares* - a number that represents this service level priority in relation to other service levels.

```
select * from system_distributed.service_levels ;

 service_level | timeout | workload_type
---------------+---------+---------------
            sl |   500ms |   interactive

```

### Service Level Timeout

Service level timeout can be used to assign a default timeout value for all operations for a particular service level.

Service level timeout takes precedence over default timeout values from scylla.yaml configuration
file, but it can still be superseded by per-query timeouts (issuing a query with USING TIMEOUT directive).

In order to set a timeout for a service level, create or alter it with proper parameters, e.g.:
```
create service level sl with timeout = 50ms;
list all service levels;

 service_level | timeout 
---------------+---------
            sl |    50ms

```

Restoring the default timeout value (from scylla.yaml file) can be done by setting the service level timeout value to null:
```
alter service level sl with timeout = null;
list all service levels;

 service_level | timeout 
---------------+---------
            sl |    null

```

#### Combining service level timeouts from multiple roles

A single role may be granted multiple other roles, which also means that more than one service level may be in effect
for a particular user. In case of timeouts, multiple timeout values are combined by using a minimum of all effective
timeouts. Example:

role1: `timeout = 1s`
role2: `timeout = 50ms`
role3: `timeout = 2s`
role4: `timeout = 10ms`

The granting hierarchy is as follows, with role1 inheriting from role2, which in turn
inherits from role3 and role4:

      role4  role3
         \   /
         role2
          /
       role1
        
With the following roles granted, here are the effective timeouts for the roles:

role1: `timeout = 10ms`
role2: `timeout = 10ms`
role3: `timeout = 2s`
role4: `timeout = 10ms`

### Workload types

It's possible to declare a workload type for a service level, currently out of three available values:
 1. NULL - unspecified workload without any characteristics; default
 2. interactive - workload sensitive to latency, expected to have high/unbounded concurrency,
    with dynamic characteristics, OLTP;
    example: users clicking on a website and generating events with their clicks
 3. batch - workload for processing large amounts of data, not sensitive to latency, expected to have
    fixed concurrency, OLAP, ETL;
    example: processing billions of historical sales records to generate useful statistics

Declaring a workload type provides more context for Scylla to decide how to handle the sessions.
For instance, if a coordinator node receives requests with a rate higher than it can handle,
it will make different decisions depending on the declared workload type:
 - for batch workloads it makes sense to apply backpressure - the concurrency is assumed to be fixed,
   so delaying a reply will likely also reduce the rate at which new requests are sent;
 - for interactive workloads, backpressure would only waste resources - delaying a reply does not
   decrease the rate of incoming requests, so it's reasonable for the coordinator to start shedding
   surplus requests.

If multiple workload types are applicable for a role, it makes sense if:
 - all the applicable workload types are identical
 - some of the service levels do not have any workload types specified

Otherwise, e.g. if a role has multiple workload types declared,
the conflicts are resolved as follows:
 - `X` vs `NULL` -> `X`
 - `batch` vs `interactive` -> `batch` - under the assumption that `batch` is safer, because it would not trigger load shedding as eagerly as `interactive`

 So for example to create a service level that is twice more important than the default service
 level (which has shares of 1000) one can run:

 ```
 INSERT INTO system_distributed.service_level (service_level, shares) VALUES ('double_importance',2000);
 ```

## Service levels REST API

In a current state, Service Levels/Workload Prioritization has its own flaws, one of which is a requirement to restart connections to apply changes of users' service levels change.

Until we improve service levels controller to make the changes automatically, here is a REST API to ease to work of maintaining and managing service levels and connections.

A `tenant` (used below) is equal to scheduling group under which a connection is working.

### Switch tenants

`/service_levels/switch_tenants` endpoint triggers a tenant switch on all opened CQL connections on a single node without any interruption or their restart.
The response is returned immediately but the actual work might take up to tens of seconds.

### Inspecting current scheduling group of connections

`/service_levels/count_connections` endpoint is a tool to inspect status of all opened CQL connections. It returns a map with connections count per scheduling group, per user:
```
{'sl:default': {'cassandra': 3}, 'sl:sl1': {'test_user': 3}}
```

In fact, this endpoint is a wrapper which executes simple query on `system.clients` table and aggregates the result. The table has added `scheduling_group` column, so to inspect a particular connection, it can be directly looked up in `system.clients` table.

### Effective service level

Actual values of service level's options may come from different service levels, not only from the one user is assigned with. This can be achieved by assigning one role to another.

For instance:
There are 2 roles: role1 and role2. Role1 is assigned with sl1 (timeout = 2s, workload_type = interactive) and role2 is assigned with sl2 (timeout = 10s, workload_type = batch).
Then, if we grant role1 to role2, the user with role2 will have 2s timeout (from sl1) and batch workload type (from sl2).

To see detail how the options are merged, check [combining service levels section](#combining-service-level-timeouts-from-multiple-roles).

To facilitate insight into which values come from which service level, there is `LIST EFFECTIVE SERVICE LEVEL OF <role_name>` command.

The command displays a table with: option name, effective service level the value comes from and the option value.

```
> LIST EFFECTIVE SERVICE LEVEL OF role2;

 service_level_option | effective_service_level | value
----------------------+-------------------------+-------------
        workload_type |                     sl2 |       batch
              timeout |                     sl1 |          2s
```
