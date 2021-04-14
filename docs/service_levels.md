## Service Level Distributed Data

There are two system tables that are used to facilitate the service level feature.


### Service Level Attachment Table

```CREATE TABLE system_auth.role_attributes (
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

```SELECT * FROM  system_auth.role_attributes WHERE role='r' and attribute_name='service_level'
```

### Service Level Configuration Table

```CREATE TABLE system_distributed.service_levels (
    service_level text PRIMARY KEY);
```

The table is used to store and distribute the service levels configuration.
The table column names meanings are:
*service_level* - the name of the service level.
*timeout* - timeout for operations performed by users under this service level

```
select * from system_distributed.service_levels ;

 service_level | timeout 
---------------+---------
            sl |    50ms

```

### Service Level Timeout

Service level timeout can be used to assign a default timeout value for all operations for a particular service level.

Service level timeout takes precenence over default timeout values from scylla.yaml configuration
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

