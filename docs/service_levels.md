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

This table is currently a stub and does not hold any parameters yet.
 ```
