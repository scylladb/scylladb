# Per-role parameters

Scylla allows configuring per-role parameters. The current list of parameters includes:
 * `read_timeout` - custom timeout for read operations
 * `write_timeout` - custom timeout for write operations

## Examples

In order to set up per-role parameters, one should use the already existing CQL API for roles:
```cql
CREATE ROLE example WITH options = {'read_timeout': 50ms}
```
or
```cql
ALTER ROLE example WITH options = {'read_timeout': 1s, 'write_timeout': 500ms}
```

Once a session with given role is established, it will use per-role timeouts instead of globally configured
timeouts, if there are any.

Role options can be viewed with the standard `LIST ROLES` statement:
```cql
LIST ROLES;

 role      | super | login | options
-----------+-------+-------+-----------------------------------------------------
 cassandra |  True |  True | {'read_timeout': '50ms', 'write_timeout': '1000us'}

```

