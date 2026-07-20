====================================================
Increase Permission Cache to Avoid Non-paged Queries
====================================================

**Topic: Mitigate non-paged queries coming from connection authentications**

**Audience: ScyllaDB administrators**



Issue
-----

If you create lots of roles and give them lots of permissions your nodes might spike with non-paged queries.

Root Cause
----------

``permissions_cache_max_entries`` is set to 1000 by default. This setting may not be high enough for bigger deployments with lots of tables, users, and roles with permissions.


Solution
--------

Open the scylla.yaml configuration for editing and adjust the following parameters:
``permissions_cache_max_entries`` - increase this value to suit your needs. See the example below.
``permissions_update_interval_in_ms``
``permissions_validity_in_ms``

Note:: ``permissions_update_interval_in_ms`` and  ``permissions_validity_in_ms`` can be set to also make the authentication records come from cache instead of lookups, which generate non-paged queries


Example
-------

Considering with ``permissions_cache_max_entries`` there is no maximum value, it's just limited by your memory.
The cache consumes memory as it caches all records from the list of users and their associated roles (similar to a cartesian product).

Every user, role, and permissions(7 types) on a per table basis are cached.

If for example, you have 1 user with 1 role and 1 table, the table will have 7 permission types and 7 entries  1 * 1 * 1 * 7 = 7.
When expanded to 5 users, 5 roles, and 10 tables this will be 5 * 5 * 10 * 7 = 1750 entries, which is above the default cache value of 1000. The entries that go over the max value (750 entries) will be non-paged queries for every new connection from the client (and clients tend to reconnect often).
In cases like this, you may want to consider trading your memory for not stressing the entire cluster with ``auth`` queries.