==============
Service Levels
==============

Service Levels CQL commands
===========================

``LIST EFFECTIVE SERVICE LEVEL OF <role>``
-----------------------------------------------------------

Actual values of service level's options may come from different service levels, not only from the one user is assigned with. This can be achieved by assigning one role to another.

For instance:
There are 2 roles: role1 and role2. Role1 is assigned with sl1 (timeout = 2s, workload_type = interactive) and role2 is assigned with sl2 (timeout = 10s, workload_type = batch).
Then, if we grant role1 to role2, the user with role2 will have 2s timeout (from sl1 because merging rule says to take lower timeout) and batch workload type (from sl2).

To facilitate insight into which values come from which service level, there is ``LIST EFFECTIVE SERVICE LEVEL OF <role_name>`` command.

The command displays a table with: option name, effective service level the value comes from and the option value.

.. code-block:: cql

    > LIST EFFECTIVE SERVICE LEVEL OF role2;

     service_level_option | effective_service_level | value
    ----------------------+-------------------------+-------------
            workload_type |                     sl2 |       batch
                  timeout |                     sl1 |          2s
    