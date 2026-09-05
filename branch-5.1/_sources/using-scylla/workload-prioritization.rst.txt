========================
Workload Prioritization
========================



.. include:: /rst_include/enterprise-only-note.rst

In a typical database there are numerous workloads running at the same time.
Each workload type dictates a different acceptable level of latency and throughput.
For example, consider the following two workloads:

* OLTP ( Online Transaction Processing) - backend database for your application

  - High volume of requests 
  - Fast processing 
  - In essence - Latency sensitive

* OLAP (Online Analytical Processing ) - performs data analytics in the background

  - High volume of data
  - Slow queries 
  - In essence - Latency agnostic

Using Service Level CQL commands, database administrators (working on Scylla Enterprise) can set different workload prioritization levels (levels of service) for each workload without sacrificing latency or throughput.
By assigning each service level to the different roles within your organization, DBAs ensure that each role_ receives the level of service the role requires.

.. _`role` : /operating-scylla/security/rbac_usecase/

Prerequisites
=============
To create a level of service and assign it to a role, you need:

* An :doc:`authenticated </operating-scylla/security/runtime-authentication>` and :doc:`authorized </operating-scylla/security/enable-authorization>` user 
* At least one :ref:`role created <create-role-statement>`.

Work by Example
---------------

To follow the examples in this document, create the roles `spark` and `web`. You can assign permissions to these roles later, if needed.

**Procedure**

Run the following:

.. code-block:: cql

   CREATE ROLE Spark;
   CREATE ROLE Web;

Workload Prioritization Workflow
================================

1. `Create a Service Level`_
2. `Assign a Service Level to a Role`_

.. _workload-priorization-service-level-management:

Service Level Management
========================

These commands set, list, and edit the level of service. 

Create a Service Level
----------------------

When you create a service level, you allocate a percentage of resources to the service level. Remember to use the correct naming convention to name your service level. If you decide to use :doc:`Reserved Keywords </cql/reserved-keywords>`, enclose them in either single or double quotes (for example ``'primary'``).

**Syntax**

.. code-block:: none

   CREATE SERVICE_LEVEL [IF NOT EXISTS] <service_level_name> [WITH SHARES = <shares_number>];

Where:

* ``service_level_name`` - Specifies the name of the service you're creating. This can be any string without spaces. The name should be meaningful, such as class names (silver, gold, diamond, platinum), or categories (OLAP or OLTP), etc.
* ``shares_number`` - The number of shares of the resources you're granting to the service level name.  You can use any number within the range from 1 to 1000. **Default : 1000**

Example
.......

There are 3 service levels (OLAP, OLTP, Default) where: (the percentage of resources = (Assigned Shares / Total Shares) x 100). Total Shares in this case is the total of all allocated shares + the Default SLA (1000). The percentage of resources would be:

.. list-table::
   :widths: 30 30 30 
   :header-rows: 1

   * - Service Level Name
     - Shares
     - Percentage of Resources 
   * - OLAP
     - 100
     - 4%
   * - OLTP
     - 1000
     - 48%
   * - Default
     - 1000
     - 48%
   * - Total 
     - 2100
     - 100%

**Procedure**

1. To create these service levels, run the following CQL commands:

.. code-block:: cql

   CREATE SERVICE_LEVEL IF NOT EXISTS OLAP WITH SHARES = 100;
   CREATE SERVICE_LEVEL IF NOT EXISTS OLTP WITH SHARES = 1000;

2. Confirm the service level change reflects the new service level allocations:

.. code-block:: cql

   LIST ALL SERVICE_LEVELS;

   service_level | shares
   --------------+-------
            olap |    100
   --------------+-------
            oltp |   1000
   (2 rows)

Change Resource Allocation for a Service Level 
-----------------------------------------------

You can change resource allocation for a given service level. If you don't specify the number the shares, the default setting (1000) is used.

**Syntax**

.. code-block:: none

   ALTER SERVICE_LEVEL <service_level_name> 
        WITH SHARES = <shares_number>;  


Where: 

* ``service_level_name``  - Specifies the name of the service level you created. See `Create a Service Level`_. 
* ``shares_number`` - The number of shares in the CPU that you're granting to the service level name.   You can use any number within the range from 1 to 1000. **Default : 1000**

Example
........

Analysts are complaining that they don't have enough resources. To increase the resources, you change the service level attributes for the OLAP service level.

**Procedure**

1. Run the following:

.. code-block:: cql

   ALTER SERVICE_LEVEL OLAP WITH SHARES = 500; 

2. Confirm the service level change reflects the new service level allocation:

.. code-block:: cql

   LIST SERVICE_LEVEL OLAP; 

   service_level | shares
   --------------+-------
            olap |    500
   (1 rows)

3. To change it back to the original setting (or to remain consistent for the examples that follow) change the shares amount back to the original.

.. code-block:: cql

   ALTER SERVICE_LEVEL OLAP WITH SHARES = 100; 

Display Specified Service Level Parameters
------------------------------------------

Lists the specified service level with its class parameters. If the service level is attached to a role it does not appear in this list. 

**Syntax**

.. code-block:: none

   LIST SERVICE_LEVEL <service_level_name>; 

Where: 

* ``service_level_name`` - Specifies the name of the service level you created. See `Create a Service Level`_.

Example
.......

In this example you list the service level parameters for OLTP.

**Procedure**

Run the following:

.. code-block:: cql

   LIST SERVICE_LEVEL OLTP; 

   service_level | shares
   --------------+-------
            oltp |   1000
   (1 rows)

Display All Service Levels and Parameters
-----------------------------------------

Lists all service levels with their class parameters. This list contains all service levels including those which are assigned to roles. 

**Syntax**

.. code-block:: none

   LIST ALL SERVICE_LEVELS;

Example
.......

In this example, you list all service levels and their parameters.

**Procedure**

Run the following:

.. code-block:: cql

   LIST ALL SERVICE_LEVELS; 

   service_level  | shares
   ---------------+--------
             olap |     100
             oltp |    1000
   (2 rows)


Delete a Service Level
----------------------

Permanently removes the service level. Any role attached to this service level is automatically assigned to the Default SLA if there is no other service level attached to the role.

**Syntax**

.. code-block:: none

   DROP SERVICE_LEVEL IF EXISTS <service_level_name>;

Where:

* ``service_level_name`` - Specifies the name of the service level you created. See `Create a Service Level`_.
* ``IF EXISTS`` - If the service level does not exist and IF EXISTS is not used an error is returned.


Example
.......

In this example you drop the OLTP service level.

**Procedure**

Run the following:

.. code-block:: cql

   DROP SERVICE_LEVEL IF EXISTS OLTP;

Manage Roles with Service Levels
================================

Once you have created roles and service levels you can attach and remove the service levels from the roles and list which roles are attached to which service levels. 

Assign a Service Level to a Role
--------------------------------

If you have created a role and a service level, you can attach the service level to the role. 

.. note:: A role can only be assigned **one** service level. However, the same service level can be attached to many roles. If a role inherits a service level from another role, the highest level of service from all the roles wins. 

**Syntax**

.. code-block:: none

   ATTACH SERVICE_LEVEL <service_level_name> TO <role_name>;

Where:

* ``service_level_name`` - Specifies the name of the service level you created. See `Create a Service Level`_.
* ``role_name`` - Specifies the role that you want to use the service level on. This is the role you created with :ref:`create role <create-role-statement>`. 

.. note:: Any role which does not have an SLA attached to it, receives the default SLA.

Example
.......

Continuing from the example in `Create a Service Level`_, you can attach the service levels that you created to different roles in your organization as follows:

.. list-table::
   :widths: 50 50 
   :header-rows: 1

   * - Service Level Name
     - Role Name
   * - OLAP
     - Spark
   * - OLTP
     - Web


**Procedure**

To assign these service levels to the roles, run the following CQL commands:

.. code-block:: cql

   ATTACH SERVICE_LEVEL OLAP TO Spark;
   ATTACH SERVICE_LEVEL OLTP TO Web;

List All Attached Service Levels for All Roles
----------------------------------------------

Lists all directly attached service levels for all roles. This does not include any service level which the role inherits from other roles.

**Syntax**

.. code-block:: none

   LIST ALL ATTACHED SERVICE_LEVELS; 

Example
.......

In this example you list all service levels attached to any role.

**Procedure**

Run the following:

.. code-block:: cql

   LIST ALL ATTACHED SERVICE_LEVELS; 

   role   | service_level
   -------+---------------
   spark  |          olap     
   -------+---------------
     web  |          oltp      

   (2 rows)

List the Roles Assigned to a Specific Service Level
----------------------------------------------------

Lists all roles directly attached to a service level. This does not include any service level which the role inherits from other roles. 

**Syntax**

.. code-block:: none

   LIST ATTACHED SERVICE_LEVEL OF <role_name>; 

Where:

* ``role_name`` - Specifies the role that you want to use the service level on. This is the role you created with :ref:`create role <create-role-statement>`.

Example
.......

In this example, you list all of Roles which are assigned to the OLAP Service Level.

**Procedure**

Run the following:

.. code-block:: cql

   LIST ATTACHED SERVICE_LEVEL OF Spark; 

   role   | service_level
   -------+---------------
   spark  |  olap     

   (1 rows) 

Remove a Service Level from a Role
----------------------------------

Removes a service level from a specified role.  Once the service level is removed from a role, if there are other service levels attached to roles which that role inherits, the service level in the hierarchy with the most amount of shares wins.

**Syntax**

.. code-block:: none

   DETACH SERVICE_LEVEL FROM <role_name>;

Where: 

* ``role_name`` - Specifies the role that you want to use the service level on. This is the role you created with :ref:`create role <create-role-statement>`.

Example
.......

In this example, you re-assign the Spark to a different level of service by detaching it from one level of service and attaching it to another.

**Procedure**

Run the following:

.. code-block:: cql

   DETACH SERVICE_LEVEL FROM Spark;

At this point, the Spark role receives the Default SLA, until it is assigned another service level. You assign a new service level to this role using `Assign a Service Level to a Role`_.

Using Workload Prioritization with your Application
===================================================

In order for workload prioritization to take effect, application users need to be assigned to a relevant role. In addition, each role you create needs to be assigned to a specific Service Level. Any user that signs into the application without a role is automatically assigned the `Default` service level.  This is always be the case with users who sign in anonymously.


Limits
======
Scylla Enterprise is limited to 8 service levels, including the default one; this means you can create up to 7 service levels.


Additional References
=====================

`OLAP or OLTP? Why Not Both? <https://www.youtube.com/watch?v=GhmgwN6ZraI>`_ Session by Glauber Costa from Scylla Summit 2018

`Scylla University: Workload Prioritization lesson <https://university.scylladb.com/courses/scylla-operations/lessons/workload-prioritization/>`_ - The lesson covers: 

* The evolving requirements for operational (OLTP) and analytics (OLAP) workloads in the modern datacenter
* How Scylla provides built-in control over workload priority and makes it easy for administrators to configure workload priorities
* The impact of minimizing integrations and maintenance tasks, while also shrinking the datacenter footprint and maximizing utilization
* Test results of how it performs in real-world settings





