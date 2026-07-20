.. _rbacc-example:

================================
Role Based Access Control (RBAC)
================================


Role Based Access Control (RBAC) is a method of reducing lists of authorized users to a few roles assigned to multiple users. RBAC is sometimes referred to as role-based security. 

Roles vs Users
--------------
Roles supersede users and generalize them. In addition to doing with `roles` everything that you could previously do with `users` in older versions of ScyllaDB, roles can be granted to other roles. If a role `developer` is granted to a role `manager`, then all permissions of the `developer` are granted to the `manager`.

In order to distinguish roles which correspond uniquely to an individual person and roles which are representative of a group, any role that can login is a user. Within that framework, you can conclude that all users are roles, but not all roles are users.

For example, there is an organization with a role-based hierarchy. The organization has roles such as `Guest`, who is not a member of the organization, has the least amount of privileges.
The `DB Administrator` role has the most. `Engineer` and `QA` roles have similar privileges but do not have permission to modify each other's keyspace.
Creating a structure like this is quite useful when you have permissions granted to representative roles instead of individual users.
Using RBAC allows you to add and remove permissions with ease without affecting other users. Suppose there is a new engineer who joined the organization.
This is not a problem! All you would do is create a user for that engineer with the engineer role. Once the role is assigned to the user, the user inherits all of the permissions for that role.
In the same manner, should someone leave the organization, all you would have to do is assign that user to a non-employee role (`Guest`, for example).
Should someone change positions at the company, just assign the new employee to the new role and revoke roles no longer required for the new position.
   
To build an RBAC environment, you need to create the roles and their associated permissions and then assign or grant the roles to the individual users. Roles inherit the permissions of any other roles that they are granted. The hierarchy of roles can be either simple or extremely complex. This gives great flexibility to database administrators, where they can  create specific permission conditions without incurring a huge administrative burden.
In addition to standard roles, `ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/>`_ users can implement `Workload Prioritization <https://enterprise.docs.scylladb.com/stable/using-scylla/workload-prioritization.html>`_, which allows you to attach roles to Service Levels, thus granting resources to roles as the role demands.

.. _rbac-usecase-grant-roles-and-permissions:

Granting roles and permissions
------------------------------
When creating a role, you grant it permissions and resources. The permission is what the role is permitted to do, and the resource is the scope over which the permission is granted. The format of the permission granting is:

``GRANT (permission | "ALL PERMISSIONS") ON resource TO role where:``

* Where permission is CREATE, DESCRIBE, etc.
* A resource is one of
   * "<ks>.<tab>"
   * "KEYSPACE <ks>"
   * "ALL KEYSPACES"
   * "ROLE <role>"
   * "ALL ROLES"
   * Note that An unqualified table name  assumes the current keyspace

.. _rbac-usecase-use-case:

Use case
--------

This is a use case that is given as an example. You should modify the commands to your organization’s requirements.
A health club has opened, and they have an application that supports their clients, which is using ScyllaDB as the database backend. The following groups would need to be given permissions:
The office staff can add new customers and can cancel subscriptions, view all customer data, and can change classes for the trainers as well as view the trainers’ data.  
Trainers can only view their schedule and can view customer data. 
Customers view the class schedule. 
There is also a database administrator who manages the database. 

In this case, four roles would be created: staff, customer, trainer, and administrator. Permissions could be any of the :ref:`following <data-control>`.
It may be helpful to make a table as follows, listing the roles and the tables or keyspaces you are granting permission on.
In the table cell, list the permission to be granted, leaving a blank space for no permission.

============   ===============   ==============   ===============   ===================   ==================
Role/on        customer.info     schedule.cust    schedule.train    customer keyspace     schedule keyspace 
============   ===============   ==============   ===============   ===================   ==================
DBA            superuser         superuser        superuser         superuser             superuser          
------------   ---------------   --------------   ---------------   -------------------   ------------------
staff          MODIFY            MODIFY           MODIFY            SELECT                SELECT            
------------   ---------------   --------------   ---------------   -------------------   ------------------
trainer        SELECT            SELECT           SELECT         
------------   ---------------   --------------   ---------------   -------------------   ------------------
customer                         SELECT       
============   ===============   ==============   ===============   ===================   ==================

**Before you begin** 

You need to login to cqlsh as a user with authentication and authorization. You **must** enable :doc:`authentication </operating-scylla/security/authentication>` **and** :doc:`authorization </operating-scylla/security/enable-authorization>` if you have not already done so. In addition, the user you create for yourself **must** have login privileges, a user name, and a password. 

.. include:: /operating-scylla/security/_common/warning-no-pwd.rst 

If you proceed with the following example without authentication enabled, or login without using an authenticated user, you will see this error: ``Unauthorized: Error from server: code=2100 [Unauthorized] message="You have to be logged in and not anonymous to perform this request"`` 

If you proceed with the following example without authorization, you will see this error: ``InvalidRequest: Error from server: code=2200 [Invalid query] message="GRANT operation is not supported by AllowAllAuthorizer"`` 



**Procedure**

1. Create all keyspaces and tables needed.

.. code-block:: cql

   CREATE KEYSPACE IF NOT EXISTS customer WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };
   CREATE TABLE IF NOT EXISTS customer.info (ssid UUID, name text, DOB text, telephone text, email text, memberid text, PRIMARY KEY (ssid,  name, memberid));
   CREATE KEYSPACE IF NOT EXISTS schedule WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };
   CREATE TABLE IF NOT EXISTS schedule.cust (memberid UUID, ssid text, class text, meeting_day text, meeting_time text, PRIMARY KEY (memberid, ssid));
   CREATE TABLE IF NOT EXISTS schedule.train (trainerid UUID, class text, meeting_day text, meeting_time text, PRIMARY KEY (trainerid));

2. Create the Customer role. It is best to start your hierarchy from the bottom. Creating the Customer role first allows you to grant it to the Trainer, which you will do in a later step.

.. code-block:: cql

   CREATE ROLE customer;

3. Set the permission settings for customer. According to our list above, the customer role would be granted permissions by running the following commands:

.. code-block:: cql
   
   GRANT SELECT ON schedule.cust TO customer;


4. Create the trainer role.

.. code-block:: cql

   CREATE ROLE trainer;


5. Assign the Customer role to the Trainer role. In this way, the Trainer role inherits the Customer role’s permissions.
   
.. code-block:: cql

   GRANT customer TO trainer;


6. With the trainer role created and granted the basic customer permission settings, give the trainer the additional permissions that the role requires.

.. code-block:: cql

   GRANT SELECT ON customer.info TO trainer;
   GRANT SELECT ON schedule.train TO trainer;

7. Create the office staff role.

.. code-block:: cql
   
   CREATE ROLE staff;

8. Assign the staff the additional permissions that the role requires. As staff will have more scope, there is no need to look at the individual tables. It is easier to grant permission on the entire keyspace.

.. code-block:: cql

   GRANT SELECT ON KEYSPACE schedule TO staff;
   GRANT SELECT ON KEYSPACE customer TO staff;
   GRANT MODIFY ON schedule.cust TO staff;
   GRANT MODIFY ON customer.info TO staff;
   GRANT MODIFY ON schedule.train TO staff;


9. Now create the database administrator role.

.. code-block:: cql

   CREATE ROLE administrator WITH SUPERUSER = true;


.. note:: This role already has complete read and write permissions on all tables and keyspaces and does not need to be granted anything else. The superuser permission setting is by default, disabled. Only for the administrator does it need to be enabled.

10. Create users and assign the roles to them. This is done in the same fashion as the role, but the password and login information is added. In this example, Lisa is a customer, Mary is the trainer, and Dennis is the office staff. Tim is the Admin.
 
.. code-block:: cql

   CREATE ROLE lisa WITH PASSWORD = 'password' AND LOGIN = true;
   CREATE ROLE mary WITH PASSWORD = 'password' AND LOGIN = true;
   CREATE ROLE dennis WITH PASSWORD = 'password' AND LOGIN = true;
   CREATE ROLE tim WITH PASSWORD = 'password' AND LOGIN = true;

 
11. Assign the roles to the users.

.. code-block:: cql

   GRANT administrator TO tim;
   GRANT staff TO dennis;
   GRANT trainer TO mary;
   GRANT customer TO lisa;

12. Check that each user has the privileges they should have.
For example, if we list Mary’s permissions, we'll see they represent what we granted to her role. Remember that the trainer role inherited the customer role.

.. code-block:: none
   
   LIST ALL PERMISSIONS OF mary;
 
   ╭─────────┬──────────┬────────────────────────┬────────────╮        
   │role     │ username │ resource               │ permission │     
   ├─────────┼──────────┼────────────────────────┼────────────┤
   │customer │ customer │ <table schedule.cust>  │ SELECT     │
   ├─────────┼──────────┼────────────────────────┼────────────┤ 
   │trainer  │ trainer  │ <table customer.info>  │ SELECT     │
   ├─────────┼──────────┼────────────────────────┼────────────┤
   │trainer  │ trainer  │ <table schedule.train> │ SELECT     │
   ╰─────────┴──────────┴────────────────────────┴────────────╯
 

Likewise, we can ask which roles Mary has been assigned.

.. code-block:: none
   
   LIST ROLES OF mary;
   ╭─────────┬─────────┬──────────────────────┬────────────╮        
   │role     │ super   │ login                │ options    │     
   ├─────────┼─────────┼──────────────────────┼────────────┤
   │customer │ False   │ False                │ {}         │
   ├─────────┼─────────┼──────────────────────┼────────────┤
   │trainer  │ False   │ False                │ {}         │
   ├─────────┼─────────┼──────────────────────┼────────────┤
   │mary     │ False   │ True                 │ {}         │
   ╰─────────┴─────────┴──────────────────────┴────────────╯ 




Additional References
---------------------

* :doc:`Authorization</operating-scylla/security/authorization/>`
* :doc:`CQLSh the CQL shell</cql/cqlsh>`
* `Workload Prioritization <https://enterprise.docs.scylladb.com/stable/using-scylla/workload-prioritization.html>`_ - to attach a service level to a role. Only available in `ScyllaDB Enterprise <https://enterprise.docs.scylladb.com/>`_.
