=====================================
LDAP Authorization (Role Management)
=====================================

:label-tip:`ScyllaDB Enterprise`

.. versionadded:: 2021.1.2

Scylla Enterprise customers can manage and authorize users’ privileges via an :abbr:`LDAP (Lightweight Directory Access Protocol)` server.
LDAP is an open, vendor-neutral, industry-standard protocol for accessing and maintaining distributed user access control over a standard IP network.
If your users are already stored in an LDAP directory, you can now use the same LDAP server to regulate their roles in Scylla.


Introduction
------------

Scylla can use LDAP to manage which roles a user has. This behavior is triggered by setting the ``role_manager`` entry in scylla.yaml to *com.scylladb.auth.LDAPRoleManager*.
When this role manager is chosen, Scylla forbids ``GRANT`` and ``REVOKE`` role statements (CQL commands) as all users get their roles from the contents in the LDAP directory.

.. _note:

.. note:: Scylla still allows ``GRANT`` and ``REVOKE`` permission statements, such as ``GRANT permission ON resource TO role``, which are handled by the authorizer, not role manager.
   This allows permissions to be granted to and revoked from LDAP-managed roles. In addition, if you have nested Scylla roles, LDAP authorization does not allow them. A role cannot be a member of another role.
   In LDAP only login users can be members of a role.

When LDAP Authorization is enabled and a Scylla user authenticates to Scylla, a query is sent to the LDAP server, whose response sets the user's roles for that login session.
The user keeps the granted roles until logout; any subsequent changes to the LDAP directory are only effective at the user's next login to Scylla.

The precise form of the LDAP query is configured by Scylla administrator in the scylla.yaml configuration file.
This configuration takes the form of a query template which is defined in the scylla.yaml configuration file using the parameter ``ldap_url_template``.
The value of ``ldap_url_template`` parameter should contain a valid LDAP URL (e.g., as returned by the ldapurl utility from OpenLDAP) representing an LDAP query that returns entries for all the user's roles.
Scylla will replace the text ``{USER}`` in the URL with the user's Scylla username before querying LDAP.

Workflow
--------

**Before you begin**
On your LDAP server, create LDAP directory entries for Scylla users and roles.

**Workflow**

#. :ref:`Create a Query Template <example-template>`
#. Ensure Scylla has the same users and roles as listed in the LDAP directory.
#. :ref:`Enable LDAP as the role manager in Scylla <role-ldap>`
#. Make Scylla reload the configuration (SIGHUP or restart)

.. _example-template:

Example: Query Template
=======================

Use this example to create a query that will retrieve from your LDAP server the information you need to create a template.
For example, this template URL will query LDAP server at ``localhost:5000`` for all entries under ``base_dn`` that list the user's username as one of their ``uniqueMember`` attribute values:

.. code-block:: none

   ldap://localhost:5000/base_dn?cn?sub?(uniqueMember={USER})

After Scylla queries LDAP and obtains the resulting entries, it looks for a particular attribute in each entry and uses that attribute's value as a Scylla role this user will have.
The name of this attribute can be configured in scylla.yaml by setting the ``ldap_attr_role`` parameter there.

When the LDAP query returns multiple entries, multiple roles will be granted to the user.
Each role must already exist in Scylla, created via the :ref:`CREATE ROLE <create-role-statement>` CQL command beforehand.

For example, if the LDAP query returns the following results:

.. code-block:: none

   # extended LDIF
   #
   # LDAPv3

   # role1, example.com
   dn: cn=role1,dc=example,dc=com
   objectClass: groupOfUniqueNames
   cn: role1
   scyllaName: sn1
   uniqueMember: uid=jsmith,ou=People,dc=example,dc=com
   uniqueMember: uid=cassandra,ou=People,dc=example,dc=com

   # role2, example.com
   dn: cn=role2,dc=example,dc=com
   objectClass: groupOfUniqueNames
   cn: role2
   scyllaName: sn2
   uniqueMember: uid=cassandra,ou=People,dc=example,dc=com

   # role3, example.com
   dn: cn=role3,dc=example,dc=com
   objectClass: groupOfUniqueNames
   cn: role3
   uniqueMember: uid=jdoe,ou=People,dc=example,dc=com

If ``ldap_attr_role`` is set to *cn*, then the resulting role set will be { role1, role2, role3 } (assuming, of course, that these roles already exist in Scylla).
However, if ``ldap_attr_role`` is set to *scyllaName*, then the resulting role set will be { sn1, sn2 }.
If an LDAP entry does not have the ``ldap_attr_role`` attribute, it is simply ignored.
Before Scylla attempts to query the LDAP server, it first performs an LDAP bind operation, to gain access to the directory information.
Scylla executes a simple bind with credentials configured in scylla.yaml.
The parameters ``ldap_bind_dn`` and ``ldap_bind_passwd`` must contain, respectively, the distinguished name and password that Scylla uses to perform the simple bind.

.. _role-ldap:

Enable LDAP Authorization
-------------------------

Enables Scylla to use LDAP Authorization. LDAP will manage the roles, not Scylla. See :ref:`Note <note>` above

#. Open the scylla.yaml file in an editor. The file is located in /etc/scylla/scylla.yaml by default.
#. Edit the ``role_manager`` section. Change the entry to ``com.scylladb.auth.LDAPRoleManager``. If this section does not exist, add it to the file.
   Configure the parameters according to your organization's IT and Security Policy.

   .. code-block:: yaml

      role_manager: "com.scylladb.auth.LDAPRoleManager"
      ldap_url_template: "ldap://localhost:123/dc=example,dc=com?cn?sub?(uniqueMember=uid={USER},ou=People,dc=example,dc=com)"
      ldap_attr_role: "cn"
      ldap_bind_dn: "cn=root,dc=example,dc=com"
      ldap_bind_passwd: "secret"

#. Restart the scylla-server service or kill the scylla process.
  
   .. include:: /rst_include/scylla-commands-restart-index.rst

Disable LDAP Authorization
--------------------------

#. Open the scylla.yaml file in an editor. The file is located in /etc/scylla/scylla.yaml by default.
#. Comment out or delete the role_manager section.
#. Restart the scylla-server service or kill the scylla process. 
  
   .. include:: /rst_include/scylla-commands-restart-index.rst


Troubleshooting
---------------

Before configuring Scylla, it is a good idea to validate the query template by manually ensuring that the LDAP server returns the correct entries when queried.
This can be accomplished by using an LDAP search tool such as `ldapsearch <https://www.openldap.org/software/man.cgi?query=ldapsearch&apropos=0&sektion=0&manpath=OpenLDAP+2.0-Release&format=html>`_.

If manual querying does not yield correct results, then Scylla cannot see correct results, either.
Try to adjust ldapsearch parameters until it returns the correct role entries for **one** user.

Once that works as expected, you can use the `ldapurl <https://linux.die.net/man/1/ldapurl>`_ utility to transform the parameters into a URL providing a basis for the ldap_url_template.

.. tip:: Always provide an explicit ``-s`` flag to both ``ldapsearch`` and ``ldapurl``; the default ``-s`` value differs among the two tools.

Remember to replace the specific user name with ``{USER}`` in the URL template.
You can turn on debug logging in the LDAP role manager by passing the following argument to the Scylla executable: ``--logger-log-level ldap_role_manager=debug``.
This will make Scylla log useful additional details about the LDAP responses it receives.

If ldapsearch yields expected results but Scylla queries do not, first check the host and port parts of the URL template and make sure both ldapsearch and
Scylla are actually querying the same LDAP server.
Then check the LDAP logs and see if there are any subtle differences between the logged queries of ldapsearch and Scylla.
