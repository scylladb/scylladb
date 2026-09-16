===================
LDAP Authentication
===================

.. toctree::
   :hidden:

   saslauthd

.. include:: /rst_include/enterprise-only-note.rst

.. versionadded:: Scylla Enterprise 2021.1.2

Scylla supports user authentication via an LDAP server by leveraging the SaslauthdAuthenticator.
By configuring saslauthd correctly against your LDAP server, you enable Scylla to check the user’s credentials through it.


Configure saslauthd for LDAP
----------------------------
**Before You Begin**

This procedure requires you to install and configure saslauthd.
The general instructions are :doc:`here </operating-scylla/security/saslauthd>`.

#. Follow all of the steps in :doc:`this procedure </operating-scylla/security/saslauthd>` and use the code snippets below to list LDAP as the authentication mechanism.

#. You must list LDAP as saslauthd’s authentication mechanism:

   .. tabs::

      .. group-tab:: rpm-based distros

         Edit ``/etc/sysconfig/saslauthd`` and add:

         .. code-block:: none

            MECH=ldap

      .. group-tab:: deb-based distros

         Edit ``/etc/default/saslauthd`` and add:

         .. code-block:: none

            MECHANISMS=ldap


#. You also have to edit the /etc/saslauthd.conf file to provide adequate `parameter <https://github.com/cyrusimap/cyrus-sasl/blob/cyrus-sasl-2.1.27/saslauthd/LDAP_SASLAUTHD#L74>`_ values for your LDAP server.




