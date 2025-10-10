Configure SaslauthdAuthenticator
--------------------------------

Scylla can outsource authentication to a third-party utility named `saslauthd <https://linux.die.net/man/8/saslauthd>`_, which, in turn,supports many different authentication mechanisms.
Scylla accomplishes this by providing a custom authenticator named SaslauthdAuthenticator.
This procedure explains how to install and configure it.
Once configured, any login to Scylla is authenticated with the SaslauthdAuthenticator.

**Procedure**

#. Install saslauthd.  The easiest way is via a Linux package, if your package manager supports it.
   Choose a package according to your distro.

   .. tabs::

      .. group-tab:: rpm-based distros

         Use the ``cyrus-sasl`` package

      .. group-tab:: deb-based distros

         Use the ``sasl2-bin`` package


#. Enable the saslauthd service. Run:

   .. code-block:: shell

      systemctl enable saslauthd.service

#. Configure saslauthd: choose the authentication mechanism (e.g., LDAP or PAM) and set the appropriate mechanism-specific parameters by following the `saslauthd documentation <https://linux.die.net/man/8/saslauthd>`_.

#. After every configuration change, restart the saslauthd service.

   .. code-block:: shell

      systemctl restart saslauthd.service

#. Test your configuration using the `testsaslauthd <https://linux.die.net/man/8/testsaslauthd>`_ command. Verify you see a success message.
   If not, verify that the user name and password are correct and then look at the saslauthd logs ( run ``dmesg -H``, and look for ``LOG_AUTH``) to diagnose problems.

#. Find the mux file (saslauthd’s Unix domain socket) and note its full path.

   .. tabs::

      .. group-tab:: rpm-based distros

         Usually, it is /run/saslauthd/mux

      .. group-tab:: deb-based distros

         Usually, it is /var/run/sasl2/mux

#. Once saslauthd is correctly configured and running, you modify the scylla.yaml configuration file, so communication can begin. Modify the following entries:

   * ``authenticator: com.scylladb.auth.SaslauthdAuthenticator``
   * ``saslauthd_socket_path: /path/to/the/mux``

#. Restart the Scylla server.  From now on, Scylla will authenticate all login attempts via saslauthd.

   .. include:: /rst_include/scylla-commands-restart-index.rst

#. Create Scylla roles which **match** the same roles in the LDAP server. To create a role, refer to the :ref:`CQL Reference <cql-security>` and the :doc:`RBAC example <rbac-usecase>`.
