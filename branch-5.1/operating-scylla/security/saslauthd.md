# Configure SaslauthdAuthenticator

Scylla can outsource authentication to a third-party utility named [saslauthd](https://linux.die.net/man/8/saslauthd), which, in turn,supports many different authentication mechanisms.
Scylla accomplishes this by providing a custom authenticator named SaslauthdAuthenticator.
This procedure explains how to install and configure it.
Once configured, any login to Scylla is authenticated with the SaslauthdAuthenticator.

**Procedure**

1. Install saslauthd.  The easiest way is via a Linux package, if your package manager supports it.
   Choose a package according to your distro.

   rpm-based distros

   Use the `cyrus-sasl` package

   deb-based distros

   Use the `sasl2-bin` package
2. Enable the saslauthd service. Run:
   ```shell
   systemctl enable saslauthd.service
   ```
3. Configure saslauthd: choose the authentication mechanism (e.g., LDAP or PAM) and set the appropriate mechanism-specific parameters by following the [saslauthd documentation](https://linux.die.net/man/8/saslauthd).

   #### NOTE
   If you are configuring saslauthd for LDAP, use the [configuration details](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/ldap-authentication.md) which are specific for Scylla.
4. After every configuration change, restart the saslauthd service.
   ```shell
   systemctl restart saslauthd.service
   ```
5. Test your configuration using the [testsaslauthd](https://linux.die.net/man/8/testsaslauthd) command. Verify you see a success message.
   If not, verify that the user name and password are correct and then look at the saslauthd logs ( run `dmesg -H`, and look for `LOG_AUTH`) to diagnose problems.
6. Find the mux file (saslauthd’s Unix domain socket) and note its full path.

   rpm-based distros

   Usually, it is /run/saslauthd/mux

   deb-based distros

   Usually, it is /var/run/sasl2/mux
7. Once saslauthd is correctly configured and running, you modify the scylla.yaml configuration file, so communication can begin. Modify the following entries:
   * `authenticator: com.scylladb.auth.SaslauthdAuthenticator`
   * `saslauthd_socket_path: /path/to/the/mux`
8. Restart the Scylla server.  From now on, Scylla will authenticate all login attempts via saslauthd.

   Supported OS
   ```shell
   sudo systemctl restart scylla-server
   ```

   Docker
   ```shell
   docker exec -it some-scylla supervisorctl restart scylla
   ```

   (without restarting *some-scylla* container)
9. Create Scylla roles which **match** the same roles in the LDAP server. To create a role, refer to the [CQL Reference](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/authorization.md#cql-security) and the [RBAC example](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/rbac-usecase.md).
