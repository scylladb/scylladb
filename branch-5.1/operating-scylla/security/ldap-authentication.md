# LDAP Authentication

#### NOTE
This feature is only available with Scylla Enterprise. If you are using Scylla Open Source, this feature will not be available.

#### Versionadded
Added in version Scylla: Enterprise 2021.1.2

Scylla supports user authentication via an LDAP server by leveraging the SaslauthdAuthenticator.
By configuring saslauthd correctly against your LDAP server, you enable Scylla to check the user’s credentials through it.

## Configure saslauthd for LDAP

**Before You Begin**

This procedure requires you to install and configure saslauthd.
The general instructions are [here](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/saslauthd.md).

1. Follow all of the steps in [this procedure](https://opensource.docs.scylladb.com/branch-5.1/operating-scylla/security/saslauthd.md) and use the code snippets below to list LDAP as the authentication mechanism.
2. You must list LDAP as saslauthd’s authentication mechanism:

   rpm-based distros

   Edit `/etc/sysconfig/saslauthd` and add:
   ```none
   MECH=ldap
   ```

   deb-based distros

   Edit `/etc/default/saslauthd` and add:
   ```none
   MECHANISMS=ldap
   ```
3. You also have to edit the /etc/saslauthd.conf file to provide adequate [parameter](https://github.com/cyrusimap/cyrus-sasl/blob/cyrus-sasl-2.1.27/saslauthd/LDAP_SASLAUTHD#L74) values for your LDAP server.
