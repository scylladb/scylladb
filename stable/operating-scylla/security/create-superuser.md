# Creating a Custom Superuser

The default ScyllaDB superuser role is `cassandra` with password `cassandra`.
Users with the `cassandra` role have full access to the database and can run
any CQL command on the database resources.

To improve security, we recommend creating a custom superuser. You should:

1. Use the default `cassandra` superuser to log in.
2. Create a custom superuser.
3. Log in as the custom superuser.
4. Remove the `cassandra` role.

In the above procedure, you only need to use the `cassandra` superuser once, during
the initial RBAC set up.
To completely eliminate the need to use `cassandra`, you can [configure the initial
custom superuser in the scylla.yaml configuration file](#create-superuser-in-config-file).

<a id="create-superuser-procedure"></a>

## Procedure

1. Start cqlsh with the default superuser settings:
   ```default
   cqlsh -u cassandra -p cassandra
   ```
2. Create a new superuser:
   ```default
   CREATE ROLE <custom_superuser name>  WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '<custom_superuser_password>';
   ```

   For example:
   ```default
   CREATE ROLE dba WITH SUPERUSER = true AND LOGIN = true and PASSWORD = '39fksah!';
   ```

   #### WARNING
   You must set a PASSWORD when creating a role with LOGIN privileges.
   Otherwise, you will not be able to log in to the database using that role.
3. Exit cqlsh:
   ```default
   EXIT;
   ```
4. Log in as the new superuser:
   ```default
   cqlsh -u <custom_superuser name> -p <custom_superuser_password>
   ```

   For example:
   ```default
   cqlsh -u dba -p 39fksah!
   ```
5. Show all the roles to verify that the new superuser was created:
   ```default
   LIST ROLES;
   ```
6. Remove the cassandra superuser:
   ```default
   DROP ROLE cassandra;
   ```
7. Show all the roles to verify that the cassandra role was deleted:
   ```default
   LIST ROLES;
   ```

<a id="create-superuser-in-config-file"></a>

## Setting Custom Superuser Credentials in scylla.yaml

Operating ScyllaDB using the default superuser `cassandra` with password `cassandra`
is insecure and impacts performance. For this reason, the default should be used only once -
to create a custom superuser role, following the CQL [procedure](#create-superuser-procedure) above.

To avoid executing with the default credentials for the period before you can make
the CQL modifications, you can configure the custom superuser name and password
in the `scylla.yaml` configuration file:

```yaml
auth_superuser_name: <superuser name>
auth_superuser_salted_password: <superuser salted password as processed by mkpassword or similar - cleartext is not allowed>
```
