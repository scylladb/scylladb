add_scylla_test(ldap_connection_test
  KIND SEASTAR)
add_scylla_test(ldap_role_manager_test
  KIND SEASTAR
  SOURCES role_manager_test.cc)
add_scylla_test(saslauthd_authenticator_test
  KIND SEASTAR)
