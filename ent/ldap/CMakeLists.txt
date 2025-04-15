find_package(OpenLDAP REQUIRED
  ldap lber)
add_library(ldap STATIC)
target_sources(ldap
  PRIVATE
    ldap_connection.cc)
target_include_directories(ldap
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(ldap
  PUBLIC
    Seastar::seastar
  PRIVATE
    OpenLDAP::ldap OpenLDAP::lber)

check_headers(check-headers ldap
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)