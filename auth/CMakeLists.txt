include(add_whole_archive)

add_library(scylla_auth STATIC)
target_sources(scylla_auth
  PRIVATE
    allow_all_authenticator.cc
    allow_all_authorizer.cc
    authenticated_user.cc
    authenticator.cc
    certificate_authenticator.cc
    common.cc
    default_authorizer.cc
    password_authenticator.cc
    passwords.cc
    permission.cc
    permissions_cache.cc
    resource.cc
    role_or_anonymous.cc
    roles-metadata.cc
    sasl_challenge.cc
    service.cc
    standard_role_manager.cc
    transitional.cc
    maintenance_socket_role_manager.cc)
target_include_directories(scylla_auth
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_auth
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    absl::headers
    cql3
    idl
    wasmtime_bindings
    libxcrypt::libxcrypt)

add_whole_archive(auth scylla_auth)

check_headers(check-headers scylla_auth
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
