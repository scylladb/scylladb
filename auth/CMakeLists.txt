add_library(scylla_auth STATIC)
target_sources(scylla_auth
  PRIVATE
    allow_all_authenticator.cc
    allow_all_authorizer.cc
    authenticated_user.cc
    authentication_options.cc
    authenticator.cc
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
    transitional.cc)
target_include_directories(scylla_auth
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_auth
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    cql3
    idl
    wasmtime_bindings)

add_library(auth INTERFACE)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.24)
    target_link_libraries(auth INTERFACE
        "$<LINK_LIBRARY:WHOLE_ARCHIVE,scylla_auth>")
else()
    add_dependencies(auth scylla_auth)
    target_include_directories(auth INTERFACE
        ${CMAKE_SOURCE_DIR})
    target_link_options(auth INTERFACE
        "$<$<CXX_COMPILER_ID:Clang>:SHELL:LINKER:-force_load $<TARGET_LINKER_FILE:scylla_auth>>"
        "$<$<CXX_COMPILER_ID:GNU>:SHELL:LINKER:--whole-archive $<TARGET_LINKER_FILE:scylla_auth> LINKER:--no-whole-archive>")
endif()
