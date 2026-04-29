include(add_whole_archive)
include(ExternalProject)

find_package(cpp-jwt REQUIRED)

add_library(scylla_encryption STATIC)

set(LIBKMIP_DIR "${PROJECT_SOURCE_DIR}/libkmip")

# TODO: once we are cmake-only, we can remove submodule and just 
# fully externalproject this.

ExternalProject_Add(
    libkmip
    SOURCE_DIR ${LIBKMIP_DIR}
    BINARY_DIR ${CMAKE_BINARY_DIR}/$<CONFIG>
    CONFIGURE_COMMAND ${CMAKE_COMMAND} -E make_directory <BINARY_DIR>/libkmip
    BUILD_COMMAND make -f "<SOURCE_DIR>/Makefile"  
        SRC_DIR=<SOURCE_DIR>/src
        INC_DIR=<SOURCE_DIR>/include
        LIB_DIR=<BINARY_DIR> 
        OBJ_DIR=<BINARY_DIR>/libkmip
        <BINARY_DIR>/libkmip.a
    BUILD_BYPRODUCTS
        <BINARY_DIR>/libkmip.a
    INSTALL_COMMAND ""
)

ExternalProject_Get_property(libkmip BINARY_DIR)

target_sources(scylla_encryption
  PRIVATE
    encrypted_file_impl.cc
    encryption.cc
    encryption_config.cc
    azure_host.cc
    azure_key_provider.cc
    gcp_host.cc
    gcp_key_provider.cc
    kmip_host.cc
    kmip_key_provider.cc
    kms_host.cc
    kms_key_provider.cc
    local_file_provider.cc
    replicated_key_provider.cc
    symmetric_key.cc
    system_key.cc
    utils.cc)
target_include_directories(scylla_encryption
  PUBLIC
    ${CMAKE_SOURCE_DIR}
  PRIVATE
    ${LIBKMIP_DIR}/include
    )
target_link_libraries(scylla_encryption
  PUBLIC
    Seastar::seastar
  PRIVATE
    cql3
    utils
    cpp-jwt::cpp-jwt
    ${BINARY_DIR}/libkmip.a)

if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(scylla_encryption REUSE_FROM scylla-precompiled-header)
endif()

target_compile_definitions(scylla_encryption
  PUBLIC
    HAVE_KMIP)

check_headers(check-headers scylla_encryption
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)

add_whole_archive(encryption scylla_encryption)
