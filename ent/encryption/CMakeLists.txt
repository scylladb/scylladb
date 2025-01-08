include(add_whole_archive)

find_package(cpp-jwt REQUIRED)
find_package(kmip)

add_library(scylla_encryption STATIC)
target_sources(scylla_encryption
  PRIVATE
    encrypted_file_impl.cc
    encryption.cc
    encryption_config.cc
    gcp_host.cc
    gcp_key_provider.cc
    kmip_host.cc
    kmip_key_provider.cc
    kms_host.cc
    kms_key_provider.cc
    local_file_provider.cc
    replicated_key_provider.cc
    symmetric_key.cc
    system_key.cc)
target_include_directories(scylla_encryption
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_encryption
  PUBLIC
    Seastar::seastar
  PRIVATE
    cql3
    utils
    cpp-jwt::cpp-jwt)
if(kmip_FOUND)
  target_link_libraries(scylla_encryption
    PRIVATE
      KMIP::kmipc)
  target_compile_definitions(scylla_encryption
    PUBLIC
      HAVE_KMIP)
endif()

check_headers(check-headers scylla_encryption
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)

add_whole_archive(encryption scylla_encryption)
