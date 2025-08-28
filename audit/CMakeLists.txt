include(add_whole_archive)

add_library(scylla_audit STATIC)
target_sources(scylla_audit
  PRIVATE
    audit.cc
    audit_cf_storage_helper.cc
    audit_syslog_storage_helper.cc)
target_include_directories(scylla_audit
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(scylla_audit
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    cql3)

add_whole_archive(audit scylla_audit)
