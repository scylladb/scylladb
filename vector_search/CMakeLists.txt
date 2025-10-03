add_library(vector_search STATIC)
target_sources(vector_search
  PRIVATE
    vector_store_client.cc
    dns.cc
    client.cc)
target_link_libraries(vector_search
  PUBLIC
    Seastar::seastar
  PRIVATE
    cql3
    db
    utils
    schema
    scylla_dht
    )

check_headers(check-headers vector_search
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
