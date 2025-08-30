add_library(gms STATIC)
target_sources(gms
  PRIVATE
    application_state.cc
    endpoint_state.cc
    feature_service.cc
    generation-number.cc
    gossip_digest_ack2.cc
    gossip_digest_ack.cc
    gossip_digest_syn.cc
    gossiper.cc
    inet_address.cc
    versioned_value.cc
    version_generator.cc)
target_include_directories(gms
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(gms
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    db
    absl::headers)

check_headers(check-headers gms
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
