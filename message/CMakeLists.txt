add_library(message STATIC)
target_sources(message
  PRIVATE
    messaging_service.cc
    messaging_service.hh)
target_include_directories(message
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(message
  PUBLIC
    gms
    Seastar::seastar
    absl::headers
  PRIVATE
    idl)

check_headers(check-headers message
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
