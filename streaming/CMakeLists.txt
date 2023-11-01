add_library(streaming STATIC)
target_sources(streaming
  PRIVATE
    consumer.cc
    progress_info.cc
    session_info.cc
    stream_coordinator.cc
    stream_manager.cc
    stream_plan.cc
    stream_receive_task.cc
    stream_request.cc
    stream_result_future.cc
    stream_session.cc
    stream_session_state.cc
    stream_summary.cc
    stream_task.cc
    stream_transfer_task.cc)
target_include_directories(streaming
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(streaming
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    replica)

check_headers(check-headers streaming
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
