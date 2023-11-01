add_library(compaction STATIC)
target_sources(compaction
  PRIVATE
    compaction.cc
    compaction_manager.cc
    compaction_strategy.cc
    leveled_compaction_strategy.cc
    size_tiered_compaction_strategy.cc
    task_manager_module.cc
    time_window_compaction_strategy.cc)
target_include_directories(compaction
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(compaction
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    mutation_writer
    replica)

check_headers(check-headers compaction
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
