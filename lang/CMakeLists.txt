add_library(lang STATIC)
if (Scylla_USE_PRECOMPILED_HEADER)
  target_precompile_headers(lang REUSE_FROM scylla-main)
endif()
target_sources(lang
  PRIVATE
    manager.cc
    lua.cc
    wasm.cc
    wasm_alien_thread_runner.cc
    wasm_instance_cache.cc)
target_include_directories(lang
  PUBLIC
    ${CMAKE_SOURCE_DIR}
  PRIVATE
    ${LUA_INCLUDE_DIR})
target_link_libraries(lang
  PUBLIC
    wasmtime_bindings
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    absl::headers
    ${LUA_LIBRARIES})

check_headers(check-headers lang
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
