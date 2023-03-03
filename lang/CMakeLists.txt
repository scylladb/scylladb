add_library(lang STATIC)
target_sources(lang
  PRIVATE
    lua.cc
    wasm.cc
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
    ${LUA_LIBRARIES})
