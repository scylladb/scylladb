add_library(tools STATIC)
target_sources(tools
  PRIVATE
    load_system_tablets.cc
    read_mutation.cc
    scylla-types.cc
    scylla-sstable.cc
    scylla-nodetool.cc
    schema_loader.cc
    utils.cc
    lua_sstable_consumer.cc)
target_include_directories(tools
  PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${RAPIDJSON_INCLUDE_DIRS}
  PRIVATE
    ${LUA_INCLUDE_DIR})
target_link_libraries(tools
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    db)

include(build_submodule)
build_submodule(cqlsh cqlsh)
build_submodule(tools java
  NOARCH)
build_submodule(jmx jmx
  NOARCH)

execute_process(
  COMMAND "${CMAKE_SOURCE_DIR}/install-dependencies.sh" --print-python3-runtime-packages
  OUTPUT_VARIABLE python3_dependencies
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND "${CMAKE_SOURCE_DIR}/install-dependencies.sh" --print-pip-runtime-packages
  OUTPUT_VARIABLE pip_dependencies
  OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(
  COMMAND "${CMAKE_SOURCE_DIR}/install-dependencies.sh" --print-pip-symlinks
  OUTPUT_VARIABLE pip_symlinks
  OUTPUT_STRIP_TRAILING_WHITESPACE)
build_submodule(python3 python3
  --packages
  "${python3_dependencies}"
  --pip-packages
  "${pip_dependencies}"
  --pip-symlinks
  "${pip_symlinks}")

check_headers(check-headers tools
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)

add_subdirectory(testing/dist-check)
