add_library(tools STATIC)
target_sources(tools
  PRIVATE
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
build_submodule(cqlsh cqlsh
  NOARCH)
build_submodule(tools java
  NOARCH)
build_submodule(jmx jmx
  NOARCH)
build_submodule(python3 python3
  --packages
  "python3-pyyaml python3-urwid python3-pyparsing python3-requests python3-pyudev python3-setuptools python3-psutil python3-distro python3-click python3-six"
  --pip-packages
  "scylla-driver geomet traceback-with-variables scylla-api-client"
  --pip-symlinks
  "scylla-api-client")
