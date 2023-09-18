find_package(jsoncpp REQUIRED)

add_subdirectory(lib)
add_subdirectory(perf)

#
# Add a scylla unit test
#
# add_scylla_test(<test_name>
#   [KIND <kind>]
#   [SOUCES <src>...])
#
# kind can be:
# * SEASTAR - a unit test that depends on the scylla sources and the
#   seastar test framework.
# * BOOST - a unit test that only depends on the listed sources and the
#   Boost test framework
# * UNIT - a test driven its own main(). and it depends on the
#   seastar test framework.
# test_name should map to a source file, such that ${test_name}.cc
# is a valid source file. If this isn't the case, please use the SOURCE
# param.
#
function(add_scylla_test name)
  cmake_parse_arguments(parsed_args
    ""
    "KIND"
    "LIBRARIES;SOURCES"
    ${ARGN})
  if(parsed_args_UNPARSED_ARGUMENTS)
    message(FATAL_ERROR "Unknown keywords given to 'add_scylla_test()': \"${parsed_args_UNPARSED_ARGUMENTS}\"")
  endif()

  if(parsed_args_KIND)
    set(kind ${parsed_args_KIND})
  else()
    set(kind "SEASTAR")
  endif()
  if(parsed_args_SOURCES)
    set(src "${parsed_args_SOURCES}")
  else()
    set(src "${name}.cc")
  endif()
  add_executable(${name} ${src})
  add_dependencies(tests ${name})

  target_include_directories(${name}
    PRIVATE
      ${CMAKE_SOURCE_DIR})
  target_link_libraries(${name}
    PRIVATE
      test-lib
      Seastar::seastar
      xxHash::xxhash)
  if(kind STREQUAL "SEASTAR")
    target_link_libraries(${name}
      PRIVATE
        Seastar::seastar_testing)
    target_compile_definitions(${name}
      PRIVATE
        SEASTAR_TESTING_MAIN)
  elseif(kind STREQUAL "BOOST")
    target_link_libraries(${name}
      PRIVATE
        Boost::unit_test_framework
        Boost::dynamic_linking)
  elseif(kind STREQUAL "UNIT")
    target_link_libraries(${name}
      PRIVATE
        Seastar::seastar_testing)
  else()
    message(FATAL_ERROR "unknown test KIND: ${kind}")
  endif()
  if(parsed_args_LIBRARIES)
    target_link_libraries(${name}
      PRIVATE
        ${parsed_args_LIBRARIES})
  endif()
endfunction()

if(BUILD_TESTING)
    add_custom_target(tests)
    add_dependencies(tests scylla)

    add_subdirectory(boost)
    add_subdirectory(manual)
    add_subdirectory(unit)
    add_subdirectory(raft)
    add_subdirectory(resource/wasm)
endif()
