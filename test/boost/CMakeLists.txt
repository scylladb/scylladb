function(add_scylla_test name)
  cmake_parse_arguments(parsed_args
    ""
    "KIND"
    "LIBRARIES;SOURCES"
    ${ARGN})
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
  elseif(kind STREQUAL "BOOST")
    target_link_libraries(${name}
      PRIVATE
        Boost::unit_test_framework
        Boost::dynamic_linking)
  endif()
  if(parsed_args_LIBRARIES)
    target_link_libraries(${name}
      PRIVATE
        ${parsed_args_LIBRARIES})
  endif()
endfunction()

add_scylla_test(anchorless_list_test
  KIND BOOST)
add_scylla_test(auth_passwords_test
  KIND BOOST
  LIBRARIES auth)
add_scylla_test(auth_resource_test
  KIND BOOST)
add_scylla_test(big_decimal_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(caching_options_test
  KIND BOOST)
add_scylla_test(cartesian_product_test
  KIND BOOST)
add_scylla_test(checksum_utils_test
  KIND BOOST)
add_scylla_test(chunked_vector_test
  KIND BOOST)
add_scylla_test(compress_test
  KIND BOOST)
add_scylla_test(cql_auth_syntax_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(crc_test
  KIND BOOST)
add_scylla_test(duration_test
  KIND BOOST)
add_scylla_test(dynamic_bitset_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(enum_option_test
  KIND BOOST)
add_scylla_test(enum_set_test
  KIND BOOST)
add_scylla_test(idl_test
  KIND BOOST
  LIBRARIES idl)
add_scylla_test(json_test
  KIND BOOST
  LIBRARIES cql3)
add_scylla_test(keys_test
  KIND BOOST
  LIBRARIES idl schema)
add_scylla_test(like_matcher_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(linearizing_input_stream_test
  KIND BOOST)
add_scylla_test(map_difference_test
  KIND BOOST)
add_scylla_test(nonwrapping_range_test
  KIND BOOST)
add_scylla_test(observable_test
  KIND BOOST)
add_scylla_test(range_test
  KIND BOOST)
add_scylla_test(range_tombstone_list_test
  KIND BOOST)
add_scylla_test(serialization_test
  KIND BOOST)
add_scylla_test(small_vector_test
  KIND BOOST)
add_scylla_test(top_k_test
  KIND BOOST)
add_scylla_test(transport_test
  KIND BOOST)
add_scylla_test(type_json_test
  KIND BOOST
  LIBRARIES
    cql3
    types
    utils)
add_scylla_test(types_test
  KIND BOOST)
add_scylla_test(vint_serialization_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(bptree_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(utf8_test
  KIND BOOST
  LIBRARIES utils)
add_scylla_test(user_function_test
  KIND BOOST)
add_scylla_test(user_types_test
  KIND BOOST)
add_scylla_test(expr_test
  KIND BOOST
  LIBRARIES cql3)
