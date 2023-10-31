set(Scylla_DATE_STAMP
  ""
  CACHE
  STRING
  "Set datestamp for SCYLLA-VERSION-GEN (like YYYYMMDD, for instance, 20230314)")

###
### Generate version file and supply appropriate compile definitions for release.cc
###
function(generate_scylla_version)
  set(version_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-VERSION-FILE)
  set(release_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-RELEASE-FILE)
  set(product_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-PRODUCT-FILE)
  if(NOT "${Scylla_DATE_STAMP}" STREQUAL "")
    set(version_gen_args
      --date-stamp "${Scylla_DATE_STAMP}")
  endif()
  execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN
      ${version_gen_args}
      --output-dir "${CMAKE_CURRENT_BINARY_DIR}"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

  file(STRINGS ${version_file} scylla_version)
  file(STRINGS ${release_file} scylla_release)
  file(STRINGS ${product_file} scylla_product)

  string(REPLACE "-" "~" scylla_version_tilde ${scylla_version})

  set(Scylla_VERSION "${scylla_version_tilde}" CACHE INTERNAL "")
  set(Scylla_RELEASE "${scylla_release}" CACHE INTERNAL "")
  set(Scylla_PRODUCT "${scylla_product}" CACHE INTERNAL "")
endfunction(generate_scylla_version)

function(add_version_library name source)
  add_library(${name} OBJECT ${source})
  target_compile_definitions(${name}
    PRIVATE
      SCYLLA_VERSION=\"${Scylla_VERSION}\"
      SCYLLA_RELEASE=\"${Scylla_RELEASE}\")
  target_link_libraries(${name}
    PRIVATE
      Seastar::seastar)
endfunction(add_version_library)
