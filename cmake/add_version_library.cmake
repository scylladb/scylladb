###
### Generate version file and supply appropriate compile definitions for release.cc
###
function(generate_scylla_version)
  set(version_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-VERSION-FILE)
  set(release_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-RELEASE-FILE)
  set(product_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-PRODUCT-FILE)
  execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN --output-dir "${CMAKE_CURRENT_BINARY_DIR}"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

  file(STRINGS ${version_file} scylla_version)
  file(STRINGS ${release_file} scylla_release)
  file(STRINGS ${product_file} scylla_product)
  set(Scylla_VERSION "${scylla_version}" CACHE INTERNAL "")
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
