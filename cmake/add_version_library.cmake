###
### Generate version file and supply appropriate compile definitions for release.cc
###
function(add_version_library name source)
  set(version_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-VERSION-FILE)
  set(release_file ${CMAKE_CURRENT_BINARY_DIR}/SCYLLA-RELEASE-FILE)
  execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN --output-dir "${CMAKE_CURRENT_BINARY_DIR}"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  file(STRINGS ${version_file} scylla_version)
  file(STRINGS ${release_file} scylla_release)

  add_library(${name} OBJECT ${source})
  target_compile_definitions(${name}
    PRIVATE
      SCYLLA_VERSION=\"${scylla_version}\"
      SCYLLA_RELEASE=\"${scylla_release}\")
  target_link_libraries(${name}
    PRIVATE
      Seastar::seastar)
endfunction(add_version_library)
