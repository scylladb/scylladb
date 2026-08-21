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
  # bootstrap the versioning files
  execute_process(
    COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN
      ${version_gen_args}
      --output-dir "${CMAKE_CURRENT_BINARY_DIR}"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

  # we need to recreate the versioning files if the sha1 of repo is updated. to
  # assure that the script _always_ recreate these files, we need to add a
  # target depending on a file which can never be generated, so that the target
  # is always built as a dependencies of the "default" target. the rule
  # generating the never-generated file have to generate the version files, so
  # that, as an intended side effect, the version files are updated when
  # necessary.
  set(phantom_file ${CMAKE_CURRENT_BINARY_DIR}/__i_never_exist_and_will_never__)
  if(EXISTS ${phantom_file})
    message(FATAL_ERROR "Please drop ${phantom_file}")
  endif()
  set(versioning_files
    ${version_file}
    ${release_file}
    ${product_file})
  list(APPEND CMAKE_CONFIGURE_DEPENDS ${versioning_files})
  add_custom_command(
    OUTPUT
      ${versioning_files}
      ${phantom_file}
    COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN
      ${version_gen_args}
      --output-dir "${CMAKE_CURRENT_BINARY_DIR}"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Updating SCYLLA-PRODUCT-FILE, SCYLLA-VERSION-FILE and SCYLLA-RELEASE-FILE")
  add_custom_target(
    scylla-version-gen ALL
    DEPENDS ${phantom_file}
    COMMENT "Check release with SCYLLA-VERSION-GEN")

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
  add_dependencies(${name}
    scylla-version-gen)
  target_compile_definitions(${name}
    PRIVATE
      SCYLLA_PRODUCT=\"${Scylla_PRODUCT}\"
      SCYLLA_VERSION=\"${Scylla_VERSION}\"
      SCYLLA_RELEASE=\"${Scylla_RELEASE}\")
  target_link_libraries(${name}
    PRIVATE
      Seastar::seastar)
endfunction(add_version_library)
