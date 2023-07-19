function(build_submodule name dir)
  file(STRINGS ${CMAKE_BINARY_DIR}/SCYLLA-PRODUCT-FILE scylla_product)
  file(STRINGS ${CMAKE_BINARY_DIR}/SCYLLA-VERSION-FILE scylla_version_dash)
  file(STRINGS ${CMAKE_BINARY_DIR}/SCYLLA-RELEASE-FILE scylla_release)
  string(REPLACE "-" "~" scylla_version_tilde scylla_version_dash)
  set(scylla_version
    "${scylla_product}-${scylla_version_dash}-${scylla_release}")
  set(reloc_pkg "${dir}/build/${name}-${scylla_version}.noarch.tar.gz")
  set(working_dir ${CMAKE_CURRENT_SOURCE_DIR}/${dir})
  add_custom_command(
    OUTPUT ${reloc_pkg}
    DEPENDS ${version_files}
    COMMAND reloc/build_reloc.sh --version ${scylla_version} --nodeps ${ARGN}
    WORKING_DIRECTORY "${working_dir}"
    COMMENT "Generating submodule ${name} in ${dir}"
    JOB_POOL submodule_pool)
  add_custom_target(dist-${name}-tar
    DEPENDS ${reloc_pkg})
  add_custom_target(dist-${name}-rpm
    COMMAND reloc/build_rpm.sh --reloc-pkg ${reloc_pkg}
    WORKING_DIRECTORY "${working_dir}")
  add_custom_target(dist-${name}-deb
    COMMAND reloc/build_deb.sh --reloc-pkg ${reloc_pkg}
    WORKING_DIRECTORY "${working_dir}")
  add_custom_target(dist-${name}
    DEPENDS dist-${name}-tar dist-${name}-rpm dist-${name}-deb)
endfunction()
