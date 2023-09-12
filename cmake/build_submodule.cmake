function(build_submodule name dir)
  cmake_parse_arguments(parsed_args "NOARCH" "" "" ${ARGN})
  string(REPLACE "-" "~" scylla_version_tilde ${Scylla_VERSION})
  set(version "${scylla_version_tilde}-${Scylla_RELEASE}")
  set(scylla_version
    "${Scylla_PRODUCT}-${scylla_version_tilde}-${Scylla_RELEASE}")
  set(working_dir ${CMAKE_CURRENT_SOURCE_DIR}/${dir})
  if(parsed_args_NOARCH)
    set(arch "noarch")
  else()
    set(arch "${CMAKE_SYSTEM_PROCESSOR}")
  endif()
  set(reloc_args ${parsed_args_UNPARSED_ARGUMENTS})
  set(reloc_pkg "${working_dir}/build/${Scylla_PRODUCT}-${name}-${version}.${arch}.tar.gz")
  add_custom_command(
    OUTPUT ${reloc_pkg}
    COMMAND reloc/build_reloc.sh --version ${scylla_version} --nodeps ${reloc_args}
    WORKING_DIRECTORY "${working_dir}"
    JOB_POOL submodule_pool)
  add_custom_target(dist-${name}-tar
    DEPENDS ${reloc_pkg})
  add_custom_target(dist-${name}-rpm
    COMMAND reloc/build_rpm.sh --reloc-pkg ${reloc_pkg}
    DEPENDS ${reloc_pkg}
    WORKING_DIRECTORY "${working_dir}")
  add_custom_target(dist-${name}-deb
    COMMAND reloc/build_deb.sh --reloc-pkg ${reloc_pkg}
    DEPENDS ${reloc_pkg}
    WORKING_DIRECTORY "${working_dir}")
  add_custom_target(dist-${name}
    DEPENDS dist-${name}-tar dist-${name}-rpm dist-${name}-deb)
endfunction()
