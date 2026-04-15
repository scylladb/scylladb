set(unstripped_dist_pkg
  ${CMAKE_BINARY_DIR}/$<CONFIG>/dist/tar/${Scylla_PRODUCT}-unstripped-${Scylla_VERSION}-${Scylla_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.tar.gz)
add_custom_command(
  OUTPUT ${unstripped_dist_pkg}
  COMMAND
    scripts/create-relocatable-package.py
      --build-dir ${CMAKE_BINARY_DIR}/$<CONFIG>
      --node-exporter-dir ${CMAKE_BINARY_DIR}/node_exporter
      --debian-dir ${CMAKE_BINARY_DIR}/debian
      ${unstripped_dist_pkg}
  DEPENDS
    ${CMAKE_BINARY_DIR}/$<CONFIG>/scylla
    ${CMAKE_BINARY_DIR}/$<CONFIG>/iotune
    ${CMAKE_BINARY_DIR}/$<CONFIG>/patchelf
    ${CMAKE_BINARY_DIR}/node_exporter/node_exporter
    ${CMAKE_BINARY_DIR}/debian
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(dist-server-deb ALL
  # the --builddir should match the paths specified by
  # "packages" in dist/docker/debian/build_docker.sh
  COMMAND reloc/build_deb.sh
    --reloc-pkg ${unstripped_dist_pkg}
    --builddir ${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/debian
  DEPENDS ${unstripped_dist_pkg}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_custom_target(dist-server-rpm ALL
  COMMAND reloc/build_rpm.sh
    --reloc-pkg ${unstripped_dist_pkg}
    --builddir ${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/redhat
  DEPENDS ${unstripped_dist_pkg}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_custom_target(dist-server
  DEPENDS dist-server-deb dist-server-rpm)

function(add_stripped name)
  # ${name} must be an absolute path
  add_custom_command(
    OUTPUT
      ${name}.debug
      ${name}.stripped
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/strip.sh ${name}
    DEPENDS ${name})
endfunction()

add_stripped("${CMAKE_BINARY_DIR}/$<CONFIG>/scylla")

# app_iotune is located in seastar/apps/iotune
if(TARGET Seastar::iotune)
  set(iotune_src "$<TARGET_FILE:Seastar::iotune>")
else()
  set(iotune_src "$<TARGET_FILE:app_iotune>")
endif()
set(iotune_dst "${CMAKE_BINARY_DIR}/$<CONFIG>/iotune")
add_custom_command(
  OUTPUT "${iotune_dst}"
  COMMAND ${CMAKE_COMMAND} -E copy "${iotune_src}" "${iotune_dst}"
  DEPENDS $<OUTPUT_CONFIG:${iotune_src}>)
add_stripped("${iotune_dst}")

add_stripped("${CMAKE_BINARY_DIR}/$<CONFIG>/patchelf")

find_program(TAR_COMMAND tar
  REQUIRED)
execute_process(
  COMMAND
    ${CMAKE_SOURCE_DIR}/install-dependencies.sh --print-node-exporter-filename
  OUTPUT_VARIABLE
    node_exporter_filename
  OUTPUT_STRIP_TRAILING_WHITESPACE)
add_custom_command(
  OUTPUT
    ${CMAKE_BINARY_DIR}/node_exporter/node_exporter
  COMMAND
    ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/node_exporter
  COMMAND
    ${TAR_COMMAND}
      -C ${CMAKE_BINARY_DIR}/node_exporter
      -xpf ${node_exporter_filename}
      --no-same-owner
      --strip-components=1)
add_stripped(${CMAKE_BINARY_DIR}/node_exporter/node_exporter)

add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/debian
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/debian/debian_files_gen.py
    --build-dir ${CMAKE_BINARY_DIR}
    --output-dir ${CMAKE_BINARY_DIR}/debian
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

set(stripped_dist_pkg
  "${CMAKE_BINARY_DIR}/$<CONFIG>/dist/tar/${Scylla_PRODUCT}-${Scylla_VERSION}-${Scylla_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.tar.gz")
add_custom_command(
  OUTPUT
    ${stripped_dist_pkg}
  COMMAND
    ${CMAKE_SOURCE_DIR}/scripts/create-relocatable-package.py
      --stripped
      --build-dir ${CMAKE_BINARY_DIR}/$<CONFIG>
      --node-exporter-dir ${CMAKE_BINARY_DIR}/node_exporter
      --debian-dir ${CMAKE_BINARY_DIR}/debian
      ${stripped_dist_pkg}
  DEPENDS
    ${CMAKE_BINARY_DIR}/$<CONFIG>/scylla.stripped
    ${CMAKE_BINARY_DIR}/$<CONFIG>/patchelf.stripped
    ${CMAKE_BINARY_DIR}/$<CONFIG>/iotune.stripped
    ${CMAKE_BINARY_DIR}/node_exporter/node_exporter.stripped
    ${CMAKE_BINARY_DIR}/debian
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_custom_target(dist-server-tar
  DEPENDS ${stripped_dist_pkg})
add_custom_target(package
  ${CMAKE_COMMAND} -E copy ${stripped_dist_pkg} ${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/${Scylla_PRODUCT}-package.tar.gz
  DEPENDS ${stripped_dist_pkg})


set(dist_pkgs
  "${stripped_dist_pkg}")
dist_submodule(cqlsh cqlsh dist_pkgs)
dist_submodule(python3 python3 dist_pkgs)
set(unified_dist_pkg
  "${CMAKE_BINARY_DIR}/$<CONFIG>/dist/tar/${Scylla_PRODUCT}-unified-${Scylla_VERSION}-${Scylla_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.tar.gz")
add_custom_command(
  OUTPUT
    "${unified_dist_pkg}"
  COMMAND
    ${CMAKE_SOURCE_DIR}/unified/build_unified.sh
      --build-dir ${CMAKE_BINARY_DIR}/$<CONFIG>
      --pkgs "${dist_pkgs}"
      --unified-pkg ${unified_dist_pkg}
  DEPENDS
    ${dist_pkgs}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  VERBATIM)
add_custom_target(dist-unified ALL
  DEPENDS ${unified_dist_pkg})

add_custom_target(dist)
add_dependencies(dist
  dist-cqlsh
  dist-python3
  dist-server)

set(dist_rpm_dir "${CMAKE_BINARY_DIR}/$<CONFIG>/dist/rpm")
set(dist_deb_dir "${CMAKE_BINARY_DIR}/$<CONFIG>/dist/deb")

# Map system processor to Debian architecture names
if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  set(deb_arch "amd64")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  set(deb_arch "arm64")
else()
  message(FATAL_ERROR "Unsupported architecture: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

set(rpm_ver "${Scylla_VERSION}-${Scylla_RELEASE}")
set(deb_ver "${Scylla_VERSION}-${Scylla_RELEASE}-1")
set(rpm_arch "${CMAKE_SYSTEM_PROCESSOR}")

set(server_rpms_dir "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/redhat/RPMS/${rpm_arch}")
set(server_rpms
  "${server_rpms_dir}/${Scylla_PRODUCT}-${rpm_ver}.${rpm_arch}.rpm"
  "${server_rpms_dir}/${Scylla_PRODUCT}-server-${rpm_ver}.${rpm_arch}.rpm"
  "${server_rpms_dir}/${Scylla_PRODUCT}-server-debuginfo-${rpm_ver}.${rpm_arch}.rpm"
  "${server_rpms_dir}/${Scylla_PRODUCT}-conf-${rpm_ver}.${rpm_arch}.rpm"
  "${server_rpms_dir}/${Scylla_PRODUCT}-kernel-conf-${rpm_ver}.${rpm_arch}.rpm"
  "${server_rpms_dir}/${Scylla_PRODUCT}-node-exporter-${rpm_ver}.${rpm_arch}.rpm")
set(cqlsh_rpms
  "${CMAKE_SOURCE_DIR}/tools/cqlsh/build/redhat/RPMS/${rpm_arch}/${Scylla_PRODUCT}-cqlsh-${rpm_ver}.${rpm_arch}.rpm")
set(python3_rpms
  "${CMAKE_SOURCE_DIR}/tools/python3/build/redhat/RPMS/${rpm_arch}/${Scylla_PRODUCT}-python3-${rpm_ver}.${rpm_arch}.rpm")

set(server_debs_dir "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/debian")
set(server_debs
  "${server_debs_dir}/${Scylla_PRODUCT}_${deb_ver}_${deb_arch}.deb"
  "${server_debs_dir}/${Scylla_PRODUCT}-server_${deb_ver}_${deb_arch}.deb"
  "${server_debs_dir}/${Scylla_PRODUCT}-server-dbg_${deb_ver}_${deb_arch}.deb"
  "${server_debs_dir}/${Scylla_PRODUCT}-conf_${deb_ver}_${deb_arch}.deb"
  "${server_debs_dir}/${Scylla_PRODUCT}-kernel-conf_${deb_ver}_${deb_arch}.deb"
  "${server_debs_dir}/${Scylla_PRODUCT}-node-exporter_${deb_ver}_${deb_arch}.deb"
  "${server_debs_dir}/scylla-enterprise_${deb_ver}_all.deb"
  "${server_debs_dir}/scylla-enterprise-server_${deb_ver}_all.deb"
  "${server_debs_dir}/scylla-enterprise-conf_${deb_ver}_all.deb"
  "${server_debs_dir}/scylla-enterprise-kernel-conf_${deb_ver}_all.deb"
  "${server_debs_dir}/scylla-enterprise-node-exporter_${deb_ver}_all.deb")
set(cqlsh_debs
  "${CMAKE_SOURCE_DIR}/tools/cqlsh/build/debian/${Scylla_PRODUCT}-cqlsh_${deb_ver}_${deb_arch}.deb"
  "${CMAKE_SOURCE_DIR}/tools/cqlsh/build/debian/scylla-enterprise-cqlsh_${deb_ver}_all.deb")
set(python3_debs
  "${CMAKE_SOURCE_DIR}/tools/python3/build/debian/${Scylla_PRODUCT}-python3_${deb_ver}_${deb_arch}.deb"
  "${CMAKE_SOURCE_DIR}/tools/python3/build/debian/scylla-enterprise-python3_${deb_ver}_all.deb")

add_custom_target(collect-dist-rpm
  COMMAND ${CMAKE_COMMAND} -E rm -rf ${dist_rpm_dir}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${dist_rpm_dir}
  COMMAND ${CMAKE_COMMAND} -E copy ${server_rpms} ${cqlsh_rpms} ${python3_rpms} ${dist_rpm_dir}/
  DEPENDS dist
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  COMMENT "Collecting RPMs into ${dist_rpm_dir}")

add_custom_target(collect-dist-deb
  COMMAND ${CMAKE_COMMAND} -E rm -rf ${dist_deb_dir}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${dist_deb_dir}
  COMMAND ${CMAKE_COMMAND} -E copy ${server_debs} ${cqlsh_debs} ${python3_debs} ${dist_deb_dir}/
  DEPENDS dist
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  COMMENT "Collecting DEBs into ${dist_deb_dir}")

add_custom_target(collect-dist
  DEPENDS collect-dist-rpm collect-dist-deb)

add_subdirectory(debuginfo)
