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
set(iotune_path "${CMAKE_BINARY_DIR}/$<CONFIG>/iotune")
add_custom_command(
  OUTPUT "${iotune_path}"
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:app_iotune> "${iotune_path}"
  DEPENDS $<OUTPUT_CONFIG:$<TARGET_FILE:app_iotune>>)
add_stripped("${iotune_path}")

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
dist_submodule(tools java dist_pkgs
  NOARCH)
dist_submodule(jmx jmx dist_pkgs
  NOARCH)
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
  dist-tools
  dist-jmx
  dist-python3
  dist-server)

add_subdirectory(debuginfo)
