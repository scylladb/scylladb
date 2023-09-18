set(unstripped_dist_pkg
  ${CMAKE_CURRENT_BINARY_DIR}/${Scylla_PRODUCT}-unstripped-${Scylla_VERSION}-${Scylla_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.tar.gz)
add_custom_command(
  OUTPUT ${unstripped_dist_pkg}
  COMMAND scripts/create-relocatable-package.py
    --build-dir ${CMAKE_BINARY_DIR}
    --node-exporter-dir ${CMAKE_BINARY_DIR}/node_exporter
    ${unstripped_dist_pkg}
  DEPENDS
    scylla
    ${CMAKE_BINARY_DIR}/iotune
    ${CMAKE_BINARY_DIR}/node_exporter/node_exporter
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(dist-server-deb
  COMMAND reloc/build_deb.sh
    --reloc-pkg ${unstripped_dist_pkg}
    --builddir ${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS ${unstripped_dist_pkg}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_custom_target(dist-server-rpm
  COMMAND reloc/build_rpm.sh
    --reloc-pkg ${unstripped_dist_pkg}
    --builddir ${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS ${unstripped_dist_pkg}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

function(add_stripped name)
  set(output ${name}.stripped)
  if(TARGET ${name})
    add_custom_command(
      OUTPUT
        ${CMAKE_BINARY_DIR}/${name}.debug
        ${CMAKE_BINARY_DIR}/${name}.stripped
      COMMAND ${CMAKE_SOURCE_DIR}/scripts/strip.sh $<TARGET_FILE:${name}>
      DEPENDS ${name})
  else()
    # ${name} must be an absolute path
    add_custom_command(
      OUTPUT
        ${name}.debug
        ${name}.stripped
      COMMAND ${CMAKE_SOURCE_DIR}/scripts/strip.sh ${name}
      DEPENDS ${name})
  endif()
endfunction()

add_stripped(scylla)

# app_iotune is located in seastar/apps/iotune
add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/iotune
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:app_iotune> ${CMAKE_BINARY_DIR}/iotune
  DEPENDS app_iotune)
add_stripped(${CMAKE_BINARY_DIR}/iotune)

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
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/debian
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/debian/debian_files_gen.py
    --build-dir ${CMAKE_BINARY_DIR}
    --output-dir ${CMAKE_CURRENT_BINARY_DIR}/debian
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

set(stripped_dist_pkg
  "${CMAKE_CURRENT_BINARY_DIR}/${Scylla_PRODUCT}-${Scylla_VERSION}-${Scylla_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.tar.gz")
add_custom_command(
  OUTPUT
    ${stripped_dist_pkg}
  COMMAND
    ${CMAKE_SOURCE_DIR}/scripts/create-relocatable-package.py
      --stripped
      --build-dir ${CMAKE_BINARY_DIR}
      --node-exporter-dir ${CMAKE_BINARY_DIR}/node_exporter
      ${stripped_dist_pkg}
  DEPENDS
    ${CMAKE_BINARY_DIR}/scylla.stripped
    ${CMAKE_BINARY_DIR}/iotune.stripped
    ${CMAKE_BINARY_DIR}/node_exporter/node_exporter.stripped
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_custom_target(dist-server-tar
  DEPENDS ${stripped_dist_pkg})
add_custom_target(package
  ${CMAKE_COMMAND} -E copy ${stripped_dist_pkg} ${Scylla_PRODUCT}-package.tar.gz
  DEPENDS ${stripped_dist_pkg})


set(dist_pkgs
  "${stripped_dist_pkg}")
dist_submodule(cqlsh cqlsh dist_pkgs
  NOARCH)
dist_submodule(tools java dist_pkgs
  NOARCH)
dist_submodule(jmx jmx dist_pkgs
  NOARCH)
dist_submodule(python3 python3 dist_pkgs)
set(unified_dist_pkg
  "${CMAKE_CURRENT_BINARY_DIR}/${Scylla_PRODUCT}-unified-${Scylla_VERSION}-${Scylla_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.tar.gz")
add_custom_command(
  OUTPUT
    "${unified_dist_pkg}"
  COMMAND
    ${CMAKE_SOURCE_DIR}/unified/build_unified.sh
      --build-dir ${CMAKE_BINARY_DIR}
      --pkgs "${dist_pkgs}"
      --unified-pkg ${unified_dist_pkg}
  DEPENDS
    ${dist_pkgs}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  VERBATIM)
add_custom_target(dist-unified
  DEPENDS ${unified_dist_pkg})

add_subdirectory(debuginfo)
