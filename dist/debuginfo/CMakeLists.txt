set(debuginfo_pkg
  "${CMAKE_CURRENT_BINARY_DIR}/${Scylla_PRODUCT}-debuginfo-${Scylla_VERSION}-${Scylla_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}.tar.gz")
add_custom_command(
  OUTPUT
    ${debuginfo_pkg}
  COMMAND
    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/create-relocatable-package.py
      --build-dir ${CMAKE_BINARY_DIR}
      ${debuginfo_pkg}
  DEPENDS
    ${CMAKE_BINARY_DIR}/scylla.debug
    ${CMAKE_BINARY_DIR}/iotune.debug
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_custom_target(dist-server-debuginfo
  DEPENDS ${debuginfo_pkg})
