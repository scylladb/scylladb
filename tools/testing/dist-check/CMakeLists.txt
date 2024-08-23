if(CMAKE_CONFIGURATION_TYPES)
  foreach(config ${CMAKE_CONFIGURATION_TYPES})
    string(APPEND build_mode
      "$<$<CONFIG:${config}>:${scylla_build_mode_${config}}>")
  endforeach()
else()
  set(build_mode ${scylla_build_mode_${CMAKE_BUILD_TYPE}})
endif()

add_custom_target(dist-check
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/dist-check.sh --mode ${build_mode}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
add_dependencies(dist-check
  dist-server-rpm
  dist-python3-rpm
  dist-jmx-rpm
  dist-tools-rpm
  dist-cqlsh-rpm)
