# ts is used by pgo/pgo.py
find_program(TIMESTAMP ts
  REQUIRED)

set(profdata_dir "${Scylla_PROFILE_DATA_DIR}/profiles")
set(sampled_profdata "${profdata_dir}/prof.profdata")
# Profile collection depends on java tools because we use cassandra-stress as the load.
add_custom_command(
  OUTPUT "${sampled_profdata}"
  DEPENDS
    scylla
    dist-tools-tar
  COMMAND ${CMAKE_COMMAND} -E rm -r "${profdata_dir}"
  COMMAND ${CMAKE_SOURCE_DIR}/pgo/train "$<TARGET_FILE:scylla>"
            ${sampled_profdata}
            ${CMAKE_BINARY_DIR}/pgo_datasets)
add_custom_target(sampled_profdata
  DEPENDS "${sampled_profdata}")

set(all_profdata_files ${sampled_profdata})
if(profdata_file)
  list(APPEND all_profdata_files "${profdata_file}")
endif()
set(merged_profdata "${profdata_dir}/merged.profdata")

find_program(LLVM_PROFDATA llvm-profdata
  REQUIRED)
add_custom_command(
  OUTPUT "${merged_profdata}"
  DEPENDS "${all_profdata_files}"
  COMMAND ${LLVM_PROFDATA} merge ${all_profdata_files} -output "${merged_profdata}")
add_custom_target(merged_profdata
  DEPENDS "${merged_profdata}")
