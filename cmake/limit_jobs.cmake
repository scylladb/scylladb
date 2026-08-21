if(NOT DEFINED Scylla_PARALLEL_LINK_JOBS)
  if(NOT DEFINED Scylla_RAM_PER_LINK_JOB)
    # preserve user-provided value
    set(_default_ram_value 4096)
    if(Scylla_ENABLE_LTO)
      # When ThinLTO optimization is enabled, the linker uses all available CPU threads.
      # To prevent excessive memory usage, we limit parallel link jobs based on available RAM,
      # as each link job requires significant memory during optimization.
      set(_default_ram_value 16384)
    endif()
    set(Scylla_RAM_PER_LINK_JOB ${_default_ram_value} CACHE STRING
      "Maximum amount of memory used by each link job (in MiB)")
  endif()
  cmake_host_system_information(
    RESULT _total_mem_mb
    QUERY AVAILABLE_PHYSICAL_MEMORY)
  math(EXPR _link_pool_depth "${_total_mem_mb} / ${Scylla_RAM_PER_LINK_JOB}")
  # Use 2 parallel link jobs to optimize build throughput. The main executable requires
  # LTO (slower link phase) while tests are linked without LTO (faster link phase).
  # This allows simultaneous linking of LTO and non-LTO targets, enabling better CPU
  # utilization by overlapping the slower LTO link with faster test links.
  if(_link_pool_depth LESS 2)
    set(_link_pool_depth 2)
  endif()

  set(Scylla_PARALLEL_LINK_JOBS "${_link_pool_depth}" CACHE STRING
    "Maximum number of concurrent link jobs")
endif()

set_property(
  GLOBAL
  APPEND
  PROPERTY JOB_POOLS
    link_pool=${Scylla_PARALLEL_LINK_JOBS}
    submodule_pool=1)
set(CMAKE_JOB_POOL_LINK link_pool)
