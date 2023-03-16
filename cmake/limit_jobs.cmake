set(LINK_MEM_PER_JOB 4096 CACHE INTERNAL "Maximum memory used by each link job in (in MiB)")

cmake_host_system_information(
  RESULT _total_mem
  QUERY AVAILABLE_PHYSICAL_MEMORY)
math(EXPR _link_pool_depth "${_total_mem} / ${LINK_MEM_PER_JOB}")
if(_link_pool_depth EQUAL 0)
  set(_link_pool_depth 1)
endif()
set_property(
  GLOBAL
  APPEND
  PROPERTY JOB_POOLS
    link_pool=${_link_pool_depth}
    submodule_pool=1)
set(CMAKE_JOB_POOL_LINK link_pool)
