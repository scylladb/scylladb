/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "resource.hh"
#include "core/align.hh"
#include <hwloc.h>
#include <unordered_map>

cpu_set_t cpuid_to_cpuset(unsigned cpuid) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuid, &cs);
    return cs;
}

namespace resource {

size_t div_roundup(size_t num, size_t denom) {
    return (num + denom - 1) / denom;
}

std::vector<cpu> allocate(configuration c) {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);
    auto machine_depth = hwloc_get_type_depth(topology, HWLOC_OBJ_MACHINE);
    assert(hwloc_get_nbobjs_by_depth(topology, machine_depth) == 1);
    auto machine = hwloc_get_obj_by_depth(topology, machine_depth, 0);
    auto available_memory = machine->memory.total_memory;
    available_memory -= c.reserve_memory.value_or(256 << 20);
    size_t mem = c.total_memory.value_or(available_memory);
    if (mem > available_memory) {
        throw std::runtime_error("insufficient physical memory");
    }
    auto available_procs = hwloc_get_nbobjs_by_depth(topology, HWLOC_OBJ_PU);
    unsigned procs = c.cpus.value_or(available_procs);
    if (procs > available_procs) {
        throw std::runtime_error("insufficient processing units");
    }
    auto mem_per_proc = align_down<size_t>(mem / procs, 2 << 20);
    std::vector<hwloc_cpuset_t> cpu_sets{procs};
    auto root = hwloc_get_root_obj(topology);
    hwloc_distribute(topology, root, cpu_sets.data(), cpu_sets.size(), INT_MAX);
    std::vector<cpu> ret;
    std::unordered_map<hwloc_obj_t, size_t> topo_used_mem;
    for (auto&& cs : cpu_sets) {
        auto cpu_id = hwloc_bitmap_first(cs);
        assert(cpu_id != -1);
        auto pu = hwloc_get_pu_obj_by_os_index(topology, cpu_id);
        // walk up the hierarchy, stealing local memory as we find it.
        auto obj = pu;
        cpu this_cpu;
        this_cpu.cpu_id = cpu_id;
        size_t remain = mem_per_proc;
        while (remain && obj) {
            auto mem_in_obj = obj->memory.local_memory - topo_used_mem[obj];
            auto taken = std::min(mem_in_obj, remain);
            if (taken) {
                topo_used_mem[obj] += taken;
                auto node_id = hwloc_bitmap_first(obj->nodeset);
                assert(node_id != -1);
                this_cpu.mem.push_back({taken, unsigned(node_id)});
                remain -= taken;
            }
            obj = obj->parent;
        }
        assert(!remain);
        ret.push_back(std::move(this_cpu));
    }
    return ret;
}

unsigned nr_processing_units() {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);
    return hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
}

}
