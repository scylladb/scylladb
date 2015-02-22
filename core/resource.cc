/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "resource.hh"
#include "core/align.hh"

#ifdef HAVE_HWLOC

#include "util/defer.hh"
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

static unsigned find_memory_depth(hwloc_topology_t& topology) {
    auto obj = hwloc_get_pu_obj_by_os_index(topology, 0);
    auto depth = hwloc_get_type_depth(topology, HWLOC_OBJ_PU);

    while (!obj->memory.local_memory && obj) {
        obj = hwloc_get_ancestor_obj_by_depth(topology, --depth, obj);
    }
    assert(obj);
    return depth;
}

static size_t alloc_from_node(cpu& this_cpu, hwloc_obj_t node, std::unordered_map<hwloc_obj_t, size_t>& used_mem, size_t alloc) {
    auto taken = std::min(node->memory.local_memory - used_mem[node], alloc);
    if (taken) {
        used_mem[node] += taken;
        auto node_id = hwloc_bitmap_first(node->nodeset);
        assert(node_id != -1);
        this_cpu.mem.push_back({taken, unsigned(node_id)});
    }
    return taken;
}

std::vector<cpu> allocate(configuration c) {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    auto free_hwloc = defer([&] { hwloc_topology_destroy(topology); });
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
    unsigned available_procs = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
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
    std::vector<std::pair<cpu, size_t>> remains;
    size_t remain;
    unsigned depth = find_memory_depth(topology);

    // Divide local memory to cpus
    for (auto&& cs : cpu_sets) {
        auto cpu_id = hwloc_bitmap_first(cs);
        assert(cpu_id != -1);
        auto pu = hwloc_get_pu_obj_by_os_index(topology, cpu_id);
        auto node = hwloc_get_ancestor_obj_by_depth(topology, depth, pu); 
        cpu this_cpu;
        this_cpu.cpu_id = cpu_id;
        remain = mem_per_proc - alloc_from_node(this_cpu, node, topo_used_mem, mem_per_proc);

        remains.emplace_back(std::move(this_cpu), remain); 
    }

    // Divide the rest of the memory
    for (auto&& r : remains) {
        cpu this_cpu;
        size_t remain; 
        std::tie(this_cpu, remain) = r;
        auto pu = hwloc_get_pu_obj_by_os_index(topology, this_cpu.cpu_id);
        auto node = hwloc_get_ancestor_obj_by_depth(topology, depth, pu); 
        auto obj = node;

        while (remain) {
            remain -= alloc_from_node(this_cpu, obj, topo_used_mem, remain);
            do {
                obj = hwloc_get_next_obj_by_depth(topology, depth, obj);
            } while (!obj);
            if (obj == node)
                break;
        }
        assert(!remain);
        ret.push_back(std::move(this_cpu));
    }
    return ret;
}

unsigned nr_processing_units() {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    auto free_hwloc = defer([&] { hwloc_topology_destroy(topology); });
    hwloc_topology_load(topology);
    return hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
}

}

#else

#include "resource.hh"
#include <unistd.h>

namespace resource {

std::vector<cpu> allocate(configuration c) {
    auto available_memory = ::sysconf(_SC_PAGESIZE) * size_t(::sysconf(_SC_PHYS_PAGES));
    available_memory -= c.reserve_memory.value_or(256 << 20);
    size_t mem = c.total_memory.value_or(available_memory);
    if (mem > available_memory) {
        throw std::runtime_error("insufficient physical memory");
    }
    auto procs = c.cpus.value_or(nr_processing_units());
    std::vector<cpu> ret;
    ret.reserve(procs);
    for (unsigned i = 0; i < procs; ++i) {
        ret.push_back(cpu{i, {{mem / procs, 0}}});
    }
    return ret;
}

unsigned nr_processing_units() {
    return ::sysconf(_SC_NPROCESSORS_ONLN);
}

}

#endif
