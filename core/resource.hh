/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

#ifndef RESOURCE_HH_
#define RESOURCE_HH_

#include <cstdlib>
#include <experimental/optional>
#include <vector>

cpu_set_t cpuid_to_cpuset(unsigned cpuid);

namespace resource {

using std::experimental::optional;

struct configuration {
    optional<size_t> total_memory;
    optional<size_t> reserve_memory;  // if total_memory not specified
    optional<size_t> cpus;
};

struct memory {
    size_t bytes;
    unsigned nodeid;

};

struct cpu {
    unsigned cpu_id;
    std::vector<memory> mem;
};

std::vector<cpu> allocate(configuration c);
unsigned nr_processing_units();

}

#endif /* RESOURCE_HH_ */
