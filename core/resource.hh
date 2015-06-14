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

#ifndef RESOURCE_HH_
#define RESOURCE_HH_

#include <cstdlib>
#include <experimental/optional>
#include <vector>
#include <set>

cpu_set_t cpuid_to_cpuset(unsigned cpuid);

namespace resource {

using std::experimental::optional;

using cpuset = std::set<unsigned>;

struct configuration {
    optional<size_t> total_memory;
    optional<size_t> reserve_memory;  // if total_memory not specified
    optional<size_t> cpus;
    optional<cpuset> cpu_set;
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
