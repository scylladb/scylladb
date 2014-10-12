/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef MEMORY_HH_
#define MEMORY_HH_

#include "resource.hh"
#include <new>
#include <functional>
#include <vector>

namespace memory {

void configure(std::vector<resource::memory> m);

void* allocate_reclaimable(size_t size);

class reclaimer {
    std::function<void ()> _reclaim;
public:
    reclaimer(std::function<void ()> reclaim);
    ~reclaimer();
    void do_reclaim() { _reclaim(); }
};

// We don't want the memory code calling back into the rest of
// the system, so allow the rest of the system to tell the memory
// code how to initiate reclaim.
//
// When memory is low, calling hook(fn) will result in fn being called
// in a safe place wrt. allocations.
void set_reclaim_hook(
        std::function<void (std::function<void ()>)> hook);

}

#endif /* MEMORY_HH_ */
