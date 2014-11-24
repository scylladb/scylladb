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

class statistics;
statistics stats();

class statistics {
    uint64_t _objects_allocated;
    uint64_t _objects_freed;
private:
    statistics(uint64_t allocs, uint64_t frees)
        : _objects_allocated(allocs), _objects_freed(frees) {}
public:
    uint64_t object_allocated() const { return _objects_allocated; }
    uint64_t object_freed() const { return _objects_freed; }
    size_t allocated_objects() const { return object_allocated() - object_freed(); }
    friend statistics stats();
};


}

#endif /* MEMORY_HH_ */
