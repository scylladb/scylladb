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

void configure(std::vector<resource::memory> m,
        std::experimental::optional<std::string> hugetlbfs_path = {});

void* allocate_reclaimable(size_t size);

class reclaimer {
    std::function<void ()> _reclaim;
public:
    reclaimer(std::function<void ()> reclaim);
    ~reclaimer();
    void do_reclaim() { _reclaim(); }
};

// Call periodically to recycle objects that were freed
// on cpu other than the one they were allocated on.
//
// Returns @true if any work was actually performed.
bool drain_cross_cpu_freelist();

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
    uint64_t _mallocs;
    uint64_t _frees;
    uint64_t _cross_cpu_frees;
private:
    statistics(uint64_t mallocs, uint64_t frees, uint64_t cross_cpu_frees)
        : _mallocs(mallocs), _frees(frees), _cross_cpu_frees(cross_cpu_frees) {}
public:
    uint64_t mallocs() const { return _mallocs; }
    uint64_t frees() const { return _frees; }
    uint64_t cross_cpu_frees() const { return _cross_cpu_frees; }
    size_t live_objects() const { return mallocs() - frees(); }
    friend statistics stats();
};


}

#endif /* MEMORY_HH_ */
