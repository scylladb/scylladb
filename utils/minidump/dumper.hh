/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

// Live minidumps: an ELF core of the running process, readable by gdb, which
// leaves out the bulk of user data (LSA memory, registered buffers, free
// memory) and stops each shard for well under a millisecond.
//
// Each shard is parked at a task boundary just long enough to save its
// registers and write-protect its memory with userfaultfd. The memory is then
// copied while the shard runs; a write to a block not copied yet makes the
// fault handler copy that block first. See docs/dev/minidump.md.
namespace minidump {

struct options {
    std::string path;
    // Rules are "<selector>" globs over memory categories, e.g. "lsa:memtable",
    // "tag:bloom_filter", "free", "small", "large", "map:/usr/lib64/*".
    // User includes are matched first, then user excludes, then the defaults.
    std::vector<std::string> include;
    std::vector<std::string> exclude;
    // Classify memory and report sizes without writing anything.
    bool dry_run = false;
    // Refuse dumps bigger than this fraction of the shards' memory.
    double max_size_ratio = 0.15;
    // Refuse dumps which would leave less than this fraction of the disk free.
    double min_free_disk_ratio = 0.1;
    // Memory for copies of the dump while it is written, taken outside
    // Scylla's own memory; 0 picks half of what the system can spare above
    // min_free_memory_bytes, up to 8GB. What doesn't fit is copied while
    // writing, which keeps it protected longer.
    size_t max_memory_bytes = 0;
    // Refuse the dump, or abort it, when the system (or this process's
    // cgroup) would have less free memory than this; 0 picks
    // max(512MB, 2% of RAM).
    size_t min_free_memory_bytes = 0;
    // Caps on blocking a shard, at least twice the worst seen on 16-vCPU x86_64 and
    // aarch64 nodes with 8GB shards under heavy load. Single events: abort
    // when stopping a shard for its snapshot, or a single write fault, takes
    // longer than this.
    unsigned max_park_us = 50000;
    unsigned max_fault_us = 50000;
    // Cumulative: abort when a shard's snapshot stop and write faults add up
    // to more than this.
    unsigned max_stall_us = 250000;
    // Abort when the copy takes longer than this.
    unsigned max_copy_seconds = 60;
    // Write rate limit in MB/s, 0 for none.
    unsigned write_rate_mb = 0;
    // CPUs for the dumper's threads, e.g. "26-31"; by default the CPUs no
    // shard runs on, or all of them.
    std::string helper_cpus;
};

struct result {
    // JSON report.
    seastar::sstring report;
    // False when the dump failed or was aborted, or a dry run failed.
    bool ok;
};

// Takes a minidump. Must be called on shard 0.
seastar::future<result> take(options opts);

}
