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
    // Rules are "<selector>" globs over memory categories, e.g. "lsa:memtable:ks.t",
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
    // Pre-touched area for blocks copied by the fault handlers; when it is
    // full, they use the copy buffer.
    size_t staging_bytes = 512 << 20;
    // Memory to hold copies of the dump while it is written; 0 picks
    // min(8GB, MemAvailable / 4). The more of the dump fits, the sooner all
    // protection is lifted.
    size_t copy_buffer_bytes = 0;
    // Abort when parking a shard takes longer than this.
    unsigned max_park_us = 5000;
    // Abort when a single write fault waits longer than this.
    unsigned max_fault_us = 5000;
    // Abort when a shard's write faults wait longer than this in total.
    unsigned max_stall_us = 30000;
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
