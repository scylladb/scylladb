/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace db {

// A listed segment is missing, a sealed generation is incomplete, or a manifest
// file cannot be parsed.
class commitlog_manifest_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// One per segment_manager. Tracks the segment files this shard owns and, once
// enabled, mirrors the set to <dir>/<prefix>manifest-<generation>/<shard>. A name is
// listed before the segment can hold data and unlisted before the file is unlinked or recycled.
class commitlog_manifest {
public:
    struct verify_result {
        // A shard file or record was read. Empty generation directories are in
        // old_generations, so they are dropped and never reused, but found stays false.
        bool found = false;
        std::vector<uint64_t> old_generations;
        uint64_t next_generation = 1;
    };

    commitlog_manifest(sstring dir, std::string fname_prefix, unsigned shard);

    // Adds a basename. Returns the durable write when persisting, a ready future otherwise.
    future<> add(sstring basename);
    // Removes basenames. Writes once, only when the set changed and persisting.
    // On a failed write the names stay listed, since the caller keeps the files.
    future<> remove(std::vector<sstring> basenames);
    // Creates the generation directory, starts persisting and writes the current set.
    future<> enable(uint64_t generation);
    // Writes <generation dir>/record. Call on the shard that drops the old
    // generations, after every shard wrote its file.
    future<> seal(unsigned shard_count);

    bool enabled() const {
        return _persist;
    }
    bool sealed() const {
        return _sealed;
    }
    uint64_t generation() const {
        return _generation;
    }

    // Reads every generation in dir and checks completeness and that every listed
    // segment is in on_disk_basenames. Throws commitlog_manifest_error, or logs at
    // error level and continues when ignore_errors.
    static future<verify_result> verify(sstring dir, std::string fname_prefix, std::vector<sstring> on_disk_basenames, bool ignore_errors);
    // Removes record first, so a partially removed generation reads as unsealed.
    static future<> drop_generations(sstring dir, std::string fname_prefix, std::vector<uint64_t> gens);
    static sstring generation_dir(const sstring& dir, const std::string& fname_prefix, uint64_t gen);

private:
    sstring _dir;
    std::string _fname_prefix;
    unsigned _shard;
    std::set<sstring> _segments;
    bool _persist = false;
    bool _sealed = false;
    uint64_t _generation = 0;
    seastar::semaphore _write_sem{1};

    future<> write();
};

}
