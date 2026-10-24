/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once


#include <seastar/core/shared_ptr.hh>
#include "shared_sstable.hh"
#include "sstables/sstable_position.hh"

namespace sstables {

struct writer_offset_tracker {
    uint64_t offset = 0;
};

class write_monitor {
public:
    virtual ~write_monitor() { }
    virtual void on_write_started(const writer_offset_tracker&) = 0;
    virtual void on_data_write_completed() = 0;
};

write_monitor& default_write_monitor();

struct reader_position_tracker {
    // The position of the first byte that has not been consumed yet.
    sstable_position position;
    // The number of uncompressed bytes consumed since the start of the stream.
    // Bytes passed over by skip(n) are counted. Moves by skip_to() are not
    // counted, so this is not a position in the data file. It is meant for
    // distances within one stream. Read monitors measure their
    // progress with `position` and read_progress_estimator instead.
    int64_t bytes_consumed = 0;
};

// Converts a reader position into read progress through the data file, in
// units of sstable::data_size(). Monitors compare it with data_size() totals.
//
// A logical position is an uncompressed offset, so it converts exactly. A
// physical position has no known uncompressed offset, because the chunk
// offsets may not be in memory. Its progress is the fraction of the compressed
// file before its chunk, scaled to data_size().
class read_progress_estimator {
    uint64_t _data_size;
    uint64_t _ondisk_data_size;
public:
    explicit read_progress_estimator(const sstable& sst);
    uint64_t operator()(sstable_position pos) const;
};

class read_monitor {
public:
    virtual ~read_monitor() { }
    // The tracker holds the reader's current position in the data file. It
    // stays valid until on_read_completed().
    virtual void on_read_started(const reader_position_tracker&) = 0;
    virtual void on_read_completed() = 0;
};

struct noop_read_monitor final : public read_monitor {
    virtual void on_read_started(const reader_position_tracker&) override {}
    virtual void on_read_completed() override {}
};

read_monitor& default_read_monitor();

struct read_monitor_generator {
    virtual read_monitor& operator()(shared_sstable sst) = 0;
    virtual ~read_monitor_generator() {}
};

struct no_read_monitoring final : public read_monitor_generator {
    virtual read_monitor& operator()(shared_sstable sst) override {
        return default_read_monitor();
    };
};

read_monitor_generator& default_read_monitor_generator();
}
