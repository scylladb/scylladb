/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once


#include <seastar/core/shared_ptr.hh>
#include "shared_sstable.hh"

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
    uint64_t position = 0;
    uint64_t total_read_size = 0;
};

class read_monitor {
public:
    virtual ~read_monitor() { }
    // parameters are the current position in the data file
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
