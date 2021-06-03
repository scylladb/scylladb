/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
