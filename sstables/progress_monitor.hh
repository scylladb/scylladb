/*
 * Copyright (C) 2017 ScyllaDB
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
#include <seastar/core/shared_ptr_incomplete.hh>

namespace sstables {

struct writer_offset_tracker {
    uint64_t offset = 0;
};

class write_monitor {
public:
    virtual ~write_monitor() { }
    virtual void on_write_started(const writer_offset_tracker&) = 0;
    virtual void on_write_completed() = 0;
    virtual void on_flush_completed() = 0;
};

struct noop_write_monitor final : public write_monitor {
    virtual void on_write_started(const writer_offset_tracker&) { };
    virtual void on_write_completed() override { }
    virtual void on_flush_completed() override { }
};

write_monitor& default_write_monitor();
}
