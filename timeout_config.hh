/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "db/timeout_clock.hh"

struct timeout_config {
    db::timeout_clock::duration read_timeout;
    db::timeout_clock::duration write_timeout;
    db::timeout_clock::duration range_read_timeout;
    db::timeout_clock::duration counter_write_timeout;
    db::timeout_clock::duration truncate_timeout;
    db::timeout_clock::duration cas_timeout;
    db::timeout_clock::duration other_timeout;
};

using timeout_config_selector = db::timeout_clock::duration (timeout_config::*);

extern const timeout_config infinite_timeout_config;

namespace db { class config; }
timeout_config make_timeout_config(const db::config& cfg);
