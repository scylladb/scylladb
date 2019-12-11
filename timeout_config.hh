/*
 * Copyright (C) 2018 ScyllaDB
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
#include "db/config.hh"
#include "utils/updateable_value.hh"

struct timeout_config {
    utils::updateable_value<std::chrono::milliseconds> read_timeout;
    utils::updateable_value<std::chrono::milliseconds> write_timeout;
    utils::updateable_value<std::chrono::milliseconds> range_read_timeout;
    utils::updateable_value<std::chrono::milliseconds> counter_write_timeout;
    utils::updateable_value<std::chrono::milliseconds> truncate_timeout;
    utils::updateable_value<std::chrono::milliseconds> cas_timeout;
    utils::updateable_value<std::chrono::milliseconds> other_timeout;
};

using timeout_config_selector = utils::updateable_value<std::chrono::milliseconds> (timeout_config::*);

extern const timeout_config infinite_timeout_config;
