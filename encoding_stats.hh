/*
 * Copyright (C) 2018 ScyllaDB
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

#include "timestamp.hh"

// Stores statistics on all the updates done to a memtable
// The collected statistics is used for flushing memtable to the disk
struct encoding_stats {

    // The fixed epoch corresponds to 2018-03-22, 00:00:00 GMT-0.
    // Encoding stats are used for delta-encoding, so we want some default values
    // that are just good enough so we take some recent date in the past
    static constexpr uint32_t deletion_time_epoch = 1521676800;
    static constexpr api::timestamp_type timestamp_epoch = deletion_time_epoch * 1000 * 1000;
    static constexpr uint32_t ttl_epoch = 0;

    api::timestamp_type min_timestamp = timestamp_epoch;
    uint32_t min_local_deletion_time = deletion_time_epoch;
    uint32_t min_ttl = ttl_epoch;
};

