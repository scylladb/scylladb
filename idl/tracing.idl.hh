/*
 * Copyright 2016 ScyllaDB
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

namespace tracing {
enum class trace_type : uint8_t {
    NONE,
    QUERY,
    REPAIR,
};

class span_id {
    uint64_t get_id();
};

class trace_info {
    utils::UUID session_id;
    tracing::trace_type type;
    bool write_on_close;
    tracing::trace_state_props_set state_props [[version 1.4]];
    uint32_t slow_query_threshold_us [[version 1.4]];
    uint32_t slow_query_ttl_sec [[version 1.4]];
    tracing::span_id parent_id [[version 1.8]]; /// RPC sender's tracing session span ID
};
}

