/*
 * Copyright (C) 2015 ScyllaDB
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

#include <algorithm>
#include "local_strategy.hh"
#include "utils/class_registrator.hh"
#include "utils/fb_utilities.hh"


namespace locator {

local_strategy::local_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options) :
        abstract_replication_strategy(keyspace_name, token_metadata, snitch, config_options, replication_strategy_type::local) {}

std::vector<inet_address> local_strategy::get_natural_endpoints(const token& t) {
    return calculate_natural_endpoints(t, _token_metadata);
}

std::vector<inet_address> local_strategy::calculate_natural_endpoints(const token& t, token_metadata& tm) const {
    return std::vector<inet_address>({utils::fb_utilities::get_broadcast_address()});
}

void local_strategy::validate_options() const {
}

std::experimental::optional<std::set<sstring>> local_strategy::recognized_options() const {
    // LocalStrategy doesn't expect any options.
    return {};
}

size_t local_strategy::get_replication_factor() const {
    return 1;
}

using registry = class_registrator<abstract_replication_strategy, local_strategy, const sstring&, token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
