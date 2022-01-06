/*
 * Copyright 2022-present ScyllaDB
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

namespace service {

struct schema_change {
    std::vector<canonical_mutation> mutations;
};

struct group0_command {
    std::variant<service::schema_change> change;
    canonical_mutation history_append;

    std::optional<utils::UUID> prev_state_id;
    utils::UUID new_state_id;

    gms::inet_address creator_addr;
    raft::server_id creator_id;
};

} // namespace service
