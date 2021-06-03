/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "service_level_statement.hh"
#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

uint32_t service_level_statement::get_bound_terms() const {
    return 0;
}

bool service_level_statement::depends_on_keyspace(
        const sstring &ks_name) const {
    return false;
}

bool service_level_statement::depends_on_column_family(
        const sstring &cf_name) const {
    return false;
}

void service_level_statement::validate(
        service::storage_proxy &,
        const service::client_state &state) const {
}

future<> service_level_statement::check_access(service::storage_proxy& sp, const service::client_state &state) const {
    return make_ready_future<>();
}

}
}
