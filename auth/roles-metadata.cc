/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "auth/roles-metadata.hh"

#include <boost/algorithm/cxx11/any_of.hpp>
#include <seastar/core/print.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include "auth/common.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"

namespace auth {

namespace meta {

namespace roles_table {

std::string_view creation_query() {
    static const sstring instance = sprint(
            "CREATE TABLE %s ("
            "  %s text PRIMARY KEY,"
            "  can_login boolean,"
            "  is_superuser boolean,"
            "  member_of set<text>,"
            "  salted_hash text"
            ")",
            qualified_name,
            role_col_name);

    return instance;
}

constexpr std::string_view qualified_name("system_auth.roles");

}

}

future<bool> default_role_row_satisfies(
        cql3::query_processor& qp,
        std::function<bool(const cql3::untyped_result_set_row&)> p) {
    static const sstring query = format("SELECT * FROM {} WHERE {} = ?",
            meta::roles_table::qualified_name,
            meta::roles_table::role_col_name);

    return do_with(std::move(p), [&qp](const auto& p) {
        return qp.execute_internal(
                query,
                db::consistency_level::ONE,
                {meta::DEFAULT_SUPERUSER_NAME},
                true).then([&qp, &p](::shared_ptr<cql3::untyped_result_set> results) {
            if (results->empty()) {
                return qp.execute_internal(
                        query,
                        db::consistency_level::QUORUM,
                        internal_distributed_query_state(),
                        {meta::DEFAULT_SUPERUSER_NAME},
                        true).then([&p](::shared_ptr<cql3::untyped_result_set> results) {
                    if (results->empty()) {
                        return make_ready_future<bool>(false);
                    }

                    return make_ready_future<bool>(p(results->one()));
                });
            }

            return make_ready_future<bool>(p(results->one()));
        });
    });
}

future<bool> any_nondefault_role_row_satisfies(
        cql3::query_processor& qp,
        std::function<bool(const cql3::untyped_result_set_row&)> p) {
    static const sstring query = format("SELECT * FROM {}", meta::roles_table::qualified_name);

    return do_with(std::move(p), [&qp](const auto& p) {
        return qp.execute_internal(
                query,
                db::consistency_level::QUORUM,
                internal_distributed_query_state()).then([&p](::shared_ptr<cql3::untyped_result_set> results) {
            if (results->empty()) {
                return false;
            }

            static const sstring col_name = sstring(meta::roles_table::role_col_name);

            return boost::algorithm::any_of(*results, [&p](const cql3::untyped_result_set_row& row) {
                const bool is_nondefault = row.get_as<sstring>(col_name) != meta::DEFAULT_SUPERUSER_NAME;
                return is_nondefault && p(row);
            });
        });
    });
}

}
