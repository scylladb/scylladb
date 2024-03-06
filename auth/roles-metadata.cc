/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
    static const sstring instance = fmt::format(
            "CREATE TABLE {}.{} ("
            "  {} text PRIMARY KEY,"
            "  can_login boolean,"
            "  is_superuser boolean,"
            "  member_of set<text>,"
            "  salted_hash text"
            ")",
            meta::legacy::AUTH_KS,
            name,
            role_col_name);

    return instance;
}

} // namespace roles_table

} // namespace meta

future<bool> default_role_row_satisfies(
        cql3::query_processor& qp,
        std::function<bool(const cql3::untyped_result_set_row&)> p,
        std::optional<std::string> rolename) {
    const sstring query = format("SELECT * FROM {}.{} WHERE {} = ?",
            get_auth_ks_name(qp),
            meta::roles_table::name,
            meta::roles_table::role_col_name);

    for (auto cl : { db::consistency_level::ONE, db::consistency_level::QUORUM }) {
        auto results = co_await qp.execute_internal(query, cl
            , internal_distributed_query_state()
            , {rolename.value_or(std::string(meta::DEFAULT_SUPERUSER_NAME))}
            , cql3::query_processor::cache_internal::yes
            );
        if (!results->empty()) {
            co_return p(results->one());
        }
    }
    co_return false;
}

future<bool> any_nondefault_role_row_satisfies(
        cql3::query_processor& qp,
        std::function<bool(const cql3::untyped_result_set_row&)> p,
        std::optional<std::string> rolename) {
    const sstring query = format("SELECT * FROM {}.{}", get_auth_ks_name(qp), meta::roles_table::name);

    auto results = co_await qp.execute_internal(query, db::consistency_level::QUORUM
        , internal_distributed_query_state(), cql3::query_processor::cache_internal::no
        );
    if (results->empty()) {
        co_return false;
    }
    static const sstring col_name = sstring(meta::roles_table::role_col_name);

    co_return boost::algorithm::any_of(*results, [&](const cql3::untyped_result_set_row& row) {
        auto superuser = rolename ? std::string_view(*rolename) : meta::DEFAULT_SUPERUSER_NAME;
        const bool is_nondefault = row.get_as<sstring>(col_name) != superuser;
        return is_nondefault && p(row);
    });
}

}
