/*
 * Copyright (C) 2020 ScyllaDB
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

#include "cql3/cql_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/stats.hh"
#include "cql3/cf_name.hh"

namespace db {
    struct builtin_routine;
}

namespace cql3::statements {

namespace raw { class call_statement; }

/*
 * CALL procedure();
 *
 * In case the procedure contains only DDL statements, returns no
 * metadata. If a procedure returns a result set, returns result
 * set metadata.
 */
class call_statement : public cql_statement_opt_metadata {
private:
    seastar::shared_ptr<cf_name> _cf_name;
    seastar::shared_ptr<db::builtin_routine> _proc;
    const uint32_t _bound_terms;
    std::unordered_map<sstring, sstring> _params;

    [[maybe_unused]] cql_stats& _stats;
    [[maybe_unused]] const ks_selector _ks_sel;
public:
    call_statement(
            seastar::shared_ptr<cf_name> cf_name,
            seastar::shared_ptr<db::builtin_routine> proc,
            uint32_t bound_terms,
            std::unordered_map<sstring, sstring> params,
            cql_stats& stats);

    virtual uint32_t get_bound_terms() const override;

    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;

    // Validate before execute, using client state and current schema
    void validate(service::storage_proxy&, const service::client_state& state) const override;

    virtual bool depends_on_keyspace(const sstring& ks_name) const override;

    virtual bool depends_on_column_family(const sstring& cf_name) const override;

    virtual future<seastar::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options) const override;

    friend class raw::call_statement;
private:
    void inc_cql_stats(bool is_internal) const;
};

} // end of namespace cql3::statements
