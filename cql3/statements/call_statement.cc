/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/orcall 
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
#include "cql3/statements/call_statement.hh"
#include "cql3/statements/raw/call_statement.hh"
#include "db/config.hh"
#include "database.hh"
#include "transport/messages/result_message.hh"
#include "db/builtin_routine.hh"
#include "seastar/coroutine/maybe_yield.hh"

namespace cql3::statements {

call_statement::call_statement(::shared_ptr<cf_name> cf_name,
            seastar::shared_ptr<db::builtin_routine> proc,
            uint32_t bound_terms,
            std::unordered_map<sstring, sstring> params,
            cql_stats& stats)
    // todo: read timeout, write timeout, or ddl tiemout,
    // depending on call body
    : cql_statement_opt_metadata(&timeout_config::write_timeout)
    , _cf_name(std::move(cf_name))
    , _proc(proc)
    , _bound_terms{bound_terms}
    , _params(std::move(params))
    , _stats(stats)
    // todo: correct ks_selector for user-defined procedures
    , _ks_sel(ks_selector::SYSTEM) {

    if (_proc->has_output()) {
        std::vector<lw_shared_ptr<column_specification>> columns;
        for (auto&& col : _proc->metadata) {
            // Add the only supported result column to result set metadata
            auto result_column = make_lw_shared<cql3::column_specification>(_cf_name->get_keyspace(),
                _cf_name->get_column_family(), make_shared<cql3::column_identifier>(col.first, false),
                col.second);

            columns.push_back(std::move(result_column));
        }
        _metadata = seastar::make_shared<cql3::metadata>(std::move(columns));
    }
}

uint32_t call_statement::get_bound_terms() const {
    return _bound_terms;
}

future<> call_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const {
    // todo: check auth::CALL permission
    return make_ready_future<>();
}

void
call_statement::validate(service::storage_proxy&, const service::client_state& state) const {
    // todo
}

bool call_statement::depends_on_column_family(const sstring& cf_name) const {
    return false;
}

bool call_statement::depends_on_keyspace(const sstring& ks_name) const {
    // todo : return true if ks_name = procedure keyspace
    return false;
}

future<::shared_ptr<cql_transport::messages::result_message>>
call_statement::execute(query_processor& qp, service::query_state& qs, const query_options& options) const {

    inc_cql_stats(qs.get_client_state().is_internal());

    db::builtin_routine::parameter_map params(_params.begin(), _params.end());

    auto output = co_await _proc->call(std::move(params));

    using rm = cql_transport::messages::result_message;
    using srm = ::shared_ptr<rm>;

    if (_proc->has_output()) {
        auto result_set = std::make_unique<cql3::result_set>(_metadata);
        for (auto &&row : output) {
            std::vector<bytes_opt> rs_row;
            rs_row.reserve(row.size());

            for (size_t i = 0; i < row.size(); ++i) {
                rs_row.emplace_back(_proc->metadata[i].second->decompose(row[i]));
            }

            co_await coroutine::maybe_yield();

            // FIXME: Large datasets can cause issues
            result_set->add_row(std::move(rs_row));
        }

        cql3::result result(std::move(result_set));
        co_return seastar::make_shared<rm::rows>(std::move(result));
    } else {
        co_return srm{};
    }
}

void call_statement::inc_cql_stats(bool is_internal) const {
    // todo: proper stats
}

namespace raw {

std::unique_ptr<prepared_statement>
call_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    auto& prepared_context = get_prepare_context();
    seastar::shared_ptr<db::builtin_routine> proc;

    if (_cf_name->get_keyspace() == db::system_keyspace_name()) {
        proc = db.real_database().get_builtin_routine_registry().find_routine(_cf_name->get_column_family());
    }

    if (!proc) {
        throw exceptions::invalid_request_exception(format("Procedure {} was not found", _cf_name));
    }

    // We move the params out, therefore invalidating raw call_statement object
    auto stmt = seastar::make_shared<cql3::statements::call_statement>(seastar::make_shared<cf_name>(_cf_name.value()), proc,
        prepared_context.bound_variables_size(), std::move(_params), stats);
    return std::make_unique<prepared_statement>(std::move(stmt), prepared_context, std::vector<uint16_t>{});
}

call_statement::call_statement(cf_name name,
            std::unordered_map<sstring, sstring> params)
        : cf_statement{std::move(name)}
        , _params{std::move(params)}
{ }

}  // end of namespace raw

} // end of namespace cql3::statements
