/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_index_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "schema/schema_builder.hh"
#include "data_dictionary/data_dictionary.hh"
#include "mutation/mutation.hh"
#include "cql3/query_processor.hh"
#include "cql3/index_name.hh"

namespace cql3 {

namespace statements {

drop_index_statement::drop_index_statement(::shared_ptr<index_name> index_name, bool if_exists)
    : schema_altering_statement{index_name->get_cf_name(), &timeout_config::truncate_timeout}
    , _index_name{index_name->get_idx()}
    , _if_exists{if_exists}
{
}

const sstring& drop_index_statement::column_family() const
{
    return _cf_name ? *_cf_name :
            // otherwise -- the empty name stored by the superclass
            cf_statement::column_family();
}

future<> drop_index_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    auto cfm = lookup_indexed_table(qp);
    if (!cfm) {
        return make_ready_future<>();
    }
    return state.has_column_family_access(cfm->ks_name(), cfm->cf_name(), auth::permission::ALTER);
}

void drop_index_statement::validate(query_processor& qp, const service::client_state& state) const
{
    // validated in lookup_indexed_table()

    auto db = qp.db();
    if (db.has_keyspace(keyspace())) {
        auto schema = db.find_indexed_table(keyspace(), _index_name);
        if (schema) {
            _cf_name = schema->cf_name();
        }
    }
}

schema_ptr drop_index_statement::make_drop_idex_schema(query_processor& qp) const {
    auto cfm = lookup_indexed_table(qp);
    if (!cfm) {
        return nullptr;
    }
    ++_cql_stats->secondary_index_drops;
    auto builder = schema_builder(cfm);
    builder.without_index(_index_name);

    return builder.build();
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>>
drop_index_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;
    auto cfm = make_drop_idex_schema(qp);

    if (cfm) {
        m = co_await service::prepare_column_family_update_announcement(qp.proxy(), cfm, {}, ts);

        using namespace cql_transport;
        ret = ::make_shared<event::schema_change>(event::schema_change::change_type::UPDATED,
                                                 event::schema_change::target_type::TABLE,
                                                 cfm->ks_name(),
                                                 cfm->cf_name());
    }

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_index_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    _cql_stats = &stats;
    return std::make_unique<prepared_statement>(make_shared<drop_index_statement>(*this));
}

schema_ptr drop_index_statement::lookup_indexed_table(query_processor& qp) const
{
    auto db = qp.db();
    if (!db.has_keyspace(keyspace())) {
        throw exceptions::keyspace_not_defined_exception(format("Keyspace {} does not exist", keyspace()));
    }
    auto cfm = db.find_indexed_table(keyspace(), _index_name);
    if (cfm) {
        return cfm;
    }
    if (_if_exists) {
        return nullptr;
    }
    throw exceptions::invalid_request_exception(
            format("Index '{}' could not be found in any of the tables of keyspace '{}'", _index_name, keyspace()));
}

}

}
