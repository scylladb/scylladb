/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"

#include <seastar/core/shared_ptr.hh>
#include <optional>
#include <memory>

#include "schema/schema_fwd.hh"

namespace cql3 {

class query_processor;
class index_name;

namespace statements {

class drop_index_statement : public schema_altering_statement {
    sstring _index_name;

    // A "drop index" statement does not specify the base table's name, just an
    // index name. Nevertheless, the virtual column_family() method is supposed
    // to return a reasonable table name. If the index doesn't exist, we return
    // an empty name (this commonly happens with "if exists").
    mutable std::optional<sstring> _cf_name;
    bool _if_exists;
    cql_stats* _cql_stats = nullptr;
public:
    drop_index_statement(::shared_ptr<index_name> index_name, bool if_exists);

    virtual const sstring& column_family() const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual void validate(query_processor&, const service::client_state& state) const override;

    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
private:
    schema_ptr lookup_indexed_table(query_processor& qp) const;
    schema_ptr make_drop_idex_schema(query_processor& qp) const;
};

}

}
