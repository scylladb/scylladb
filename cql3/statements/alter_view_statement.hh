/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {

class query_processor;
class cf_name;

namespace statements {

/** An <code>ALTER MATERIALIZED VIEW</code> parsed from a CQL query statement. */
class alter_view_statement : public schema_altering_statement {
private:
    std::optional<cf_prop_defs> _properties;
    view_ptr prepare_view(data_dictionary::database db) const;
public:
    alter_view_statement(cf_name view_name, std::optional<cf_prop_defs> properties);

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}
}
