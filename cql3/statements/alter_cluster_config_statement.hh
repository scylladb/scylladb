/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>

#include <seastar/core/sstring.hh>

#include "cql3/statements/schema_altering_statement.hh"
#include "utils/UUID.hh"

namespace cql3 {

class query_processor;

namespace statements {

class alter_cluster_config_statement final : public schema_altering_statement {
public:
    enum class scope {
        cluster,
        datacenter,
        rack,
        node,
    };

    // The value an option is given. A structured option may be given a CQL literal rather
    // than a string; that arrives here already rendered as the text the option's codec
    // reads, and is told apart so that an option that is not structured can refuse it,
    // rather than store the text of a map as though it had been written as a string.
    struct config_value {
        sstring text;
        bool written_as_literal = false;
    };

private:
    scope _scope;
    std::optional<sstring> _dc_name;
    std::optional<sstring> _rack_name;
    std::optional<utils::UUID> _node_uuid;
    sstring _config_name;
    std::optional<config_value> _value;

public:
    alter_cluster_config_statement(scope statement_scope, std::optional<sstring> dc_name, std::optional<sstring> rack_name, std::optional<utils::UUID> node_uuid, sstring config_name, std::optional<config_value> value);

    static std::unique_ptr<alter_cluster_config_statement> for_cluster(sstring config_name, std::optional<config_value> value);
    static std::unique_ptr<alter_cluster_config_statement> for_datacenter(sstring dc_name, sstring config_name, std::optional<config_value> value);
    static std::unique_ptr<alter_cluster_config_statement> for_rack(sstring dc_name, sstring rack_name, sstring config_name, std::optional<config_value> value);
    // `node_uuid` is the text of the grammar's UUID token, so it always parses.
    static std::unique_ptr<alter_cluster_config_statement> for_node(const sstring& node_uuid, sstring config_name, std::optional<config_value> value);

    future<> check_access(query_processor& qp, const service::client_state& state) const override;
    void validate(query_processor& qp, const service::client_state& state) const override;
    future<std::tuple<::shared_ptr<event_t>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const override;
    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) override;

protected:
    audit::audit_info_ptr audit_info() const override {
        return audit::audit::create_audit_info(category(), sstring(), sstring());
    }
};

}

}
