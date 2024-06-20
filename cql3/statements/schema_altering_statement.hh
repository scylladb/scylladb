/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "transport/messages_fwd.hh"
#include "transport/event.hh"

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/cql_statement.hh"

#include <seastar/core/shared_ptr.hh>

#include "service/raft/raft_group0_client.hh"

class mutation;

namespace cql3 {

class query_processor;

namespace statements {

namespace messages = cql_transport::messages;

/**
 * Abstract class for statements that alter the schema.
 */
class schema_altering_statement : public raw::cf_statement, public cql_statement_no_metadata {
private:
    const bool _is_column_family_level;

protected:
    explicit schema_altering_statement(timeout_config_selector timeout_selector = &timeout_config::other_timeout);

    schema_altering_statement(cf_name name, timeout_config_selector timeout_selector = &timeout_config::other_timeout);
    
    virtual bool needs_guard(query_processor& qp, service::query_state& state) const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual uint32_t get_bound_terms() const override;

    virtual void prepare_keyspace(const service::client_state& state) override;

    virtual future<::shared_ptr<messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

public:
    /**
     * When a new data_dictionary::database object (keyspace, table) is created, the creator needs to be granted all applicable
     * permissions on it.
     *
     * By default, this function does nothing.
     */
    virtual future<> grant_permissions_to_creator(const service::client_state&, service::group0_batch&) const;

    using event_t = cql_transport::event::schema_change;
    virtual future<std::tuple<::shared_ptr<event_t>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const;
    virtual future<std::tuple<::shared_ptr<event_t>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const;
};

}

}
