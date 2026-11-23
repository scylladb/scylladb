/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"
#include "schema/schema_fwd.hh"

namespace cql3 {

class query_processor;

namespace statements {

// DELETE ALL DATA FROM <table> [USING TIMESTAMP <timestamp> [AND TIMEOUT <duration>]]
//
// Deletes all the data of a tablet table written at or before the statement's timestamp, like a DELETE
// of every partition of the table, and keeps the data written after it. Unlike TRUNCATE, which removes
// the table's data physically, it is a tombstone: the table's truncation tombstone, which is kept in
// the tablet metadata rather than in the data, so the statement goes through group 0 and has no
// consistency level. See docs/dev/delete-all-data.md.
class delete_all_data_statement : public cql_statement {
    schema_ptr _schema;
    const std::unique_ptr<attributes> _attrs;
    const uint32_t _bound_terms;
public:
    delete_all_data_statement(schema_ptr schema, std::unique_ptr<attributes> prepared_attrs, uint32_t bound_terms);
    delete_all_data_statement(const delete_all_data_statement&);

    const sstring& keyspace() const;

    const sstring& column_family() const;

    virtual uint32_t get_bound_terms() const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    // Like TRUNCATE, this is not something a driver runs on its control connection.
    bool should_reclassify_control_connection() const override {
        return true;
    }

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
private:
    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;
};

}

}
