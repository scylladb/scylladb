/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/cf_name.hh"

#include <optional>

#include "parsed_statement.hh"

namespace service { class client_state; }

namespace cql3 {

namespace statements {

namespace raw {

/**
 * Abstract class for statements that apply on a given column family.
 */
class cf_statement : public parsed_statement {
protected:
    std::optional<cf_name> _cf_name;

    cf_statement(std::optional<cf_name> cf_name);
public:
    virtual void prepare_keyspace(const service::client_state& state);

    // Only for internal calls, use the version with ClientState for user queries
    void prepare_keyspace(std::string_view keyspace);

    virtual bool has_keyspace() const;

    virtual const sstring& keyspace() const;

    virtual const sstring& column_family() const;
};

}

}

}
