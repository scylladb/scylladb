/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/statements/raw/modification_statement.hh"
#include "service/client_state.hh"

namespace cql3 {

namespace statements {

namespace raw {

class modification_statement;

class batch_statement : public raw::cf_statement {
public:
    enum class type {
        LOGGED, UNLOGGED, COUNTER
    };
private:
    type _type;
    std::unique_ptr<attributes::raw> _attrs;
    std::vector<std::unique_ptr<raw::modification_statement>> _parsed_statements;
public:
    batch_statement(
        type type_,
        std::unique_ptr<attributes::raw> attrs,
        std::vector<std::unique_ptr<raw::modification_statement>> parsed_statements)
            : cf_statement(std::nullopt)
            , _type(type_)
            , _attrs(std::move(attrs))
            , _parsed_statements(std::move(parsed_statements)) {
    }

    virtual void prepare_keyspace(const service::client_state& state) override {
        for (auto&& s : _parsed_statements) {
            s->prepare_keyspace(state);
        }
    }

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}

}
}
