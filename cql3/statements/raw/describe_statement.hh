/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/functions/function_name.hh"
#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/cf_name.hh"

#include <memory>
#include <optional>
#include <seastar/core/sstring.hh>

namespace cql3 {

namespace statements {

class prepared_statement;

namespace raw {

class describe_statement : public parsed_statement {
public:
    enum class element_type {
        keyspace,
        type,
        function,
        aggregate,
        table,
        index,
        view
    };

private:
    using internals = bool_class<class internals_tag>;

    struct describe_cluster {};
    struct describe_schema {
        bool full_schema;
    };
    struct describe_keyspace {
        std::optional<sstring> keyspace;
        bool only_keyspace;
    };
    struct describe_listing {
        describe_statement::element_type element_type;
    };
    struct describe_element {
        describe_statement::element_type element_type;
        std::optional<sstring> keyspace;
        sstring name;
    };
    struct describe_generic {
        std::optional<sstring> keyspace;
        sstring name;
    };
    using describe_config = std::variant<
        describe_cluster,
        describe_schema,
        describe_keyspace,
        describe_listing,
        describe_element,
        describe_generic
    >;
   
private:
    describe_config _config;
    internals _with_internals = internals(false);

public:
    explicit describe_statement(describe_config config);
    void with_internals_details();

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    static std::unique_ptr<describe_statement> cluster();
    static std::unique_ptr<describe_statement> schema(bool full);
    static std::unique_ptr<describe_statement> keyspaces();
    static std::unique_ptr<describe_statement> keyspace(std::optional<sstring> keyspace, bool only);
    static std::unique_ptr<describe_statement> tables();
    static std::unique_ptr<describe_statement> table(const cf_name& cf_name);
    static std::unique_ptr<describe_statement> index(const cf_name& cf_name);
    static std::unique_ptr<describe_statement> view(const cf_name& cf_name);
    static std::unique_ptr<describe_statement> types();
    static std::unique_ptr<describe_statement> type(const ut_name& ut_name);
    static std::unique_ptr<describe_statement> functions();
    static std::unique_ptr<describe_statement> function(const functions::function_name& fn_name);
    static std::unique_ptr<describe_statement> aggregates();
    static std::unique_ptr<describe_statement> aggregate(const functions::function_name& fn_name);
    static std::unique_ptr<describe_statement> generic(std::optional<sstring> keyspace, const sstring& name);
};

}

}

}
