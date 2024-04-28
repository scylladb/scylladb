/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/ut_name.hh"
#include "data_dictionary/data_dictionary.hh"
#include "schema/schema.hh"

namespace cql3 {

class query_processor;

namespace statements {

class alter_type_statement : public schema_altering_statement {
protected:
    ut_name _name;
public:
    alter_type_statement(const ut_name& name);

    virtual void prepare_keyspace(const service::client_state& state) override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual const sstring& keyspace() const override;


    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;

    class add_or_alter;
    class renames;
protected:
    virtual user_type make_updated_type(data_dictionary::database db, user_type to_update) const = 0;
private:
    future<std::vector<mutation>> prepare_announcement_mutations(service::storage_proxy& sp, api::timestamp_type) const;
};

class alter_type_statement::add_or_alter : public alter_type_statement {
    bool _is_add;
    shared_ptr<column_identifier> _field_name;
    shared_ptr<cql3_type::raw> _field_type;
public:
    add_or_alter(const ut_name& name, bool is_add,
                 const shared_ptr<column_identifier> field_name,
                 const shared_ptr<cql3_type::raw> field_type);
    virtual user_type make_updated_type(data_dictionary::database db, user_type to_update) const override;
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
private:
    user_type do_add(data_dictionary::database db, user_type to_update) const;
    user_type do_alter(data_dictionary::database db, user_type to_update) const;
};


class alter_type_statement::renames : public alter_type_statement {
    using renames_type = std::vector<std::pair<shared_ptr<column_identifier>,
                                               shared_ptr<column_identifier>>>;
    renames_type _renames;
public:
    renames(const ut_name& name);

    void add_rename(shared_ptr<column_identifier> previous_name, shared_ptr<column_identifier> new_name);

    virtual user_type make_updated_type(data_dictionary::database db, user_type to_update) const override;
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}

}
