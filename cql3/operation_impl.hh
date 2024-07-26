/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "operation.hh"
#include "constants.hh"

namespace cql3 {

class operation::set_value : public raw_update {
protected:
    expr::expression _value;
public:
    set_value(expr::expression value) : _value(std::move(value)) {}

    virtual ::shared_ptr <operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;

#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s", column, value);
        }
#endif

    virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const override;
};

class operation::set_counter_value_from_tuple_list : public set_value {
public:
    using set_value::set_value;
    ::shared_ptr <operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override;
};

class operation::column_deletion : public raw_deletion {
private:
    ::shared_ptr<column_identifier::raw> _id;
public:
    column_deletion(::shared_ptr<column_identifier::raw> id)
        : _id(std::move(id))
    { }

    virtual const column_identifier::raw& affected_column() const override {
        return *_id;
    }

    virtual ::shared_ptr<operation> prepare(data_dictionary::database db, const sstring& keyspace, const column_definition& receiver) const override {
        // No validation, deleting a column is always "well typed"
        return ::make_shared<constants::deleter>(receiver);
    }
};

}
