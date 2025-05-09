/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index_manager.hh"
#include "schema/schema.hh"
#include <seastar/core/sstring.hh>
#include <optional>

namespace replica {

class schema_describe_helper : public ::schema_describe_helper {
    data_dictionary::database _db;
public:
    explicit schema_describe_helper(data_dictionary::database db) : _db(db) { }

    virtual bool is_global_index(const table_id& base_id, const schema& view_s) const override {
        return  _db.find_column_family(base_id).get_index_manager().is_global_index(view_s);
    }

    virtual bool is_index(const table_id& base_id, const schema& view_s) const override {
        return  _db.find_column_family(base_id).get_index_manager().is_index(view_s);
    }

    virtual std::optional<sstring> custom_index_class(const table_id& base_id, const schema& view_s) const override {
        return  _db.find_column_family(base_id).get_index_manager().custom_index_class(view_s);
    }

    virtual schema_ptr find_schema(const table_id& id) const override {
        return _db.find_schema(id);
    }
};

} // namespace data_dictionary
