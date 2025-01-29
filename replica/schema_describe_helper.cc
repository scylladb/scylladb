/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "replica/schema_describe_helper.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/secondary_index_manager.hh"
#include "schema/schema.hh"
#include "view_info.hh"
#include "replica/database.hh"

namespace replica {

::schema_describe_helper make_schema_describe_helper(schema_ptr schema, const data_dictionary::database& db) {
    ::schema_describe_helper h{
        .type = ::schema_describe_helper::type::table,
    };
    if (schema->is_view()) {
        auto table = db.find_column_family(schema->view_info()->base_id());
        h.base_schema = table.schema();
        if (table.get_index_manager().is_index(*schema)) {
            h.type = ::schema_describe_helper::type::index;
            h.is_global_index = table.get_index_manager().is_global_index(*schema);
        } else {
            h.type = ::schema_describe_helper::type::view;
        }
    }
    return h;
}

::schema_describe_helper make_schema_describe_helper(const global_table_ptr& table_shards) {
    ::schema_describe_helper h{
        .type = ::schema_describe_helper::type::table,
    };
    auto& schema = *table_shards->schema();
    if (schema.is_view()) {
        h.base_schema = table_shards.base().schema();
        if (table_shards->get_index_manager().is_index(schema)) {
            h.type = ::schema_describe_helper::type::index;
            h.is_global_index = table_shards->get_index_manager().is_global_index(schema);
        } else {
            h.type = ::schema_describe_helper::type::view;
        }
    }
    return h;
}

} // namespace replica
