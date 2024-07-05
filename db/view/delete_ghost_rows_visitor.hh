/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

/* Copyright 2022-present ScyllaDB */

#pragma once

#include "query-result-reader.hh"
#include "replica/database_fwd.hh"
#include "db/timeout_clock.hh"

namespace service {
class storage_proxy;
class query_state;
}

namespace db::view {

class delete_ghost_rows_visitor {
    service::storage_proxy& _proxy;
    service::query_state& _state;
    db::timeout_clock::duration _timeout_duration;
    view_ptr _view;
    replica::table& _view_table;
    schema_ptr _base_schema;
    std::optional<partition_key> _view_pk;
public:
    delete_ghost_rows_visitor(service::storage_proxy& proxy, service::query_state& state, view_ptr view, db::timeout_clock::duration timeout_duration);

    void add_value(const column_definition& def, query::result_row_view::iterator_type& i) {
    }

    void accept_new_partition(const partition_key& key, uint32_t row_count);

    void accept_new_partition(uint32_t row_count) {
    }

    // Assumes running in seastar::thread
    void accept_new_row(const clustering_key& ck, const query::result_row_view& static_row, const query::result_row_view& row);

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
    }

    uint32_t accept_partition_end(const query::result_row_view& static_row) {
        return 0;
    }
};

} //namespace db::view
