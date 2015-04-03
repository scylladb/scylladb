/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/statements/select_statement.hh"

#include "transport/messages/result_message.hh"
#include "cql3/selection/selection.hh"
#include "core/shared_ptr.hh"
#include "query.hh"

namespace cql3 {

namespace statements {

const shared_ptr<select_statement::parameters> select_statement::_default_parameters = ::make_shared<select_statement::parameters>();

future<shared_ptr<transport::messages::result_message>>
select_statement::execute(service::storage_proxy& proxy, service::query_state& state, const query_options& options) {
    auto cl = options.get_consistency();

    validate_for_read(_schema->ks_name, cl);

    int32_t limit = get_limit(options);
    auto now = db_clock::now();

    auto command = ::make_lw_shared<query::read_command>(_schema->id(),
        _restrictions->get_partition_key_ranges(options), make_partition_slice(options), limit);

    int32_t page_size = options.get_page_size();

    // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
    // If we user provided a page_size we'll use that to page internally (because why not), otherwise we use our default
    // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
    if (_selection->is_aggregate() && page_size <= 0) {
        page_size = DEFAULT_COUNT_PAGE_SIZE;
    }

    warn(unimplemented::cause::PAGING);
    return execute(proxy, command, state, options, now);

#if 0
    if (page_size <= 0 || !command || !query_pagers::may_need_paging(command, page_size)) {
        return execute(proxy, command, state, options, now);
    }

    auto pager = query_pagers::pager(command, cl, state.get_client_state(), options.get_paging_state());

    if (selection->isAggregate()) {
        return page_aggregate_query(pager, options, page_size, now);
    }

    // We can't properly do post-query ordering if we page (see #6722)
    if (needs_post_query_ordering()) {
        throw exceptions::invalid_request_exception(
              "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
              " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");
    }

    return pager->fetch_page(page_size).then([this, pager, &options, limit, now] (auto page) {
        auto msg = process_results(page, options, limit, now);

        if (!pager->is_exhausted()) {
            msg->result->metadata->set_has_more_pages(pager->state());
        }

        return msg;
    });
#endif
}

future<shared_ptr<transport::messages::result_message>>
select_statement::execute(service::storage_proxy& proxy, lw_shared_ptr<query::read_command> cmd,
        service::query_state& state, const query_options& options, db_clock::time_point now) {
    return proxy.query(std::move(cmd), options.get_consistency())
        .then([this, &options, now, cmd] (auto result) {
            return this->process_results(std::move(result), cmd, options, now);
        });
}

// Implements ResultVisitor concept from query.hh
class result_set_building_visitor {
    cql3::selection::result_set_builder& builder;
    select_statement& stmt;
    uint32_t _row_count;
    std::vector<bytes> _partition_key;
    std::vector<bytes> _clustering_key;
public:
    result_set_building_visitor(cql3::selection::result_set_builder& builder, select_statement& stmt)
        : builder(builder)
        , stmt(stmt)
        , _row_count(0)
    { }

    void add_value(const column_definition& def, query::result_row_view::iterator_type& i) {
        if (def.type->is_multi_cell()) {
            auto cell = i.next_collection_cell();
            if (!cell) {
                builder.add_empty();
                return;
            }
            builder.add(def, *cell);
        } else {
            auto cell = i.next_atomic_cell();
            if (!cell) {
                builder.add_empty();
                return;
            }
            builder.add(def, *cell);
        }
    };

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        _partition_key = key.explode(*stmt._schema);
        _row_count = row_count;
    }

    void accept_new_partition(uint32_t row_count) {
        _row_count = row_count;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row,
            const query::result_row_view& row) {
        _clustering_key = key.explode(*stmt._schema);
        accept_new_row(static_row, row);
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        auto static_row_iterator = static_row.iterator();
        auto row_iterator = row.iterator();
        builder.new_row();
        for (auto&& def : stmt._selection->get_columns()) {
            switch (def->kind) {
                case column_definition::column_kind::PARTITION:
                    builder.add(_partition_key[def->component_index()]);
                    break;
                case column_definition::column_kind::CLUSTERING:
                    builder.add(_clustering_key[def->component_index()]);
                    break;
                case column_definition::column_kind::REGULAR:
                    add_value(*def, row_iterator);
                    break;
                case column_definition::column_kind::STATIC:
                    add_value(*def, static_row_iterator);
                    break;
                default:
                    assert(0);
            }
        }
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_row_count == 0
            && !static_row.empty()
            && !stmt._restrictions->uses_secondary_indexing()
            && stmt._restrictions->has_no_clustering_columns_restriction())
        {
            builder.new_row();
            auto static_row_iterator = static_row.iterator();
            for (auto&& def : stmt._selection->get_columns()) {
                if (def->is_partition_key()) {
                    builder.add(_partition_key[def->component_index()]);
                } else if (def->is_static()) {
                    add_value(*def, static_row_iterator);
                } else {
                    builder.add_empty();
                }
            }
        }
    }
};

shared_ptr<transport::messages::result_message>
select_statement::process_results(foreign_ptr<lw_shared_ptr<query::result>> results, lw_shared_ptr<query::read_command> cmd,
        const query_options& options, db_clock::time_point now) {
    cql3::selection::result_set_builder builder(*_selection, now, options.get_serialization_format());

    // FIXME: This special casing saves us the cost of copying an already
    // linearized response. When we switch views to scattered_reader this will go away.
    if (results->buf().is_linearized()) {
        query::result_view view(results->buf().view());
        view.consume(cmd->slice, result_set_building_visitor(builder, *this));
    } else {
        bytes_ostream w(results->buf());
        query::result_view view(w.linearize());
        view.consume(cmd->slice, result_set_building_visitor(builder, *this));
    }

    auto rs = builder.build();
    if (needs_post_query_ordering()) {
        rs->sort(_ordering_comparator);
    }
    if (_is_reversed) {
        rs->reverse();
    }
    rs->trim(cmd->row_limit);
    return ::make_shared<transport::messages::result_message::rows>(std::move(rs));
}

}
}
