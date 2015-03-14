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

    auto command = ::make_lw_shared<query::read_command>(_schema->ks_name, _schema->cf_name,
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

shared_ptr<transport::messages::result_message>
select_statement::process_results(foreign_ptr<lw_shared_ptr<query::result>> results, lw_shared_ptr<query::read_command> cmd,
        const query_options& options, db_clock::time_point now) {
    auto builder = _selection->make_result_set_builder(now, options.get_protocol_version());

    auto add_value = [builder] (const column_definition& def, std::experimental::optional<atomic_cell_or_collection>& cell) {
        if (!cell) {
            builder->add_empty();
            return;
        }

        if (def.type->is_multi_cell()) {
            fail(unimplemented::cause::COLLECTIONS);
#if 0
            List<Cell> cells = row.getMultiCellColumn(def.name);
            ByteBuffer buffer = cells == null
                             ? null
                             : ((CollectionType)def.type).serializeForNativeProtocol(cells, options.getProtocolVersion());
            result.add(buffer);
            return;
#endif
        }

        builder->add(def, cell->as_atomic_cell());
    };

    for (auto&& e : results->partitions) {
        // FIXME: deserialize into views
        auto key = e.first.explode(*_schema);
        auto& partition = e.second;

        if (!partition.static_row.empty() && partition.rows.empty()
                && !_restrictions->uses_secondary_indexing()
                && _restrictions->has_no_clustering_columns_restriction()) {
            builder->new_row();
            uint32_t static_id = 0;
            for (auto&& def : _selection->get_columns()) {
                if (def->is_partition_key()) {
                    builder->add(key[def->component_index()]);
                } else if (def->is_static()) {
                    add_value(*def, partition.static_row.cells[static_id++]);
                } else {
                    builder->add_empty();
                }
            }
        } else {
            for (auto&& e : partition.rows) {
                auto c_key = e.first.explode(*_schema);
                auto& cells = e.second.cells;
                uint32_t static_id = 0;
                uint32_t regular_id = 0;

                builder->new_row();
                for (auto&& def : _selection->get_columns()) {
                    switch (def->kind) {
                        case column_definition::column_kind::PARTITION:
                            builder->add(key[def->component_index()]);
                            break;
                        case column_definition::column_kind::CLUSTERING:
                            builder->add(c_key[def->component_index()]);
                            break;
                        case column_definition::column_kind::REGULAR:
                            add_value(*def, cells[regular_id++]);
                            break;
                        case column_definition::column_kind::STATIC:
                            add_value(*def, partition.static_row.cells[static_id++]);
                            break;
                        default:
                            assert(0);
                    }
                }
            }
        }
    }

    auto rs = builder->build();
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
