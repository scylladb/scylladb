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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "query_pagers.hh"
#include "query_pager.hh"
#include "cql3/selection/selection.hh"
#include "log.hh"
#include "service/storage_proxy.hh"
#include "to_string.hh"

static logging::logger qlogger("paging");

namespace service::pager {

static bool has_clustering_keys(const schema& s, const query::read_command& cmd) {
    return s.clustering_key_size() > 0
            && !cmd.slice.options.contains<query::partition_slice::option::distinct>();
}

    query_pager::query_pager(schema_ptr s, shared_ptr<const cql3::selection::selection> selection,
                    service::query_state& state,
                    const cql3::query_options& options,
                    db::timeout_clock::duration timeout,
                    lw_shared_ptr<query::read_command> cmd,
                    dht::partition_range_vector ranges)
                    : _has_clustering_keys(has_clustering_keys(*s, *cmd))
                    , _max(cmd->row_limit)
                    , _schema(std::move(s))
                    , _selection(selection)
                    , _state(state)
                    , _options(options)
                    , _timeout(timeout)
                    , _cmd(std::move(cmd))
                    , _ranges(std::move(ranges))
    {}

    future<> query_pager::fetch_page(cql3::selection::result_set_builder& builder, uint32_t page_size, gc_clock::time_point now) {
        auto state = _options.get_paging_state();

        if (!_last_pkey && state) {
            _max = state->get_remaining();
            _last_pkey = state->get_partition_key();
            _last_ckey = state->get_clustering_key();
            _cmd->query_uuid = state->get_query_uuid();
            _cmd->is_first_page = false;
            _last_replicas = state->get_last_replicas();
            _query_read_repair_decision = state->get_query_read_repair_decision();
        } else {
            // Reusing readers is currently only supported for singular queries.
            if (_ranges.front().is_singular()) {
                _cmd->query_uuid = utils::make_random_uuid();
            }
            _cmd->is_first_page = true;
        }
        qlogger.trace("fetch_page query id {}", _cmd->query_uuid);

        if (_last_pkey) {
            auto dpk = dht::global_partitioner().decorate_key(*_schema, *_last_pkey);
            dht::ring_position lo(dpk);

            auto reversed = _cmd->slice.options.contains<query::partition_slice::option::reversed>();

            qlogger.trace("PKey={}, CKey={}, reversed={}", dpk, _last_ckey, reversed);

            // Note: we're assuming both that the ranges are checked
            // and "cql-compliant", and that storage_proxy will process
            // the ranges in order
            //
            // If the original query has singular restrictions like "col in (x, y, z)",
            // we will eventually generate an empty range. This is ok, because empty range == nothing,
            // which is what we thus mean.
            auto modify_ranges = [reversed](auto& ranges, auto& lo, bool inclusive, const auto& cmp) {
                typedef typename std::remove_reference_t<decltype(ranges)>::value_type range_type;
                typedef typename range_type::bound bound_type;
                bool found = false;

                auto i = ranges.begin();
                while (i != ranges.end()) {
                    bool contains = i->contains(lo, cmp);

                    if (contains) {
                        found = true;
                    }

                    bool remove = !found
                            || (contains && !inclusive && (i->is_singular()
                                || (reversed && i->start() && !cmp(i->start()->value(), lo))
                                || (!reversed && i->end() && !cmp(i->end()->value(), lo))))
                            ;

                    if (remove) {
                        qlogger.trace("Remove range {}", *i);
                        i = ranges.erase(i);
                        continue;
                    }
                    if (contains) {
                        auto r = reversed && !i->is_singular()
                                ? range_type(i->start(), bound_type{ lo, inclusive })
                                : range_type( bound_type{ lo, inclusive }, i->end(), i->is_singular())
                                ;
                        qlogger.trace("Modify range {} -> {}", *i, r);
                        *i = std::move(r);
                    }
                    ++i;
                }
                qlogger.trace("Result ranges {}", ranges);
            };

            // Because of #1446 we don't have a comparator to use with
            // range<clustering_key_prefix> which would produce correct results.
            // This means we cannot reuse the same logic for dealing with
            // partition and clustering keys.
            auto modify_ck_ranges = [reversed] (const schema& s, auto& ranges, auto& lo) {
                typedef typename std::remove_reference_t<decltype(ranges)>::value_type range_type;
                typedef typename range_type::bound bound_type;

                auto cmp = [reversed, bv_cmp = bound_view::compare(s)] (const auto& a, const auto& b) {
                    return reversed ? bv_cmp(b, a) : bv_cmp(a, b);
                };
                auto start_bound = [reversed] (const auto& range) -> const bound_view& {
                    return reversed ? range.second : range.first;
                };
                auto end_bound = [reversed] (const auto& range) -> const bound_view& {
                    return reversed ? range.first : range.second;
                };
                clustering_key_prefix::equality eq(s);

                auto it = ranges.begin();
                while (it != ranges.end()) {
                    auto range = bound_view::from_range(*it);
                    if (cmp(end_bound(range), lo) || eq(end_bound(range).prefix, lo)) {
                        qlogger.trace("Remove ck range {}", *it);
                        it = ranges.erase(it);
                        continue;
                    } else if (cmp(start_bound(range), lo)) {
                        assert(cmp(lo, end_bound(range)));
                        auto r = reversed ? range_type(it->start(), bound_type { lo, false })
                                          : range_type(bound_type { lo, false }, it->end());
                        qlogger.trace("Modify ck range {} -> {}", *it, r);
                        *it = std::move(r);
                    }
                    ++it;
                }
            };

            // last ck can be empty depending on whether we
            // deserialized state or not. This case means "last page ended on
            // something-not-bound-by-clustering" (i.e. a static row, alone)
            const bool has_ck = _has_clustering_keys && _last_ckey;

            // If we have no clustering keys, it should mean we only have one row
            // per PK. Thus we can just bypass the last one.
            modify_ranges(_ranges, lo, has_ck, dht::ring_position_comparator(*_schema));

            if (has_ck) {
                query::clustering_row_ranges row_ranges = _cmd->slice.default_row_ranges();
                clustering_key_prefix ckp = clustering_key_prefix::from_exploded(*_schema, _last_ckey->explode(*_schema));
                modify_ck_ranges(*_schema, row_ranges, ckp);

                _cmd->slice.set_range(*_schema, *_last_pkey, row_ranges);
            }
        }

        auto max_rows = std::min(_max, page_size);

        // We always need PK so we can determine where to start next.
        _cmd->slice.options.set<query::partition_slice::option::send_partition_key>();
        // don't add empty bytes (cks) unless we have to
        if (_has_clustering_keys) {
            _cmd->slice.options.set<
                    query::partition_slice::option::send_clustering_key>();
        }
        _cmd->row_limit = max_rows;

        qlogger.debug("Fetching {}, page size={}, max_rows={}",
                _cmd->cf_id, page_size, max_rows
                );

        auto ranges = _ranges;
        auto command = ::make_lw_shared<query::read_command>(*_cmd);
        auto this_timeout = db::timeout_clock::now() + _timeout;
        return get_local_storage_proxy().query(_schema,
                std::move(command),
                std::move(ranges),
                _options.get_consistency(),
                {this_timeout, _state.get_trace_state(), std::move(_last_replicas), _query_read_repair_decision}).then(
                [this, &builder, page_size, now](service::storage_proxy::coordinator_query_result qr) {
                    _last_replicas = std::move(qr.last_replicas);
                    _query_read_repair_decision = qr.read_repair_decision;
                    handle_result(builder, std::move(qr.query_result), page_size, now);
                });
    }

    future<std::unique_ptr<cql3::result_set>> query_pager::fetch_page(uint32_t page_size,
            gc_clock::time_point now) {
        return do_with(
                cql3::selection::result_set_builder(*_selection, now,
                        _options.get_cql_serialization_format()),
                [this, page_size, now](auto& builder) {
                    return this->fetch_page(builder, page_size, now).then([&builder] {
                       return builder.build();
                    });
                });
    }

    void query_pager::handle_result(
            cql3::selection::result_set_builder& builder,
            foreign_ptr<lw_shared_ptr<query::result>> results,
            uint32_t page_size, gc_clock::time_point now) {

        class myvisitor : public cql3::selection::result_set_builder::visitor {
        public:
            uint32_t total_rows = 0;
            std::experimental::optional<partition_key> last_pkey;
            std::experimental::optional<clustering_key> last_ckey;

            myvisitor(cql3::selection::result_set_builder& builder,
                    const schema& s,
                    const cql3::selection::selection& selection)
                    : visitor(builder, s, selection) {
            }

            void accept_new_partition(uint32_t) {
                throw std::logic_error("Should not reach!");
            }
            void accept_new_partition(const partition_key& key, uint32_t row_count) {
                qlogger.trace("Accepting partition: {} ({})", key, row_count);
                total_rows += std::max(row_count, 1u);
                last_pkey = key;
                last_ckey = { };
                visitor::accept_new_partition(key, row_count);
            }
            void accept_new_row(const clustering_key& key,
                    const query::result_row_view& static_row,
                    const query::result_row_view& row) {
                last_ckey = key;
                visitor::accept_new_row(key, static_row, row);
            }
            void accept_new_row(const query::result_row_view& static_row,
                    const query::result_row_view& row) {
                visitor::accept_new_row(static_row, row);
            }
            void accept_partition_end(const query::result_row_view& static_row) {
                visitor::accept_partition_end(static_row);
            }
        };

        myvisitor v(builder, *_schema, *_selection);
        query::result_view::consume(*results, _cmd->slice, v);

        if (_last_pkey) {
            // refs #752, when doing aggregate queries we will re-use same
            // slice repeatedly. Since "specific ck ranges" only deal with
            // a single extra range, we must clear out the old one
            // Even if it was not so of course, leaving junk in the slice
            // is bad.
            _cmd->slice.clear_range(*_schema, *_last_pkey);
        }

        _max = _max - v.total_rows;
        _exhausted = (v.total_rows < page_size && !results->is_short_read()) || _max == 0;
        _last_pkey = v.last_pkey;
        _last_ckey = v.last_ckey;

        qlogger.debug("Fetched {} rows, max_remain={} {}", v.total_rows, _max, _exhausted ? "(exh)" : "");

        if (_last_pkey) {
            qlogger.debug("Last partition key: {}", *_last_pkey);
        }
        if (_has_clustering_keys && _last_ckey) {
            qlogger.debug("Last clustering key: {}", *_last_ckey);
        }
    }

    shared_ptr<const service::pager::paging_state> query_pager::state() const {
        return _exhausted ?  nullptr : ::make_shared<const paging_state>(*_last_pkey,
                _last_ckey, _max, _cmd->query_uuid, _last_replicas, _query_read_repair_decision);
    }

}

bool service::pager::query_pagers::may_need_paging(uint32_t page_size,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges) {
    auto est_max_rows =
            [&] {
                if (ranges.empty()) {
                    return cmd.row_limit;
                }
                uint32_t n = 0;
                for (auto& r : ranges) {
                    if (r.is_singular() && cmd.slice.options.contains<query::partition_slice::option::distinct>()) {
                        ++n;
                        continue;
                    }
                    return cmd.row_limit;
                }
                return n;
            };

    auto est = est_max_rows();
    auto need_paging = est > page_size;

    qlogger.debug("Query of {}, page_size={}, limit={} {}", cmd.cf_id, page_size,
                    cmd.row_limit,
                    need_paging ? "requires paging" : "does not require paging");

    return need_paging;
}

::shared_ptr<service::pager::query_pager> service::pager::query_pagers::pager(
        schema_ptr s, shared_ptr<const cql3::selection::selection> selection,
        service::query_state& state, const cql3::query_options& options,
        db::timeout_clock::duration timeout,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector ranges) {
    return ::make_shared<query_pager>(std::move(s), std::move(selection), state,
            options, timeout, std::move(cmd), std::move(ranges));
}

