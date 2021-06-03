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
 * Copyright (C) 2015-present ScyllaDB
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
#include "cql3/query_options.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "log.hh"
#include "service/storage_proxy.hh"
#include "to_string.hh"

static logging::logger qlogger("paging");

namespace service::pager {

struct noop_visitor {
    void accept_new_partition(uint32_t) { }
    void accept_new_partition(const partition_key& key, uint64_t row_count) { }
    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row) { }
    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) { }
    uint64_t accept_partition_end(const query::result_row_view& static_row) { return 0; }
};

static bool has_clustering_keys(const schema& s, const query::read_command& cmd) {
    return s.clustering_key_size() > 0
            && !cmd.slice.options.contains<query::partition_slice::option::distinct>();
}

    query_pager::query_pager(schema_ptr s, shared_ptr<const cql3::selection::selection> selection,
                    service::query_state& state,
                    const cql3::query_options& options,
                    lw_shared_ptr<query::read_command> cmd,
                    dht::partition_range_vector ranges)
                    : _has_clustering_keys(has_clustering_keys(*s, *cmd))
                    , _max(cmd->get_row_limit())
                    , _per_partition_limit(cmd->slice.partition_row_limit())
                    , _schema(std::move(s))
                    , _selection(selection)
                    , _state(state)
                    , _options(options)
                    , _cmd(std::move(cmd))
                    , _ranges(std::move(ranges))
    {}

    future<service::storage_proxy::coordinator_query_result> query_pager::do_fetch_page(uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout) {
        auto state = _options.get_paging_state();

        auto& proxy = get_local_storage_proxy();

        // Most callers should set this but we want to make sure, as results
        // won't be paged without it.
        _cmd->slice.options.set<query::partition_slice::option::allow_short_read>();
        // Override this, to make sure we use the value appropriate for paging
        // (with allow_short_read set).
        _cmd->max_result_size = proxy.get_max_result_size(_cmd->slice);

        if (!_last_pkey && state) {
            _max = state->get_remaining();
            _last_pkey = state->get_partition_key();
            _last_ckey = state->get_clustering_key();
            _cmd->query_uuid = state->get_query_uuid();
            _cmd->is_first_page = query::is_first_page::no;
            _last_replicas = state->get_last_replicas();
            _query_read_repair_decision = state->get_query_read_repair_decision();
            _rows_fetched_for_last_partition = state->get_rows_fetched_for_last_partition();
        } else {
            _cmd->query_uuid = utils::make_random_uuid();
            _cmd->is_first_page = query::is_first_page::yes;
        }
        qlogger.trace("fetch_page query id {}", _cmd->query_uuid);

        if (_last_pkey) {
            auto dpk = dht::decorate_key(*_schema, *_last_pkey);
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
                                || (reversed && i->start() && cmp(i->start()->value(), lo) == 0)
                                || (!reversed && i->end() && cmp(i->end()->value(), lo) == 0)))
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
                query::trim_clustering_row_ranges_to(*_schema, row_ranges, ckp, reversed);

                _cmd->slice.set_range(*_schema, *_last_pkey, row_ranges);
            }
        }

        auto max_rows = max_rows_to_fetch(page_size);

        // We always need PK so we can determine where to start next.
        _cmd->slice.options.set<query::partition_slice::option::send_partition_key>();
        // don't add empty bytes (cks) unless we have to
        if (_has_clustering_keys) {
            _cmd->slice.options.set<
                    query::partition_slice::option::send_clustering_key>();
        }
        _cmd->set_row_limit(max_rows);
        maybe_adjust_per_partition_limit(page_size);

        qlogger.debug("Fetching {}, page size={}, max_rows={}",
                _cmd->cf_id, page_size, max_rows
                );

        auto ranges = _ranges;
        auto command = ::make_lw_shared<query::read_command>(*_cmd);
        return proxy.query(_schema,
                std::move(command),
                std::move(ranges),
                _options.get_consistency(),
                {timeout, _state.get_permit(), _state.get_client_state(), _state.get_trace_state(), std::move(_last_replicas), _query_read_repair_decision});
    }

    future<> query_pager::fetch_page(cql3::selection::result_set_builder& builder, uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout) {
        return do_fetch_page(page_size, now, timeout).then([this, &builder, page_size, now] (service::storage_proxy::coordinator_query_result qr) {
            _last_replicas = std::move(qr.last_replicas);
            _query_read_repair_decision = qr.read_repair_decision;
            return builder.with_thread_if_needed([this, &builder, page_size, now, qr = std::move(qr)] {
                handle_result(cql3::selection::result_set_builder::visitor(builder, *_schema, *_selection),
                              std::move(qr.query_result), page_size, now);
            });
        });
    }

    future<std::unique_ptr<cql3::result_set>> query_pager::fetch_page(uint32_t page_size,
            gc_clock::time_point now, db::timeout_clock::time_point timeout) {
        return do_with(
                cql3::selection::result_set_builder(*_selection, now,
                        _options.get_cql_serialization_format()),
                [this, page_size, now, timeout](auto& builder) {
                    return this->fetch_page(builder, page_size, now, timeout).then([&builder] {
                        return builder.with_thread_if_needed([&builder] { return builder.build(); });
                    });
                });
    }

future<cql3::result_generator> query_pager::fetch_page_generator(uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout, cql3::cql_stats& stats) {
    return do_fetch_page(page_size, now, timeout).then([this, page_size, now, &stats] (service::storage_proxy::coordinator_query_result qr) {
        _last_replicas = std::move(qr.last_replicas);
        _query_read_repair_decision = qr.read_repair_decision;
        handle_result(noop_visitor(), qr.query_result, page_size, now);
        return cql3::result_generator(_schema, std::move(qr.query_result), _cmd, _selection, stats);
    });
}

class filtering_query_pager : public query_pager {
    ::shared_ptr<cql3::restrictions::statement_restrictions> _filtering_restrictions;
public:
    filtering_query_pager(schema_ptr s, shared_ptr<const cql3::selection::selection> selection,
                service::query_state& state,
                const cql3::query_options& options,
                lw_shared_ptr<query::read_command> cmd,
                dht::partition_range_vector ranges,
                ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions)
        : query_pager(s, selection, state, options, std::move(cmd), std::move(ranges))
        , _filtering_restrictions(std::move(filtering_restrictions))
        {}
    virtual ~filtering_query_pager() {}

    virtual future<> fetch_page(cql3::selection::result_set_builder& builder, uint32_t page_size, gc_clock::time_point now, db::timeout_clock::time_point timeout) override {
        return do_fetch_page(page_size, now, timeout).then([this, &builder, page_size, now] (service::storage_proxy::coordinator_query_result qr) {
            _last_replicas = std::move(qr.last_replicas);
            _query_read_repair_decision = qr.read_repair_decision;
            qr.query_result->ensure_counts();
            _stats.rows_read_total += *qr.query_result->row_count();
            handle_result(cql3::selection::result_set_builder::visitor(builder, *_schema, *_selection,
                          cql3::selection::result_set_builder::restrictions_filter(_filtering_restrictions, _options, _max, _schema, _per_partition_limit, _last_pkey, _rows_fetched_for_last_partition)),
                          std::move(qr.query_result), page_size, now);
        });
    }

protected:
    virtual uint64_t max_rows_to_fetch(uint32_t page_size) override {
        return static_cast<uint64_t>(page_size);
    }

    virtual void maybe_adjust_per_partition_limit(uint32_t page_size) const override {
        _cmd->slice.set_partition_row_limit(page_size);
    }
};

template<typename Base>
class query_pager::query_result_visitor : public Base {
    using visitor = Base;
public:
    uint64_t total_rows = 0;
    uint64_t dropped_rows = 0;
    uint64_t last_partition_row_count = 0;
    std::optional<partition_key> last_pkey;
    std::optional<clustering_key> last_ckey;

    query_result_visitor(Base&& v) : Base(std::move(v)) { }

    void accept_new_partition(uint64_t) {
        throw std::logic_error("Should not reach!");
    }
    void accept_new_partition(const partition_key& key, uint64_t row_count) {
        qlogger.trace("Accepting partition: {} ({})", key, row_count);
        total_rows += std::max(row_count, uint64_t(1));
        last_pkey = key;
        last_ckey = { };
        last_partition_row_count = row_count;
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
        const uint64_t dropped = visitor::accept_partition_end(static_row);
        dropped_rows += dropped;
        last_partition_row_count -= dropped;
    }
};

    template<typename Visitor>
    requires query::ResultVisitor<Visitor>
    void query_pager::handle_result(
            Visitor&& visitor,
            const foreign_ptr<lw_shared_ptr<query::result>>& results,
            uint32_t page_size, gc_clock::time_point now) {

        auto update_slice = [&] (const partition_key& last_pkey) {
            // refs #752, when doing aggregate queries we will re-use same
            // slice repeatedly. Since "specific ck ranges" only deal with
            // a single extra range, we must clear out the old one
            // Even if it was not so of course, leaving junk in the slice
            // is bad.
            _cmd->slice.clear_range(*_schema, last_pkey);
        };

        auto view = query::result_view(*results);

        uint64_t row_count;
        if constexpr(!std::is_same_v<std::decay_t<Visitor>, noop_visitor>) {
            query_result_visitor<Visitor> v(std::forward<Visitor>(visitor));
            view.consume(_cmd->slice, v);

            if (_last_pkey) {
                update_slice(*_last_pkey);
            }

            row_count = v.total_rows - v.dropped_rows;
            _max = _max - row_count;
            _exhausted = (v.total_rows < page_size && !results->is_short_read() && v.dropped_rows == 0) || _max == 0;
            // If per partition limit is defined, we need to accumulate rows fetched for last partition key if the key matches
            if (_cmd->slice.partition_row_limit() < query::max_rows_if_set) {
                if (_last_pkey && v.last_pkey && _last_pkey->equal(*_schema, *v.last_pkey)) {
                    _rows_fetched_for_last_partition += v.last_partition_row_count;
                } else {
                    _rows_fetched_for_last_partition = v.last_partition_row_count;
                }
            }
            _last_pkey = v.last_pkey;
            _last_ckey = v.last_ckey;
        } else {
            row_count = results->row_count() ? *results->row_count() : std::get<1>(view.count_partitions_and_rows());
            _max = _max - row_count;
            _exhausted = (row_count < page_size && !results->is_short_read()) || _max == 0;

            if (!_exhausted && row_count > 0) {
                if (_last_pkey) {
                    update_slice(*_last_pkey);
                }
                auto [ last_pkey, last_ckey ] = view.get_last_partition_and_clustering_key();
                _last_pkey = std::move(last_pkey);
                _last_ckey = std::move(last_ckey);
            }
        }

        qlogger.debug("Fetched {} rows, max_remain={} {}", row_count, _max, _exhausted ? "(exh)" : "");

        if (_last_pkey) {
            qlogger.debug("Last partition key: {}", *_last_pkey);
        }
        if (_has_clustering_keys && _last_ckey) {
            qlogger.debug("Last clustering key: {}", *_last_ckey);
        }
    }

    lw_shared_ptr<const paging_state> query_pager::state() const {
        return make_lw_shared<paging_state>(_last_pkey.value_or(partition_key::make_empty()), _last_ckey, _exhausted ? 0 : _max, _cmd->query_uuid, _last_replicas, _query_read_repair_decision, _rows_fetched_for_last_partition);
    }

}

bool service::pager::query_pagers::may_need_paging(const schema& s, uint32_t page_size,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges) {

    // Disabling paging also disables query result size limiter. We can do this safely
    // only if we know for sure that it wouldn't limit anything i.e. the result will
    // not contain more than one row.
    auto need_paging = [&] {
        if (cmd.get_row_limit() <= 1 || ranges.empty()) {
            return false;
        } else if (cmd.partition_limit <= 1
                || (ranges.size() == 1 && query::is_single_partition(ranges.front()))) {
            auto effective_partition_row_limit = cmd.slice.options.contains<query::partition_slice::option::distinct>() ? 1 : cmd.slice.partition_row_limit();

            auto& cr_ranges = cmd.slice.default_row_ranges();
            if (effective_partition_row_limit <= 1 || cr_ranges.empty()
                    || (cr_ranges.size() == 1 && query::is_single_row(s, cr_ranges.front()))) {
                return false;
            }
        }
        return true;
    }();

    qlogger.debug("Query of {}, page_size={}, limit={} {}", cmd.cf_id, page_size,
                    cmd.get_row_limit(),
                    need_paging ? "requires paging" : "does not require paging");

    return need_paging;
}

std::unique_ptr<service::pager::query_pager> service::pager::query_pagers::pager(
        schema_ptr s, shared_ptr<const cql3::selection::selection> selection,
        service::query_state& state, const cql3::query_options& options,
        lw_shared_ptr<query::read_command> cmd,
        dht::partition_range_vector ranges,
        ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions) {
    // If partition row limit is applied to paging, we still need to fall back
    // to filtering the results to avoid extraneous rows on page breaks.
    if (!filtering_restrictions && cmd->slice.partition_row_limit() < query::max_rows_if_set) {
        filtering_restrictions = ::make_shared<cql3::restrictions::statement_restrictions>(s, true);
    }
    if (filtering_restrictions) {
        return std::make_unique<filtering_query_pager>(std::move(s), std::move(selection), state,
                    options, std::move(cmd), std::move(ranges), std::move(filtering_restrictions));
    }
    return std::make_unique<query_pager>(std::move(s), std::move(selection), state,
            options, std::move(cmd), std::move(ranges));
}
