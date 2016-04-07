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
#include "to_string.hh"

static logging::logger logger("paging");

class service::pager::query_pagers::impl : public query_pager {
public:
    impl(schema_ptr s, ::shared_ptr<cql3::selection::selection> selection,
                    service::query_state& state,
                    const cql3::query_options& options,
                    lw_shared_ptr<query::read_command> cmd,
                    std::vector<query::partition_range> ranges)
                    : _has_clustering_keys(s->clustering_key_size() > 0)
                    , _max(cmd->row_limit)
                    , _schema(std::move(s))
                    , _selection(selection)
                    , _state(state)
                    , _options(options)
                    , _cmd(std::move(cmd))
                    , _ranges(std::move(ranges))
    {}

private:   
    future<> fetch_page(cql3::selection::result_set_builder& builder, uint32_t page_size, db_clock::time_point now) override {
        auto state = _options.get_paging_state();

        if (!_last_pkey && state) {
            _max = state->get_remaining();
            _last_pkey = state->get_partition_key();
            _last_ckey = state->get_clustering_key();
        }

        if (_last_pkey) {
            auto dpk = dht::global_partitioner().decorate_key(*_schema, *_last_pkey);
            dht::ring_position lo(dpk);

            auto reversed = _cmd->slice.options.contains<query::partition_slice::option::reversed>();

            logger.trace("PKey={}, CKey={}, reversed={}", dpk, _last_ckey, reversed);

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
                            || (contains && (i->is_singular() && !inclusive))
                            ;

                    if (remove) {
                        logger.trace("Remove range {}", *i);
                        i = ranges.erase(i);
                        continue;
                    }
                    if (contains) {
                        auto r = reversed && !i->is_singular()
                                ? range_type(i->start(), bound_type{ lo, inclusive })
                                : range_type( bound_type{ lo, inclusive }, i->end(), i->is_singular())
                                ;
                        logger.trace("Modify range {} -> {}", *i, r);
                        *i = std::move(r);
                    }
                    ++i;
                }
                logger.trace("Result ranges {}", ranges);
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
                clustering_key_prefix::less_compare cmp_rt(*_schema);
                modify_ranges(row_ranges, ckp, false, [&cmp_rt](auto& c1, auto c2) {
                    if (cmp_rt(c1, c2)) {
                        return -1;
                    } else if (cmp_rt(c2, c1)) {
                        return 1;
                    }
                    return 0;
                });

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

        logger.debug("Fetching {}, page size={}, max_rows={}",
                _cmd->cf_id, page_size, max_rows
                );

        auto ranges = _ranges;
        return get_local_storage_proxy().query(_schema, _cmd, std::move(ranges),
                _options.get_consistency()).then(
                [this, &builder, page_size, now](foreign_ptr<lw_shared_ptr<query::result>> results) {
                    handle_result(builder, std::move(results), page_size, now);
                });
    }

    future<std::unique_ptr<cql3::result_set>> fetch_page(uint32_t page_size,
            db_clock::time_point now) override {
        return do_with(
                cql3::selection::result_set_builder(*_selection, now,
                        _options.get_cql_serialization_format()),
                [this, page_size, now](auto& builder) {
                    return this->fetch_page(builder, page_size, now).then([&builder] {
                       return builder.build();
                    });
                });
    }

    void handle_result(
            cql3::selection::result_set_builder& builder,
            foreign_ptr<lw_shared_ptr<query::result>> results,
            uint32_t page_size, db_clock::time_point now) {

        class myvisitor : public cql3::selection::result_set_builder::visitor {
        public:
            impl& _impl;
            uint32_t page_size;
            uint32_t part_rows = 0;

            uint32_t included_rows = 0;
            uint32_t total_rows = 0;
            std::experimental::optional<partition_key> last_pkey;
            std::experimental::optional<clustering_key> last_ckey;

            // just for verbosity
            uint32_t part_ignored = 0;

            clustering_key::less_compare _less;

            bool include_row() {
                ++total_rows;
                ++part_rows;
                if (included_rows >= page_size) {
                    ++part_ignored;
                    return false;
                }
                ++included_rows;
                return true;
            }

            bool include_row(const clustering_key& key) {
                if (!include_row()) {
                    return false;
                }
                if (included_rows == page_size) {
                    last_ckey = key;
                }
                return true;
            }
            myvisitor(impl& i, uint32_t ps,
                    cql3::selection::result_set_builder& builder,
                    const schema& s,
                    const cql3::selection::selection& selection)
                    : visitor(builder, s, selection), _impl(i), page_size(ps), _less(*_impl._schema) {
            }

            void accept_new_partition(uint32_t) {
                throw std::logic_error("Should not reach!");
            }
            void accept_new_partition(const partition_key& key, uint32_t row_count) {
                logger.trace("Begin partition: {} ({})", key, row_count);
                part_rows = 0;
                part_ignored = 0;
                if (included_rows < page_size) {
                    last_pkey = key;
                }
                visitor::accept_new_partition(key, row_count);
            }
            void accept_new_row(const clustering_key& key,
                    const query::result_row_view& static_row,
                    const query::result_row_view& row) {
                // TODO: should we use exception/long jump or introduce
                // a "stop" condition to the calling result_view and
                // avoid processing unneeded rows?
                auto ok = include_row(key);
                if (ok) {
                    visitor::accept_new_row(key, static_row, row);
                }
            }
            void accept_new_row(const query::result_row_view& static_row,
                    const query::result_row_view& row) {
                auto ok = include_row();
                if (ok) {
                    visitor::accept_new_row(static_row, row);
                }
            }
            void accept_partition_end(const query::result_row_view& static_row) {
                // accept_partition_end with row_count == 0
                // means we had an empty partition but live
                // static columns, and since the fix,
                // no CK restrictions.
                // I.e. _row_count == 0 -> add a partially empty row
                // So, treat this case as an accept_row variant
                if (_row_count > 0 || include_row()) {
                    visitor::accept_partition_end(static_row);
                }
                logger.trace(
                        "End partition, included={}, ignored={}",
                        part_rows - part_ignored,
                        part_ignored);
            }
        };

        myvisitor v(*this, std::min(page_size, _max), builder, *_schema, *_selection);
        query::result_view::consume(*results, _cmd->slice, v);

        if (_last_pkey) {
            // refs #752, when doing aggregate queries we will re-use same
            // slice repeatedly. Since "specific ck ranges" only deal with
            // a single extra range, we must clear out the old one
            // Even if it was not so of course, leaving junk in the slice
            // is bad.
            _cmd->slice.clear_range(*_schema, *_last_pkey);
        }

        _max = _max - v.included_rows;
        _exhausted = v.included_rows < page_size || _max == 0;
        _last_pkey = v.last_pkey;
        _last_ckey = v.last_ckey;

        logger.debug("Fetched {}/{} rows, max_remain={} {}", v.included_rows, v.total_rows,
                _max, _exhausted ? "(exh)" : "");

        if (_last_pkey) {
            logger.debug("Last partition key: {}", *_last_pkey);
        }
        if (_has_clustering_keys && _last_ckey) {
            logger.debug("Last clustering key: {}", *_last_ckey);
        }
    }

    bool is_exhausted() const override {
        return _exhausted;
    }

    int max_remaining() const override {
        return _max;
    }

    ::shared_ptr<const service::pager::paging_state> state() const override {
        return _exhausted ?
                        nullptr :
                        ::make_shared<const paging_state>(*_last_pkey,
                                        _last_ckey, _max);
    }

private:
    // remember if we use clustering. if not, each partition == one row
    const bool _has_clustering_keys;
    bool _exhausted = false;
    uint32_t _rem = 0;
    uint32_t _max;

    std::experimental::optional<partition_key> _last_pkey;
    std::experimental::optional<clustering_key> _last_ckey;

    schema_ptr _schema;
    ::shared_ptr<cql3::selection::selection> _selection;
    service::query_state& _state;
    const cql3::query_options& _options;
    lw_shared_ptr<query::read_command> _cmd;
    std::vector<query::partition_range> _ranges;
};

bool service::pager::query_pagers::may_need_paging(uint32_t page_size,
        const query::read_command& cmd,
        const std::vector<query::partition_range>& ranges) {
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

    logger.debug("Query of {}, page_size={}, limit={} {}", cmd.cf_id, page_size,
                    cmd.row_limit,
                    need_paging ? "requires paging" : "does not require paging");

    return need_paging;
}

::shared_ptr<service::pager::query_pager> service::pager::query_pagers::pager(
        schema_ptr s, ::shared_ptr<cql3::selection::selection> selection,
        service::query_state& state, const cql3::query_options& options,
        lw_shared_ptr<query::read_command> cmd,
        std::vector<query::partition_range> ranges) {
    return ::make_shared<impl>(std::move(s), std::move(selection), state,
            options, std::move(cmd), std::move(ranges));
}

