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

static logging::logger logger("paging");

class service::pager::query_pagers::impl : public query_pager {
public:
    impl(schema_ptr s, ::shared_ptr<cql3::selection::selection> selection,
            service::query_state& state, const cql3::query_options& options,
            lw_shared_ptr<query::read_command> cmd,
            std::vector<query::partition_range> ranges)
            : _max(cmd->row_limit), _schema(std::move(s)), _selection(
                    selection), _state(state), _options(options), _cmd(
                    std::move(cmd)), _ranges(std::move(ranges)) {
    }

private:   
    future<> fetch_page(cql3::selection::result_set_builder& builder, uint32_t page_size, db_clock::time_point now) override {
        auto state = _options.get_paging_state();
        uint32_t extra = 0;
        if (state) {
            _max = state->get_remaining();
            _last_pkey = state->get_partition_key();
            _last_ckey = state->get_clustering_key();

            auto dpk = dht::global_partitioner().decorate_key(*_schema, *_last_pkey);
            dht::ring_position lo(dpk);

            auto reversed = _cmd->slice.options.contains<query::partition_slice::option::reversed>();

            logger.trace("PKey={}, CKey={}, reversed={}", dpk, *_last_ckey, reversed);

            bool has_cks = _schema->clustering_key_size() > 0;

            // Note: we're assuming both that the ranges are checked
            // and "cql-compliant", and that storage_proxy will process
            // the ranges in order
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

            // If we have no clustering keys, it should mean we only have one row
            // per PK. Thus we can just bypass the last one.
            modify_ranges(_ranges, lo, has_cks, dht::ring_position_comparator(*_schema));

            if (has_cks) {
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

                _cmd->slice.set_range(*_last_pkey, row_ranges);

                // If we're doing cluster filtering, we might be at the last CK.
                // Next query will operate on PK + a CK that does not give any row.
                // The query code still counts this into row_limit (bug?).
                // Add an extra row to fetch to deal with this.
                extra = 1;
            }
        }

        auto max_rows = std::min(_max, page_size) + extra;

        // We always need PK so we can determine where to start next.
        _cmd->slice.options.set<query::partition_slice::option::send_partition_key>();
        _cmd->slice.options.set<query::partition_slice::option::send_clustering_key>();
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
                        _options.get_serialization_format()),
                [this, page_size, now](auto& builder) {
                    return this->fetch_page(builder, page_size, now).then([&builder] {
                       return builder.build();
                    });
                });
    }
    
    bool ignore_if_in_last_pk(const clustering_key& key) const {
        auto reversed = _cmd->slice.options.contains<query::partition_slice::option::reversed>();

        if (reversed && !clustering_key::less_compare(*_schema)(key, *_last_ckey)) {
            return true;
        }
        if (!reversed && !clustering_key::less_compare(*_schema)(*_last_ckey, key)) {
            return true;
        }
        return false;
    }

    void handle_result(
            cql3::selection::result_set_builder& builder,
            foreign_ptr<lw_shared_ptr<query::result>> results,
            uint32_t page_size, db_clock::time_point now) {

        class myvisitor : public cql3::selection::result_set_builder::visitor {
        public:
            impl& _impl;
            uint32_t page_size;
            uint32_t part_skip = 0;
            uint32_t part_rows = 0;

            uint32_t included_rows = 0;
            uint32_t total_rows = 0;
            std::experimental::optional<partition_key> last_pkey;
            std::experimental::optional<clustering_key> last_ckey;

            bool _is_prev_last_pkey = false;
            // just for verbosity
            uint32_t part_ignored = 0;

            clustering_key::less_compare _less;

            bool include_row(const clustering_key& key) {
                ++total_rows;
                ++part_rows;
                if (included_rows >= page_size) {
                    ++part_ignored;
                    return false;
                }
                if (_is_prev_last_pkey && _impl.ignore_if_in_last_pk(key)) {
                    ++part_skip;
                    return false;
                }
                ++included_rows;
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
                part_skip = 0;
                part_rows = 0;
                part_ignored = 0;
                _is_prev_last_pkey = _impl._last_pkey && key.equal(*_impl._schema, *_impl._last_pkey);
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
                throw std::logic_error("Should not reach!");
            }
            void accept_partition_end(const query::result_row_view& static_row) {
                logger.trace(
                        "End partition, skipped={}, included={}, ignored={}",
                        part_skip,
                        part_rows - part_skip - part_ignored,
                        part_ignored);
                // Do not call visitor::accept_partition_end.
                // It only exists to create a row of static cols in the case
                // we optimized a call to have to rows (only stats selected),
                // in which row_count in accept_new_row was zero.
                // However, we always force clustering keys (even when there are none!)
                // so this should never happen. Also we confuse the APE code
                // when we've selected a row range for first key that results in
                // zero rows.
            }
        };

        myvisitor v(*this, page_size, builder, *_schema, *_selection);
        query::result_view::consume(results->buf(), _cmd->slice, v);

        _max = _max - v.included_rows;
        _exhausted = v.included_rows < page_size || _max == 0;
        _last_pkey = v.last_pkey;
        _last_ckey = v.last_ckey;

        logger.debug("Fetched {}/{} rows, max_remain={} {}", v.included_rows, v.total_rows,
                _max, _exhausted ? "(exh)" : "");

        if (_last_pkey) {
            logger.debug("Last partition key: {}", *_last_pkey);
        }
        if (_last_ckey) {
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
                nullptr : ::make_shared<const paging_state>(*_last_pkey, *_last_ckey, _max);
    }

private:
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

    logger.debug("Query of {}, page_size={}, limit={} requires paging",
            cmd.cf_id, page_size, cmd.row_limit);

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

