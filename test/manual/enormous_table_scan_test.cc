/*
 * Copyright (C) 2020-present ScyllaDB
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


#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_assertions.hh"
#include "transport/messages/result_message.hh"

#include <boost/range/adaptor/indirected.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/find_if.hpp>

#include "clustering_bounds_comparator.hh"
#include "dht/i_partitioner.hh"
#include "mutation_fragment.hh"
#include "mutation_reader.hh"
#include "partition_range_compat.hh"
#include "range.hh"
#include "sstables/sstables.hh"
#include "schema_builder.hh"

class enormous_table_reader final : public flat_mutation_reader::impl {
// Reader for a table with 4.5 billion rows, all with partition key 0 and an incrementing clustering key
public:
    static constexpr uint64_t CLUSTERING_ROW_COUNT = 4500ULL * 1000ULL * 1000ULL;

    enormous_table_reader(schema_ptr schema, reader_permit permit, const dht::partition_range& prange, const query::partition_slice& slice)
        : impl(std::move(schema), std::move(permit))
        , _slice(slice)
    {
        do_fast_forward_to(prange);
    }

    virtual ~enormous_table_reader() {
    }

    virtual future<> fill_buffer() override {
        if (!_partition_in_range) {
            return make_ready_future<>();
        }
        return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
            auto int_to_ck = [this] (int64_t i) -> clustering_key {
                auto ck_data = data_value(i).serialize_nonnull();
                return clustering_key::from_single_value(*_schema, std::move(ck_data));
            };

            auto ck_to_int = [this] (const clustering_key& ck) -> int64_t {
                auto exploded = ck.explode();
                assert(exploded.size() == 1);
                return value_cast<int64_t>(long_type->deserialize(exploded[0]));
            };

            auto dk = get_dk();
            if (_pps == partition_production_state::before_partition_start) {
                push_mutation_fragment(*_schema, _permit, partition_start(std::move(dk), tombstone()));
                _pps = partition_production_state::after_partition_start;

            } else if (_pps == partition_production_state::after_partition_start) {
                auto cmp = clustering_key::tri_compare(*_schema);

                auto ck = int_to_ck(_clustering_row_idx);
                for (const auto& range : _slice.row_ranges(*_schema, dk.key())) {
                    if (range.before(ck, cmp)) {
                        _clustering_row_idx = ck_to_int(range.start()->value());
                        if (!range.start()->is_inclusive()) {
                            ++_clustering_row_idx;
                        }
                        ck = int_to_ck(_clustering_row_idx);
                        break;
                    }
                    if (!range.after(ck, cmp)) {
                        break;
                    }
                }

                if (_clustering_row_idx >= CLUSTERING_ROW_COUNT) {
                    _pps = partition_production_state::before_partition_end;
                    return make_ready_future<>();
                }

                ++_clustering_row_idx;
                auto crow = clustering_row(std::move(ck));
                // crow.set_cell(_cdef, atomic_cell::make_live(*_cdef.type, ));
                crow.marker() = row_marker(api::new_timestamp());
                push_mutation_fragment(*_schema, _permit, std::move(crow));

            } else if (_pps == partition_production_state::before_partition_end) {
                push_mutation_fragment(*_schema, _permit, partition_end());
                _pps = partition_production_state::after_partition_end;
                _end_of_stream = true;
            }
            return make_ready_future<>();
        });
    }

    virtual future<> next_partition() override {
        clear_buffer();
        _end_of_stream = true;
        return make_ready_future<>();
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        do_fast_forward_to(pr);
        return make_ready_future<>();
    }

    virtual future<> fast_forward_to(position_range pr) override {
        throw runtime_exception("not forwardable");
        return make_ready_future<>();
    }

    virtual future<> close() noexcept override {
        return make_ready_future<>();
    }

private:
    void get_next_partition() {
        if (_pps != partition_production_state::not_started) {
            _end_of_stream = true;
        }
    }

    void do_fast_forward_to(const dht::partition_range& pr) {
        clear_buffer();
        auto pos = dht::ring_position(get_dk());
        _partition_in_range = pr.contains(pos, dht::ring_position_comparator(*_schema));
        _end_of_stream = !_partition_in_range;
        if (_partition_in_range) {
            _pps = partition_production_state::before_partition_start;
        }
    }

    partition_key get_pk() {
        auto pk_data = data_value(int64_t(0)).serialize_nonnull();
        return partition_key::from_single_value(*_schema, std::move(pk_data));
    }
    dht::decorated_key get_dk() {
        return dht::decorate_key(*_schema, get_pk());
    }

    enum class partition_production_state {
        not_started,
        before_partition_start,
        after_partition_start,
        before_partition_end,
        after_partition_end,
    };

    schema_ptr _schema;
    const query::partition_slice& _slice;
    streamed_mutation::forwarding _fwd;
    partition_production_state _pps = partition_production_state::not_started;

    bool _partition_in_range = false;
    uint64_t _clustering_row_idx = 0;
};

struct enormous_virtual_reader {
    flat_mutation_reader operator()(schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            const io_priority_class& pc,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd,
            mutation_reader::forwarding fwd_mr) {
        auto reader = make_flat_mutation_reader<enormous_table_reader>(schema, permit, range, slice);
        if (fwd == streamed_mutation::forwarding::yes) {
            return make_forwardable(std::move(reader));
        }
        return reader;
    }
};


static lw_shared_ptr<service::pager::paging_state> extract_paging_state(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    auto paging_state = rows->rs().get_metadata().paging_state();
    if (!paging_state) {
        return nullptr;
    }
    return make_lw_shared<service::pager::paging_state>(*paging_state);
};

static size_t count_rows_fetched(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    return rows->rs().result_set().size();
};

static bool has_more_pages(::shared_ptr<cql_transport::messages::result_message> res) {
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
    return rows->rs().get_metadata().flags().contains(cql3::metadata::flag::HAS_MORE_PAGES);
};

SEASTAR_TEST_CASE(scan_enormous_table_test) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.create_table([&e](std::string_view ks_name) {
            return *schema_builder(e.local_db().get_schema_registry(), ks_name, "enormous_table")
                    .with_column("pk", long_type, column_kind::partition_key)
                    .with_column("ck", long_type, column_kind::clustering_key)
                    .set_comment("a very big table (4.5 billion entries, one partition)")
                    .build();
        }).get();
        auto& db = e.local_db();
        db.find_column_family("ks", "enormous_table").set_virtual_reader(mutation_source(enormous_virtual_reader()));

        uint64_t rows_fetched = 0;
        shared_ptr<cql_transport::messages::result_message> msg;
        lw_shared_ptr<service::pager::paging_state> paging_state;
        std::unique_ptr<cql3::query_options> qo;
        uint64_t fetched_rows_log_counter = 1e7;
        do {
            qo = std::make_unique<cql3::query_options>(db::consistency_level::LOCAL_ONE, std::vector<cql3::raw_value>{},
                    cql3::query_options::specific_options{10000, paging_state, {}, api::new_timestamp()});
            msg = e.execute_cql("select * from enormous_table;", std::move(qo)).get0();
            rows_fetched += count_rows_fetched(msg);
            paging_state = extract_paging_state(msg);
            if (rows_fetched >= fetched_rows_log_counter){
                testlog.info("Fetched {} rows", rows_fetched);
                fetched_rows_log_counter += 1e7;
            }
        } while(has_more_pages(msg));
        BOOST_REQUIRE_EQUAL(rows_fetched, enormous_table_reader::CLUSTERING_ROW_COUNT);
    });
}

SEASTAR_TEST_CASE(count_enormous_table_test) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.create_table([&e](std::string_view ks_name) {
            return *schema_builder(e.local_db().get_schema_registry(), ks_name, "enormous_table")
                    .with_column("pk", long_type, column_kind::partition_key)
                    .with_column("ck", long_type, column_kind::clustering_key)
                    .set_comment("a very big table (4.5 billion entries, one partition)")
                    .build();
        }).get();
        auto& db = e.local_db();
        db.find_column_family("ks", "enormous_table").set_virtual_reader(mutation_source(enormous_virtual_reader()));

        auto msg = e.execute_cql("select count(*) from enormous_table").get0();
        assert_that(msg).is_rows().with_rows({{{long_type->decompose(int64_t(enormous_table_reader::CLUSTERING_ROW_COUNT))}}});
    });
}
