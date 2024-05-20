#include <chrono>
#include <cstdint>
#include <seastar/core/coroutine.hh>
#include "test/lib/scylla_test_case.hh"

#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include "mutation/mutation.hh"
#include "service/storage_proxy.hh"

SEASTAR_TEST_CASE(test_internal_operation_filtering) {
    return do_with_cql_env_thread([] (cql_test_env& e) -> future<> {
        // The test requires at least two shards
        // so that it can test the shard!=coordinator case
        BOOST_REQUIRE_GT(smp::count, 1);

        cquery_nofail(e, "CREATE TABLE ks.tbl (pk int PRIMARY KEY) \
                WITH per_partition_rate_limit = {'max_reads_per_second': 1, 'max_writes_per_second': 1}");
        
        auto& db = e.db();
        auto& qp = e.qp();
        const auto sptr = db.local().find_schema("ks", "tbl");

        auto pk = partition_key::from_singular(*sptr, int32_t(0));

        unsigned local_shard = sptr->table().shard_for_reads(dht::get_token(*sptr, pk.view()));
        unsigned foreign_shard = (local_shard + 1) % smp::count;
        
        auto run_writes = [&qp, &db, pk] (db::allow_per_partition_rate_limit allow_limit) -> future<> {
            BOOST_TEST_MESSAGE("Testing writes");

            const auto sptr = db.local().find_schema("ks", "tbl");
            auto m = mutation(sptr, partition_key(pk));

            // Rejection is probabilistic, so try many times
            for (int i = 0; i < 100; i++) {
                qp.local().proxy().mutate({m},
                        db::consistency_level::ALL,
                        service::storage_proxy::clock_type::now() + std::chrono::seconds(10),
                        nullptr,
                        empty_service_permit(),
                        allow_limit).get();
            }

            return make_ready_future<>();
        };

        auto run_reads = [&qp, &db, pk] (db::allow_per_partition_rate_limit allow_limit) -> future<> {
            BOOST_TEST_MESSAGE("Testing reads");

            const auto sptr = db.local().find_schema("ks", "tbl");
            auto pk_def = sptr->get_column_definition("pk");
            auto dk = dht::decorate_key(*sptr, partition_key(pk));
            auto selection = cql3::selection::selection::for_columns(sptr, {pk_def});
            auto opts = selection->get_query_options();
            auto partition_slice = query::partition_slice(
                    {query::clustering_range::make_open_ended_both_sides()}, {}, {}, std::move(opts));

            auto cmd = make_lw_shared<query::read_command>(sptr->id(), sptr->version(), partition_slice, query::max_result_size(1), query::tombstone_limit::max, query::row_limit(1));
            cmd->allow_limit = allow_limit;

            // Rejection is probabilistic, so try many times
            for (int i = 0; i < 100; i++) {
                qp.local().proxy().query(sptr,
                        cmd,
                        {dht::partition_range(dk)},
                        db::consistency_level::ALL,
                        service::storage_proxy::coordinator_query_options(
                                db::timeout_clock::now() + std::chrono::seconds(10),
                                empty_service_permit(),
                                service::client_state::for_internal_calls())).get();
            }

            return make_ready_future<>();
        };

        auto sgroups = get_scheduling_groups().get();

        for (unsigned shard : {local_shard, foreign_shard}) {
            for (scheduling_group sg : {sgroups.statement_scheduling_group, sgroups.streaming_scheduling_group}) {
                for (db::allow_per_partition_rate_limit allow_limit : {db::allow_per_partition_rate_limit::yes, db::allow_per_partition_rate_limit::no}) {
                    // Rate limiting must be explicitly enabled and handled on the correct scheduling group.
                    const bool expect_limiting = (sg == sgroups.statement_scheduling_group) && bool(allow_limit);

                    BOOST_TEST_MESSAGE(format("Test config, shard: {}, scheduling_group: {}, allow_limit: {}, expect_limiting: {}",
                            (shard == local_shard) ? "local" : "foreign",
                            (sg == sgroups.statement_scheduling_group) ? "statement" : "streaming",
                            allow_limit,
                            expect_limiting));
                    
                    smp::submit_to(shard, [&] () mutable {
                        return seastar::async(thread_attributes{sg}, [&] {
                            if (expect_limiting) {
                                BOOST_REQUIRE_THROW(run_writes(allow_limit).get(), exceptions::rate_limit_exception);
                                BOOST_REQUIRE_THROW(run_reads(allow_limit).get(), exceptions::rate_limit_exception);
                            } else {
                                BOOST_REQUIRE_NO_THROW(run_writes(allow_limit).get());
                                BOOST_REQUIRE_NO_THROW(run_reads(allow_limit).get());
                            }
                        });
                    }).get();
                }
            }
        }

        return make_ready_future<>();
    });
}