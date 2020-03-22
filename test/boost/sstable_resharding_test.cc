#include <boost/test/unit_test.hpp>
#include <memory>
#include <utility>

#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/distributed.hh>
#include "types/map.hh"
#include "sstables/sstables.hh"
#include <seastar/testing/test_case.hh>
#include "schema.hh"
#include "database.hh"
#include "dht/murmur3_partitioner.hh"
#include "sstables/compaction_manager.hh"
#include "mutation_reader.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/test_services.hh"
#include "test/lib/tmpdir.hh"
#include "cell_locking.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/sstable_utils.hh"
#include "service/storage_proxy.hh"
#include "db/config.hh"

using namespace sstables;

static schema_ptr get_schema() {
    auto builder = schema_builder("tests", "sstable_resharding_test")
        .with_column("id", utf8_type, column_kind::partition_key)
        .with_column("value", int32_type);
    return builder.build();
}

void run_sstable_resharding_test() {
    test_env env;
    cache_tracker tracker;
  for (const auto version : all_sstable_versions) {
    storage_service_for_tests ssft;
    auto tmp = tmpdir();
    auto s = get_schema();
    auto cm = make_lw_shared<compaction_manager>();
    auto cl_stats = make_lw_shared<cell_locker_stats>();
    auto cf = make_lw_shared<column_family>(s, column_family_test_config(), column_family::no_commitlog(), *cm, *cl_stats, tracker);
    cf->mark_ready_for_writes();
    std::unordered_map<shard_id, std::vector<mutation>> muts;
    static constexpr auto keys_per_shard = 1000u;

    // create sst shared by all shards
    {
        auto mt = make_lw_shared<memtable>(s);
        auto get_mutation = [mt, s] (sstring key_to_write, auto value) {
            auto key = partition_key::from_exploded(*s, {to_bytes(key_to_write)});
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(value)), api::timestamp_type(0));
            return m;
        };
        auto cfg = db::config();
        for (auto i : boost::irange(0u, smp::count)) {
            auto key_token_pair = token_generation_for_shard(keys_per_shard, i, cfg.murmur3_partitioner_ignore_msb_bits());
            BOOST_REQUIRE(key_token_pair.size() == keys_per_shard);
            muts[i].reserve(keys_per_shard);
            for (auto k : boost::irange(0u, keys_per_shard)) {
                auto m = get_mutation(key_token_pair[k].first, i);
                muts[i].push_back(m);
                mt->apply(std::move(m));
            }
        }
        auto sst = env.make_sstable(s, tmp.path().string(), 0, version, sstables::sstable::format_types::big);
        write_memtable_to_sstable_for_test(*mt, sst).get();
    }
    auto sst = env.reusable_sst(s, tmp.path().string(), 0, version, sstables::sstable::format_types::big).get0();

    // FIXME: sstable write has a limitation in which it will generate sharding metadata only
    // for a single shard. workaround that by setting shards manually. from this test perspective,
    // it doesn't matter because we check each partition individually of each sstable created
    // for a shard that owns the shared input sstable.
    sstables::test(sst).set_shards(boost::copy_range<std::vector<unsigned>>(boost::irange(0u, smp::count)));

    auto filter_fname = sstables::test(sst).filename(component_type::Filter);
    uint64_t bloom_filter_size_before = file_size(filter_fname).get0();

    auto creator = [&env, &cf, &tmp, version] (shard_id shard) mutable {
        // we need generation calculated by instance of cf at requested shard,
        // or resource usage wouldn't be fairly distributed among shards.
        auto gen = smp::submit_to(shard, [&cf] () {
            return column_family_test::calculate_generation_for_new_table(*cf);
        }).get0();

        return env.make_sstable(cf->schema(), tmp.path().string(), gen,
            version, sstables::sstable::format_types::big);
    };
    auto new_sstables = sstables::reshard_sstables({ sst }, *cf, creator, std::numeric_limits<uint64_t>::max(), 0).get0();
    BOOST_REQUIRE(new_sstables.size() == smp::count);

    uint64_t bloom_filter_size_after = 0;

    for (auto& sstable : new_sstables) {
        auto new_sst = env.reusable_sst(s, tmp.path().string(), sstable->generation(),
            version, sstables::sstable::format_types::big).get0();
        filter_fname = sstables::test(new_sst).filename(component_type::Filter);
        bloom_filter_size_after += file_size(filter_fname).get0();
        auto shards = new_sst->get_shards_for_this_sstable();
        BOOST_REQUIRE(shards.size() == 1); // check sstable is unshared.
        auto shard = shards.front();
        BOOST_REQUIRE(column_family_test::calculate_shard_from_sstable_generation(new_sst->generation()) == shard);

        auto rd = assert_that(new_sst->as_mutation_source().make_reader(s, no_reader_permit()));
        BOOST_REQUIRE(muts[shard].size() == keys_per_shard);
        for (auto k : boost::irange(0u, keys_per_shard)) {
            rd.produces(muts[shard][k]);
        }
        rd.produces_end_of_stream();
    }
    BOOST_REQUIRE_CLOSE_FRACTION(float(bloom_filter_size_before), float(bloom_filter_size_after), 0.1);
  }
}

SEASTAR_TEST_CASE(sstable_resharding_test) {
    return seastar::async([] {
        run_sstable_resharding_test();
    });
}

SEASTAR_THREAD_TEST_CASE(sstable_resharding_strategy_tests) {
    test_env env;

    for (const auto version : all_sstable_versions) {
        auto s = make_lw_shared(schema({}, "ks", "cf", {{"p1", utf8_type}}, {}, {}, {}, utf8_type));
        auto get_sstable = [&] (int64_t gen, sstring first_key, sstring last_key) mutable {
            auto sst = env.make_sstable(s, "", gen, version, sstables::sstable::format_types::big);
            stats_metadata stats = {};
            stats.sstable_level = 1;
            sstables::test(sst).set_values(std::move(first_key), std::move(last_key), std::move(stats));
            return sst;
        };

        column_family_for_tests cf;

        auto tokens = token_generation_for_current_shard(2);
        auto stcs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::size_tiered, s->compaction_strategy_options());
        auto lcs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, s->compaction_strategy_options());

        auto sst1 = get_sstable(1, tokens[0].first, tokens[1].first);
        auto sst2 = get_sstable(2, tokens[1].first, tokens[1].first);

        {
            // TODO: sstable_test runs with smp::count == 1, thus we will not be able to stress it more
            // until we move this test case to sstable_resharding_test.
            auto descriptors = stcs.get_resharding_jobs(*cf, { sst1, sst2 });
            BOOST_REQUIRE(descriptors.size() == 2);
        }
        {
            auto ssts = std::vector<sstables::shared_sstable>{ sst1, sst2 };
            auto descriptors = lcs.get_resharding_jobs(*cf, ssts);
            auto expected_jobs = (ssts.size()+smp::count-1)/smp::count;
            BOOST_REQUIRE(descriptors.size() == expected_jobs);
        }
    }
}
