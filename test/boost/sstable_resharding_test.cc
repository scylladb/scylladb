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
#include "compaction/compaction_manager.hh"
#include "mutation_reader.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/tmpdir.hh"
#include "cell_locking.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/sstable_utils.hh"
#include "service/storage_proxy.hh"
#include "db/config.hh"

using namespace sstables;

static schema_builder get_schema_builder(::schema_registry& registry) {
    return schema_builder(registry, "tests", "sstable_resharding_test")
        .with_column("id", utf8_type, column_kind::partition_key)
        .with_column("value", int32_type);
}

static schema_ptr get_schema(::schema_registry& registry) {
    return get_schema_builder(registry).build();
}

static schema_ptr get_schema(::schema_registry& registry, unsigned shard_count, unsigned sharding_ignore_msb_bits) {
    return get_schema_builder(registry).with_sharder(shard_count, sharding_ignore_msb_bits).build();
}

void run_sstable_resharding_test() {
    tests::schema_registry_wrapper registry;
    test_env env;
    auto close_env = defer([&] { env.stop().get(); });
    cache_tracker tracker;
  for (const auto version : writable_sstable_versions) {
    auto tmp = tmpdir();
    auto s = get_schema(registry);
    auto cm = make_lw_shared<compaction_manager>();
    auto cl_stats = make_lw_shared<cell_locker_stats>();
    auto cf = make_lw_shared<column_family>(s, column_family_test_config(env.manager(), env.semaphore()), column_family::no_commitlog(), *cm, *cl_stats, tracker);
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
        auto cfg = std::make_unique<db::config>();
        for (auto i : boost::irange(0u, smp::count)) {
            auto key_token_pair = token_generation_for_shard(keys_per_shard, i, cfg->murmur3_partitioner_ignore_msb_bits());
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

    auto descriptor = sstables::compaction_descriptor({sst}, std::nullopt, default_priority_class(), 0, std::numeric_limits<uint64_t>::max());
    descriptor.options = sstables::compaction_type_options::make_reshard();
    descriptor.creator = [&env, &cf, &tmp, version] (shard_id shard) mutable {
        // we need generation calculated by instance of cf at requested shard,
        // or resource usage wouldn't be fairly distributed among shards.
        auto gen = smp::submit_to(shard, [&cf] () {
            return column_family_test::calculate_generation_for_new_table(*cf);
        }).get0();

        return env.make_sstable(cf->schema(), tmp.path().string(), gen,
            version, sstables::sstable::format_types::big);
    };
    auto cdata = compaction_manager::create_compaction_data();
    auto res = sstables::compact_sstables(std::move(descriptor), cdata, *cf).get0();
    auto new_sstables = std::move(res.new_sstables);
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

        auto rd = assert_that(new_sst->as_mutation_source().make_reader(s, env.make_reader_permit()));
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

SEASTAR_TEST_CASE(sstable_is_shared_correctness) {
    return test_env::do_with_async([] (test_env& env) {
      tests::schema_registry_wrapper registry;
      for (const auto version : writable_sstable_versions) {
        cell_locker_stats cl_stats;

        auto tmp = tmpdir();
        auto cfg = std::make_unique<db::config>();

        auto get_mutation = [] (const schema_ptr& s, sstring key_to_write, auto value) {
            auto key = partition_key::from_exploded(*s, {to_bytes(key_to_write)});
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(value)), api::timestamp_type(0));
            return m;
        };
        auto gen = make_lw_shared<unsigned>(1);

        // created sstable owned only by this shard
        {
            auto s = get_schema(registry);
            auto sst_gen = [&env, s, &tmp, gen, version]() mutable {
                return env.make_sstable(s, tmp.path().string(), (*gen)++, version, big);
            };

            auto tokens = token_generation_for_shard(smp::count * 10, this_shard_id(), cfg->murmur3_partitioner_ignore_msb_bits());
            std::vector<mutation> muts;
            for (auto& t : tokens) {
                muts.push_back(get_mutation(s, t.first, 0));
            }

            auto sst = make_sstable_containing(sst_gen, muts);
            BOOST_REQUIRE(!sst->is_shared());
        }

        // create sstable owned by all shards
        // created unshared sstable
        {
            auto single_sharded_s = get_schema(registry, 1, cfg->murmur3_partitioner_ignore_msb_bits());
            auto sst_gen = [&env, single_sharded_s, &tmp, gen, version]() mutable {
                return env.make_sstable(single_sharded_s, tmp.path().string(), (*gen)++, version, big);
            };

            auto tokens = token_generation_for_shard(10, this_shard_id(), cfg->murmur3_partitioner_ignore_msb_bits());
            std::vector<mutation> muts;
            for (shard_id shard : boost::irange(0u, smp::count)) {
                auto tokens = token_generation_for_shard(10, shard, cfg->murmur3_partitioner_ignore_msb_bits());
                for (auto& t : tokens) {
                    muts.push_back(get_mutation(single_sharded_s, t.first, shard));
                }
            }

            auto sst = make_sstable_containing(sst_gen, muts);
            BOOST_REQUIRE(!sst->is_shared());

            auto all_shards_s = get_schema(registry, smp::count, cfg->murmur3_partitioner_ignore_msb_bits());
            sst = env.reusable_sst(all_shards_s, tmp.path().string(), sst->generation(), version).get0();
            BOOST_REQUIRE(smp::count == 1 || sst->is_shared());
            BOOST_REQUIRE(sst->get_shards_for_this_sstable().size() == smp::count);
        }
      }
    });
}
