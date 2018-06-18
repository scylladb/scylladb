#include <boost/test/unit_test.hpp>
#include <memory>
#include <utility>

#include "core/sstring.hh"
#include "core/future-util.hh"
#include "core/do_with.hh"
#include "core/distributed.hh"
#include "sstables/sstables.hh"
#include "tests/test-utils.hh"
#include "schema.hh"
#include "database.hh"
#include "sstables/compaction_manager.hh"
#include "mutation_reader.hh"
#include "sstable_test.hh"
#include "tmpdir.hh"
#include "cell_locking.hh"
#include "flat_mutation_reader_assertions.hh"
#include "sstable_utils.hh"
#include "service/storage_proxy.hh"

using namespace sstables;

static db::nop_large_partition_handler nop_lp_handler;

static inline dht::token create_token_from_key(sstring key) {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = dht::global_partitioner().get_token(key_view);
    assert(token == dht::global_partitioner().get_token(key_view));
    return token;
}

static inline std::vector<std::pair<sstring, dht::token>> token_generation_for_shard(shard_id shard, unsigned tokens_to_generate) {
    unsigned tokens = 0;
    unsigned key_id = 0;
    std::vector<std::pair<sstring, dht::token>> key_and_token_pair;

    key_and_token_pair.reserve(tokens_to_generate);
    dht::set_global_partitioner(to_sstring("org.apache.cassandra.dht.Murmur3Partitioner"));

    while (tokens < tokens_to_generate) {
        sstring key = to_sstring(key_id++);
        dht::token token = create_token_from_key(key);
        if (shard != dht::global_partitioner().shard_of(token)) {
            continue;
        }
        tokens++;
        key_and_token_pair.emplace_back(key, token);
    }
    assert(key_and_token_pair.size() == tokens_to_generate);

    std::sort(key_and_token_pair.begin(),key_and_token_pair.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    return key_and_token_pair;
}

static schema_ptr get_schema() {
    auto builder = schema_builder("tests", "sstable_resharding_test")
        .with_column("id", utf8_type, column_kind::partition_key)
        .with_column("value", int32_type);
    return builder.build();
}

void run_sstable_resharding_test() {
    cache_tracker tracker;
  for (const auto version : all_sstable_versions) {
    storage_service_for_tests ssft;
    auto tmp = make_lw_shared<tmpdir>();
    auto s = get_schema();
    auto cm = make_lw_shared<compaction_manager>();
    auto cl_stats = make_lw_shared<cell_locker_stats>();
    column_family::config cfg;
    cfg.large_partition_handler = &nop_lp_handler;
    auto cf = make_lw_shared<column_family>(s, cfg, column_family::no_commitlog(), *cm, *cl_stats, tracker);
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
        for (auto i : boost::irange(0u, smp::count)) {
            auto key_token_pair = token_generation_for_shard(i, keys_per_shard);
            BOOST_REQUIRE(key_token_pair.size() == keys_per_shard);
            muts[i].reserve(keys_per_shard);
            for (auto k : boost::irange(0u, keys_per_shard)) {
                auto m = get_mutation(key_token_pair[k].first, i);
                muts[i].push_back(m);
                mt->apply(std::move(m));
            }
        }
        auto sst = sstables::make_sstable(s, tmp->path, 0, version, sstables::sstable::format_types::big);
        write_memtable_to_sstable_for_test(*mt, sst).get();
    }
    auto sst = sstables::make_sstable(s, tmp->path, 0, version, sstables::sstable::format_types::big);
    sst->load().get();

    // FIXME: sstable write has a limitation in which it will generate sharding metadata only
    // for a single shard. workaround that by setting shards manually. from this test perspective,
    // it doesn't matter because we check each partition individually of each sstable created
    // for a shard that owns the shared input sstable.
    sstables::test(sst).set_shards(boost::copy_range<std::vector<unsigned>>(boost::irange(0u, smp::count)));

    auto filter_fname = sstables::test(sst).filename(component_type::Filter);
    uint64_t bloom_filter_size_before = file_size(filter_fname).get0();

    auto creator = [&cf, tmp, version] (shard_id shard) mutable {
        // we need generation calculated by instance of cf at requested shard,
        // or resource usage wouldn't be fairly distributed among shards.
        auto gen = smp::submit_to(shard, [&cf] () {
            return column_family_test::calculate_generation_for_new_table(*cf);
        }).get0();

        auto sst = sstables::make_sstable(cf->schema(), tmp->path, gen,
            version, sstables::sstable::format_types::big,
            gc_clock::now(), default_io_error_handler_gen());
        return sst;
    };
    auto new_sstables = sstables::reshard_sstables({ sst }, *cf, creator, std::numeric_limits<uint64_t>::max(), 0).get0();
    BOOST_REQUIRE(new_sstables.size() == smp::count);

    uint64_t bloom_filter_size_after = 0;

    for (auto& sstable : new_sstables) {
        auto new_sst = sstables::make_sstable(s, tmp->path, sstable->generation(),
            version, sstables::sstable::format_types::big);
        new_sst->load().get();
        filter_fname = sstables::test(new_sst).filename(component_type::Filter);
        bloom_filter_size_after += file_size(filter_fname).get0();
        auto shards = new_sst->get_shards_for_this_sstable();
        BOOST_REQUIRE(shards.size() == 1); // check sstable is unshared.
        auto shard = shards.front();
        BOOST_REQUIRE(column_family_test::calculate_shard_from_sstable_generation(new_sst->generation()) == shard);

        auto rd = assert_that(new_sst->as_mutation_source().make_reader(s));
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

