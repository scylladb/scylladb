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
#include "mutation_reader.hh"
#include "sstable_test.hh"
#include "tmpdir.hh"
#include "cell_locking.hh"
#include "mutation_reader_assertions.hh"
#include "memtable-sstable.hh"
#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace sstables;

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
    auto tmp = make_lw_shared<tmpdir>();
    auto s = get_schema();
    auto cm = make_lw_shared<compaction_manager>();
    auto cl_stats = make_lw_shared<cell_locker_stats>();
    auto cf = make_lw_shared<column_family>(s, column_family::config(), column_family::no_commitlog(), *cm, *cl_stats);
    cf->mark_ready_for_writes();
    std::unordered_map<shard_id, mutation> muts;

    // create sst shared by all shards
    {
        auto mt = make_lw_shared<memtable>(s);
        auto get_mutation = [mt, s] (sstring key_to_write, auto value) {
            auto key = partition_key::from_exploded(*s, {to_bytes(key_to_write)});
            mutation m(key, s);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(value)), api::timestamp_type(0));
            return m;
        };
        for (auto i : boost::irange(0u, smp::count)) {
            auto key_token_pair = token_generation_for_shard(i, 1);
            BOOST_REQUIRE(key_token_pair.size() == 1);
            auto m = get_mutation(key_token_pair[0].first, i);
            muts.emplace(i, m);
            mt->apply(std::move(m));
        }
        auto sst = make_lw_shared<sstable>(s, tmp->path, 0, sstables::sstable::version_types::ka, sstables::sstable::format_types::big);
        write_memtable_to_sstable(*mt, sst).get();
    }
    auto sst = make_lw_shared<sstables::sstable>(s, tmp->path, 0, sstables::sstable::version_types::ka, sstables::sstable::format_types::big);
    sst->load().get();

    auto creator = [&cf, tmp] (shard_id shard) mutable {
        // we need generation calculated by instance of cf at requested shard,
        // or resource usage wouldn't be fairly distributed among shards.
        auto gen = smp::submit_to(shard, [&cf] () {
            return column_family_test::calculate_generation_for_new_table(*cf);
        }).get0();

        auto sst = make_lw_shared<sstables::sstable>(cf->schema(), tmp->path, gen,
            sstables::sstable::version_types::ka, sstables::sstable::format_types::big,
            gc_clock::now(), default_io_error_handler_gen());
        return sst;
    };
    auto new_sstables = sstables::reshard_sstables({ sst }, *cf, creator, std::numeric_limits<uint64_t>::max(), 0).get0();
    BOOST_REQUIRE(new_sstables.size() == smp::count);

    for (auto& sstable : new_sstables) {
        auto new_sst = make_lw_shared<sstables::sstable>(s, tmp->path, sstable->generation(),
            sstables::sstable::version_types::ka, sstables::sstable::format_types::big);
        new_sst->load().get();
        auto shards = new_sst->get_shards_for_this_sstable();
        BOOST_REQUIRE(shards.size() == 1); // check sstable is unshared.
        auto shard = shards.front();
        BOOST_REQUIRE(column_family_test::calculate_shard_from_sstable_generation(new_sst->generation()) == shard);

        assert_that(sst->as_mutation_source()(s))
            .produces(muts.at(shard))
            .produces_end_of_stream();
    }
}

SEASTAR_TEST_CASE(sstable_resharding_test) {
    return seastar::async([] {
        run_sstable_resharding_test();
    });
}

