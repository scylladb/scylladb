/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/sstable_utils.hh"

#include "replica/database.hh"
#include "replica/memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include <boost/range/irange.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/range/algorithm/sort.hpp>
#include "sstables/version.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>

using namespace sstables;
using namespace std::chrono_literals;

std::vector<sstring> do_make_keys(unsigned n, const schema_ptr& s, size_t min_key_size, std::optional<shard_id> shard) {
    std::vector<std::pair<sstring, dht::decorated_key>> p;
    p.reserve(n);

    auto key_id = 0U;
    auto generated = 0U;
    while (generated < n) {
        auto raw_key = sstring(std::max(min_key_size, sizeof(key_id)), int8_t(0));
        std::copy_n(reinterpret_cast<int8_t*>(&key_id), sizeof(key_id), raw_key.begin());
        auto dk = dht::decorate_key(*s, partition_key::from_single_value(*s, to_bytes(raw_key)));
        key_id++;
        if (shard) {
            if (*shard != shard_of(*s, dk.token())) {
                continue;
            }
        }
        generated++;
        p.emplace_back(std::move(raw_key), std::move(dk));
    }
    boost::sort(p, [&] (auto& p1, auto& p2) {
        return p1.second.less_compare(*s, p2.second);
    });
    return boost::copy_range<std::vector<sstring>>(p | boost::adaptors::map_keys);
}

std::vector<sstring> do_make_keys(unsigned n, const schema_ptr& s, size_t min_key_size, local_shard_only lso) {
    return do_make_keys(n, s, min_key_size, lso ? std::optional(this_shard_id()) : std::nullopt);
}

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts) {
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto sst = sst_factory();
    schema_ptr s = muts[0].schema();
    auto mt = make_lw_shared<replica::memtable>(s);

    std::size_t i{0};
    for (auto&& m : muts) {
        mt->apply(m);
        ++i;

        // Give the reactor some time to breathe
        if(i == 10) {
            seastar::thread::yield();
            i = 0;
        }
    }
    write_memtable_to_sstable_for_test(*mt, sst).get();
    sst->open_data().get();

    std::set<mutation, mutation_decorated_key_less_comparator> merged;
    for (auto&& m : muts) {
        auto result = merged.insert(m);
        if (!result.second) {
            auto old = *result.first;
            merged.erase(result.first);
            merged.insert(old + m);
        }
    }

    // validate the sstable
    auto rd = assert_that(sst->as_mutation_source().make_reader_v2(s, semaphore.make_permit()));
    for (auto&& m : merged) {
        rd.produces(m);
    }
    rd.produces_end_of_stream();

    return sst;
}

shared_sstable make_sstable(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time) {
    auto mt = make_lw_shared<replica::memtable>(s);
    fs::path dir_path(dir);

    for (auto&& m : mutations) {
        mt->apply(m);
    }

    return make_sstable_easy(env, dir_path, mt, cfg, 1, version, mutations.size(), query_time);
}

shared_sstable make_sstable_easy(test_env& env, const fs::path& path, flat_mutation_reader_v2 rd, sstable_writer_config cfg,
        int64_t generation, const sstables::sstable::version_types version, int expected_partition) {
    auto s = rd.schema();
    auto sst = env.make_sstable(s, path.string(), generation, version, sstable_format_types::big);
    sst->write_components(std::move(rd), expected_partition, s, cfg, encoding_stats{}).get();
    sst->load().get();
    return sst;
}

shared_sstable make_sstable_easy(test_env& env, const fs::path& path, lw_shared_ptr<replica::memtable> mt, sstable_writer_config cfg,
        unsigned long gen, const sstable::version_types v, int estimated_partitions, gc_clock::time_point query_time) {
    schema_ptr s = mt->schema();
    auto sst = env.make_sstable(s, path.string(), gen, v, sstable_format_types::big, default_sstable_buffer_size, query_time);
    auto mr = mt->make_flat_reader(s, env.make_reader_permit());
    sst->write_components(std::move(mr), estimated_partitions, s, cfg, mt->get_encoding_stats()).get();
    sst->load().get();
    return sst;
}

std::vector<std::pair<sstring, dht::token>>
token_generation_for_shard(unsigned tokens_to_generate, unsigned shard,
        unsigned ignore_msb, unsigned smp_count) {
    unsigned tokens = 0;
    unsigned key_id = 0;
    std::vector<std::pair<sstring, dht::token>> key_and_token_pair;

    key_and_token_pair.reserve(tokens_to_generate);
    dht::murmur3_partitioner partitioner;
    dht::sharder sharder(smp_count, ignore_msb);

    while (tokens < tokens_to_generate) {
        sstring key = to_sstring(key_id++);
        dht::token token = create_token_from_key(partitioner, key);
        if (shard != sharder.shard_of(token)) {
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

future<compaction_result> compact_sstables(compaction_manager& cm, sstables::compaction_descriptor descriptor, replica::column_family& cf, std::function<shared_sstable()> creator, compaction_sstable_replacer_fn replacer,
                                           can_purge_tombstones can_purge) {
    descriptor.creator = [creator = std::move(creator)] (shard_id dummy) mutable {
        return creator();
    };
    descriptor.replacer = std::move(replacer);
    if (can_purge) {
        descriptor.enable_garbage_collection(cf.get_sstable_set());
    }
    auto cmt = compaction_manager_test(cm);
    sstables::compaction_result ret;
    co_await cmt.run(descriptor.run_identifier, &cf, [&] (sstables::compaction_data& cdata) {
        return sstables::compact_sstables(std::move(descriptor), cdata, cf.as_table_state()).then([&] (sstables::compaction_result res) {
            ret = std::move(res);
        });
    });
    co_return ret;
}

std::vector<std::pair<sstring, dht::token>> token_generation_for_current_shard(unsigned tokens_to_generate) {
    return token_generation_for_shard(tokens_to_generate, this_shard_id());
}

static sstring toc_filename(const sstring& dir, schema_ptr schema, unsigned int generation, sstable_version_types v) {
    return sstable::filename(dir, schema->ks_name(), schema->cf_name(), v, generation_from_value(generation),
                             sstable_format_types::big, component_type::TOC);
}

future<shared_sstable> test_env::reusable_sst(schema_ptr schema, sstring dir, unsigned long generation) {
    for (auto v : boost::adaptors::reverse(all_sstable_versions)) {
        if (co_await file_exists(toc_filename(dir, schema, generation, v))) {
            co_return co_await reusable_sst(schema, dir, generation, v);
        }
    }
    throw sst_not_found(dir, generation);
}

compaction_manager_for_testing::wrapped_compaction_manager::wrapped_compaction_manager(bool enabled)
        : cm(compaction_manager::for_testing_tag{})
{
    if (enabled) {
        cm.enable();
    }
}

// Must run in a seastar thread
compaction_manager_for_testing::wrapped_compaction_manager::~wrapped_compaction_manager() {
    cm.stop().get();
}

class compaction_manager::compaction_manager_test_task : public compaction_manager::task {
    utils::UUID _run_id;
    noncopyable_function<future<> (sstables::compaction_data&)> _job;

public:
    compaction_manager_test_task(compaction_manager& cm, replica::column_family* cf, utils::UUID run_id, noncopyable_function<future<> (sstables::compaction_data&)> job)
        : compaction_manager::task(cm, &cf->as_table_state(), sstables::compaction_type::Compaction, "Test compaction")
        , _run_id(run_id)
        , _job(std::move(job))
    { }

protected:
    virtual future<> do_run() override {
        setup_new_compaction(_run_id);
        return _job(_compaction_data);
    }
};

future<> compaction_manager_test::run(utils::UUID output_run_id, replica::column_family* cf, noncopyable_function<future<> (sstables::compaction_data&)> job) {
    auto task = make_shared<compaction_manager::compaction_manager_test_task>(_cm, cf, output_run_id, std::move(job));
    auto& cdata = register_compaction(task);
    return task->run().finally([this, &cdata] {
        deregister_compaction(cdata);
    });
}

sstables::compaction_data& compaction_manager_test::register_compaction(shared_ptr<compaction_manager::task> task) {
    testlog.debug("compaction_manager_test: register_compaction uuid={}: {}", task->compaction_data().compaction_uuid, *task);
    _cm._tasks.push_back(task);
    return task->compaction_data();
}

void compaction_manager_test::deregister_compaction(const sstables::compaction_data& c) {
    auto it = boost::find_if(_cm._tasks, [&c] (auto& task) { return task->compaction_data().compaction_uuid == c.compaction_uuid; });
    if (it != _cm._tasks.end()) {
        auto task = *it;
        testlog.debug("compaction_manager_test: deregister_compaction uuid={}: {}", c.compaction_uuid, *task);
        _cm._tasks.erase(it);
    } else {
        testlog.error("compaction_manager_test: deregister_compaction uuid={}: task not found", c.compaction_uuid);
    }
}
