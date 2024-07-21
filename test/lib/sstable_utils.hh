/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>

#include "sstables/sstables.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/index_reader.hh"
#include "sstables/binary_search.hh"
#include "sstables/writer.hh"
#include "compaction/compaction_manager.hh"
#include "replica/memtable-sstable.hh"
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "gc_clock.hh"
#include <seastar/core/coroutine.hh>

using namespace sstables;
using namespace std::chrono_literals;

using validate = bool_class<struct validate_tag>;
// Must be called in a seastar thread.
sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, lw_shared_ptr<replica::memtable> mt);
sstables::shared_sstable make_sstable_containing(sstables::shared_sstable sst, lw_shared_ptr<replica::memtable> mt);
sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts, validate do_validate = validate::yes);
sstables::shared_sstable make_sstable_containing(sstables::shared_sstable sst, std::vector<mutation> muts, validate do_validate = validate::yes);

namespace sstables {

using sstable_ptr = shared_sstable;

class test {
    sstable_ptr _sst;
public:

    test(sstable_ptr s) : _sst(s) {}

    summary& _summary() {
        return _sst->_components->summary;
    }

    future<temporary_buffer<char>> data_read(reader_permit permit, uint64_t pos, size_t len) {
        return _sst->data_read(pos, len, std::move(permit));
    }

    std::unique_ptr<index_reader> make_index_reader(reader_permit permit) {
        return std::make_unique<index_reader>(_sst, std::move(permit));
    }

    struct index_entry {
        sstables::key sstables_key;
        partition_key key;
        uint64_t promoted_index_size;

        key_view get_key() const {
            return sstables_key;
        }
    };

    future<std::vector<index_entry>> read_indexes(reader_permit permit) {
        std::vector<index_entry> entries;
        auto s = _sst->get_schema();
        auto ir = make_index_reader(std::move(permit));
        std::exception_ptr err = nullptr;
        try {
            while (!ir->eof()) {
                co_await ir->read_partition_data();
                auto pk = ir->get_partition_key();
                entries.emplace_back(index_entry{sstables::key::from_partition_key(*s, pk),
                                        pk, ir->get_promoted_index_size()});
                co_await ir->advance_to_next_partition();
            }
        } catch (...) {
            err = std::current_exception();
        }
        co_await ir->close();
        if (err) {
            co_return coroutine::exception(std::move(err));
        }
        co_return entries;
    }

    future<> read_statistics() {
        return _sst->read_statistics();
    }

    statistics& get_statistics() {
        return _sst->_components->statistics;
    }

    future<> read_summary() noexcept {
        return _sst->read_summary();
    }

    future<summary_entry&> read_summary_entry(size_t i) {
        return _sst->read_summary_entry(i);
    }

    summary& get_summary() {
        return _sst->_components->summary;
    }

    summary move_summary() {
        return std::move(_sst->_components->summary);
    }

    future<> read_toc() noexcept {
        return _sst->read_toc();
    }

    auto& get_components() {
        return _sst->_recognized_components;
    }

    template <typename T>
    int binary_search(const dht::i_partitioner& p, const T& entries, const key& sk) {
        return sstables::binary_search(p, entries, sk);
    }

    void change_generation_number(sstables::generation_type generation) {
        _sst->_generation = generation;
    }

    future<> change_dir(sstring dir) {
        return _sst->_storage->change_dir_for_test(dir);
    }

    void set_data_file_size(uint64_t size) {
        _sst->_data_file_size = size;
    }

    void set_data_file_write_time(db_clock::time_point wtime) {
        _sst->_data_file_write_time = wtime;
    }

    void set_run_identifier(sstables::run_id identifier) {
        _sst->_run_identifier = identifier;
    }

    future<> store() {
        _sst->_recognized_components.erase(component_type::Index);
        _sst->_recognized_components.erase(component_type::Data);
        return seastar::async([sst = _sst] {
            sst->open_sstable("test");
            sst->write_statistics();
            sst->write_compression();
            sst->write_filter();
            sst->write_summary();
            sst->seal_sstable(false).get();
        });
    }

    // Used to create synthetic sstables for testing leveled compaction strategy.
    void set_values_for_leveled_strategy(uint64_t fake_data_size, uint32_t sstable_level, int64_t max_timestamp, const partition_key& first_key, const partition_key& last_key) {
        // Create a synthetic stats metadata
        stats_metadata stats = {};
        // leveled strategy sorts sstables by age using max_timestamp, let's set it to 0.
        stats.max_timestamp = max_timestamp;
        stats.sstable_level = sstable_level;

        set_values(first_key, last_key, std::move(stats), fake_data_size);
    }

    void set_values(const partition_key& first_key, const partition_key& last_key, stats_metadata stats, uint64_t data_file_size = 1) {
        _sst->_data_file_size = data_file_size;
        _sst->_index_file_size = std::max(1UL, uint64_t(data_file_size * 0.1));
        _sst->_metadata_size_on_disk = std::max(1UL, uint64_t(data_file_size * 0.01));
        // scylla component must be present for a sstable to be considered fully expired.
        _sst->_recognized_components.insert(component_type::Scylla);
        _sst->_components->statistics.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));
        _sst->_components->summary.first_key.value = sstables::key::from_partition_key(*_sst->_schema, first_key).get_bytes();
        _sst->_components->summary.last_key.value = sstables::key::from_partition_key(*_sst->_schema, last_key).get_bytes();
        _sst->set_first_and_last_keys();
        _sst->_components->statistics.contents[metadata_type::Compaction] = std::make_unique<compaction_metadata>();
        _sst->_run_identifier = run_id::create_random_id();
        _sst->_shards.push_back(this_shard_id());
    }

    void rewrite_toc_without_scylla_component() {
        _sst->_recognized_components.erase(component_type::Scylla);
        remove_file(_sst->filename(component_type::TOC)).get();
        _sst->_storage->open(*_sst);
        _sst->seal_sstable(false).get();
    }

    future<> remove_component(component_type c) {
        return remove_file(_sst->filename(c));
    }

    fs::path filename(component_type c) const {
        return fs::path(_sst->filename(c));
    }

    void set_shards(std::vector<unsigned> shards) {
        _sst->_shards = std::move(shards);
    }

    static future<> create_links(const sstable& sst, const sstring& dir) {
        return sst._storage->create_links(sst, std::filesystem::path(dir));
    }

    future<> move_to_new_dir(sstring new_dir, generation_type new_generation) {
        co_await _sst->_storage->move(*_sst, std::move(new_dir), new_generation, nullptr);
        _sst->_generation = std::move(new_generation);
    }

    void create_bloom_filter(uint64_t estimated_partitions, double max_false_pos_prob = 0.1) {
        _sst->_components->filter = utils::i_filter::get_filter(estimated_partitions, max_false_pos_prob, utils::filter_format::m_format);
        _sst->_total_reclaimable_memory.reset();
    }

    void write_filter() {
        _sst->_recognized_components.insert(component_type::Filter);
        _sst->write_filter();
    }

    size_t total_reclaimable_memory_size() const {
        return _sst->total_reclaimable_memory_size();
    }

    size_t reclaim_memory_from_components() {
        return _sst->reclaim_memory_from_components();
    }

    void reload_reclaimed_components() {
        _sst->reload_reclaimed_components().get();
    }

    const utils::filter_ptr& get_filter() const {
        return _sst->_components->filter;
    }
};

inline auto replacer_fn_no_op() {
    return [](sstables::compaction_completion_desc desc) -> void {};
}

template<typename AsyncAction>
requires requires (AsyncAction aa, sstables::sstable::version_types& c) { { aa(c) } -> std::same_as<future<>>; }
inline
future<> for_each_sstable_version(AsyncAction action) {
    return seastar::do_for_each(all_sstable_versions, std::move(action));
}

} // namespace sstables

using can_purge_tombstones = compaction_manager::can_purge_tombstones;
future<> run_compaction_task(test_env&, sstables::run_id output_run_id, table_state& table_s, noncopyable_function<future<> (sstables::compaction_data&)> job);
future<compaction_result> compact_sstables(test_env& env, sstables::compaction_descriptor descriptor, table_for_tests t,
                 std::function<shared_sstable()> creator, sstables::compaction_sstable_replacer_fn replacer = sstables::replacer_fn_no_op(),
                 can_purge_tombstones can_purge = can_purge_tombstones::yes);

shared_sstable make_sstable_easy(test_env& env, mutation_reader rd, sstable_writer_config cfg,
        sstables::generation_type gen, const sstable::version_types version = sstables::get_highest_sstable_version(), int expected_partition = 1, gc_clock::time_point = gc_clock::now());
shared_sstable make_sstable_easy(test_env& env, lw_shared_ptr<replica::memtable> mt, sstable_writer_config cfg,
        sstables::generation_type gen, const sstable::version_types v = sstables::get_highest_sstable_version(), int estimated_partitions = 1, gc_clock::time_point = gc_clock::now());


inline shared_sstable make_sstable_easy(test_env& env, mutation_reader rd, sstable_writer_config cfg,
        const sstable::version_types version = sstables::get_highest_sstable_version(), int expected_partition = 1) {
    return make_sstable_easy(env, std::move(rd), std::move(cfg), env.new_generation(), version, expected_partition);
}
inline shared_sstable make_sstable_easy(test_env& env, lw_shared_ptr<replica::memtable> mt, sstable_writer_config cfg,
        const sstable::version_types version = sstables::get_highest_sstable_version(), int estimated_partitions = 1, gc_clock::time_point query_time = gc_clock::now()) {
    return make_sstable_easy(env, std::move(mt), std::move(cfg), env.new_generation(), version, estimated_partitions, query_time);
}

lw_shared_ptr<replica::memtable> make_memtable(schema_ptr s, const std::vector<mutation>& muts);
std::vector<replica::memtable*> active_memtables(replica::table& t);
