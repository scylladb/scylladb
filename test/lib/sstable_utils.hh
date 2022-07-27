/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "sstables/sstables.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/index_reader.hh"
#include "sstables/binary_search.hh"
#include "sstables/writer.hh"
#include "compaction/compaction_manager.hh"
#include "replica/memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "gc_clock.hh"
#include <seastar/core/coroutine.hh>

using namespace sstables;
using namespace std::chrono_literals;

struct local_shard_only_tag { };
using local_shard_only = bool_class<local_shard_only_tag>;

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts);

inline future<> write_memtable_to_sstable_for_test(replica::memtable& mt, sstables::shared_sstable sst) {
    return write_memtable_to_sstable(mt, sst, sst->manager().configure_writer("memtable"));
}

//
// Make set of keys sorted by token for current or remote shard.
//
std::vector<sstring> do_make_keys(unsigned n, const schema_ptr& s, size_t min_key_size, std::optional<shard_id> shard);
std::vector<sstring> do_make_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1, local_shard_only lso = local_shard_only::yes);

inline std::vector<sstring> make_keys_for_shard(shard_id shard, unsigned n, const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(n, s, min_key_size, shard);
}

inline sstring make_key_for_shard(shard_id shard, const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(1, s, min_key_size, shard).front();
}

inline std::vector<sstring> make_local_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(n, s, min_key_size, local_shard_only::yes);
}

inline sstring make_local_key(const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(1, s, min_key_size, local_shard_only::yes).front();
}

inline std::vector<sstring> make_keys(unsigned n, const schema_ptr& s, size_t min_key_size = 1) {
    return do_make_keys(n, s, min_key_size, local_shard_only::no);
}

shared_sstable make_sstable(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time = gc_clock::now());

std::vector<std::pair<sstring, dht::token>>
token_generation_for_shard(unsigned tokens_to_generate, unsigned shard,
        unsigned ignore_msb = 0, unsigned smp_count = smp::count);

std::vector<std::pair<sstring, dht::token>>
token_generation_for_current_shard(unsigned tokens_to_generate);

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
        return _sst->data_read(pos, len, default_priority_class(), std::move(permit));
    }

    std::unique_ptr<index_reader> make_index_reader(reader_permit permit) {
        return std::make_unique<index_reader>(_sst, std::move(permit), default_priority_class(),
                                              tracing::trace_state_ptr(), use_caching::yes);
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
        return _sst->read_statistics(default_priority_class());
    }

    statistics& get_statistics() {
        return _sst->_components->statistics;
    }

    future<> read_summary() noexcept {
        return _sst->read_summary(default_priority_class());
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

    void change_generation_number(int64_t generation) {
        _sst->_generation = generation_from_value(generation);
    }

    void change_dir(sstring dir) {
        _sst->_dir = dir;
    }

    void set_data_file_size(uint64_t size) {
        _sst->_data_file_size = size;
    }

    void set_data_file_write_time(db_clock::time_point wtime) {
        _sst->_data_file_write_time = wtime;
    }

    void set_run_identifier(utils::UUID identifier) {
        _sst->_run_identifier = identifier;
    }

    future<> store() {
        _sst->_recognized_components.erase(component_type::Index);
        _sst->_recognized_components.erase(component_type::Data);
        return seastar::async([sst = _sst] {
            sst->write_toc(default_priority_class());
            sst->write_statistics(default_priority_class());
            sst->write_compression(default_priority_class());
            sst->write_filter(default_priority_class());
            sst->write_summary(default_priority_class());
            sst->seal_sstable().get();
        });
    }

    // Used to create synthetic sstables for testing leveled compaction strategy.
    void set_values_for_leveled_strategy(uint64_t fake_data_size, uint32_t sstable_level, int64_t max_timestamp, sstring first_key, sstring last_key) {
        _sst->_data_file_size = fake_data_size;
        _sst->_bytes_on_disk = fake_data_size;
        // Create a synthetic stats metadata
        stats_metadata stats = {};
        // leveled strategy sorts sstables by age using max_timestamp, let's set it to 0.
        stats.max_timestamp = max_timestamp;
        stats.sstable_level = sstable_level;
        _sst->_components->statistics.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));
        _sst->_components->summary.first_key.value = bytes(reinterpret_cast<const signed char*>(first_key.c_str()), first_key.size());
        _sst->_components->summary.last_key.value = bytes(reinterpret_cast<const signed char*>(last_key.c_str()), last_key.size());
        _sst->set_first_and_last_keys();
        _sst->_run_identifier = utils::make_random_uuid();
    }

    void set_values(sstring first_key, sstring last_key, stats_metadata stats) {
        // scylla component must be present for a sstable to be considered fully expired.
        _sst->_recognized_components.insert(component_type::Scylla);
        _sst->_components->statistics.contents[metadata_type::Stats] = std::make_unique<stats_metadata>(std::move(stats));
        _sst->_components->summary.first_key.value = bytes(reinterpret_cast<const signed char*>(first_key.c_str()), first_key.size());
        _sst->_components->summary.last_key.value = bytes(reinterpret_cast<const signed char*>(last_key.c_str()), last_key.size());
        _sst->set_first_and_last_keys();
        _sst->_components->statistics.contents[metadata_type::Compaction] = std::make_unique<compaction_metadata>();
        _sst->_run_identifier = utils::make_random_uuid();
    }

    void rewrite_toc_without_scylla_component() {
        _sst->_recognized_components.erase(component_type::Scylla);
        remove_file(_sst->filename(component_type::TOC)).get();
        _sst->write_toc(default_priority_class());
        _sst->seal_sstable().get();
    }

    future<> remove_component(component_type c) {
        return remove_file(_sst->filename(c));
    }

    const sstring filename(component_type c) const {
        return _sst->filename(c);
    }

    void set_shards(std::vector<unsigned> shards) {
        _sst->_shards = std::move(shards);
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

class test_setup {
    file _f;
    std::function<future<> (directory_entry de)> _walker;
    sstring _path;
    future<> _listing_done;

    static sstring& path() {
        static sstring _p = "test/resource/sstables/tests-temporary";
        return _p;
    };

public:
    test_setup(file f, sstring path)
            : _f(std::move(f))
            , _path(path)
            , _listing_done(_f.list_directory([this] (directory_entry de) { return _remove(de); }).done()) {
    }
    ~test_setup() {
        // FIXME: discarded future.
        (void)_f.close().finally([save = _f] {});
    }
protected:
    future<> _create_directory(sstring name) {
        return make_directory(name);
    }

    future<> _remove(directory_entry de) {
        sstring t = _path + "/" + de.name;
        return file_type(t).then([t] (std::optional<directory_entry_type> det) {
            auto f = make_ready_future<>();

            if (!det) {
                throw std::runtime_error("Can't determine file type\n");
            } else if (det == directory_entry_type::directory) {
                f = empty_test_dir(t);
            }
            return f.then([t] {
                return remove_file(t);
            });
        });
    }
    future<> done() { return std::move(_listing_done); }

    static future<> empty_test_dir(sstring p = path()) {
        return open_directory(p).then([p] (file f) {
            auto l = make_lw_shared<test_setup>(std::move(f), p);
            return l->done().then([l] { });
        });
    }
public:
    static future<> create_empty_test_dir(sstring p = path()) {
        return make_directory(p).then_wrapped([p] (future<> f) {
            try {
                f.get();
            // it's fine if the directory exists, just shut down the exceptional future message
            } catch (std::exception& e) {}
            return empty_test_dir(p);
        });
    }

    static future<> do_with_tmp_directory(std::function<future<> (test_env&, sstring tmpdir_path)>&& fut) {
        return seastar::async([fut = std::move(fut)] {
            auto tmp = tmpdir();
            test_env env;
            auto close_env = defer([&] { env.stop().get(); });
            fut(env, tmp.path().string()).get();
        });
    }

    static future<> do_with_cloned_tmp_directory(sstring src, std::function<future<> (test_env&, sstring srcdir_path, sstring destdir_path)>&& fut) {
        return seastar::async([fut = std::move(fut), src = std::move(src)] {
            auto src_dir = tmpdir();
            auto dest_dir = tmpdir();
            for (const auto& entry : std::filesystem::directory_iterator(src.c_str())) {
                std::filesystem::copy(entry.path(), src_dir.path() / entry.path().filename());
            }
            auto dest_path = dest_dir.path() / src.c_str();
            std::filesystem::create_directories(dest_path);
            test_env env;
            auto close_env = defer([&] { env.stop().get(); });
            fut(env, src_dir.path().string(), dest_path.string()).get();
        });
    }
};

} // namespace sstables

// Must be used in a seastar thread
class compaction_manager_for_testing {
    struct wrapped_compaction_manager {
        compaction_manager cm;
        explicit wrapped_compaction_manager(bool enabled);
        // Must run in a seastar thread
        ~wrapped_compaction_manager();
    };

    lw_shared_ptr<wrapped_compaction_manager> _wcm;
public:
    explicit compaction_manager_for_testing(bool enabled = true) : _wcm(make_lw_shared<wrapped_compaction_manager>(enabled)) {}

    compaction_manager& operator*() noexcept {
        return _wcm->cm;
    }
    const compaction_manager& operator*() const noexcept {
        return _wcm->cm;
    }

    compaction_manager* operator->() noexcept {
        return &_wcm->cm;
    }
    const compaction_manager* operator->() const noexcept {
        return &_wcm->cm;
    }
};

class compaction_manager_test {
    compaction_manager& _cm;
public:
    explicit compaction_manager_test(compaction_manager& cm) noexcept : _cm(cm) {}

    future<> run(utils::UUID output_run_id, replica::column_family* cf, noncopyable_function<future<> (sstables::compaction_data&)> job);

    void propagate_replacement(replica::table* t, const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added) {
        _cm.propagate_replacement(t->as_table_state(), removed, added);
    }
private:
    sstables::compaction_data& register_compaction(shared_ptr<compaction_manager::task> task);

    void deregister_compaction(const sstables::compaction_data& c);
};

using can_purge_tombstones = compaction_manager::can_purge_tombstones;
future<compaction_result> compact_sstables(compaction_manager& cm, sstables::compaction_descriptor descriptor, replica::column_family& cf,
        std::function<shared_sstable()> creator, sstables::compaction_sstable_replacer_fn replacer = sstables::replacer_fn_no_op(),
        can_purge_tombstones can_purge = can_purge_tombstones::yes);

shared_sstable make_sstable_easy(test_env& env, const fs::path& path, flat_mutation_reader_v2 rd, sstable_writer_config cfg,
        int64_t generation = 1, const sstables::sstable::version_types version = sstables::get_highest_sstable_version(), int expected_partition = 1);
shared_sstable make_sstable_easy(test_env& env, const fs::path& path, lw_shared_ptr<replica::memtable> mt, sstable_writer_config cfg,
        unsigned long gen = 1, const sstable::version_types v = sstables::get_highest_sstable_version(), int estimated_partitions = 1, gc_clock::time_point = gc_clock::now());
