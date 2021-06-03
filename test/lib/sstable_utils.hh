/*
 * Copyright (C) 2017-present ScyllaDB
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

#pragma once

#include "sstables/sstables.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/index_reader.hh"
#include "sstables/binary_search.hh"
#include "sstables/writer.hh"
#include "memtable-sstable.hh"
#include "dht/i_partitioner.hh"
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/reader_permit.hh"
#include "gc_clock.hh"

using namespace sstables;
using namespace std::chrono_literals;

struct local_shard_only_tag { };
using local_shard_only = bool_class<local_shard_only_tag>;

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts);

inline future<> write_memtable_to_sstable_for_test(memtable& mt, sstables::shared_sstable sst) {
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

    future<temporary_buffer<char>> data_read(uint64_t pos, size_t len) {
        return _sst->data_read(pos, len, default_priority_class(), tests::make_permit());
    }

    future<index_list> read_indexes() {
        auto l = make_lw_shared<index_list>();
        return do_with(std::make_unique<index_reader>(_sst, tests::make_permit(), default_priority_class(), tracing::trace_state_ptr()),
                [this, l] (std::unique_ptr<index_reader>& ir) {
            return ir->read_partition_data().then([&, l] {
                l->push_back(std::move(ir->current_partition_entry()));
            }).then([&, l] {
                return repeat([&, l] {
                    return ir->advance_to_next_partition().then([&, l] {
                        if (ir->eof()) {
                            return stop_iteration::yes;
                        }

                        l->push_back(std::move(ir->current_partition_entry()));
                        return stop_iteration::no;
                    });
                });
            });
        }).then([l] {
            return std::move(*l);
        });
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
        _sst->_generation = generation;
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

future<compaction_info> compact_sstables(sstables::compaction_descriptor descriptor, column_family& cf,
        std::function<shared_sstable()> creator, sstables::compaction_sstable_replacer_fn replacer = sstables::replacer_fn_no_op());
