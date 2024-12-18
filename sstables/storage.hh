/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/assert.hh"
#include <filesystem>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include "data_dictionary/storage_options.hh"
#include "seastarx.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/component_type.hh"
#include "sstables/generation_type.hh"
#include "utils/disk-error-handler.hh"
#include "sstables/sstable_directory.hh"

class schema;

namespace data_dictionary {
class storage_options;
}

namespace db { class config; }

namespace sstables {

enum class sstable_state;
class delayed_commit_changes;
class sstable;
class sstables_manager;
class entry_descriptor;

using atomic_delete_context = sstable_directory::pending_delete_result;

class opened_directory final {
    std::filesystem::path _pathname;
    file _file;

public:
    explicit opened_directory(std::filesystem::path pathname) : _pathname(std::move(pathname)) {};
    explicit opened_directory(const sstring &dir) : _pathname(std::string_view(dir)) {};
    opened_directory(const opened_directory&) = delete;
    opened_directory& operator=(const opened_directory&) = delete;
    opened_directory(opened_directory&&) = default;
    opened_directory& operator=(opened_directory&&) = default;
    ~opened_directory() = default;

    const std::filesystem::path::string_type& native() const noexcept {
        return _pathname.native();
    }

    const std::filesystem::path& path() const noexcept {
        return _pathname;
    }

    future<> sync(io_error_handler error_handler) {
        if (!_file) {
            _file = co_await do_io_check(error_handler, open_directory, _pathname.native());
        }
        co_await do_io_check(error_handler, std::mem_fn(&file::flush), _file);
    };

    future<> close() {
        return _file ? _file.close() : make_ready_future<>();
    }
};

class storage {
    friend class test;

    // Internal, but can also be used by tests
    virtual future<> change_dir_for_test(sstring nd) {
        SCYLLA_ASSERT(false && "Changing directory not implemented");
    }
    virtual future<> create_links(const sstable& sst, const std::filesystem::path& dir) const {
        SCYLLA_ASSERT(false && "Direct links creation not implemented");
    }
    virtual future<> move(const sstable& sst, sstring new_dir, generation_type generation, delayed_commit_changes* delay) {
        SCYLLA_ASSERT(false && "Direct move not implemented");
    }

public:
    virtual ~storage() {}

    using absolute_path = bool_class<class absolute_path_tag>; // FIXME -- should go away eventually
    using sync_dir = bool_class<struct sync_dir_tag>; // meaningful only to filesystem storage

    virtual future<> seal(const sstable& sst) = 0;
    virtual future<> snapshot(const sstable& sst, sstring dir, absolute_path abs, std::optional<generation_type> gen = {}) const = 0;
    virtual future<> change_state(const sstable& sst, sstable_state to, generation_type generation, delayed_commit_changes* delay) = 0;
    // runs in async context
    virtual void open(sstable& sst) = 0;
    virtual future<> wipe(const sstable& sst, sync_dir) noexcept = 0;
    virtual future<file> open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) = 0;
    virtual future<data_sink> make_data_or_index_sink(sstable& sst, component_type type) = 0;
    virtual future<data_sink> make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) = 0;
    virtual future<> destroy(const sstable& sst) = 0;
    virtual future<atomic_delete_context> atomic_delete_prepare(const std::vector<shared_sstable>&) const = 0;
    virtual future<> atomic_delete_complete(atomic_delete_context ctx) const = 0;
    virtual future<> remove_by_registry_entry(entry_descriptor desc) = 0;
    // Free space available in the underlying storage.
    virtual future<uint64_t> free_space() const = 0;

    virtual sstring prefix() const  = 0;
};

std::unique_ptr<sstables::storage> make_storage(sstables_manager& manager, const data_dictionary::storage_options& s_opts, sstable_state state);
future<lw_shared_ptr<const data_dictionary::storage_options>> init_table_storage(const sstables_manager&, const schema&, const data_dictionary::storage_options& so);
future<> destroy_table_storage(const data_dictionary::storage_options& so);
future<> init_keyspace_storage(const sstables_manager&, const data_dictionary::storage_options& so, sstring ks_name);

std::vector<std::filesystem::path> get_local_directories(const db::config& db, const data_dictionary::storage_options::local& so);

} // namespace sstables
