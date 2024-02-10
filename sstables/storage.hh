/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once


#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>

#include "data_dictionary/storage_options.hh"
#include "seastarx.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/component_type.hh"
#include "sstables/generation_type.hh"

namespace data_dictionary {
class storage_options;
}

namespace sstables {

enum class sstable_state;
class delayed_commit_changes;
class sstable;
class sstables_manager;
class entry_descriptor;
class atomic_delete_context_impl {
public:
    virtual ~atomic_delete_context_impl() {}
};
using atomic_delete_context = std::unique_ptr<atomic_delete_context_impl>;

class storage {
    friend class test;

    // Internal, but can also be used by tests
    virtual void change_dir_for_test(sstring nd) {
        assert(false && "Changing directory not implemented");
    }
    virtual future<> create_links(const sstable& sst, const std::filesystem::path& dir) const {
        assert(false && "Direct links creation not implemented");
    }
    virtual future<> move(const sstable& sst, sstring new_dir, generation_type generation, delayed_commit_changes* delay) {
        assert(false && "Direct move not implemented");
    }

public:
    virtual ~storage() {}

    using absolute_path = bool_class<class absolute_path_tag>; // FIXME -- should go away eventually
    using sync_dir = bool_class<struct sync_dir_tag>; // meaningful only to filesystem storage

    virtual future<> seal(const sstable& sst) = 0;
    virtual future<> snapshot(const sstable& sst, sstring dir, absolute_path abs) const = 0;
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

    virtual sstring prefix() const  = 0;
};

std::unique_ptr<sstables::storage> make_storage(sstables_manager& manager, const data_dictionary::storage_options& s_opts, sstring table_dir, sstable_state state);
future<> init_table_storage(const data_dictionary::storage_options& so, sstring dir);
future<> destroy_table_storage(const data_dictionary::storage_options& so, sstring dir);
future<> init_keyspace_storage(const data_dictionary::storage_options& so, sstring dir);

} // namespace sstables
